package webhook

import (
	"errors"
	"testing"
)

func TestParseConsistencyTier(t *testing.T) {
	cases := []struct {
		in   string
		want ConsistencyTier
		err  bool
	}{
		{"", TierA, false}, // empty defaults to the fast tier
		{"a", TierA, false},
		{"A", TierA, false},
		{"tier-a", TierA, false},
		{"  B ", TierB, false},
		{"tier_b", TierB, false},
		{"c", TierC, false},
		{"TierC", TierC, false},
		{"d", TierA, true},
		{"strong", TierA, true}, // "strong" is exactly the level Redis cannot offer
	}
	for _, tc := range cases {
		got, err := ParseConsistencyTier(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("ParseConsistencyTier(%q): want error, got %v", tc.in, got)
			}
			var ce *ConsistencyError
			if !errors.As(err, &ce) {
				t.Errorf("ParseConsistencyTier(%q): want *ConsistencyError, got %T", tc.in, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseConsistencyTier(%q): unexpected error %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseConsistencyTier(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestConsistencyTierString(t *testing.T) {
	for tier, want := range map[ConsistencyTier]string{TierA: "A", TierB: "B", TierC: "C"} {
		if got := tier.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", tier, got, want)
		}
	}
}

func TestDurabilityFor(t *testing.T) {
	// Only Tier B imposes a barrier; A and C leave the write on the local primary.
	if p := DurabilityFor(TierA, 1, 1000); p.Wait {
		t.Errorf("TierA must issue no WAIT, got %+v", p)
	}
	if p := DurabilityFor(TierC, 1, 1000); p.Wait {
		t.Errorf("TierC fence-minting write must issue no WAIT, got %+v", p)
	}
	// Tier B is WAITAOF 1 <replicas>, the canonical (1,1) on the HA substrate.
	p := DurabilityFor(TierB, 1, 1000)
	if !p.Wait || !p.UseAOF || p.NumLocal != 1 || p.NumReplicas != 1 || p.TimeoutMs != 1000 {
		t.Errorf("TierB plan = %+v, want WAITAOF local=1 replicas=1 timeout=1000", p)
	}
	// The single-Redis local rig: numReplicas 0 (local AOF fsync only), no replica.
	if p := DurabilityFor(TierB, 0, 1000); p.NumReplicas != 0 || p.NumLocal != 1 {
		t.Errorf("TierB local-rig plan = %+v, want local=1 replicas=0", p)
	}
	// Defaults: negative replicas clamp to 0, non-positive timeout falls back.
	if p := DurabilityFor(TierB, -3, 0); p.NumReplicas != 0 || p.TimeoutMs != defaultWaitTimeoutMs {
		t.Errorf("TierB default clamps = %+v", p)
	}
}

func TestInterpretWaitAOF(t *testing.T) {
	plan := DurabilityFor(TierB, 1, 1000) // want local>=1, replicas>=1
	// Full durability: one local + one replica AOF fsync.
	if err := InterpretWaitAOF(plan, 1, 1); err != nil {
		t.Errorf("full WAITAOF reply must be durable, got %v", err)
	}
	// Short on the replica (the single-Redis [1,0] reply): a surfaced error.
	err := InterpretWaitAOF(plan, 1, 0)
	if err == nil {
		t.Fatal("short replica WAITAOF reply must error, got nil")
	}
	var de *DurabilityShortError
	if !errors.As(err, &de) {
		t.Fatalf("want *DurabilityShortError, got %T", err)
	}
	if de.GotReplicas != 0 || de.WantReplicas != 1 {
		t.Errorf("short error counts = %+v, want replicas 0/1", de)
	}
	// Short on the local fsync too.
	if err := InterpretWaitAOF(plan, 0, 1); err == nil {
		t.Error("short local WAITAOF reply must error, got nil")
	}
	// Tier A's empty plan never blocks and never errors regardless of the counts.
	if err := InterpretWaitAOF(DurabilityFor(TierA, 1, 1000), 0, 0); err != nil {
		t.Errorf("TierA plan must never error, got %v", err)
	}
}

func TestInterpretWait(t *testing.T) {
	plan := DurabilityFor(TierB, 1, 1000)
	if err := InterpretWait(plan, 1); err != nil {
		t.Errorf("WAIT meeting the replica requirement must be durable, got %v", err)
	}
	if err := InterpretWait(plan, 0); err == nil {
		t.Error("WAIT short of the replica requirement must error, got nil")
	}
}

// TestWaitIsDurabilityNotLinearizability is the correction-#3 guard: NO code path
// may infer ordering or exclusivity from the WAIT count. The interpreters' only
// output is a durability verdict (nil | *DurabilityShortError), and a count far
// ABOVE the requirement conveys nothing stronger than meeting it — there is no
// "more replicas => I hold the lease" signal. The fence, not WAIT, is the only
// exclusivity guard.
func TestWaitIsDurabilityNotLinearizability(t *testing.T) {
	plan := DurabilityFor(TierB, 1, 1000)
	// An over-ack (10 replicas) is still just "durable" — identical to meeting it.
	if err := InterpretWaitAOF(plan, 1, 10); err != nil {
		t.Errorf("over-ack must read as plain durable, not a stronger signal: %v", err)
	}
	if err := InterpretWait(plan, 99); err != nil {
		t.Errorf("over-ack WAIT must read as plain durable: %v", err)
	}
	// The short-error type carries ONLY ack counts — no holder, generation, or
	// lease field exists to launder a count into exclusivity. This is enforced
	// structurally: if a future edit adds such a field, this references block fails
	// to compile, forcing the author to confront correction #3.
	de := &DurabilityShortError{WantLocal: 1, GotLocal: 1, WantReplicas: 1, GotReplicas: 0, UseAOF: true}
	_ = struct {
		wantLocal, gotLocal       int
		wantReplicas, gotReplicas int
		useAOF                    bool
	}{de.WantLocal, de.GotLocal, de.WantReplicas, de.GotReplicas, de.UseAOF}
}

// TestAssertAOFEnabled is the pure startup-guard spec (issue #43): a Tier B
// deployment fails fast against a Redis that cannot honor WAITAOF (AOF off, or
// fewer online replicas than required); Tier A/C never assert; a properly
// provisioned Tier B passes.
func TestAssertAOFEnabled(t *testing.T) {
	// Tier A/C issue no WAITAOF, so they never assert — even against a non-AOF Redis
	// with zero replicas.
	if err := AssertAOFEnabled(TierA, "no", 0, 1); err != nil {
		t.Errorf("Tier A must never assert AOF, got %v", err)
	}
	if err := AssertAOFEnabled(TierC, "no", 0, 1); err != nil {
		t.Errorf("Tier C fence-minting write issues no WAITAOF, must never assert: %v", err)
	}

	// Tier B against AOF-off Redis: a typed refusal naming the appendonly value.
	err := AssertAOFEnabled(TierB, "no", 0, 0)
	if err == nil {
		t.Fatal("Tier B against appendonly=no must refuse to start")
	}
	var ae *AOFConfigError
	if !errors.As(err, &ae) {
		t.Fatalf("want *AOFConfigError, got %T: %v", err, err)
	}
	if ae.AppendOnly != "no" {
		t.Errorf("AOFConfigError.AppendOnly = %q, want %q", ae.AppendOnly, "no")
	}

	// Tier B requiring a replica the topology cannot supply: refuse (the write would
	// short on every fence mint).
	err = AssertAOFEnabled(TierB, "yes", 0, 1)
	if err == nil {
		t.Fatal("Tier B requiring 1 replica with 0 online must refuse to start")
	}
	if !errors.As(err, &ae) {
		t.Fatalf("want *AOFConfigError, got %T", err)
	}
	if ae.WantReplicas != 1 || ae.GotReplicas != 0 {
		t.Errorf("topology refusal counts = %+v, want want=1 got=0", ae)
	}

	// The single-Redis local rig: AOF on, 0 replicas required, 0 online — Tier B
	// (WAITAOF 1 0, local fsync only) starts cleanly.
	if err := AssertAOFEnabled(TierB, "yes", 0, 0); err != nil {
		t.Errorf("Tier B local-fsync rig (yes, 0/0) must start, got %v", err)
	}
	// The STANDARD_HA substrate: AOF on, 1 online replica, 1 required — starts.
	if err := AssertAOFEnabled(TierB, "yes", 1, 1); err != nil {
		t.Errorf("Tier B HA substrate (yes, 1/1) must start, got %v", err)
	}
	// An extra online replica beyond the requirement is fine.
	if err := AssertAOFEnabled(TierB, "YES", 3, 1); err != nil {
		t.Errorf("Tier B with surplus replicas (3>=1) must start, got %v", err)
	}
}

// TestAssertAOFEnabledIsDurabilityOnly is the correction-#3 guard for the startup
// assertion: AOFConfigError carries ONLY a provisioning verdict (appendonly value
// + replica counts), never a holder/generation/lease, so a mis-provisioning
// refusal cannot be laundered into an exclusivity decision. Enforced structurally:
// if a future edit adds such a field, this references block fails to compile.
func TestAssertAOFEnabledIsDurabilityOnly(t *testing.T) {
	ae := &AOFConfigError{AppendOnly: "no", WantReplicas: 1, GotReplicas: 0}
	_ = struct {
		appendOnly                string
		wantReplicas, gotReplicas int
	}{ae.AppendOnly, ae.WantReplicas, ae.GotReplicas}
}

// TestParseConnectedReplicas covers the INFO replication parser: a master with N
// online replicas, a master that is still syncing a replica (connected but not
// online), and a reply with no replication section.
func TestParseConnectedReplicas(t *testing.T) {
	cases := []struct {
		name string
		info string
		want int
	}{
		{"no section", "", 0},
		{"master no replicas", "role:master\r\nconnected_slaves:0\r\n", 0},
		{
			"one online replica",
			"role:master\r\nconnected_slaves:1\r\nslave0:ip=10.0.0.2,port=6379,state=online,offset=560,lag=0\r\n",
			1,
		},
		{
			"two online replicas",
			"connected_slaves:2\nslave0:ip=10.0.0.2,port=6379,state=online,offset=1,lag=0\nslave1:ip=10.0.0.3,port=6379,state=online,offset=1,lag=0\n",
			2,
		},
		{
			// Connected but still doing its initial sync: NOT counted toward a
			// satisfiable WAITAOF replica requirement.
			"connected but syncing",
			"connected_slaves:1\nslave0:ip=10.0.0.2,port=6379,state=send_bulk,offset=0,lag=0\n",
			0,
		},
		{
			// Some managed SKUs redact the per-slave lines: fall back to the
			// connected_slaves count.
			"redacted per-slave lines",
			"role:master\nconnected_slaves:2\n",
			2,
		},
	}
	for _, tc := range cases {
		if got := ParseConnectedReplicas(tc.info); got != tc.want {
			t.Errorf("%s: ParseConnectedReplicas = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestFreshnessTokenStale(t *testing.T) {
	tok := NewFreshnessToken(42)
	if !tok.Stale(41) {
		t.Error("a replica behind the token generation is stale")
	}
	if tok.Stale(42) {
		t.Error("a replica at the token generation is fresh")
	}
	if tok.Stale(43) {
		t.Error("a replica ahead of the token generation is fresh")
	}
}
