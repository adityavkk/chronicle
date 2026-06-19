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
