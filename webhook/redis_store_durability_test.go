package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// These integration tests cover the #16 Tier B durability path against a live
// Redis (REDIS_URL or redis://localhost:6379/14), skipped under -short like the
// rest of redis_store_test.go. The Tier B WAITAOF barrier needs an AOF server, so
// they skip when appendonly is off. They assert the correction-#3 separation
// directly: WAITAOF is durability, the (gen,wake_id) fence is exclusivity.

// requireAOF skips when the test Redis is not running with appendonly yes —
// WAITAOF errors without AOF and the Tier B path is only meaningful against one.
func requireAOF(t *testing.T, client goredis.UniversalClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	vals, err := client.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		t.Skipf("CONFIG GET appendonly: %v", err)
	}
	if vals["appendonly"] != "yes" {
		t.Skip("Tier B durability test needs Redis with appendonly yes (point REDIS_URL at an AOF server)")
	}
}

// TestStoreTierBArmDurableLocalFsync: with numReplicas=0 (the single-Redis local
// rig) WAITAOF 1 0 is satisfied by the local AOF fsync, so the arm is durable.
func TestStoreTierBArmDurableLocalFsync(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 0, 1000) // local AOF fsync only
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	res, err := s.ArmWake("s1", now, 1000, true, "w_a")
	if err != nil {
		t.Fatalf("Tier B arm with local fsync must be durable: %v", err)
	}
	if !res.Armed || res.Generation != 1 {
		t.Fatalf("arm result = %+v, want Armed gen=1", res)
	}
}

// TestStoreTierBArmShortReplicaSurfacedAsError: requiring a replica on a
// single-Redis rig yields the [1,0] short reply, surfaced as a *DurabilityShort
// Error and NEVER swallowed — yet the fence is still minted on the primary. This
// is correction #3 end to end: WAITAOF is durability, the fence is exclusivity.
func TestStoreTierBArmShortReplicaSurfacedAsError(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 1, 200) // require 1 replica; the local rig has none
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)

	_, err := s.ArmWake("s1", now, 1000, true, "w_a")
	if err == nil {
		t.Fatal("Tier B arm requiring a replica must surface the short WAITAOF reply as an error")
	}
	var de *DurabilityShortError
	if !errors.As(err, &de) {
		t.Fatalf("want *DurabilityShortError, got %T: %v", err, err)
	}
	if de.GotReplicas != 0 || de.WantReplicas != 1 {
		t.Errorf("short error counts = %+v, want replicas 0/1", de)
	}
	// The durability miss did NOT prevent the fence from being minted on the
	// primary: generation advanced and the phase moved to waking. The fence — not
	// the WAIT count — is what governs exclusivity.
	sub, ok, gerr := s.Get("s1")
	if gerr != nil || !ok {
		t.Fatalf("get after arm: ok=%v err=%v", ok, gerr)
	}
	if sub.Generation != 1 || sub.Phase != PhaseWaking {
		t.Errorf("fence must be minted on the primary despite the durability miss: gen=%d phase=%v", sub.Generation, sub.Phase)
	}
}

// TestStoreTierAIssuesNoWait: Tier A never blocks the write — even configured
// with a replica requirement it issues no WAIT and never reports a durability
// error (the fast, at-least-once default path).
func TestStoreTierAIssuesNoWait(t *testing.T) {
	base, _ := newTestStore(t)
	s := base.WithConsistency(TierA, 1, 200)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	res, err := s.ArmWake("s1", now, 1000, true, "w_a")
	if err != nil {
		t.Fatalf("Tier A must issue no WAIT and never error on durability: %v", err)
	}
	if !res.Armed {
		t.Fatal("Tier A arm should succeed")
	}
}

// TestStoreTierBClaimDurable: the pull-wake claim grant (claim.lua fence rotation)
// also takes the Tier B barrier; with local fsync it is durable.
func TestStoreTierBClaimDurable(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 0, 1000)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")
	cfg.Type = DispatchPullWake
	cfg.WakeStream = "wake/pool"
	_, _ = s.CreateOrConfirm("p1", cfg, nil, now)
	c, err := s.Claim("p1", "worker-1", "w_a", now, 1000)
	if err != nil {
		t.Fatalf("Tier B claim with local fsync must be durable: %v", err)
	}
	if !c.Claimed {
		t.Fatalf("claim result = %+v, want Claimed", c)
	}
}
