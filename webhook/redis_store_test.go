package webhook

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Integration tests run against live Redis (REDIS_URL or
// redis://localhost:6379/14) and are skipped under -short. The database is
// flushed at setup. They exercise the Lua scripts and the durability paths the
// conformance suite does not: restart survival, lease expiry, and the
// re-scoring due claim.

func newTestStore(t *testing.T) (*RedisStore, goredis.UniversalClient) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/14"
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	client := goredis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable (%s): %v", url, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client), client
}

func webhookCfg(url string) Config {
	return Config{Type: DispatchWebhook, Pattern: "events/*", WebhookURL: url, LeaseTTLMs: 1000}
}

func TestStoreCreateConfirmConflict(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")

	if st, err := s.CreateOrConfirm("s1", cfg, nil, now); err != nil || st != CreateCreated {
		t.Fatalf("first create = %v/%v, want CreateCreated", st, err)
	}
	if st, _ := s.CreateOrConfirm("s1", cfg, nil, now); st != CreateMatched {
		t.Fatalf("re-confirm identical = %v, want CreateMatched", st)
	}
	other := cfg
	other.WebhookURL = "https://w.example/other"
	if st, _ := s.CreateOrConfirm("s1", other, nil, now); st != CreateConflict {
		t.Fatalf("different config = %v, want CreateConflict", st)
	}
}

func TestStoreLinkPrecedenceAndGet(t *testing.T) {
	s, _ := newTestStore(t)
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now())

	if err := s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	// Explicit upgrades the glob link, preserving the cursor.
	if err := s.Link("s1", "events/a", LinkExplicit, "ignored"); err != nil {
		t.Fatal(err)
	}
	sub, ok, err := s.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if len(sub.Links) != 1 || sub.Links[0].LinkType != LinkExplicit {
		t.Fatalf("expected one explicit link, got %+v", sub.Links)
	}
	if sub.Links[0].AckedOffset != "0000000000000000_0000000000000000" {
		t.Fatalf("explicit upgrade must preserve cursor, got %q", sub.Links[0].AckedOffset)
	}
}

func TestStoreArmWakeSurvivesRestart(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	res, err := s.ArmWake("s1", now, 1000, true, "w_first")
	if err != nil || !res.Armed || res.Generation != 1 || res.WakeID != "w_first" {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	// Coalesce: a second arm while in flight is BUSY, not a new generation.
	if busy, _ := s.ArmWake("s1", now, 1000, true, "w_second"); !busy.Busy || busy.Generation != 1 {
		t.Fatalf("second arm should be BUSY at gen 1, got %+v", busy)
	}

	// Simulate an origin restart: a brand-new store over the same Redis sees the
	// durable wake/lease/cursor — the in-memory map is gone, the state is not.
	restarted := NewRedisStore(client)
	sub, ok, err := restarted.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get after restart: %v %v", ok, err)
	}
	if sub.Phase != PhaseWaking || sub.Generation != 1 || sub.WakeID != "w_first" {
		t.Fatalf("durable wake lost across restart: %+v", sub)
	}
	if len(sub.Links) != 1 || sub.Links[0].AckedOffset != "0000000000000000_0000000000000000" {
		t.Fatalf("durable cursor lost across restart: %+v", sub.Links)
	}
	// The lease deadline is a durable ZSET score, not a goroutine.
	if n, _ := client.ZCard(context.Background(), leaseZKey).Result(); n != 1 {
		t.Fatalf("lease ZSET should hold one durable deadline, got %d", n)
	}
}

func TestStoreClaimCASAckFence(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")
	cfg.Type = DispatchPullWake
	cfg.WakeStream = "wake/pool"
	_, _ = s.CreateOrConfirm("s1", cfg, nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	c1, err := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-1", "w_a", now, 1000)
	if err != nil || !c1.Claimed {
		t.Fatalf("claim1 = %+v err=%v", c1, err)
	}
	// Second worker is fenced out while the lease is held.
	if c2, _ := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-2", "w_b", now, 1000); !c2.Busy || c2.Holder != "worker-1" {
		t.Fatalf("claim2 should be BUSY held by worker-1, got %+v", c2)
	}
	// Stale-generation ack is fenced.
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation+9, c1.WakeID, c1.Generation+9, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("stale ack should FENCE, got %q", st)
	}
	// Correct ack advances the cursor forward-only and releases.
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000050"}}
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "OK" {
		t.Fatalf("valid ack = %q, want OK", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Links[0].AckedOffset != "0000000000000001_0000000000000050" {
		t.Fatalf("ack(done) must release and advance cursor: %+v", sub)
	}
	// A replayed ack on the now-cleared wake is fenced (cursor not advanced twice).
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "FENCED" {
		t.Fatalf("replayed ack should FENCE, got %q", st)
	}
}

func TestStoreClaimModeRejectsMixedLegacyAndShardedClaims(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("legacy-first", pullWakeCfg(), nil, now)
	legacy, err := s.Claim("legacy-first", ClaimModeLegacy, DefaultClaimShard, "worker-legacy", "w_legacy", now, 1000)
	if err != nil || !legacy.Claimed {
		t.Fatalf("legacy claim = %+v err=%v", legacy, err)
	}
	if mode, _ := client.HGet(context.Background(), subKey("legacy-first"), "claim_mode").Result(); mode != ClaimModeLegacy.String() {
		t.Fatalf("claim_mode = %q, want legacy", mode)
	}
	// Simulate an in-flight pre-upgrade legacy claim: base shard-0 lease/fence
	// fields exist, but the new claim_mode field has not been written yet.
	if err := client.HDel(context.Background(), subKey("legacy-first"), "claim_mode").Err(); err != nil {
		t.Fatal(err)
	}
	shard, _ := NewClaimShard(4)
	if got, _ := s.Claim("legacy-first", ClaimModeSharded, shard, "worker-sharded", "w_sharded", now, 1000); !got.ModeConflict || got.Mode != ClaimModeLegacy {
		t.Fatalf("sharded claim should conflict with legacy mode, got %+v", got)
	}
	if st, _ := s.Ack("legacy-first", ClaimModeSharded, DefaultClaimShard, legacy.Generation, legacy.WakeID, legacy.Generation, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("mixed-mode ack should FENCE, got %q", st)
	}
	if exists, _ := client.HExists(context.Background(), subKey("legacy-first"), "claim_mode").Result(); exists {
		t.Fatal("pre-upgrade conflict should be inferred from base fields, not claim_mode")
	}

	_, _ = s.CreateOrConfirm("sharded-first", pullWakeCfg(), nil, now)
	if exists, _ := client.HExists(context.Background(), subKey("sharded-first"), "claim_mode").Result(); exists {
		t.Fatal("fresh subscription should not start with claim_mode")
	}
	first, err := s.Claim("sharded-first", ClaimModeSharded, shard, "worker-sharded", "w_sharded", now, 1000)
	if err != nil || !first.Claimed {
		t.Fatalf("sharded claim = %+v err=%v", first, err)
	}
	if got, _ := s.Claim("sharded-first", ClaimModeLegacy, DefaultClaimShard, "worker-legacy", "w_legacy", now, 1000); !got.ModeConflict || got.Mode != ClaimModeSharded {
		t.Fatalf("legacy claim should conflict with sharded mode, got %+v", got)
	}
	if st, _ := s.Ack("sharded-first", ClaimModeLegacy, shard, first.Generation, first.WakeID, first.Generation, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("legacy ack against sharded mode should FENCE, got %q", st)
	}
}

func TestStoreLeaseExpiryAndDueReScore(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a")

	// Before the deadline: not expired.
	if st, _ := s.ExpireLease(NewLeaseRef("s1", DefaultClaimShard), base); st != "ACTIVE" {
		t.Fatalf("lease not yet due should be ACTIVE, got %q", st)
	}
	// After the deadline: expired, back to idle.
	after := base.Add(2 * time.Second)
	if st, _ := s.ExpireLease(NewLeaseRef("s1", DefaultClaimShard), after); st != "EXPIRED" {
		t.Fatalf("expired lease should be EXPIRED, got %q", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Holder {
		t.Fatalf("expiry must clear holder and idle the subscription: %+v", sub)
	}

	// Due claim re-scores forward, it does not remove (research 07 §6.1): a
	// crashed worker's item must recur.
	_, _ = s.ArmWake("s1", after, 1000, true, "w_b")
	due, err := s.DueLeases(after.Add(2*time.Second), 16, 30*time.Second)
	if err != nil || len(due) != 1 || due[0] != NewLeaseRef("s1", DefaultClaimShard) {
		t.Fatalf("due = %v err=%v, want [s1]", due, err)
	}
	if n, _ := client.ZCard(context.Background(), leaseZKey).Result(); n != 1 {
		t.Fatalf("claimed member must remain in the ZSET (re-scored, not removed), got %d", n)
	}
	// Immediately re-claiming finds nothing — it was re-scored into the future.
	if again, _ := s.DueLeases(after.Add(2*time.Second), 16, 30*time.Second); len(again) != 0 {
		t.Fatalf("re-scored member must not be due again immediately, got %v", again)
	}
}

func TestStoreSigningKeyPersistence(t *testing.T) {
	s, client := newTestStore(t)
	k1, err := s.LoadSigningKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A new store over the same Redis adopts the same persisted key (stable kid).
	k2, err := NewRedisStore(client).LoadSigningKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if k1.Kid != k2.Kid {
		t.Fatalf("kid not stable across restart: %q vs %q", k1.Kid, k2.Kid)
	}
}

func pullWakeCfg() Config {
	return Config{Type: DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool", LeaseTTLMs: 1000}
}

// TestClaimExpiredLeaseRotatesFence is the slice-2 hardening: when a claim takes
// over a lease whose deadline has passed (before the lease worker expires it),
// the fence must rotate so the deposed worker's still-unexpired token can no
// longer ack — otherwise two workers hold the same (generation, wake_id) and the
// single-holder invariant is broken.
func TestClaimExpiredLeaseRotatesFence(t *testing.T) {
	s, _ := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", pullWakeCfg(), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	// Worker A claims an idle subscription: mints generation 1, wake w_a.
	a, err := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-A", "w_a", base, 1000)
	if err != nil || !a.Claimed {
		t.Fatalf("claim A = %+v err=%v", a, err)
	}
	// Worker B claims after A's lease deadline has passed: takeover rotates.
	after := base.Add(2 * time.Second)
	b, err := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-B", "w_b", after, 1000)
	if err != nil || !b.Claimed {
		t.Fatalf("claim B = %+v err=%v", b, err)
	}
	if b.Generation == a.Generation || b.WakeID == a.WakeID {
		t.Fatalf("expired-lease takeover must rotate the fence: A=(gen %d,%s) B=(gen %d,%s)",
			a.Generation, a.WakeID, b.Generation, b.WakeID)
	}
	// The deposed worker A can no longer ack — its old generation/wake is fenced.
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, a.Generation, a.WakeID, a.Generation, true, nil, after, 1000); st != "FENCED" {
		t.Fatalf("deposed worker A ack should FENCE, got %q", st)
	}
	// The current holder B acks successfully.
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, b.Generation, b.WakeID, b.Generation, true, nil, after, 1000); st != "OK" {
		t.Fatalf("current holder B ack should be OK, got %q", st)
	}
}

// TestClaimUnexpiredLeaseStillBusy confirms the rotation change did not regress
// the BUSY path: a claim against an unexpired live lease is still rejected.
func TestClaimUnexpiredLeaseStillBusy(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	a, _ := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-A", "w_a", now, 1000)
	if !a.Claimed {
		t.Fatal("worker A should claim the idle subscription")
	}
	// B claims while A's lease is still live.
	b, _ := s.Claim("s1", ClaimModeLegacy, DefaultClaimShard, "worker-B", "w_b", now, 1000)
	if !b.Busy || b.Holder != "worker-A" {
		t.Fatalf("unexpired lease should be BUSY held by worker-A, got %+v", b)
	}
}

func TestClaimShardGranularityAllowsDisjointClaimsAndFences(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	shardA, _ := NewClaimShard(3)
	shardB, _ := NewClaimShard(7)

	a, err := s.Claim("s1", ClaimModeSharded, shardA, "worker-A", "w_a", now, 1000)
	if err != nil || !a.Claimed {
		t.Fatalf("shard A claim = %+v err=%v", a, err)
	}
	b, err := s.Claim("s1", ClaimModeSharded, shardB, "worker-B", "w_b", now, 1000)
	if err != nil || !b.Claimed {
		t.Fatalf("shard B claim = %+v err=%v", b, err)
	}
	if again, _ := s.Claim("s1", ClaimModeSharded, shardA, "worker-C", "w_c", now, 1000); !again.Busy || again.Holder != "worker-A" {
		t.Fatalf("same shard should still be single-holder, got %+v", again)
	}
	if st, _ := s.Ack("s1", ClaimModeSharded, shardB, a.Generation, a.WakeID, a.Generation, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("shard A fence must not ack shard B, got %q", st)
	}
	if st, _ := s.Ack("s1", ClaimModeSharded, shardA, a.Generation, a.WakeID, a.Generation, true, nil, now, 1000); st != "OK" {
		t.Fatalf("current shard A holder should ack, got %q", st)
	}
	if st, _ := s.Ack("s1", ClaimModeSharded, shardB, b.Generation, b.WakeID, b.Generation, true, nil, now, 1000); st != "OK" {
		t.Fatalf("current shard B holder should ack, got %q", st)
	}
	if n, _ := client.ZCard(context.Background(), leaseZKey).Result(); n != 0 {
		t.Fatalf("all sharded lease members should be removed after done acks, got %d", n)
	}
}

// TestReconcileRepairsMissingFanoutIndex is the slice-4 repair: a canonical link
// whose fan-out index entry was dropped by a crash is re-added from the link.
func TestReconcileRepairsMissingFanoutIndex(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now())
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	// Simulate the crash-dropped index: the canonical link survives, the fan-out
	// SADD did not happen.
	if err := client.SRem(ctx, streamSubsKey("events/a"), "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if subs, _ := s.StreamSubscribers("events/a"); len(subs) != 0 {
		t.Fatalf("precondition: fan-out should be empty, got %v", subs)
	}
	if err := s.ReconcileIndexes(); err != nil {
		t.Fatal(err)
	}
	if subs, _ := s.StreamSubscribers("events/a"); len(subs) != 1 || subs[0] != "s1" {
		t.Fatalf("repair should restore the fan-out entry from the canonical link, got %v", subs)
	}
}

// TestReconcileIndexDoesNotInventMembership proves the repair only mirrors
// canonical links and never invents stream membership.
func TestReconcileIndexDoesNotInventMembership(t *testing.T) {
	s, _ := newTestStore(t)
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now())
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	if err := s.ReconcileIndexes(); err != nil {
		t.Fatal(err)
	}
	if subs, _ := s.StreamSubscribers("events/unrelated"); len(subs) != 0 {
		t.Fatalf("repair must not invent membership for an unlinked stream, got %v", subs)
	}
	if subs, _ := s.StreamSubscribers("events/a"); len(subs) != 1 {
		t.Fatalf("the linked stream should be indexed, got %v", subs)
	}
}
