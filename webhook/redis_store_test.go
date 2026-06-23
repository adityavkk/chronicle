package webhook

import (
	"context"
	"errors"
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
	if n, _ := client.ZCard(context.Background(), leaseZKey(slotOf("s1"))).Result(); n != 1 {
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

	c1, err := s.Claim("s1", "worker-1", "w_a", now, 1000)
	if err != nil || !c1.Claimed {
		t.Fatalf("claim1 = %+v err=%v", c1, err)
	}
	// Second worker is fenced out while the lease is held.
	if c2, _ := s.Claim("s1", "worker-2", "w_b", now, 1000); !c2.Busy || c2.Holder != "worker-1" {
		t.Fatalf("claim2 should be BUSY held by worker-1, got %+v", c2)
	}
	// Stale-generation ack is fenced.
	if st, _ := s.Ack("s1", c1.Generation+9, c1.WakeID, c1.Generation+9, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("stale ack should FENCE, got %q", st)
	}
	// Correct ack advances the cursor forward-only and releases.
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000050"}}
	if st, _ := s.Ack("s1", c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "OK" {
		t.Fatalf("valid ack = %q, want OK", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Links[0].AckedOffset != "0000000000000001_0000000000000050" {
		t.Fatalf("ack(done) must release and advance cursor: %+v", sub)
	}
	// A replayed ack on the now-cleared wake is fenced (cursor not advanced twice).
	if st, _ := s.Ack("s1", c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "FENCED" {
		t.Fatalf("replayed ack should FENCE, got %q", st)
	}
}

func TestStoreLeaseExpiryAndDueReScore(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a")

	// Before the deadline: not expired.
	if st, _ := s.ExpireLease("s1", base); st != "ACTIVE" {
		t.Fatalf("lease not yet due should be ACTIVE, got %q", st)
	}
	// After the deadline: expired, back to idle.
	after := base.Add(2 * time.Second)
	if st, _ := s.ExpireLease("s1", after); st != "EXPIRED" {
		t.Fatalf("expired lease should be EXPIRED, got %q", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Holder {
		t.Fatalf("expiry must clear holder and idle the subscription: %+v", sub)
	}

	// Due claim re-scores forward, it does not remove (research 07 §6.1): a
	// crashed worker's item must recur.
	_, _ = s.ArmWake("s1", after, 1000, true, "w_b")
	due, err := s.DueLeases(slotOf("s1"), after.Add(2*time.Second), 16, 30*time.Second)
	if err != nil || len(due) != 1 || due[0] != "s1" {
		t.Fatalf("due = %v err=%v, want [s1]", due, err)
	}
	if n, _ := client.ZCard(context.Background(), leaseZKey(slotOf("s1"))).Result(); n != 1 {
		t.Fatalf("claimed member must remain in the ZSET (re-scored, not removed), got %d", n)
	}
	// Immediately re-claiming finds nothing — it was re-scored into the future.
	if again, _ := s.DueLeases(slotOf("s1"), after.Add(2*time.Second), 16, 30*time.Second); len(again) != 0 {
		t.Fatalf("re-scored member must not be due again immediately, got %v", again)
	}
}

// dueCard reads the due-set outbox cardinality — the count of subscriptions
// currently owed a wake. It must track owed and return to 0 at quiescence.
func dueCard(t *testing.T, client goredis.UniversalClient) int64 {
	t.Helper()
	// Slot-homed: the due outbox is sharded into S per-slot ZSETs, so the global
	// "owed" count is their sum (a sub's mark lives in dueZKey(slotOf(id))).
	ctx := context.Background()
	var total int64
	for h := 0; h < subSlots; h++ {
		n, err := client.ZCard(ctx, dueZKey(h)).Result()
		if err != nil {
			t.Fatalf("zcard due slot %d: %v", h, err)
		}
		total += n
	}
	return total
}

// TestRestoreLeaseReDerivesDroppedTail is the issue-#13 store contract for the
// failover-aware eager reconcile: a live/waking sub whose lease (and due) tail a
// failover dropped is re-derived from the durable sub hash. The re-ZADD restores
// the exact lease_until_ns the hash carries (so the lease worker sees the same
// lapse), re-owes the due mark only when owed, and is conditioned on the live/
// waking phase so an idled sub is left untouched (no stale schedule entry leaked).
func TestRestoreLeaseReDerivesDroppedTail(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	// Arm a webhook wake: phase=waking, lease ZADDed, due ZADDed (the arm outbox).
	res, err := s.ArmWake("s1", now, 1000, true, "w_a")
	if err != nil || !res.Armed {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	// The deadline the durable hash carries — what restore_lease.lua re-derives.
	sub, _, _ := s.Get("s1")
	if sub.LeaseUntilNs == 0 {
		t.Fatalf("armed webhook lease should parse a non-zero lease_until_ns, got %d", sub.LeaseUntilNs)
	}

	// The L3 fault: drop the lease AND due tail, leaving the sub hash intact.
	if err := client.ZRem(ctx, leaseZKey(slotOf("s1")), "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZRem(ctx, dueZKey(slotOf("s1")), "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if ids, _ := s.LeasedIDs(); len(ids) != 0 {
		t.Fatalf("lease tail should be dropped, LeasedIDs=%v", ids)
	}

	// Restore: owed (the sub has pending work) re-ZADDs both entries, re-derived
	// from the durable hash's lease_until_ns.
	if st, err := s.RestoreLease("s1", true, now); err != nil || st != "RESTORED" {
		t.Fatalf("RestoreLease = %q err=%v, want RESTORED", st, err)
	}
	got, err := client.ZScore(ctx, leaseZKey(slotOf("s1")), "s1").Result()
	if err != nil {
		t.Fatalf("lease entry should be restored: %v", err)
	}
	// Re-derived from the hash, so it matches the parsed deadline (within the float
	// rounding the Lua double already carried, far under a lease's ms granularity).
	if delta := got - float64(sub.LeaseUntilNs); delta > 1e6 || delta < -1e6 {
		t.Fatalf("restored lease score %v should match the hash deadline %d (delta %v ns)", got, sub.LeaseUntilNs, delta)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("owed restore should re-owe the due mark, got card %d", n)
	}
	if ids, _ := s.LeasedIDs(); len(ids) != 1 || ids[0] != "s1" {
		t.Fatalf("LeasedIDs should now see the restored sub, got %v", ids)
	}

	// An idle sub is never stranded: RestoreLease must leave the schedule untouched
	// (else a stale entry would churn the lease ZSET forever via claim_due).
	if st, _ := s.Release("s1", res.Generation, res.WakeID, res.Generation); st != "OK" {
		t.Fatalf("release to idle = %q, want OK", st)
	}
	if ids, _ := s.LeasedIDs(); len(ids) != 0 {
		t.Fatalf("release should drop the lease entry, got %v", ids)
	}
	if st, err := s.RestoreLease("s1", true, now); err != nil || st != "INTACT" {
		t.Fatalf("RestoreLease on an idle sub = %q err=%v, want INTACT (no stale entry)", st, err)
	}
	if ids, _ := s.LeasedIDs(); len(ids) != 0 {
		t.Fatalf("RestoreLease must not leak a lease entry for an idle sub, got %v", ids)
	}
}

// TestDueSetArmOutboxesAndAckClears is the core Move-2 round trip: arm_wake's
// ARMED branch outboxes the wake into the due-set, a coalescing re-arm (BUSY) does
// not add a second mark, and ack(done=1) clears it.
func TestDueSetArmOutboxesAndAckClears(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	res, err := s.ArmWake("s1", now, 1000, true, "w_a")
	if err != nil || !res.Armed {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("ARMED branch must ZADD the due mark, got card %d", n)
	}
	// Coalesce: a BUSY re-arm must not touch the due-set (no second mark).
	if busy, _ := s.ArmWake("s1", now, 1000, true, "w_b"); !busy.Busy {
		t.Fatalf("second arm should be BUSY, got %+v", busy)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("BUSY (coalesced) re-arm must not add a due mark, got card %d", n)
	}
	// ack(done=1) clears the mark alongside the lease/retry ZREMs.
	if st, _ := s.Ack("s1", res.Generation, res.WakeID, res.Generation, true, nil, now, 1000); st != "OK" {
		t.Fatalf("ack(done) = %q, want OK", st)
	}
	if n := dueCard(t, client); n != 0 {
		t.Fatalf("ack(done=1) must ZREM the due mark, got card %d", n)
	}
}

// TestDueSetAckHeartbeatLeavesMark proves a heartbeat ack (done=0) keeps the wake
// owed — only a done-ack clears the due mark.
func TestDueSetAckHeartbeatLeavesMark(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	res, _ := s.ArmWake("s1", now, 1000, true, "w_a")

	if st, _ := s.Ack("s1", res.Generation, res.WakeID, res.Generation, false, nil, now, 1000); st != "OK" {
		t.Fatalf("heartbeat ack = %q, want OK", st)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("heartbeat (done=0) must NOT clear the due mark, got card %d", n)
	}
}

// TestDueSetExpireLeaseReOwes proves expire_lease's EXPIRED branch re-owes the
// wake at a fresh now_ns (a re-armed-after-lapse sub re-ZADDs at the new time).
func TestDueSetExpireLeaseReOwes(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a")

	armScore, err := client.ZScore(context.Background(), dueZKey(slotOf("s1")), "s1").Result()
	if err != nil {
		t.Fatalf("zscore after arm: %v", err)
	}
	// After the deadline: EXPIRED re-owes at the new now_ns.
	after := base.Add(2 * time.Second)
	if st, _ := s.ExpireLease("s1", after); st != "EXPIRED" {
		t.Fatalf("expired lease = %q, want EXPIRED", st)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("EXPIRED branch must re-owe the due mark, got card %d", n)
	}
	reScore, _ := client.ZScore(context.Background(), dueZKey(slotOf("s1")), "s1").Result()
	if reScore <= armScore {
		t.Fatalf("re-owe must ZADD at the new now_ns: arm=%v re-owe=%v", armScore, reScore)
	}
}

// TestDueSetReleaseClearsPhantom is the GAP3 regression: release idles the sub
// exactly like ack(done), so it MUST clear the due mark — a voluntarily-released
// sub leaves no phantom mark the dueWorker would re-fire forever.
func TestDueSetReleaseClearsPhantom(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	res, _ := s.ArmWake("s1", now, 1000, true, "w_a")
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("precondition: arm should outbox one due mark, got %d", n)
	}
	if st, _ := s.Release("s1", res.Generation, res.WakeID, res.Generation); st != "OK" {
		t.Fatalf("release = %q, want OK", st)
	}
	if n := dueCard(t, client); n != 0 {
		t.Fatalf("GAP3: release must clear the due mark, got phantom card %d", n)
	}
}

// TestDueSetClaimDueReScoresForward mirrors the lease/retry re-score contract for
// the due path: ClaimDue re-scores members forward (never ZREM), so the due-set is
// at-least-once and a claimed mark is not immediately due again.
func TestDueSetClaimDueReScoresForward(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", now, 1000, true, "w_a")

	due, err := s.ClaimDue(slotOf("s1"), now.Add(time.Millisecond), 16, 30*time.Second)
	if err != nil || len(due) != 1 || due[0] != "s1" {
		t.Fatalf("ClaimDue = %v err=%v, want [s1]", due, err)
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("claimed mark must remain (re-scored, not removed), got card %d", n)
	}
	if again, _ := s.ClaimDue(slotOf("s1"), now.Add(time.Millisecond), 16, 30*time.Second); len(again) != 0 {
		t.Fatalf("re-scored mark must not be due again immediately, got %v", again)
	}
	// ClearDue removes it (the dueWorker's reconcile of a no-longer-owed mark).
	if err := s.ClearDue("s1"); err != nil {
		t.Fatalf("ClearDue: %v", err)
	}
	if n := dueCard(t, client); n != 0 {
		t.Fatalf("ClearDue must remove the mark, got card %d", n)
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
	a, err := s.Claim("s1", "worker-A", "w_a", base, 1000)
	if err != nil || !a.Claimed {
		t.Fatalf("claim A = %+v err=%v", a, err)
	}
	// Worker B claims after A's lease deadline has passed: takeover rotates.
	after := base.Add(2 * time.Second)
	b, err := s.Claim("s1", "worker-B", "w_b", after, 1000)
	if err != nil || !b.Claimed {
		t.Fatalf("claim B = %+v err=%v", b, err)
	}
	if b.Generation == a.Generation || b.WakeID == a.WakeID {
		t.Fatalf("expired-lease takeover must rotate the fence: A=(gen %d,%s) B=(gen %d,%s)",
			a.Generation, a.WakeID, b.Generation, b.WakeID)
	}
	// The deposed worker A can no longer ack — its old generation/wake is fenced.
	if st, _ := s.Ack("s1", a.Generation, a.WakeID, a.Generation, true, nil, after, 1000); st != "FENCED" {
		t.Fatalf("deposed worker A ack should FENCE, got %q", st)
	}
	// The current holder B acks successfully.
	if st, _ := s.Ack("s1", b.Generation, b.WakeID, b.Generation, true, nil, after, 1000); st != "OK" {
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

	a, _ := s.Claim("s1", "worker-A", "w_a", now, 1000)
	if !a.Claimed {
		t.Fatal("worker A should claim the idle subscription")
	}
	// B claims while A's lease is still live.
	b, _ := s.Claim("s1", "worker-B", "w_b", now, 1000)
	if !b.Busy || b.Holder != "worker-A" {
		t.Fatalf("unexpired lease should be BUSY held by worker-A, got %+v", b)
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
	if err := client.SRem(ctx, streamSubsKey(slotOf("s1"), "events/a"), "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if subs, _, _ := s.StreamSubscribers("events/a"); len(subs) != 0 {
		t.Fatalf("precondition: fan-out should be empty, got %v", subs)
	}
	if err := s.ReconcileIndexes(); err != nil {
		t.Fatal(err)
	}
	if subs, _, _ := s.StreamSubscribers("events/a"); len(subs) != 1 || subs[0] != "s1" {
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
	if subs, _, _ := s.StreamSubscribers("events/unrelated"); len(subs) != 0 {
		t.Fatalf("repair must not invent membership for an unlinked stream, got %v", subs)
	}
	if subs, _, _ := s.StreamSubscribers("events/a"); len(subs) != 1 {
		t.Fatalf("the linked stream should be indexed, got %v", subs)
	}
}

// TestLazyMigrationServesFromNewTag is the Move-1 migration contract (05 §Migration
// step 3): a subscription seeded under the LEGACY {__ds} keyspace (simulating
// pre-slot-homing data) is lazily migrated to its slot-homed {__ds:h} keyspace on
// first access, served from the new tag, and the legacy copy is flipped away — its
// schedule entries, id-set membership, and fan-out re-homed under the new tag.
func TestLazyMigrationServesFromNewTag(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const id = "legacy-sub-007"
	h := slotOf(id)
	now := time.Now()
	leaseScore := float64(now.Add(time.Second).UnixNano())
	dueScore := float64(now.UnixNano())

	// Seed the sub ONLY under the legacy {__ds} tag — a waking webhook sub with a
	// link, a lease-schedule entry, a due-mark, and a legacy fan-out entry.
	if err := client.HSet(ctx, subKeyLegacy(id), map[string]any{
		"id": id, "type": string(DispatchWebhook), "pattern": "events/*",
		"webhook_url": "https://w.example/h", "lease_ttl_ms": "1000",
		"status": "active", "phase": "waking", "generation": "1", "wake_id": "w_a",
		"holder": "0", "lease_until_ns": "0", "created_ns": nsArg(now),
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, linksKeyLegacy(id), "events/a", "glob:0000000000000000_0000000000000000").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, subsKeyLegacy, id).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, leaseZKeyLegacy, goredis.Z{Score: leaseScore, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, dueZKeyLegacy, goredis.Z{Score: dueScore, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, streamSubsKeyLegacy("events/a"), id).Err(); err != nil {
		t.Fatal(err)
	}

	// Precondition: nothing under the slot-homed tag yet.
	if n, _ := client.Exists(ctx, subKey(id)).Result(); n != 0 {
		t.Fatalf("precondition: slot-homed sub must not exist before migration")
	}

	// First access migrates and serves from the new tag.
	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("Get after seeding legacy = ok %v err %v, want a migrated hit", ok, err)
	}
	if sub.Config.WebhookURL != "https://w.example/h" || sub.Phase != PhaseWaking || sub.Generation != 1 {
		t.Fatalf("migrated sub hydrated wrong: %+v", sub)
	}
	if len(sub.Links) != 1 || sub.Links[0].Path != "events/a" {
		t.Fatalf("migrated links lost: %+v", sub.Links)
	}

	// Flipped: the slot-homed copy exists, the legacy copy is gone.
	if n, _ := client.Exists(ctx, subKey(id)).Result(); n != 1 {
		t.Fatalf("migrated sub must exist under the slot-homed tag")
	}
	if n, _ := client.Exists(ctx, subKeyLegacy(id)).Result(); n != 0 {
		t.Fatalf("legacy sub copy must be flipped away after migration")
	}

	// Schedule entries re-homed to slot h, legacy entries dropped.
	if _, err := client.ZScore(ctx, leaseZKey(h), id).Result(); err != nil {
		t.Fatalf("lease schedule entry must be re-homed to slot %d: %v", h, err)
	}
	if _, err := client.ZScore(ctx, dueZKey(h), id).Result(); err != nil {
		t.Fatalf("due mark must be re-homed to slot %d: %v", h, err)
	}
	if _, err := client.ZScore(ctx, leaseZKeyLegacy, id).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("legacy lease entry must be dropped, got %v", err)
	}

	// Found by List() (id-set re-homed) and reachable by the slot-homed fan-out.
	ids, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if !contains(ids, id) {
		t.Fatalf("List() must return the migrated sub, got %v", ids)
	}
	subs, _, err := s.StreamSubscribers("events/a")
	if err != nil || len(subs) != 1 || subs[0] != id {
		t.Fatalf("StreamSubscribers after migration = %v err %v, want [%s]", subs, err, id)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
