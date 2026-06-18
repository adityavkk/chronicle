package webhook

import (
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"sync"
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
	client := newTestRedisClient(t, true)
	return NewRedisStore(client), client
}

func newTestRedisClient(t *testing.T, flush bool) *goredis.Client {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable (%s): %v", url, err)
	}
	if flush {
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatalf("flushdb: %v", err)
		}
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitForTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForTestErr(t *testing.T, ch <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

type pauseAfterLegacyReadHook struct {
	key     string
	paused  chan struct{}
	resume  chan struct{}
	pauseMu sync.Once
}

func (h *pauseAfterLegacyReadHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (h *pauseAfterLegacyReadHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil || cmd.Name() != "hgetall" || redisCommandKey(cmd) != h.key {
			return err
		}
		h.pauseMu.Do(func() {
			close(h.paused)
			select {
			case <-h.resume:
			case <-ctx.Done():
			}
		})
		return err
	}
}

func (h *pauseAfterLegacyReadHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func redisCommandKey(cmd goredis.Cmder) string {
	args := cmd.Args()
	if len(args) < 2 {
		return ""
	}
	key, _ := args[1].(string)
	return key
}

func TestConcurrentLegacyMigrationCannotReplayStaleHash(t *testing.T) {
	s, _ := newTestStore(t)
	const (
		id    = "legacy-race"
		path  = "events/legacy-race"
		begin = "0000000000000000_0000000000000000"
	)
	now := time.Now()
	createLegacySubForTest(t, s, id, pullWakeCfg(), []StreamLink{{Path: path, LinkType: LinkGlob, AckedOffset: begin}}, now)

	pausingClient := newTestRedisClient(t, false)
	hook := &pauseAfterLegacyReadHook{
		key:    legacySubKey(id),
		paused: make(chan struct{}),
		resume: make(chan struct{}),
	}
	var resumeStaleMigrator sync.Once
	defer resumeStaleMigrator.Do(func() { close(hook.resume) })
	pausingClient.AddHook(hook)
	pausingStore := NewRedisStore(pausingClient)

	staleMigratorDone := make(chan error, 1)
	go func() {
		_, ok, err := pausingStore.Get(id)
		if err == nil && !ok {
			err = errors.New("stale migrator Get returned no subscription")
		}
		staleMigratorDone <- err
	}()
	waitForTestSignal(t, hook.paused, "stale migrator to read legacy hash")

	if _, ok, err := s.Get(id); err != nil || !ok {
		t.Fatalf("winning migration = ok %v err %v", ok, err)
	}
	armed, err := s.ArmWake(id, now.Add(time.Millisecond), 1000, false, "w_after_migration", NoOwnerFence())
	if err != nil {
		t.Fatalf("ArmWake after winning migration: %v", err)
	}
	if !armed.Armed {
		t.Fatalf("ArmWake after winning migration = %+v, want ARMED", armed)
	}
	sentAt := now.Add(2 * time.Millisecond)
	if err := s.RecordWakeEventSent(id, armed.Generation, armed.WakeID, sentAt); err != nil {
		t.Fatal(err)
	}

	resumeStaleMigrator.Do(func() { close(hook.resume) })
	if err := waitForTestErr(t, staleMigratorDone, "stale migrator to finish"); err != nil {
		t.Fatal(err)
	}
	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("Get after stale migration = ok %v err %v", ok, err)
	}
	if sub.Generation != armed.Generation {
		t.Fatalf("stale migration replayed generation=%d, want %d", sub.Generation, armed.Generation)
	}
	if sub.Phase != PhaseWaking {
		t.Fatalf("stale migration replayed phase=%q, want %q", sub.Phase, PhaseWaking)
	}
	if sub.WakeID != armed.WakeID {
		t.Fatalf("stale migration replayed wake_id=%q, want %q", sub.WakeID, armed.WakeID)
	}
	if sub.WakeEventSentNs != sentAt.UnixNano() {
		t.Fatalf("stale migration replayed wake_event_sent_ns=%d, want %d", sub.WakeEventSentNs, sentAt.UnixNano())
	}
}

func webhookCfg(url string) Config {
	return Config{Type: DispatchWebhook, Pattern: "events/*", WebhookURL: url, LeaseTTLMs: 1000}
}

func testLeaseZKey(id string) string {
	return leaseZKey(subscriptionSlot(id))
}

func testRetryZKey(id string) string {
	return retryZKey(subscriptionSlot(id))
}

func testDueSetKey(id string) string {
	return dueSetKey(subscriptionSlot(id))
}

func dueMembers(t *testing.T, client goredis.UniversalClient, id string) []string {
	t.Helper()
	members, err := client.ZRange(context.Background(), testDueSetKey(id), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return members
}

func dueScore(t *testing.T, client goredis.UniversalClient, id string) float64 {
	t.Helper()
	score, err := client.ZScore(context.Background(), testDueSetKey(id), id).Result()
	if err != nil {
		t.Fatal(err)
	}
	return score
}

func claimOwnershipForTest(t *testing.T, s *RedisStore, slot OwnershipSlot, replica ReplicaID, now time.Time) SlotLease {
	t.Helper()
	res, err := s.ClaimSlot(slot, replica, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Granted() {
		t.Fatalf("claim slot %d by %s = %+v, want grant", slot.Int(), replica, res)
	}
	return res.Lease
}

func TestSubscriptionFromHashParsesScientificLeaseDeadline(t *testing.T) {
	sub := subscriptionFromHash("s1", map[string]string{
		"created_ns":     "1781800000000000000",
		"type":           string(DispatchPullWake),
		"pattern":        "events/*",
		"lease_ttl_ms":   "1000",
		"status":         string(StatusActive),
		"phase":          string(PhaseLive),
		"generation":     "1",
		"wake_id":        "w_a",
		"holder":         "1",
		"holder_worker":  "worker-A",
		"lease_until_ns": "1.7818e+18",
	}, nil)
	if sub.LeaseUntilNs == 0 {
		t.Fatal("scientific-notation lease_until_ns should parse for upgrade compatibility")
	}
}

func TestSubscriptionFromHashHydratesNonDefaultClaimShardLease(t *testing.T) {
	shard, _ := NewClaimShard(9)
	sub := subscriptionFromHash("s1", map[string]string{
		"created_ns":                             "1781800000000000000",
		"type":                                   string(DispatchPullWake),
		"pattern":                                "events/*",
		"lease_ttl_ms":                           "1000",
		"status":                                 string(StatusActive),
		"phase":                                  string(PhaseIdle),
		"lease_until_ns":                         "0",
		claimShardField("phase", shard):          string(PhaseLive),
		claimShardField("generation", shard):     "3",
		claimShardField("wake_id", shard):        "w_shard",
		claimShardField("holder", shard):         "1",
		claimShardField("holder_worker", shard):  "worker-shard",
		claimShardField("lease_until_ns", shard): "1781800000000000000",
	}, nil)
	if len(sub.ClaimLeases) != 1 {
		t.Fatalf("hydrated claim leases = %d, want 1", len(sub.ClaimLeases))
	}
	got := sub.ClaimLeases[0]
	if got.Shard != shard || got.State.Phase != PhaseLive || got.State.LeaseUntilNs == 0 {
		t.Fatalf("hydrated shard lease = %+v", got)
	}
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

func TestClaimShardScriptGoldenSemantics(t *testing.T) {
	s, _ := newTestStore(t)
	slot, _ := NewOwnershipSlot(0)
	base := time.Now()

	first, err := s.ClaimSlot(slot, ReplicaID("replica-a"), base, time.Second)
	if err != nil || first.Status != SlotClaimed || first.Lease.Epoch != 1 {
		t.Fatalf("first claim = %+v err=%v, want CLAIMED epoch 1", first, err)
	}
	renewed, err := s.ClaimSlot(slot, ReplicaID("replica-a"), base.Add(100*time.Millisecond), time.Second)
	if err != nil || renewed.Status != SlotRenewed || renewed.Lease.Epoch != first.Lease.Epoch {
		t.Fatalf("renew = %+v err=%v, want RENEWED epoch unchanged", renewed, err)
	}
	busy, err := s.ClaimSlot(slot, ReplicaID("replica-b"), base.Add(200*time.Millisecond), time.Second)
	if err != nil || busy.Status != SlotBusy || busy.Lease.Owner != ReplicaID("replica-a") || busy.Lease.Epoch != first.Lease.Epoch {
		t.Fatalf("foreign live claim = %+v err=%v, want BUSY owner replica-a epoch 1", busy, err)
	}
	transferred, err := s.ClaimSlot(slot, ReplicaID("replica-b"), base.Add(2*time.Second), time.Second)
	if err != nil || transferred.Status != SlotClaimed || transferred.Lease.Epoch != first.Lease.Epoch+1 {
		t.Fatalf("expired transfer = %+v err=%v, want CLAIMED epoch bump", transferred, err)
	}
	resumed, err := s.CheckOwner(first.Lease.Fence())
	if err != nil || resumed != CheckOwnerFenced {
		t.Fatalf("deposed resumed owner check = %q err=%v, want FENCED", resumed, err)
	}
}

func TestCheckOwnerScriptGoldenSemantics(t *testing.T) {
	s, _ := newTestStore(t)
	slot, _ := NewOwnershipSlot(0)
	status, err := s.CheckOwner(OwnershipFence{Enabled: true, Slot: slot, Owner: ReplicaID("missing"), Epoch: 1})
	if err != nil || status != CheckOwnerUnowned {
		t.Fatalf("missing owner = %q err=%v, want UNOWNED", status, err)
	}

	lease := claimOwnershipForTest(t, s, slot, ReplicaID("replica-a"), time.Now())
	status, err = s.CheckOwner(lease.Fence())
	if err != nil || status != CheckOwnerOwner {
		t.Fatalf("current owner = %q err=%v, want OWNER", status, err)
	}
	status, err = s.CheckOwner(OwnershipFence{Enabled: true, Slot: slot, Owner: ReplicaID("replica-b"), Epoch: lease.Epoch})
	if err != nil || status != CheckOwnerFenced {
		t.Fatalf("wrong owner = %q err=%v, want FENCED", status, err)
	}
	status, err = s.CheckOwner(OwnershipFence{Enabled: true, Slot: slot, Owner: lease.Owner, Epoch: lease.Epoch + 1})
	if err != nil || status != CheckOwnerFenced {
		t.Fatalf("wrong epoch = %q err=%v, want FENCED", status, err)
	}
}

func TestMembershipHeartbeatRemovesExpiredMembers(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	if err := client.ZAdd(
		context.Background(), membersKey,
		goredis.Z{Score: float64(now.Add(-time.Second).UnixNano()), Member: "replica-dead"},
		goredis.Z{Score: float64(now.Add(time.Second).UnixNano()), Member: "replica-live"},
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.HeartbeatMember(ReplicaID("replica-self"), now, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	members, err := s.LiveMembers(now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[ReplicaID]bool{}
	for _, member := range members {
		got[member] = true
	}
	if got[ReplicaID("replica-dead")] || !got[ReplicaID("replica-live")] || !got[ReplicaID("replica-self")] {
		t.Fatalf("live members after heartbeat = %v", members)
	}
}

func TestOwnerEpochFencesScheduleMutatingScriptsInline(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	staleFenceForID := func(id string) OwnershipFence {
		slot := subscriptionOwnershipSlot(id)
		stale := claimOwnershipForTest(t, s, slot, ReplicaID("replica-a"), now)
		current := claimOwnershipForTest(t, s, slot, ReplicaID("replica-b"), now.Add(2*time.Second))
		if current.Epoch <= stale.Epoch {
			t.Fatalf("precondition: transfer should bump epoch, stale=%d current=%d", stale.Epoch, current.Epoch)
		}
		return stale.Fence()
	}

	if _, err := s.CreateOrConfirm("arm-stale", webhookCfg("https://w.example/h"), nil, now); err != nil {
		t.Fatal(err)
	}
	staleFence := staleFenceForID("arm-stale")
	armed, err := s.ArmWake("arm-stale", now, 1000, true, "w_stale", staleFence)
	if err != nil || !armed.Fenced {
		t.Fatalf("stale ArmWake = %+v err=%v, want FENCED", armed, err)
	}
	if sub, _, _ := s.Get("arm-stale"); sub.Phase != PhaseIdle {
		t.Fatalf("fenced ArmWake must not mutate phase, got %q", sub.Phase)
	}

	if _, err := s.CreateOrConfirm("ack-stale", webhookCfg("https://w.example/h"), nil, now); err != nil {
		t.Fatal(err)
	}
	staleFence = staleFenceForID("ack-stale")
	goodArm, _ := s.ArmWake("ack-stale", now, 1000, true, "w_good", NoOwnerFence())
	if st, _ := s.Ack("ack-stale", ClaimModeLegacy, DefaultClaimShard, goodArm.Generation, goodArm.WakeID, goodArm.Generation, true, nil, now, 1000, staleFence); st != "FENCED" {
		t.Fatalf("stale Ack = %q, want FENCED", st)
	}
	if sub, _, _ := s.Get("ack-stale"); sub.Phase != PhaseWaking {
		t.Fatalf("fenced Ack must not release, got %q", sub.Phase)
	}

	if st, _ := s.ExpireLease(NewLeaseRef("ack-stale", DefaultClaimShard), now.Add(2*time.Second), true, staleFence); st != "FENCED" {
		t.Fatalf("stale ExpireLease = %q, want FENCED", st)
	}
	if _, err := s.ScheduleRetry("ack-stale", now, now.Add(time.Second), staleFence); !errors.Is(err, errOwnerFenced) {
		t.Fatalf("stale ScheduleRetry err=%v, want errOwnerFenced", err)
	}
	if retry, _ := client.ZCard(context.Background(), testRetryZKey("ack-stale")).Result(); retry != 0 {
		t.Fatalf("fenced ScheduleRetry must not add retry member, got %d", retry)
	}
	if st, _ := s.Release("ack-stale", ClaimModeLegacy, DefaultClaimShard, goodArm.Generation, goodArm.WakeID, goodArm.Generation, staleFence); st != "FENCED" {
		t.Fatalf("stale Release = %q, want FENCED", st)
	}
	if _, err := s.ReconcileLeaseSchedule(NewLeaseRef("ack-stale", DefaultClaimShard), now, true, staleFence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DueWakes(subscriptionOwnershipSlot("ack-stale"), now.Add(2*time.Second), 1, time.Second, staleFence); !errors.Is(err, errOwnerFenced) {
		t.Fatalf("stale DueWakes err=%v, want errOwnerFenced", err)
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

	res, err := s.ArmWake("s1", now, 1000, true, "w_first", NoOwnerFence())
	if err != nil || !res.Armed || res.Generation != 1 || res.WakeID != "w_first" {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 1 || members[0] != "s1" {
		t.Fatalf("arm should add one due-set member, got %v", members)
	}
	firstDueScore := dueScore(t, client, "s1")
	// Coalesce: a second arm while in flight is BUSY, not a new generation.
	if busy, _ := s.ArmWake("s1", now, 1000, true, "w_second", NoOwnerFence()); !busy.Busy || busy.Generation != 1 {
		t.Fatalf("second arm should be BUSY at gen 1, got %+v", busy)
	}
	if got := dueScore(t, client, "s1"); got != firstDueScore {
		t.Fatalf("BUSY arm must not update due-set score: got %v want %v", got, firstDueScore)
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
	if n, _ := client.ZCard(context.Background(), testLeaseZKey("s1")).Result(); n != 1 {
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
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation+9, c1.WakeID, c1.Generation+9, true, nil, now, 1000, NoOwnerFence()); st != "FENCED" {
		t.Fatalf("stale ack should FENCE, got %q", st)
	}
	// Correct ack advances the cursor forward-only and releases.
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000050"}}
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000, NoOwnerFence()); st != "OK" {
		t.Fatalf("valid ack = %q, want OK", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Links[0].AckedOffset != "0000000000000001_0000000000000050" {
		t.Fatalf("ack(done) must release and advance cursor: %+v", sub)
	}
	// A replayed ack on the now-cleared wake is fenced (cursor not advanced twice).
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000, NoOwnerFence()); st != "FENCED" {
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
	if st, _ := s.Ack("legacy-first", ClaimModeSharded, DefaultClaimShard, legacy.Generation, legacy.WakeID, legacy.Generation, true, nil, now, 1000, NoOwnerFence()); st != "FENCED" {
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
	if st, _ := s.Ack("sharded-first", ClaimModeLegacy, shard, first.Generation, first.WakeID, first.Generation, true, nil, now, 1000, NoOwnerFence()); st != "FENCED" {
		t.Fatalf("legacy ack against sharded mode should FENCE, got %q", st)
	}
}

func TestStoreAckDoneClearsDueSetHeartbeatDoesNot(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	armed, err := s.ArmWake("s1", now, 1000, true, "w_a", NoOwnerFence())
	if err != nil || !armed.Armed {
		t.Fatalf("arm = %+v err=%v", armed, err)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 1 {
		t.Fatalf("precondition: due set should contain s1, got %v", members)
	}
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, armed.Generation, armed.WakeID, armed.Generation, false, nil, now, 1000, NoOwnerFence()); st != "OK" {
		t.Fatalf("heartbeat ack = %q, want OK", st)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 1 || members[0] != "s1" {
		t.Fatalf("heartbeat ack must leave due member in place, got %v", members)
	}
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, armed.Generation, armed.WakeID, armed.Generation, true, nil, now, 1000, NoOwnerFence()); st != "OK" {
		t.Fatalf("done ack = %q, want OK", st)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 0 {
		t.Fatalf("done ack must clear due member, got %v", members)
	}
}

func TestStoreLeaseExpiryAndDueReScore(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a", NoOwnerFence())

	// Before the deadline: not expired.
	if st, _ := s.ExpireLease(NewLeaseRef("s1", DefaultClaimShard), base, true, NoOwnerFence()); st != "ACTIVE" {
		t.Fatalf("lease not yet due should be ACTIVE, got %q", st)
	}
	// After the deadline: expired, back to idle.
	after := base.Add(2 * time.Second)
	if st, _ := s.ExpireLease(NewLeaseRef("s1", DefaultClaimShard), after, true, NoOwnerFence()); st != "EXPIRED" {
		t.Fatalf("expired lease should be EXPIRED, got %q", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Holder {
		t.Fatalf("expiry must clear holder and idle the subscription: %+v", sub)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 1 || members[0] != "s1" {
		t.Fatalf("pending expiry should re-owe s1 in due set, got %v", members)
	}

	// Due claim re-scores forward, it does not remove (research 07 §6.1): a
	// crashed worker's item must recur.
	_, _ = s.ArmWake("s1", after, 1000, true, "w_b", NoOwnerFence())
	due, err := s.DueLeases(subscriptionOwnershipSlot("s1"), after.Add(2*time.Second), 16, 30*time.Second, NoOwnerFence())
	if err != nil || len(due) != 1 || due[0] != NewLeaseRef("s1", DefaultClaimShard) {
		t.Fatalf("due = %v err=%v, want [s1]", due, err)
	}
	if n, _ := client.ZCard(context.Background(), testLeaseZKey("s1")).Result(); n != 1 {
		t.Fatalf("claimed member must remain in the ZSET (re-scored, not removed), got %d", n)
	}
	// Immediately re-claiming finds nothing — it was re-scored into the future.
	if again, _ := s.DueLeases(subscriptionOwnershipSlot("s1"), after.Add(2*time.Second), 16, 30*time.Second, NoOwnerFence()); len(again) != 0 {
		t.Fatalf("re-scored member must not be due again immediately, got %v", again)
	}
}

func TestStoreExpireLeaseClearsDueSetWhenNoPending(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a", NoOwnerFence())
	if members := dueMembers(t, client, "s1"); len(members) != 1 {
		t.Fatalf("precondition: arm should add due member, got %v", members)
	}
	if st, _ := s.ExpireLease(NewLeaseRef("s1", DefaultClaimShard), base.Add(2*time.Second), false, NoOwnerFence()); st != "EXPIRED" {
		t.Fatalf("expired lease should be EXPIRED, got %q", st)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 0 {
		t.Fatalf("non-pending expiry should clear stale due member, got %v", members)
	}
}

func TestStoreReleaseClearsDueSet(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	armed, _ := s.ArmWake("s1", now, 1000, true, "w_a", NoOwnerFence())
	if members := dueMembers(t, client, "s1"); len(members) != 1 {
		t.Fatalf("precondition: arm should add due member, got %v", members)
	}
	if st, _ := s.Release("s1", ClaimModeLegacy, DefaultClaimShard, armed.Generation, armed.WakeID, armed.Generation, NoOwnerFence()); st != "OK" {
		t.Fatalf("release = %q, want OK", st)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 0 {
		t.Fatalf("release must clear due member, got %v", members)
	}
}

func TestStoreStaleAckAfterReleaseDoesNotClearNewDue(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	first, _ := s.ArmWake("s1", now, 1000, true, "w_a", NoOwnerFence())
	if st, _ := s.Release("s1", ClaimModeLegacy, DefaultClaimShard, first.Generation, first.WakeID, first.Generation, NoOwnerFence()); st != "OK" {
		t.Fatalf("release first wake = %q, want OK", st)
	}
	second, _ := s.ArmWake("s1", now.Add(time.Millisecond), 1000, true, "w_b", NoOwnerFence())
	if !second.Armed || second.Generation == first.Generation {
		t.Fatalf("second arm should mint a newer generation, got %+v after %+v", second, first)
	}
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000050"}}
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, first.Generation, first.WakeID, first.Generation, true, acks, now, 1000, NoOwnerFence()); st != "FENCED" {
		t.Fatalf("stale ack after release/re-arm should FENCE, got %q", st)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 1 || members[0] != "s1" {
		t.Fatalf("stale ack must not clear newer due member, got %v", members)
	}
	sub, _, _ := s.Get("s1")
	if sub.Links[0].AckedOffset != "0000000000000000_0000000000000000" {
		t.Fatalf("stale ack must not advance cursor, got %+v", sub.Links)
	}
}

func TestStoreDeleteClearsDueSet(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_, _ = s.ArmWake("s1", now, 1000, true, "w_a", NoOwnerFence())
	if members := dueMembers(t, client, "s1"); len(members) != 1 {
		t.Fatalf("precondition: arm should add due member, got %v", members)
	}
	if err := s.Delete("s1"); err != nil {
		t.Fatal(err)
	}
	if members := dueMembers(t, client, "s1"); len(members) != 0 {
		t.Fatalf("delete must clear due member, got %v", members)
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
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, a.Generation, a.WakeID, a.Generation, true, nil, after, 1000, NoOwnerFence()); st != "FENCED" {
		t.Fatalf("deposed worker A ack should FENCE, got %q", st)
	}
	// The current holder B acks successfully.
	if st, _ := s.Ack("s1", ClaimModeLegacy, DefaultClaimShard, b.Generation, b.WakeID, b.Generation, true, nil, after, 1000, NoOwnerFence()); st != "OK" {
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
	if st, _ := s.Ack("s1", ClaimModeSharded, shardB, a.Generation, a.WakeID, a.Generation, true, nil, now, 1000, NoOwnerFence()); st != "FENCED" {
		t.Fatalf("shard A fence must not ack shard B, got %q", st)
	}
	if st, _ := s.Ack("s1", ClaimModeSharded, shardA, a.Generation, a.WakeID, a.Generation, true, nil, now, 1000, NoOwnerFence()); st != "OK" {
		t.Fatalf("current shard A holder should ack, got %q", st)
	}
	if st, _ := s.Ack("s1", ClaimModeSharded, shardB, b.Generation, b.WakeID, b.Generation, true, nil, now, 1000, NoOwnerFence()); st != "OK" {
		t.Fatalf("current shard B holder should ack, got %q", st)
	}
	if n, _ := client.ZCard(context.Background(), testLeaseZKey("s1")).Result(); n != 0 {
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
	if err := client.SRem(ctx, streamSubsKey(subscriptionSlot("s1"), "events/a"), "s1").Err(); err != nil {
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

func TestOccupiedSlotsBitmapSetAndNeverCleared(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const (
		id   = "s1"
		path = "events/occupied"
	)
	if _, err := s.CreateOrConfirm(id, webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Link(id, path, LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	h := subscriptionSlot(id)
	if bit, err := client.GetBit(ctx, occupiedStreamSlotsKey(path), int64(h)).Result(); err != nil || bit != 1 {
		t.Fatalf("occupied bit h=%d = %d err=%v, want 1", h, bit, err)
	}
	if err := s.Unlink(id, path, false); err != nil {
		t.Fatal(err)
	}
	if bit, err := client.GetBit(ctx, occupiedStreamSlotsKey(path), int64(h)).Result(); err != nil || bit != 1 {
		t.Fatalf("deindex must not clear occupied bit h=%d, got %d err=%v", h, bit, err)
	}
	subs, slots, err := s.StreamSubscribers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 0 || slots != 1 {
		t.Fatalf("stale occupied bit should probe one empty slot, got subs=%v slots=%d", subs, slots)
	}
}

func TestHighSlotSubscriptionFoundByScatterGatherPaths(t *testing.T) {
	store, client := newTestStore(t)
	ctx := context.Background()
	const (
		id    = "sub-0" // FNV-1a % 256 == 200.
		path  = "events/high-slot"
		begin = "0000000000000000_0000000000000000"
		tail  = "0000000000000001_0000000000000000"
	)
	if got := subscriptionSlot(id); got != 200 {
		t.Fatalf("test fixture %q homed to slot %d, want 200", id, got)
	}
	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Link(id, path, LinkGlob, begin); err != nil {
		t.Fatal(err)
	}
	ids, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, id) {
		t.Fatalf("List missed high-slot sub %q in %v", id, ids)
	}
	subs, err := store.GetMany([]string{id})
	if err != nil || len(subs) != 1 || subs[0].ID != id {
		t.Fatalf("GetMany high-slot = %+v err=%v", subs, err)
	}
	if err := client.SRem(ctx, streamSubsKey(subscriptionSlot(id), path), id).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileIndexes(); err != nil {
		t.Fatal(err)
	}
	indexed, slots, err := store.StreamSubscribers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 1 || indexed[0] != id || slots != 1 {
		t.Fatalf("reconciled high-slot fan-out = %v slots=%d", indexed, slots)
	}

	fs := &fakeStreams{tails: map[string]string{path: tail}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatal(err)
	}
	mgr.OnStreamAppend(path)
	if fs.count() != 1 {
		t.Fatalf("OnStreamAppend should reach high-slot subscriber, wake events=%d", fs.count())
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.fanOuts != 1 || fm.fanSlots != 1 || fm.fanSubs != 1 {
		t.Fatalf("FanOut metrics = out=%d slots=%d subs=%d, want 1/1/1", fm.fanOuts, fm.fanSlots, fm.fanSubs)
	}
}

func TestLegacySubscriptionMigratesLazilyToSlotHome(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const (
		id    = "legacy-sub"
		path  = "events/legacy"
		begin = "0000000000000000_0000000000000000"
	)
	now := time.Now()
	createLegacySubForTest(t, s, id, pullWakeCfg(), []StreamLink{{Path: path, LinkType: LinkGlob, AckedOffset: begin}}, now)
	if err := client.ZAdd(ctx, legacyDueSetKey(), goredis.Z{Score: float64(now.UnixNano()), Member: id}).Err(); err != nil {
		t.Fatal(err)
	}

	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("Get legacy after lazy migration = ok %v err %v", ok, err)
	}
	if sub.ID != id || len(sub.Links) != 1 || sub.Links[0].Path != path {
		t.Fatalf("migrated sub = %+v", sub)
	}
	h := subscriptionSlot(id)
	if exists, _ := client.Exists(ctx, legacySubKey(id), legacyLinksKey(id)).Result(); exists != 0 {
		t.Fatalf("legacy keys should be removed after migration, exists=%d", exists)
	}
	if exists, _ := client.Exists(ctx, subKey(id), linksKey(id)).Result(); exists != 2 {
		t.Fatalf("new slot keys should exist after migration, exists=%d", exists)
	}
	if _, err := client.ZScore(ctx, dueSetKey(h), id).Result(); err != nil {
		t.Fatalf("due member was not migrated to slot %d: %v", h, err)
	}
	if err := validateSingleHashTag([]string{subKey(id), linksKey(id), subsKey(h), leaseZKey(h), retryZKey(h), dueSetKey(h), ownershipSlotKey(subscriptionOwnershipSlot(id))}); err != nil {
		t.Fatalf("migrated key set split across slots: %v", err)
	}
	indexed, slots, err := s.StreamSubscribers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 1 || indexed[0] != id || slots != 1 {
		t.Fatalf("migrated stream index = %v slots=%d", indexed, slots)
	}
}

func TestCompletedMigrationCleansLegacyResidueWithoutOverwritingNewState(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const (
		id    = "legacy-residue"
		path  = "events/legacy-residue"
		begin = "0000000000000000_0000000000000000"
	)
	now := time.Now()
	createLegacySubForTest(t, s, id, pullWakeCfg(), []StreamLink{{Path: path, LinkType: LinkGlob, AckedOffset: begin}}, now)
	if _, ok, err := s.Get(id); err != nil || !ok {
		t.Fatalf("initial migration = ok %v err %v", ok, err)
	}
	if err := client.HSet(ctx, subKey(id), "generation", "7").Err(); err != nil {
		t.Fatal(err)
	}
	createLegacySubForTest(t, s, id, pullWakeCfg(), []StreamLink{{Path: path, LinkType: LinkGlob, AckedOffset: begin}}, now)

	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("residue cleanup migration = ok %v err %v", ok, err)
	}
	if sub.Generation != 7 {
		t.Fatalf("completed migration overwrote new state from stale legacy hash: generation=%d", sub.Generation)
	}
	if exists, _ := client.Exists(ctx, legacySubKey(id), legacyLinksKey(id)).Result(); exists != 0 {
		t.Fatalf("legacy residue should be removed, exists=%d", exists)
	}
	if member, err := client.SIsMember(ctx, legacyStreamSubsKey(path), id).Result(); err != nil || member {
		t.Fatalf("legacy stream index residue member=%v err=%v, want false", member, err)
	}
}

func createLegacySubForTest(t *testing.T, s *RedisStore, id string, cfg Config, links []StreamLink, now time.Time) {
	t.Helper()
	cfg = NormalizeConfig(cfg)
	args := make([]any, 0, 10+3*len(links))
	args = append(
		args,
		id, ConfigHash(cfg), nsArg(now),
		string(cfg.Type), cfg.Pattern, cfg.WebhookURL, cfg.WakeStream,
		strconv.FormatInt(cfg.LeaseTTLMs, 10), cfg.Description,
		strconv.Itoa(len(links)),
	)
	for _, l := range links {
		args = append(args, l.Path, string(l.LinkType), l.AckedOffset)
	}
	if _, err := s.evalStrings(createSubScript, []string{legacySubKey(id), legacySubsKey(), legacyLinksKey(id)}, args...); err != nil {
		t.Fatal(err)
	}
	for _, l := range links {
		if err := s.client.SAdd(context.Background(), legacyStreamSubsKey(l.Path), id).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
