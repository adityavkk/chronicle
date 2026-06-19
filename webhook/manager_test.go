package webhook

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// fakeStreams is an in-memory Streams adapter for manager tests: it records the
// wake-stream appends so a test can assert a stranded pull-wake was re-emitted.
type fakeStreams struct {
	mu     sync.Mutex
	tails  map[string]string
	events []string // wake_stream paths appended to
}

func (f *fakeStreams) TailOffset(path string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.tails[path]
	return v, ok
}

func (f *fakeStreams) TailOffsets(paths []string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(paths))
	for _, p := range paths {
		if v, ok := f.tails[p]; ok {
			out[p] = v
		}
	}
	return out
}

func (f *fakeStreams) BeginningOffset() string { return "0000000000000000_0000000000000000" }

func (f *fakeStreams) AppendWakeEvent(wakeStream string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, wakeStream)
	return nil
}

func (f *fakeStreams) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func newTestManager(t *testing.T) (*Manager, *RedisStore, *fakeStreams) {
	t.Helper()
	store, _ := newTestStore(t) // skips when Redis is unavailable / -short
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr, store, fs
}

func TestDefaultSweepIntervalIsCoarseFloor(t *testing.T) {
	if defaultSweepInterval == 2*time.Second {
		t.Fatal("default sweep interval must not remain the old 2s recovery sweep")
	}
	if defaultSweepInterval < 5*time.Second || defaultSweepInterval > 5*time.Minute {
		t.Fatalf("default sweep interval %s outside seconds-to-minutes floor band", defaultSweepInterval)
	}
	if defaultSweepInterval != defaultReconcileInterval {
		t.Fatalf("default sweep interval = %s, want alignment with reconcile interval %s",
			defaultSweepInterval, defaultReconcileInterval)
	}
}

// TestRecordWakeEventSentFences is the slice-1 store contract: a matching
// (generation, wake) stamps the sent flag; a stale one is ignored.
func TestRecordWakeEventSentFences(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	res, err := s.ArmWake("s1", now, 1000, false, "w_a", NoOwnerFence(), ConsistencyTierA)
	if err != nil || !res.Armed {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	if sub, _, _ := s.Get("s1"); sub.WakeEventSentNs != 0 {
		t.Fatalf("a freshly armed pull-wake must be unsent, got %d", sub.WakeEventSentNs)
	}
	if err := s.RecordWakeEventSent("s1", res.Generation, res.WakeID, now); err != nil {
		t.Fatal(err)
	}
	sub, _, _ := s.Get("s1")
	if sub.WakeEventSentNs == 0 {
		t.Fatal("matching record should stamp wake_event_sent_ns")
	}
	stamp := sub.WakeEventSentNs
	// A stale record (superseded generation) must not move the flag.
	if err := s.RecordWakeEventSent("s1", res.Generation+99, "w_stale", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if sub, _, _ := s.Get("s1"); sub.WakeEventSentNs != stamp {
		t.Fatal("stale record (wrong generation) must not change wake_event_sent_ns")
	}
}

// TestSweepReemitsStrandedPullWake is the slice-1 recovery: a pull-wake armed but
// never emitted (the crash window) is re-emitted by the sweep, and a healthy
// re-sweep does not duplicate it.
func TestSweepReemitsStrandedPullWake(t *testing.T) {
	mgr, store, fs := newTestManager(t)
	now := time.Now()
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	// Simulate "arm, then crash before the wake-stream append": ArmWake sets
	// phase=waking and wake_event_sent_ns=0, but no event is written.
	res, err := store.ArmWake("s1", now, 1000, false, "w_a", NoOwnerFence(), ConsistencyTierA)
	if err != nil || !res.Armed {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	if sub, _, _ := store.Get("s1"); sub.Phase != PhaseWaking || sub.WakeEventSentNs != 0 {
		t.Fatalf("expected stranded waking/sent=0, got %+v", sub)
	}
	if fs.count() != 0 {
		t.Fatal("no wake event should exist before the sweep")
	}

	// The recovery sweep re-emits the stranded wake and records the emit.
	mgr.RunSweep()
	if fs.count() == 0 {
		t.Fatal("sweep should have re-emitted the stranded pull-wake event")
	}
	if sub, _, _ := store.Get("s1"); sub.WakeEventSentNs == 0 {
		t.Fatal("sweep should have recorded the emit")
	}

	// A second sweep must not re-emit — the event is now marked sent.
	before := fs.count()
	mgr.RunSweep()
	if fs.count() != before {
		t.Fatalf("second sweep must not re-emit, got %d -> %d", before, fs.count())
	}
}

// TestSweepBatchedWakesOnlyPendingSubs verifies the batched sweep (GetMany +
// TailOffsets + HasPendingWorkFrom) wakes exactly the idle subscriptions whose
// linked tail is beyond the cursor, and leaves not-pending and missing-stream
// subscriptions idle — the same decision the per-link sweep made.
func TestSweepBatchedWakesOnlyPendingSubs(t *testing.T) {
	mgr, store, fs := newTestManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	for _, id := range []string{"s1", "s2", "s3"} {
		if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	_ = store.Link("s1", "events/1", LinkGlob, begin)
	_ = store.Link("s2", "events/2", LinkGlob, begin)
	_ = store.Link("s3", "events/3", LinkGlob, begin)

	// s1 pending (tail > acked); s2 not pending (tail == acked); s3 stream missing.
	fs.mu.Lock()
	fs.tails["events/1"] = "0000000000000001_0000000000000000"
	fs.tails["events/2"] = begin
	fs.mu.Unlock()

	mgr.RunSweep()

	if got := fs.count(); got != 1 {
		t.Fatalf("expected exactly one wake event (s1), got %d", got)
	}
	if sub, _, _ := store.Get("s1"); sub.Phase != PhaseWaking {
		t.Fatalf("s1 (pending) should be waking after the sweep, got %q", sub.Phase)
	}
	for _, id := range []string{"s2", "s3"} {
		if sub, _, _ := store.Get(id); sub.Phase != PhaseIdle {
			t.Fatalf("%s (not pending) should stay idle, got %q", id, sub.Phase)
		}
	}
}

func TestExpiredNonzeroClaimShardRewakesPendingStreams(t *testing.T) {
	mgr, store, fs := newTestManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	path := "events/nonzero"
	for StreamClaimShard(path) == DefaultClaimShard {
		path += "x"
	}
	shard := StreamClaimShard(path)
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link("s1", path, LinkGlob, begin); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	fs.tails[path] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()

	claimed, err := store.Claim("s1", ClaimModeSharded, shard, "worker-shard", "w_shard", now, 1000, ConsistencyTierA)
	if err != nil || !claimed.Claimed {
		t.Fatalf("claim shard %d = %+v err=%v", shard.Int(), claimed, err)
	}
	if status, err := store.ExpireLease(NewLeaseRef("s1", shard), now.Add(2*time.Second), true, NoOwnerFence()); err != nil || status != "EXPIRED" {
		t.Fatalf("expire shard %d = %q err=%v", shard.Int(), status, err)
	}

	if !mgr.rewakeIfPending("s1") {
		t.Fatal("expired nonzero shard with pending work should re-wake")
	}
	if got := fs.count(); got != 1 {
		t.Fatalf("expected one re-wake event, got %d", got)
	}
	if sub, _, _ := store.Get("s1"); sub.Phase != PhaseWaking {
		t.Fatalf("re-wake should arm the default wake state, got %q", sub.Phase)
	}
}

// TestSweepWindowCapCoversAll verifies the optional per-tick cap rolls a cursor
// across ticks so every subscription is eventually covered, and that the default
// (no cap) returns every id.
func TestSweepWindowCapCoversAll(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	if got := (&Manager{}).sweepWindow(append([]string{}, ids...)); len(got) != len(ids) {
		t.Fatalf("sweepBatch=0 must return all %d ids, got %d", len(ids), len(got))
	}

	m := &Manager{sweepBatch: 3}
	seen := map[string]bool{}
	work := append([]string{}, ids...)
	for tick := 0; tick < 4; tick++ { // ceil(10/3) = 4 ticks cover all
		win := m.sweepWindow(work)
		if len(win) > 3 {
			t.Fatalf("tick %d window %v exceeds cap 3", tick, win)
		}
		for _, id := range win {
			seen[id] = true
		}
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("id %q never swept across the rolling window", id)
		}
	}
}

// fakeMetrics records what the Manager observes, so a test can assert the
// instrumentation seam is actually wired through the sweep.
type fakeMetrics struct {
	mu        sync.Mutex
	sweeps    int
	lastSubs  int
	lastTails int
	lastWakes int
	dueTicks  int
	dueFired  int
	dueOps    map[string]int
	slotOps   map[string]int
	fenced    map[string]int
	fanOuts   int
	fanSlots  int
	fanSubs   int
}

func (f *fakeMetrics) SweepTick(_ time.Duration, subs, tails, wakes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	f.lastSubs, f.lastTails, f.lastWakes = subs, tails, wakes
}
func (f *fakeMetrics) WakeDelivery(time.Duration, string) {}
func (f *fakeMetrics) WakeEvent(time.Duration, string)    {}
func (f *fakeMetrics) WorkerTick(string, int)             {}
func (f *fakeMetrics) FanOut(_ time.Duration, slotsProbed, subs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fanOuts++
	f.fanSlots = slotsProbed
	f.fanSubs = subs
}

func (f *fakeMetrics) DueSetMutation(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dueOps == nil {
		f.dueOps = map[string]int{}
	}
	f.dueOps[op]++
}

func (f *fakeMetrics) DueWorkerTick(_ time.Duration, fired int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueTicks++
	f.dueFired = fired
}

func (f *fakeMetrics) SlotOwnership(event string, _ int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.slotOps == nil {
		f.slotOps = map[string]int{}
	}
	f.slotOps[event]++
}
func (f *fakeMetrics) CoverageGap(time.Duration) {}
func (f *fakeMetrics) OwnerFenced(scope string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fenced == nil {
		f.fenced = map[string]int{}
	}
	f.fenced[scope]++
}
func (f *fakeMetrics) ClaimContention(string, string) {}

// TestSweepRecordsMetrics verifies the sweep reports its per-tick cost to the
// Metrics seam: one tick recorded, carrying the subscription/tail counts and the
// wakes it issued.
func TestSweepRecordsMetrics(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	_ = store.Link("s1", "events/1", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/1"] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()

	mgr.RunSweep()

	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.sweeps != 1 {
		t.Fatalf("expected exactly one sweep tick recorded, got %d", fm.sweeps)
	}
	if fm.lastSubs < 1 || fm.lastTails < 1 || fm.lastWakes < 1 {
		t.Fatalf("sweep metrics should reflect the pending sub: subs=%d tails=%d wakes=%d",
			fm.lastSubs, fm.lastTails, fm.lastWakes)
	}
}

func TestRecoveryScopesRunExactlyOneSweepPath(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link("s1", "events/1", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}

	for i, scope := range []recoveryScope{
		recoveryScopeBoot,
		recoveryScopeReconnect,
		recoveryScopeAppendError,
		recoveryScopeFloor,
	} {
		mgr.reconcile(scope)
		fm.mu.Lock()
		got := fm.sweeps
		fm.mu.Unlock()
		if got != i+1 {
			t.Fatalf("%s should run exactly one sweep path, got %d total sweeps after %d scopes",
				scope, got, i+1)
		}
	}

	mgr.reconcile(recoveryScopeEpochBump)
	mgr.reconcile(recoveryScopeNewOwnerCAS)
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.sweeps != 4 {
		t.Fatalf("ownership transfer scopes should run lease repair, not the full sweep; got %d sweeps", fm.sweeps)
	}
}

func TestSlotReconcileOwnsHRWTargetAndHeldLease(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL:         "http://x/v1/stream/",
		ReplicaID:             "replica-a",
		MemberLeaseTTL:        900 * time.Millisecond,
		HeartbeatInterval:     300 * time.Millisecond,
		SlotLeaseTTL:          900 * time.Millisecond,
		SlotReconcileInterval: 300 * time.Millisecond,
		Metrics:               fm,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := store.CreateOrConfirm("ownership-active-sub", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.HeartbeatMember(ReplicaID("replica-a"), now, time.Second); err != nil {
		t.Fatal(err)
	}
	mgr.slotReconcileOnce(now)
	owned := mgr.ownedSlots(now)
	wantSlot := subscriptionOwnershipSlot("ownership-active-sub")
	if len(owned) != 1 {
		t.Fatalf("owned slots = %d, want 1", len(owned))
	}
	if owned[0].Slot != wantSlot || owned[0].Owner != ReplicaID("replica-a") || owned[0].Epoch != 1 {
		t.Fatalf("owned lease = %+v, want slot %d replica-a epoch 1", owned[0], wantSlot.Int())
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.slotOps[string(SlotClaimed)] != 1 {
		t.Fatalf("slot ownership metrics = %v, want CLAIMED=1", fm.slotOps)
	}
}

func TestDeadMemberAgesOutAndSlotReclaimsAfterLeaseExpiry(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	slot := subscriptionOwnershipSlot("reconcile-on-claim")
	base := time.Now()
	_ = store.HeartbeatMember(ReplicaID("replica-a"), base, 500*time.Millisecond)
	_ = store.HeartbeatMember(ReplicaID("replica-b"), base, 500*time.Millisecond)
	owner, ok := HRWOwner([]ReplicaID{"replica-a", "replica-b"}, slot)
	if !ok {
		t.Fatal("expected HRW owner")
	}
	survivor := ReplicaID("replica-a")
	if owner == survivor {
		survivor = "replica-b"
	}
	dead := owner
	if _, err := store.ClaimSlot(slot, dead, base, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOrConfirm("reconcile-on-claim", pullWakeCfg(), nil, base); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim("reconcile-on-claim", ClaimModeLegacy, DefaultClaimShard, "worker", "w_reconcile", base, 1000, ConsistencyTierA)
	if err != nil || !claimed.Claimed {
		t.Fatalf("precondition claim = %+v err=%v", claimed, err)
	}
	if err := store.client.ZRem(context.Background(), testLeaseZKey("reconcile-on-claim"), "reconcile-on-claim").Err(); err != nil {
		t.Fatal(err)
	}

	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL:         "http://x/v1/stream/",
		ReplicaID:             survivor.String(),
		MemberLeaseTTL:        900 * time.Millisecond,
		HeartbeatInterval:     300 * time.Millisecond,
		SlotLeaseTTL:          900 * time.Millisecond,
		SlotReconcileInterval: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	after := base.Add(time.Second)
	if err := store.HeartbeatMember(survivor, after, time.Second); err != nil {
		t.Fatal(err)
	}
	mgr.slotReconcileOnce(after)
	owned := mgr.ownedSlots(after)
	var found bool
	for _, lease := range owned {
		if lease.Slot == slot {
			found = true
			if lease.Owner != survivor || lease.Epoch != 2 {
				t.Fatalf("reclaimed slot lease = %+v, want survivor %s epoch 2", lease, survivor)
			}
		}
	}
	if !found {
		t.Fatalf("reclaimed owned slots missing slot %d: %+v", slot.Int(), owned)
	}
	if _, err := store.client.ZScore(context.Background(), testLeaseZKey("reconcile-on-claim"), "reconcile-on-claim").Result(); err != nil {
		t.Fatalf("epoch-bump reconcile did not repair dropped lease schedule: %v", err)
	}
}

func TestManagerStartStopOwnershipLoopsDrain(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL:         "http://x/v1/stream/",
		ReplicaID:             "replica-start-stop",
		MemberLeaseTTL:        200 * time.Millisecond,
		HeartbeatInterval:     50 * time.Millisecond,
		SlotLeaseTTL:          200 * time.Millisecond,
		SlotReconcileInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.Start()
	time.Sleep(120 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not drain manager goroutines")
	}
}

func TestDeliverWebhookChecksOwnerBeforePost(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		ReplicaID:     "replica-a",
		Metrics:       fm,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	slot := subscriptionOwnershipSlot("webhook-stale")
	stale := claimOwnershipForTest(t, store, slot, ReplicaID("replica-a"), now)
	_ = claimOwnershipForTest(t, store, slot, ReplicaID("replica-b"), now.Add(2*time.Second))
	if _, err := store.CreateOrConfirm("webhook-stale", webhookCfg(server.URL), nil, now); err != nil {
		t.Fatal(err)
	}

	mgr.deliverWebhook("webhook-stale", 1, "w_stale", stale.Fence())

	if posts != 0 {
		t.Fatalf("stale owner must not POST webhook, got %d posts", posts)
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.fenced["deliver_webhook"] != 1 {
		t.Fatalf("owner fence metrics = %v, want deliver_webhook=1", fm.fenced)
	}
}

type streamSubscribersErrorStore struct{ *RedisStore }

func (s streamSubscribersErrorStore) StreamSubscribers(string) ([]string, int, error) {
	return nil, 0, errors.New("stream subscriber index unavailable")
}

func TestOnStreamAppendErrorTriggersOneReconcile(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(streamSubscribersErrorStore{store}, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		Metrics:       fm,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link("s1", "events/1", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}

	mgr.OnStreamAppend("events/1")

	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.sweeps != 1 {
		t.Fatalf("append-error trigger should run one reconcile sweep, got %d", fm.sweeps)
	}
}

func TestReconcileLeasesRestoresDroppedLeaseAndDueFromDurableState(t *testing.T) {
	store, client := newTestStore(t)
	now := time.Now()
	const (
		id    = "s1"
		path  = "events/lease-reconcile"
		begin = "0000000000000000_0000000000000000"
		tail  = "0000000000000001_0000000000000000"
	)
	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link(id, path, LinkGlob, begin); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(id, ClaimModeLegacy, DefaultClaimShard, "worker-A", "w_a", now, 1000, ConsistencyTierA)
	if err != nil || !claimed.Claimed {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}
	sub, _, _ := store.Get(id)
	if sub.Phase != PhaseLive || sub.LeaseUntilNs == 0 {
		t.Fatalf("precondition: expected live lease in durable hash, got %+v", sub)
	}
	if err := client.ZRem(context.Background(), testLeaseZKey(id), id).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZRem(context.Background(), testDueSetKey(id), id).Err(); err != nil {
		t.Fatal(err)
	}

	fs := &fakeStreams{tails: map[string]string{path: tail}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	mgr.RunRedisReconnect()

	leaseScore, err := client.ZScore(context.Background(), testLeaseZKey(id), id).Result()
	if err != nil {
		t.Fatalf("missing repaired lease schedule member: %v", err)
	}
	if math.Abs(leaseScore-float64(sub.LeaseUntilNs)) > float64(time.Millisecond) {
		t.Fatalf("lease schedule score = %.0f, want durable lease_until_ns %d", leaseScore, sub.LeaseUntilNs)
	}
	members := dueMembers(t, client, id)
	if len(members) != 1 || members[0] != id {
		t.Fatalf("pending durable cursor state should re-derive due entry, got %v", members)
	}
}

func TestRedisReconnectRepairsDroppedNonDefaultClaimShardLease(t *testing.T) {
	store, client := newTestStore(t)
	now := time.Now()
	const (
		id    = "s-sharded"
		begin = "0000000000000000_0000000000000000"
		tail  = "0000000000000001_0000000000000000"
	)
	path := "events/lease-reconcile-shard"
	for StreamClaimShard(path) == DefaultClaimShard {
		path += "x"
	}
	shard := StreamClaimShard(path)
	ref := NewLeaseRef(id, shard)

	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link(id, path, LinkGlob, begin); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(id, ClaimModeSharded, shard, "worker-shard", "w_sharded", now, 1000, ConsistencyTierA)
	if err != nil || !claimed.Claimed {
		t.Fatalf("claim shard %d = %+v err=%v", shard.Int(), claimed, err)
	}
	durablePhase, err := client.HGet(context.Background(), subKey(id), claimShardField("phase", shard)).Result()
	if err != nil || Phase(durablePhase) != PhaseLive {
		t.Fatalf("durable shard phase = %q err=%v, want live", durablePhase, err)
	}
	deadlineRaw, err := client.HGet(context.Background(), subKey(id), claimShardField("lease_until_ns", shard)).Result()
	if err != nil {
		t.Fatalf("missing durable shard lease deadline: %v", err)
	}
	deadline, err := strconv.ParseInt(deadlineRaw, 10, 64)
	if err != nil || deadline == 0 {
		t.Fatalf("durable shard lease deadline = %q err=%v", deadlineRaw, err)
	}
	if err := client.ZRem(context.Background(), testLeaseZKey(id), ref.Member()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZRem(context.Background(), testDueSetKey(id), id).Err(); err != nil {
		t.Fatal(err)
	}

	fs := &fakeStreams{tails: map[string]string{path: tail}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	mgr.RunRedisReconnect()

	leaseScore, err := client.ZScore(context.Background(), testLeaseZKey(id), ref.Member()).Result()
	if err != nil {
		t.Fatalf("missing repaired shard lease member %q: %v", ref.Member(), err)
	}
	if math.Abs(leaseScore-float64(deadline)) > float64(time.Millisecond) {
		t.Fatalf("shard lease schedule score = %.0f, want durable lease_until_ns %d", leaseScore, deadline)
	}
	members := dueMembers(t, client, id)
	if len(members) != 1 || members[0] != id {
		t.Fatalf("pending durable cursor state should re-derive due entry, got %v", members)
	}
	if fs.count() != 0 {
		t.Fatalf("live sharded lease should not trigger a duplicate legacy wake, got %d events", fs.count())
	}
	if sub, _, _ := store.Get(id); sub.Phase != PhaseIdle {
		t.Fatalf("live sharded lease should leave legacy shard idle, got %q", sub.Phase)
	}
	if _, err := client.ZScore(context.Background(), testLeaseZKey(id), id).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("reconnect should not create legacy shard lease member while non-default shard is live: %v", err)
	}
}

func TestDueWorkerFiresOwedIdleSubscription(t *testing.T) {
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	_ = store.Link("s1", "events/1", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/1"] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()
	if err := client.ZAdd(context.Background(), testDueSetKey("s1"), goredis.Z{
		Score:  float64(now.Add(-time.Second).UnixNano()),
		Member: "s1",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	if fired := mgr.RunDue(); fired != 1 {
		t.Fatalf("due worker fired %d wakes, want 1", fired)
	}
	if fs.count() != 1 {
		t.Fatalf("due worker should append one pull-wake event, got %d", fs.count())
	}
	if sub, _, _ := store.Get("s1"); sub.Phase != PhaseWaking {
		t.Fatalf("due worker should arm the owed subscription, got phase %q", sub.Phase)
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.dueTicks != 1 || fm.dueFired != 1 || fm.dueOps["arm"] != 1 {
		t.Fatalf("due metrics not wired: ticks=%d fired=%d ops=%v", fm.dueTicks, fm.dueFired, fm.dueOps)
	}
}

func TestDueWorkerCoalescesBusyDueMark(t *testing.T) {
	store, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	_ = store.Link("s1", "events/1", LinkGlob, "0000000000000000_0000000000000000")
	fs.mu.Lock()
	fs.tails["events/1"] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()
	if res, err := store.ArmWake("s1", now, 1000, false, "w_a", NoOwnerFence(), ConsistencyTierA); err != nil || !res.Armed {
		t.Fatalf("arm = %+v err=%v", res, err)
	}

	if fired := mgr.RunDue(); fired != 0 {
		t.Fatalf("busy due mark should coalesce without firing, got %d", fired)
	}
	if fs.count() != 0 {
		t.Fatalf("busy due mark should not append another wake event, got %d", fs.count())
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.dueTicks != 1 || fm.dueFired != 0 {
		t.Fatalf("due worker metrics should record a zero-fire tick, got ticks=%d fired=%d",
			fm.dueTicks, fm.dueFired)
	}
}

// fakeLister is an in-memory StreamLister for reconcile tests.
type fakeLister struct{ streams []StreamMeta }

func (f *fakeLister) ListStreams() ([]StreamMeta, error) { return f.streams, nil }

// TestReconcileBackfillsPatternLinkForStreamCreatedDuringOutage is the slice-3
// recovery: a stream created (with data) during an outage, whose OnStreamCreated
// hook was lost, is re-linked by the reconcile loop at the beginning offset so
// its data wakes the subscription.
func TestReconcileBackfillsPatternLinkForStreamCreatedDuringOutage(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Now()
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now) // pattern events/*

	// A matching stream created AFTER the subscription, with data, never linked.
	stream := StreamMeta{Path: "events/late", Tail: "0000000000000001_0000000000000010", CreatedAtNs: now.Add(time.Second).UnixNano()}
	fs := &fakeStreams{tails: map[string]string{stream.Path: stream.Tail}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Lister: &fakeLister{streams: []StreamMeta{stream}}})
	if err != nil {
		t.Fatal(err)
	}

	if sub, _, _ := store.Get("s1"); len(sub.Links) != 0 {
		t.Fatalf("expected no links before reconcile, got %+v", sub.Links)
	}
	mgr.RunReconcile()
	sub, _, _ := store.Get("s1")
	if len(sub.Links) != 1 || sub.Links[0].Path != "events/late" {
		t.Fatalf("reconcile should link the missed stream, got %+v", sub.Links)
	}
	// Created after the subscription → linked at the beginning so its data wakes.
	if sub.Links[0].AckedOffset != fs.BeginningOffset() {
		t.Fatalf("stream created during outage should link at beginning, got %q", sub.Links[0].AckedOffset)
	}
}

// TestReconcileBackfillsPreexistingStreamAtTail is the slice-3 no-replay case: a
// stream that predates the subscription, missed by the create-time backfill, is
// linked at its current tail so no history is replayed.
func TestReconcileBackfillsPreexistingStreamAtTail(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Now()
	stream := StreamMeta{Path: "events/old", Tail: "0000000000000001_0000000000000099", CreatedAtNs: now.Add(-time.Hour).UnixNano()}
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now) // created after the stream
	fs := &fakeStreams{tails: map[string]string{stream.Path: stream.Tail}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Lister: &fakeLister{streams: []StreamMeta{stream}}})
	if err != nil {
		t.Fatal(err)
	}
	mgr.RunReconcile()
	sub, _, _ := store.Get("s1")
	if len(sub.Links) != 1 || sub.Links[0].AckedOffset != stream.Tail {
		t.Fatalf("pre-existing stream should link at tail (no replay), got %+v", sub.Links)
	}
}
