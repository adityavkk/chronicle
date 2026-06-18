package webhook

import (
	"context"
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

// TestRecordWakeEventSentFences is the slice-1 store contract: a matching
// (generation, wake) stamps the sent flag; a stale one is ignored.
func TestRecordWakeEventSentFences(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	res, err := s.ArmWake("s1", now, 1000, false, "w_a")
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
	res, err := store.ArmWake("s1", now, 1000, false, "w_a")
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
	mu          sync.Mutex
	sweeps      int
	lastSubs    int
	lastTails   int
	lastWakes   int
	dueMuts     map[string]int // DueSetMutation ops by op (arm|ack|expire|release)
	dueTicks    int
	dueFiredSum int
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

func (f *fakeMetrics) FanOut(time.Duration, int, int) {}

// DueSetMutation / DueWorkerTick are wired by Move 2 (#12); record them so the
// dueWorker tests can assert the mutations and drain ticks actually fire.
func (f *fakeMetrics) DueSetMutation(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dueMuts == nil {
		f.dueMuts = map[string]int{}
	}
	f.dueMuts[op]++
}

func (f *fakeMetrics) DueWorkerTick(_ time.Duration, fired int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueTicks++
	f.dueFiredSum += fired
}

func (f *fakeMetrics) SlotOwnership(string, int)      {}
func (f *fakeMetrics) CoverageGap(time.Duration)      {}
func (f *fakeMetrics) OwnerFenced(string)             {}
func (f *fakeMetrics) ClaimContention(string, string) {}

func (f *fakeMetrics) dueMutation(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dueMuts[op]
}

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

// newDueManager builds a manager over live Redis with a fakeStreams and a
// recording fakeMetrics, for the dueWorker drain tests.
func newDueManager(t *testing.T) (*Manager, *RedisStore, *fakeStreams, *fakeMetrics, goredis.UniversalClient) {
	t.Helper()
	store, client := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{}}
	fm := &fakeMetrics{}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: fm})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr, store, fs, fm, client
}

// TestDueWorkerFiresOwedSubscription is the Move-2 drain: a re-owed mark on an
// idle subscription with pending work is re-fired in O(owed) by the dueWorker
// (here a pull-wake, so the fire is an observable wake-stream append), and the
// drain records DueWorkerTick.
func TestDueWorkerFiresOwedSubscription(t *testing.T) {
	mgr, store, fs, fm, client := newDueManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/a"] = "0000000000000001_0000000000000000" // pending work
	fs.mu.Unlock()
	// A re-owed mark on an idle sub (as expire_lease would leave it).
	if err := client.ZAdd(context.Background(), dueZKey, goredis.Z{Score: float64(now.UnixNano()), Member: "s1"}).Err(); err != nil {
		t.Fatal(err)
	}

	if fired := mgr.RunDueWorker(); fired != 1 {
		t.Fatalf("dueWorker should fire the one owed sub, fired=%d", fired)
	}
	if fs.count() != 1 {
		t.Fatalf("owed pull-wake should have been re-fired (wake-stream append), got %d", fs.count())
	}
	if fm.dueTicks != 1 || fm.dueFiredSum != 1 {
		t.Fatalf("DueWorkerTick not recorded: ticks=%d firedSum=%d", fm.dueTicks, fm.dueFiredSum)
	}
	// The re-fire arms a fresh wake, recording the arm mutation.
	if got := fm.dueMutation("arm"); got != 1 {
		t.Fatalf("re-fire should record one arm mutation, got %d", got)
	}
}

// TestDueWorkerClearsStaleMark proves the dueWorker reconciles away a mark for a
// subscription that is no longer owed (idle, cursor caught up) — without this the
// due-set would never return to ~0 at quiescence (claim_due never ZREMs and
// expire_lease re-owes unconditionally).
func TestDueWorkerClearsStaleMark(t *testing.T) {
	mgr, store, fs, fm, client := newDueManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, begin)
	// No tail set => cursor caught up => not pending.
	if err := client.ZAdd(context.Background(), dueZKey, goredis.Z{Score: float64(now.UnixNano()), Member: "s1"}).Err(); err != nil {
		t.Fatal(err)
	}

	if fired := mgr.RunDueWorker(); fired != 0 {
		t.Fatalf("a not-owed sub must not be fired, fired=%d", fired)
	}
	if fs.count() != 0 {
		t.Fatalf("no wake should be issued for a caught-up sub, got %d", fs.count())
	}
	if n := dueCard(t, client); n != 0 {
		t.Fatalf("stale mark must be cleared, got card %d", n)
	}
	if fm.dueTicks != 1 {
		t.Fatalf("drain over a non-empty due-set should record one tick, got %d", fm.dueTicks)
	}
}

// TestDueWorkerSkipsInFlight proves a mark for a subscription with a wake already
// in flight (waking/live) is left to coalesce, not re-fired or cleared — it clears
// on the eventual done-ack/release.
func TestDueWorkerSkipsInFlight(t *testing.T) {
	mgr, store, fs, _, client := newDueManager(t)
	now := time.Now()
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	// Arm directly so the sub is waking with a due mark, without a wake-stream append.
	if res, _ := store.ArmWake("s1", now, 1000, false, "w_a"); !res.Armed {
		t.Fatal("arm should succeed")
	}

	if fired := mgr.RunDueWorker(); fired != 0 {
		t.Fatalf("an in-flight wake must coalesce (not re-fire), fired=%d", fired)
	}
	if fs.count() != 0 {
		t.Fatalf("no new wake should be issued for an in-flight sub, got %d", fs.count())
	}
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("an in-flight mark must be left in place, got card %d", n)
	}
}

// TestDueRoundTripReturnsToZero is the integration round trip the issue names:
// maybeWake arms (due-ZADD), the dueWorker coalesces the in-flight mark (no
// double-fire), and the done-ack ZREMs it — so the due-set's cardinality tracks
// owed and returns to 0 at quiescence, never lingering at K.
func TestDueRoundTripReturnsToZero(t *testing.T) {
	mgr, store, fs, _, client := newDueManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/a"] = "0000000000000001_0000000000000000" // pending work
	fs.mu.Unlock()

	// arm via the append-driven path → due-ZADD + a pull-wake event.
	mgr.maybeWake("s1", "events/a")
	if n := dueCard(t, client); n != 1 {
		t.Fatalf("maybeWake should outbox one due mark, got card %d", n)
	}
	if fs.count() != 1 {
		t.Fatalf("maybeWake should have emitted one wake event, got %d", fs.count())
	}

	// dueWorker drains while the wake is in flight: it must coalesce, not re-fire.
	if fired := mgr.RunDueWorker(); fired != 0 {
		t.Fatalf("in-flight mark must coalesce at the dueWorker, fired=%d", fired)
	}
	if fs.count() != 1 {
		t.Fatalf("dueWorker must not duplicate the in-flight wake, got %d", fs.count())
	}

	// The worker completes: a done-ack clears the mark.
	sub, _, _ := store.Get("s1")
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000000"}}
	if st, _ := mgr.ack("s1", sub.Generation, sub.WakeID, sub.Generation, true, acks, now, 1000); st != "OK" {
		t.Fatalf("done ack = %q, want OK", st)
	}
	if n := dueCard(t, client); n != 0 {
		t.Fatalf("done-ack must clear the mark — due-set should return to 0, got %d", n)
	}
}

// TestDueMutationWrappersRecord asserts each due-mutating wrapper records its op
// exactly when the corresponding Lua branch runs — and that a heartbeat ack does
// not.
func TestDueMutationWrappersRecord(t *testing.T) {
	mgr, store, _, fm, _ := newDueManager(t)
	now := time.Now()
	_, _ = store.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	res, err := mgr.armWake("s1", now, 1000, true, "w_a")
	if err != nil || !res.Armed {
		t.Fatalf("armWake = %+v err=%v", res, err)
	}
	// Heartbeat ack: no due mutation recorded.
	if st, _ := mgr.ack("s1", res.Generation, res.WakeID, res.Generation, false, nil, now, 1000); st != "OK" {
		t.Fatalf("heartbeat ack = %q", st)
	}
	// Done ack: records "ack".
	if st, _ := mgr.ack("s1", res.Generation, res.WakeID, res.Generation, true, nil, now, 1000); st != "OK" {
		t.Fatalf("done ack = %q", st)
	}
	// Re-arm then release: records "release".
	res2, _ := mgr.armWake("s1", now, 1000, true, "w_b")
	if st, _ := mgr.release("s1", res2.Generation, res2.WakeID, res2.Generation); st != "OK" {
		t.Fatalf("release = %q", st)
	}
	// Re-arm then expire past the deadline: records "expire".
	res3, _ := mgr.armWake("s1", now, 1000, true, "w_c")
	_ = res3
	if st, _ := mgr.expireLease("s1", now.Add(2*time.Second)); st != "EXPIRED" {
		t.Fatalf("expireLease = %q", st)
	}

	want := map[string]int{"arm": 3, "ack": 1, "release": 1, "expire": 1}
	for op, n := range want {
		if got := fm.dueMutation(op); got != n {
			t.Errorf("DueSetMutation(%q) = %d, want %d", op, got, n)
		}
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
