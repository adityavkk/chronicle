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

func (f *fakeMetrics) sweepCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sweeps
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

// TestRecoveryFloorIsCoarse asserts the recovery floor was re-defaulted off the old
// 2s fast sweep into the seconds-to-minutes band, aligned with the index reconcile
// (issue #13: the latency-sensitive cases are event-triggered now, so the floor
// bounds only the one eventless case).
func TestRecoveryFloorIsCoarse(t *testing.T) {
	if defaultSweepInterval < 10*time.Second {
		t.Fatalf("the recovery floor must be coarse (off the old 2s), got %s", defaultSweepInterval)
	}
	if defaultSweepInterval != defaultReconcileInterval {
		t.Fatalf("the floor should align with the index-reconcile band: sweep=%s reconcile=%s",
			defaultSweepInterval, defaultReconcileInterval)
	}
}

// TestReconcileScopeRouting asserts every recovery event routes through the single
// reconcile seam to a full cursor reconcile (sweepOnce), and the stubbed #14
// owner-epoch / new-owner-CAS scopes route correctly while taking no action — so
// #14 fills in a named branch rather than refactoring the seam.
func TestReconcileScopeRouting(t *testing.T) {
	mgr, store, _, fm, _ := newDueManager(t)
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}

	detectable := []scope{scopeBoot, scopeReconnect, scopeAppendError, scopeFloor}
	for _, s := range detectable {
		mgr.reconcile(s)
	}
	if got := fm.sweepCount(); got != len(detectable) {
		t.Fatalf("each detectable event must run exactly one sweep: got %d for %d events", got, len(detectable))
	}
	// The #14 stubs route correctly but perform no recovery action (no sweep).
	before := fm.sweepCount()
	mgr.reconcile(scopeEpochBump)
	mgr.reconcile(scopeNewOwnerCAS)
	if got := fm.sweepCount(); got != before {
		t.Fatalf("the stubbed epoch-bump/new-owner-CAS scopes must be no-ops, got %d sweeps (was %d)", got, before)
	}
}

// TestOnRedisReconnectCoalesces asserts the reconnect seam enqueues a single
// coalesced reconcile onto the depth-1 channel without blocking: repeated events
// collapse to one queued reconcile (duplicate reconciles are claim-fence-safe).
func TestOnRedisReconnectCoalesces(t *testing.T) {
	mgr := &Manager{reconcileC: make(chan scope, 1)}
	mgr.OnRedisReconnect()
	mgr.OnRedisReconnect() // must not block; must coalesce, not enqueue a second
	select {
	case s := <-mgr.reconcileC:
		if s != scopeReconnect {
			t.Fatalf("reconnect should enqueue scopeReconnect, got %v", s)
		}
	default:
		t.Fatal("OnRedisReconnect should have enqueued a reconcile")
	}
	select {
	case s := <-mgr.reconcileC:
		t.Fatalf("a repeated event must coalesce, but a second reconcile was queued: %v", s)
	default:
	}
}

// TestReconcileLeasesSkipsHealthySubs asserts the failover-aware reconcile restores
// only stranded subs: an idle sub (holds no lease) and a live/waking sub still
// present in the lease ZSET are both left untouched, so reconcileLeases issues no
// spurious schedule writes in steady state.
func TestReconcileLeasesSkipsHealthySubs(t *testing.T) {
	mgr, store, fs, _, _ := newDueManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	// idle: created, never armed.
	_, _ = store.CreateOrConfirm("idle", pullWakeCfg(), nil, now)
	_ = store.Link("idle", "events/i", LinkGlob, begin)
	// present: webhook armed (waking, lease ZADDed, still in the lease ZSET).
	_, _ = store.CreateOrConfirm("present", webhookCfg("https://w.example/h"), nil, now)
	_ = store.Link("present", "events/p", LinkGlob, begin)
	if res, _ := store.ArmWake("present", now, 1000, true, "w_p"); !res.Armed {
		t.Fatal("arm present")
	}
	fs.mu.Lock()
	fs.tails["events/i"] = "0000000000000001_0000000000000000"
	fs.tails["events/p"] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()

	ids, _ := store.List()
	subs, _ := store.GetMany(ids)
	tails := fs.TailOffsets(distinctLinkPaths(subs))
	if n := mgr.reconcileLeases(subs, tails, mgr.leasedSet(), now); n != 0 {
		t.Fatalf("no sub is stranded (idle holds no lease; present is in the zset), got %d restored", n)
	}
}

// TestLeaseTailDropRecoveredByEagerReconcile is the sharpened L3 (07 line 60,
// honest-gap #4) as a deterministic in-process test: a claimed pull-wake whose
// lease tail a failover dropped is recovered ONLY by the failover-aware eager
// reconcile, with the coarse floor never running. It proves the lease worker is
// blind to the drop, that the eager reconcile re-derives the lease entry from the
// durable hash so the worker sees it, that recovery then re-fires the owed wake,
// and that the deposed holder's late ack is FENCED.
func TestLeaseTailDropRecoveredByEagerReconcile(t *testing.T) {
	mgr, store, fs, _, client := newDueManager(t)
	now := time.Now()
	const leaseTTLMs = 1000
	begin := "0000000000000000_0000000000000000"
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/a"] = "0000000000000001_0000000000000000" // pending work
	fs.mu.Unlock()

	// Arm + claim: worker A holds the lease at generation gA (the pre-recovery fence).
	mgr.maybeWake("s1", "events/a")
	armed, _, _ := store.Get("s1")
	a, err := store.Claim("s1", "worker-A", armed.WakeID, now, leaseTTLMs)
	if err != nil || !a.Claimed {
		t.Fatalf("worker A claim = %+v err=%v", a, err)
	}
	if firedBefore := fs.count(); firedBefore != 1 {
		t.Fatalf("the initial arm should have emitted one wake event, got %d", firedBefore)
	}

	// The L3 fault: drop the lease AND due tail, sub hash intact (phase live).
	if err := client.ZRem(context.Background(), leaseZKey, "s1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZRem(context.Background(), dueZKey, "s1").Err(); err != nil {
		t.Fatal(err)
	}

	// The lease worker is now blind: even well past the deadline it sees nothing,
	// because the schedule entry — not the durable cursor — is what it reads.
	future := now.Add(2 * time.Second) // past the 1s lease
	if due, _ := store.DueLeases(future, dueClaimLimit, time.Second); len(due) != 0 {
		t.Fatalf("the lease worker must be blind to the dropped tail, got due=%v", due)
	}

	// The explicit takeover trigger — a Redis reconnect — fires the eager reconcile.
	// The coarse floor never runs (we drive reconcile directly, no recoveryLoop).
	mgr.reconcile(scopeReconnect)

	// The eager reconcile re-derived the lease entry from the durable hash, so the
	// fast lease worker now sees the lapse — the cursor-reading reconciler doing
	// exactly what the lease worker could not.
	due, _ := store.DueLeases(future, dueClaimLimit, time.Second)
	if len(due) != 1 || due[0] != "s1" {
		t.Fatalf("the eager reconcile must restore the lease entry so the worker sees it, got due=%v", due)
	}

	// Drive the recovery the restored schedule now enables, on the same lapsed
	// clock the lease worker and dueWorker would observe in production: expire the
	// lapsed lease (idle + re-owe due at `future`), then drain the due-set at
	// `future` — exactly what drainDue does, with the clock injected so the test is
	// deterministic rather than sleeping out the lease TTL.
	if st, _ := mgr.expireLease("s1", future); st != "EXPIRED" {
		t.Fatalf("the restored lapsed lease must expire to idle, got %q", st)
	}
	claimed, _ := store.ClaimDue(future, dueClaimLimit, time.Second)
	fired := 0
	for _, id := range claimed {
		if mgr.fireDue(id) {
			fired++
		}
	}
	if fired != 1 {
		t.Fatalf("recovery must re-fire the owed wake, fired=%d (claimed=%v)", fired, claimed)
	}
	if fs.count() != 2 {
		t.Fatalf("the re-fired wake should be a fresh wake-stream append, got %d", fs.count())
	}

	// The deposed worker A's late ack with its stale (generation, wake_id) must be
	// FENCED — recovery rotated a fresh generation on the re-fire.
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000000"}}
	if st, _ := mgr.ack("s1", a.Generation, a.WakeID, a.Generation, true, acks, future, leaseTTLMs); st != "FENCED" {
		t.Fatalf("the deposed holder's late ack must be FENCED, got %q", st)
	}
}
