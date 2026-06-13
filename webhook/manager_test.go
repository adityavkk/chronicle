package webhook

import (
	"sync"
	"testing"
	"time"
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
