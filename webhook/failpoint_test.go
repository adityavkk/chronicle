package webhook

import (
	"testing"
	"time"
)

// TestFailpointArmedBeforeEmitStrandsThenRecovers exercises the gofail-style
// in-process seam (07 honest-gap #2): it crashes at the EXACT few-µs arm→emit
// window inside issueWake — the window a host-driven `kubectl delete pod` nemesis
// cannot land on — leaving a pull-wake armed (waking, wake_event_sent_ns=0) but
// not yet emitted, then proves the recovery sweep re-emits it. This pins down,
// deterministically and in-process, the property runPullWakeArmCrash can only
// approximate end-to-end from the host.
func TestFailpointArmedBeforeEmitStrandsThenRecovers(t *testing.T) {
	mgr, store, fs, _, _ := newDueManager(t)
	now := time.Now()
	begin := "0000000000000000_0000000000000000"
	_, _ = store.CreateOrConfirm("s1", pullWakeCfg(), nil, now)
	_ = store.Link("s1", "events/a", LinkGlob, begin)
	fs.mu.Lock()
	fs.tails["events/a"] = "0000000000000001_0000000000000000" // pending work
	fs.mu.Unlock()

	// Install the failpoint: crash at the arm→emit window. The hook is process-
	// global, so reset it on cleanup.
	fired := false
	FailpointHook = func(name string) {
		if name == fpArmedBeforeEmit {
			fired = true
			panic("failpoint: simulated crash between arm and emit")
		}
	}
	t.Cleanup(func() { FailpointHook = nil })

	// Drive the wake; the panic unwinds through issueWake and is recovered here,
	// modeling a process crash in the surgical window.
	func() {
		defer func() { _ = recover() }()
		mgr.maybeWake("s1", "events/a")
	}()
	if !fired {
		t.Fatal("the failpoint must fire at the arm→emit window")
	}

	// The arm committed (fence minted on the primary) but the emit did not: the sub
	// is stranded exactly as a real crash in this window would leave it.
	sub, _, _ := store.Get("s1")
	if sub.Phase != PhaseWaking || sub.WakeEventSentNs != 0 {
		t.Fatalf("expected stranded waking/sent=0, got phase=%v sent=%d", sub.Phase, sub.WakeEventSentNs)
	}
	if fs.count() != 0 {
		t.Fatalf("no wake event should have been emitted before the crash, got %d", fs.count())
	}

	// Recovery: with the failpoint cleared, the sweep re-emits the stranded wake —
	// the durability backstop honest-gap #2's surgical window was built to test.
	FailpointHook = nil
	mgr.RunSweep()
	if fs.count() != 1 {
		t.Fatalf("the recovery sweep must re-emit the stranded wake, got %d", fs.count())
	}
}
