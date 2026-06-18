package main

import (
	"fmt"
	"net/http"
	"time"
)

// scenario_leasetail.go is the IMPERATIVE SHELL for L3 (failover recovery of a
// stranded wake, docs/specs/horizontal-scale/research/07 line 60). It claims a
// pull-wake subscription, then ZREMs its lease-schedule entry while leaving the
// sub hash intact (the dropLeaseTail nemesis — the exact L3 fault: a failover
// that lost the schedule tail). The lease WORKER can no longer see the lease
// (its ZADD is gone), so the only thing that can recover the subscription is a
// cursor-reading reconciler, which re-derives owed work from the durable cursor.
// The test asserts the cursor reaches tail AND the deposed holder's late ack is
// FENCED.
//
// Two variants, selected by -floor (07 honest-gap #4, resolved by #13):
//
//   - SHARPENED (-floor=0 + explicit takeover): the server runs with its periodic
//     floor disabled, so a sweep tick can NOT be the recoverer. After the drop we
//     force an explicit takeover (restart the origins → the boot recovery event),
//     and the new process's failover-aware eager reconcile (manager.go
//     reconcileLeases) re-ZADDs the lease entry from the durable sub hash so the
//     fast lease worker drives expiry. Recovery within lease_ttl_ms + RTT — far
//     under any periodic floor — proves the EAGER reconcile at the trigger, not a
//     tick, recovered the stranded live/waking sub (#13's load-bearing claim).
//
//   - FLOOR (-floor>0, no takeover): no event fires, so the coarse periodic floor
//     is the recoverer; the bound is lease_ttl_ms + floor + RTT. This is the
//     eventless case the floor exists for, tested here without a takeover.
//
// Both are GREEN: sweepOnce (whether run on the floor tick or at the boot event)
// reads store.List() — the full subscription SET, not the lease ZSET — so it sees
// the sub, and reconcileLeases re-derives its dropped tail. The deposed holder's
// late ack is FENCED in both, since recovery rotated a fresh generation.

// runLeaseTailDrop drives the L3 fault and asserts recovery + the deposed fence.
func runLeaseTailDrop(c config, nem *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	subID := fmt.Sprintf("jepsen-l3-%d", time.Now().UnixNano())
	const leaseTTLMs = 1500
	stream := "events/l3-0"

	tail, err := appendStream(c.base, stream, 6)
	if err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/l3-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created pull-wake subscription %s (lease_ttl_ms=%d), tail=%s\n", subID, leaseTTLMs, short(tail))

	// Worker A claims (coalescing onto the append-armed wake) and then stalls — it
	// never acks. A's token is the pre-recovery generation.
	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	fmt.Printf("worker A claimed: generation=%d (holds the lease)\n", a.Generation)

	// The L3 fault: drop A's lease-schedule entry, leaving the sub hash intact. The
	// lease worker is now blind to this lease; only a cursor-reading reconciler can
	// recover it.
	nem.dropLeaseTail(subID)
	fmt.Printf("nemesis: dropped the lease-schedule tail for %s (sub hash intact)\n", subID)

	// Optional extra faults to widen the window (off unless flagged): a clock skew
	// and a toxiproxy Redis partition/heal churn run concurrently with recovery.
	stopOpt := startOptionalNemeses(c, nem)
	defer stopOpt()

	// Two variants (07 honest-gap #4). -floor=0 is the SHARPENED variant: the
	// server's periodic floor is disabled, so we force an explicit takeover — a
	// restart, firing the boot recovery event — and the new process's eager
	// reconcile (reconcileLeases) re-derives the dropped tail. Recovery within
	// lease + RTT, far under any periodic tick, proves the eager reconcile, not a
	// sweep, recovered it. -floor>0 is the eventless FLOOR variant: no event fires,
	// so the coarse periodic floor is the recoverer.
	sharpened := c.floor == 0
	var bound time.Duration
	if sharpened {
		bound = time.Duration(leaseTTLMs)*time.Millisecond + c.sweep // lease + an RTT/eager margin
		fmt.Printf("L3 SHARPENED (-floor=0): forcing an explicit takeover so ONLY the eager reconcile can recover...\n")
		nem.killAllOrigins() // the explicit takeover: restart -> boot reconcile -> reconcileLeases
		if err := waitReady(c.base, 60*time.Second); err != nil {
			return fmt.Errorf("origins did not come back after the takeover: %w", err)
		}
	} else {
		bound = time.Duration(leaseTTLMs)*time.Millisecond + c.floor // lease + the coarse floor
		fmt.Printf("L3 FLOOR (-floor=%s): no takeover; the coarse periodic floor is the recoverer...\n", c.floor)
	}
	settle := c.settle
	if settle < bound+5*time.Second {
		settle = bound + 5*time.Second
	}
	fmt.Printf("L3 recovery bound: %s; settling %s while a drainer acks the re-armed wake...\n", bound, settle)

	// A drainer claims the re-armed wake and acks the pending offsets to tail. It
	// acks the snapshot's offsets (unlike main.go's drainWorker, which sends none),
	// so the cursor actually advances — proving recovery, not just a re-wake.
	stopDrain := make(chan struct{})
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); drainAckOffsets(c.base, subID, stopDrain) }()

	deadline := time.Now().Add(settle)
	reached := false
	for time.Now().Before(deadline) {
		if v, err := getSubscription(c.base, subID); err == nil {
			if cursorAtTail(v, stream, tail) {
				reached = true
				break
			}
		}
		sleep(500 * time.Millisecond)
	}
	close(stopDrain)
	<-drainDone

	// The deposed worker A's late ack with its stale (generation, wake_id) must be
	// FENCED — the sweep re-armed a fresh generation.
	aStatus, aCode, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		return fmt.Errorf("worker A late ack: %w", err)
	}

	recoverer := "the coarse periodic floor"
	if sharpened {
		recoverer = "the eager reconcile at the explicit takeover"
	}
	fmt.Println("---- result ----")
	fmt.Printf("scenario:           %s\n", c.scenario)
	fmt.Printf("variant:            %s\n", map[bool]string{true: "sharpened (-floor=0 + takeover)", false: "floor (eventless)"}[sharpened])
	fmt.Printf("nemesis actions:    %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("cursor reached tail:%v (want true — recovered by %s, not the lease worker)\n", reached, recoverer)
	fmt.Printf("A late-ack status:  %d %s (want 409 FENCED)\n", aStatus, aCode)

	if !reached {
		return fmt.Errorf("L3 violated: the stranded wake was never recovered — cursor did not reach %s within %s", short(tail), settle)
	}
	if aStatus != http.StatusConflict || aCode != "FENCED" {
		return fmt.Errorf("L3 violated: the deposed worker A's late ack returned %d %q, want 409 FENCED", aStatus, aCode)
	}
	fmt.Printf("PASS: L3 — the lease-tail-drop was recovered by %s (the lease worker was blind to it) and the deposed ack was fenced\n", recoverer)
	return nil
}

// drainAckOffsets repeatedly claims the subscription and acks the claim
// snapshot's pending offsets (done=true), advancing the cursor toward tail, until
// stop is closed. Errors (no pending work, churn) are retried after a short pause.
func drainAckOffsets(base, subID string, stop <-chan struct{}) {
	worker := fmt.Sprintf("l3-drain-%d", time.Now().UnixNano())
	for {
		select {
		case <-stop:
			return
		default:
		}
		res, err := claim(base, subID, worker)
		if err != nil || res.Token == "" {
			sleep(300 * time.Millisecond)
			continue
		}
		acks := make([]ackEntry, 0, len(res.Streams))
		for _, s := range res.Streams {
			if s.HasPending {
				acks = append(acks, ackEntry{Stream: s.Path, Offset: s.TailOffset})
			}
		}
		_, _, _ = callbackWithAcks(base, subID, res.Token, res.WakeID, res.Generation, acks, true)
	}
}

// cursorAtTail reports whether the subscription's cursor for stream has reached
// (or passed) tail.
func cursorAtTail(v subscriptionView, stream, tail string) bool {
	for _, s := range v.Streams {
		if s.Path == stream {
			return s.AckedOffset == tail || offsetGreater(s.AckedOffset, tail)
		}
	}
	return false
}

// startOptionalNemeses launches the flag-gated extra faults (clock skew, a
// toxiproxy Redis partition/heal churn) and returns a stop function. Both are
// off by default; they need privileged / proxy substrate (07 honest-gap #3).
func startOptionalNemeses(c config, nem *nemesis) func() {
	if c.clockSkewBy == 0 && c.toxiproxy == "" {
		return func() {}
	}
	if c.clockSkewBy != 0 {
		nem.clockSkew(c.clockSkewBy)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if c.toxiproxy == "" {
			<-stop
			return
		}
		tp := newToxiproxy(c.toxiproxy, c.redisProxy)
		for {
			select {
			case <-stop:
				_ = tp.heal()
				return
			case <-time.After(3 * time.Second):
				_ = tp.partition()
				sleep(time.Second)
				_ = tp.heal()
			}
		}
	}()
	return func() { close(stop); <-done }
}
