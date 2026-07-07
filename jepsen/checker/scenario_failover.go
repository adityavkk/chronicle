package main

import (
	"fmt"
	"net/http"
	"time"
)

// scenario_failover.go is the IMPERATIVE SHELL for the gate #5 REAL-failover
// assertion (issue #43, P4.4 — INV-DUR-01 / INV-JEP-L1-01 / INV-FENCE-01). It is
// the empirical half of the durability-honesty contract: the Lean proofs and the
// TLA+/Apalache model-checks all assume the durable Lua write HAPPENED; none can
// model a primary dying and a replica that never received the write being promoted.
// This scenario drives exactly that and asserts the load-bearing claim — a lost
// fence-minting write degrades ONLY to at-least-once (a re-delivery deduplicated by
// the consumer's monotone cursor), NEVER to a safety violation (no double-grant, no
// cursor regression, no lost update past the fence).
//
// Sequence (on the STANDARD_HA substrate; standard-ha-failover.sh drives it):
//
//   1. Seed K pull-wake subscriptions over linked streams and drive the
//      claim/ack loop to a known cursor == tail (the pre-failover steady state),
//      then append one more message per stream so recovery has real pending work.
//   2. Worker A claims one subscription and STALLS (never acks) — A holds the
//      pre-failover (generation, wake_id) fence, the deposed-worker case. The
//      harness then tries to surgically remove that target fence from the would-be
//      promoted replica; without this, a run is PASS-NoLoss/Inconclusive only.
//   3. Inject a REAL failover via the redisFailover nemesis: kill the primary,
//      REPLICAOF NO ONE on the AOF replica, flip the stable `redis` endpoint.
//      Some fence-minting writes in the async-replication RPO window are dropped.
//   4. The boot reconcile (the same eager reconcile Manager.Promote drives, fired
//      by the rollout-restart standard-ha-failover.sh runs after the flip) re-fires
//      any sub stranded in the RPO window; a drainer acks the re-armed wakes.
//   5. ASSERT, via the pure CheckAtLeastOnce oracle, that every linked stream still
//      reaches acked_offset == tail — the dropped write re-fired and was deduped by
//      the forward-only cursor (at-least-once), with zero skippable gaps.
//   6. ASSERT that worker A's late ack across the promotion is FENCED (409 FENCED):
//      the at-least-once re-delivery never became a double-grant or a cursor
//      regression — the monotone fence survived the failover.
//   7. Record the empirical RPO (replication bytes the failover dropped) and RTO
//      (promotion + reconnect + reconcile time) as durability-honest tiers.
//
// The verdict is a pure function (failoverVerdict) over frozen inputs, so the
// PASS/FAIL logic + the RPO/RTO framing are unit-tested without a cluster; the
// chaos run stays an on-demand job. This scenario makes NO strong-consistency
// claim. It credits the at-least-once degradation only when the target fence loss
// is proven; zero-RPO and unrelated positive-RPO runs are smoke only.

// failoverResult is the machine-readable verdict of one failover run. It is
// emitted on stdout as a single PASS/FAIL line plus the durability-honest tiers, so
// standard-ha-failover.sh can grep it and the orchestrator can record it.
type failoverResult struct {
	pass            bool
	verdict         string
	gaps            []deliveryGap // streams that did NOT reach tail (a real L1 violation)
	deposedFenced   bool          // worker A's late ack returned 409 FENCED
	deposedStatus   int           // the actual late-ack HTTP status (for the report)
	deposedCode     string        // the actual late-ack error code
	rpoBytes        int64         // empirical RPO: replication bytes the failover dropped (>=0; -1 = injection failed)
	targetLostFence bool          // the targeted (generation,wake_id) was absent on the promoted dataset
	rto             time.Duration // empirical RTO: kill -> endpoint flip -> reconnect/reconcile -> ready
	streamsAtTail   int
	streamsExpected int
}

const (
	failoverFail                   = "FAIL"
	failoverPassLostTargetFence    = "PASS-LostTargetFence"
	failoverPassNoLossInconclusive = "PASS-NoLoss/Inconclusive"
)

// failoverVerdict is the PURE decision over a completed failover run. It FAILs if
// the injection failed, a linked stream never reached tail, or the deposed worker's
// late ack was not FENCED. A clean run then splits in two: PASS-LostTargetFence
// only when the harness proved the target (generation,wake_id) was absent on the
// promoted dataset; otherwise PASS-NoLoss/Inconclusive, a smoke result that does
// not claim the lost-write degradation was exercised. RPO bytes alone are not
// enough: rpoBytes > 0 can be unrelated bytes, and rpoBytes == 0 is no-loss smoke.
func failoverVerdict(gaps []deliveryGap, deposedStatus int, deposedCode string, rpoBytes int64, targetLostFence bool, rto time.Duration, streamsExpected int) failoverResult {
	fenced := deposedStatus == http.StatusConflict && deposedCode == "FENCED"
	injectionOK := rpoBytes >= 0
	r := failoverResult{
		verdict:         failoverFail,
		gaps:            gaps,
		deposedFenced:   fenced,
		deposedStatus:   deposedStatus,
		deposedCode:     deposedCode,
		rpoBytes:        rpoBytes,
		targetLostFence: targetLostFence,
		rto:             rto,
		streamsExpected: streamsExpected,
		streamsAtTail:   streamsExpected - len(gaps),
	}
	if injectionOK && len(gaps) == 0 && fenced {
		r.pass = true
		if targetLostFence {
			r.verdict = failoverPassLostTargetFence
		} else {
			r.verdict = failoverPassNoLossInconclusive
		}
	}
	return r
}

func failoverAssertingGateOK(r failoverResult) bool {
	return r.verdict == failoverPassLostTargetFence
}

// String renders the durability-honest verdict block, including the explicit
// at-least-once framing (NOT a strong-consistency claim) and the RPO/RTO tiers.
func (r failoverResult) String() string {
	rpo := fmt.Sprintf("%d bytes", r.rpoBytes)
	if r.rpoBytes < 0 {
		rpo = "n/a (failover injection failed)"
	}
	s := fmt.Sprintf("GATE5-FAILOVER-VERDICT: %s\n", r.verdict)
	streamNote := "at-least-once smoke: all linked streams reached tail"
	if r.targetLostFence {
		streamNote = "at-least-once: the targeted lost fence was re-fired and deduped by the monotone cursor"
	}
	s += fmt.Sprintf("  streams at tail:      %d/%d (%s)\n", r.streamsAtTail, r.streamsExpected, streamNote)
	s += fmt.Sprintf("  deposed late-ack:     %d %s (want 409 FENCED — the fence survived the promotion; no double-grant, no cursor regression)\n", r.deposedStatus, r.deposedCode)
	s += fmt.Sprintf("  empirical RPO:        %s (Tier B WAITAOF 1 1 shrinks this to the replica-fsync ack; Tier A = full async lag)\n", rpo)
	s += fmt.Sprintf("  targeted fence lost:  %v (must be true for the lost-write degradation claim; RPO bytes alone are not proof)\n", r.targetLostFence)
	s += fmt.Sprintf("  empirical RTO:        %s (promotion + endpoint flip + reconnect + boot reconcile)\n", r.rto)
	switch r.verdict {
	case failoverPassLostTargetFence:
		s += "  CLAIM: the targeted lost generation/wake degraded ONLY to at-least-once delivery (deduped downstream), NEVER to a safety violation. This is NOT a strong-consistency claim.\n"
	case failoverPassNoLossInconclusive:
		s += "  CLAIM: no targeted fence loss was proven; this is a no-loss smoke run only, not evidence for the lost-write degradation. This is NOT a strong-consistency claim.\n"
	}
	return s
}

// runFailover drives the real-failover assertion end to end. It is self-contained
// over the pull-wake claim/ack HTTP API (like lease-tail-drop), so it needs no
// webhook receiver. It requires the STANDARD_HA substrate (a real replica to
// promote); on the single-Redis rig the redisFailover nemesis cannot find a
// primary/replica pair and the injection fails honestly (rpoBytes = -1 -> FAIL with
// a clear reason), rather than faking a cloud result.
func runFailover(c config, nem *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	const leaseTTLMs = 1500
	nstreams := c.streams
	if nstreams <= 0 {
		nstreams = 4
	}

	// 1. Seed K linked streams + a pull-wake subscription, and drive the cursor to
	//    tail before the failover (the pre-failover steady state).
	subID := fmt.Sprintf("jepsen-failover-%d", time.Now().UnixNano())
	expected := map[string]string{}
	for i := 0; i < nstreams; i++ {
		stream := fmt.Sprintf("events/fo-%d", i)
		tail, err := appendStream(c.base, stream, c.msgs)
		if err != nil {
			return fmt.Errorf("seed stream %s: %w", stream, err)
		}
		expected[stream] = tail
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/fo-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created pull-wake subscription %s over %d linked streams (lease_ttl_ms=%d)\n", subID, nstreams, leaseTTLMs)

	// Drive the cursor to tail pre-failover with a short-lived drainer.
	if err := drainToTail(c, subID, expected, 30*time.Second); err != nil {
		fmt.Printf("note: pre-failover drain did not fully reach tail (%v); the post-failover assertion still holds the bar\n", err)
	}
	for _, stream := range sortedKeys(expected) {
		tail, err := appendOne(c.base, stream, c.msgs)
		if err != nil {
			return fmt.Errorf("seed post-drain pending message on %s: %w", stream, err)
		}
		expected[stream] = tail
	}
	fmt.Printf("seeded one post-drain pending message on each linked stream; target tail now set for a real re-delivery check\n")

	// 2. Worker A claims and STALLS — it holds the pre-failover fence and never acks.
	//    Its late ack across the promotion must be FENCED.
	a, err := claim(c.base, subID, "worker-A-deposed")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	fmt.Printf("worker A claimed: generation=%d wake_id=%s (holds the pre-failover fence; will be deposed by the failover)\n", a.Generation, short(a.WakeID))
	targetLostFence, err := nem.dropTargetFenceOnReplica(subID, a.Generation, a.WakeID)
	if err != nil {
		fmt.Printf("note: targeted fence-loss failpoint was not applied (%v); this run can only be PASS-NoLoss/Inconclusive if safety holds\n", err)
	} else if targetLostFence {
		fmt.Printf("nemesis: surgically dropped worker A's target fence from the would-be promoted replica before endpoint flip\n")
	}

	// 3. Inject the REAL failover and bracket it for RTO.
	fmt.Println("nemesis: injecting a REAL primary failover (kill primary -> REPLICAOF NO ONE -> flip endpoint)...")
	t0 := time.Now()
	rpoBytes := nem.redisFailover()
	if rpoBytes < 0 {
		fmt.Println("nemesis: failover injection FAILED — this needs the STANDARD_HA substrate (a primary/replica pair); refusing to fake a cloud result")
	} else {
		fmt.Printf("nemesis: failover injected; empirical RPO = %d replication bytes dropped\n", rpoBytes)
	}

	// 4. Wait for chronicle to reconnect to the promoted node and run the boot
	//    reconcile (standard-ha-failover.sh rolls the deployment after the flip; here
	//    we wait for readiness on the same stable endpoint), then drain the re-armed
	//    wakes. The RTO clock stops when the system is ready and the cursor has
	//    recovered (the drain re-fires + acks the stranded RPO-window wakes).
	if err := waitReady(c.base, 120*time.Second); err != nil {
		rto := time.Since(t0)
		// Even if readiness times out, emit an honest verdict block rather than just erroring.
		res := failoverVerdict([]deliveryGap{{path: "(all)", want: "tail", got: "unreachable"}}, 0, "", rpoBytes, targetLostFence, rto, nstreams)
		fmt.Print(res.String())
		return fmt.Errorf("chronicle did not recover after the failover within 120s: %w", err)
	}
	_ = drainToTail(c, subID, expected, 60*time.Second)
	rto := time.Since(t0)

	// 5. The L1 at-least-once oracle over the keyspace: every linked stream must have
	//    reached its tail after recovery.
	view, err := getSubscription(c.base, subID)
	if err != nil {
		return fmt.Errorf("get subscription after failover: %w", err)
	}
	acked := map[string]string{}
	for _, s := range view.Streams {
		acked[s.Path] = s.AckedOffset
	}
	exp := make([]deliveryExpectation, 0, len(expected))
	for _, s := range sortedKeys(expected) {
		exp = append(exp, deliveryExpectation{path: s, tail: expected[s], msgs: c.msgs + 1})
	}
	gaps := CheckAtLeastOnce(exp, acked)

	// 6. The decisive safety assertion: worker A's late ack with its now-stale
	//    pre-failover (generation, wake_id) MUST be FENCED. The at-least-once
	//    re-delivery never becomes a double-grant or a cursor regression.
	deposedStatus, deposedCode, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		// A transport error reaching the deposed ack is itself a failure to prove the
		// fence held; report it via the verdict block.
		fmt.Printf("note: worker A late ack transport error: %v\n", err)
	}

	// 7. Build and emit the pure verdict.
	res := failoverVerdict(gaps, deposedStatus, deposedCode, rpoBytes, targetLostFence, rto, nstreams)

	fmt.Println("---- result ----")
	fmt.Printf("scenario:           %s\n", c.scenario)
	fmt.Printf("nemesis actions:    %d (%s)\n", len(nem.log), join(nem.log))
	for _, g := range gaps {
		fmt.Printf("  LAGGING %s\n", g)
	}
	fmt.Print(res.String())

	if !res.pass {
		switch {
		case res.rpoBytes < 0:
			return fmt.Errorf("gate #5 failover: injection failed (needs the STANDARD_HA substrate — a real primary/replica pair to promote)")
		case len(res.gaps) > 0:
			return fmt.Errorf("L1/INV-JEP-L1-01 VIOLATED across the failover: %d/%d streams never reached their tail — a dropped fence-write was NOT re-fired (this would be a lost update, not at-least-once)", len(res.gaps), res.streamsExpected)
		case !res.deposedFenced:
			return fmt.Errorf("INV-FENCE-01 VIOLATED across the failover: the deposed worker A's late ack returned %d %q, want 409 FENCED — the at-least-once re-delivery became a double-grant / cursor regression (a SAFETY violation)", res.deposedStatus, res.deposedCode)
		default:
			return fmt.Errorf("gate #5 failover: FAIL")
		}
	}
	if failoverAssertingGateOK(res) {
		fmt.Println("PASS: gate #5 real failover — the targeted lost fence-write degraded only to at-least-once (deduped by the monotone cursor); the deposed ack was FENCED; safety survived a REAL promotion (not AOF replay)")
		return nil
	}
	fmt.Println("FAIL: gate #5 asserting failover — safety held, but no targeted fence loss was proven; do not credit this run as the lost-write degradation proof")
	return fmt.Errorf("gate #5 failover: inconclusive no-loss smoke (want %s, got %s)", failoverPassLostTargetFence, res.verdict)
}

// drainToTail repeatedly claims the subscription and acks the claim snapshot's
// pending offsets (done=true) until every expected stream's cursor reaches its tail
// or the deadline passes. It returns nil once all streams are at tail, else an error
// naming the shortfall. It is the recovery driver shared by the pre- and
// post-failover phases.
func drainToTail(c config, subID string, expected map[string]string, within time.Duration) error {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); drainAckOffsets(c.base, subID, stop) }()
	defer func() { close(stop); <-done }()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		view, err := getSubscription(c.base, subID)
		if err == nil {
			acked := map[string]string{}
			for _, s := range view.Streams {
				acked[s.Path] = s.AckedOffset
			}
			allAtTail := true
			for stream, tail := range expected {
				got := acked[stream]
				if got != tail && !offsetGreater(got, tail) {
					allAtTail = false
					break
				}
			}
			if allAtTail {
				return nil
			}
		}
		sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not all streams reached tail within %s", within)
}
