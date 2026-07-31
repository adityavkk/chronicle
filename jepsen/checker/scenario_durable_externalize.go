package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// scenario_durable_externalize.go is the Tier-B acknowledged-loss checker for
// issue #140. It has the same pure-verdict + imperative-shell shape as
// scenario_failover.go, but the extra clause is different: once chronicle has
// externalized a wake/claim as Tier-B-durable, the promoted dataset must not lose
// that minting write. The run can only catch this when failover lands in the
// externalize->persist timing window; it cannot prove the bug absent, and it
// cannot observe go-redis connection routing directly. It observes the only thing
// Jepsen can see: a Tier-B-externalized fence missing after promotion.

type durableExternalizedWake struct {
	subID                      string
	generation                 int64
	primaryOffsetAtExternalize int64
	promotedOffset             int64
	survivingGeneration        int64
	reestablishedWithinBudget  bool
}

func (w durableExternalizedWake) lostAfterPromotion() bool {
	return w.promotedOffset < w.primaryOffsetAtExternalize || w.survivingGeneration < w.generation
}

type durableExternalizeResult struct {
	pass                    bool
	verdict                 string
	base                    failoverResult
	externalized            []durableExternalizedWake
	lostExternalized        []durableExternalizedWake
	reestablishedLostWithin int
}

const (
	durableExternalizeFail = "FAIL"
	durableExternalizePass = "PASS-DurableExternalize"
)

// durableExternalizeVerdict keeps the existing failover clauses (L1 at-least-once
// and deposed-worker FENCED), then adds the Tier-B-only clause: a wake/claim that
// chronicle already externalized after awaitDurable() must survive promotion. A
// positive async RPO remains acceptable only for writes that were not Tier-B
// externalized; losing an externalized Tier-B fence is a failure even if recovery
// later re-fires it, because the durability acknowledgement was false.
func durableExternalizeVerdict(gaps []deliveryGap, deposedStatus int, deposedCode string, rpoBytes int64, targetLostFence bool, rto time.Duration, streamsExpected int, externalized []durableExternalizedWake) durableExternalizeResult {
	base := failoverVerdict(gaps, deposedStatus, deposedCode, rpoBytes, targetLostFence, rto, streamsExpected)
	r := durableExternalizeResult{
		verdict:          durableExternalizeFail,
		base:             base,
		externalized:     externalized,
		lostExternalized: nil,
	}
	for _, w := range externalized {
		if !w.lostAfterPromotion() {
			continue
		}
		r.lostExternalized = append(r.lostExternalized, w)
		if w.reestablishedWithinBudget {
			r.reestablishedLostWithin++
		}
	}
	if base.pass && len(r.lostExternalized) == 0 {
		r.pass = true
		r.verdict = durableExternalizePass
	}
	return r
}

func (r durableExternalizeResult) String() string {
	s := fmt.Sprintf("TIERB-DURABLE-EXTERNALIZE-VERDICT: %s\n", r.verdict)
	s += strings.TrimRight(r.base.String(), "\n") + "\n"
	s += fmt.Sprintf("  Tier-B externalized wakes checked: %d\n", len(r.externalized))
	if len(r.lostExternalized) == 0 {
		s += "  Tier-B acknowledged losses: 0 (every externalized fence survived promotion)\n"
	} else {
		s += fmt.Sprintf("  Tier-B acknowledged losses: %d (FAIL: awaitDurable externalized a fence that promotion lost; %d later re-established within budget)\n", len(r.lostExternalized), r.reestablishedLostWithin)
		for _, w := range r.lostExternalized {
			s += fmt.Sprintf("    lost sub=%s gen=%d primary_off=%d promoted_off=%d surviving_gen=%d reestablished=%v\n", w.subID, w.generation, w.primaryOffsetAtExternalize, w.promotedOffset, w.survivingGeneration, w.reestablishedWithinBudget)
		}
	}
	return s
}

func durableExternalizeAssertingGateOK(r durableExternalizeResult) bool {
	return r.pass && len(r.externalized) > 0
}

// runDurableExternalize drives a pull-wake claim because a successful claim HTTP
// response is an externalized Tier-B-durable grant: Claim waits on WAITAOF before
// the route mints tokens and writes the 200 response. The checker records the
// primary replication offset immediately after that response and then injects the
// real Redis failover. This catches the #140 class when old code's barrier ran on
// a stale pooled connection and the promoted replica lacks the externalized claim.
func runDurableExternalize(c config, nem *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	const leaseTTLMs = 1500
	nstreams := c.streams
	if nstreams <= 0 {
		nstreams = 4
	}
	subID := fmt.Sprintf("jepsen-durable-externalize-%d", time.Now().UnixNano())
	expected := map[string]string{}
	for i := 0; i < nstreams; i++ {
		stream := fmt.Sprintf("events/de-%d", i)
		tail, err := appendStream(c.base, stream, c.msgs)
		if err != nil {
			return fmt.Errorf("seed stream %s: %w", stream, err)
		}
		expected[stream] = tail
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/de-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	for _, stream := range sortedKeys(expected) {
		tail, err := appendOne(c.base, stream, c.msgs)
		if err != nil {
			return fmt.Errorf("seed pending message on %s: %w", stream, err)
		}
		expected[stream] = tail
	}

	claimed, err := claim(c.base, subID, "worker-tierb-externalized")
	if err != nil {
		return fmt.Errorf("externalized claim: %w", err)
	}
	primaryOff := parseMasterReplOffset(asString(nem.primaryCLI("INFO", "replication")))
	replicaBefore := parseMasterReplOffset(asString(nem.replicaCLI("INFO", "replication")))
	externalized := durableExternalizedWake{subID: subID, generation: claimed.Generation, primaryOffsetAtExternalize: primaryOff}
	fmt.Printf("Tier-B externalized claim: sub=%s gen=%d wake=%s primary_off=%d replica_off_before=%d\n", subID, claimed.Generation, short(claimed.WakeID), primaryOff, replicaBefore)

	fmt.Println("nemesis: injecting real Redis failover after Tier-B externalization...")
	t0 := time.Now()
	rpoBytes := nem.redisFailover()
	if err := waitReady(c.base, 120*time.Second); err != nil {
		rto := time.Since(t0)
		externalized.promotedOffset = parseMasterReplOffset(asString(nem.replicaCLI("INFO", "replication")))
		externalized.survivingGeneration = readPromotedGeneration(nem, subID)
		res := durableExternalizeVerdict([]deliveryGap{{path: "(all)", want: "tail", got: "unreachable"}}, 0, "", rpoBytes, false, rto, nstreams, []durableExternalizedWake{externalized})
		fmt.Print(res.String())
		return fmt.Errorf("chronicle did not recover after failover within 120s: %w", err)
	}

	stopDrain := make(chan struct{})
	go drainAckOffsets(c.base, subID, stopDrain)
	deadline := time.Now().Add(60 * time.Second)
	var gaps []deliveryGap
	for time.Now().Before(deadline) {
		view, err := getSubscription(c.base, subID)
		if err == nil {
			acked := map[string]string{}
			for _, s := range view.Streams {
				acked[s.Path] = s.AckedOffset
			}
			exp := make([]deliveryExpectation, 0, len(expected))
			for _, stream := range sortedKeys(expected) {
				exp = append(exp, deliveryExpectation{path: stream, tail: expected[stream], msgs: c.msgs + 1})
			}
			gaps = CheckAtLeastOnce(exp, acked)
			if len(gaps) == 0 {
				break
			}
		}
		sleep(500 * time.Millisecond)
	}
	close(stopDrain)
	rto := time.Since(t0)
	externalized.promotedOffset = parseMasterReplOffset(asString(nem.replicaCLI("INFO", "replication")))
	externalized.survivingGeneration = readPromotedGeneration(nem, subID)
	budget := c.floor
	if budget <= 0 {
		budget = 30 * time.Second
	}
	externalized.reestablishedWithinBudget = len(gaps) == 0 && rto <= budget

	deposedStatus, deposedCode, err := ackPullWake(c.base, subID, claimed.Token, claimed.WakeID, claimed.Generation)
	if err != nil {
		fmt.Printf("note: externalized worker late ack transport error: %v\n", err)
	}
	res := durableExternalizeVerdict(gaps, deposedStatus, deposedCode, rpoBytes, externalized.lostAfterPromotion(), rto, nstreams, []durableExternalizedWake{externalized})

	fmt.Println("---- result ----")
	fmt.Printf("scenario:           %s\n", c.scenario)
	fmt.Printf("nemesis actions:    %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Print(res.String())
	if !res.pass {
		return fmt.Errorf("Tier-B durable externalize verdict %s", res.verdict)
	}
	if !durableExternalizeAssertingGateOK(res) {
		return fmt.Errorf("Tier-B durable externalize gate was only smoke")
	}
	return nil
}

func readPromotedGeneration(nem *nemesis, subID string) int64 {
	out, err := nem.replicaCLI("--raw", "HGET", dsSubKey(subID), "generation")
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return n
}
