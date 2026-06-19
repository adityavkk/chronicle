package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func runStaleGenerationNoop(c config) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	subID := fmt.Sprintf("jepsen-stale-%d", time.Now().UnixNano())
	const leaseTTLMs = int64(1000)

	if _, err := appendStream(c.base, "events/stale-0", 2); err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/stale-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)

	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	sleep(gcPauseDuration(leaseTTLMs))
	b, err := claim(c.base, subID, "worker-B")
	if err != nil {
		return fmt.Errorf("worker B takeover claim: %w", err)
	}
	if b.Generation == a.Generation {
		return fmt.Errorf("takeover did not rotate generation: A=%d B=%d", a.Generation, b.Generation)
	}

	before, err := getSubscriptionRaw(c.base, subID)
	if err != nil {
		return err
	}
	status, code, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		return fmt.Errorf("worker A stale ack: %w", err)
	}
	after, err := getSubscriptionRaw(c.base, subID)
	if err != nil {
		return err
	}
	obs := []staleGenerationObservation{{
		scope:      "sub:" + subID,
		op:         "ack",
		requestGen: a.Generation,
		currentGen: b.Generation,
		status:     ackStatusOf(status, code),
		before:     before,
		after:      after,
	}}
	violations := CheckStaleGenerationNoop(obs)

	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("A generation:      %d\n", a.Generation)
	fmt.Printf("B generation:      %d (current)\n", b.Generation)
	fmt.Printf("stale ack status:  %d %s\n", status, code)
	fmt.Printf("snapshot bytes:    before=%d after=%d\n", len(before), len(after))
	if len(violations) > 0 {
		for _, v := range violations {
			fmt.Printf("  VIOLATION %s\n", v)
		}
		return fmt.Errorf("stale generation had an effect")
	}
	fmt.Println("PASS: stale-generation ack returned no-authority status and left the durable response snapshot byte-identical")
	return nil
}

func runLeaseTailDropRecovery(c config, nem *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	stamp := time.Now().UnixNano()
	subID := fmt.Sprintf("jepsen-taildrop-%d", stamp)
	pattern := fmt.Sprintf("events/taildrop-%d/*", stamp)
	stream := fmt.Sprintf("events/taildrop-%d/data", stamp)
	wakeStream := fmt.Sprintf("__jepsen_wake__/taildrop-%d", stamp)
	const leaseTTLMs = int64(1000)
	if err := createPullWakeSubscription(c.base, subID, pattern, wakeStream, leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	tail, err := appendStream(c.base, stream, 3)
	if err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}

	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	if !claimSnapshotCovers(a.Streams, stream, tail) {
		return fmt.Errorf("worker A setup claim did not include seeded stream at tail: stream=%s tail=%s snapshot=%+v", stream, short(tail), a.Streams)
	}
	if err := nem.dropLeaseTail(subID); err != nil {
		return err
	}
	if present, err := leaseScheduleMemberPresent(nem, subID); err != nil {
		return fmt.Errorf("check dropped lease tail: %w", err)
	} else if present {
		return fmt.Errorf("dropLeaseTail did not remove %s from the lease schedule", subID)
	}
	nem.killRedis()
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle did not reconnect after Redis restart: %w", err)
	}
	wait := gcPauseDuration(leaseTTLMs) + c.floor + c.settle
	fmt.Printf("dropped lease tail for %s; waiting up to %s for reconnect-triggered eager reconcile to re-arm from cursor/tail state\n", subID, wait)
	recovered, elapsed, err := waitRecoveredPullWake(nem, subID, a.Generation, wait)
	if err != nil {
		return err
	}

	b, err := claim(c.base, subID, "worker-B")
	if err != nil {
		return fmt.Errorf("worker B claim after sweep re-arm: %w", err)
	}
	if b.Generation != recovered.Generation || b.WakeID != recovered.WakeID {
		return fmt.Errorf("worker B did not claim the sweep-recovered wake: recovered=(gen %d,%s) B=(gen %d,%s)",
			recovered.Generation, short(recovered.WakeID), b.Generation, short(b.WakeID))
	}
	status, code, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		return fmt.Errorf("worker A stale ack after tail drop: %w", err)
	}
	if status != http.StatusConflict || code != statusFenced {
		return fmt.Errorf("deposed worker ack returned %d %q, want 409 FENCED", status, code)
	}
	acks := acksFromClaimSnapshot(b.Streams)
	if !ackSetCovers(acks, stream, tail) {
		return fmt.Errorf("worker B claim did not expose pending seeded tail: stream=%s tail=%s snapshot=%+v", stream, short(tail), b.Streams)
	}
	bStatus, bCode, err := ackPullWake(c.base, subID, b.Token, b.WakeID, b.Generation, acks...)
	if err != nil {
		return fmt.Errorf("worker B ack recovered wake: %w", err)
	}
	if bStatus != http.StatusOK {
		return fmt.Errorf("worker B ack returned %d %q, want 200", bStatus, bCode)
	}
	view, err := getSubscription(c.base, subID)
	if err != nil {
		return err
	}
	acked := ackedOffset(view, stream)
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("stream tail:       %s\n", short(tail))
	fmt.Printf("eager re-armed in: %s\n", elapsed)
	fmt.Printf("A generation:      %d\n", a.Generation)
	fmt.Printf("recovered gen:     %d\n", recovered.Generation)
	fmt.Printf("B generation:      %d (claimed recovered wake)\n", b.Generation)
	fmt.Printf("A late-ack status: %d %s\n", status, code)
	fmt.Printf("acked offset:      %s\n", short(acked))
	if acked != tail {
		return fmt.Errorf("cursor did not reach seeded tail after recovered wake: acked=%s want=%s", short(acked), short(tail))
	}
	fmt.Println("PASS: dropped lease-tail stranded a live pull-wake, the reconnect-triggered eager reconcile re-armed it from cursor/tail state, B claimed that recovered wake, cursor reached tail, and A's deposed ack was fenced")
	return nil
}

type pullWakeRuntime struct {
	Phase      string
	Generation int64
	WakeID     string
}

func waitRecoveredPullWake(nem *nemesis, id string, afterGeneration int64, timeout time.Duration) (pullWakeRuntime, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	var last pullWakeRuntime
	for time.Now().Before(deadline) {
		rt, err := pullWakeRuntimeState(nem, id)
		if err != nil {
			return pullWakeRuntime{}, 0, err
		}
		last = rt
		if rt.Phase == "waking" && rt.Generation > afterGeneration && rt.WakeID != "" {
			return rt, time.Since(start), nil
		}
		sleep(200 * time.Millisecond)
	}
	return pullWakeRuntime{}, time.Since(start), fmt.Errorf("eager reconcile did not re-arm %s after lease-tail drop within %s; last phase=%q generation=%d wake=%s",
		id, timeout, last.Phase, last.Generation, short(last.WakeID))
}

func pullWakeRuntimeState(nem *nemesis, id string) (pullWakeRuntime, error) {
	phase, err := subscriptionField(nem, id, "phase")
	if err != nil {
		return pullWakeRuntime{}, err
	}
	genRaw, err := subscriptionField(nem, id, "generation")
	if err != nil {
		return pullWakeRuntime{}, err
	}
	wakeID, err := subscriptionField(nem, id, "wake_id")
	if err != nil {
		return pullWakeRuntime{}, err
	}
	gen, err := strconv.ParseInt(genRaw, 10, 64)
	if err != nil {
		return pullWakeRuntime{}, fmt.Errorf("parse generation %q: %w", genRaw, err)
	}
	return pullWakeRuntime{Phase: phase, Generation: gen, WakeID: wakeID}, nil
}

func subscriptionField(nem *nemesis, id, field string) (string, error) {
	out, err := nem.redisCLI(subscriptionFieldCommand(id, field)...)
	if err != nil {
		return "", fmt.Errorf("hget subscription %s field %s: %w", id, field, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func subscriptionFieldCommand(id, field string) []string {
	return []string{"--raw", "hget", checkerSubKey(id), field}
}

func leaseScheduleMemberPresent(nem *nemesis, id string) (bool, error) {
	out, err := nem.redisCLI(leaseScheduleScoreCommand(id)...)
	if err != nil {
		return false, err
	}
	score := strings.TrimSpace(string(out))
	return score != "" && score != "(nil)", nil
}

func leaseScheduleScoreCommand(id string) []string {
	return []string{"--raw", "zscore", checkerLeaseZKeyForSub(id), id}
}

func claimSnapshotCovers(streams []claimStreamSnap, stream, tail string) bool {
	for _, s := range streams {
		if s.Path == stream && s.TailOffset == tail && s.HasPending {
			return true
		}
	}
	return false
}

func ackSetCovers(acks []ackBody, stream, tail string) bool {
	for _, ack := range acks {
		if ack.Stream == stream && ack.Offset == tail {
			return true
		}
	}
	return false
}

func ackedOffset(view subscriptionView, stream string) string {
	for _, s := range view.Streams {
		if s.Path == stream {
			return s.AckedOffset
		}
	}
	return ""
}

func runOwnershipExclusivity(c config) error {
	ctx := "k3d-" + c.cluster
	if c.nemDryRun {
		nem := c.newNemesis(ctx)
		_ = nem.killSlotOwner(0)
		fmt.Println("DRY RUN: ownership-exclusivity live checker is installed")
		return nil
	}
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	nem := c.newNemesis(ctx)
	before, err := waitOwnershipSlot(nem, 0, 20*time.Second)
	if err != nil {
		return err
	}
	if before.Owner == "" || before.Epoch <= 0 {
		return fmt.Errorf("ownership slot has no live owner: %+v", before)
	}
	if err := nem.killSlotOwner(0); err != nil {
		return err
	}
	after, err := waitOwnershipTransfer(nem, 0, before, 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("scenario:          ownership-exclusivity\n")
	fmt.Printf("slot:              0\n")
	fmt.Printf("before_owner:      %s\n", before.Owner)
	fmt.Printf("before_epoch:      %d\n", before.Epoch)
	fmt.Printf("after_owner:       %s\n", after.Owner)
	fmt.Printf("after_epoch:       %d\n", after.Epoch)
	fmt.Printf("nemesis_actions:   %s\n", strings.Join(nem.actions(), ","))
	fmt.Printf("result:            PASS\n")
	return nil
}

type ownershipSlotView struct {
	Owner    string
	Epoch    int64
	ExpiryNs int64
}

func waitOwnershipSlot(nem *nemesis, slot int, timeout time.Duration) (ownershipSlotView, error) {
	deadline := time.Now().Add(timeout)
	var last ownershipSlotView
	for time.Now().Before(deadline) {
		view, err := readOwnershipSlot(nem, slot)
		if err == nil && view.Owner != "" && view.Epoch > 0 && view.ExpiryNs > time.Now().UnixNano() {
			return view, nil
		}
		last = view
		sleep(250 * time.Millisecond)
	}
	return ownershipSlotView{}, fmt.Errorf("ownership slot %d did not become live before timeout; last=%+v", slot, last)
}

func waitOwnershipTransfer(nem *nemesis, slot int, before ownershipSlotView, timeout time.Duration) (ownershipSlotView, error) {
	deadline := time.Now().Add(timeout)
	var last ownershipSlotView
	for time.Now().Before(deadline) {
		view, err := readOwnershipSlot(nem, slot)
		if err == nil &&
			view.Owner != "" &&
			view.Owner != before.Owner &&
			view.Epoch > before.Epoch &&
			view.ExpiryNs > time.Now().UnixNano() {
			return view, nil
		}
		last = view
		sleep(250 * time.Millisecond)
	}
	return ownershipSlotView{}, fmt.Errorf("ownership slot %d did not transfer before timeout; before=%+v last=%+v", slot, before, last)
}

func readOwnershipSlot(nem *nemesis, slot int) (ownershipSlotView, error) {
	key := checkerOwnershipSlotKey(slot)
	out, err := nem.redisCLI("hgetall", key)
	if err != nil {
		return ownershipSlotView{}, err
	}
	fields := strings.Fields(string(out))
	values := map[string]string{}
	for i := 0; i+1 < len(fields); i += 2 {
		values[fields[i]] = fields[i+1]
	}
	epoch, _ := strconv.ParseInt(values["owner_epoch"], 10, 64)
	expiry, _ := strconv.ParseInt(values["lease_expiry_ns"], 10, 64)
	return ownershipSlotView{Owner: values["owner_id"], Epoch: epoch, ExpiryNs: expiry}, nil
}

func runSlotIsolation(c config) error {
	if c.nemDryRun {
		fmt.Println("DRY RUN: slot-isolation pure checker is installed; live T5 still requires the faulted scatter/gather run")
		return nil
	}
	return fmt.Errorf("slot-isolation live T5 is not a dry-run: run against a prepared faulted cluster and feed CheckSlotIsolationT5 observations; this command intentionally fails closed until that driver is complete")
}

func runContentionContract(c config) error {
	if c.nemDryRun {
		fmt.Println("DRY RUN: contention-contract pure checker and live local fan-in driver are installed")
		return nil
	}
	return runLiveContentionContract(c)
}

func getSubscriptionRaw(base, id string) ([]byte, error) {
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, id)
	var out []byte
	err := retry(20, 500*time.Millisecond, func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("get subscription status %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		out = bytes.TrimSpace(body)
		return nil
	})
	return out, err
}
