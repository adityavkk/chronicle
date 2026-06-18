package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
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
	if c.sweep == 0 {
		return fmt.Errorf("lease-tail-drop-recovery with -sweep=0 is blocked on today's SUT: the deployed binary exposes the recovery sweep, not a separately disableable floor/eager-reconcile path")
	}
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	subID := fmt.Sprintf("jepsen-taildrop-%d", time.Now().UnixNano())
	const leaseTTLMs = int64(1000)
	if _, err := appendStream(c.base, "events/taildrop-0", 2); err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/taildrop-wake", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)

	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	if err := nem.dropLeaseTail(subID); err != nil {
		return err
	}
	wait := gcPauseDuration(leaseTTLMs) + c.sweep + c.settle
	fmt.Printf("dropped lease tail for %s; waiting %s for expiry + recovery sweep\n", subID, wait)
	sleep(wait)

	b, err := claim(c.base, subID, "worker-B")
	if err != nil {
		return fmt.Errorf("worker B claim after tail drop: %w", err)
	}
	status, code, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		return fmt.Errorf("worker A stale ack after tail drop: %w", err)
	}
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("A generation:      %d\n", a.Generation)
	fmt.Printf("B generation:      %d\n", b.Generation)
	fmt.Printf("A late-ack status: %d %s\n", status, code)
	if b.Generation <= a.Generation {
		return fmt.Errorf("post-tail-drop claim did not rotate generation: A=%d B=%d", a.Generation, b.Generation)
	}
	if status != http.StatusConflict || code != statusFenced {
		return fmt.Errorf("deposed worker ack returned %d %q, want 409 FENCED", status, code)
	}
	fmt.Println("PASS: lease-tail ZSET entry was dropped, a later holder recovered a rotated fence, and the deposed ack was fenced")
	return nil
}

func runOwnershipExclusivity(c config) error {
	if c.nemDryRun {
		fmt.Println("DRY RUN: ownership-exclusivity scaffold is installed; live check waits for claim_shard.lua/check_owner.lua and ds:{ownership}:slot:<h>")
		return nil
	}
	return fmt.Errorf("ownership-exclusivity is a proposed-mechanism scaffold: claim_shard.lua/check_owner.lua and ds:{ownership}:slot:<h> do not exist in today's SUT")
}

func runSlotIsolation(c config) error {
	if c.nemDryRun {
		fmt.Println("DRY RUN: slot-isolation scaffold is installed; live check waits for S-slot {__ds:h} homing")
		return nil
	}
	return fmt.Errorf("slot-isolation is a proposed-mechanism scaffold: S-slot {__ds:h} key homing is not implemented in today's SUT")
}

func runContentionContract(c config) error {
	if c.nemDryRun {
		fmt.Println("DRY RUN: contention-contract pure checker is installed; live C1/C2 fan-in measurements are owned by the Electric/load rig")
		return nil
	}
	return fmt.Errorf("contention-contract is a measurement scaffold: provide real claimant fan-in results from the #11/Electric rig before marking C1/C2/C3 green")
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
