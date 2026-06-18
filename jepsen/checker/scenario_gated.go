package main

import (
	"fmt"
	"time"
)

// scenario_gated.go holds the acceptance-gate scenario for a mechanism that does
// NOT exist on today's code: T5 (slot isolation, needs the S-slot {__ds:h} tagging
// — #15). Per docs/specs/horizontal-scale/research/07 this is "the executable
// contract each migration step must satisfy — red until the step lands." Its
// differential checker is unit-tested without a cluster; the LIVE driver wires
// onto the cluster when the mechanism ships.
//
// T3 (ownership exclusivity) was a gated scaffold here until #14; it is now a LIVE
// local-Redis driver in scenario_ownership.go.
//
// A gated scenario is not a failure — there is nothing to fail yet — so it prints
// its gated status and exits 0.

// runSlotIsolation is the T5 acceptance-gate scaffold (#15).
func runSlotIsolation(c config, _ *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Println("GATED: T5 (no cross-subscriber leakage) is the acceptance gate for the S-slot")
	fmt.Println("       {__ds:h} state shard, which lands in #15. Its differential checker — the")
	fmt.Println("       S-slot scatter-gather subscriber set must equal the unsharded single-")
	fmt.Println("       SMEMBERS reference, with every sub whole-homed in one slot — needs the")
	fmt.Println("       slotTag(id) keyspace that does not exist on today's single-{__ds}-slot")
	fmt.Println("       code, so it is red until #15 lands the tagging.")
	return nil
}
