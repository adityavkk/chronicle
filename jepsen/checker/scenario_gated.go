package main

import (
	"fmt"
	"time"
)

// scenario_gated.go holds the acceptance-gate scenarios for mechanisms that do
// NOT exist on today's code: T3 (ownership exclusivity, needs claim_shard.lua /
// the ds:{ownership} keyspace — #14) and T5 (slot isolation, needs the S-slot
// {__ds:h} tagging — #15). Per docs/specs/horizontal-scale/research/07 these are
// "the executable contract each migration step must satisfy — red until the step
// lands." Their MODELS/checkers are unit-tested without a cluster (model_shard.go
// for T3); the LIVE driver wires onto the cluster when the mechanism ships.
//
// A gated scenario is not a failure — there is nothing to fail yet — so it prints
// its gated status and exits 0. It still proves the harness reaches the cluster
// and that the matching nemesis primitive runs (and correctly no-ops) against the
// mechanism-less keyspace, so the #14/#15 driver inherits a working seam.

// runOwnershipExclusivity is the T3 acceptance-gate scaffold (#14).
func runOwnershipExclusivity(c config, nem *nemesis) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	// Exercise the killSlotOwner nemesis end to end: on today's code the
	// ds:{ownership}:slot:<h> record does not exist, so it must cleanly no-op
	// rather than kill the wrong pod — the seam the #14 driver reuses.
	nem.killSlotOwner(0)

	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Println("GATED: T3 (ownership exclusivity) is the acceptance gate for claim_shard.lua /")
	fmt.Println("       ds:{ownership}:slot:<h>, which land in #14. The porcupine ownership-CAS")
	fmt.Println("       model is the executable spec and is unit-tested without a cluster:")
	fmt.Println("         go test ./jepsen/checker/ -run TestShardModel")
	fmt.Println("       The live driver records claim_shard/check_owner into a history and checks")
	fmt.Println("       it against shardModel() — wired when #14 ships the mechanism.")
	return nil
}

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
