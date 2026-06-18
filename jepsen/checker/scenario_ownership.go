package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// scenario_ownership.go is the LIVE driver for T3 — ownership exclusivity (07 line
// 44), the acceptance gate for #14's claim_shard.lua / check_owner.lua. It is the
// imperative shell over the pure model_shard.go oracle: N concurrent claimants
// (each a distinct replica id) race claim_shard / check_owner over a shared set of
// ds:{ownership} slots, with an in-process gcPause nemesis forcing intra-slot
// takeovers, and the recorded history is checked for linearizability against the
// porcupine shardModel — Unknown counts as FAIL (a too-concurrent history proves
// nothing). A PASS proves claim_shard is a real single-holder CAS, not a
// silently-dropping last-writer-wins register (06 correction #3).
//
// Like shard-linz / contention it drives webhook.RedisStore directly against a
// local Redis — NO cluster (the porcupine ownership-CAS model needs only Redis +
// concurrent claimants). The L2/L4 churn properties that DO need ≥2 replicas + a
// kill-slot-owner nemesis run on the multi-replica/k3d rig (see docs/jepsen).

// runOwnershipExclusivity drives the T3 ownership-CAS linearizability gate.
func runOwnershipExclusivity(c config) error {
	store, client, err := contentionStore(c)
	if err != nil {
		return err
	}
	defer client.Close()

	slots := c.slots
	if slots < 1 {
		slots = 4
	}
	workers := c.workers
	if workers < 2 {
		workers = 2 // exclusivity needs at least two contenders per slot
	}
	// A short ownership lease so a gcPause reliably outlives it and a peer takes
	// over (rotating the slot's owner_epoch). This is the {ownership} slotLeaseTTL,
	// a different layer from the per-subscription webhook lease_ttl_ms.
	slotTTL := 250 * time.Millisecond
	// Bound the run so each slot partition stays in porcupine's tractable range (07
	// gap #1: the linearizability search is NP-hard). A small CAS-register state
	// linearizes fast, but we still cap per-worker ops and the wall-clock.
	opsPerWorker := 30 * slots / workers
	if opsPerWorker < 12 {
		opsPerWorker = 12
	}
	deadline := time.Now().Add(time.Duration(minInt(c.workloadMs, 4000)) * time.Millisecond)

	// Unique slot keys per run so a rerun starts from a fresh ownership register
	// (the model's Init is "unowned, epoch 0"). Cleaned up on exit.
	runTag := time.Now().UnixNano()
	slotKeys := make([]string, slots)
	slotIDs := make([]string, slots)
	for h := 0; h < slots; h++ {
		slotKeys[h] = fmt.Sprintf("ds:{ownership}:slot:t3-%d-%d", runTag, h)
		slotIDs[h] = fmt.Sprintf("h%d", h)
	}
	defer func() {
		ctx := context.Background()
		for _, k := range slotKeys {
			client.Del(ctx, k)
		}
	}()

	fmt.Printf("== ownership-exclusivity (T3): slots=%d claimants=%d slot_ttl=%s ops/worker=%d ==\n",
		slots, workers, slotTTL, opsPerWorker)

	rec := newRecorder()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			ownershipClaimant(store, slotKeys, slotIDs, wid, slotTTL, opsPerWorker, deadline, rec)
		}(w)
	}
	wg.Wait()

	history := rec.history()
	parts := len(partitionByShard(history))
	fmt.Printf("operations: %d across %d slot partitions\n", len(history), parts)

	result, info := porcupine.CheckOperationsVerbose(shardModel(), history, 20*time.Second)
	switch result {
	case porcupine.Ok:
		fmt.Println("PASS: T3 ownership exclusivity linearizable — claim_shard is a real single-holder")
		fmt.Println("      CAS (epoch bumped on every transfer, kept on renew); no deposed owner was")
		fmt.Println("      ever told it still owns a slot. The owner-epoch fence holds under concurrency")
		fmt.Println("      + GC pauses, layered above the (gen,wake_id) fence.")
		return nil
	case porcupine.Illegal:
		const path = "ownership-exclusivity-counterexample.html"
		if verr := porcupine.VisualizePath(shardModel(), info, path); verr == nil {
			fmt.Printf("counterexample: %s\n", path)
		}
		return fmt.Errorf("NOT linearizable: two owners accepted for one slot, or a transfer reused an epoch — T3 violated (claim_shard is a silently-dropping LWW, not a CAS)")
	default:
		return fmt.Errorf("linearizability UNKNOWN: history too concurrent (reduce -workers/-workload-ms/-slots; 07 gap #1) — Unknown counts as FAIL for T3")
	}
}

// ownershipClaimant is one replica racing for the shared slots: it claims a slot,
// records the CAS reply, occasionally GC-pauses past the lease so a peer takes the
// slot over (rotating owner_epoch), then verifies ownership with check_owner at
// the epoch it believes it holds — so a deposed-but-resumed claimant records the
// FENCED verdict the model proves is a safe no-op while a live owner records OWNER.
func ownershipClaimant(store *webhook.RedisStore, slotKeys, slotIDs []string, wid int, slotTTL time.Duration, opsPerWorker int, deadline time.Time, rec *recorder) {
	replica := fmt.Sprintf("r-%d", wid)
	backoff := time.Duration(15+wid*7) * time.Millisecond
	held := map[int]string{} // slot index -> the epoch this claimant last held
	grants := 0
	S := len(slotKeys)
	for cycle := 0; cycle < opsPerWorker && time.Now().Before(deadline); cycle++ {
		h := (cycle + wid) % S // round-robin so every claimant contends every slot

		callNs := rec.now()
		claim, err := store.ClaimSlot(slotKeys[h], replica, time.Now(), slotTTL)
		if err != nil {
			sleep(backoff)
			continue
		}
		rec.recordOp(wid, shardInput{shard: slotIDs[h], op: opClaimShard, caller: replica}, claimOutput(claim, replica), callNs)
		if !claim.Granted() {
			sleep(backoff)
			continue
		}
		held[h] = claim.Epoch.String()
		grants++
		// gcPause on ~every third grant: stall past the lease so a same-slot peer
		// takes over (bumping owner_epoch) and this claimant resumes deposed.
		if grants%3 == 0 {
			sleep(gcPause(slotTTL))
		}
		// check_owner at the epoch we believe we hold: OWNER if still current,
		// FENCED if a peer deposed us during the pause.
		callNs = rec.now()
		chk, cerr := store.CheckOwner(slotKeys[h], replica, held[h])
		if cerr != nil {
			continue
		}
		reqEpoch, _ := strconv.ParseInt(held[h], 10, 64)
		rec.recordOp(wid, shardInput{shard: slotIDs[h], op: opCheckOwner, caller: replica, reqEpoch: reqEpoch}, checkOutput(chk), callNs)
		sleep(backoff)
	}
}

// claimOutput maps a webhook.SlotClaim to the model's shardOutput. On BUSY the
// model ignores owner/epoch (a legal no-op), but we record the observed foreign
// owner for a readable counterexample.
func claimOutput(c webhook.SlotClaim, me string) shardOutput {
	switch c.Status {
	case webhook.SlotClaimed:
		return shardOutput{status: statusClaimed, owner: me, epoch: c.Epoch.Value()}
	case webhook.SlotRenewed:
		return shardOutput{status: statusRenewed, owner: me, epoch: c.Epoch.Value()}
	default:
		return shardOutput{status: statusBusy, owner: c.Owner.String(), epoch: c.Epoch.Value()}
	}
}

func checkOutput(c webhook.OwnerCheck) shardOutput {
	switch c {
	case webhook.OwnerCheckOwner:
		return shardOutput{status: statusOwner}
	case webhook.OwnerCheckUnowned:
		return shardOutput{status: statusUnowned}
	default:
		return shardOutput{status: statusFenced}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
