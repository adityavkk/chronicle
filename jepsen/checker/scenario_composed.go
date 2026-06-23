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

// scenario_composed.go is the LIVE driver for the TWO-FENCE COMPOSITION gate
// (P2.2, issue #36): the imperative shell over the pure model_composed.go oracle.
// K concurrent workers contend on ONE (subscription, slot) — racing the
// subscription's inner (gen, wake) lease fence AND that slot's outer (owner, epoch)
// ownership lease at the same time — and the recorded history is checked for
// linearizability against composedModel(). Unknown counts as FAIL.
//
// THE INTERACTION IT BINDS TO LIVE REDIS. Each worker is a distinct replica that
// (1) claims the ownership SLOT via ClaimSlot (claim_shard.lua) — transfer bumps
// the owner epoch, renew keeps it — and (2) drives the subscription's lease via
// Claim/AckShard/Release (claim.lua / ack.lua / release.lua) PASSING its OwnerScope
// {slotKey, replica, epoch}, so ack.lua/release.lua inline owner_fenced ABOVE the
// still-byte-for-byte (gen, wake) fence (common.lua). A gcPause stalls a holder
// past BOTH leases, so a peer transfers the slot (rotating the epoch) and/or takes
// over the subscription lease (rotating the generation) and the paused worker
// resumes DEPOSED on one or both layers. Its ack then races in stale:
//
//   - inner-stale  (gen, wake rotated under it) -> ack.lua's (gen,wake) fence -> FENCED
//   - owner-stale  (epoch bumped under it)      -> the inlined owner_fenced     -> FENCED
//   - both current -> OK
//
// The composed model proves the load-bearing layering claim (INV-OWNER-02): every
// OK the server returns is fence-valid under the INNER (gen, wake) register ALONE,
// regardless of the owner-epoch verdict — the owner fence only ever turns an
// otherwise-legal OK into a FENCED no-op, never the reverse. A history where a
// (gen,wake)-stale ack returned OK (the single-holder violation) has no
// linearization, so porcupine surfaces the witness — and crucially it stays
// catchable WITH the owner layer live, which neither single-layer model can show.
//
// Like ownership-exclusivity / shard-linz it drives webhook.RedisStore directly
// against a local Redis — NO cluster. The in-process gcPause is the only nemesis
// (07's highest-ROI T1/T3 fault); the network/partition/clock-skew nemeses are
// honest-best-effort substrate the Redis-direct job does not wire (see nemesis.go).

// runComposedFences drives the composed two-fence linearizability gate.
func runComposedFences(c config) error {
	store, client, err := contentionStore(c)
	if err != nil {
		return err
	}
	defer client.Close()

	workers := c.workers
	if workers < 3 {
		workers = 3 // the composition needs >= 3 contenders to interleave both layers
	}
	// Short leases on BOTH layers so a single gcPause reliably outlives them and a
	// peer takes over, rotating the generation and/or the epoch. The subscription
	// lease_ttl_ms (inner) and the {ownership} slotLeaseTTL (outer) are INDEPENDENT,
	// and the slot lease is deliberately SHORTER so ownership churns more often than
	// the inner lease — a deposed-on-the-outer-layer worker is the optimization case
	// the composition tests, distinct from the inner-fence takeover.
	leaseTTLMs := int64(c.leaseTTLMs)
	if leaseTTLMs <= 0 || leaseTTLMs > 1500 {
		leaseTTLMs = 600 // short by default; -lease-ttl-ms overrides within bound
	}
	slotTTL := time.Duration(leaseTTLMs) * time.Millisecond / 2

	// The wall-clock deadline is the real bound; opsPerWorker is a generous safety
	// cap so a worker that never blocks still terminates. The single (sub, slot)
	// partition's composed register is small, so porcupine linearizes a few-hundred-op
	// history inside its budget (07 gap #1: the search is NP-hard, but a 6-field
	// register over one key stays tractable). Unknown counts as FAIL.
	opsPerWorker := 200
	deadline := time.Now().Add(time.Duration(minInt(c.workloadMs, 5000)) * time.Millisecond)

	// Unique sub + slot per run so a rerun starts from the model's Init (idle inner
	// fence, unowned slot). Both are cleaned up on exit.
	runTag := time.Now().UnixNano()
	subID := fmt.Sprintf("composed-%d", runTag)
	slotKey := fmt.Sprintf("ds:{ownership}:slot:composed-%d", runTag)
	const subLabel = "S" // the model's partition sub-key (one subscription)
	const slotLabel = "h0"

	if _, err := store.CreateOrConfirm(subID, pullWakeContentionCfg(subID, leaseTTLMs), nil, time.Now()); err != nil {
		return fmt.Errorf("create sub: %w", err)
	}
	defer func() {
		ctx := context.Background()
		_ = store.Delete(subID)
		client.Del(ctx, slotKey)
	}()

	fmt.Printf("== composed two-fence (P2.2): sub=%s slot=%s workers=%d lease_ttl=%dms ops/worker=%d ==\n",
		subID, slotLabel, workers, leaseTTLMs, opsPerWorker)

	rec := newRecorder()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			composedWorker(store, subID, slotKey, subLabel, slotLabel, wid, leaseTTLMs, slotTTL, opsPerWorker, deadline, rec)
		}(w)
	}
	wg.Wait()

	history := rec.history()
	parts := len(partitionByComposed(history))
	fmt.Printf("operations: %d across %d (sub,slot) partitions\n", len(history), parts)

	result, info := porcupine.CheckOperationsVerbose(composedModel(), history, 20*time.Second)
	switch result {
	case porcupine.Ok:
		fmt.Println("PASS: composed two-fence linearizable — every OK is fence-valid under the INNER")
		fmt.Println("      (gen,wake) register ALONE; the owner-epoch fence only turned legal OKs into")
		fmt.Println("      FENCED no-ops, never the reverse. INV-FENCE-01 single-holder holds ACROSS")
		fmt.Println("      owner-epoch transitions, confirming INV-OWNER-02 (owner-epoch is")
		fmt.Println("      optimization-only, never a correctness dependency).")
		return nil
	case porcupine.Illegal:
		const path = "composed-counterexample.html"
		if verr := porcupine.VisualizePath(composedModel(), info, path); verr == nil {
			fmt.Printf("counterexample: %s\n", path)
		}
		return fmt.Errorf("NOT linearizable: an OK was not justifiable under the inner (gen,wake) fence " +
			"(two holders accepted), or an owner transfer reused an epoch — the two-fence composition is violated")
	default:
		return fmt.Errorf("linearizability UNKNOWN: history too concurrent (reduce -workers/-workload-ms; 07 gap #1) — Unknown counts as FAIL")
	}
}

// composedWorker is one replica contending on BOTH fences of the shared
// (sub, slot). The structure that produces a REAL two-fence interaction (not two
// layers that never collide) is: a worker captures BOTH a slot epoch and an inner
// (gen, wake) token, THEN — on a pausing cycle — gcPauses past both leases BEFORE
// acking, so a peer takes over the inner lease (rotating gen) and/or the slot
// (bumping epoch) and the paused worker resumes holding a STALE token on one or
// both layers. Its ack then races in stale UNDER ITS OWNER SCOPE: ack.lua checks
// owner_fenced FIRST (outer), then the (gen, wake) fence (inner), so a deposed
// worker is FENCED by whichever layer rotated and a still-live worker is OK.
//
// Crucially the worker does NOT bail when the slot is BUSY — it still races the
// inner subscription lease using the LAST epoch it held (which may now be stale).
// That decoupling is what lets the inner fence have competing claimants while the
// owner-epoch register churns independently, so the composition is exercised.
func composedWorker(store *webhook.RedisStore, subID, slotKey, subLabel, slotLabel string, wid int, leaseTTLMs int64, slotTTL time.Duration, opsPerWorker int, deadline time.Time, rec *recorder) {
	replica := fmt.Sprintf("r-%d", wid)
	backoff := time.Duration(8+wid*5) * time.Millisecond
	grants := 0
	heldEpoch := "0" // the slot epoch this worker last held; "0" until it owns the slot

	for cycle := 0; cycle < opsPerWorker && time.Now().Before(deadline); cycle++ {
		// (1) Outer: claim the ownership slot. A transfer bumps the epoch (fencing the
		// prior owner); a renew keeps it; BUSY grants nothing — but the worker keeps
		// its LAST-held epoch and proceeds to the inner lease regardless, so a stale
		// epoch can be presented later (the deposed-owner case).
		callNs := rec.now()
		claim, serr := store.ClaimSlot(slotKey, replica, time.Now(), slotTTL)
		if serr == nil {
			rec.recordOp(wid, composedInput{
				sub: subLabel, slot: slotLabel, class: classOuter, outerOp: opClaimShard, caller: replica,
			}, composedClaimShardOutput(claim, replica), callNs)
			if claim.Granted() {
				heldEpoch = claim.Epoch.String()
			}
		}
		owner := webhook.OwnerScope{SlotKey: slotKey, ReplicaID: replica, Epoch: heldEpoch}
		reqEpoch, _ := strconv.ParseInt(heldEpoch, 10, 64)

		// (2) Inner: claim the subscription lease (shard 0). CLAIMED rotates/confirms
		// the (gen, wake) fence; BUSY/NOSUB grant nothing. Every worker races this
		// every cycle, so the inner fence has genuine contenders.
		callNs = rec.now()
		res, cerr := store.Claim(subID, replica, fmt.Sprintf("%s-wk-%d", replica, cycle), time.Now(), leaseTTLMs)
		if cerr != nil {
			sleep(backoff)
			continue
		}
		rec.recordOp(wid, composedInput{
			sub: subLabel, slot: slotLabel, class: classInner, innerOp: opClaim, worker: replica,
		}, composedClaimOutput(res), callNs)
		if !res.Claimed {
			sleep(backoff)
			continue
		}
		grants++

		// (3) gcPause on ~every third grant, AFTER capturing the token (res) but
		// BEFORE acking: stall past BOTH leases so a same-(sub,slot) peer takes over —
		// rotating the subscription generation and/or transferring the slot — and this
		// worker resumes holding a now-STALE (gen, wake) token and/or a stale epoch.
		if grants%3 == 0 {
			sleep(gcPause(time.Duration(leaseTTLMs) * time.Millisecond))
			// Observe the owner verdict at the epoch we believe we hold: a deposed
			// worker records FENCED, a still-current one OWNER. Read-only, no state.
			callNs = rec.now()
			if chk, ckerr := store.CheckOwner(slotKey, replica, heldEpoch); ckerr == nil {
				rec.recordOp(wid, composedInput{
					sub: subLabel, slot: slotLabel, class: classOuter, outerOp: opCheckOwner, caller: replica, reqEpoch: reqEpoch,
				}, composedCheckOwnerOutput(chk), callNs)
			}
		}

		// (4) Inner ack(done) under the owner scope, using the token captured in (2).
		// ack.lua checks owner_fenced FIRST (outer), then the (gen, wake) fence
		// (inner). A worker deposed on EITHER layer during the pause is FENCED; a live
		// worker is OK. Recorded as the inner op, carrying the presented epoch.
		callNs = rec.now()
		st, aerr := store.AckShard(subID, 0, res.Generation, res.WakeID, res.Generation, true, nil, time.Now(), leaseTTLMs, owner)
		if aerr != nil {
			sleep(backoff)
			continue
		}
		rec.recordOp(wid, composedInput{
			sub: subLabel, slot: slotLabel, class: classInner, innerOp: opAck, worker: replica,
			reqGen: res.Generation, reqWake: res.WakeID, tokenGen: res.Generation, reqEpoch: reqEpoch, done: true,
		}, composedAckOutput(st), callNs)

		sleep(backoff)
	}
}

// composedClaimShardOutput maps a webhook.SlotClaim to the composed model's outer
// output. On BUSY the model ignores owner/epoch (a legal no-op), but the observed
// foreign owner is recorded for a readable counterexample.
func composedClaimShardOutput(c webhook.SlotClaim, me string) composedOutput {
	switch c.Status {
	case webhook.SlotClaimed:
		return composedOutput{status: statusClaimed, owner: me, epoch: c.Epoch.Value()}
	case webhook.SlotRenewed:
		return composedOutput{status: statusRenewed, owner: me, epoch: c.Epoch.Value()}
	default:
		return composedOutput{status: statusBusy, owner: c.Owner.String(), epoch: c.Epoch.Value()}
	}
}

// composedCheckOwnerOutput maps a webhook.OwnerCheck to the composed model's outer
// output (the read-only deposed-owner verdict).
func composedCheckOwnerOutput(c webhook.OwnerCheck) composedOutput {
	switch c {
	case webhook.OwnerCheckOwner:
		return composedOutput{status: statusOwner}
	case webhook.OwnerCheckUnowned:
		return composedOutput{status: statusUnowned}
	default:
		return composedOutput{status: statusFenced}
	}
}

// composedClaimOutput maps a webhook.ClaimResult to the composed model's inner
// output. CLAIMED carries the minted (gen, wake); BUSY/NOSUB grant nothing.
func composedClaimOutput(r webhook.ClaimResult) composedOutput {
	switch {
	case r.Claimed:
		return composedOutput{status: statusClaimed, gen: r.Generation, wake: r.WakeID}
	case r.NoSub:
		return composedOutput{status: statusNoSub}
	default:
		return composedOutput{status: statusBusy}
	}
}

// composedAckOutput maps an ack.lua reply string to the composed model's inner
// output. A FENCED is the same observed no-op whether the inner (gen, wake) fence
// or the inlined owner_fenced fired — the model treats it identically.
func composedAckOutput(reply string) composedOutput {
	switch reply {
	case "OK":
		return composedOutput{status: statusOK}
	case "FENCED":
		return composedOutput{status: statusFenced}
	case "NOSUB":
		return composedOutput{status: statusNoSub}
	default:
		return composedOutput{status: reply}
	}
}
