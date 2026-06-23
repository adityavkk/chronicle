package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// These tests exercise the pure composed two-fence model (model_composed.go)
// directly against crafted histories — no cluster, no Redis. They are the oracle's
// OWN spec: the proof that it accepts every legal interleaving of the two-fence
// protocol and rejects the violations the composition exists to catch. With no
// timeout, CheckOperations is definitive: true == linearizable (Ok), false == a
// violation (Illegal). The binding to the shipped Lua is the -scenario composed
// gate (runComposedFences in scenario_composed.go).
//
// The whole point of these cases is the INTERACTION the single-layer models miss:
// an owner transfer that fences a deposed worker via the OUTER register while the
// INNER (gen, wake) register is still current, and the proof that the inner fence
// alone carries single-holder safety REGARDLESS of the owner verdict (INV-OWNER-02
// owner-epoch-is-optimization-only).

// Composed-history operation builder.
func composedOp(client int, in composedInput, out composedOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

// Inner-fence (gen, wake) op builders. reqEpoch carries the owner epoch the caller
// presented on a slot-scoped ack/release (the inlined owner_fenced ARGV); it is
// recorded for witness readability and is NOT consulted to justify an OK.
func cArm(sub, slot string) composedInput {
	return composedInput{sub: sub, slot: slot, class: classInner, innerOp: opArm}
}
func cClaim(sub, slot, w string) composedInput {
	return composedInput{sub: sub, slot: slot, class: classInner, innerOp: opClaim, worker: w}
}
func cAck(sub, slot, w string, gen int64, wake string, epoch int64, done bool) composedInput {
	return composedInput{
		sub: sub, slot: slot, class: classInner, innerOp: opAck, worker: w,
		reqGen: gen, reqWake: wake, tokenGen: gen, reqEpoch: epoch, done: done,
	}
}
func cRelease(sub, slot, w string, gen int64, wake string, epoch int64) composedInput {
	return composedInput{
		sub: sub, slot: slot, class: classInner, innerOp: opRelease, worker: w,
		reqGen: gen, reqWake: wake, tokenGen: gen, reqEpoch: epoch,
	}
}

// Outer-fence (owner, epoch) op builders.
func cClaimShard(sub, slot, caller string) composedInput {
	return composedInput{sub: sub, slot: slot, class: classOuter, outerOp: opClaimShard, caller: caller}
}
func cCheckOwner(sub, slot, caller string, epoch int64) composedInput {
	return composedInput{sub: sub, slot: slot, class: classOuter, outerOp: opCheckOwner, caller: caller, reqEpoch: epoch}
}

// Output builders.
func cArmedOut(gen int64, wake string) composedOutput {
	return composedOutput{status: statusArmed, gen: gen, wake: wake}
}
func cClaimedOut(gen int64, wake string) composedOutput {
	return composedOutput{status: statusClaimed, gen: gen, wake: wake}
}
func cOKOut() composedOutput     { return composedOutput{status: statusOK} }
func cFencedOut() composedOutput { return composedOutput{status: statusFenced} }
func cBusyOut() composedOutput   { return composedOutput{status: statusBusy} }

func cClaimedShardOut(owner string, epoch int64) composedOutput {
	return composedOutput{status: statusClaimed, owner: owner, epoch: epoch}
}
func cRenewedShardOut(owner string, epoch int64) composedOutput {
	return composedOutput{status: statusRenewed, owner: owner, epoch: epoch}
}
func cOwnerOut() composedOutput   { return composedOutput{status: statusOwner} }
func cUnownedOut() composedOutput { return composedOutput{status: statusUnowned} }

func checkComposedLinearizable(t *testing.T, name string, h []porcupine.Operation, want bool) {
	t.Helper()
	got := porcupine.CheckOperations(composedModel(), h)
	if got != want {
		t.Fatalf("%s: CheckOperations = %v, want %v", name, got, want)
	}
}

// THE canonical two-fence interleaving (the acceptance-criteria legal history):
// replica A owns the slot (epoch 1) and claims the subscription's lease (gen 1).
// A transfer hands the slot to B (epoch 2) — the OUTER register advances — while
// the INNER (gen, wake) fence is STILL current (A never released, no claim
// rotated it). A's slot-scoped ack is then FENCED by the OWNER register (its epoch
// 1 is stale), even though its (gen, wake) token is still inner-valid. B, now the
// owner at epoch 2, acks under the SAME live inner fence and succeeds. This is the
// composition both single-layer models miss: model_fence.go would call A's ack a
// valid OK (the inner fence is current); model_shard.go never sees the (gen, wake)
// ack at all. The composed model accepts it because a FENCED is an unconditional
// no-op AND B's OK is inner-fence-valid.
func TestComposedModel_OwnerTransferFencesDeposedAck(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(0, cCheckOwner("s", "h0", "A", 1), cOwnerOut(), 5, 6),
		// B transfers the SLOT (outer epoch 1 -> 2); the inner (gen, wake) fence is
		// untouched — owner transfer is orthogonal to the lease fence.
		composedOp(1, cClaimShard("s", "h0", "B"), cClaimedShardOut("B", 2), 7, 8),
		// A's ack: inner token (gen 1, wA) is STILL current, but A's owner epoch 1 is
		// stale, so the inlined owner_fenced fires FIRST -> FENCED. A legal no-op.
		composedOp(0, cAck("s", "h0", "A", 1, "wA", 1, true), cFencedOut(), 9, 10),
		// B, the new owner at epoch 2, acks done under the live inner fence -> OK.
		// The inner fence justifies the OK on (gen 1, wA) ALONE; the owner register
		// only permitted it to reach the inner check.
		composedOp(1, cAck("s", "h0", "B", 1, "wA", 2, true), cOKOut(), 11, 12),
	}
	checkComposedLinearizable(t, "owner transfer fences deposed ack", h, true)
}

// The same shape, but the OUTER register also exposes the deposed-owner gate via
// check_owner: after B's takeover, A's check_owner at epoch 1 is FENCED and B's at
// epoch 2 is OWNER — while the inner lease fence stays at gen 1 the whole time.
// Proves the two registers advance independently in one history.
func TestComposedModel_BothFencesLiveTogether(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(1, cClaimShard("s", "h0", "B"), cClaimedShardOut("B", 2), 5, 6),
		composedOp(0, cCheckOwner("s", "h0", "A", 1), cFencedOut(), 7, 8),       // deposed owner fenced (outer)
		composedOp(1, cCheckOwner("s", "h0", "B", 2), cOwnerOut(), 9, 10),       // new owner ok (outer)
		composedOp(1, cAck("s", "h0", "B", 1, "wA", 2, true), cOKOut(), 11, 12), // inner fence still gen 1
	}
	checkComposedLinearizable(t, "both fences live together", h, true)
}

// THE load-bearing negative (the acceptance-criteria illegal history): an OK the
// INNER fence cannot justify. A holds (gen 1, wA); B takes over the lease, rotating
// the inner fence to gen 2 (wB). A's later ack carries the STALE inner token
// (gen 1, wA) yet returns OK. No owner-epoch state can rescue it: INV-OWNER-02 says
// the owner fence may only turn a legal OK into a FENCED, never make a
// (gen,wake)-stale write pass. The composed model MUST reject it — this is the
// single-holder violation (two holders' tokens both accepted) the composition
// exists to keep catchable even with the owner layer live.
func TestComposedModel_StaleInnerOKIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(1, cClaim("s", "h0", "A"), cClaimedOut(2, "wB"), 5, 6), // lease takeover rotates 1 -> 2
		// A's stale ack returns OK under (gen 1, wA): the inner fence is gen 2 now.
		// Owner epoch is current (1) — irrelevant: the inner fence alone rejects.
		composedOp(0, cAck("s", "h0", "A", 1, "wA", 1, true), cOKOut(), 7, 8),
	}
	checkComposedLinearizable(t, "stale inner OK", h, false)
}

// The crucial INV-OWNER-02 corner: an OK that would have been caught by the OWNER
// fence (the acking caller's epoch is stale) but that the model must STILL reject
// on the INNER fence — proving the OK branch consults the inner register ALONE and
// does NOT lean on owner-epoch for safety. Here both fences are stale, but the
// rejection is attributed to the inner fence: even if we imagined the owner layer
// switched off, the OK is unjustifiable. (Operationally the live server fences this
// on the owner gate first; the model proves safety does not DEPEND on that.)
func TestComposedModel_OKUnjustifiableUnderInnerEvenIfOwnerWouldCatch(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(1, cClaimShard("s", "h0", "B"), cClaimedShardOut("B", 2), 5, 6), // owner 1 -> 2
		composedOp(1, cClaim("s", "h0", "B"), cClaimedOut(2, "wB"), 7, 8),          // inner 1 -> 2
		// A acks with BOTH a stale inner token (gen 1) AND a stale owner epoch (1),
		// yet OK. The model rejects on the INNER fence regardless of the owner state.
		composedOp(0, cAck("s", "h0", "A", 1, "wA", 1, true), cOKOut(), 9, 10),
	}
	checkComposedLinearizable(t, "OK unjustifiable under inner even if owner would catch", h, false)
}

// A FENCED is an unconditional legal no-op whether it was the inner OR the owner
// fence firing — it grants nothing, so it can never be half of a two-holder
// violation. A history of nothing but a deposed ack FENCED (owner stale, inner
// current) plus the live holder's OK must linearize.
func TestComposedModel_FencedIsUnconditionalNoop(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cArm("s", "h0"), cArmedOut(1, "w1"), 3, 4),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "w1"), 5, 6), // coalesce onto the armed wake
		composedOp(1, cClaimShard("s", "h0", "B"), cClaimedShardOut("B", 2), 7, 8),
		composedOp(0, cAck("s", "h0", "A", 1, "w1", 1, false), cFencedOut(), 9, 10), // owner-fenced no-op
		composedOp(1, cAck("s", "h0", "B", 1, "w1", 2, true), cOKOut(), 11, 12),     // live inner holder OK
	}
	checkComposedLinearizable(t, "FENCED is unconditional no-op", h, true)
}

// An owner transfer that REUSES the epoch is still the silently-dropping LWW the
// OUTER register must reject, EVEN in the composed model — the inner fence does not
// mask an outer-register violation. Proves the composition did not weaken the shard
// model's CAS check.
func TestComposedModel_OuterNonBumpingTransferStillIllegal(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(1, cClaimShard("s", "h0", "B"), cClaimedShardOut("B", 1), 5, 6), // BUG: reused epoch 1
	}
	checkComposedLinearizable(t, "outer non-bumping transfer", h, false)
}

// Renew keeps the epoch; the inner lease heartbeats under a live fence; both
// registers stay coherent. A long legal interleaving exercising arm/claim/ack on
// the inner side and claim_shard renew on the outer side together.
func TestComposedModel_RenewAndHeartbeatTogether(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cArm("s", "h0"), cArmedOut(1, "w1"), 3, 4),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "w1"), 5, 6),
		composedOp(0, cClaimShard("s", "h0", "A"), cRenewedShardOut("A", 1), 7, 8), // renew keeps epoch 1
		composedOp(0, cAck("s", "h0", "A", 1, "w1", 1, false), cOKOut(), 9, 10),    // heartbeat under live fence
		composedOp(0, cAck("s", "h0", "A", 1, "w1", 1, true), cOKOut(), 11, 12),    // done: lease drops
	}
	checkComposedLinearizable(t, "renew and heartbeat together", h, true)
}

// A release under the owner scope is the other mutating inner op gated by both
// fences: the live holder releases (lease drops, wake clears) and then a deposed
// peer's release with a stale inner token is FENCED. The release path's OK is
// inner-fence-valid just like ack's, and the FENCED is the unconditional no-op.
func TestComposedModel_ReleaseUnderOwnerScope(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(0, cRelease("s", "h0", "A", 1, "wA", 1), cOKOut(), 5, 6), // live holder releases -> idle
		composedOp(1, cClaim("s", "h0", "B"), cClaimedOut(2, "wB"), 7, 8),   // B claims fresh fence gen 2
		// A's late release with its stale (gen 1, wA) token is FENCED — a no-op.
		composedOp(0, cRelease("s", "h0", "A", 1, "wA", 1), cFencedOut(), 9, 10),
	}
	checkComposedLinearizable(t, "release under owner scope", h, true)
}

// Two distinct (sub, slot) pairs are checked in isolation by the partitioner: a
// violation on one is caught even when the other is clean, and the per-key search
// stays small. Confirms partitionByComposed keys on BOTH sub and slot.
func TestComposedModel_PartitionsBySubAndSlot(t *testing.T) {
	clean := []porcupine.Operation{
		composedOp(0, cClaimShard("s1", "h0", "A"), cClaimedShardOut("A", 1), 1, 2),
		composedOp(0, cClaim("s1", "h0", "A"), cClaimedOut(1, "wA"), 3, 4),
		composedOp(0, cAck("s1", "h0", "A", 1, "wA", 1, true), cOKOut(), 5, 6),
	}
	checkComposedLinearizable(t, "two clean keys", append(append([]porcupine.Operation{}, clean...),
		composedOp(1, cClaimShard("s2", "h1", "B"), cClaimedShardOut("B", 1), 7, 8),
		composedOp(1, cClaim("s2", "h1", "B"), cClaimedOut(1, "wB"), 9, 10),
		composedOp(1, cAck("s2", "h1", "B", 1, "wB", 1, true), cOKOut(), 11, 12),
	), true)

	checkComposedLinearizable(t, "violation isolated to one key", append(append([]porcupine.Operation{}, clean...),
		composedOp(1, cClaimShard("s2", "h1", "B"), cClaimedShardOut("B", 1), 7, 8),
		composedOp(1, cClaim("s2", "h1", "B"), cClaimedOut(1, "wB"), 9, 10),
		composedOp(2, cClaim("s2", "h1", "B"), cClaimedOut(2, "wB2"), 11, 12),    // inner rotates 1 -> 2
		composedOp(1, cAck("s2", "h1", "B", 1, "wB", 1, true), cOKOut(), 13, 14), // BUG: stale inner OK
	), false)
}

// A BUSY claim and an UNOWNED check are legal no-ops in the composed model just as
// in the parents — neither grants anything in either register.
func TestComposedModel_BusyAndUnownedAreNoops(t *testing.T) {
	h := []porcupine.Operation{
		composedOp(0, cClaimShard("s", "h0", "A"), cClaimedShardOut("A", 1), 1, 8),
		composedOp(1, cClaimShard("s", "h0", "B"), cBusyShardOut(), 2, 3),
		composedOp(1, cCheckOwner("s", "h0", "B", 0), cFencedOut(), 4, 5),
		composedOp(2, cCheckOwner("s", "h0", "C", 0), cUnownedOut(), 6, 7), // never-claimed view (concurrent)
		composedOp(0, cClaim("s", "h0", "A"), cClaimedOut(1, "wA"), 9, 10),
		composedOp(0, cClaim("s", "h0", "A"), cBusyOut(), 11, 12), // second claim races BUSY: no-op
	}
	checkComposedLinearizable(t, "busy and unowned are no-ops", h, true)
}

func cBusyShardOut() composedOutput { return composedOutput{status: statusBusy} }
