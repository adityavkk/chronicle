package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// model_composed.go is the PURE CORE of the TWO-FENCE COMPOSITION test (P2.2,
// issue #36): a single sequential register carrying BOTH the inner (gen, wake)
// lease fence (model_fence.go) AND the outer (owner, epoch) slot-ownership fence
// (model_shard.go), so the linearizability search sees the INTERACTION between
// the two layers that each single-layer model misses in isolation. Like its two
// parents it has no I/O, no clock, and no dependency on the package under test —
// an independent oracle, deterministic and unit-testable (model_composed_test.go).
//
// WHY A COMPOSED MODEL. A mutating write (ack/release/arm) is authorized in the
// live system only when BOTH owner_fenced(slot, me, epoch) AND fenced(gen, wake)
// pass (common.lua: owner_fenced is the script's FIRST gate, inlined above the
// still-byte-for-byte (gen,wake) fence). model_fence.go checks the inner fence
// with the owner register absent; model_shard.go checks the outer with the
// (gen,wake) register absent. Neither can see a write authorized by one fence but
// stale under the other, nor a deposed owner whose ack is fenced by the (gen,wake)
// register rather than the owner register. This model carries both registers in
// one state so porcupine reasons over the composition.
//
// THE LAYERING CLAIM (INV-OWNER-02), encoded as an assertion. The owner-epoch
// fence is OPTIMIZATION-ONLY: it suppresses a deposed owner's wasted work, but it
// is NEVER a correctness dependency. The single-holder safety (INV-FENCE-01) rests
// on the inner (gen, wake) fence ALONE. So the load-bearing invariant the composed
// step asserts is:
//
//	every server OK is fence-valid under the inner (gen, wake) register,
//	INDEPENDENT of the owner-epoch verdict.
//
// Owner-epoch may only convert an otherwise-legal OK into a FENCED no-op (turn a
// safe write off), NEVER the reverse (it can never make a (gen,wake)-stale write
// pass). Concretely: the OK branch checks ONLY the inner fence — the owner
// register is deliberately NOT consulted to justify an OK. If the inner fence is
// stale, an OK is rejected whether or not the owner fence would also have caught
// it; that is exactly the proof that the inner fence is the sole safety boundary
// and the owner fence is layered above it as a pure optimization. A FENCED is an
// unconditional legal no-op (it could be the inner fence OR the owner fence
// firing; either way nothing was granted), so it can never be half of a
// two-holder violation. This is the same single-gate discipline model_fence.go
// uses, now proven to hold ACROSS owner-epoch transitions.
//
// THE OWNER REGISTER, checked as in model_shard.go. claim_shard transfers/renews
// the (owner, epoch) CAS exactly as the shard model checks: a CLAIMED bumps the
// epoch strictly upward (fencing the prior owner); a RENEWED keeps it; a BUSY
// grants nothing. check_owner's OWNER verdict is the strict deposed-owner gate.
// These transitions move the owner half of the composed state WITHOUT touching the
// (gen, wake) half — the two registers are orthogonal, which is the whole point:
// an owner transfer must not perturb the inner fence's single-holder algebra.
//
// Time is absent, exactly as in both parents: grant-vs-BUSY and the owner lease
// clock are observed outputs, never modeled; the model verifies only the time-free
// generation algebra (inner) and epoch algebra (outer).

// Composed operation kinds. The inner-fence ops (arm/claim/ack/release) reuse the
// fence model's opKind vocabulary; the outer-fence ops (claim_shard/check_owner)
// reuse the shard model's. We tag each composed op with which register it drives.
type composedOpClass int

const (
	classInner composedOpClass = iota // (gen, wake): arm | claim | ack | release
	classOuter                        // (owner, epoch): claim_shard | check_owner
)

// composedInput is the model input. class selects which register the op drives;
// the embedded inner/outer fields carry that register's request shape (mirroring
// fenceInput / shardInput so the two parents' transition logic is reused verbatim).
type composedInput struct {
	sub   string // partition key part 1: the subscription whose (gen,wake) fence
	slot  string // partition key part 2: the slot whose (owner,epoch) lease
	class composedOpClass

	// Inner (gen, wake) request fields, set when class == classInner.
	innerOp  opKind
	worker   string
	reqGen   int64
	reqWake  string
	tokenGen int64
	done     bool

	// Outer (owner, epoch) request fields, set when class == classOuter.
	outerOp  shardOpKind
	caller   string
	reqEpoch int64
}

// composedOutput is the observed server reply. For an inner op it carries the
// (status, gen, wake) of fenceOutput; for an outer op the (status, owner, epoch)
// of shardOutput. status uses the shared status constants.
type composedOutput struct {
	status string
	gen    int64
	wake   string
	owner  string
	epoch  int64
}

// composedState is the sequential model state for one (sub, slot): BOTH the inner
// fence register (gen, wake, phase) — checked by ack.lua's fenced() — AND the
// outer ownership register (owner, epoch) — checked by check_owner.lua / the
// inlined owner_fenced(). Carrying both in one state is what lets porcupine see
// the two-fence interaction. The two halves are orthogonal: an inner op never
// reads or writes (owner, epoch), and an outer op never reads or writes
// (gen, wake, phase) — that orthogonality IS the optimization-only claim.
type composedState struct {
	// Inner (gen, wake) lease fence — identical fields to fenceState.
	gen   int64
	wake  string
	phase string
	// Outer (owner, epoch) slot-ownership fence — identical fields to shardState.
	owner string // "" = unowned
	epoch int64  // current owner_epoch; 0 = none granted
}

// composedModel is the porcupine model for the two-fence composition, partitioned
// per (sub, slot) so the linearizability search stays per-key.
func composedModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByComposed,
		Init: func() interface{} {
			// No lease and no owner yet: inner fence below any minted generation
			// (every grant HINCRBYs to >= 0, so the first rotation advances), outer
			// register unowned at epoch 0 (claim_shard HINCRBYs to >= 1 on first
			// transfer, so the first claim advances).
			return composedState{gen: -1, wake: "", phase: phaseIdle, owner: "", epoch: 0}
		},
		Step:              composedStep,
		Equal:             func(a, b interface{}) bool { return a.(composedState) == b.(composedState) },
		DescribeOperation: describeComposedOp,
		DescribeState:     describeComposedState,
	}
}

// composedStep is the pure transition over the composed register. It dispatches by
// op class, then — for an inner op — reuses the fence model's transition logic
// over the (gen, wake, phase) half while leaving (owner, epoch) untouched, and for
// an outer op reuses the shard model's transition logic over the (owner, epoch)
// half while leaving (gen, wake, phase) untouched. It never mutates its arguments.
func composedStep(state, input, output interface{}) (bool, interface{}) {
	s := state.(composedState)
	in := input.(composedInput)
	out := output.(composedOutput)

	if in.class == classOuter {
		return composedOuterStep(s, in, out)
	}
	return composedInnerStep(s, in, out)
}

// composedInnerStep advances the inner (gen, wake, phase) half over an
// arm/claim/ack/release, reusing the EXACT fence model transitions (stepArm /
// stepClaim / stepAckOrRelease) so the inner-fence algebra is byte-for-byte the
// single-layer model. The owner half is carried through unchanged: an inner op is
// orthogonal to (owner, epoch).
//
// THE LAYERING ASSERTION lives in stepAckOrRelease's OK branch: an accepted
// ack/release must carry the CURRENT inner fence, judged by checkerFenced over
// (gen, wake) ALONE — the (owner, epoch) register is NOT consulted to justify the
// OK. That is the encoding of INV-OWNER-02: owner-epoch can only turn a legal OK
// into a FENCED no-op (a separate observed output, an unconditional no-op here),
// never make a (gen,wake)-stale write pass. So this step proves the inner
// SingleHolder property holds REGARDLESS of the owner fence's verdict.
func composedInnerStep(s composedState, in composedInput, out composedOutput) (bool, composedState) {
	inner := fenceState{gen: s.gen, wake: s.wake, phase: s.phase}
	fin := fenceInput{
		sub: in.sub, op: in.innerOp, worker: in.worker,
		reqGen: in.reqGen, reqWake: in.reqWake, tokenGen: in.tokenGen, done: in.done,
	}
	fout := fenceOutput{status: out.status, gen: out.gen, wake: out.wake}

	var ok bool
	var next fenceState
	switch in.innerOp {
	case opArm:
		ok, next = stepArm(inner, fout)
	case opClaim:
		ok, next = stepClaim(inner, fout)
	case opAck, opRelease:
		ok, next = stepAckOrRelease(inner, fin, fout)
	default:
		return false, s
	}
	// Fold the advanced inner half back into the composed state, leaving the owner
	// half (owner, epoch) untouched — the orthogonality that makes owner-epoch an
	// optimization, not a correctness input.
	return ok, composedState{gen: next.gen, wake: next.wake, phase: next.phase, owner: s.owner, epoch: s.epoch}
}

// composedOuterStep advances the outer (owner, epoch) half over a
// claim_shard/check_owner, reusing the EXACT shard model transitions
// (stepClaimShard / stepCheckOwner). The inner half is carried through unchanged:
// an owner transfer/renew must not perturb the (gen, wake) single-holder algebra.
func composedOuterStep(s composedState, in composedInput, out composedOutput) (bool, composedState) {
	outer := shardState{owner: s.owner, epoch: s.epoch}
	sin := shardInput{shard: in.slot, op: in.outerOp, caller: in.caller, reqEpoch: in.reqEpoch}
	sout := shardOutput{status: out.status, owner: out.owner, epoch: out.epoch}

	var ok bool
	var next shardState
	switch in.outerOp {
	case opClaimShard:
		ok, next = stepClaimShard(outer, sin, sout)
	case opCheckOwner:
		ok, next = stepCheckOwner(outer, sin, sout)
	default:
		return false, s
	}
	return ok, composedState{gen: s.gen, wake: s.wake, phase: s.phase, owner: next.owner, epoch: next.epoch}
}

// partitionByComposed groups a history by (sub, slot) so each composed register is
// checked independently — keeping the per-key search modest (07's gap #1). A
// subscription's (gen, wake) fence and its slot's (owner, epoch) lease are checked
// together precisely BECAUSE the composition is the point; different (sub, slot)
// pairs are independent.
func partitionByComposed(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	order := []string{}
	for _, o := range history {
		in := o.Input.(composedInput)
		key := in.sub + "\x00" + in.slot
		if _, seen := byKey[key]; !seen {
			order = append(order, key)
		}
		byKey[key] = append(byKey[key], o)
	}
	parts := make([][]porcupine.Operation, 0, len(order))
	for _, key := range order {
		parts = append(parts, byKey[key])
	}
	return parts
}

// describeComposedOp renders one operation for a counterexample timeline, reusing
// the parents' phrasing for whichever register the op drove.
func describeComposedOp(input, output interface{}) string {
	in := input.(composedInput)
	out := output.(composedOutput)
	if in.class == classOuter {
		switch in.outerOp {
		case opClaimShard:
			if out.status == statusClaimed || out.status == statusRenewed {
				return fmt.Sprintf("%s claim_shard(%s) -> %s(owner=%s,epoch=%d)", in.caller, in.slot, out.status, in.caller, out.epoch)
			}
			return fmt.Sprintf("%s claim_shard(%s) -> %s", in.caller, in.slot, out.status)
		case opCheckOwner:
			return fmt.Sprintf("%s check_owner(%s,epoch=%d) -> %s", in.caller, in.slot, in.reqEpoch, out.status)
		}
		return "?outer"
	}
	switch in.innerOp {
	case opArm:
		if out.status == statusArmed {
			return fmt.Sprintf("arm -> ARMED(gen=%d,wake=%s)", out.gen, short(out.wake))
		}
		return fmt.Sprintf("arm -> %s", out.status)
	case opClaim:
		if out.status == statusClaimed {
			return fmt.Sprintf("%s claim -> CLAIMED(gen=%d,wake=%s)", in.worker, out.gen, short(out.wake))
		}
		return fmt.Sprintf("%s claim -> %s", in.worker, out.status)
	case opAck:
		verb := "ack"
		if in.done {
			verb = "ack(done)"
		}
		return fmt.Sprintf("%s %s[gen=%d,wake=%s,epoch=%d] -> %s", in.worker, verb, in.reqGen, short(in.reqWake), in.reqEpoch, out.status)
	case opRelease:
		return fmt.Sprintf("%s release[gen=%d,wake=%s,epoch=%d] -> %s", in.worker, in.reqGen, short(in.reqWake), in.reqEpoch, out.status)
	}
	return "?inner"
}

// describeComposedState renders the full composed register for a counterexample
// timeline: both fences side by side, so a witness shows which half was stale.
func describeComposedState(state interface{}) string {
	s := state.(composedState)
	owner := s.owner
	if owner == "" {
		owner = "<none>"
	}
	return fmt.Sprintf("{gen=%d wake=%s phase=%s | owner=%s epoch=%d}", s.gen, short(s.wake), s.phase, owner, s.epoch)
}
