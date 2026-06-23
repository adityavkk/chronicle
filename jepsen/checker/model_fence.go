package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// model_fence.go is the PURE CORE of the single-holder safety test (T1 in
// docs/specs/horizontal-scale/research/07): a sequential model of one
// subscription's lease fence, checked for linearizability by porcupine. It has
// no I/O, no clock, and no dependency on the package under test — it is an
// independent oracle, deterministic and unit-testable in isolation
// (model_fence_test.go). The imperative shell that drives a live cluster and
// feeds histories into it lives in scenario_lease.go.
//
// The modeling insight (07): a *wake* is not a linearizable read or write, but
// the LEASE FENCE is. chronicle's correctness rests on a monotonic generation
// token (Kleppmann's fencing token): ack.lua accepts a callback only when the
// token's (generation, wake_id) equals the subscription's current pair, and
// claim.lua rotates that pair to a strictly greater generation whenever it grants
// a lease to anyone but the original waiter. So "at most one worker holds a token
// ack.lua will accept" reduces to an invariant over a (generation, wake_id)
// register — exactly the shape porcupine checks.
//
// The single-holder guarantee is enforced across two transitions, not one:
//   - claim: every grant to a NEW holder rotates the generation strictly upward,
//     fencing whoever held it before; only the original waiter of an in-flight
//     wake coalesces onto the existing fence.
//   - ack:   an accepted callback must carry the CURRENT fence; a deposed
//     holder's ack is stale and must be FENCED.
// A history that breaks either — a takeover that reuses the generation, or a
// stale ack that returns OK — has no valid linearization, so porcupine reports
// the witness. This is the etcd-lock bug class (Jepsen found etcd double-granted
// a lock and lost ~18% of acked updates), modeled directly.
//
// Time is deliberately absent. Lease expiry governs only whether a claim is
// granted or returns BUSY, which depends on the server's clock; the model treats
// that grant/BUSY split as an observed output and verifies only the time-free
// generation algebra (rotate => strictly greater generation; coalesce =>
// identical). This is faithful to the research's central claim: the fence, not
// the lease TTL, is the safety boundary, and clock skew can corrupt a TTL but
// never a monotonic generation.
//
// One consequence of leaving time out: the server clears an expired lease's
// wake_id (expire_lease.lua, fired by the manager's lease worker) WITHOUT
// rotating the generation, and that is a server-side event with no client
// operation — invisible to this client-op history. So the model can believe a
// holder's (gen, wake) is still current after the server has quietly cleared it,
// and a deposed holder's later ack is then FENCED by the server even though the
// model would compute it valid. The model absorbs this by treating every FENCED
// as a legal no-op (see stepAckOrRelease): a FENCED grants nothing and mutates
// nothing, so it can never be half of a two-holder violation, and the model
// re-syncs to the server on the next observed grant.

// Fence phases mirror webhook.Phase. They are redeclared here (rather than
// imported) so the harness stays an independent oracle, as the existing client
// shapes in main.go already are.
const (
	phaseIdle   = "idle"
	phaseWaking = "waking"
	phaseLive   = "live"
)

// Observed operation statuses, mirroring the Lua replies (arm_wake.lua,
// claim.lua, ack.lua, release.lua) and the wire error codes (webhook/types.go).
const (
	statusArmed   = "ARMED"
	statusClaimed = "CLAIMED"
	statusBusy    = "BUSY"
	statusOK      = "OK"
	statusFenced  = "FENCED"
	statusNoSub   = "NOSUB"
)

// opKind is the operation recorded into a history.
type opKind int

const (
	opArm     opKind = iota // server-side arm of a wake          -> ARMED | BUSY | NOSUB
	opClaim                 // POST .../claim (pull-wake)          -> CLAIMED | BUSY | NOSUB
	opAck                   // POST .../ack                        -> OK | FENCED | NOSUB
	opRelease               // POST .../release                   -> OK | FENCED | NOSUB
)

// fenceInput is the model input: which subscription, which operation, and the
// fence token the request carries. A claim/arm sets only sub/op/worker; an
// ack/release sets the (reqGen, reqWake, tokenGen, done) fence fields the server
// checks.
type fenceInput struct {
	sub      string
	op       opKind
	worker   string
	reqGen   int64
	reqWake  string
	tokenGen int64
	done     bool
}

// fenceOutput is the model output: the server's observed reply. status is always
// set; gen and wake carry the minted fence on an ARMED or CLAIMED.
type fenceOutput struct {
	status string
	gen    int64
	wake   string
}

// fenceState is the sequential model state for one subscription: the fence
// register (gen, wake) that ack.lua compares against, plus the phase that decides
// claim.lua's rotate-vs-coalesce branch. The lease holder is deliberately NOT
// part of the state — it never changes a future transition's outcome, and a
// minimal state keeps porcupine's (NP-hard) search small (07's gap #1).
type fenceState struct {
	gen   int64
	wake  string
	phase string
}

// leaseModel is the porcupine model for T1, partitioned per subscription so the
// linearizability search stays per-key.
func leaseModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionBySub,
		Init: func() interface{} {
			// No lease has ever been granted: phase idle, no wake, and a
			// generation strictly below any the server can mint (every grant
			// HINCRBYs to >= 0), so the first rotation always advances.
			return fenceState{gen: -1, wake: "", phase: phaseIdle}
		},
		Step:              leaseStep,
		Equal:             func(a, b interface{}) bool { return a.(fenceState) == b.(fenceState) },
		DescribeOperation: describeFenceOp,
		DescribeState:     describeFenceState,
	}
}

// leaseStep is the heart of the model: given the current state and an observed
// (input, output), it returns whether the step is consistent with the fence
// invariant and the resulting state. It is a pure function — it never mutates its
// arguments — as porcupine requires.
func leaseStep(state, input, output interface{}) (bool, interface{}) {
	s := state.(fenceState)
	in := input.(fenceInput)
	out := output.(fenceOutput)

	switch in.op {
	case opArm:
		return stepArm(s, out)
	case opClaim:
		return stepClaim(s, out)
	case opAck, opRelease:
		return stepAckOrRelease(s, in, out)
	default:
		return false, s
	}
}

// stepArm models arm_wake.lua: a wake is armed only from idle, minting a fresh,
// strictly greater generation and a new wake_id and moving the phase to waking.
// BUSY (already waking/live) and NOSUB grant nothing and are legal no-ops.
func stepArm(s fenceState, out fenceOutput) (bool, fenceState) {
	if out.status != statusArmed {
		return true, s
	}
	// The safety boundary is the strictly-increasing generation, not wake-id
	// distinctness: arm_wake.lua sets wake_id to a caller-supplied value and never
	// requires it to differ from the prior one, so the model must not assert that.
	ok := s.phase == phaseIdle && out.gen > s.gen && out.wake != ""
	return ok, fenceState{gen: out.gen, wake: out.wake, phase: phaseWaking}
}

// stepClaim models claim.lua's compare-and-set grant. The rotate-vs-coalesce
// branch is decided purely by the pre-state (state.go's ClaimRotatesFence): the
// in-flight (gen, wake) is reused ONLY for the normal first claim of an
// already-issued wake (phase waking with a wake set); idle, a cleared wake, and
// taking over an expired live lease all rotate.
func stepClaim(s fenceState, out fenceOutput) (bool, fenceState) {
	if out.status != statusClaimed {
		// BUSY / NOSUB grant nothing and mutate no fence state, so they are legal
		// no-ops under any linearization. Whether a claim is granted or BUSY
		// depends on the server's lease clock, which the model does not track; we
		// verify only the time-free generation algebra on the grants.
		return true, s
	}
	if s.phase == phaseWaking && s.wake != "" {
		// Coalesce: two workers racing one wake event must collide on the same
		// fence, not each get a private one. The grant must reuse it exactly.
		ok := out.gen == s.gen && out.wake == s.wake
		return ok, fenceState{gen: s.gen, wake: s.wake, phase: phaseLive}
	}
	// Rotate: the fence must advance to a STRICTLY greater generation, fencing out
	// any prior holder (Kleppmann's monotonic token). Distinctness of the wake_id
	// is NOT a claim.lua invariant — it mints a caller-supplied wake_id with no
	// uniqueness check — so the monotone generation alone carries the safety.
	ok := out.gen > s.gen && out.wake != ""
	return ok, fenceState{gen: out.gen, wake: out.wake, phase: phaseLive}
}

// checkerFenced is the checker's INDEPENDENT copy of the fence predicate (the
// third mirror of webhook.FenceDecision / common.lua's `fenced`): a token is
// stale — and the op must be rejected — unless its generation, the request
// generation, and the request wake_id all match the current fence and the
// wake_id is non-empty. Like FenceDecision and `fenced` it is hand-maintained and
// must be changed together with the other two; the triple-mirror differential
// (predicate_mirror_test.go / webhook's predicate_differential_test.go) pins all
// three together over a generated domain. Extracted from the inline expression
// stepAckOrRelease used so the checker copy is a single reachable reference, not
// a buried literal a fourth transcription could drift from. [INV-FENCE-01/03]
func checkerFenced(curGen, reqGen, tokenGen int64, curWake, reqWake string) bool {
	return tokenGen != curGen || reqGen != curGen || reqWake == "" || reqWake != curWake
}

// stepAckOrRelease models ack.lua / release.lua, whose first act is the fence
// check (common.lua's `fenced`).
func stepAckOrRelease(s fenceState, in fenceInput, out fenceOutput) (bool, fenceState) {
	// The fence predicate, byte-for-byte the common.lua mirror: a token is stale
	// unless its generation, the request generation, and the request wake_id all
	// match the current fence (and the wake_id is non-empty).
	fenced := checkerFenced(s.gen, in.reqGen, in.tokenGen, s.wake, in.reqWake)

	switch out.status {
	case statusOK:
		// THE load-bearing safety assertion. An accepted ack/release MUST carry
		// the current fence. An OK under a stale token means two holders' tokens
		// were both accepted — the single-holder violation — and no linearization
		// can justify it, so porcupine surfaces the witness.
		if fenced {
			return false, s
		}
		if in.op == opRelease || in.done {
			// release / ack(done=true): the lease drops and the wake clears, so the
			// generation persists but no token matches until the next claim rotates
			// it (ack.lua / release.lua set wake_id='', phase=idle).
			return true, fenceState{gen: s.gen, wake: "", phase: phaseIdle}
		}
		// ack(done=false): a heartbeat extending the live lease; fence unchanged.
		return true, fenceState{gen: s.gen, wake: s.wake, phase: phaseLive}
	case statusFenced:
		// A FENCED is an unconditional legal no-op. It grants nothing and mutates
		// nothing, so it can never be half of a two-holder violation — the OK
		// branch above is the sole safety gate. We do NOT assert the token was
		// "genuinely stale", for two reasons: (1) the server clears an expired
		// lease's wake_id without rotating the generation (expire_lease.lua), a
		// server-side event with no client op, so the model can legitimately
		// believe a token is current when the server has already fenced it — see
		// the file header; and (2) catching a spurious FENCED (a live holder
		// wrongly rejected) is a LIVENESS defect — lost progress, not a broken
		// invariant — which the L-series covers, not this safety model.
		return true, s
	default: // NOSUB and anything else: no durable effect.
		return true, s
	}
}

// partitionBySub groups a history by subscription id so each subscription's fence
// is checked independently — keeping the per-key search modest (07's gap #1).
func partitionBySub(history []porcupine.Operation) [][]porcupine.Operation {
	bySub := map[string][]porcupine.Operation{}
	order := []string{}
	for _, o := range history {
		sub := o.Input.(fenceInput).sub
		if _, seen := bySub[sub]; !seen {
			order = append(order, sub)
		}
		bySub[sub] = append(bySub[sub], o)
	}
	parts := make([][]porcupine.Operation, 0, len(order))
	for _, sub := range order {
		parts = append(parts, bySub[sub])
	}
	return parts
}

// describeFenceOp renders one operation for a counterexample timeline.
func describeFenceOp(input, output interface{}) string {
	in := input.(fenceInput)
	out := output.(fenceOutput)
	switch in.op {
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
		return fmt.Sprintf("%s %s[gen=%d,wake=%s] -> %s", in.worker, verb, in.reqGen, short(in.reqWake), out.status)
	case opRelease:
		return fmt.Sprintf("%s release[gen=%d,wake=%s] -> %s", in.worker, in.reqGen, short(in.reqWake), out.status)
	}
	return "?"
}

// describeFenceState renders the fence register for a counterexample timeline.
func describeFenceState(state interface{}) string {
	s := state.(fenceState)
	return fmt.Sprintf("{gen=%d wake=%s phase=%s}", s.gen, short(s.wake), s.phase)
}
