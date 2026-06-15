package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// These tests exercise the pure fence model (model_fence.go) directly against
// crafted histories — no cluster, no Redis. They are the proof that the oracle
// accepts every legal interleaving of the fence protocol and rejects the
// single-holder violations it exists to catch. With no timeout, CheckOperations
// is definitive: true == linearizable (Ok), false == a violation (Illegal).
//
// Generations start at 1 to mirror the live system: create_sub.lua sets
// generation='0', and the first grant HINCRBYs to 1 — so 1 is the first
// generation a real run ever records.

// op is a terse history-operation builder for the tests.
func op(client int, in fenceInput, out fenceOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

func armIn(sub string) fenceInput      { return fenceInput{sub: sub, op: opArm} }
func claimIn(sub, w string) fenceInput { return fenceInput{sub: sub, op: opClaim, worker: w} }

func ackIn(sub, w string, gen int64, wake string, done bool) fenceInput {
	return fenceInput{sub: sub, op: opAck, worker: w, reqGen: gen, reqWake: wake, tokenGen: gen, done: done}
}

func releaseIn(sub, w string, gen int64, wake string) fenceInput {
	return fenceInput{sub: sub, op: opRelease, worker: w, reqGen: gen, reqWake: wake, tokenGen: gen}
}

func armedOut(gen int64, wake string) fenceOutput {
	return fenceOutput{status: statusArmed, gen: gen, wake: wake}
}

func claimedOut(gen int64, wake string) fenceOutput {
	return fenceOutput{status: statusClaimed, gen: gen, wake: wake}
}
func busyOut() fenceOutput   { return fenceOutput{status: statusBusy} }
func okOut() fenceOutput     { return fenceOutput{status: statusOK} }
func fencedOut() fenceOutput { return fenceOutput{status: statusFenced} }
func noSubOut() fenceOutput  { return fenceOutput{status: statusNoSub} }

func checkLinearizable(t *testing.T, name string, h []porcupine.Operation, want bool) {
	t.Helper()
	got := porcupine.CheckOperations(leaseModel(), h)
	if got != want {
		t.Fatalf("%s: CheckOperations = %v, want %v", name, got, want)
	}
}

// The canonical expired-lease takeover (the runExpiredLeaseTakeover sequence,
// now as a history): A holds, B takes over with a rotated fence, B acks, and A's
// late ack is FENCED. This is the property the whole test exists to generalize.
func TestLeaseModel_ValidTakeover(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "B"), claimedOut(2, "wB"), 3, 4), // takeover rotates 1 -> 2
		op(1, ackIn("s", "B", 2, "wB", true), okOut(), 5, 6),
		op(0, ackIn("s", "A", 1, "wA", true), fencedOut(), 7, 8), // deposed holder is fenced
	}
	checkLinearizable(t, "valid takeover", h, true)
}

// A takeover that REUSES the prior generation instead of rotating is the slice-2
// split-brain regression: the deposed holder's token stays valid. The model must
// reject it — a claim from a live (non-waking) state must rotate.
func TestLeaseModel_NonRotatingTakeoverIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "B"), claimedOut(1, "wA"), 3, 4), // BUG: reused gen 1
	}
	checkLinearizable(t, "non-rotating takeover", h, false)
}

// The etcd double-grant shape: after B has rotated the fence, A's stale-token ack
// must not be accepted. An OK here is two holders' tokens both honored.
func TestLeaseModel_StaleAckAcceptedIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "B"), claimedOut(2, "wB"), 3, 4),
		op(0, ackIn("s", "A", 1, "wA", true), okOut(), 5, 6), // BUG: stale ack accepted
	}
	checkLinearizable(t, "stale ack accepted", h, false)
}

// The fence checks the bearer-token generation INDEPENDENTLY of the request-body
// generation (common.lua: token_gen != cur OR req_gen != cur). A replay that
// carries the current request generation and wake but a stale bearer token must
// still be FENCED — so an OK is a violation. This pins the token_gen clause that
// the live driver (which always sends tokenGen == reqGen) cannot reach.
func TestLeaseModel_StaleBearerTokenIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "B"), claimedOut(2, "wB"), 3, 4),
		// reqGen/reqWake are current (2, wB) but the bearer token is the stale gen 1.
		op(0, fenceInput{sub: "s", op: opAck, worker: "A", reqGen: 2, reqWake: "wB", tokenGen: 1, done: true}, okOut(), 5, 6),
	}
	checkLinearizable(t, "stale bearer token accepted", h, false)
}

// A deposed holder's late ack is FENCED even when no peer reclaimed first: the
// server's lease worker expired the lease and cleared the wake (expire_lease.lua)
// WITHOUT rotating the generation — a server-side event with no client op. The
// model, lacking an expire transition, would compute this ack as fence-valid, so
// it must treat the observed FENCED as a legal no-op rather than a violation.
// This is the regression guard for the false-Illegal the adversarial review found.
func TestLeaseModel_PostExpiryFencedIsLinearizable(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(0, ackIn("s", "A", 1, "wA", true), fencedOut(), 3, 4), // server expired+cleared the wake
	}
	checkLinearizable(t, "post-expiry fenced", h, true)
}

// Concurrent claims whose observed generations (A=1 then B=2) admit exactly one
// linearization (A before B). porcupine must find it.
func TestLeaseModel_ConcurrentClaimsForceOrder(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 6),
		op(1, claimIn("s", "B"), claimedOut(2, "wB"), 2, 5),
		op(1, ackIn("s", "B", 2, "wB", true), okOut(), 7, 8),
	}
	checkLinearizable(t, "concurrent claims force order", h, true)
}

// A claim that loses the race returns BUSY; it grants nothing and must be a legal
// no-op, leaving the winner free to ack.
func TestLeaseModel_BusyClaimIsNoop(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 4),
		op(1, claimIn("s", "B"), busyOut(), 2, 3),
		op(0, ackIn("s", "A", 1, "wA", true), okOut(), 5, 6),
	}
	checkLinearizable(t, "busy claim is no-op", h, true)
}

// A NOSUB (the subscription was deleted out from under a worker) grants nothing
// and mutates nothing, so a claim or ack observing it is a legal no-op.
func TestLeaseModel_NoSubIsNoop(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(0, ackIn("s", "A", 1, "wA", true), okOut(), 3, 4),
		op(1, claimIn("s", "B"), noSubOut(), 5, 6),            // sub deleted: claim no-ops
		op(1, ackIn("s", "B", 0, "", true), noSubOut(), 7, 8), // and a follow-up ack no-ops
	}
	checkLinearizable(t, "no-sub is no-op", h, true)
}

// The coalesce branch: an armed wake (idle -> waking, gen 1) claimed by the first
// worker reuses (gen 1, wA); a second concurrent claimer is BUSY. Both legal.
func TestLeaseModel_CoalesceOnArmedWake(t *testing.T) {
	h := []porcupine.Operation{
		op(0, armIn("s"), armedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "A"), claimedOut(1, "wA"), 3, 6), // coalesce: reuse the in-flight fence
		op(2, claimIn("s", "B"), busyOut(), 4, 5),
		op(1, ackIn("s", "A", 1, "wA", true), okOut(), 7, 8),
	}
	checkLinearizable(t, "coalesce on armed wake", h, true)
}

// The other coalesce failure direction: the first claim of an in-flight wake must
// NOT rotate (it would split the fence and orphan the waiter). The model rejects a
// claim of an armed wake that mints a new generation.
func TestLeaseModel_OverRotatingArmedWakeIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		op(0, armIn("s"), armedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "A"), claimedOut(2, "wB"), 3, 4), // BUG: rotated an in-flight wake
	}
	checkLinearizable(t, "over-rotating armed wake", h, false)
}

// A release fences exactly like an ack: the live holder may release; a deposed
// holder's release is FENCED and changes nothing.
func TestLeaseModel_ReleaseFencesLikeAck(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(1, claimIn("s", "B"), claimedOut(2, "wB"), 3, 4),
		op(0, releaseIn("s", "A", 1, "wA"), fencedOut(), 5, 6), // deposed release fenced
		op(1, releaseIn("s", "B", 2, "wB"), okOut(), 7, 8),     // live holder releases
	}
	checkLinearizable(t, "release fences like ack", h, true)
}

// Two independent subscriptions are checked in isolation by the partitioner: a
// violation on one is caught even when the other is clean, and the per-key search
// stays small.
func TestLeaseModel_PartitionsBySubscription(t *testing.T) {
	clean := []porcupine.Operation{
		op(0, claimIn("s1", "A"), claimedOut(1, "wA"), 1, 2),
		op(0, ackIn("s1", "A", 1, "wA", true), okOut(), 3, 4),
	}
	checkLinearizable(t, "two clean subs", append(
		clean,
		op(1, claimIn("s2", "B"), claimedOut(1, "wB"), 5, 6),
		op(1, ackIn("s2", "B", 1, "wB", true), okOut(), 7, 8),
	), true)

	checkLinearizable(t, "violation isolated to one sub", append(
		clean,
		op(1, claimIn("s2", "B"), claimedOut(1, "wB"), 5, 6),
		op(1, ackIn("s2", "B", 9, "wrong", true), okOut(), 7, 8), // s2 violates; s1 clean
	), false)
}

// A long single-holder sequence with heartbeats: claim, two heartbeats extending
// the lease, then ack(done). Every step is fence-valid, so the trace linearizes.
func TestLeaseModel_HeartbeatsKeepFence(t *testing.T) {
	h := []porcupine.Operation{
		op(0, claimIn("s", "A"), claimedOut(1, "wA"), 1, 2),
		op(0, ackIn("s", "A", 1, "wA", false), okOut(), 3, 4),
		op(0, ackIn("s", "A", 1, "wA", false), okOut(), 5, 6),
		op(0, ackIn("s", "A", 1, "wA", true), okOut(), 7, 8),
	}
	checkLinearizable(t, "heartbeats keep fence", h, true)
}
