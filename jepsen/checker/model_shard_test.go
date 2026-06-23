package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// These tests exercise the pure slot-ownership CAS model (model_shard.go)
// directly against crafted histories — no cluster, no Redis. They prove the
// oracle accepts every legal interleaving of the SHIPPED claim_shard /
// check_owner protocol and rejects the silently-dropping-LWW violations T3
// exists to catch. With no timeout, CheckOperations is definitive: true ==
// linearizable (Ok), false == a violation (Illegal).
//
// These are the oracle's OWN spec, NOT the binding to the shipped Lua. The
// real-Lua binding — the proof that claim_shard.lua / check_owner.lua and the
// ds:{ownership}:slot:<h> hash agree with this model under live concurrency — is
// the -scenario ownership-exclusivity gate (runOwnershipExclusivity in
// scenario_ownership.go), which drives webhook.RedisStore against a containerized
// Redis and checks the recorded history against shardModel(). The negative-control
// cases below (NonBumpingTransferIsIllegal, RenewThatBumpsIsIllegal,
// DeposedOwnerGrantedIsIllegal, RenewWithoutPriorOwnershipIsIllegal) are what
// guarantees the oracle still bites if the shipped Lua ever drifts into an LWW or
// a stale-OWNER verdict.
//
// Epochs start at 1 to mirror the shipped claim_shard.lua: HINCRBY owner_epoch
// mints 1 on the first transfer, so 1 is the first epoch a real run records.

// shardOp is the shard-history operation builder (model_fence_test.go's op() is
// typed to the fence model's input/output, so the shard model needs its own).
func shardOp(client int, in shardInput, out shardOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

// shard-history builders, mirroring model_fence_test.go's terse style.
func claimShard(shard, caller string) shardInput {
	return shardInput{shard: shard, op: opClaimShard, caller: caller}
}

func checkOwner(shard, caller string, epoch int64) shardInput {
	return shardInput{shard: shard, op: opCheckOwner, caller: caller, reqEpoch: epoch}
}

func claimedShardOut(owner string, epoch int64) shardOutput {
	return shardOutput{status: statusClaimed, owner: owner, epoch: epoch}
}

func renewedShardOut(owner string, epoch int64) shardOutput {
	return shardOutput{status: statusRenewed, owner: owner, epoch: epoch}
}

func busyShardOut() shardOutput   { return shardOutput{status: statusBusy} }
func ownerOut() shardOutput       { return shardOutput{status: statusOwner} }
func fencedOwnerOut() shardOutput { return shardOutput{status: statusFenced} }
func unownedOut() shardOutput     { return shardOutput{status: statusUnowned} }

func checkShardLinearizable(t *testing.T, name string, h []porcupine.Operation, want bool) {
	t.Helper()
	got := porcupine.CheckOperations(shardModel(), h)
	if got != want {
		t.Fatalf("%s: CheckOperations = %v, want %v", name, got, want)
	}
}

// The canonical ownership handoff: A claims an unowned slot (epoch 1), A renews
// (epoch kept), B takes over the expired slot with a bumped epoch (2), and A's
// stale-epoch check_owner is FENCED while B's is OWNER. This is the property the
// whole model exists to generalize.
func TestShardModel_ValidTakeover(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h7", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(0, checkOwner("h7", "A", 1), ownerOut(), 3, 4),
		shardOp(0, claimShard("h7", "A"), renewedShardOut("A", 1), 5, 6), // renew keeps epoch
		shardOp(1, claimShard("h7", "B"), claimedShardOut("B", 2), 7, 8), // takeover bumps 1 -> 2
		shardOp(1, checkOwner("h7", "B", 2), ownerOut(), 9, 10),
		shardOp(0, checkOwner("h7", "A", 1), fencedOwnerOut(), 11, 12), // deposed owner is fenced
	}
	checkShardLinearizable(t, "valid takeover", h, true)
}

// First claim of an unowned slot mints epoch 1 (HINCRBY from 0).
func TestShardModel_FirstClaimMintsEpochOne(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h0", "A"), claimedShardOut("A", 1), 1, 2),
	}
	checkShardLinearizable(t, "first claim mints epoch 1", h, true)
}

// A transfer that REUSES the prior epoch instead of bumping is the silently-
// dropping LWW T3 exists to reject: the deposed owner's epoch stays valid. The
// model must reject it — a CLAIMED that transfers ownership must bump the epoch.
func TestShardModel_NonBumpingTransferIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(1, claimShard("h1", "B"), claimedShardOut("B", 1), 3, 4), // BUG: reused epoch 1
	}
	checkShardLinearizable(t, "non-bumping transfer", h, false)
}

// A renew that BUMPS the epoch is the inverse bug: bump-on-transfer-ONLY means a
// same-owner re-claim keeps the epoch. A renew that minted a new epoch would
// gratuitously fence the owner's own outstanding work.
func TestShardModel_RenewThatBumpsIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(0, claimShard("h1", "A"), renewedShardOut("A", 2), 3, 4), // BUG: renew bumped 1 -> 2
	}
	checkShardLinearizable(t, "renew that bumps", h, false)
}

// The split-brain shape: after B has taken over (epoch 2), a check_owner that
// returns OWNER to the deposed A (still presenting epoch 1) must be a violation —
// two owners would both perform the external side effect.
func TestShardModel_DeposedOwnerGrantedIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(1, claimShard("h1", "B"), claimedShardOut("B", 2), 3, 4),
		shardOp(0, checkOwner("h1", "A", 1), ownerOut(), 5, 6), // BUG: deposed owner told it owns
	}
	checkShardLinearizable(t, "deposed owner granted", h, false)
}

// A RENEWED observed when the caller never owned the slot has no linearization:
// claim_shard only renews when owner == caller.
func TestShardModel_RenewWithoutPriorOwnershipIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(1, claimShard("h1", "B"), renewedShardOut("B", 1), 3, 4), // BUG: B never owned h1
	}
	checkShardLinearizable(t, "renew without prior ownership", h, false)
}

// A claim that loses the race returns BUSY; it grants nothing and must be a legal
// no-op, leaving the owner free to renew.
func TestShardModel_BusyClaimIsNoop(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 6),
		shardOp(1, claimShard("h1", "B"), busyShardOut(), 2, 3),
		shardOp(0, claimShard("h1", "A"), renewedShardOut("A", 1), 7, 8),
	}
	checkShardLinearizable(t, "busy claim is no-op", h, true)
}

// check_owner on a never-claimed slot returns UNOWNED — a legal no-op.
func TestShardModel_UnownedCheckIsNoop(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, checkOwner("h9", "A", 0), unownedOut(), 1, 2),
		shardOp(0, claimShard("h9", "A"), claimedShardOut("A", 1), 3, 4),
	}
	checkShardLinearizable(t, "unowned check is no-op", h, true)
}

// Concurrent claims whose observed epochs (A=1 then B=2) admit exactly one
// linearization (A before B). porcupine must find it.
func TestShardModel_ConcurrentClaimsForceOrder(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 6),
		shardOp(1, claimShard("h1", "B"), claimedShardOut("B", 2), 2, 5),
		shardOp(1, checkOwner("h1", "B", 2), ownerOut(), 7, 8),
	}
	checkShardLinearizable(t, "concurrent claims force order", h, true)
}

// A renew-heartbeat sequence: A claims, renews twice (epoch held throughout),
// then a peer takes over with a single bump. Every step is CAS-valid.
func TestShardModel_RenewHeartbeatsKeepEpoch(t *testing.T) {
	h := []porcupine.Operation{
		shardOp(0, claimShard("h2", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(0, claimShard("h2", "A"), renewedShardOut("A", 1), 3, 4),
		shardOp(0, claimShard("h2", "A"), renewedShardOut("A", 1), 5, 6),
		shardOp(1, claimShard("h2", "B"), claimedShardOut("B", 2), 7, 8),
	}
	checkShardLinearizable(t, "renew heartbeats keep epoch", h, true)
}

// Two independent slots are checked in isolation by the partitioner: a violation
// on one is caught even when the other is clean, and the per-key search stays
// small.
func TestShardModel_PartitionsBySlot(t *testing.T) {
	clean := []porcupine.Operation{
		shardOp(0, claimShard("h1", "A"), claimedShardOut("A", 1), 1, 2),
		shardOp(0, checkOwner("h1", "A", 1), ownerOut(), 3, 4),
	}
	checkShardLinearizable(t, "two clean slots", append(
		clean,
		shardOp(1, claimShard("h2", "B"), claimedShardOut("B", 1), 5, 6),
		shardOp(1, checkOwner("h2", "B", 1), ownerOut(), 7, 8),
	), true)

	checkShardLinearizable(t, "violation isolated to one slot", append(
		clean,
		shardOp(1, claimShard("h2", "B"), claimedShardOut("B", 1), 5, 6),
		shardOp(2, claimShard("h2", "C"), claimedShardOut("C", 1), 7, 8), // h2 transfer reused epoch 1
	), false)
}
