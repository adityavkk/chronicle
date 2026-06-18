package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func shardHistoryOp(client int, in shardInput, out shardOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

func shardClaimIn(shard int, owner string) shardInput {
	return shardInput{shard: shardID(shard), op: shardOpClaim, owner: ownerID(owner)}
}

func shardCheckIn(shard int, owner string, epoch int64) shardInput {
	return shardInput{shard: shardID(shard), op: shardOpCheck, owner: ownerID(owner), expectedEpoch: ownerEpoch(epoch)}
}

func shardWriteIn(shard int, owner string, epoch int64) shardInput {
	return shardInput{shard: shardID(shard), op: shardOpWrite, owner: ownerID(owner), expectedEpoch: ownerEpoch(epoch)}
}

func shardClaimedOut(owner string, epoch int64) shardOutput {
	return shardOutput{status: statusClaimed, owner: ownerID(owner), epoch: ownerEpoch(epoch)}
}

func shardRenewedOut(owner string, epoch int64) shardOutput {
	return shardOutput{status: statusRenewed, owner: ownerID(owner), epoch: ownerEpoch(epoch)}
}

func shardStatusOut(status string) shardOutput { return shardOutput{status: status} }

func checkShardLinearizable(t *testing.T, name string, h []porcupine.Operation, want bool) {
	t.Helper()
	if got := porcupine.CheckOperations(shardModel(), h); got != want {
		t.Fatalf("%s: CheckOperations = %v, want %v", name, got, want)
	}
}

func TestShardModel_ClaimTransferRenewAndFence(t *testing.T) {
	h := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(0, shardCheckIn(3, "owner-a", 1), shardStatusOut(statusOwner), 3, 4),
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardRenewedOut("owner-a", 1), 5, 6),
		shardHistoryOp(1, shardClaimIn(3, "owner-b"), shardClaimedOut("owner-b", 2), 7, 8),
		shardHistoryOp(0, shardWriteIn(3, "owner-a", 1), shardStatusOut(statusFenced), 9, 10),
		shardHistoryOp(1, shardWriteIn(3, "owner-b", 2), shardStatusOut(statusOK), 11, 12),
	}
	checkShardLinearizable(t, "transfer renew and fence", h, true)
}

func TestShardModel_TransferMustAdvanceEpoch(t *testing.T) {
	h := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(1, shardClaimIn(3, "owner-b"), shardClaimedOut("owner-b", 1), 3, 4), // BUG: transfer reused epoch
	}
	checkShardLinearizable(t, "transfer reused epoch", h, false)
}

func TestShardModel_SameOwnerClaimMustRenew(t *testing.T) {
	h := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 2), 3, 4), // BUG: renew bumped epoch
	}
	checkShardLinearizable(t, "same owner claim bumped epoch", h, false)
}

func TestShardModel_StaleOwnerWriteAcceptedIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(1, shardClaimIn(3, "owner-b"), shardClaimedOut("owner-b", 2), 3, 4),
		shardHistoryOp(0, shardWriteIn(3, "owner-a", 1), shardStatusOut(statusOK), 5, 6), // BUG: stale owner accepted
	}
	checkShardLinearizable(t, "stale write accepted", h, false)
}

func TestShardModel_StaleOwnerCheckAcceptedIsIllegal(t *testing.T) {
	h := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(3, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(1, shardClaimIn(3, "owner-b"), shardClaimedOut("owner-b", 2), 3, 4),
		shardHistoryOp(0, shardCheckIn(3, "owner-a", 1), shardStatusOut(statusOwner), 5, 6), // BUG: check_owner accepted stale epoch
	}
	checkShardLinearizable(t, "stale check accepted", h, false)
}

func TestShardModel_PartitionsByShard(t *testing.T) {
	cleanShardOne := []porcupine.Operation{
		shardHistoryOp(0, shardClaimIn(1, "owner-a"), shardClaimedOut("owner-a", 1), 1, 2),
		shardHistoryOp(0, shardWriteIn(1, "owner-a", 1), shardStatusOut(statusOK), 3, 4),
	}
	checkShardLinearizable(t, "two clean shards", append(
		cleanShardOne,
		shardHistoryOp(1, shardClaimIn(2, "owner-b"), shardClaimedOut("owner-b", 1), 5, 6),
		shardHistoryOp(1, shardWriteIn(2, "owner-b", 1), shardStatusOut(statusOK), 7, 8),
	), true)

	checkShardLinearizable(t, "violation isolated to shard two", append(
		cleanShardOne,
		shardHistoryOp(1, shardClaimIn(2, "owner-b"), shardClaimedOut("owner-b", 1), 5, 6),
		shardHistoryOp(2, shardClaimIn(2, "owner-c"), shardClaimedOut("owner-c", 1), 7, 8), // BUG: transfer reused epoch
	), false)
}
