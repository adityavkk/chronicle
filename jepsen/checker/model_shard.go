package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// model_shard.go is the PURE CORE scaffold for T3 (ownership exclusivity in
// docs/specs/horizontal-scale/research/07): a time-free model of the proposed
// ds:{ownership}:slot:<h> CAS register. The live claim_shard.lua/check_owner.lua
// mechanism does not exist in today's production code, so this file is the
// executable contract future slices must satisfy rather than a green live test.
//
// The model deliberately omits TTL time. A CLAIMED output is treated as the
// observed fact that the current owner was absent/expired or the caller won the
// transfer; the safety algebra is that every transfer strictly increases
// owner_epoch, renewals preserve it, and any OWNER/OK side effect must carry the
// current (owner_id, owner_epoch) fence. BUSY/FENCED/UNOWNED grant no authority
// and are legal no-ops, matching the fence-model rule from model_fence.go.

type (
	shardID    int
	ownerID    string
	ownerEpoch int64
)

const noOwner ownerID = ""

const (
	statusRenewed = "RENEWED"
	statusOwner   = "OWNER"
	statusUnowned = "UNOWNED"
)

type shardOpKind int

const (
	shardOpClaim shardOpKind = iota
	shardOpCheck
	shardOpWrite
)

type shardInput struct {
	shard         shardID
	op            shardOpKind
	owner         ownerID
	expectedEpoch ownerEpoch
}

type shardOutput struct {
	status string
	owner  ownerID
	epoch  ownerEpoch
}

type shardState struct {
	owner ownerID
	epoch ownerEpoch
}

// shardModel is partitioned per shard id so porcupine checks one ownership
// register at a time. This is mandatory for useful results once histories grow.
func shardModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByShard,
		Init: func() interface{} {
			return shardState{}
		},
		Step:              shardStep,
		Equal:             func(a, b interface{}) bool { return a.(shardState) == b.(shardState) },
		DescribeOperation: describeShardOp,
		DescribeState:     describeShardState,
	}
}

func shardStep(state, input, output interface{}) (bool, interface{}) {
	s := state.(shardState)
	in := input.(shardInput)
	out := output.(shardOutput)

	switch in.op {
	case shardOpClaim:
		return stepShardClaim(s, in, out)
	case shardOpCheck:
		return stepShardCheck(s, in, out)
	case shardOpWrite:
		return stepShardWrite(s, in, out)
	default:
		return false, s
	}
}

func stepShardClaim(s shardState, in shardInput, out shardOutput) (bool, shardState) {
	switch out.status {
	case statusClaimed:
		ok := in.owner != noOwner && out.owner == in.owner && out.epoch > s.epoch && s.owner != in.owner
		return ok, shardState{owner: out.owner, epoch: out.epoch}
	case statusRenewed:
		ok := in.owner != noOwner && s.owner == in.owner && out.owner == in.owner && out.epoch == s.epoch
		return ok, s
	case statusBusy:
		return true, s
	default:
		return false, s
	}
}

func stepShardCheck(s shardState, in shardInput, out shardOutput) (bool, shardState) {
	switch out.status {
	case statusOwner:
		ok := s.owner != noOwner && in.owner == s.owner && in.expectedEpoch == s.epoch
		return ok, s
	case statusFenced, statusUnowned:
		return true, s
	default:
		return false, s
	}
}

func stepShardWrite(s shardState, in shardInput, out shardOutput) (bool, shardState) {
	switch out.status {
	case statusOK:
		ok := s.owner != noOwner && in.owner == s.owner && in.expectedEpoch == s.epoch
		return ok, s
	case statusFenced, statusUnowned:
		return true, s
	default:
		return false, s
	}
}

func partitionByShard(history []porcupine.Operation) [][]porcupine.Operation {
	byShard := map[shardID][]porcupine.Operation{}
	order := []shardID{}
	for _, o := range history {
		shard := o.Input.(shardInput).shard
		if _, seen := byShard[shard]; !seen {
			order = append(order, shard)
		}
		byShard[shard] = append(byShard[shard], o)
	}
	parts := make([][]porcupine.Operation, 0, len(order))
	for _, shard := range order {
		parts = append(parts, byShard[shard])
	}
	return parts
}

func describeShardOp(input, output interface{}) string {
	in := input.(shardInput)
	out := output.(shardOutput)
	switch in.op {
	case shardOpClaim:
		if out.status == statusClaimed || out.status == statusRenewed {
			return fmt.Sprintf("%s claim shard=%d -> %s(epoch=%d)", in.owner, in.shard, out.status, out.epoch)
		}
		return fmt.Sprintf("%s claim shard=%d -> %s", in.owner, in.shard, out.status)
	case shardOpCheck:
		return fmt.Sprintf("%s check shard=%d epoch=%d -> %s", in.owner, in.shard, in.expectedEpoch, out.status)
	case shardOpWrite:
		return fmt.Sprintf("%s write shard=%d epoch=%d -> %s", in.owner, in.shard, in.expectedEpoch, out.status)
	default:
		return "?"
	}
}

func describeShardState(state interface{}) string {
	s := state.(shardState)
	return fmt.Sprintf("{owner=%s epoch=%d}", s.owner, s.epoch)
}
