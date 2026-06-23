package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// model_shard.go is the PURE CORE of the slot-ownership exclusivity test (T3 in
// docs/specs/horizontal-scale/research/07): a sequential model of one slot's
// ownership CAS register, checked for linearizability by porcupine. Like
// model_fence.go it has no I/O, no clock, and no dependency on the package under
// test — an independent oracle, deterministic and unit-testable in isolation
// (model_shard_test.go).
//
// SHIPPED. The mechanism it models — claim_shard.lua / check_owner.lua and the
// ds:{ownership}:slot:<h> record — landed in #14 and is live in webhook/. This
// model is the PURE ORACLE for that mechanism, and it is exercised against the
// shipped Lua by the live driver runOwnershipExclusivity in scenario_ownership.go
// (the -scenario ownership-exclusivity gate): N concurrent claimants race
// webhook.RedisStore.ClaimSlot (-> claim_shard.lua) / CheckOwner (-> check_owner.lua)
// over real ds:{ownership} slot hashes with a gcPause nemesis, and the recorded
// history is checked against shardModel() with Unknown counted as FAIL. So
// INV-OWNER-01/02 are validated against the shipped Redis, not just the model
// agreeing with itself. The unit tests in model_shard_test.go are the oracle's
// OWN spec (crafted histories, no Redis); the binding to real Lua is the
// ownership-exclusivity scenario. NOTE for the next reader: the live owner-epoch
// driver is scenario_ownership.go (this file's shell), NOT scenario_shard.go —
// scenario_shard.go drives the orthogonal per-(subId,g) lease layer via
// leaseModel() (the ROADMAP P0.4 row names the wrong file).
//
// It is T3's acceptance gate — it proves the CAS is a real single-holder
// compare-and-set, not a silently-dropping last-writer-wins register (the exact
// failure mode 06 correction #3 warns against putting a correctness-critical
// lease on).
//
// The modeling insight mirrors T1's: the linearizable object is not the wake but
// the OWNERSHIP register, here {owner_id, owner_epoch}. claim_shard.lua is a CAS:
//   - CLAIMED: a grant to a NEW owner (an unowned slot, or taking over an expired
//     one) bumps owner_epoch strictly upward, fencing whoever held it before.
//   - RENEWED: the current owner re-claims; the epoch is KEPT (bump-on-transfer-
//     only), so a renew is never mistaken for a transfer.
//   - BUSY: another owner holds an unexpired lease; nothing is granted.
// A transfer that reuses the epoch (an LWW that silently drops a writer) has no
// valid linearization, so porcupine surfaces the witness.
//
// Time is deliberately absent, exactly as in model_fence.go: whether a claim is
// granted (CLAIMED) or refused (BUSY) depends on the slot's lease clock, which
// the model treats as an observed output; it verifies only the time-free epoch
// algebra (transfer => strictly greater epoch; renew => identical).
//
// ONE DIFFERENCE FROM THE FENCE MODEL, and it is load-bearing. The fence model
// must treat FENCED as an unconditional no-op because expire_lease.lua clears a
// wake_id WITHOUT rotating the generation — a server-side event with no client
// op. Ownership has NO such silent mutation: a slot lease expiry leaves
// owner_id/owner_epoch INTACT in the hash until the next claim_shard *transfer*
// (itself a client op), and check_owner.lua reads owner+epoch only, never the
// lease clock. So the model can verify check_owner's OWNER verdict STRICTLY (the
// caller really is the current owner at the current epoch) without a false
// Illegal — no escape hatch is needed, and that stricter check is what makes the
// deposed-owner-is-fenced property checkable.

// Slot-ownership statuses, mirroring the proposed claim_shard.lua / check_owner.lua
// replies (docs/specs/horizontal-scale/research/05). statusClaimed, statusBusy,
// and statusFenced are shared with model_fence.go.
const (
	statusRenewed = "RENEWED" // claim_shard: same-owner renew (epoch kept)
	statusOwner   = "OWNER"   // check_owner: caller is the current owner at epoch
	statusUnowned = "UNOWNED" // check_owner: the slot has no owner
)

// shardOpKind is the operation recorded into a slot-ownership history.
type shardOpKind int

const (
	opClaimShard shardOpKind = iota // claim_shard.lua -> CLAIMED | RENEWED | BUSY
	opCheckOwner                    // check_owner.lua -> OWNER | FENCED | UNOWNED
)

// shardInput is the model input: which slot, which operation, the caller's
// replica id, and (for check_owner) the epoch the caller believes it holds.
type shardInput struct {
	shard    string // partition key — the virtual slot h
	op       shardOpKind
	caller   string // replica_id (claim_shard ARGV[1] / check_owner ARGV[1])
	reqEpoch int64  // check_owner's expected_epoch; claim_shard ignores it
}

// shardOutput is the model output: the server's observed reply. owner/epoch carry
// the slot record on a CLAIMED or RENEWED.
type shardOutput struct {
	status string
	owner  string
	epoch  int64
}

// shardState is the sequential model state for one slot: the ownership register
// claim_shard CASes and check_owner reads. owner == "" mirrors HGET owner_id
// returning false (no owner). The lease expiry is deliberately NOT part of the
// state — it never changes a future transition's verdict in the time-free model,
// and a minimal state keeps porcupine's (NP-hard) search small (07 gap #1).
type shardState struct {
	owner string // "" = unowned
	epoch int64  // current owner_epoch; 0 = none granted (HINCRBY mints 1 on first claim)
}

// shardModel is the porcupine model for T3, partitioned per slot so the
// linearizability search stays per-key.
func shardModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByShard,
		Init: func() interface{} {
			// No owner has ever been granted: unowned, epoch strictly below any the
			// server can mint (claim_shard HINCRBYs to >= 1 on the first transfer), so
			// the first claim always advances the epoch.
			return shardState{owner: "", epoch: 0}
		},
		Step:              shardStep,
		Equal:             func(a, b interface{}) bool { return a.(shardState) == b.(shardState) },
		DescribeOperation: describeShardOp,
		DescribeState:     describeShardState,
	}
}

// shardStep is the pure transition: given the current state and an observed
// (input, output), it returns whether the step is consistent with the CAS
// invariant and the resulting state. It never mutates its arguments.
func shardStep(state, input, output interface{}) (bool, interface{}) {
	s := state.(shardState)
	in := input.(shardInput)
	out := output.(shardOutput)

	switch in.op {
	case opClaimShard:
		return stepClaimShard(s, in, out)
	case opCheckOwner:
		return stepCheckOwner(s, in, out)
	default:
		return false, s
	}
}

// stepClaimShard models claim_shard.lua's compare-and-set takeover.
func stepClaimShard(s shardState, in shardInput, out shardOutput) (bool, shardState) {
	switch out.status {
	case statusBusy:
		// Another owner holds an unexpired lease; nothing is granted or mutated.
		// Whether a claim is BUSY vs CLAIMED depends on the lease clock the model
		// does not track, so BUSY is a legal no-op under any linearization.
		return true, s
	case statusRenewed:
		// Same-owner renew: the epoch MUST be unchanged (bump-on-transfer-only), and
		// a renew is observable only when the caller already owns the slot. The lease
		// extends (time, untracked); owner and epoch do not move.
		ok := s.owner == in.caller && out.owner == in.caller && out.epoch == s.epoch
		return ok, shardState{owner: in.caller, epoch: s.epoch}
	case statusClaimed:
		// Transfer (or first claim from unowned): the CAS MUST bump owner_epoch
		// STRICTLY upward so a deposed-but-resumed owner carries a stale epoch and is
		// fenced by check_owner. A CLAIMED that reused the epoch is the silently-
		// dropping LWW T3 exists to reject — it has no valid linearization.
		ok := out.epoch > s.epoch && out.owner == in.caller
		return ok, shardState{owner: in.caller, epoch: out.epoch}
	default: // an unexpected status grants nothing and has no durable effect
		return true, s
	}
}

// stepCheckOwner models check_owner.lua, the owner-epoch fence above the
// (gen,wake_id) fence — read-only, so it never changes the state.
func stepCheckOwner(s shardState, in shardInput, out shardOutput) (bool, shardState) {
	switch out.status {
	case statusOwner:
		// THE load-bearing safety assertion. An OWNER verdict authorizes the caller's
		// external side effect (the webhook POST). It MUST be the current owner at the
		// current epoch — an OWNER granted under a stale epoch means a deposed owner
		// was told it still owns, and two owners' side effects both proceed: the
		// split-brain T3 rejects. Checked strictly (see the file header: ownership has
		// no silent server mutation, so no FENCED-style escape hatch is needed).
		ok := s.owner == in.caller && in.reqEpoch == s.epoch
		return ok, s
	case statusFenced, statusUnowned:
		// FENCED (wrong owner or stale epoch) and UNOWNED (no owner) grant nothing and
		// mutate nothing, so they are legal no-ops under any linearization. A spurious
		// FENCED — a live owner wrongly rejected — is a LIVENESS defect (lost work),
		// not a safety one; the L-series covers it, not this model.
		return true, s
	default:
		return true, s
	}
}

// partitionByShard groups a history by slot so each slot's ownership register is
// checked independently — keeping the per-key search modest (07 gap #1).
func partitionByShard(history []porcupine.Operation) [][]porcupine.Operation {
	byShard := map[string][]porcupine.Operation{}
	order := []string{}
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

// describeShardOp renders one operation for a counterexample timeline.
func describeShardOp(input, output interface{}) string {
	in := input.(shardInput)
	out := output.(shardOutput)
	switch in.op {
	case opClaimShard:
		if out.status == statusClaimed || out.status == statusRenewed {
			return fmt.Sprintf("%s claim_shard(%s) -> %s(owner=%s,epoch=%d)", in.caller, in.shard, out.status, in.caller, out.epoch)
		}
		return fmt.Sprintf("%s claim_shard(%s) -> %s", in.caller, in.shard, out.status)
	case opCheckOwner:
		return fmt.Sprintf("%s check_owner(%s,epoch=%d) -> %s", in.caller, in.shard, in.reqEpoch, out.status)
	}
	return "?"
}

// describeShardState renders the ownership register for a counterexample timeline.
func describeShardState(state interface{}) string {
	s := state.(shardState)
	owner := s.owner
	if owner == "" {
		owner = "<none>"
	}
	return fmt.Sprintf("{owner=%s epoch=%d}", owner, s.epoch)
}
