package webhook

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"time"
)

// ownership.go is the PURE CORE of work-sharded leased slot ownership (Move 3,
// docs/specs/horizontal-scale/research/05). It shards autonomous BACKGROUND work
// (the lease/retry/due workers + the failover-aware reconcile) across replicas by
// a leased SLOT, so total work is O(total owed) regardless of replica count N —
// the inverse of #10's gate-#1 O(N·K) sweep.
//
// THIS SLOT-OWNERSHIP AXIS IS ORTHOGONAL TO #11's per-(subId,g) CLAIM GRANULARITY
// (shard.go). #11 splits ONE subscription's single-holder lease into G fences so
// concurrent claimants on different entity-shards do not serialize on one lease.
// THIS layer shards which REPLICA runs the background loops for a slot of the
// keyspace. They share only the word "shard": different keys (ds:{ownership}:slot
// vs the per-(subId,g) fence hash), different types, the owner-epoch fence here vs
// the (gen,wake_id) fence there. Do not conflate them.
//
// Like state.go and shard.go these are total, I/O-free functions over plain
// values: the HRW assignment, the CAS-reply interpretation, and the OwnedSlots set
// math carry no Redis and no clock. The membership/heartbeat/reconcile IO is the
// thin shell in manager.go; the slot records live in Redis under the {ownership}
// tag (keys.go).
//
// THE OWNER-EPOCH FENCE IS LAYERED ABOVE THE (gen,wake_id) FENCE, NEVER REPLACING
// IT (06 correction #1): it only SUPPRESSES a deposed owner's wasted work; the
// (gen,wake_id) fence remains the safety boundary that makes any leaked duplicate
// harmless, so work-sharding is an optimization over a still-correct full sweep.

// ownershipSlots (S) is the number of virtual ownership slots. It is 1 for #14:
// subscription STATE is not slot-homed yet (the S-slot {__ds:h} tagging is #15),
// so OwnedSlots() runs the degenerate single-slot case — one ownership slot gates
// ALL background work and exactly one replica owns it at a time. That degenerate
// case is precisely what delivers O(total owed): the slot owner runs the workers,
// every other replica idles. #15 raises this to 256 (h = fnv32a(subId) % S) once
// state is slot-homed; the HRW math below is already general over slot indices.
const ownershipSlots = 1

// SlotID is a validated virtual ownership slot index in [0, S). It is a distinct
// type (not a bare int) so a slot can only be obtained through a constructor that
// enforces the bound — making an out-of-range slot unrepresentable (state.go /
// shard.go discipline).
type SlotID struct{ h int }

// NewSlotID returns the validated slot for h, or an error when h is out of range.
// It is the only public way to build a SlotID, so an in-range index is an
// invariant of the type rather than a check at every use.
func NewSlotID(h int) (SlotID, error) {
	if h < 0 || h >= ownershipSlots {
		return SlotID{}, fmt.Errorf("webhook: slot id %d out of range [0,%d)", h, ownershipSlots)
	}
	return SlotID{h: h}, nil
}

// Index is the slot's integer index.
func (s SlotID) Index() int { return s.h }

// String renders the slot index for the slotKey tag and logs.
func (s SlotID) String() string { return strconv.Itoa(s.h) }

// AllSlots is every ownership slot [0, S). The slot-reconcile loop computes the
// HRW target of each and claims the ones it targets.
func AllSlots() []SlotID {
	out := make([]SlotID, ownershipSlots)
	for h := range out {
		out[h] = SlotID{h: h}
	}
	return out
}

// OwnerEpoch is a slot's monotonic ownership generation — bumped on every TRANSFER
// of slot ownership (never on a same-owner renew), so a deposed-but-resumed owner
// carries a STALE epoch and is fenced by check_owner / the inlined checks. It is
// the {ownership}-layer analogue of the per-subscription generation. Distinct
// type, not a bare int64.
type OwnerEpoch struct{ e int64 }

// Value is the epoch as an int64.
func (e OwnerEpoch) Value() int64 { return e.e }

// String renders the epoch in the base-10 form the Lua scripts exchange (HGET /
// HINCRBY of owner_epoch). check_owner.lua compares this string against the
// stored field, so the round-trip must be base-10, never scientific notation.
func (e OwnerEpoch) String() string { return strconv.FormatInt(e.e, 10) }

// parseOwnerEpoch reads the string form claim_shard.lua replies with. A malformed
// or empty epoch parses to 0, which is below any epoch the server mints (HINCRBY
// mints >= 1 on the first transfer), so it can never be mistaken for a live one.
func parseOwnerEpoch(s string) OwnerEpoch {
	n, _ := strconv.ParseInt(s, 10, 64)
	return OwnerEpoch{e: n}
}

// ReplicaID identifies one live Chronicle process incarnation: POD_NAME + a
// crypto/rand per-process-start nonce (so a reused pod name is distinguished),
// falling back to a generated form locally. It need only be unique per live
// process — owner_epoch (bumped on transfer), NOT replica_id, is what fences a
// paused-then-resumed SAME incarnation (05 §membership). Distinct type, not a bare
// string, so a slot's owner_id and the HRW score can never be confused with an
// arbitrary string.
type ReplicaID struct{ id string }

// NewReplicaID validates a non-empty replica id.
func NewReplicaID(id string) (ReplicaID, error) {
	if id == "" {
		return ReplicaID{}, fmt.Errorf("webhook: empty replica id")
	}
	return ReplicaID{id: id}, nil
}

// String is the wire form (the slot owner_id / claim_shard ARGV[1]).
func (r ReplicaID) String() string { return r.id }

// GenerateReplicaID builds a process-unique replica id "<podName>-<nonce>", where
// nonce is 16 crypto-random bytes hex (32 chars) — the exact form the jepsen
// killSlotOwner nemesis parses back to a pod name (ownerPodFromReplicaID). An
// empty podName (local/dev, no Downward API) falls back to "local". The nonce
// makes the id unique per process start even when a pod name is reused, so a
// restarted incarnation never collides with its predecessor; owner_epoch — not
// this id — is what fences a paused-then-resumed SAME process. Deterministic given
// rnd, so it is unit-testable; the os.Getenv("POD_NAME") read stays in the shell.
func GenerateReplicaID(podName string, rnd io.Reader) (ReplicaID, error) {
	nonce := make([]byte, 16)
	if _, err := io.ReadFull(rnd, nonce); err != nil {
		return ReplicaID{}, err
	}
	if podName == "" {
		podName = "local"
	}
	return ReplicaID{id: podName + "-" + hex.EncodeToString(nonce)}, nil
}

// SlotClaimStatus is the sealed result of claim_shard.lua — a sum type, never a
// bool, so the silently-dropping last-writer-wins register 06 correction #3 warns
// against is unrepresentable: a caller MUST switch over all three cases.
type SlotClaimStatus int

const (
	// SlotClaimed is a grant to a NEW owner (unowned, expired-takeover, or any
	// transfer): owner_epoch is bumped strictly upward, fencing the prior holder.
	SlotClaimed SlotClaimStatus = iota
	// SlotRenewed is the current owner re-leasing: owner_epoch is UNCHANGED
	// (bump-on-transfer-only), so a renew is never mistaken for a transfer.
	SlotRenewed
	// SlotBusy is a live foreign owner holding an unexpired lease: nothing granted.
	SlotBusy
)

// SlotClaim is the parsed claim_shard.lua reply: the status plus the slot record
// it observed/wrote ({owner_id, owner_epoch, lease_expiry_ns}). On CLAIMED/RENEWED
// the owner is the caller; on BUSY it is the live foreign owner.
type SlotClaim struct {
	Status   SlotClaimStatus
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}

// Granted reports whether the caller holds the slot lease after this claim
// (CLAIMED or RENEWED). BUSY is not a grant.
func (c SlotClaim) Granted() bool { return c.Status == SlotClaimed || c.Status == SlotRenewed }

// Transferred reports whether ownership changed hands (a new owner CAS / epoch
// bump). It is the trigger the slot-reconcile loop fires #13's reconcile(scope)
// on. RENEWED is not a transfer.
func (c SlotClaim) Transferred() bool { return c.Status == SlotClaimed }

// parseSlotClaim decodes the {status, owner_id, owner_epoch, lease_expiry_ns}
// reply. Pure over the string slice evalStrings already produced.
func parseSlotClaim(reply []string) (SlotClaim, error) {
	if len(reply) == 0 {
		return SlotClaim{}, fmt.Errorf("webhook: empty claim_shard reply")
	}
	var status SlotClaimStatus
	switch reply[0] {
	case "CLAIMED":
		status = SlotClaimed
	case "RENEWED":
		status = SlotRenewed
	case "BUSY":
		status = SlotBusy
	default:
		return SlotClaim{}, fmt.Errorf("webhook: unexpected claim_shard status %q", reply[0])
	}
	c := SlotClaim{Status: status}
	if len(reply) > 1 && reply[1] != "" {
		c.Owner = ReplicaID{id: reply[1]}
	}
	if len(reply) > 2 {
		c.Epoch = parseOwnerEpoch(reply[2])
	}
	if len(reply) > 3 {
		c.ExpiryNs = parseLeaseUntilNs(reply[3])
	}
	return c, nil
}

// OwnerCheck is the sealed result of check_owner.lua, the owner-epoch fence for
// the one write that cannot be inlined (the external webhook POST). Sum type, not
// a bool, for the same reason as SlotClaimStatus.
type OwnerCheck int

const (
	// OwnerCheckOwner: the caller is the current owner at the expected epoch — its
	// external side effect is authorized.
	OwnerCheckOwner OwnerCheck = iota
	// OwnerCheckFenced: a different owner, or the caller at a stale epoch — a
	// deposed owner. Its side effect is suppressed.
	OwnerCheckFenced
	// OwnerCheckUnowned: the slot has no owner.
	OwnerCheckUnowned
)

// OK reports whether the check authorizes the caller's side effect.
func (o OwnerCheck) OK() bool { return o == OwnerCheckOwner }

// parseOwnerCheck decodes the {status} OWNER | FENCED | UNOWNED reply.
func parseOwnerCheck(reply []string) (OwnerCheck, error) {
	if len(reply) == 0 {
		return OwnerCheckFenced, fmt.Errorf("webhook: empty check_owner reply")
	}
	switch reply[0] {
	case "OWNER":
		return OwnerCheckOwner, nil
	case "FENCED":
		return OwnerCheckFenced, nil
	case "UNOWNED":
		return OwnerCheckUnowned, nil
	default:
		return OwnerCheckFenced, fmt.Errorf("webhook: unexpected check_owner status %q", reply[0])
	}
}

// hrwScore is score(r,h) = mix(fnv64a(r + ":" + itoa(h))) (05 §HRW). Pure and
// language-stable — the SAME hash every replica computes — so all replicas agree
// on each slot's target owner with no coordination.
//
// The base digest is the spec's fnv64a(r + ":" + itoa(h)); the splitmix64
// finalizer below is a load-bearing strengthening, NOT a deviation from the
// intent. FNV-1a has weak avalanche on its trailing bytes: with the slot index in
// the trailing position, each replica's raw Sum64 barely moves across slots, so
// the cross-replica ranking is nearly identical for every slot — one replica wins
// almost all slots and a joiner gains ~none. That breaks the headline HRW property
// the spec itself states ("adding/removing one replica reassigns only ~1/N of
// slots"), on which L4 re-convergence and gate #4 depend. Finalizing the 64-bit
// digest with splitmix64's mix fully avalanches the low-order slot bits across the
// whole word, restoring a balanced ~1/N assignment (verified by
// TestHRWReassignmentFraction). It is a pure bijection over the digest, so
// determinism and language-stability are preserved.
func hrwScore(r ReplicaID, h SlotID) uint64 {
	x := fnv.New64a()
	_, _ = x.Write([]byte(r.id + ":" + strconv.Itoa(h.h)))
	return mix64(x.Sum64())
}

// mix64 is the splitmix64 finalizer: a well-distributed bijective bit-mixer that
// gives the HRW score full avalanche over FNV-1a's poorly-mixed output.
func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// HRWOwner returns the rendezvous-hashing owner of slot h among the live members:
// the argmax score, tie-broken by the lexicographically greatest replica_id (05
// §HRW). Adding/removing one replica reassigns only ~1/N of slots — no rebalancing
// storm. ok is false when there are no live members (the slot has no target owner;
// the full sweep still covers it).
func HRWOwner(members []ReplicaID, h SlotID) (owner ReplicaID, ok bool) {
	var bestScore uint64
	for _, m := range members {
		s := hrwScore(m, h)
		if !ok || s > bestScore || (s == bestScore && m.id > owner.id) {
			owner, bestScore, ok = m, s, true
		}
	}
	return owner, ok
}

// TargetedSlots is the set of slots HRW assigns to `me` out of all slots, given
// the live member set. It is the "HRW" half of OwnedSlots.
func TargetedSlots(me ReplicaID, members []ReplicaID, slots []SlotID) map[SlotID]struct{} {
	out := make(map[SlotID]struct{})
	for _, h := range slots {
		if owner, ok := HRWOwner(members, h); ok && owner.id == me.id {
			out[h] = struct{}{}
		}
	}
	return out
}

// OwnedSlots is the pure set intersection at the heart of "THE CAS IS THE
// AUTHORITY, NOT THE HRW MATH" (05:399-402): a replica runs a slot's background
// work only if it BOTH targets the slot (HRW: `targeted`) AND holds its lease
// (claim_shard returned CLAIMED/RENEWED: `held`). A brief disagreement during a
// stale-member-read window is SAFE — a double-wake coalesces and a zero-owner gap
// is covered by the full sweep until claim_shard resolves it — so the intersection
// is the correct, conservative set to drive the fast workers from.
func OwnedSlots(targeted, held map[SlotID]struct{}) []SlotID {
	out := make([]SlotID, 0, len(targeted))
	for h := range targeted {
		if _, ok := held[h]; ok {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].h < out[j].h })
	return out
}

// OwnerScope carries the slot-ownership identity a background worker presents so a
// schedule/due-mutating script can inline the owner-epoch fence ATOMICALLY with
// its write (the TOCTOU resolution, 05:372-385): a separate check_owner round-trip
// could not fence a GC pause between the check and the write. The zero value
// (Epoch == "") means "not slot-scoped" — the load-balanced external/hot-path
// callers pass it, and the script skips the check, leaving the (gen,wake_id) fence
// as the guard. A non-empty Epoch makes the script verify the caller is SlotKey's
// current owner at that epoch before its write, FENCING a deposed owner. Layered
// ABOVE the (gen,wake_id) fence, never replacing it.
type OwnerScope struct {
	SlotKey   string // ds:{ownership}:slot:<h>
	ReplicaID string // me
	Epoch     string // the epoch I hold (OwnerEpoch.String form); "" = skip the check
}

// active reports whether this scope enforces the owner-epoch fence (a non-empty
// expected epoch). A non-active scope is the external/hot-path no-op.
func (o OwnerScope) active() bool { return o.Epoch != "" }

// firstOwnerScope resolves the variadic owner argument the schedule/due store
// methods take into the (slotKey, replicaID, epoch) ARGV/KEY triple. With no
// active scope it returns slot 0's key and an empty epoch, so the script's
// owner_fenced short-circuits without reading the slot — today's behavior on the
// external path, byte-for-byte. (On a single-node Redis declaring the extra key is
// harmless; see keys.go on the {ownership} tag.)
func firstOwnerScope(owner []OwnerScope) (slotKeyStr, replicaID, epoch string) {
	if len(owner) > 0 && owner[0].active() {
		return owner[0].SlotKey, owner[0].ReplicaID, owner[0].Epoch
	}
	return slotKey(0), "", ""
}

// CheckOwnershipConfig enforces the two membership invariants (05:507-508):
// heartbeatInterval < memberLeaseTTL/2 (renew with headroom so a single late beat
// does not drop the replica) and slotReconcileInterval <= heartbeatInterval
// (re-claim owned slots at least as often as we prove liveness). Pure, so it is
// unit-tested without a Manager and reused to validate operator-supplied config.
func CheckOwnershipConfig(memberLeaseTTL, heartbeatInterval, slotLeaseTTL, slotReconcileInterval time.Duration) error {
	if memberLeaseTTL <= 0 || heartbeatInterval <= 0 || slotLeaseTTL <= 0 || slotReconcileInterval <= 0 {
		return fmt.Errorf("webhook: ownership TTLs must be positive (member=%s heartbeat=%s slot=%s reconcile=%s)",
			memberLeaseTTL, heartbeatInterval, slotLeaseTTL, slotReconcileInterval)
	}
	if heartbeatInterval >= memberLeaseTTL/2 {
		return fmt.Errorf("webhook: heartbeatInterval (%s) must be < memberLeaseTTL/2 (%s)", heartbeatInterval, memberLeaseTTL/2)
	}
	if slotReconcileInterval > heartbeatInterval {
		return fmt.Errorf("webhook: slotReconcileInterval (%s) must be <= heartbeatInterval (%s)", slotReconcileInterval, heartbeatInterval)
	}
	return nil
}
