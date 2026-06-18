package webhook

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	// OwnershipSlotCount matches the immutable subscription state shard count.
	// Each ownership record is co-located with the subscription slot it fences.
	OwnershipSlotCount = subSlots

	defaultMemberLeaseTTL        = 9 * time.Second
	defaultHeartbeatInterval     = 3 * time.Second
	defaultSlotLeaseTTL          = 9 * time.Second
	defaultSlotReconcileInterval = 3 * time.Second
)

// ReplicaID names one live Chronicle process incarnation in Redis membership.
type ReplicaID string

// NewReplicaID parses a non-empty replica id once at the config boundary.
func NewReplicaID(raw string) (ReplicaID, error) {
	if raw == "" {
		return "", fmt.Errorf("replica id is required")
	}
	return ReplicaID(raw), nil
}

func (r ReplicaID) String() string { return string(r) }

// GenerateReplicaID derives the default process-incarnation id: POD_NAME plus a
// random nonce, or a local prefix when POD_NAME is unset.
func GenerateReplicaID(lookup func(string) (string, bool), reader io.Reader) (ReplicaID, error) {
	pod, ok := lookup("POD_NAME")
	if !ok || pod == "" {
		pod = "local"
	}
	var nonce [16]byte
	if _, err := io.ReadFull(reader, nonce[:]); err != nil {
		return "", err
	}
	return NewReplicaID(pod + "-" + hex.EncodeToString(nonce[:]))
}

func defaultReplicaID() (ReplicaID, error) {
	return GenerateReplicaID(os.LookupEnv, rand.Reader)
}

// OwnershipSlot is the background-work ownership slot id.
type OwnershipSlot uint16

// NewOwnershipSlot parses a non-negative slot id.
func NewOwnershipSlot(n int) (OwnershipSlot, error) {
	if n < 0 {
		return 0, fmt.Errorf("ownership slot %d must be non-negative", n)
	}
	if n > math.MaxUint16 {
		return 0, fmt.Errorf("ownership slot %d exceeds max %d", n, math.MaxUint16)
	}
	return OwnershipSlot(n), nil
}

// Int returns the slot as a plain integer for metrics and Redis key rendering.
func (s OwnershipSlot) Int() int { return int(s) }

// String returns the decimal slot id.
func (s OwnershipSlot) String() string { return strconv.Itoa(s.Int()) }

func ownershipSlots(n int) []OwnershipSlot {
	out := make([]OwnershipSlot, 0, n)
	for i := 0; i < n; i++ {
		slot, _ := NewOwnershipSlot(i)
		out = append(out, slot)
	}
	return out
}

func subscriptionOwnershipSlot(id string) OwnershipSlot {
	slot, _ := NewOwnershipSlot(subscriptionSlot(id))
	return slot
}

// OwnerEpoch is the monotonic fence layered above (generation,wake_id).
type OwnerEpoch int64

// NewOwnerEpoch parses a positive owner epoch.
func NewOwnerEpoch(n int64) (OwnerEpoch, error) {
	if n <= 0 {
		return 0, fmt.Errorf("owner epoch %d must be positive", n)
	}
	return OwnerEpoch(n), nil
}

// Int64 returns the epoch as the Redis integer value.
func (e OwnerEpoch) Int64() int64 { return int64(e) }

// String returns the decimal epoch id.
func (e OwnerEpoch) String() string { return strconv.FormatInt(e.Int64(), 10) }

func parseOwnerEpoch(raw string) OwnerEpoch {
	n, _ := strconv.ParseInt(raw, 10, 64)
	epoch, _ := NewOwnerEpoch(n)
	return epoch
}

// SlotLease is the held ownership lease for one slot.
type SlotLease struct {
	Slot          OwnershipSlot
	Owner         ReplicaID
	Epoch         OwnerEpoch
	LeaseExpiryNs int64
}

// Active reports whether the lease is still live at now.
func (l SlotLease) Active(now time.Time) bool {
	return l.Owner != "" && l.Epoch > 0 && l.LeaseExpiryNs > now.UnixNano()
}

// Fence returns the owner-epoch fence carried by this held lease.
func (l SlotLease) Fence() OwnershipFence {
	return OwnershipFence{Enabled: true, Slot: l.Slot, Owner: l.Owner, Epoch: l.Epoch}
}

// OwnershipFence is passed into Redis scripts that mutate schedule/due state.
// Disabled is explicit for route callbacks and the all-slot sweep backstop.
type OwnershipFence struct {
	Enabled bool
	Slot    OwnershipSlot
	Owner   ReplicaID
	Epoch   OwnerEpoch
}

// NoOwnerFence disables owner-epoch checking for protocol callbacks and the
// all-slot sweep backstop, which remain protected by the generation/wake fence.
func NoOwnerFence() OwnershipFence { return OwnershipFence{} }

func (f OwnershipFence) args() (owner, epoch string) {
	if !f.Enabled {
		return "", "0"
	}
	return f.Owner.String(), f.Epoch.String()
}

// SlotClaimStatus is the finite claim_shard.lua outcome set.
type SlotClaimStatus string

const (
	// SlotClaimed means claim_shard.lua granted a missing or transferred slot.
	SlotClaimed SlotClaimStatus = "CLAIMED"
	// SlotRenewed means the same owner renewed its slot without bumping epoch.
	SlotRenewed SlotClaimStatus = "RENEWED"
	// SlotBusy means a live foreign owner still holds the slot.
	SlotBusy SlotClaimStatus = "BUSY"
)

// SlotClaimResult is a parsed claim_shard.lua reply.
type SlotClaimResult struct {
	Status SlotClaimStatus
	Lease  SlotLease
}

type slotClaimAttempt struct {
	Slot   OwnershipSlot
	Result SlotClaimResult
	Err    error
}

// Granted reports whether the claim result authorizes this replica to run work.
func (r SlotClaimResult) Granted() bool {
	return r.Status == SlotClaimed || r.Status == SlotRenewed
}

// CheckOwnerStatus is the finite check_owner.lua outcome set.
type CheckOwnerStatus string

const (
	// CheckOwnerOwner means the owner id and epoch match the slot record.
	CheckOwnerOwner CheckOwnerStatus = "OWNER"
	// CheckOwnerFenced means a stale or foreign owner tried to act.
	CheckOwnerFenced CheckOwnerStatus = "FENCED"
	// CheckOwnerUnowned means no owner record exists for the slot.
	CheckOwnerUnowned CheckOwnerStatus = "UNOWNED"
)

func hrwScore(replica ReplicaID, slot OwnershipSlot) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(replica.String()))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(slot.String()))
	return h.Sum64()
}

// HRWOwner returns the deterministic rendezvous-hash owner for slot. Ties break
// toward the lexicographically greatest replica id.
func HRWOwner(members []ReplicaID, slot OwnershipSlot) (ReplicaID, bool) {
	if len(members) == 0 {
		return "", false
	}
	var best ReplicaID
	var bestScore uint64
	for i, member := range members {
		score := hrwScore(member, slot)
		if i == 0 || score > bestScore || (score == bestScore && member.String() > best.String()) {
			best, bestScore = member, score
		}
	}
	return best, true
}

// HRWTargetSlots returns the slots whose deterministic HRW owner is self.
func HRWTargetSlots(members []ReplicaID, self ReplicaID, slots []OwnershipSlot) []OwnershipSlot {
	out := make([]OwnershipSlot, 0, len(slots))
	for _, slot := range slots {
		owner, ok := HRWOwner(members, slot)
		if ok && owner == self {
			out = append(out, slot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func validateOwnershipTiming(memberLeaseTTL, heartbeatInterval, slotLeaseTTL, slotReconcileInterval time.Duration) error {
	if memberLeaseTTL <= 0 {
		return fmt.Errorf("member lease TTL must be positive")
	}
	if heartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	if slotLeaseTTL <= 0 {
		return fmt.Errorf("slot lease TTL must be positive")
	}
	if slotReconcileInterval <= 0 {
		return fmt.Errorf("slot reconcile interval must be positive")
	}
	if heartbeatInterval >= memberLeaseTTL/2 {
		return fmt.Errorf("heartbeat interval %s must be less than half member lease TTL %s", heartbeatInterval, memberLeaseTTL)
	}
	if slotReconcileInterval > heartbeatInterval {
		return fmt.Errorf("slot reconcile interval %s must be <= heartbeat interval %s", slotReconcileInterval, heartbeatInterval)
	}
	return nil
}
