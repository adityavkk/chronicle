package webhook

import (
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// ClaimShardCount is the fixed Chronicle claim-granularity fan-out for one
// logical pull-wake subscription. Shard 0 is the protocol-compatible default.
const ClaimShardCount = 16

// ClaimShard names one lease and fence shard within a subscription.
type ClaimShard uint8

const (
	// DefaultClaimShard is the legacy single-lease behavior.
	DefaultClaimShard ClaimShard = 0

	shardedLeaseMemberPrefix = "@shard:"
)

// ClaimMode records whether a pull-wake subscription is using the original
// unsharded claim contract or the Chronicle explicit-shard extension.
type ClaimMode string

const (
	// ClaimModeLegacy is selected when a claim omits shard. It preserves the
	// original protocol behavior: one shard-0 lease covers every linked stream.
	ClaimModeLegacy ClaimMode = "legacy"
	// ClaimModeSharded is selected when a claim includes shard, including
	// explicit shard zero. It makes shard presence part of the wire contract.
	ClaimModeSharded ClaimMode = "sharded"
)

// String returns the Redis/wire representation of the claim mode.
func (m ClaimMode) String() string { return string(m) }

// ClaimShardSelection is the parsed shard input at an HTTP boundary.
type ClaimShardSelection struct {
	Shard ClaimShard
	Mode  ClaimMode
}

// Sharded reports whether the request used the explicit shard extension.
func (s ClaimShardSelection) Sharded() bool { return s.Mode == ClaimModeSharded }

// NewClaimShardSelection parses an optional shard body field once at the wire
// boundary. Omitting the field means legacy mode; providing shard 0 is distinct
// and means sharded mode bound to shard zero.
func NewClaimShardSelection(raw *int) (ClaimShardSelection, error) {
	if raw == nil {
		return ClaimShardSelection{Shard: DefaultClaimShard, Mode: ClaimModeLegacy}, nil
	}
	shard, err := NewClaimShard(*raw)
	if err != nil {
		return ClaimShardSelection{}, err
	}
	return ClaimShardSelection{Shard: shard, Mode: ClaimModeSharded}, nil
}

// NewClaimShard parses an external shard number into the bounded domain.
func NewClaimShard(n int) (ClaimShard, error) {
	if n < 0 || n >= ClaimShardCount {
		return 0, fmt.Errorf("claim shard %d outside [0,%d)", n, ClaimShardCount)
	}
	return ClaimShard(n), nil
}

// Int returns the shard as a plain integer for wire and metrics code.
func (s ClaimShard) Int() int { return int(s) }

// String returns the decimal shard id.
func (s ClaimShard) String() string { return strconv.Itoa(s.Int()) }

// LeaseRef is the durable schedule member for one subscription claim shard.
type LeaseRef struct {
	SubID string
	Shard ClaimShard
}

// NewLeaseRef builds a schedule member reference for a subscription shard.
func NewLeaseRef(subID string, shard ClaimShard) LeaseRef {
	return LeaseRef{SubID: subID, Shard: shard}
}

// Member encodes the Redis ZSET member. Shard 0 keeps the historic member value
// equal to the subscription id so existing schedules survive an upgrade.
func (r LeaseRef) Member() string {
	if r.Shard == DefaultClaimShard {
		return r.SubID
	}
	return shardedLeaseMemberPrefix + r.Shard.String() + ":" +
		base64.RawURLEncoding.EncodeToString([]byte(r.SubID))
}

// ParseLeaseMember decodes a Redis ZSET member into a lease reference.
func ParseLeaseMember(member string) (LeaseRef, error) {
	if !strings.HasPrefix(member, shardedLeaseMemberPrefix) {
		return NewLeaseRef(member, DefaultClaimShard), nil
	}
	rest := strings.TrimPrefix(member, shardedLeaseMemberPrefix)
	shardRaw, idRaw, ok := strings.Cut(rest, ":")
	if !ok {
		return LeaseRef{}, fmt.Errorf("malformed sharded lease member %q", member)
	}
	shardN, err := strconv.Atoi(shardRaw)
	if err != nil {
		return LeaseRef{}, fmt.Errorf("parse claim shard %q: %w", shardRaw, err)
	}
	shard, err := NewClaimShard(shardN)
	if err != nil {
		return LeaseRef{}, err
	}
	idBytes, err := base64.RawURLEncoding.DecodeString(idRaw)
	if err != nil {
		return LeaseRef{}, fmt.Errorf("decode subscription id: %w", err)
	}
	if len(idBytes) == 0 {
		return LeaseRef{}, fmt.Errorf("empty subscription id in %q", member)
	}
	return NewLeaseRef(string(idBytes), shard), nil
}

// ClaimLeaseState is the fence-bearing state for one claim shard.
type ClaimLeaseState struct {
	Phase        Phase
	Generation   int64
	WakeID       string
	Holder       bool
	HolderWorker string
	LeaseUntilNs int64
}

// ClaimShardLeaseState binds one shard id to its durable fence-bearing state.
type ClaimShardLeaseState struct {
	Shard ClaimShard
	State ClaimLeaseState
}

// Ref returns the volatile lease schedule member for this durable shard state.
func (s ClaimShardLeaseState) Ref(subID string) LeaseRef {
	return NewLeaseRef(subID, s.Shard)
}

// ClaimLeaseFromSubscription adapts the legacy subscription-level fields to the
// shard-neutral pure decision helpers.
func ClaimLeaseFromSubscription(sub Subscription) ClaimLeaseState {
	return ClaimLeaseState{
		Phase:        sub.Phase,
		Generation:   sub.Generation,
		WakeID:       sub.WakeID,
		Holder:       sub.Holder,
		HolderWorker: sub.HolderWorker,
		LeaseUntilNs: sub.LeaseUntilNs,
	}
}

// ClaimLeasesFromSubscription returns the default shard plus any hydrated
// non-default shard states. Shard 0 intentionally stays first to preserve legacy
// behavior when code only needs the subscription-level lease.
func ClaimLeasesFromSubscription(sub Subscription) []ClaimShardLeaseState {
	leases := make([]ClaimShardLeaseState, 0, 1+len(sub.ClaimLeases))
	leases = append(leases, ClaimShardLeaseState{
		Shard: DefaultClaimShard,
		State: ClaimLeaseFromSubscription(sub),
	})
	leases = append(leases, sub.ClaimLeases...)
	return leases
}

func claimShardField(base string, shard ClaimShard) string {
	if shard == DefaultClaimShard {
		return base
	}
	return base + ":" + shard.String()
}

// StreamClaimShard maps a stream path to the claim shard Chronicle can enforce.
// The Electric client contract maps entity ids to the same shard before it
// chooses which worker claims that entity.
func StreamClaimShard(path string) ClaimShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	shard, _ := NewClaimShard(int(h.Sum32() % ClaimShardCount))
	return shard
}

// FilterSnapshotForClaimShard keeps only the stream snapshots owned by shard.
func FilterSnapshotForClaimShard(snap []StreamSnapshot, shard ClaimShard) []StreamSnapshot {
	out := make([]StreamSnapshot, 0, len(snap))
	for _, s := range snap {
		if StreamClaimShard(s.Path) == shard {
			out = append(out, s)
		}
	}
	return out
}

// FilterAcksForClaimShard keeps only acks for streams owned by shard.
func FilterAcksForClaimShard(acks []Ack, shard ClaimShard) []Ack {
	out := make([]Ack, 0, len(acks))
	for _, ack := range acks {
		if StreamClaimShard(ack.Stream) == shard {
			out = append(out, ack)
		}
	}
	return out
}
