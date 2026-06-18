package webhook

import "hash/fnv"

// shard.go is the pure core of claim granularity (the third axis, 05 §"A third
// axis: per-type claim contention"; design 08). A subscription's single-holder
// lease is split into G per-shard fences so concurrent claimants on different
// entity-shards of one type do not serialize on one lease. These types are total
// and I/O-free (mirror state.go's discipline): a claim NAMES a shard, and invalid
// shard indices are unrepresentable outside a validated constructor.

// ShardCount is a validated claim-granularity G — the number of per-shard-of-type
// leases one logical type is split into. It is always >= 1; G == 1 is today's
// single per-type lease. It is a distinct type (not a bare int) so a granularity
// can only be obtained through NewShardCount, which enforces the bound.
type ShardCount struct {
	g int
}

// DefaultShardCount is the design-08 default granularity (G = 16): it lifts the
// per-type claimant capacity ~16x past the ~6–12-claimant collapse point while
// keeping per-type fence state a small constant.
const DefaultShardCount = 16

// NewShardCount returns the validated granularity for g, or an error if g < 1.
// It is the only way to construct a ShardCount, so a ShardCount value is always
// in range (parse, don't validate).
func NewShardCount(g int) (ShardCount, error) {
	if g < 1 {
		return ShardCount{}, &ShardError{G: g, reason: "shard count must be >= 1"}
	}
	return ShardCount{g: g}, nil
}

// MustShardCount is NewShardCount for compile-time-known constants (e.g.
// DefaultShardCount); it panics on an invalid count, so it is for initializers,
// never request data.
func MustShardCount(g int) ShardCount {
	c, err := NewShardCount(g)
	if err != nil {
		panic(err)
	}
	return c
}

// Value is the granularity G.
func (c ShardCount) Value() int { return c.g }

// Shard maps an entity id to its shard within this subscription:
// index = ShardIndex(entityId, G). It is total — every entity id yields a valid
// ShardKey — so it never errors.
func (c ShardCount) Shard(subID, entityID string) ShardKey {
	return ShardKey{SubID: subID, Index: ShardIndex(entityID, c.g)}
}

// ShardAt builds the ShardKey for an explicit index, validating index in [0, G).
// Used when the shard is already chosen (e.g. a recovery sweep enumerating every
// shard) rather than derived from an entity id.
func (c ShardCount) ShardAt(subID string, index int) (ShardKey, error) {
	if index < 0 || index >= c.g {
		return ShardKey{}, &ShardError{G: c.g, reason: "shard index out of range"}
	}
	return ShardKey{SubID: subID, Index: index}, nil
}

// ShardKey names one shard of a subscription's claim space: the pair
// (SubID, Index) the per-shard fence is keyed by. Index is in [0, G). It is built
// only through ShardCount, so an in-range index is an invariant of the type, not
// a runtime check at every use.
type ShardKey struct {
	SubID string
	Index int
}

// ShardIndex is the claim-granularity hash, g = hash(entityId) % G, FNV-1a/32 —
// the SAME hash both sides of the client contract use (design 08 §5) so the
// client and chronicle agree on which shard an entity belongs to. G <= 1 always
// returns 0 (the single per-type lease); a non-positive G is treated as 1.
func ShardIndex(entityID string, g int) int {
	if g <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(entityID))
	return int(h.Sum32() % uint32(g))
}

// ShardError is a typed error for an invalid shard count or index (mirrors the
// package's wrapped-error discipline rather than a bare errors.New).
type ShardError struct {
	G      int
	reason string
}

func (e *ShardError) Error() string {
	return "webhook: invalid shard (" + e.reason + ")"
}
