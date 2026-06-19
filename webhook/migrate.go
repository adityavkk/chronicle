package webhook

import "github.com/redis/go-redis/v9"

// migrate.go is the shadow-write + lazy per-sub migration from the pre-slot-homing
// keyspace (the single {__ds} tag) to the slot-homed {__ds:h} keyspace (Move 1,
// 05 §Migration step 3 — "behind a shadow write + lazy per-sub migration").
//
// REVERSIBLE BY CONSTRUCTION: S=subSlots is a compile-time const and the legacy
// readers below are retained, so a rollback is the mirror move (read new, write
// legacy). The migration is non-destructive — it COPIES legacy → slot-homed and only
// then FLIPS (drops the legacy copy) — so a crash mid-migrate leaves the legacy copy
// intact to be re-migrated on the next access; a half-done copy is superseded the
// moment the slot-homed copy is read (Get prefers it). It is cross-slot by nature
// (legacy {__ds} and {__ds:h} are different cluster slots — 05 calls it a "dual-write
// migration", never one atomic Lua), so it runs as a Go-side shadow write.
//
// TRIGGER: lazily, on the first read miss of a sub's slot-homed hash (Get/GetMany).
// Every WRITE path already targets the slot-homed tag (redis_store computes h from
// the id), and the read entry points (maybeWake→Get, the sweep→GetMany, the callback
// →applyAck→Get) all Get before any arm/ack, so a legacy sub is migrated before any
// slot-homed mutation would otherwise see it as absent. The full sweep (List unions
// the legacy id-set) is the backstop that drains stragglers on quiet slots.

// ---- legacy (pre-slot-homing) key schema: the single fixed {__ds} tag ----

func subKeyLegacy(id string) string   { return keyPrefix + ":sub:" + id }
func linksKeyLegacy(id string) string { return keyPrefix + ":sub:" + id + ":links" }

const (
	subsKeyLegacy   = keyPrefix + ":subs"
	leaseZKeyLegacy = keyPrefix + ":sched:lease"
	retryZKeyLegacy = keyPrefix + ":sched:retry"
	dueZKeyLegacy   = keyPrefix + ":due"
)

func streamSubsKeyLegacy(path string) string { return keyPrefix + ":stream:" + path }

// migrateSub copies a subscription from the legacy {__ds} keyspace into its
// slot-homed {__ds:h} keyspace and flips (drops the legacy copy), returning whether
// a legacy copy was found and migrated. It is invoked ONLY on a slot-homed read miss
// (the caller already knows subKey(id) is absent), so a migrated sub never pays for
// it. Idempotent: a re-run re-copies (HSET overwrites the same fields) and re-flips
// (ZREM/DEL of absent members are no-ops).
func (s *RedisStore) migrateSub(id string) (bool, error) {
	ctx := s.ctx()
	fields, err := s.client.HGetAll(ctx, subKeyLegacy(id)).Result()
	if err != nil {
		return false, err
	}
	if len(fields) == 0 {
		return false, nil // genuinely absent — nothing under the legacy tag either
	}
	h := slotOf(id)

	// Shadow-write the slot-homed copy: config/runtime hash, links, id-set membership,
	// and any schedule/due entries — all under {__ds:h}.
	if err := s.client.HSet(ctx, subKey(id), fields).Err(); err != nil {
		return false, err
	}
	links, err := s.client.HGetAll(ctx, linksKeyLegacy(id)).Result()
	if err != nil {
		return false, err
	}
	if len(links) > 0 {
		m := make(map[string]any, len(links))
		for k, v := range links {
			m[k] = v
		}
		if err := s.client.HSet(ctx, linksKey(id), m).Err(); err != nil {
			return false, err
		}
	}
	if err := s.client.SAdd(ctx, subsKey(h), id).Err(); err != nil {
		return false, err
	}
	for legacyZ, newZ := range map[string]string{
		leaseZKeyLegacy: leaseZKey(h),
		retryZKeyLegacy: retryZKey(h),
		dueZKeyLegacy:   dueZKey(h),
	} {
		score, serr := s.client.ZScore(ctx, legacyZ, id).Result()
		if serr == redis.Nil {
			continue
		}
		if serr != nil {
			return false, serr
		}
		if err := s.client.ZAdd(ctx, newZ, redis.Z{Score: score, Member: id}).Err(); err != nil {
			return false, err
		}
	}
	// Re-home the fan-out membership (per-slot shard + occupied-slots bit) for every
	// linked path, so the migrated sub is reachable by the slot-homed OnStreamAppend.
	for path := range links {
		if err := s.indexStream(path, id); err != nil {
			return false, err
		}
	}

	// Flip: drop the legacy copy. Its path-keyed fan-out SET is left for the index
	// reconcile / new bitmap to supersede (a stale legacy fan-out entry is never read
	// by the slot-homed OnStreamAppend), but its id-set/schedule/hash entries go now.
	if err := s.client.Del(ctx, subKeyLegacy(id), linksKeyLegacy(id)).Err(); err != nil {
		return false, err
	}
	if err := s.client.SRem(ctx, subsKeyLegacy, id).Err(); err != nil {
		return false, err
	}
	if err := s.client.ZRem(ctx, leaseZKeyLegacy, id).Err(); err != nil {
		return false, err
	}
	if err := s.client.ZRem(ctx, retryZKeyLegacy, id).Err(); err != nil {
		return false, err
	}
	if err := s.client.ZRem(ctx, dueZKeyLegacy, id).Err(); err != nil {
		return false, err
	}
	for path := range links {
		_ = s.client.SRem(ctx, streamSubsKeyLegacy(path), id).Err()
	}
	return true, nil
}
