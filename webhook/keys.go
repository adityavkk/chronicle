package webhook

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// Key schema for the reserved subscription control plane. Whole subscriptions
// are homed into one of 256 application slots, each with its own Redis hash tag
// {__ds:h}. Every Lua script key set for one subscription is built from the same
// h, so Redis Cluster sees one slot and the scripts remain atomic. The legacy
// {__ds} keys are kept only for lazy migration of pre-slot-homing records.
const (
	subSlots = 256

	legacyDSTag     = "{__ds}"
	legacyKeyPrefix = "ds:" + legacyDSTag
	ownershipTag    = "{ownership}"
	ownershipKey    = "ds:" + ownershipTag
	occupiedTag     = "{__ds-occ}"
	occupiedPrefix  = "ds:" + occupiedTag

	membersKey   = ownershipKey + ":members"       // ZSET replica_id -> lease_expiry_ns
	jwksKey      = legacyKeyPrefix + ":jwks"       // HASH kid -> key material
	activeKidKey = legacyKeyPrefix + ":active_kid" // STRING current signing kid
	tokenKeyKey  = legacyKeyPrefix + ":tokenkey"   // STRING HMAC token key
)

func fnv32a(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func subscriptionSlot(id string) int {
	return int(fnv32a(id) % subSlots)
}

func slotTag(id string) string {
	return slotTagFor(subscriptionSlot(id))
}

func slotTagFor(h int) string {
	return "{__ds:" + strconv.Itoa(h) + "}"
}

func keyPrefix(h int) string {
	return "ds:" + slotTagFor(h)
}

func legacySubsKey() string                  { return legacyKeyPrefix + ":subs" }
func legacyLeaseZKey() string                { return legacyKeyPrefix + ":sched:lease" }
func legacyRetryZKey() string                { return legacyKeyPrefix + ":sched:retry" }
func legacyDueSetKey() string                { return legacyKeyPrefix + ":due" }
func legacySubKey(id string) string          { return legacyKeyPrefix + ":sub:" + id }
func legacyLinksKey(id string) string        { return legacyKeyPrefix + ":sub:" + id + ":links" }
func legacyStreamSubsKey(path string) string { return legacyKeyPrefix + ":stream:" + path }

// subsKey is the per-slot SET of subscription ids.
func subsKey(h int) string { return keyPrefix(h) + ":subs" }

// leaseZKey is the per-slot ZSET id/member -> lease_expiry_ns.
func leaseZKey(h int) string { return keyPrefix(h) + ":sched:lease" }

// retryZKey is the per-slot ZSET id -> next_attempt_ns.
func retryZKey(h int) string { return keyPrefix(h) + ":sched:retry" }

// dueSetKey is the per-slot ZSET id -> owed_at_ns.
func dueSetKey(h int) string { return keyPrefix(h) + ":due" }

// subKey is the HASH holding a subscription's config and runtime state.
func subKey(id string) string {
	return subKeyForSlot(id, subscriptionSlot(id))
}

func subKeyForSlot(id string, h int) string { return keyPrefix(h) + ":sub:" + id }

// linksKey is the HASH of a subscription's per-stream cursors:
// field=<stream path> -> "<link_type>:<acked_offset>".
func linksKey(id string) string {
	return linksKeyForSlot(id, subscriptionSlot(id))
}

func linksKeyForSlot(id string, h int) string { return keyPrefix(h) + ":sub:" + id + ":links" }

// streamSubsKey is the per-slot, per-stream fan-out SET of subscription ids
// linked to a stream. Maintained from Go as a best-effort index reconciled by
// the sweep.
func streamSubsKey(h int, path string) string { return keyPrefix(h) + ":stream:" + path }

func occupiedStreamSlotsKey(path string) string { return occupiedPrefix + ":streamslots:" + path }

// ownershipSlotKey is the HASH {owner_id, owner_epoch, lease_expiry_ns} for one
// background-work ownership slot. It is co-located with the subscription-control
// keys that inline-check owner epochs.
func ownershipSlotKey(slot OwnershipSlot) string {
	return keyPrefix(slot.Int()) + ":owner:slot:" + slot.String()
}

func redisHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

func validateSingleHashTag(keys []string) error {
	if len(keys) == 0 {
		return fmt.Errorf("empty Redis script key set")
	}
	want := redisHashTag(keys[0])
	for _, key := range keys[1:] {
		if got := redisHashTag(key); got != want {
			return fmt.Errorf("mixed Redis hash tags: first=%q key=%q tag=%q", want, key, got)
		}
	}
	return nil
}
