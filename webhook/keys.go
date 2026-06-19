package webhook

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// keys.go is the key schema for the reserved subscription control plane and the
// PURE CORE of slot-homing (Move 1, docs/specs/horizontal-scale/research/05). It
// homes a WHOLE subscription's key set — its config/runtime hash, links hash,
// lease/retry/due schedule ZSETs, and per-stream fan-out membership — under ONE
// hash tag {__ds:h}, h = fnv32a(subId) % subSlots, so every multi-key atomic Lua
// script (ack.lua 4-5 keys, delete_sub.lua 5 keys, arm_wake/claim) stays
// byte-for-byte single-slot and cluster-safe: sharding is "compute the tag," not
// "rewrite the atomicity contract" (05:138-157). These functions are total and
// I/O-free (mirror state.go / shard.go); the S-parallel pipelining and bitmap
// probing live in the thin shell (redis_store.go / manager.go).
//
// THREE COEXISTING LAYERS — do not conflate (05, design 08):
//   - Slot h (THIS file): keyspace shard, h = fnv32a(subId) % subSlots.
//   - Shard g (#11, claim granularity): subShardKey(id,g)/shardMember(id,g) are
//     built on subKey(id), so they AUTOMATICALLY inherit slot h — a sub's g-shards
//     live in its slot. slotOf strips the ":g:<n>" suffix so a drained g>0 schedule
//     member resolves back to its parent sub's slot (and thus to subShardKey).
//   - {ownership} keys (#14): slotKey(h)/membersKey keep their OWN literal
//     {ownership} tag (cross-slot cluster-membership metadata) — NOT slot-homed.

// subSlots (S) is the number of keyspace slots a subscription can be homed into. It
// is a COMPILE-TIME CONSTANT, immutable for the life of a keyspace: changing it
// re-tags every key, so a change is a dual-write migration (read old tag, write new
// tag, flip — see migrate.go), never a config edit. 256 comfortably exceeds any
// expected replica count and is the upper bound gate #2 measures (05:138-141).
const subSlots = 256

// dsTag / keyPrefix are the FIXED single-slot tag for cluster-wide SINGLETON keys
// that are NOT slot-homed (the signing JWKS, the active kid, the HMAC token key):
// one of each exists for the whole keyspace, so there is no id to home them by, and
// get_or_create_key.lua touches jwks+active_kid together (one slot). It is ALSO the
// legacy tag every per-sub key used before slot-homing — the migration reads the
// old {__ds} location and writes the new {__ds:h} one (migrate.go).
const (
	dsTag     = "{__ds}"
	keyPrefix = "ds:" + dsTag
)

// baseSubID strips a "<id>:g:<n>" claim-granularity shard suffix (shardMember's
// g>0 form) to the base subscription id. The slot is computed from the BASE id so a
// sub's g>0 shard hash homes to the SAME slot as its parent sub (the #11 split
// inherits the #15 slot tag), and so a lease/retry/due worker that drained the
// member "<id>:g:<n>" resolves subKey(member) back to subShardKey(id,n) in that one
// slot — without it the shard would CROSSSLOT or strand. Stripping only when the
// suffix is ":g:<digits>" matches shardMember byte-for-byte; a real id that happens
// to end that way still hashes deterministically to one slot (only its co-location
// coincidence changes, never correctness).
func baseSubID(id string) string {
	i := strings.LastIndex(id, ":g:")
	if i < 0 {
		return id
	}
	suffix := id[i+3:]
	if suffix == "" {
		return id
	}
	for k := 0; k < len(suffix); k++ {
		if suffix[k] < '0' || suffix[k] > '9' {
			return id
		}
	}
	return id[:i]
}

// slotOf is the keyspace slot a subscription is homed into: h = fnv32a(baseSubID) %
// subSlots, using Go's hash/fnv (FNV-1a, 32-bit). It is DELIBERATELY NOT Redis
// CRC16: the Redis client already CRC16-hashes the {…} tag to a cluster slot, so h
// must be an INDEPENDENT, language-stable application choice, or we re-introduce the
// CROSSSLOT slot-homing is meant to kill (05:148). Total — every id yields a slot.
func slotOf(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(baseSubID(id)))
	return int(h.Sum32() % uint32(subSlots))
}

// slotTagAt is the {__ds:h} hash tag for an explicit slot index h. The per-slot
// schedule/index keys (subsKey/leaseZKey/retryZKey/dueZKey) build their tag from a
// slot the caller already holds (an owned slot, or slotOf(id) computed once).
func slotTagAt(h int) string { return "{__ds:" + strconv.Itoa(h) + "}" }

// slotTag is the {__ds:h} hash tag a subscription id is homed under:
// slotTag(id) = "{__ds:" + itoa(fnv32a(id)%subSlots) + "}". The per-sub keys derive
// their tag from the id through this one helper, so a store method that computes the
// id's slot once and reuses it builds the sub's whole key set in a single slot.
func slotTag(id string) string { return slotTagAt(slotOf(id)) }

// subKey is the HASH holding a subscription's config and runtime state, homed under
// the sub's slot tag. The id (full, suffix included) is appended AFTER the tag, so
// an id carrying its own braces cannot escape the tag (the tag is the first {...}
// pair). subKey(shardMember(id,g)) == subShardKey(id,g) by construction (the tag is
// slotOf-stripped, the body keeps the full member).
func subKey(id string) string { return "ds:" + slotTag(id) + ":sub:" + id }

// subShardKey is the HASH holding shard g's per-(subId,g) fence
// (generation/wake_id/phase/holder/lease_until_ns) under claim granularity
// (design 08 §4). Shard 0 lives in the main sub hash (subKey) — so the config
// existence check, the fence, and the schedule member coincide exactly as today —
// and only g>0 gets its own hash. At G=1 (only shard 0) this is byte-for-byte
// today's keyspace; the granularity split is purely additive. It APPENDS to
// subKey(id), so it inherits the sub's slot tag (a sub's g-shards live in its slot).
func subShardKey(id string, g int) string {
	if g <= 0 {
		return subKey(id)
	}
	return subKey(id) + ":g:" + strconv.Itoa(g)
}

// shardMember is the lease/retry/due ZSET member for shard g. Shard 0 keeps the
// bare id (today's member); g>0 derives "<id>:g:<g>", which is exactly the
// subKey of subShardKey(id,g), so expire_lease.lua and the manager's lease worker
// operate on a g>0 shard unchanged (a shard's fence hash is just a sub-keyed
// record at a derived id). baseSubID strips this suffix so the derived id homes to
// the parent sub's slot.
func shardMember(id string, g int) string {
	if g <= 0 {
		return id
	}
	return id + ":g:" + strconv.Itoa(g)
}

// linksKey is the HASH of a subscription's per-stream cursors:
// field=<stream path> -> "<link_type>:<acked_offset>", homed under the sub's slot.
// The cursor hash is shared across a subscription's g-shards (cursors are
// forward-only watermarks), so it is keyed by the base id, never a shard member.
func linksKey(id string) string { return "ds:" + slotTag(id) + ":sub:" + id + ":links" }

// ---- per-slot schedule / index keys (slot-homed, func(h int)) ----
//
// These were single global constants before slot-homing; now there is one per slot,
// so the lease/retry/due ZSETs shard WITH the subs and the single global schedule
// ceiling is gone (05:152-157). claim_due.lua (1 key) runs unchanged, once per slot.
// A store method computes h once (slotOf(id), or an owned slot) and passes it here.

// subsKey is the SET of subscription ids homed in slot h.
func subsKey(h int) string { return "ds:" + slotTagAt(h) + ":subs" }

// leaseZKey is slot h's lease schedule ZSET (member -> lease_expiry_ns).
func leaseZKey(h int) string { return "ds:" + slotTagAt(h) + ":sched:lease" }

// retryZKey is slot h's retry schedule ZSET (member -> next_attempt_ns).
func retryZKey(h int) string { return "ds:" + slotTagAt(h) + ":sched:retry" }

// dueZKey is slot h's "needs a wake" outbox ZSET (member -> now_ns at arm/append
// time; NOT a deadline). arm_wake ZADDs it, ack(done)/release ZREM it, expire_lease
// re-owes it, and the per-slot dueWorker drains it in O(owed). Kept as the single
// due-key helper (#12) so its re-tag to a per-slot key was this one-line change.
func dueZKey(h int) string { return "ds:" + slotTagAt(h) + ":due" }

// streamSubsKey is the per-stream fan-out SET of subscriber ids homed in slot h —
// one shard of a stream's fan-out per keyspace slot (05:194). A subscriber linked to
// <path> is SADDed into streamSubsKey(slotOf(id), path), co-located with its own
// {__ds:h} keys. OnStreamAppend scatter-gathers across the occupied slots.
func streamSubsKey(h int, path string) string { return "ds:" + slotTagAt(h) + ":stream:" + path }

// streamSlotsKey is the per-stream occupied-slots bitmap: a 256-bit STRING whose bit
// h is set when slot h has at least one subscriber for <path>. It carries its OWN
// literal {__ds-occ} tag (cross-slot fan-out metadata, like {ownership}), not a
// per-slot tag — it is read once to decide which slots OnStreamAppend probes.
// SETBIT h on indexStream, NEVER cleared on deindex (a stale set bit only costs one
// empty SMEMBERS, so it is race-safe), repaired by the reconcile loop (05:496-500).
func streamSlotsKey(path string) string { return "ds:{__ds-occ}:streamslots:" + path }

// OccupiedSlots is the decoded per-stream occupied-slots bitmap (the value of
// streamSlotsKey): the set of keyspace slots that have at least one subscriber for a
// stream. It is a TYPED value, not a bare bit-string, so OnStreamAppend's
// scatter-gather probes a parsed slot set rather than re-deriving bit offsets at the
// call site (parse, don't validate). Bits are MSB-first within each byte, matching
// Redis SETBIT offset semantics ("counting from the most significant bit of the
// first byte"); bits at or beyond subSlots are ignored (defensive).
type OccupiedSlots struct{ slots []int }

// decodeOccupiedSlots parses a raw bitmap string (the GET of streamSlotsKey; "" when
// the key is absent) into the ascending set of occupied slot indices. Pure.
func decodeOccupiedSlots(raw string) OccupiedSlots {
	if raw == "" {
		return OccupiedSlots{}
	}
	out := make([]int, 0, 8)
	for byteIdx := 0; byteIdx < len(raw); byteIdx++ {
		b := raw[byteIdx]
		if b == 0 {
			continue
		}
		for bit := 0; bit < 8; bit++ {
			if b&(1<<(7-uint(bit))) != 0 {
				if h := byteIdx*8 + bit; h < subSlots {
					out = append(out, h)
				}
			}
		}
	}
	return OccupiedSlots{slots: out}
}

// Slots is the ascending occupied slot indices.
func (o OccupiedSlots) Slots() []int { return o.slots }

// Len is the number of occupied slots (the slotsProbed the FanOut metric records).
func (o OccupiedSlots) Len() int { return len(o.slots) }

// ---- cluster-wide singleton keys (NOT slot-homed; fixed {__ds} tag) ----

const (
	jwksKey      = keyPrefix + ":jwks"       // HASH kid -> key material
	activeKidKey = keyPrefix + ":active_kid" // STRING current signing kid
	tokenKeyKey  = keyPrefix + ":tokenkey"   // STRING HMAC token key
)

// ---- {ownership} keyspace (issue #14, work-sharded leased slot ownership) ----
//
// The membership ZSET and the per-slot ownership records use their OWN literal
// hash tag {ownership}, deliberately NOT slot-homed: they are cross-slot
// cluster-membership metadata, separate from the per-subscription {__ds:h} control
// plane (05:311). This ownership axis (which REPLICA runs autonomous background
// work for a slot of the keyspace) is ORTHOGONAL to the per-(subId,g) claim
// granularity above (#11): different keys, different tag, the owner-epoch fence
// vs the (gen,wake_id) fence. On a single-node Redis (the deploy/test substrate)
// one EVAL may touch an {ownership} slot key alongside the {__ds:h} schedule keys —
// the TOCTOU inline checks rely on that atomicity; co-locating the two tags for a
// real Redis Cluster is out of scope here (the state shard is #15, DR is #16).
const ownershipTag = "{ownership}"

// membersKey is the ZSET of live replica ids -> heartbeat lease-expiry ns: every
// replica ZADDs itself each heartbeatInterval and evicts entries past their TTL
// with ZREMRANGEBYSCORE. This Redis ZSET (not Kubernetes) decides which replicas
// are eligible to own subscription slots.
const membersKey = "ds:" + ownershipTag + ":members"

// slotKey is the HASH holding ownership slot h's CAS record
// {owner_id, owner_epoch, lease_expiry_ns}, claimed/renewed by claim_shard.lua
// and read by check_owner.lua. The literal "ds:{ownership}:slot:<h>" form matches
// the keyspace block in doc-05 and the jepsen killSlotOwner nemesis.
func slotKey(h int) string { return "ds:" + ownershipTag + ":slot:" + strconv.Itoa(h) }
