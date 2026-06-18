package webhook

import "strconv"

// Key schema for the reserved subscription control plane. Every key shares the
// fixed {__ds} hash tag so all subscription-control Lua scripts touch a single
// slot and are cluster-safe (docs/research/09 §2). The subscription id is
// appended after the tag, so an id containing braces cannot escape the tag (the
// tag is the first {...} pair, which is always {__ds}). The high-cardinality
// per-stream log data stays sharded under each stream's own {<path>} tag.
const (
	dsTag     = "{__ds}"
	keyPrefix = "ds:" + dsTag

	subsKey      = keyPrefix + ":subs"        // SET of subscription ids
	leaseZKey    = keyPrefix + ":sched:lease" // ZSET id -> lease_expiry_ns
	retryZKey    = keyPrefix + ":sched:retry" // ZSET id -> next_attempt_ns
	jwksKey      = keyPrefix + ":jwks"        // HASH kid -> key material
	activeKidKey = keyPrefix + ":active_kid"  // STRING current signing kid
	tokenKeyKey  = keyPrefix + ":tokenkey"    // STRING HMAC token key
)

// subKey is the HASH holding a subscription's config and runtime state.
func subKey(id string) string { return keyPrefix + ":sub:" + id }

// subShardKey is the HASH holding shard g's per-(subId,g) fence
// (generation/wake_id/phase/holder/lease_until_ns) under claim granularity
// (design 08 §4). Shard 0 lives in the main sub hash (subKey) — so the config
// existence check, the fence, and the schedule member coincide exactly as today
// — and only g>0 gets its own hash. At G=1 (only shard 0) this is byte-for-byte
// today's keyspace; the granularity split is purely additive.
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
// record at a derived id).
func shardMember(id string, g int) string {
	if g <= 0 {
		return id
	}
	return id + ":g:" + strconv.Itoa(g)
}

// linksKey is the HASH of a subscription's per-stream cursors:
// field=<stream path> -> "<link_type>:<acked_offset>".
func linksKey(id string) string { return keyPrefix + ":sub:" + id + ":links" }

// streamSubsKey is the per-stream fan-out SET of subscription ids linked to a
// stream. Maintained from Go as a best-effort index reconciled by the sweep.
func streamSubsKey(path string) string { return keyPrefix + ":stream:" + path }
