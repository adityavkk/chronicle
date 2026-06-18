package webhook

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
	dueZKey      = keyPrefix + ":due"         // ZSET id -> owed_at_ns
	jwksKey      = keyPrefix + ":jwks"        // HASH kid -> key material
	activeKidKey = keyPrefix + ":active_kid"  // STRING current signing kid
	tokenKeyKey  = keyPrefix + ":tokenkey"    // STRING HMAC token key
)

// dueSetKey centralizes the due outbox key derivation so later slot-homing only
// has one call surface to re-tag.
func dueSetKey() string { return dueZKey }

// subKey is the HASH holding a subscription's config and runtime state.
func subKey(id string) string { return keyPrefix + ":sub:" + id }

// linksKey is the HASH of a subscription's per-stream cursors:
// field=<stream path> -> "<link_type>:<acked_offset>".
func linksKey(id string) string { return keyPrefix + ":sub:" + id + ":links" }

// streamSubsKey is the per-stream fan-out SET of subscription ids linked to a
// stream. Maintained from Go as a best-effort index reconciled by the sweep.
func streamSubsKey(path string) string { return keyPrefix + ":stream:" + path }
