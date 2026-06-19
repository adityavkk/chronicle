// Package webhook implements the Durable Streams reserved subscription APIs
// (PROTOCOL §6–7, the "__ds" control plane) on Redis.
//
// It is the Redis re-implementation of the Caddy plugin's webhook engine:
// subscriptions are durable cursors that wake workers when linked streams have
// pending events, delivered either by signed webhook (push) or by pull-wake
// (the server writes wake events to a durable stream and workers claim a
// subscription-scoped lease). Where the pinned Caddy checkout keeps every
// cursor, generation, lease, and retry schedule in an in-memory map — lost on
// restart — chronicle persists all of it to Redis so the protocol's
// "MUST survive a restart" requirements hold. See docs/research/07 and 09.
//
// Vocabulary follows PROTOCOL §6–7 on the wire and in records (generation,
// holder, lease_ttl_ms, link_type, acked_offset, wake_id, pull-wake) and keeps
// the Caddy engine's structure (Subscription, Manager, Routes, the
// idle/waking/live state machine). The one deliberate rename from the Caddy
// webhook package is epoch → generation; see docs/CADDY-PARITY.md.
//
// Pure core (deterministic, clock and randomness injected): glob.go, config.go,
// ssrf.go, state.go, wire.go. Imperative shell (Redis, HTTP, time): crypto.go,
// store.go, redis_store.go, manager.go, routes.go.
package webhook

import "time"

// DispatchType is how a subscription delivers wakes (PROTOCOL §6.2).
type DispatchType string

const (
	// DispatchWebhook pushes signed wake notifications to webhook.url.
	DispatchWebhook DispatchType = "webhook"
	// DispatchPullWake writes wake events to wake_stream for workers to claim.
	DispatchPullWake DispatchType = "pull-wake"
)

// Phase is the subscription's wake/lease state. A subscription is idle when no
// lease is held and no wake is in flight (PROTOCOL §7).
type Phase string

const (
	// PhaseIdle is the rest state: no wake in flight and no lease held; eligible
	// to wake.
	PhaseIdle Phase = "idle"
	// PhaseWaking is the state after a wake has been issued (webhook POSTed or
	// wake event written) but before it is claimed or completed.
	PhaseWaking Phase = "waking"
	// PhaseLive is the state while a worker holds the lease (webhook callback
	// received without done, or pull-wake claimed); extended by heartbeats.
	PhaseLive Phase = "live"
)

// LinkType records how a stream came to be linked to a subscription
// (PROTOCOL §6.1). explicit takes precedence over glob in serialized responses.
type LinkType string

const (
	// LinkGlob marks a stream matched by the subscription's pattern.
	LinkGlob LinkType = "glob"
	// LinkExplicit marks a stream added via streams[] or the /streams endpoint.
	LinkExplicit LinkType = "explicit"
)

// Status is the serialized delivery status (PROTOCOL §6.3): active while
// delivery operates normally, failed while a webhook retry is scheduled.
type Status string

const (
	// StatusActive is the serialized status while delivery operates normally.
	StatusActive Status = "active"
	// StatusFailed is the serialized status while a webhook retry is scheduled.
	StatusFailed Status = "failed"
)

// Lease bounds, PROTOCOL §6.2: lease_ttl_ms is 1 second to 10 minutes, default
// 30 seconds.
const (
	MinLeaseTTLMs     int64 = 1_000
	MaxLeaseTTLMs     int64 = 600_000
	DefaultLeaseTTLMs int64 = 30_000
)

// Backoff bounds, PROTOCOL §7.1: exponential from 1 s to 60 s with 20% jitter.
const (
	minRetryDelay = 1 * time.Second
	maxRetryDelay = 60 * time.Second
	retryJitter   = 0.20
	// gcFailureWindow GCs a subscription whose webhook has failed continuously
	// for this long (mirrors the Caddy webhook engine's 3-day window).
	gcFailureWindow = 3 * 24 * time.Hour
)

// Config is the immutable part of a subscription — the fields hashed for
// idempotent re-confirmation (PROTOCOL §6.2). Streams is normalized (sorted,
// deduplicated) so the hash is order-independent.
type Config struct {
	Type       DispatchType
	Pattern    string
	Streams    []string
	WebhookURL string
	WakeStream string
	LeaseTTLMs int64
	// ConsistencyTier controls durability work after generation-minting writes.
	// It does not participate in ownership or lease exclusivity; the monotonic
	// generation fence remains that safety boundary.
	ConsistencyTier ConsistencyTier
	Description     string
}

// Subscription is a full subscription record: immutable Config plus the durable
// runtime state (the fields the Caddy ConsumerInstance kept in RAM).
type Subscription struct {
	ID        string
	Config    Config
	CfgHash   string
	CreatedAt time.Time

	Status     Status
	Phase      Phase
	Generation int64
	WakeID     string

	// Holder is set while a lease is held. HolderWorker names the pull-wake
	// worker; for webhook delivery the holder is the in-flight wake itself.
	Holder       bool
	HolderWorker string
	LeaseUntilNs int64

	// Retry/failure bookkeeping for webhook backoff (PROTOCOL §7.1).
	RetryCount    int
	FirstFailNs   int64
	NextAttemptNs int64

	// WakeEventSentNs marks when the current pull-wake's event was durably
	// appended to the wake stream; 0 while waking means it has not been emitted
	// yet (or its emit was not recorded), so the recovery sweep re-emits it.
	// Closes the crash window between arming a pull-wake and writing its event.
	WakeEventSentNs int64

	// ClaimLeases holds durable non-default claim-shard lease/fence states. The
	// default shard stays in the legacy fields above for upgrade compatibility.
	ClaimLeases []ClaimShardLeaseState

	// Links is hydrated from the per-subscription links HASH; nil until loaded.
	Links []StreamLink
}

// StreamLink is one stream's durable cursor within a subscription (PROTOCOL
// §6.1): the cursor that must survive a restart.
type StreamLink struct {
	Path        string
	LinkType    LinkType
	AckedOffset string
}

// StreamSnapshot is a link plus the stream's current tail and whether it has
// pending events — the per-stream shape sent in wake notifications and claim
// responses (PROTOCOL §7.1, §7.2).
type StreamSnapshot struct {
	Path        string   `json:"path"`
	LinkType    LinkType `json:"link_type"`
	AckedOffset string   `json:"acked_offset"`
	TailOffset  string   `json:"tail_offset"`
	HasPending  bool     `json:"has_pending"`
}

// Ack is a single offset acknowledgment in a callback/ack body (PROTOCOL §7.1).
type Ack struct {
	Stream string `json:"stream"`
	Offset string `json:"offset"`
}

// Error codes returned in {"error":{"code":...}} bodies. FENCED, ALREADY_CLAIMED
// and WEBHOOK_URL_REJECTED are wire-contract codes checked by the conformance
// suite; the rest are chronicle-internal.
const (
	ErrCodeFenced             = "FENCED"
	ErrCodeAlreadyClaimed     = "ALREADY_CLAIMED"
	ErrCodeWebhookURLRejected = "WEBHOOK_URL_REJECTED"
	ErrCodeInvalidRequest     = "INVALID_REQUEST"
	ErrCodeConfigConflict     = "CONFIG_CONFLICT"
	ErrCodeClaimModeConflict  = "CLAIM_MODE_CONFLICT"
	ErrCodeNotFound           = "NOT_FOUND"
	ErrCodeTokenInvalid       = "TOKEN_INVALID"
)
