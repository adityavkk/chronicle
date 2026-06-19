package chronicle

import (
	"fmt"
	"strconv"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// Environment variables recognized by Config.LoadEnv. Precedence in
// cmd/chronicle is flags over environment over defaults.
const (
	EnvListen               = "CHRONICLE_LISTEN"
	EnvRedisURL             = "CHRONICLE_REDIS_URL"
	EnvStore                = "CHRONICLE_STORE"
	EnvLongPollTimeout      = "CHRONICLE_LONG_POLL_TIMEOUT"
	EnvSSEReconnectInterval = "CHRONICLE_SSE_RECONNECT_INTERVAL"
	EnvPublicURL            = "CHRONICLE_PUBLIC_URL"
	EnvSubscriptions        = "CHRONICLE_SUBSCRIPTIONS"
	EnvWebhookAllowPrivate  = "CHRONICLE_WEBHOOK_ALLOW_PRIVATE"
	EnvSweepInterval        = "CHRONICLE_SWEEP_INTERVAL"
	EnvReconcileInterval    = "CHRONICLE_RECONCILE_INTERVAL"
	EnvSweepBatch           = "CHRONICLE_SWEEP_BATCH"
	EnvReplicaID            = "CHRONICLE_REPLICA_ID"
	EnvMemberLeaseTTL       = "CHRONICLE_MEMBER_LEASE_TTL"
	EnvHeartbeatInterval    = "CHRONICLE_HEARTBEAT_INTERVAL"
	EnvSlotLeaseTTL         = "CHRONICLE_SLOT_LEASE_TTL"
	EnvSlotReconcile        = "CHRONICLE_SLOT_RECONCILE_INTERVAL"
	EnvConsistencyTier      = "CHRONICLE_CONSISTENCY_TIER"
	EnvMetricsListen        = "CHRONICLE_METRICS_LISTEN"
)

// Config holds the chronicle server configuration. LongPollTimeout and
// SSEReconnectInterval are the Caddy plugin's knobs with the Caddy defaults;
// the rest replaces the plugin's DataDir/MaxFileHandles with chronicle's
// listen/Redis wiring.
type Config struct {
	// Listen is the HTTP listen address.
	Listen string

	// StreamRoot is the URL prefix the protocol is served under. The
	// conformance suite hardcodes "/v1/stream/". Stream paths passed to the
	// store are relative to this root (see Mount).
	StreamRoot string

	// RedisURL is the Redis connection URL for the redis backend.
	RedisURL string

	// StoreBackend selects the storage backend: "redis" or "memory".
	StoreBackend string

	// LongPollTimeout is the server-side timeout for long-poll requests.
	// Caddy default: 30s. The conformance harness overrides it to 500ms.
	LongPollTimeout time.Duration

	// SSEReconnectInterval is how often SSE connections are closed to allow
	// CDN request collapsing. Caddy default: 60s.
	SSEReconnectInterval time.Duration

	// PublicBaseURL is the externally reachable origin (scheme + host[:port])
	// the server is served behind. It is combined with StreamRoot to build the
	// absolute callback_url and jwks_url in webhook notifications and
	// subscription responses, which a webhook receiver must be able to reach.
	PublicBaseURL string

	// Subscriptions enables the reserved __ds subscription APIs. Requires the
	// redis backend (the subscription layer is Redis-backed).
	Subscriptions bool

	// WebhookAllowPrivate relaxes webhook-URL SSRF validation to accept any
	// http(s) target, including RFC1918 cluster-internal receivers. Off by
	// default; enable only on a trusted network.
	WebhookAllowPrivate bool

	// SweepInterval is the coarse recovery floor for eventless gaps (default
	// 30s). Detectable recovery events reconcile immediately; lengthen the floor
	// to trade eventless recovery latency for less steady-state Redis load on a
	// large subscription keyspace.
	SweepInterval time.Duration

	// ReconcileInterval is how often the slow reconcile loop runs (missed glob
	// links + fan-out index repair; default 30s). It scans the stream keyspace,
	// so it is deliberately separate from per-append wake delivery.
	ReconcileInterval time.Duration

	// SweepBatch caps how many subscriptions one sweep tick evaluates (0 = no
	// cap, the default). A positive cap bounds per-tick cost on a very large
	// keyspace at the price of up to ceil(K/SweepBatch) ticks of recovery latency.
	SweepBatch int

	// ReplicaID optionally fixes the process incarnation id used for
	// Redis-backed ownership. Empty defaults to POD_NAME plus a random nonce.
	ReplicaID string

	// MemberLeaseTTL, HeartbeatInterval, SlotLeaseTTL, and SlotReconcileInterval
	// tune Redis membership and slot ownership. Zero values fall back to the
	// Manager defaults: 9s, 3s, 9s, and 3s.
	MemberLeaseTTL        time.Duration
	HeartbeatInterval     time.Duration
	SlotLeaseTTL          time.Duration
	SlotReconcileInterval time.Duration

	// ConsistencyTier is the default for new subscriptions that omit
	// consistency_tier. Tier A is default; Tier B adds WAIT/WAITAOF after
	// generation-minting writes; Tier C is parsed but rejected on Redis.
	ConsistencyTier webhook.ConsistencyTier

	// MetricsListen is the address for the observability server (/metrics,
	// /healthz, /readyz). Empty (the default) disables it; a load-test or
	// production deployment sets e.g. ":9090".
	MetricsListen string
}

// DefaultConfig returns the defaults: port 4437 (the IANA-assigned Durable
// Streams port), the conformance suite's stream root, and the Caddy plugin's
// Provision defaults for the shared knobs.
func DefaultConfig() Config {
	return Config{
		Listen:                ":4437",
		StreamRoot:            "/v1/stream/",
		RedisURL:              "redis://localhost:6379",
		StoreBackend:          "redis",
		LongPollTimeout:       30 * time.Second,
		SSEReconnectInterval:  60 * time.Second,
		PublicBaseURL:         "http://localhost:4437",
		Subscriptions:         true,
		SweepInterval:         30 * time.Second,
		ReconcileInterval:     30 * time.Second,
		MemberLeaseTTL:        9 * time.Second,
		HeartbeatInterval:     3 * time.Second,
		SlotLeaseTTL:          9 * time.Second,
		SlotReconcileInterval: 3 * time.Second,
		ConsistencyTier:       webhook.ConsistencyTierA,
	}
}

// LoadEnv overlays configuration from environment variables onto c. lookup
// is os.LookupEnv in production; it is a parameter so tests can inject one.
func (c *Config) LoadEnv(lookup func(key string) (value string, ok bool)) error {
	if v, ok := lookup(EnvListen); ok {
		c.Listen = v
	}
	if v, ok := lookup(EnvRedisURL); ok {
		c.RedisURL = v
	}
	if v, ok := lookup(EnvStore); ok {
		c.StoreBackend = v
	}
	if v, ok := lookup(EnvLongPollTimeout); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvLongPollTimeout, err)
		}
		c.LongPollTimeout = d
	}
	if v, ok := lookup(EnvSSEReconnectInterval); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvSSEReconnectInterval, err)
		}
		c.SSEReconnectInterval = d
	}
	if v, ok := lookup(EnvPublicURL); ok {
		c.PublicBaseURL = v
	}
	if v, ok := lookup(EnvSubscriptions); ok {
		c.Subscriptions = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvWebhookAllowPrivate); ok {
		c.WebhookAllowPrivate = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvSweepInterval); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvSweepInterval, err)
		}
		c.SweepInterval = d
	}
	if v, ok := lookup(EnvReconcileInterval); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvReconcileInterval, err)
		}
		c.ReconcileInterval = d
	}
	if v, ok := lookup(EnvSweepBatch); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvSweepBatch, err)
		}
		c.SweepBatch = n
	}
	if v, ok := lookup(EnvReplicaID); ok {
		c.ReplicaID = v
	}
	if v, ok := lookup(EnvMemberLeaseTTL); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvMemberLeaseTTL, err)
		}
		c.MemberLeaseTTL = d
	}
	if v, ok := lookup(EnvHeartbeatInterval); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvHeartbeatInterval, err)
		}
		c.HeartbeatInterval = d
	}
	if v, ok := lookup(EnvSlotLeaseTTL); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvSlotLeaseTTL, err)
		}
		c.SlotLeaseTTL = d
	}
	if v, ok := lookup(EnvSlotReconcile); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvSlotReconcile, err)
		}
		c.SlotReconcileInterval = d
	}
	if v, ok := lookup(EnvConsistencyTier); ok {
		tier, err := webhook.ParseConsistencyTier(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvConsistencyTier, err)
		}
		c.ConsistencyTier = tier
	}
	if v, ok := lookup(EnvMetricsListen); ok {
		c.MetricsListen = v
	}
	return nil
}
