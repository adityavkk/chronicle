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
	EnvMetricsListen        = "CHRONICLE_METRICS_LISTEN"
	// Tunable-consistency surface (issue #16, doc 05 "Tunable consistency").
	EnvConsistencyTier = "CHRONICLE_CONSISTENCY_TIER" // A (default) | B | C
	EnvWaitReplicas    = "CHRONICLE_WAIT_REPLICAS"    // Tier B WAITAOF numreplicas (1 on STANDARD_HA, 0 on a single Redis)
	EnvWaitTimeoutMs   = "CHRONICLE_WAIT_TIMEOUT_MS"  // Tier B WAIT/WAITAOF server-side block bound
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

	// SweepInterval is how often the recovery sweep re-evaluates every
	// subscription against its durable cursor (default 2s). The sweep is the
	// backstop for a wake lost to a crash; lengthen it to trade recovery latency
	// for less steady-state Redis load on a large subscription keyspace.
	SweepInterval time.Duration

	// ReconcileInterval is how often the slow reconcile loop runs (missed glob
	// links + fan-out index repair; default 30s). It scans the stream keyspace,
	// so it is deliberately slower than the sweep.
	ReconcileInterval time.Duration

	// SweepBatch caps how many subscriptions one sweep tick evaluates (0 = no
	// cap, the default). A positive cap bounds per-tick cost on a very large
	// keyspace at the price of up to ceil(K/SweepBatch) ticks of recovery latency.
	SweepBatch int

	// MetricsListen is the address for the observability server (/metrics,
	// /healthz, /readyz). Empty (the default) disables it; a load-test or
	// production deployment sets e.g. ":9090".
	MetricsListen string

	// Consistency is the tunable-consistency tier for the fence-minting writes
	// (issue #16, doc 05). Parsed into the sealed webhook.ConsistencyTier at the env
	// boundary (parse, don't validate). TierA (no WAIT, the default) keeps today's
	// best-latency behavior; TierB adds the WAITAOF durability barrier; TierC is the
	// read-your-writes config surface (freshness token designed + stubbed). Only
	// Tier B touches the hot path.
	Consistency webhook.ConsistencyTier

	// WaitReplicas is the Tier B WAITAOF replica requirement (1 on the STANDARD_HA
	// substrate — the Redis Software HA per-shard ceiling; 0 on a single Redis with
	// AOF — local fsync only). Ignored by Tier A/C.
	WaitReplicas int

	// WaitTimeoutMs bounds the Tier B WAIT/WAITAOF server-side block; on timeout the
	// achieved-ack count is checked and a short reply is surfaced as an error.
	WaitTimeoutMs int
}

// DefaultConfig returns the defaults: port 4437 (the IANA-assigned Durable
// Streams port), the conformance suite's stream root, and the Caddy plugin's
// Provision defaults for the shared knobs.
func DefaultConfig() Config {
	return Config{
		Listen:               ":4437",
		StreamRoot:           "/v1/stream/",
		RedisURL:             "redis://localhost:6379",
		StoreBackend:         "redis",
		LongPollTimeout:      30 * time.Second,
		SSEReconnectInterval: 60 * time.Second,
		PublicBaseURL:        "http://localhost:4437",
		Subscriptions:        true,
		SweepInterval:        2 * time.Second,
		ReconcileInterval:    30 * time.Second,
		Consistency:          webhook.TierA, // no WAIT by default — best latency, at-least-once
		WaitReplicas:         1,             // the realistic Redis Software HA ceiling (06:70), used only by Tier B
		WaitTimeoutMs:        1000,
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
	if v, ok := lookup(EnvMetricsListen); ok {
		c.MetricsListen = v
	}
	if v, ok := lookup(EnvConsistencyTier); ok {
		tier, err := webhook.ParseConsistencyTier(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvConsistencyTier, err)
		}
		c.Consistency = tier
	}
	if v, ok := lookup(EnvWaitReplicas); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvWaitReplicas, err)
		}
		c.WaitReplicas = n
	}
	if v, ok := lookup(EnvWaitTimeoutMs); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvWaitTimeoutMs, err)
		}
		c.WaitTimeoutMs = n
	}
	return nil
}
