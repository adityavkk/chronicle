package chronicle

import (
	"fmt"
	"time"
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
	return nil
}
