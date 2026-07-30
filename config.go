package chronicle

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// Environment variables recognized by Config.LoadEnv. Precedence in
// cmd/chronicle is flags over environment over defaults.
const (
	EnvListen                = "CHRONICLE_LISTEN"
	EnvRedisURL              = "CHRONICLE_REDIS_URL"
	EnvRedisPoolSize         = "CHRONICLE_REDIS_POOL_SIZE"
	EnvStore                 = "CHRONICLE_STORE"
	EnvSegmentMode           = "CHRONICLE_SEGMENT_MODE"
	EnvSegmentDir            = "CHRONICLE_SEGMENT_DIR"
	EnvSegmentTargetBytes    = "CHRONICLE_SEGMENT_TARGET_BYTES"
	EnvSegmentIndexStride    = "CHRONICLE_SEGMENT_INDEX_STRIDE"
	EnvSegmentCacheBytes     = "CHRONICLE_SEGMENT_CACHE_BYTES"
	EnvSegmentAutoSealRead   = "CHRONICLE_SEGMENT_AUTO_SEAL_READ"
	EnvSegmentInitialState   = "CHRONICLE_SEGMENT_INITIAL_STATE"
	EnvLongPollTimeout       = "CHRONICLE_LONG_POLL_TIMEOUT"
	EnvSSEReconnectInterval  = "CHRONICLE_SSE_RECONNECT_INTERVAL"
	EnvReadPageBytes         = "CHRONICLE_READ_PAGE_BYTES"
	EnvSSEHubReplayBytes     = "CHRONICLE_SSE_HUB_REPLAY_BYTES"
	EnvSSEHubBatchBytes      = "CHRONICLE_SSE_HUB_BATCH_BYTES"
	EnvSSEClientWriteTimeout = "CHRONICLE_SSE_CLIENT_WRITE_TIMEOUT"
	EnvPublicURL             = "CHRONICLE_PUBLIC_URL"
	EnvSubscriptions         = "CHRONICLE_SUBSCRIPTIONS"
	EnvUI                    = "CHRONICLE_UI"
	EnvUIServer              = "CHRONICLE_UI_SERVER"
	EnvWebhookAllowPrivate   = "CHRONICLE_WEBHOOK_ALLOW_PRIVATE"
	EnvSweepInterval         = "CHRONICLE_SWEEP_INTERVAL"
	EnvReconcileInterval     = "CHRONICLE_RECONCILE_INTERVAL"
	EnvSweepBatch            = "CHRONICLE_SWEEP_BATCH"
	EnvMetricsListen         = "CHRONICLE_METRICS_LISTEN"
	EnvMetricsPprof          = "CHRONICLE_METRICS_PPROF"
	// Tunable-consistency surface (issue #16, doc 05 "Tunable consistency").
	EnvConsistencyTier = "CHRONICLE_CONSISTENCY_TIER" // A (default) | B | C
	EnvWaitReplicas    = "CHRONICLE_WAIT_REPLICAS"    // Tier B WAITAOF numreplicas (1 on STANDARD_HA, 0 on a single Redis)
	EnvWaitTimeoutMs   = "CHRONICLE_WAIT_TIMEOUT_MS"  // Tier B WAIT/WAITAOF server-side block bound
	// Stream authn/authz enforcement (issue #126): insecure (default) | enforce.
	EnvAuthMode = "CHRONICLE_AUTH_MODE"
	// Trusted service principals (issue #126 TB4, trusted-backend mode).
	EnvServiceBearer          = "CHRONICLE_SERVICE_BEARER"            // "token" or "name:token[,name:token...]"
	EnvTrustedSPIFFE          = "CHRONICLE_TRUSTED_SPIFFE_IDS"        // comma-separated spiffe:// allowlist (mesh add-on)
	EnvXFCCRequiredHeader     = "CHRONICLE_XFCC_REQUIRED_HEADER"      // optional "Name: value" sidecar marker gating XFCC trust
	EnvXFCCTrustWithoutMarker = "CHRONICLE_XFCC_TRUST_WITHOUT_MARKER" // explicit opt-in to trust XFCC with no marker (#130): fail-closed default requires it when TrustedSPIFFE is set without a marker
	// Key custody (issues #123/#126): path to a mounted secrets file holding the
	// Ed25519 signing key(s) + HMAC token key. Unset = keys live in Redis.
	EnvKeysFile = "CHRONICLE_KEYS_FILE"
	// EnvKeysFileAllowGroupRead is the explicit opt-in to load a group-readable
	// (never group-writable, never world-anything) keys file (#131): the
	// fail-closed default refuses group-read since on a shared group it is
	// read-to-forge, but a non-root container reading a root-owned secret via a
	// dedicated Kubernetes fsGroup legitimately needs the group-read bit.
	EnvKeysFileAllowGroupRead = "CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ"
	// Key rotation (#123): override for BOTH Ed25519 families' overlap window —
	// how long a retiring kid keeps verifying after its successor takes over.
	// Unset keeps the per-family defaults derived from each family's maximum
	// token lifetime (wake: 60s cap + skew; webhook envelope/caller: 1h + skew).
	EnvKeyRotationOverlap = "CHRONICLE_KEY_ROTATION_OVERLAP"
	// wake_token audience (#123/#126 TB6a): the egress gateway aud claim.
	EnvWakeTokenAud = "CHRONICLE_WAKE_TOKEN_AUD"
	// OIDC user principals (issue #126 TB5, multi-issuer verify). All three
	// are required together; the namespace claim's value→scope mapping is
	// IdP-side deployment configuration (the Q1 decision).
	EnvOIDCIssuer   = "CHRONICLE_OIDC_ISSUER"
	EnvOIDCAudience = "CHRONICLE_OIDC_AUDIENCE"
	EnvOIDCNSClaim  = "CHRONICLE_OIDC_NS_CLAIM"
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
	// RedisPoolSize overrides go-redis' per-node connection pool size when >0.
	RedisPoolSize int

	// StoreBackend selects the storage backend: "redis" or "memory".
	StoreBackend string

	// SegmentMode feature-gates the immutable sealed-prefix read plane:
	// "off" (default), "redis-chunks", "local-files", or "object-cache".
	// Redis remains the complete acknowledged-write authority in every mode.
	SegmentMode string
	// SegmentDir is required by local-files and object-cache. Object-cache
	// creates separate object-origin and bounded-cache trees beneath it.
	SegmentDir string
	// SegmentTargetBytes is the approximate immutable data-blob size.
	SegmentTargetBytes int
	// SegmentIndexStride emits one fixed-width sparse boundary per N records.
	SegmentIndexStride int
	// SegmentCacheBytes bounds object-cache's local cache.
	SegmentCacheBytes int64
	// SegmentAutoSealRead reconciles Redis into a new durable generation before
	// a catch-up read. It is useful for the prototype and local evidence runs;
	// a production follow-up should use a background sealing queue.
	SegmentAutoSealRead bool
	// SegmentInitialState is "shadow" or "serving". Cutover is an explicit
	// control-plane transition and is never a startup default.
	SegmentInitialState string

	// LongPollTimeout is the server-side timeout for long-poll requests.
	// Caddy default: 30s. The conformance harness overrides it to 500ms.
	LongPollTimeout time.Duration

	// SSEReconnectInterval is how often SSE connections are closed to allow
	// CDN request collapsing. Caddy default: 60s.
	SSEReconnectInterval time.Duration

	// ReadPageBytes is the returned storage page payload target for catch-up.
	// The default is 1 MiB. One valid frame may exceed the target.
	ReadPageBytes int

	// SSEHubReplayBytes bounds each active stream's shared live replay window.
	// Clients that fall behind reconnect from their last durable offset.
	SSEHubReplayBytes int

	// SSEHubBatchBytes is the target retained size of one shared SSE event.
	// A single message may exceed it because Chronicle never splits a message.
	SSEHubBatchBytes int

	// SSEClientWriteTimeout bounds one shared event flush to one SSE client.
	SSEClientWriteTimeout time.Duration

	// PublicBaseURL is the externally reachable origin (scheme + host[:port])
	// the server is served behind. It is combined with StreamRoot to build the
	// absolute callback_url and jwks_url in webhook notifications and
	// subscription responses, which a webhook receiver must be able to reach.
	PublicBaseURL string

	// Subscriptions enables the reserved __ds subscription APIs. Requires the
	// redis backend (the subscription layer is Redis-backed).
	Subscriptions bool

	// UI serves the embedded dsui console (and its /dsui-config.json) alongside
	// the API. Default true, but only takes effect if the UI was built into the
	// binary; set false (CHRONICLE_UI=false) to run the backend API-only from the
	// same binary — the UI stays fully decoupled and optional.
	UI bool

	// UIServer overrides the server URL the served console prefills. Empty (the
	// default) means same-origin: the console drives whatever chronicle served it.
	// Set it to point the console at a different chronicle instance.
	UIServer string

	// WebhookAllowPrivate relaxes webhook-URL SSRF validation to accept any
	// http(s) target, including RFC1918 cluster-internal receivers. Off by
	// default; enable only on a trusted network.
	WebhookAllowPrivate bool

	// SweepInterval is the coarse recovery FLOOR — how often the full cursor
	// reconcile runs on a timer with no triggering event (issue #13; default 30s).
	// It is NOT the old 2s fast sweep: the latency-sensitive recovery cases are now
	// event-triggered (boot, a Redis reconnect, an append/delivery error and, from
	// #14, an owner-epoch bump each fire a reconcile at the moment they happen), so
	// the timer only bounds the one eventless case (an owed mark on an unowned,
	// quiet slot). Steady-state delivery latency is unchanged by the longer floor.
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

	// MetricsPprof exposes Go runtime profiles under /debug/pprof/ on the
	// observability server. It is disabled by default because profiles can
	// reveal process internals and add sampling overhead.
	MetricsPprof bool

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

	// KeysFile, when non-empty, loads the subscription layer's key material
	// (Ed25519 signing key(s) + HMAC token key) from a mounted secrets file
	// instead of Redis (issues #123/#126 custody). Empty keeps Redis custody.
	KeysFile string

	// KeysFileAllowGroupRead is the explicit opt-in to load a group-readable
	// keys file (issue #126 hardening, #131). The default is fail closed:
	// group-read (like world-access and any write bit) refuses to load, because
	// on a shared group it is read-to-forge. Set it only for the documented
	// Kubernetes fsGroup exception — a non-root container reading a root-owned
	// secret through the group bit — where 0400 would be unreadable and 0440 is
	// the minimum that works. Parsed from CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ.
	KeysFileAllowGroupRead bool

	// KeyRotationOverlap, when non-zero, overrides both Ed25519 families'
	// rotation overlap window (#123). Zero keeps the per-family defaults.
	KeyRotationOverlap time.Duration

	// AuthMode selects stream authn/authz enforcement (issue #126). The default
	// auth.ModeInsecure evaluates decisions for telemetry only, so a base
	// protocol client keeps working and a deploy sync can never auto-enforce;
	// auth.ModeEnforce fails closed (401/403 before any store access). Flipped
	// per-stage at the deployment layer, not in code.
	AuthMode auth.Mode

	// ServiceCredentials are the trusted-backend static bearer identities
	// (issue #126 TB4): the Electric agents-server's DURABLE_STREAMS_BEARER,
	// with comma-separated entries for rotation overlap. Empty disables the
	// bearer path.
	ServiceCredentials []auth.ServiceCredential

	// TrustedSPIFFEIDs is the in-mesh service allowlist matched against the
	// sidecar-injected X-Forwarded-Client-Cert URI SAN (issue #126 TB4).
	// Configure it only when a sidecar fronts chronicle and sanitizes
	// inbound XFCC. Empty disables the mesh path.
	TrustedSPIFFEIDs []string

	// XFCCMarkerName / XFCCMarkerValue are the optional sidecar-marker gate
	// (issue #126 hardening): when Name is set, an XFCC mesh identity is
	// honored only if the request also carries this header with this exact
	// value — a header the sidecar injects on mTLS-verified traffic and strips
	// from inbound requests, so an external peer that forges XFCC cannot also
	// produce it. Parsed from CHRONICLE_XFCC_REQUIRED_HEADER ("Name: value").
	XFCCMarkerName  string
	XFCCMarkerValue string

	// AllowXFCCWithoutMarker is the explicit opt-in to trust XFCC mesh identity
	// with no marker gate (issue #126 hardening, #130). The default is fail
	// closed: when TrustedSPIFFEIDs is set but no marker is configured, XFCC is
	// NOT trusted and LoadEnv refuses startup — an operator must either set a
	// marker (CHRONICLE_XFCC_REQUIRED_HEADER) or consciously accept the
	// marker-less posture here (CHRONICLE_XFCC_TRUST_WITHOUT_MARKER), which is
	// only safe behind a sidecar that strips inbound XFCC (SANITIZE_SET).
	AllowXFCCWithoutMarker bool

	// WakeTokenAudience is the aud claim minted into wake_tokens (#123/#126
	// TB6a): the egress gateway the token is intended for. Empty (the
	// default) mints wake_tokens without an aud claim.
	WakeTokenAudience string

	// OIDC configures the IdP issuer route (issue #126 TB5): a PingFed
	// access token carrying the configured audience and namespace claim
	// becomes a user principal. The zero value disables the route; a
	// partially set config fails startup (LoadEnv validates completeness).
	OIDC auth.OIDCConfig
}

// DefaultConfig returns the defaults: port 4437 (the IANA-assigned Durable
// Streams port), the conformance suite's stream root, and the Caddy plugin's
// Provision defaults for the shared knobs.
func DefaultConfig() Config {
	return Config{
		Listen:                ":4437",
		StreamRoot:            "/v1/stream/",
		RedisURL:              "redis://localhost:6379",
		RedisPoolSize:         0, // go-redis default
		StoreBackend:          "redis",
		SegmentMode:           "off",
		SegmentTargetBytes:    256 << 10,
		SegmentIndexStride:    128,
		SegmentCacheBytes:     256 << 20,
		SegmentAutoSealRead:   false,
		SegmentInitialState:   "shadow",
		LongPollTimeout:       30 * time.Second,
		SSEReconnectInterval:  60 * time.Second,
		ReadPageBytes:         1 << 20,
		SSEHubReplayBytes:     defaultSSEHubReplayBytes,
		SSEHubBatchBytes:      defaultSSEHubBatchBytes,
		SSEClientWriteTimeout: defaultSSEWriteTimeout,
		PublicBaseURL:         "http://localhost:4437",
		Subscriptions:         true,
		UI:                    true,
		SweepInterval:         30 * time.Second, // coarse recovery floor (issue #13); recovery is event-triggered, not a 2s sweep
		ReconcileInterval:     30 * time.Second,
		Consistency:           webhook.TierA, // no WAIT by default — best latency, at-least-once
		WaitReplicas:          1,             // the realistic Redis Software HA ceiling (06:70), used only by Tier B
		WaitTimeoutMs:         1000,
		AuthMode:              auth.ModeInsecure, // telemetry-first: enforcement is an explicit per-stage opt-in (issue #126)
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
	if v, ok := lookup(EnvRedisPoolSize); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvRedisPoolSize, err)
		}
		c.RedisPoolSize = n
	}
	if v, ok := lookup(EnvStore); ok {
		c.StoreBackend = v
	}
	if v, ok := lookup(EnvSegmentMode); ok {
		c.SegmentMode = v
	}
	if v, ok := lookup(EnvSegmentDir); ok {
		c.SegmentDir = v
	}
	if v, ok := lookup(EnvSegmentTargetBytes); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: want a positive integer, got %q", EnvSegmentTargetBytes, v)
		}
		c.SegmentTargetBytes = n
	}
	if v, ok := lookup(EnvSegmentIndexStride); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: want a positive integer, got %q", EnvSegmentIndexStride, v)
		}
		c.SegmentIndexStride = n
	}
	if v, ok := lookup(EnvSegmentCacheBytes); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: want a positive integer, got %q", EnvSegmentCacheBytes, v)
		}
		c.SegmentCacheBytes = n
	}
	if v, ok := lookup(EnvSegmentAutoSealRead); ok {
		c.SegmentAutoSealRead = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvSegmentInitialState); ok {
		c.SegmentInitialState = v
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
	if v, ok := lookup(EnvReadPageBytes); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: must be a positive integer", EnvReadPageBytes)
		}
		c.ReadPageBytes = n
	}
	if v, ok := lookup(EnvSSEHubReplayBytes); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: must be a positive integer", EnvSSEHubReplayBytes)
		}
		c.SSEHubReplayBytes = n
	}
	if v, ok := lookup(EnvSSEHubBatchBytes); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("%s: must be a positive integer", EnvSSEHubBatchBytes)
		}
		c.SSEHubBatchBytes = n
	}
	if v, ok := lookup(EnvSSEClientWriteTimeout); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s: must be a positive duration", EnvSSEClientWriteTimeout)
		}
		c.SSEClientWriteTimeout = d
	}
	if v, ok := lookup(EnvPublicURL); ok {
		c.PublicBaseURL = v
	}
	if v, ok := lookup(EnvSubscriptions); ok {
		c.Subscriptions = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvUI); ok {
		c.UI = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvUIServer); ok {
		c.UIServer = v
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
	if v, ok := lookup(EnvMetricsPprof); ok {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvMetricsPprof, err)
		}
		c.MetricsPprof = enabled
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
	if v, ok := lookup(EnvKeysFile); ok {
		c.KeysFile = v
	}
	if v, ok := lookup(EnvKeyRotationOverlap); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvKeyRotationOverlap, err)
		}
		c.KeyRotationOverlap = d
	}
	if v, ok := lookup(EnvAuthMode); ok {
		mode, err := auth.ParseMode(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvAuthMode, err)
		}
		c.AuthMode = mode
	}
	if v, ok := lookup(EnvServiceBearer); ok {
		creds, err := auth.ParseServiceBearerConfig(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvServiceBearer, err)
		}
		c.ServiceCredentials = creds
	}
	if v, ok := lookup(EnvTrustedSPIFFE); ok {
		ids, err := auth.ParseTrustedSPIFFEIDs(v)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvTrustedSPIFFE, err)
		}
		c.TrustedSPIFFEIDs = ids
	}
	if v, ok := lookup(EnvXFCCRequiredHeader); ok {
		name, value, found := strings.Cut(v, ":")
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		// An empty value is a fail-open trap: Header.Get can't tell "present
		// and empty" from "absent", so an empty-value marker would pass the
		// gate for any request that simply omits the header — no security at
		// all. Require a non-empty value (#130).
		if !found || name == "" || value == "" {
			return fmt.Errorf("%s: want \"Name: value\" with a non-empty value, got %q", EnvXFCCRequiredHeader, v)
		}
		c.XFCCMarkerName = name
		c.XFCCMarkerValue = value
	}
	if v, ok := lookup(EnvXFCCTrustWithoutMarker); ok {
		c.AllowXFCCWithoutMarker = v == "1" || v == "true"
	}
	if v, ok := lookup(EnvKeysFileAllowGroupRead); ok {
		c.KeysFileAllowGroupRead = v == "1" || v == "true"
	}
	// Fail closed on the XFCC-spoof misconfiguration (issue #126 hardening,
	// #130): a SPIFFE allowlist with no marker gate means raw client XFCC would
	// be trusted if the sidecar ever failed to strip it. Refuse startup unless
	// the operator either configures a marker or consciously accepts the
	// marker-less posture — never silently default to trusting raw XFCC.
	if len(c.TrustedSPIFFEIDs) > 0 && c.XFCCMarkerName == "" && !c.AllowXFCCWithoutMarker {
		return fmt.Errorf("%s is set without %s: XFCC mesh identity would rest on raw client input — set %s to gate it, or set %s=true only if the sidecar provably strips inbound XFCC (Envoy forward_client_cert_details: SANITIZE_SET)",
			EnvTrustedSPIFFE, EnvXFCCRequiredHeader, EnvXFCCRequiredHeader, EnvXFCCTrustWithoutMarker)
	}
	if v, ok := lookup(EnvWakeTokenAud); ok {
		c.WakeTokenAudience = v
	}
	oidcSet := false
	if v, ok := lookup(EnvOIDCIssuer); ok {
		c.OIDC.Issuer = v
		oidcSet = true
	}
	if v, ok := lookup(EnvOIDCAudience); ok {
		c.OIDC.Audience = v
		oidcSet = true
	}
	if v, ok := lookup(EnvOIDCNSClaim); ok {
		c.OIDC.NamespaceClaim = v
		oidcSet = true
	}
	if oidcSet {
		// All-or-nothing: a partially configured issuer must refuse startup
		// rather than run a weaker verification than the operator intended.
		if err := c.OIDC.Validate(); err != nil {
			return fmt.Errorf("%s/%s/%s: %w", EnvOIDCIssuer, EnvOIDCAudience, EnvOIDCNSClaim, err)
		}
	}
	return nil
}
