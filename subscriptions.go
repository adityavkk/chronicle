package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// SubscriptionTuning configures the subscription background loops. Zero values
// fall back to the Manager's defaults (a 30s coarse recovery floor, 30s reconcile,
// no sweep cap).
type SubscriptionTuning struct {
	// SweepInterval is the coarse recovery FLOOR, not a fast sweep (issue #13):
	// recovery is event-triggered (boot, a Redis reconnect, an append/delivery
	// error and, from #14, an owner-epoch bump), so this only bounds the one
	// eventless case. Zero defaults to 30s. Steady-state delivery latency is
	// unaffected by the value — the happy path is the event-driven wake pipeline.
	SweepInterval     time.Duration
	ReconcileInterval time.Duration
	SweepBatch        int
	// Metrics, if set, receives sweep/delivery/worker observations from the
	// Manager. Nil leaves the Manager on its no-op recorder.
	Metrics webhook.Metrics

	// WakeTokenAudience is the aud claim minted into wake_tokens (#123/#126
	// TB6a). Empty mints wake_tokens without an aud claim.
	WakeTokenAudience string

	// ---- leased slot ownership (issue #14) ----
	// ReplicaID is this process's membership identity; empty makes the Manager
	// generate it (POD_NAME + a crypto/rand nonce). MemberLeaseTTL /
	// HeartbeatInterval / SlotLeaseTTL / SlotReconcileInterval are the membership +
	// slot-ownership timers (a DIFFERENT lease layer from the per-subscription
	// webhook lease_ttl_ms). Zero values default to 9s/3s/9s/3s; the Manager
	// enforces heartbeatInterval < memberLeaseTTL/2 and slotReconcileInterval <=
	// heartbeatInterval, falling back to defaults if violated.
	ReplicaID             string
	MemberLeaseTTL        time.Duration
	HeartbeatInterval     time.Duration
	SlotLeaseTTL          time.Duration
	SlotReconcileInterval time.Duration

	// ---- tunable consistency (issue #16) ----
	// Consistency is the durability tier applied to the fence-minting writes
	// (arm_wake/claim generation HINCRBY). TierA (the zero value) issues no WAIT —
	// today's behavior; TierB issues WAITAOF and checks the returned pair before
	// dispatch (durability, not linearizability — the fence stays the only
	// exclusivity guard). WaitReplicas / WaitTimeoutMs parameterize Tier B's barrier
	// (1 replica on STANDARD_HA, 0 on a single AOF Redis). Per-deployment here; the
	// tier type is also the vocabulary for the documented per-subscription surface.
	Consistency   webhook.ConsistencyTier
	WaitReplicas  int
	WaitTimeoutMs int

	// AuthMode is the shared #126 enforcement toggle, threaded into the
	// subscription Manager so the control-plane gates (claim, and later
	// create/add-streams) follow the same telemetry-default posture as the
	// data plane. One mode, never a second flag.
	AuthMode auth.Mode
	// ServiceAccess is the same service authenticator and policy evaluator used
	// by the data-plane handler. Nil keeps caller-token-only control-plane auth.
	ServiceAccess *auth.ServiceAccess

	// ---- key custody (issues #123/#126) ----
	// KeysFile, when non-empty, loads the Ed25519 signing key(s) and the HMAC
	// token key from a mounted secrets file (the Akeyless /etc/secrets
	// pattern) instead of generating-and-persisting them in Redis. On a shared
	// data-plane Redis the persisted form means Redis read access can forge
	// webhook signatures and tokens; the file mount is what breaks that.
	// Empty keeps the Redis custody (dev default). A configured-but-unloadable
	// file refuses startup — fail closed, never fall back to Redis keys.
	// After boot the file is watched: replacing the mounted secret rotates
	// keys live (#123 rotation); an invalid replacement keeps the last good
	// state and logs.
	KeysFile string
	// KeysFileAllowGroupRead opts into loading a group-readable keys file
	// (#131 fail-closed custody exception): the default refuses any group/world
	// readability, but a non-root container reading a root-owned secret via a
	// dedicated Kubernetes fsGroup legitimately needs the group-read bit.
	KeysFileAllowGroupRead bool
	// KeysReloadInterval bounds how stale a replica's key snapshot may be
	// (rotations, denylist entries, keys-file replacements land within one
	// interval). Zero defaults to 15s.
	KeysReloadInterval time.Duration
	// KeyRotationOverlap overrides both families' rotation overlap window
	// (how long a retiring key keeps verifying). Zero keeps the per-family
	// defaults derived from each family's maximum token lifetime.
	KeyRotationOverlap time.Duration
}

// storePath maps a stream-root-relative subscription path ("events/abc") to the
// store's leading-slash key ("/events/abc"). The inverse of subStreamPath.
func storePath(p string) string { return "/" + strings.TrimPrefix(p, "/") }

// SubscriptionRouter handles reserved __ds requests, returning true when it has
// claimed the request. *webhook.Routes satisfies it.
type SubscriptionRouter interface {
	HandleRequest(w http.ResponseWriter, r *http.Request) bool
}

// SubscriptionHooks receives stream lifecycle events so the subscription layer
// can wake subscribers. *webhook.Manager satisfies it.
type SubscriptionHooks interface {
	OnStreamCreated(path string)
	OnStreamAppend(path string)
	OnStreamDeleted(path string)
}

// SubscriptionService is the runnable subscription manager: the lifecycle hooks
// plus its background loops (lease worker, retry worker, due worker, the recovery
// loop — a coarse floor plus event-triggered reconciles — and the slow reconcile
// loop). *webhook.Manager satisfies it; its OnRedisReconnect is the reconnect
// recovery seam the connection/DR layer drives (issue #13, exercised by #16).
type SubscriptionService interface {
	SubscriptionHooks
	Start()
	Stop()
	RunSweep()
	// OnRedisReconnect drives the eager reconnect reconcile when go-redis opens a
	// fresh connection after a drop/failover. Coalesced and non-blocking in the
	// concrete manager.
	OnRedisReconnect()
	// Promote drives the failover-aware eager reconcile a DR promotion requires
	// (issue #16): on an active-passive failover the DR layer calls this so each
	// owner re-establishes slot ownership on the promoted primary and re-derives any
	// schedule tail the async-replication RPO window dropped. *webhook.Manager
	// satisfies it.
	Promote()
}

// streamAdapter adapts the durable stream store to webhook.Streams: the seam the
// subscription Manager uses to read tails and append pull-wake events. rs is
// optional: when set it enables a pipelined batch tail read for the recovery
// sweep; when nil, TailOffsets falls back to per-path reads.
type streamAdapter struct {
	st store.Store
	rs *redisstore.Store
}

func (a streamAdapter) TailOffset(path string) (string, bool) {
	off, err := a.st.GetCurrentOffset(storePath(path))
	if err != nil {
		return "", false
	}
	return off.String(), true
}

// TailOffsets reads many stream tails at once: one pipelined batch when the Redis
// store is available, else a per-path fallback. Paths absent from the result do
// not exist. The sweep reads every linked tail per tick, so the batch keeps that
// from being a round trip per link.
func (a streamAdapter) TailOffsets(paths []string) (map[string]string, error) {
	if a.rs != nil {
		keyed := make([]string, len(paths))
		for i, p := range paths {
			keyed[i] = storePath(p)
		}
		offs, err := a.rs.GetCurrentOffsets(context.Background(), keyed)
		if err != nil {
			return nil, err
		}
		out := make(map[string]string, len(offs))
		for sp, off := range offs {
			out[strings.TrimPrefix(sp, "/")] = off.String()
		}
		return out, nil
	}
	out := make(map[string]string, len(paths))
	for _, p := range paths {
		off, err := a.st.GetCurrentOffset(storePath(p))
		if errors.Is(err, store.ErrStreamNotFound) || errors.Is(err, store.ErrStreamExpired) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[p] = off.String()
	}
	return out, nil
}

func (a streamAdapter) BeginningOffset() string { return store.ZeroOffset.String() }

func (a streamAdapter) AppendWakeEvent(wakeStream string, data []byte) error {
	_, err := a.st.Append(storePath(wakeStream), data, store.AppendOptions{ContentType: "application/json"})
	return err
}

// redisLister adapts the Redis stream store to webhook.StreamLister for pattern
// backfill and recovery reconciliation.
type redisLister struct {
	rs *redisstore.Store
}

func (l redisLister) ListStreams() ([]webhook.StreamMeta, error) {
	metas, err := l.rs.ListStreamMeta(context.Background())
	if err != nil {
		return nil, err
	}
	out := make([]webhook.StreamMeta, len(metas))
	for i, m := range metas {
		// Store keys carry a leading slash; the subscription layer is slash-free.
		out[i] = webhook.StreamMeta{
			Path:        strings.TrimPrefix(m.Path, "/"),
			Tail:        m.Tail,
			CreatedAtNs: m.CreatedAtNs,
		}
	}
	return out, nil
}

// NewSubscriptions builds the Redis-backed __ds subscription stack: the HTTP
// router, the Manager whose background loops (lease, retry, recovery sweep)
// the caller starts with Manager.Start(), and the authorizer bundle wired to
// the same persisted keys the mints use (issue #126) — hand it to the
// Handler so the token gates and the mints can never disagree. streamRootURL is the public URL the protocol is served
// under (scheme+host+root, trailing slash), used to build callback and JWKS
// URLs. rs may be nil to disable pattern backfill of existing streams (new
// streams are still linked as they are created).
func NewSubscriptions(client redis.UniversalClient, streamStore store.Store, rs *redisstore.Store, streamRootURL string, allowPrivateWebhooks bool, tuning SubscriptionTuning, logger *slog.Logger) (SubscriptionRouter, SubscriptionService, *Authorizers, error) {
	opts := webhook.ManagerOptions{
		StreamRootURL:              streamRootURL,
		Logger:                     logger,
		AllowPrivateWebhookTargets: allowPrivateWebhooks,
		SweepInterval:              tuning.SweepInterval,
		ReconcileInterval:          tuning.ReconcileInterval,
		SweepBatch:                 tuning.SweepBatch,
		Metrics:                    tuning.Metrics,
		WakeTokenAudience:          tuning.WakeTokenAudience,
		ReplicaID:                  tuning.ReplicaID,
		MemberLeaseTTL:             tuning.MemberLeaseTTL,
		HeartbeatInterval:          tuning.HeartbeatInterval,
		SlotLeaseTTL:               tuning.SlotLeaseTTL,
		SlotReconcileInterval:      tuning.SlotReconcileInterval,
		AuthMode:                   tuning.AuthMode,
		ServiceAccess:              tuning.ServiceAccess,
	}
	if rs != nil {
		opts.Lister = redisLister{rs: rs}
	}
	if tuning.KeysFile != "" {
		src, err := webhook.NewFileKeyWatcher(tuning.KeysFile, tuning.KeysFileAllowGroupRead, logger)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, w := range src.Warnings() {
			logger.Warn(w)
		}
		logger.Info("subscription keys loaded from file (Redis custody disabled)",
			"path", tuning.KeysFile, "kids", src.Kids())
		opts.Keys = src
	}
	opts.KeysReloadInterval = tuning.KeysReloadInterval
	opts.KeyRotationOverlap = tuning.KeyRotationOverlap
	// Tier B (issue #16) arms the WAITAOF durability barrier on the fence-minting
	// writes; TierA/C leave the store on the no-WAIT default. WithConsistency is a
	// no-op for the zero (TierA) value, so a deployment that never sets the tier is
	// byte-for-byte unchanged.
	store := webhook.NewRedisStore(client).
		WithConsistency(tuning.Consistency, tuning.WaitReplicas, tuning.WaitTimeoutMs).
		WithMetrics(tuning.Metrics)
	// Tier B fails fast at startup if the connected Redis cannot honor its WAITAOF
	// barrier — appendonly is off, or the topology has fewer online replicas than
	// WaitReplicas requires (issue #43). A no-op for Tier A/C. Running a Tier B
	// durability path against a non-AOF Redis would prove nothing, so refuse to
	// start rather than silently expose the RPO the tier exists to bound.
	if err := store.AssertAOFEnabled(tuning.Consistency, tuning.WaitReplicas); err != nil {
		return nil, nil, nil, err
	}
	mgr, err := webhook.NewManager(store, streamAdapter{st: streamStore, rs: rs}, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	authz := &Authorizers{
		Append: mgr.WriteAuthorizer(),
		Read:   mgr.ReadAuthorizer(),
		Caller: mgr.CallerAuthorizer(),
		Entity: mgr.EntityAuthorizer(),
	}
	return webhook.NewRoutes(mgr), mgr, authz, nil
}
