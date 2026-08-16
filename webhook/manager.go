package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Streams is the seam over the durable stream store the subscription Manager
// needs: tail offsets to compute pending work, the canonical beginning offset
// to link new streams at, and appending wake events for pull-wake delivery. The
// chronicle wiring adapts store.Store to this interface, keeping the webhook
// package independent of the store package.
type Streams interface {
	// TailOffset returns a stream's current tail and whether it exists.
	TailOffset(path string) (string, bool)
	// TailOffsets returns the current tail for each given path that exists, in as
	// few round trips as the implementation can manage. The recovery sweep reads
	// every linked stream's tail per tick, so a per-path round trip does not
	// scale; this is the batched form. A path whose stream does not exist is
	// omitted from the map (the batched form of TailOffset's not-ok result).
	TailOffsets(paths []string) (map[string]string, error)
	// BeginningOffset is the canonical "start of stream" cursor (store.ZeroOffset);
	// a stream linked here has no pending work until its first append.
	BeginningOffset() string
	// AppendWakeEvent appends a JSON wake event to a pull-wake wake stream.
	AppendWakeEvent(wakeStream string, data []byte) error
}

// WriteFenceStreams replicates one live subscription claim into each linked
// stream's Redis Cluster slot. The data-plane store checks the same marker in
// its atomic append or close transaction.
type WriteFenceStreams interface {
	GrantAppendFence(path string, fence auth.AppendFence) (bool, error)
	RevokeAppendFence(path string, fence auth.AppendFence) error
}

// StreamMeta is a stream's path, current tail, and creation time — the inputs the
// pattern reconciler needs to recover a missed glob link at the right offset.
type StreamMeta struct {
	Path        string
	Tail        string
	CreatedAtNs int64
}

// StreamLister optionally enumerates live streams so a new pattern subscription
// can backfill matching streams and the recovery reconciler can re-link streams
// whose OnStreamCreated hook (or initial backfill) was lost to a crash
// (PROTOCOL §6.2). It is optional: without it, new streams are still linked as
// they are created.
type StreamLister interface {
	ListStreams() ([]StreamMeta, error)
}

const (
	webhookDeliveryTimeout = 30 * time.Second
	defaultWorkerTick      = 250 * time.Millisecond
	// defaultSweepInterval is the coarse recovery FLOOR, not a fast sweep (issue
	// #13). Recovery is event-triggered now — boot, a Redis reconnect, an
	// append/delivery error, and (from #14) an owner-epoch bump each fire a
	// reconcile at the moment they happen — so the periodic sweep only bounds the
	// one undetectable case (an owed mark on a slot that is unowned and quiet). That
	// case is rare and not latency-sensitive, so the floor sits in the
	// seconds-to-minutes band, aligned with the index reconcile, not at the old 2s.
	defaultSweepInterval     = 30 * time.Second
	defaultReconcileInterval = 30 * time.Second
	dueClaimLimit            = 256
	dirtyQueueCapacity       = 1024
	dirtyBatchSize           = 64

	// Leased slot-ownership timers (issue #14, 05:502-505). A DIFFERENT lease layer
	// from the per-subscription webhook lease_ttl_ms — these govern which replica
	// owns a slot of background work, not a subscription's claim. Invariants
	// (CheckOwnershipConfig): heartbeatInterval < memberLeaseTTL/2 and
	// slotReconcileInterval <= heartbeatInterval.
	defaultMemberLeaseTTL        = 9 * time.Second
	defaultHeartbeatInterval     = 3 * time.Second
	defaultSlotLeaseTTL          = 9 * time.Second
	defaultSlotReconcileInterval = 3 * time.Second
)

// ManagerOptions configures a Manager. Zero values fall back to sensible
// defaults; StreamRootURL is required to build absolute callback and JWKS URLs.
type ManagerOptions struct {
	// StreamRootURL is the public URL the protocol is served under, including
	// scheme and host (e.g. "http://localhost:4437/v1/stream/"). NewManager
	// normalizes it to end in exactly one "/", so a missing or doubled trailing
	// slash still yields correct callback and JWKS URLs.
	StreamRootURL string
	Lister        StreamLister
	Resolver      IPResolver
	HTTPClient    *http.Client
	Logger        *slog.Logger
	WorkerTick    time.Duration
	// SweepInterval is the coarse recovery FLOOR — how often the full cursor
	// reconcile runs on a timer with no triggering event (issue #13). It is NOT a
	// fast 2s sweep: the latency-sensitive cases are event-triggered (boot, a Redis
	// reconnect, an append/delivery error and, from #14, an owner-epoch bump each
	// fire reconcile(scope) immediately), so this only bounds the one eventless case
	// — an owed mark on a slot that is unowned and quiet. Default 30s
	// (defaultSweepInterval), the seconds-to-minutes band, aligned with
	// ReconcileInterval. Steady-state delivery latency is unchanged by the raise:
	// the old 2s sweep fired nothing in steady state (the happy path is the
	// event-driven OnStreamAppend → wake pipeline).
	SweepInterval time.Duration
	// ReconcileInterval is how often the slow reconciliation loop runs (pattern
	// link recovery + fan-out index repair). Default 30s — it is O(streams), so it
	// runs on its own coarse timer, the same band as the recovery floor.
	ReconcileInterval time.Duration
	// SweepBatch caps how many subscriptions one sweep tick evaluates, the rest
	// rolling to following ticks. 0 (the default) means no cap — every tick
	// sweeps all subscriptions. A positive cap bounds per-tick cost on a large
	// keyspace at the price of up to ceil(K/SweepBatch) ticks of recovery latency.
	SweepBatch int
	// AllowPrivateWebhookTargets relaxes SSRF validation to accept any http(s)
	// webhook URL (e.g. cluster-internal receivers on RFC1918 addresses). Off by
	// default; the operator opts in for trusted networks.
	AllowPrivateWebhookTargets bool
	// Metrics receives sweep/delivery/worker observations. Nil defaults to a
	// no-op recorder, so instrumentation is opt-in.
	Metrics Metrics
	// Keys overrides where key material comes from (issues #123/#126 custody):
	// nil keeps the store as the source (generate-and-persist in Redis, the dev
	// default); a FileKeySource reads a mounted secrets file so key material
	// never touches the shared data-plane Redis.
	Keys KeySource

	// WakeTokenAudience is the aud claim minted into wake_tokens (#123): the
	// egress gateway the token is intended for. Empty (the default) mints
	// tokens without an aud claim; the gateway-side audience check activates
	// once a deployment configures it (CHRONICLE_WAKE_TOKEN_AUD).
	WakeTokenAudience string

	// KeysReloadInterval bounds how stale this replica's key snapshot may be:
	// rotations, denylist entries, and keys-file replacements land within one
	// interval (#123 rotation). Zero defaults to 15s.
	KeysReloadInterval time.Duration
	// KeyRotationOverlap overrides BOTH families' rotation overlap window —
	// how long a retiring key keeps verifying after its successor takes over.
	// Zero keeps the per-family defaults derived from each family's max token
	// lifetime (see rotationOverlap).
	KeyRotationOverlap time.Duration

	// ---- leased slot ownership (issue #14) ----

	// ReplicaID is this process's stable membership identity for its pod lifetime
	// (POD_NAME + a crypto/rand nonce). Empty (the default) makes NewManager
	// generate it via GenerateReplicaID, reading POD_NAME from the environment and
	// falling back to a local form. owner_epoch — not this id — fences a
	// paused-then-resumed same incarnation.
	ReplicaID string
	// MemberLeaseTTL / HeartbeatInterval / SlotLeaseTTL / SlotReconcileInterval are
	// the membership + slot-ownership timers. Zero values default to 9s/3s/9s/3s.
	// They are a DIFFERENT lease layer from the per-subscription webhook
	// lease_ttl_ms (already in Config). NewManager validates the invariants and
	// falls back to all defaults if a supplied set violates them.
	MemberLeaseTTL        time.Duration
	HeartbeatInterval     time.Duration
	SlotLeaseTTL          time.Duration
	SlotReconcileInterval time.Duration

	// AuthMode selects control-plane authorization enforcement (issue #126).
	// The zero value (insecure) evaluates gates as telemetry only; enforce
	// fails closed before any store access. One mode shared with the data
	// plane — never a second flag.
	AuthMode auth.Mode
	// ServiceAccess is the shared SPIFFE/static-bearer service authenticator and
	// explicit policy evaluator. Nil keeps control-plane caller-token auth only.
	ServiceAccess *auth.ServiceAccess
}

// Manager orchestrates the subscription control plane: stream hooks, webhook
// delivery and callbacks, pull-wake claim/ack/release, the lease and retry
// worker ticks, and the recovery sweep that closes the restart gap
// (docs/research/07 §8). It is the imperative shell over the pure core and the
// durable Store.
type Manager struct {
	store         Store
	streams       Streams
	lister        StreamLister
	streamRootURL string // normalized in NewManager to end in exactly one "/"
	writeFences   WriteFenceStreams
	client        *http.Client
	resolver      IPResolver
	wakeTokenAud  string
	tokenKey      []byte

	// keySnap is the atomically-swapped view of both Ed25519 families + the
	// kid denylist (#123 rotation): every mint, verification, and JWKS read
	// goes through it, and keysReloadLoop refreshes it, so no key is ever
	// cached past one reload interval. Never nil after NewManager.
	keySnap            atomic.Pointer[keyState]
	keys               KeySource // custody source (the store unless ManagerOptions.Keys overrides)
	custodyIsStore     bool      // rotation/denylist write through the store only
	keysReloadInterval time.Duration
	keyRotationOverlap time.Duration
	log                *slog.Logger
	workerTick         time.Duration
	sweepInterval      time.Duration
	reconcileInterval  time.Duration
	sweepBatch         int
	sweepCursor        int // rolling start index when sweepBatch caps a tick
	allowPrivate       bool
	metrics            Metrics

	// authMode is the shared #126 enforcement toggle (see ManagerOptions).
	authMode      auth.Mode
	serviceAccess *auth.ServiceAccess

	// ---- leased slot ownership (issue #14) ----
	replicaID             ReplicaID
	memberLeaseTTL        time.Duration
	heartbeatInterval     time.Duration
	slotLeaseTTL          time.Duration
	slotReconcileInterval time.Duration

	// held is the set of ownership slots this replica currently holds a lease on,
	// SlotID -> the epoch it holds, recomputed each slotReconcileInterval as
	// HRW-targeted ∩ claim_shard-granted. The fast workers (lease/retry/due)
	// iterate ownedSlots() over it; the full sweep deliberately ignores it (the
	// unguarded backstop). Guarded by ownMu. THE CAS IS THE AUTHORITY, NOT THE HRW
	// MATH (05:399): a slot is here only if claim_shard granted it.
	ownMu sync.RWMutex
	held  map[SlotID]OwnerEpoch

	// reconcileC coalesces event-triggered recovery onto the single recovery loop
	// (issue #13). Depth 1 + non-blocking sends mean concurrent recovery events
	// collapse to at most one queued reconcile while one runs — duplicate
	// reconciles are claim-fence-safe, so dropping the surplus is sound.
	reconcileC chan scope

	// dirty is the bounded, process-local append hint queue. dirtyMu protects
	// only in-memory transitions; no Redis, stream, or delivery call is made
	// while it is held.
	dirtyMu     sync.Mutex
	dirty       dirtyQueue
	dirtyNotify chan struct{}
	dirtyClosed bool
	now         func() time.Time

	// runCtx owns every Manager background loop. The lifecycle state makes Start
	// and Stop race-safe and idempotent without holding lifeMu across I/O.
	runCtx    context.Context
	cancelRun context.CancelFunc
	lifeMu    sync.Mutex
	life      managerLifecycle
	startDone chan struct{}
	wg        sync.WaitGroup
}

type managerLifecycle uint8

const (
	managerNew managerLifecycle = iota
	managerStarting
	managerRunning
	managerStopping
	managerStopped
)

// NewManager builds a Manager and loads the signing and token keys from the
// configured KeySource — the store (which installs persisted keys, so the kid
// is stable and tokens validate across restarts) unless opts.Keys overrides
// custody with a mounted secrets file (#123/#126: then nothing here writes
// key material to Redis).
func NewManager(store Store, streams Streams, opts ManagerOptions) (*Manager, error) {
	now := time.Now()
	keys := opts.Keys
	if keys == nil {
		keys = store
	}
	// Load-or-install both families up front (for Redis custody this is what
	// persists a fresh deployment's keys); the equal-kid conflation check
	// (#123, RFC 8725 §2.8) re-runs on every snapshot build after this.
	signing, err := keys.LoadSigningKey(now)
	if err != nil {
		return nil, err
	}
	tokenKey, err := keys.LoadTokenKey()
	if err != nil {
		return nil, err
	}
	wakeKey, err := keys.LoadWakeKey(now)
	if err != nil {
		return nil, err
	}
	if wakeKey.Kid == signing.Kid {
		return nil, fmt.Errorf("webhook: wake-token key equals the envelope signing key (kid %s)", wakeKey.Kid)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	writeFences, _ := streams.(WriteFenceStreams)
	m := &Manager{
		store:                 store,
		keys:                  keys,
		custodyIsStore:        opts.Keys == nil,
		keysReloadInterval:    opts.KeysReloadInterval,
		keyRotationOverlap:    opts.KeyRotationOverlap,
		streams:               streams,
		writeFences:           writeFences,
		lister:                opts.Lister,
		streamRootURL:         normalizeStreamRootURL(opts.StreamRootURL),
		client:                opts.HTTPClient,
		resolver:              opts.Resolver,
		wakeTokenAud:          opts.WakeTokenAudience,
		tokenKey:              tokenKey,
		log:                   opts.Logger,
		workerTick:            opts.WorkerTick,
		sweepInterval:         opts.SweepInterval,
		reconcileInterval:     opts.ReconcileInterval,
		sweepBatch:            opts.SweepBatch,
		allowPrivate:          opts.AllowPrivateWebhookTargets,
		metrics:               opts.Metrics,
		authMode:              opts.AuthMode,
		serviceAccess:         opts.ServiceAccess,
		memberLeaseTTL:        opts.MemberLeaseTTL,
		heartbeatInterval:     opts.HeartbeatInterval,
		slotLeaseTTL:          opts.SlotLeaseTTL,
		slotReconcileInterval: opts.SlotReconcileInterval,
		held:                  map[SlotID]OwnerEpoch{},
		reconcileC:            make(chan scope, 1),
		dirty:                 newDirtyQueue(dirtyQueueCapacity),
		dirtyNotify:           make(chan struct{}, 1),
		now:                   time.Now,
		runCtx:                runCtx,
		cancelRun:             cancelRun,
		startDone:             make(chan struct{}),
	}
	if m.metrics == nil {
		m.metrics = NopMetrics{}
	}
	if m.client == nil {
		m.client = &http.Client{Timeout: webhookDeliveryTimeout}
	}
	if m.resolver == nil {
		m.resolver = defaultResolver
	}
	if m.log == nil {
		m.log = slog.Default()
	}
	if m.workerTick == 0 {
		m.workerTick = defaultWorkerTick
	}
	if m.sweepInterval == 0 {
		m.sweepInterval = defaultSweepInterval
	}
	if m.reconcileInterval == 0 {
		m.reconcileInterval = defaultReconcileInterval
	}
	if m.keysReloadInterval == 0 {
		m.keysReloadInterval = defaultKeysReloadInterval
	}
	// The initial snapshot must build — a custody source that cannot produce
	// a coherent key state refuses startup (fail closed); after this, reload
	// failures keep the last good snapshot instead.
	if err := m.ReloadKeys(); err != nil {
		cancelRun()
		return nil, err
	}
	if err := m.initOwnership(opts); err != nil {
		cancelRun()
		return nil, err
	}
	m.metrics.DirtyQueue(0, dirtyQueueCapacity, 0)
	return m, nil
}

// initOwnership defaults the slot-ownership timers, resolves the replica id, and
// enforces the membership invariants (issue #14). A zero TTL takes its default;
// then if the resolved set violates CheckOwnershipConfig (an operator passed an
// inconsistent combination) the manager logs a warning and falls back to ALL
// defaults rather than failing startup — a misconfigured timer must not stop the
// process from serving, and the defaults are known-good.
func (m *Manager) initOwnership(opts ManagerOptions) error {
	if m.memberLeaseTTL == 0 {
		m.memberLeaseTTL = defaultMemberLeaseTTL
	}
	if m.heartbeatInterval == 0 {
		m.heartbeatInterval = defaultHeartbeatInterval
	}
	if m.slotLeaseTTL == 0 {
		m.slotLeaseTTL = defaultSlotLeaseTTL
	}
	if m.slotReconcileInterval == 0 {
		m.slotReconcileInterval = defaultSlotReconcileInterval
	}
	if err := CheckOwnershipConfig(m.memberLeaseTTL, m.heartbeatInterval, m.slotLeaseTTL, m.slotReconcileInterval); err != nil {
		m.log.Warn("webhook: ownership timers violate invariants, using defaults", "error", err)
		m.memberLeaseTTL = defaultMemberLeaseTTL
		m.heartbeatInterval = defaultHeartbeatInterval
		m.slotLeaseTTL = defaultSlotLeaseTTL
		m.slotReconcileInterval = defaultSlotReconcileInterval
	}
	if opts.ReplicaID != "" {
		r, err := NewReplicaID(opts.ReplicaID)
		if err != nil {
			return err
		}
		m.replicaID = r
		return nil
	}
	r, err := GenerateReplicaID(os.Getenv("POD_NAME"), randReader)
	if err != nil {
		return err
	}
	m.replicaID = r
	return nil
}

// ReplicaID is this process's membership identity (for logs/tests).
func (m *Manager) ReplicaID() string { return m.replicaID.String() }

// randReader is the package's randomness source for wake ids and tokens.
var randReader = rand.Reader

func defaultResolver(host string) ([]net.IP, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

func (m *Manager) tailOf(path string) (string, bool) { return m.streams.TailOffset(path) }

// normalizeStreamRootURL forces the stream root to end in exactly one "/" so
// callbackURL and JWKSURL join cleanly regardless of how the caller configured
// --stream-root (missing, single, or doubled trailing slash). The URLs are
// handed to external webhook receivers, so a stray "stream__ds/..." must never
// escape.
func normalizeStreamRootURL(root string) string {
	return strings.TrimRight(root, "/") + "/"
}

func (m *Manager) callbackURL(id string) string {
	return m.streamRootURL + "__ds/subscriptions/" + id + "/callback"
}

// JWKSURL is the absolute URL of the signing key set.
func (m *Manager) JWKSURL() string { return m.streamRootURL + "__ds/jwks.json" }

// SigningView returns the signing metadata block for a subscription response.
func (m *Manager) signingView() *SigningView {
	return &SigningView{Alg: "ed25519", Kid: m.keySnapshot().webhook.active.Kid, JWKSURL: m.JWKSURL()}
}

// JWKS returns the key set served at __ds/jwks.json: the webhook-envelope
// family (active first) read from the same custody source the signing key
// came from — a file-sourced deployment serves exactly the file's keys and
// never consults Redis for them — followed by the wake-token family (#123;
// file custody for it lands with the Wave-2 rotation work). The set is
// kid-selective for every consumer — the conformance receiver picks the kid
// named in Webhook-Signature, a gateway picks the kid named in a wake_token's
// JOSE header — so publishing both families in one document is additive.
func (m *Manager) JWKS() (JWKS, error) {
	st := m.keySnapshot()
	keys := make([]SigningKey, 0, len(st.webhook.verify)+len(st.wake.verify))
	keys = append(keys, st.webhook.verify...)
	keys = append(keys, st.wake.verify...)
	return BuildJWKS(keys), nil
}

// mintWakeTokenOnAck re-reads the subscription and mints the heartbeat
// refresh wake_token (ShouldRefreshWakeToken): the ack already passed the
// (generation, wake_id) fence, so the request's fence pair is the one bound
// into the fresh token. ok is false when no token should ride the response.
func (m *Manager) mintWakeTokenOnAck(id string, generation int64, wakeID string, done bool, now time.Time) (string, bool) {
	if !ShouldRefreshWakeToken(done) {
		return "", false
	}
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return "", false
	}
	return m.mintWakeTokenFor(sub, generation, wakeID, now)
}

// mintWakeTokenFor mints the wake_token for a wake on sub at (generation,
// wakeID), applying the honest single-entity rule: only a single-link
// subscription names an entity (WakeEntityPath), and the token lives
// strictly under one lease (WakeTokenTTL). ok is false — mint nothing,
// never a wrong assertion — for multi-link subscriptions, non-positive
// TTLs, or a mint error.
func (m *Manager) mintWakeTokenFor(sub Subscription, generation int64, wakeID string, now time.Time) (string, bool) {
	entity, ok := WakeEntityPath(sub.Links)
	if !ok {
		return "", false
	}
	ttl := WakeTokenTTL(sub.Config.LeaseTTLMs)
	if ttl <= 0 {
		return "", false
	}
	claims, err := NewWakeTokenClaims(m.streamRootURL, entity, m.wakeTokenAud, generation, wakeID, now, ttl, randReader)
	if err != nil {
		m.log.Warn("webhook: wake token claims", "sub", sub.ID, "error", err)
		return "", false
	}
	wakeKey := m.keySnapshot().wake.active
	if wakeKey.Kid == "" {
		// Emergency stop: the active wake kid is denylisted and no successor
		// has been rotated in. Minting a wrong assertion is worse than none.
		m.log.Warn("webhook: wake-token key unavailable (denylisted); not minting", "sub", sub.ID)
		return "", false
	}
	tok, err := MintWakeToken(wakeKey, claims)
	if err != nil {
		m.log.Warn("webhook: mint wake token", "sub", sub.ID, "error", err)
		return "", false
	}
	return tok, true
}

// ---- stream hooks (called by the chronicle handler after a durable write) ----

// OnStreamCreated links a newly created stream to matching glob subscriptions at
// the beginning offset, so the stream's first append wakes them (PROTOCOL §6.2).
func (m *Manager) OnStreamCreated(path string) {
	ids, err := m.store.List()
	if err != nil {
		m.log.Warn("webhook: list subscriptions on stream create", "error", err)
		return
	}
	begin := m.streams.BeginningOffset()
	for _, id := range ids {
		sub, ok, err := m.store.Get(id)
		if err != nil || !ok {
			continue
		}
		if sub.Config.Pattern != "" && GlobMatch(sub.Config.Pattern, path) {
			if err := m.store.Link(id, path, LinkGlob, begin); err != nil {
				m.log.Warn("webhook: link glob stream", "sub", id, "path", path, "error", err)
			}
		}
	}
}

// OnStreamAppend records one process-local dirty hint after a durable append.
// The handoff is bounded by dirtyQueueCapacity and never calls Redis, reads a
// stream tail, delivers a wake, or starts a goroutine. The recovery sweep is the
// durable backstop if this hint is lost to shutdown or overflow.
func (m *Manager) OnStreamAppend(path string) {
	now := m.now()
	m.dirtyMu.Lock()
	result, requestRecovery := dirtyStopped, false
	if !m.dirtyClosed {
		result, requestRecovery = m.dirty.enqueue(path, now)
	}
	stats := m.dirty.stats(now)
	m.dirtyMu.Unlock()

	m.metrics.DirtyEnqueue(result.String(), stats.depth, stats.capacity, stats.oldestAge)
	switch result {
	case dirtyEnqueued:
		failpoint(fpDirtyAfterEnqueueBeforeSignal)
		m.signalDirty()
	case dirtyOverflowed:
		m.metrics.DirtyOverflow()
		m.signalDirty()
	default:
		if requestRecovery {
			panic("webhook: dirty recovery request without enqueue outcome")
		}
	}
}

func (m *Manager) signalDirty() {
	select {
	case m.dirtyNotify <- struct{}{}:
	default:
	}
}

func (m *Manager) takeDirtyBatch() ([]dirtyWork, dirtyQueueStats) {
	now := m.now()
	m.dirtyMu.Lock()
	work := m.dirty.take(dirtyBatchSize)
	stats := m.dirty.stats(now)
	m.dirtyMu.Unlock()
	return work, stats
}

func (m *Manager) completeDirty(path string, completion dirtyCompletion) dirtyQueueStats {
	now := m.now()
	m.dirtyMu.Lock()
	m.dirty.complete(path, completion)
	stats := m.dirty.stats(now)
	m.dirtyMu.Unlock()
	return stats
}

type dirtyProcessStage uint8

const (
	dirtyStageNone dirtyProcessStage = iota
	dirtyStageLookup
	dirtyStageHydrate
	dirtyStageTails
	dirtyStageArm
)

func (s dirtyProcessStage) String() string {
	switch s {
	case dirtyStageLookup:
		return "lookup"
	case dirtyStageHydrate:
		return "hydrate"
	case dirtyStageTails:
		return "tails"
	case dirtyStageArm:
		return "arm"
	default:
		return "none"
	}
}

type dirtyProcessResult struct {
	subs       int
	wakes      int
	duplicates int
}

// processDirtyStream is the asynchronous fan-out shell for one stream. It
// performs one scatter-gather subscriber lookup, one pipelined hydration, and
// one batched read of all distinct linked tails before making pure pending-work
// decisions. Subscription ownership cannot filter this append hint: only the
// replica that accepted the stream append observes it, and it must cover every
// subscriber slot. Generation and owner fences remain in the existing arm and
// worker paths.
func (m *Manager) processDirtyStream(path string) (dirtyProcessResult, dirtyProcessStage, error) {
	lookupStart := m.now()
	ids, slotsProbed, err := m.store.StreamSubscribers(path)
	if err != nil {
		return dirtyProcessResult{}, dirtyStageLookup, err
	}
	m.metrics.FanOut(m.now().Sub(lookupStart), slotsProbed, len(ids))
	if len(ids) == 0 {
		return dirtyProcessResult{}, dirtyStageNone, nil
	}

	subs, err := m.store.GetMany(ids)
	if err != nil {
		return dirtyProcessResult{}, dirtyStageHydrate, err
	}
	result := dirtyProcessResult{subs: len(subs), duplicates: len(ids) - len(subs)}
	idle := make([]Subscription, 0, len(subs))
	for _, sub := range subs {
		if sub.Phase != PhaseIdle {
			result.duplicates++
			continue
		}
		idle = append(idle, sub)
	}
	if len(idle) == 0 {
		return result, dirtyStageNone, nil
	}

	paths := distinctLinkPaths(idle)
	tails, err := m.streams.TailOffsets(paths)
	if err != nil {
		return result, dirtyStageTails, err
	}
	armFailed := false
	for _, sub := range idle {
		if !HasPendingWorkFrom(sub.Links, tails) {
			continue
		}
		switch m.issueWakeResult(sub, path) {
		case wakeIssueArmed:
			result.wakes++
		case wakeIssueDuplicate:
			result.duplicates++
		case wakeIssueFailed:
			armFailed = true
		}
	}
	if armFailed {
		return result, dirtyStageArm, fmt.Errorf("one or more subscription wakes failed to arm")
	}
	return result, dirtyStageNone, nil
}

// processDirtyBatch runs at most dirtyBatchSize streams. A failed stream returns
// to the queue tail and requests eager recovery. The worker loop delays further
// retries until its next tick, so a Redis outage cannot create a hot retry loop.
func (m *Manager) processDirtyBatch() (processed int, hadError bool) {
	work, stats := m.takeDirtyBatch()
	m.metrics.DirtyQueue(stats.depth, stats.capacity, stats.oldestAge)
	if len(work) == 0 {
		return 0, false
	}

	start := m.now()
	total := dirtyProcessResult{}
	for _, item := range work {
		result, stage, err := m.processDirtyStream(item.path)
		total.subs += result.subs
		total.wakes += result.wakes
		total.duplicates += result.duplicates
		completion := dirtySucceeded
		if err != nil {
			completion = dirtyRetry
			hadError = true
			m.metrics.DirtyProcessingError(stage.String())
			m.log.Warn("webhook: async stream fan-out", "stage", stage.String(), "error", err)
			m.triggerReconcile(scopeAppendError)
		} else {
			m.metrics.DirtyRecoveryDelay(m.now().Sub(item.since))
		}
		stats = m.completeDirty(item.path, completion)
		processed++
	}
	outcome := "ok"
	if hadError {
		outcome = "error"
	}
	m.metrics.DirtyProcess(m.now().Sub(start), total.subs, total.wakes, total.duplicates, outcome)
	m.metrics.DirtyQueue(stats.depth, stats.capacity, stats.oldestAge)
	if !hadError && m.dirtyHasReady() {
		m.signalDirty()
	}
	return processed, hadError
}

func (m *Manager) dirtyHasReady() bool {
	m.dirtyMu.Lock()
	ready := m.dirty.hasReady()
	m.dirtyMu.Unlock()
	return ready
}

func (m *Manager) dirtyOverflowPending() bool {
	m.dirtyMu.Lock()
	pending := m.dirty.hasPendingOverflow()
	m.dirtyMu.Unlock()
	return pending
}

// RunDirtyWorker processes one bounded dirty batch immediately. It is the
// deterministic test and benchmark seam, parallel to RunSweep and RunDueWorker.
func (m *Manager) RunDirtyWorker() int {
	processed, _ := m.processDirtyBatch()
	return processed
}

// OnRedisReconnect signals that the Redis connection healed after a drop, so any
// wake/lease op lost with the broken connection is recovered by an eager reconcile
// rather than waiting for the coarse floor (doc-05 correction #2, the reconnect
// event). It is the seam the connection layer wires to the client's reconnect, and
// the one #16's DR promotion drives from a failover. Coalesced and non-blocking.
func (m *Manager) OnRedisReconnect() { m.triggerReconcile(scopeReconnect) }

// Promote drives the failover-aware recovery a DR promotion requires (#16, doc 05
// "Regional DR"). On an active-passive failover (the managed-Redis floor, 06:130)
// the standby region's Redis is promoted to primary and this process now talks to
// it; because replication is async, the promoted primary may be missing the most
// recent, un-replicated schedule tail — the RPO window. Promote re-establishes
// ownership on the new primary (slotReconcileOnce re-CASes each targeted slot,
// bumping owner_epoch on every transfer) and then fires the eager reconcile
// (scopeEpochBump), which re-derives each owner's stranded lease/due entries from
// the durable `sub` hash (reconcileLeases) — recovering the stranded-webhook-wake
// case the failover created.
//
// The division of labour is correction #3 made operational: WAIT/WAITAOF (Tier B)
// bound HOW MUCH tail a failover can lose (the RPO); the monotonic (gen,wake_id)
// fence + this eager reconcile make whatever IS lost self-healing — neither path
// infers exclusivity from durability. The eager reconcile is routed through the
// coalescing reconcileC (not run inline) so sweepOnce stays single-goroutine even
// when the recovery loop is already running; Promote is therefore idempotent and
// safe to call on every promotion signal.
func (m *Manager) Promote() {
	m.slotReconcileOnce()
	m.triggerReconcile(scopeEpochBump)
}

// OnStreamDeleted unlinks a deleted stream from all its subscribers.
func (m *Manager) OnStreamDeleted(path string) {
	ids, _, err := m.store.StreamSubscribers(path)
	if err != nil {
		return
	}
	for _, id := range ids {
		_ = m.store.Unlink(id, path, false)
	}
}

// maybeWake issues a wake for one subscription if it is idle and has pending
// work. triggerStream names the stream that prompted the wake (for pull-wake
// event payloads).
func (m *Manager) maybeWake(id, triggerStream string) {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return
	}
	if sub.Phase != PhaseIdle {
		return
	}
	if !HasPendingWork(sub.Links, m.tailOf) {
		return
	}
	m.issueWake(sub, triggerStream)
}

type wakeIssueResult uint8

const (
	wakeIssueArmed wakeIssueResult = iota
	wakeIssueDuplicate
	wakeIssueFailed
)

// issueWake arms a new wake generation and delivers it (webhook POST or pull-wake
// event). For webhook the lease is armed at issue; for pull-wake the lease waits
// for a claim (PROTOCOL §7.3).
func (m *Manager) issueWake(sub Subscription, triggerStream string) bool {
	return m.issueWakeResult(sub, triggerStream) == wakeIssueArmed
}

func (m *Manager) issueWakeResult(sub Subscription, triggerStream string) wakeIssueResult {
	wakeID, err := GenerateWakeID(rand.Reader)
	if err != nil {
		m.log.Warn("webhook: generate wake id", "error", err)
		return wakeIssueFailed
	}
	armLease := sub.Config.Type == DispatchWebhook
	res, err := m.armWakeUnscoped(sub.ID, m.now(), sub.Config.LeaseTTLMs, armLease, wakeID)
	if err != nil {
		m.log.Warn("webhook: arm wake", "sub", sub.ID, "error", err)
		return wakeIssueFailed
	}
	if !res.Armed {
		return wakeIssueDuplicate // already in flight (coalesced) or gone
	}
	// The arm→emit surgical window (07 honest-gap #2): the fence is minted but the
	// wake is not yet emitted. A no-op in production; a test failpoint can crash/stall
	// here to exercise the stranded-wake recovery the host nemesis cannot pin down.
	failpoint(fpArmedBeforeEmit)
	switch sub.Config.Type {
	case DispatchWebhook:
		if err := durableWakeIntent().RunExternalAction(func() error {
			go m.deliverWebhookUnscoped(sub.ID, res.Generation, res.WakeID)
			return nil
		}); err != nil {
			m.log.Warn("webhook: dispatch wake", "sub", sub.ID, "error", err)
		}
	case DispatchPullWake:
		m.writeWakeEvent(sub, triggerStream, res.Generation, res.WakeID)
	}
	return wakeIssueArmed
}

// issueWakeOwned is issueWake for owner-driven background workers. The arm_wake
// write carries the slot owner scope, so a GC-paused/deposed owner cannot mint a
// fresh generation or due mark after takeover.
func (m *Manager) issueWakeOwned(scope OwnerScope, sub Subscription, triggerStream string) bool {
	wakeID, err := GenerateWakeID(rand.Reader)
	if err != nil {
		m.log.Warn("webhook: generate wake id", "error", err)
		return false
	}
	armLease := sub.Config.Type == DispatchWebhook
	res, err := m.armWakeOwned(scope, sub.ID, time.Now(), sub.Config.LeaseTTLMs, armLease, wakeID)
	if err != nil {
		m.log.Warn("webhook: arm wake", "sub", sub.ID, "error", err)
		return false
	}
	if !res.Armed {
		return false
	}
	failpoint(fpArmedBeforeEmit)
	switch sub.Config.Type {
	case DispatchWebhook:
		if err := durableWakeIntent().RunExternalAction(func() error {
			go m.deliverWebhookOwned(scope, sub.ID, res.Generation, res.WakeID)
			return nil
		}); err != nil {
			m.log.Warn("webhook: dispatch owned wake", "sub", sub.ID, "error", err)
		}
	case DispatchPullWake:
		m.writeWakeEvent(sub, triggerStream, res.Generation, res.WakeID)
	}
	return true
}

func (m *Manager) writeWakeEvent(sub Subscription, triggerStream string, generation int64, wakeID string) {
	m.writeWakeEventExternalized(durablePullWakeUnstampedEmit(), sub, triggerStream, generation, wakeID)
}

func (m *Manager) writeWakeEventExternalized(ext DurableExternalization, sub Subscription, triggerStream string, generation int64, wakeID string) {
	if triggerStream == "" && len(sub.Links) > 0 {
		triggerStream = sub.Links[0].Path
	}
	data, err := NewWakeEvent(sub.ID, triggerStream, generation, time.Now())
	if err != nil {
		return
	}
	appendStart := time.Now()
	if err := ext.RunExternalAction(func() error { return m.streams.AppendWakeEvent(sub.Config.WakeStream, data) }); err != nil {
		m.metrics.WakeEvent(time.Since(appendStart), "error")
		// Leave wake_event_sent_ns at 0 so the recovery sweep re-emits, and trigger
		// an eager reconcile so it re-emits now rather than on the coarse floor
		// (doc-05 correction #2, the delivery-path error event).
		m.log.Warn("webhook: write wake event", "sub", sub.ID, "wake_stream", sub.Config.WakeStream, "error", err)
		m.triggerReconcile(scopeAppendError)
		return
	}
	m.metrics.WakeEvent(time.Since(appendStart), "ok")
	// Record the durable emit, fenced on (generation, wake), so the sweep does
	// not re-emit a wake that was already delivered.
	if err := m.store.RecordWakeEventSent(sub.ID, generation, wakeID, time.Now()); err != nil {
		m.log.Warn("webhook: record wake event sent", "sub", sub.ID, "error", err)
	}
}

func (m *Manager) deliverWebhookUnscoped(id string, generation int64, wakeID string) {
	m.deliverWebhook(id, generation, wakeID, nil)
}

func (m *Manager) deliverWebhookOwned(scope OwnerScope, id string, generation int64, wakeID string) {
	m.deliverWebhook(id, generation, wakeID, &scope)
}

// deliverWebhook signs and POSTs a wake notification, then handles the response:
// a 2xx {done:true} auto-acks the snapshot and releases; any other 2xx clears
// the failure state and leaves the wake in flight for an async callback; a
// non-2xx or transport error schedules a retry (PROTOCOL §7.1).
func (m *Manager) deliverWebhook(id string, generation int64, wakeID string, owner *OwnerScope) {
	// Owner-epoch fence for the EXTERNAL POST (issue #14): the retry worker drives
	// this for a slot it owns, so verify ownership via check_owner immediately
	// before the POST — the one schedule write that cannot inline the check, since
	// the side effect crosses the network. The append-path caller (issueWake)
	// passes no scope and proceeds: the (gen,wake_id) fence on the returned ack is
	// the guard and a duplicate POST coalesces (a double-wake is safe).
	if owner != nil {
		chk, cerr := m.store.CheckOwner(owner.SlotKey, owner.ReplicaID, owner.Epoch)
		if cerr != nil {
			m.log.Warn("webhook: check owner before delivery", "sub", id, "error", cerr)
			return
		}
		if !chk.OK() {
			m.metrics.OwnerFenced("check_owner")
			return
		}
	}
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return
	}
	snapshot, _ := Snapshot(sub.Links, m.tailOf)
	token, err := GenerateToken(m.tokenKey, id, generation, time.Now(), m.tokenTTL(sub), rand.Reader)
	if err != nil {
		m.log.Warn("webhook: mint callback token", "sub", id, "error", err)
		return
	}
	notif := WakeNotification{
		SubscriptionID: id,
		WakeID:         wakeID,
		Generation:     generation,
		Streams:        snapshot,
		CallbackURL:    m.callbackURL(id),
		CallbackToken:  token,
	}
	// wake_token (#123/#126 TB6a): the entity-identity assertion rides the
	// signed notification when the subscription names a single entity.
	// Additive — receivers read fields, and the envelope signature covers
	// whatever body is marshaled.
	if wt, ok := m.mintWakeTokenFor(sub, generation, wakeID, time.Now()); ok {
		notif.WakeToken = wt
	}
	body, err := json.Marshal(notif)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, sub.Config.WebhookURL, bytes.NewReader(body))
	if err != nil {
		m.recordFailure(id, generation, wakeID, owner)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	signing := m.keySnapshot().webhook.active
	if signing.Kid == "" {
		// Emergency stop (#123): the active envelope kid is denylisted with no
		// successor. An unsigned or mis-signed wake must not go out; the retry
		// worker re-attempts after rotation restores a mint key.
		m.log.Error("webhook: envelope signing key unavailable (denylisted); delivery deferred", "sub", id)
		m.recordFailure(id, generation, wakeID, owner)
		return
	}
	req.Header.Set("Webhook-Signature", SignWebhookPayload(signing, body, time.Now()))

	postStart := time.Now()
	resp, err := m.doWebhookRequest(req)
	if err != nil {
		m.metrics.WakeDelivery(time.Since(postStart), "error")
		m.recordFailure(id, generation, wakeID, owner)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.metrics.WakeDelivery(time.Since(postStart), "failed")
		m.recordFailure(id, generation, wakeID, owner)
		return
	}
	m.metrics.WakeDelivery(time.Since(postStart), "ok")

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var parsed struct {
		Done *bool `json:"done"`
	}
	_ = json.Unmarshal(respBody, &parsed)

	status, err := m.recordSuccessWithOwner(owner, id, generation, wakeID)
	if err != nil {
		m.log.Warn("webhook: record success", "sub", id, "error", err)
		return
	}
	if status != "OK" {
		return
	}
	if parsed.Done != nil && *parsed.Done {
		acks := acksFromSnapshot(snapshot)
		// The auto-ack(done) is a schedule/due-mutating write, so it carries the
		// retry worker's owner scope: a deposed owner's done-ack (it released/ZREMed
		// a slot it no longer owns) is FENCED inline, atomically with the write — the
		// same TOCTOU resolution the expire path uses. The append-path caller passes
		// no scope, so its auto-ack stays unfenced (the (gen,wake_id) fence guards it).
		status, err := m.ackWithOwner(owner, id, generation, wakeID, generation, true, acks, time.Now(), sub.Config.LeaseTTLMs)
		if err != nil {
			m.log.Warn("webhook: auto-ack done", "sub", id, "error", err)
			return
		}
		if status == "OK" {
			if owner != nil {
				m.rewakeIfPendingOwned(*owner, id)
			} else {
				m.rewakeIfPendingUnscoped(id)
			}
		}
	}
}

func (m *Manager) doWebhookRequest(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	err := durableWebhookDelivery().RunExternalAction(func() error {
		var doErr error
		resp, doErr = m.client.Do(req) //nolint:bodyclose // caller closes returned response body
		return doErr
	})
	return resp, err
}

func (m *Manager) recordSuccessWithOwner(owner *OwnerScope, id string, generation int64, wakeID string) (string, error) {
	if owner != nil {
		return m.store.RecordSuccessOwned(*owner, id, generation, wakeID)
	}
	return m.store.RecordSuccessUnscoped(id, generation, wakeID)
}

func (m *Manager) recordFailure(id string, generation int64, wakeID string, owner *OwnerScope) {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return
	}
	// GC a webhook that has been failing past the window (mirrors Caddy).
	if sub.FirstFailNs != 0 && time.Since(time.Unix(0, sub.FirstFailNs)) > gcFailureWindow {
		_ = m.store.Delete(id)
		return
	}
	next := time.Now().Add(RetryDelay(sub.RetryCount+1, jitterFraction()))
	// The retry worker drives this for a slot it owns, so it carries the owner
	// scope: schedule_retry inlines the owner-epoch fence and a deposed owner
	// schedules nothing (no phantom retry on a slot it lost). It also carries the
	// delivery's (generation, wakeID), so a duplicate failure cannot resurrect retry
	// state after a concurrent success/ack has cleared the wake. The append-path
	// caller passes no scope (unfenced).
	var schedErr error
	if owner != nil {
		_, schedErr = m.store.ScheduleRetryOwned(*owner, id, generation, wakeID, time.Now(), next)
	} else {
		_, schedErr = m.store.ScheduleRetryUnscoped(id, generation, wakeID, time.Now(), next)
	}
	if schedErr != nil {
		m.log.Warn("webhook: schedule retry", "sub", id, "error", schedErr)
	}
}

func (m *Manager) tokenTTL(sub Subscription) time.Duration {
	// A grace beyond the lease so an in-flight callback's token outlives a
	// just-extended lease.
	return time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + time.Hour
}

func (m *Manager) writeTokenTTL(sub Subscription) time.Duration {
	return time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + writeTokenFenceGrace
}

const writeTokenFenceGrace = 5 * time.Second

func appendFenceFor(sub Subscription) auth.AppendFence {
	return auth.AppendFence{
		SubscriptionID:          sub.ID,
		SubscriptionIncarnation: sub.Incarnation,
		Generation:              sub.Generation,
		WakeID:                  sub.WakeID,
		Holder:                  sub.HolderWorker,
		LeaseUntilNs:            sub.LeaseUntilNs,
	}
}

func (m *Manager) grantWriteFences(sub Subscription) error {
	if m.writeFences == nil {
		return nil
	}
	fence := appendFenceFor(sub)
	if !sub.Holder || sub.Phase != PhaseLive || !fence.Complete() || fence.LeaseUntilNs <= 0 {
		return fmt.Errorf("webhook: cannot grant append fence for non-live claim %q", sub.ID)
	}
	granted := make([]string, 0, len(sub.Links))
	for _, link := range sub.Links {
		installed, err := m.writeFences.GrantAppendFence(link.Path, fence)
		if err != nil {
			for _, path := range granted {
				_ = m.writeFences.RevokeAppendFence(path, fence)
			}
			return fmt.Errorf("webhook: grant append fence for %q on %q: %w", sub.ID, link.Path, err)
		}
		if installed {
			granted = append(granted, link.Path)
		}
	}
	return nil
}

func (m *Manager) revokeWriteFences(sub Subscription) error {
	if m.writeFences == nil || !sub.Holder {
		return nil
	}
	fence := appendFenceFor(sub)
	if !fence.Complete() {
		return nil
	}
	var errs []error
	for _, link := range sub.Links {
		if err := m.writeFences.RevokeAppendFence(link.Path, fence); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", link.Path, err))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) revokeWriteFencePath(sub Subscription, path string) error {
	if m.writeFences == nil || !sub.Holder {
		return nil
	}
	linked := false
	for _, link := range sub.Links {
		if link.Path == path {
			linked = true
			break
		}
	}
	if !linked {
		return nil
	}
	fence := appendFenceFor(sub)
	if !fence.Complete() {
		return nil
	}
	return m.writeFences.RevokeAppendFence(path, fence)
}

func (m *Manager) revokeWriteFencesIfCurrent(
	id string,
	generation int64,
	wakeID string,
	tokenGeneration int64,
) error {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return err
	}
	if !sub.Holder ||
		sub.Phase != PhaseLive ||
		sub.Generation != generation ||
		sub.Generation != tokenGeneration ||
		sub.WakeID != wakeID {
		return nil
	}
	return m.revokeWriteFences(sub)
}

// mintWriteTokenOnAck refreshes a current holder's data-plane write capability
// when the ack response is already refreshing credentials. Done acks end the
// lease, so they mint nothing and preserve the conformance suite's
// {ok,next_wake} done-ack body shape.
func (m *Manager) mintWriteTokenOnAck(id string, generation int64, wakeID string, done bool, now time.Time) (string, bool, error) {
	if done {
		return "", false, nil
	}
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok || sub.Config.Type != DispatchPullWake || sub.Phase != PhaseLive || sub.WakeID != wakeID || sub.Generation != generation || !sub.Holder {
		return "", false, err
	}
	if err := m.grantWriteFences(sub); err != nil {
		return "", false, err
	}
	scope := writeScopeFromLinks(sub.Links)
	tok, err := GenerateClaimWriteToken(m.tokenKey, id, sub.Incarnation, generation, wakeID, sub.HolderWorker, 0, scope, now, m.writeTokenTTL(sub), randReader)
	if err != nil {
		return "", false, err
	}
	return tok, true, nil
}

// mintToken mints a fresh callback/claim token for a subscription at the given
// generation, TTL'd off the sub's lease (tokenTTL). It is the imperative-shell
// step the in-band token refresh and the expired-token retry path share (issue
// #77); ok is false when the sub is gone or minting fails, so the caller falls
// back to the plain response.
func (m *Manager) mintToken(id string, generation int64, now time.Time) (string, bool) {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return "", false
	}
	tok, err := GenerateToken(m.tokenKey, id, generation, now, m.tokenTTL(sub), randReader)
	if err != nil {
		return "", false
	}
	return tok, true
}

// rewakeIfPendingUnscoped re-issues a wake when work remains after an external
// callback/release (PROTOCOL §7.2/§7.3). Returns whether a re-wake was issued
// (the next_wake flag).
func (m *Manager) rewakeIfPendingUnscoped(id string) bool {
	return m.rewakeIfPending(id, m.issueWake)
}

// rewakeIfPendingOwned is rewakeIfPendingUnscoped plus the owner-epoch fence for
// owner-driven auto-acks. A deposed retry worker must not silently fall back to
// the external/unscoped arm path while re-waking post-ack work.
func (m *Manager) rewakeIfPendingOwned(scope OwnerScope, id string) bool {
	return m.rewakeIfPending(id, func(sub Subscription, triggerStream string) bool {
		return m.issueWakeOwned(scope, sub, triggerStream)
	})
}

func (m *Manager) rewakeIfPending(id string, issue func(Subscription, string) bool) bool {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return false
	}
	if sub.Phase != PhaseIdle || !HasPendingWork(sub.Links, m.tailOf) {
		return false
	}
	return issue(sub, "")
}

func acksFromSnapshot(snap []StreamSnapshot) []Ack {
	acks := make([]Ack, 0, len(snap))
	for _, s := range snap {
		if s.HasPending {
			acks = append(acks, Ack{Stream: s.Path, Offset: s.TailOffset})
		}
	}
	return acks
}

// jitterFraction returns a crypto-random fraction in [0,1) for backoff jitter.
func jitterFraction() float64 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<53))
	if err != nil {
		return 0
	}
	return float64(n.Int64()) / float64(int64(1)<<53)
}

// ---- due-set outbox mutators (Move 2) ----
//
// arm_wake / ack(done) / release / expire_lease are the four scripts that mutate
// the ds:{__ds}:due "needs a wake" outbox. These thin wrappers record the
// DueSetMutation the corresponding Lua branch performed, at the one place the
// reply reveals which branch ran, so the metric stays honest while the store
// stays free of the Metrics seam. Each is the sole entry point its callers use,
// so a mutation cannot escape unrecorded.

// armWakeUnscoped arms a wake (arm_wake): the ARMED branch ZADDs the due mark.
func (m *Manager) armWakeUnscoped(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error) {
	res, err := m.store.ArmWakeUnscoped(id, now, leaseTTLMs, armLease, wakeID)
	if err == nil && res.Armed {
		m.metrics.DueSetMutation("arm")
	}
	return res, err
}

// armWakeOwned is armWakeUnscoped plus the owner-epoch fence for dueWorker.
func (m *Manager) armWakeOwned(scope OwnerScope, id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error) {
	res, err := m.store.ArmWakeOwned(scope, id, now, leaseTTLMs, armLease, wakeID)
	if err == nil && res.Armed {
		m.metrics.DueSetMutation("arm")
	}
	return res, err
}

// ackUnscoped fences and acks (ack): the done branch ZREMs the due mark; a
// heartbeat (done=false) does not, so only a done-ack records the mutation.
func (m *Manager) ackUnscoped(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error) {
	status, err := m.store.AckUnscoped(id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs)
	if err == nil && done && status == "OK" {
		m.metrics.DueSetMutation("ack")
	}
	return status, err
}

// ackOwned is ackUnscoped plus the owner-epoch fence for owner-driven auto-acks.
func (m *Manager) ackOwned(scope OwnerScope, id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error) {
	status, err := m.store.AckOwned(scope, id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs)
	if err == nil && done && status == "OK" {
		m.metrics.DueSetMutation("ack")
	}
	return status, err
}

func (m *Manager) ackWithOwner(owner *OwnerScope, id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error) {
	if owner != nil {
		return m.ackOwned(*owner, id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs)
	}
	return m.ackUnscoped(id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs)
}

// release voluntarily releases the lease (release): the idle-reset branch ZREMs
// the due mark (GAP3).
func (m *Manager) release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64) (string, error) {
	status, err := m.store.ReleaseUnscoped(id, reqGeneration, reqWakeID, tokenGeneration)
	if err == nil && status == "OK" {
		m.metrics.DueSetMutation("release")
	}
	return status, err
}

// expireLeaseUnscoped clears an expired lease (expire_lease): the EXPIRED branch
// re-owes (ZADDs) the due mark so the dueWorker re-fires it.
func (m *Manager) expireLeaseUnscoped(id string, now time.Time) (string, error) {
	status, err := m.store.ExpireLeaseUnscoped(id, now)
	if err == nil && status == "EXPIRED" {
		m.metrics.DueSetMutation("expire")
	}
	return status, err
}

// expireLeaseOwned is expireLeaseUnscoped plus the owner-epoch fence; a FENCED
// result is a deposed owner's expiry suppressed atomically.
func (m *Manager) expireLeaseOwned(scope OwnerScope, id string, now time.Time) (string, error) {
	status, err := m.store.ExpireLeaseOwned(scope, id, now)
	if err == nil && status == "EXPIRED" {
		m.metrics.DueSetMutation("expire")
	}
	return status, err
}

// ---- background loops ----

// Start launches every Manager-owned loop once. It first runs boot recovery
// without holding the lifecycle mutex, then publishes the running state before
// returning. A concurrent Stop cancels startup and waits for this transition.
func (m *Manager) Start() {
	m.lifeMu.Lock()
	if m.life != managerNew {
		m.lifeMu.Unlock()
		return
	}
	m.life = managerStarting
	m.lifeMu.Unlock()

	m.reconcile(scopeBoot)
	if m.runCtx.Err() != nil {
		m.finishStart(false)
		return
	}
	// Join membership and claim our owned slots BEFORE the fast workers tick, so a
	// fresh replica does not idle a whole slotReconcileInterval before owning work
	// (and so the boot owner is established before serving). Both are best-effort:
	// a failure here is retried by the loops below.
	if err := m.store.Heartbeat(m.replicaID.String(), time.Now(), m.memberLeaseTTL); err != nil {
		m.log.Warn("webhook: initial heartbeat", "replica", m.replicaID, "error", err)
	}
	if m.runCtx.Err() != nil {
		m.finishStart(false)
		return
	}
	m.slotReconcileOnce()
	if !m.finishStart(true) {
		return
	}
	go m.keysReloadLoop()
	go m.leaseWorker()
	go m.retryWorker()
	go m.dueWorker()
	go m.dirtyWorker()
	go m.recoveryLoop()
	go m.reconcileLoop()
	go m.heartbeatLoop()
	go m.slotReconcileLoop()
}

// finishStart publishes the end of startup. The wait-group increment happens
// before startDone closes, so Stop cannot race Wait against Add.
func (m *Manager) finishStart(run bool) bool {
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	if !run || m.life == managerStopping || m.runCtx.Err() != nil {
		m.life = managerStopped
		close(m.startDone)
		return false
	}
	m.wg.Add(9)
	m.life = managerRunning
	close(m.startDone)
	return true
}

// Stop signals the background loops and waits for them to drain. It does NOT
// explicitly release owned slots: lease expiry is the authoritative handoff
// (05:248), so a surviving replica reclaims this one's slots once its slot lease
// (and membership lease) lapse. The full sweep covers the interim coverage gap.
func (m *Manager) Stop() {
	m.lifeMu.Lock()
	switch m.life {
	case managerNew:
		m.life = managerStopped
		m.cancelRun()
		close(m.startDone)
	case managerStarting:
		m.life = managerStopping
		m.cancelRun()
	case managerRunning:
		m.life = managerStopped
		m.cancelRun()
	case managerStopping, managerStopped:
		m.cancelRun()
	}
	startDone := m.startDone
	m.lifeMu.Unlock()

	m.dirtyMu.Lock()
	m.dirtyClosed = true
	stats := m.dirty.stats(m.now())
	m.dirtyMu.Unlock()
	m.metrics.DirtyQueue(stats.depth, stats.capacity, stats.oldestAge)

	<-startDone
	m.wg.Wait()
}

// dirtyWorker owns async append fan-out. A notification starts work immediately.
// After an error, retries wait for workerTick; this is the bounded-delay analogue
// of a delaying workqueue and prevents a Redis outage from becoming a hot loop.
func (m *Manager) dirtyWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.workerTick)
	defer ticker.Stop()
	retrying := false
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-m.dirtyNotify:
			if retrying {
				continue
			}
			_, retrying = m.processDirtyBatch()
			if m.dirtyOverflowPending() {
				m.triggerReconcile(scopeDirtyOverflow)
			}
		case <-ticker.C:
			_, retrying = m.processDirtyBatch()
			if m.dirtyOverflowPending() {
				m.triggerReconcile(scopeDirtyOverflow)
			}
		}
	}
}

// leaseWorker expires due leases (PROTOCOL §7.3). Due members are re-scored
// forward, so a crash mid-handling leaves the lease to fall due again. The
// EXPIRED branch re-owes the due-set; re-firing a still-pending subscription is
// the dueWorker's job (Move 2 — doc-05 background-loop change map), so this loop
// no longer re-evaluates each expired sub inline.
func (m *Manager) leaseWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.workerTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			// Work-sharded: a replica runs the lease worker only over the slots it
			// owns (issue #14, now real S slots — #15). For each owned slot it drains
			// that slot's per-slot lease schedule and presents the slot's owner scope,
			// so a just-deposed owner's expiry/re-owe is FENCED atomically (TOCTOU).
			// The full sweep stays the unguarded backstop for unowned slots.
			now := time.Now()
			for _, h := range m.ownedSlots() {
				scope, ok := m.ownerScope(h)
				if !ok {
					continue
				}
				ids, err := m.store.DueLeases(h.Index(), now, dueClaimLimit, m.workerTick*2)
				if err != nil {
					continue
				}
				if len(ids) > 0 {
					m.metrics.WorkerTick("lease", len(ids))
				}
				for _, id := range ids {
					_, _ = m.expireLeaseOwned(scope, id, now) // EXPIRED re-owes the due-set; dueWorker re-fires
				}
			}
		}
	}
}

// dueWorker drains the "needs a wake" due-set outbox (Move 2): it claims owed
// subscriptions in O(owed) via the unchanged claim_due.lua (re-score-forward,
// never ZREM — at-least-once by construction) and reconciles each against its
// live state. This is the event-driven replacement for re-firing owed wakes by
// re-evaluating every subscription on every tick; the full recovery sweep stays
// the correctness backstop for what the outbox cannot cover (an owed mark on an
// unowned, quiet slot — narrowed further in #13).
func (m *Manager) dueWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.workerTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			// Work-sharded: a replica drains the due-set only for its owned slots
			// (issue #14, real S slots — #15). The directly-invokable drainDue
			// (RunDueWorker, tests) stays ungated, sweeping every slot.
			for _, h := range m.ownedSlots() {
				scope, ok := m.ownerScope(h)
				if !ok {
					continue
				}
				m.drainDueOwned(h.Index(), scope)
			}
		}
	}
}

// drainDue runs one due-set drain over SLOT h: claim h's owed ids in O(owed) and
// reconcile each. Split out so a test can drive a single pass deterministically (cf.
// RunSweep). It records DueWorkerTick only for non-empty passes, so the duration
// histogram reflects real work rather than idle ticks. Returns the number of wakes
// fired.
func (m *Manager) drainDue(h int) int {
	return m.drainDueWithFire(h, m.fireDue)
}

func (m *Manager) drainDueOwned(h int, scope OwnerScope) int {
	return m.drainDueWithFire(h, func(id string) bool { return m.fireDueOwned(scope, id) })
}

func (m *Manager) drainDueWithFire(h int, fire func(string) bool) int {
	start := time.Now()
	ids, err := m.store.ClaimDue(h, start, dueClaimLimit, m.workerTick*2)
	if err != nil || len(ids) == 0 {
		return 0
	}
	fired := 0
	for _, id := range ids {
		if fire(id) {
			fired++
		}
	}
	m.metrics.DueWorkerTick(time.Since(start), fired)
	return fired
}

// RunDueWorker drains every slot's due-set once immediately (tests). It is ungated
// by ownership (the test driver, unlike the dueWorker loop), so it finds an owed sub
// in whichever slot it is homed.
func (m *Manager) RunDueWorker() int {
	fired := 0
	for _, h := range AllSlots() {
		fired += m.drainDue(h.Index())
	}
	return fired
}

// fireDue reconciles one drained due-set mark against the subscription's live
// state (DecideDue): re-fire an owed idle sub, clear a stale mark (gone, or idle
// with its cursor caught up), or leave an in-flight wake to coalesce. A mark
// wrongly cleared by a race with a concurrent re-arm is re-covered by the
// retained full sweep — the due-set is an optimization over a still-correct
// baseline (epic #9, correction #1). Returns whether a wake was issued.
func (m *Manager) fireDue(id string) bool {
	return m.fireDueWithIssue(id, m.issueWake)
}

func (m *Manager) fireDueOwned(scope OwnerScope, id string) bool {
	return m.fireDueWithIssue(id, func(sub Subscription, triggerStream string) bool { return m.issueWakeOwned(scope, sub, triggerStream) })
}

func (m *Manager) fireDueWithIssue(id string, issue func(Subscription, string) bool) bool {
	sub, ok, err := m.store.Get(id)
	if err != nil {
		return false
	}
	switch DecideDue(ok, sub.Phase, ok && HasPendingWork(sub.Links, m.tailOf)) {
	case DueFire:
		return issue(sub, "")
	case DueClear:
		if err := m.store.ClearDue(id); err != nil {
			m.log.Warn("webhook: clear due mark", "sub", id, "error", err)
		}
	case DueSkip:
		// a wake is in flight; the mark clears on the eventual done-ack/release
	}
	return false
}

// retryWorker re-delivers webhooks whose backoff has elapsed (PROTOCOL §7.1).
func (m *Manager) retryWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.workerTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			// Work-sharded: a replica runs the retry worker only over its owned slots
			// (issue #14, real S slots — #15), draining each slot's per-slot retry
			// schedule under that slot's owner scope.
			now := time.Now()
			for _, h := range m.ownedSlots() {
				scope, ok := m.ownerScope(h)
				if !ok {
					continue
				}
				ids, err := m.store.DueRetries(h.Index(), now, dueClaimLimit, m.workerTick*2)
				if err != nil {
					continue
				}
				if len(ids) > 0 {
					m.metrics.WorkerTick("retry", len(ids))
				}
				for _, id := range ids {
					sub, ok, err := m.store.Get(id)
					if err != nil || !ok || sub.Phase != PhaseWaking {
						continue
					}
					// deliverWebhook gates the external POST on check_owner OWNER (the one
					// write that cannot inline the check — it crosses the network).
					m.deliverWebhookOwned(scope, id, sub.Generation, sub.WakeID)
				}
			}
		}
	}
}

// scope is the sealed reason a recovery reconcile fired — the recovery-event
// taxonomy of doc-05 correction #2. Every recovery event routes through the one
// reconcile(scope) seam, so #14's owner-epoch-bump / new-owner-CAS trigger plugs
// into a named case rather than forcing a refactor. It is an unexported int over a
// fixed set of constants: an out-of-set scope is unrepresentable.
type scope int

const (
	scopeBoot          scope = iota // process boot: re-fire anything owed before serving
	scopeReconnect                  // a Redis reconnect: the connection that lost in-flight ops healed
	scopeAppendError                // an OnStreamAppend / wake-event append failed: the low-latency wake path errored
	scopeFloor                      // the coarse periodic floor: the one eventless case (an owed mark on an unowned, quiet slot)
	scopeEpochBump                  // #16: a DR promotion / owner_epoch bump drives the eager reconcile (Manager.Promote)
	scopeNewOwnerCAS                // #14: a new-owner claim_shard CAS reconciles its freshly-claimed slot
	scopeDirtyOverflow              // the bounded append-hint queue overflowed and needs a cursor rebuild
)

func (s scope) String() string {
	switch s {
	case scopeBoot:
		return "boot"
	case scopeReconnect:
		return "reconnect"
	case scopeAppendError:
		return "append-error"
	case scopeFloor:
		return "floor"
	case scopeEpochBump:
		return "epoch-bump"
	case scopeNewOwnerCAS:
		return "new-owner-cas"
	case scopeDirtyOverflow:
		return "dirty-overflow"
	default:
		return "unknown"
	}
}

// reconcile is the single recovery seam every recovery event routes through
// (doc-05 correction #2). The detectable events — boot, a Redis reconnect, an
// append/delivery-path error — and the coarse periodic floor all run the same full
// cursor reconcile (sweepOnce), which re-derives owed work from the durable cursor
// and includes the failover-aware reconcileLeases pass.
//
// #14 fills in the owner-epoch-bump / new-owner-CAS scopes that #13 stubbed: when
// the slot-reconcile loop CASes a slot to a NEW owner (a transfer / epoch bump),
// it fires this seam so the new owner EAGERLY re-derives the freshly-claimed
// slot's owed work at the takeover TRIGGER — closing the rebalance coverage gap at
// <= membership-lease TTL + RTT, not on a floor tick (07 L2). At S=1 the slot's
// subscriptions are the whole keyspace, so the eager reconcile is the same full
// sweepOnce (which re-derives the lease/due tails via reconcileLeases); #15
// narrows it to the freshly-claimed slot's subs once state is slot-homed.
func (m *Manager) reconcile(s scope) {
	switch s {
	case scopeBoot, scopeReconnect, scopeAppendError, scopeFloor, scopeEpochBump, scopeNewOwnerCAS, scopeDirtyOverflow:
		m.dirtyMu.Lock()
		overflowSince, coveringOverflow := m.dirty.beginReconcile()
		m.dirtyMu.Unlock()

		success := m.sweepOnce()
		if !coveringOverflow {
			return
		}
		m.dirtyMu.Lock()
		requestAgain := m.dirty.completeReconcile(success)
		stats := m.dirty.stats(m.now())
		m.dirtyMu.Unlock()
		m.metrics.DirtyQueue(stats.depth, stats.capacity, stats.oldestAge)
		if success {
			m.metrics.DirtyRecoveryDelay(m.now().Sub(overflowSince))
		}
		if requestAgain && success {
			// An append overflowed after the successful sweep began, so its later
			// epoch needs another pass. Failed sweeps retry on dirtyWorker's tick to
			// avoid a hot loop during a Redis outage.
			m.triggerReconcile(scopeDirtyOverflow)
		}
	}
}

// triggerReconcile routes a recovery event to the single reconcile seam without
// blocking the caller (OnStreamAppend, a reconnect callback). The send is
// non-blocking onto the depth-1 reconcileC, so concurrent events coalesce into at
// most one queued reconcile while one runs: duplicate reconciles are claim-fence-
// safe, and a storm of append errors cannot pile up a reconcile per error.
func (m *Manager) triggerReconcile(s scope) {
	result := "enqueued"
	select {
	case m.reconcileC <- s:
	default:
		// a reconcile is already queued; this event coalesces into it.
		result = "coalesced"
	}
	if m.metrics != nil {
		m.metrics.ReconcileRequest(s.String(), result)
	}
}

// recoveryLoop is the recovery backstop (doc-05 correction #2): the coarse
// periodic floor plus the event-triggered reconciles coalesced onto reconcileC.
// The floor runs in the seconds-to-minutes band (sweepInterval, default 30s), NOT
// the old 2s fast sweep — the latency-sensitive cases are event-triggered now
// (boot/reconnect/append-error, and from #14 the owner-epoch bump), so the floor
// bounds only the one eventless case: an owed mark lost on a slot that is unowned
// and quiet. It replaces recoverySweeper, folding the timer and the event triggers
// into one loop so sweepOnce stays single-goroutine.
func (m *Manager) recoveryLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.reconcile(scopeFloor)
		case s := <-m.reconcileC:
			m.reconcile(s)
		}
	}
}

// ---- leased slot ownership (issue #14) ----
//
// This shards autonomous BACKGROUND work across replicas by a leased slot: only
// the slot owner runs the fast lease/retry/due workers for it, so total work is
// O(total owed) regardless of N. The full sweep (sweepOnce) stays the UNGUARDED
// backstop covering unowned slots — work-sharding is an optimization over a
// still-correct baseline (06 correction #1), never a correctness dependency. This
// axis is orthogonal to #11's per-(subId,g) claim granularity.

// heartbeatLoop re-ZADDs this replica into the members ZSET every
// heartbeatInterval (and evicts expired members), proving liveness so HRW keeps
// assigning it slots. A missed beat past memberLeaseTTL drops it from the live
// set and its slots become claimable by the survivors.
func (m *Manager) heartbeatLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			if err := m.store.Heartbeat(m.replicaID.String(), time.Now(), m.memberLeaseTTL); err != nil {
				m.log.Warn("webhook: membership heartbeat", "replica", m.replicaID, "error", err)
			}
		}
	}
}

// slotReconcileLoop recomputes the HRW assignment and (re)claims owned slots every
// slotReconcileInterval. It is the loop that drives ownedSlots(); a dead member
// ages out of the member set, so on the next tick a survivor's HRW targets its
// slots and claim_shard takes them over (a transfer / epoch bump), firing the
// eager reconcile.
func (m *Manager) slotReconcileLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.slotReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.slotReconcileOnce()
		}
	}
}

// slotReconcileOnce reads the live member set, computes the HRW target for every
// slot, and CASes the ones this replica targets via claim_shard. It then snapshots
// the held set (HRW-targeted ∩ claim_shard-granted) that ownedSlots() returns —
// THE CAS IS THE AUTHORITY, NOT THE HRW MATH (05:399-402): a slot is "owned" only
// when claim_shard granted it AND HRW still targets it here. A transfer (CLAIMED —
// a new-owner CAS / epoch bump) fires #13's reconcile(scope) so the freshly-claimed
// slot's owed work is re-derived at the takeover trigger.
func (m *Manager) slotReconcileOnce() {
	now := time.Now()
	memberStrs, err := m.store.LiveMembers(now)
	if err != nil {
		m.log.Warn("webhook: read live members", "error", err)
		return
	}
	members := make([]ReplicaID, 0, len(memberStrs)+1)
	for _, s := range memberStrs {
		if r, rerr := NewReplicaID(s); rerr == nil {
			members = append(members, r)
		}
	}
	// Be over-inclusive of ourselves if our heartbeat has not yet landed (a fresh
	// boot, or a transient read): HRW may then assign us a slot we go on to CAS.
	// Safe — the CAS is the real authority, so an over-inclusive member set at
	// worst produces a double-claim attempt that BUSY-rejects.
	if !containsReplica(members, m.replicaID) {
		members = append(members, m.replicaID)
	}
	targeted := TargetedSlots(m.replicaID, members, AllSlots())
	newHeld := make(map[SlotID]OwnerEpoch, len(targeted))
	for h := range targeted {
		claim, cerr := m.store.ClaimSlot(slotKey(h.Index()), m.replicaID.String(), now, m.slotLeaseTTL)
		if cerr != nil {
			m.log.Warn("webhook: claim slot", "slot", h, "error", cerr)
			continue
		}
		switch claim.Status {
		case SlotClaimed:
			m.metrics.SlotOwnership("claimed", h.Index())
			newHeld[h] = claim.Epoch
			// A transfer / new-owner CAS: eagerly reconcile the freshly-claimed slot
			// at the takeover trigger (fires #13's reconcile seam — the EpochBump /
			// NewOwnerCAS scopes #13 stubbed). Coalesced onto the recovery loop so
			// sweepOnce stays single-goroutine.
			m.triggerReconcile(scopeNewOwnerCAS)
		case SlotRenewed:
			m.metrics.SlotOwnership("renewed", h.Index())
			newHeld[h] = claim.Epoch
		case SlotBusy:
			// A live foreign owner holds it: we do NOT run its work (CAS authority).
			m.metrics.SlotOwnership("busy", h.Index())
		}
	}
	m.ownMu.Lock()
	m.held = newHeld
	m.ownMu.Unlock()
}

// RunSlotReconcile runs one slot-reconcile pass immediately (startup and tests).
func (m *Manager) RunSlotReconcile() { m.slotReconcileOnce() }

// ownedSlots is the slots this replica currently owns (HRW-targeted ∩
// claim_shard-granted), snapshotted at the last reconcile tick. The fast workers
// iterate it; a brief stale-read disagreement is SAFE (a double-wake coalesces,
// and a zero-owner gap is covered by the full sweep until claim_shard resolves it).
func (m *Manager) ownedSlots() []SlotID {
	m.ownMu.RLock()
	defer m.ownMu.RUnlock()
	out := make([]SlotID, 0, len(m.held))
	for h := range m.held {
		out = append(out, h)
	}
	return out
}

// ownsAnySlot reports whether this replica holds any slot lease — the gate the
// fast lease/retry/due workers check before doing work.
func (m *Manager) ownsAnySlot() bool {
	m.ownMu.RLock()
	defer m.ownMu.RUnlock()
	return len(m.held) > 0
}

// ownerScope builds the OwnerScope for an owned slot so a background-worker write
// inlines the owner-epoch fence atomically. ok is false if the slot is no longer
// held (a reconcile released it between calls).
func (m *Manager) ownerScope(h SlotID) (OwnerScope, bool) {
	m.ownMu.RLock()
	defer m.ownMu.RUnlock()
	e, ok := m.held[h]
	if !ok {
		return OwnerScope{}, false
	}
	return OwnerScope{SlotKey: slotKey(h.Index()), ReplicaID: m.replicaID.String(), Epoch: e.String()}, true
}

// containsReplica reports whether r is in the member set.
func containsReplica(members []ReplicaID, r ReplicaID) bool {
	for _, m := range members {
		if m.id == r.id {
			return true
		}
	}
	return false
}

func (m *Manager) sweepOnce() bool {
	start := time.Now()
	ids, err := m.store.List()
	if err != nil {
		return false
	}
	if len(ids) == 0 {
		return true
	}
	ids = m.sweepWindow(ids)
	now := time.Now()
	// Batch the per-tick reads. The sweep is O(subscriptions x links) and the
	// naive form was one round trip per subscription (Get) plus one per link
	// (tail) — the poll backstop's scaling ceiling. GetMany pipelines the
	// subscription reads; TailOffsets pipelines every linked tail into one batch.
	subs, err := m.store.GetMany(ids)
	if err != nil {
		return false
	}
	// Collect tails across all subs (not just idle ones) so a subscription that
	// lease expiry flips to idle below still has its tails in the batch.
	paths := distinctLinkPaths(subs)
	tails, err := m.streams.TailOffsets(paths)
	if err != nil {
		return false
	}
	snapshot := RecoverySnapshot{
		Subs:               subs,
		Tails:              tails,
		Leased:             m.leasedSet(),
		Now:                now,
		StalePullWakeAfter: 3 * m.sweepInterval,
	}
	wakes := 0
	for _, phase := range recoveryPipeline() {
		result := phase.Decide(snapshot)
		next, phaseWakes := phase.Apply(m, snapshot, result, start)
		snapshot = next
		wakes += phaseWakes
	}
	perSub := perSubscriptionRecoveryPipeline()
	for _, sub := range subs {
		for _, phase := range perSub {
			result := phase.Decide(snapshot.onlySub(sub.ID))
			next, phaseWakes := phase.Apply(m, snapshot, result, start)
			snapshot = next
			wakes += phaseWakes
			if snapshot.handled(sub.ID) {
				break
			}
		}
	}
	m.metrics.SweepTick(time.Since(start), len(subs), len(paths), wakes)
	return true
}

// leasedSet reads the lease-ZSET membership into a set for the failover-aware
// reconcile to diff against. An error yields a nil set, which reconcileLeases
// treats as "nothing currently leased" — conservative: it may attempt a restore
// the script then no-ops (INTACT) for a sub that is in fact present, never the
// reverse, so a transient read error cannot strand a sub.
func (m *Manager) leasedSet() map[string]struct{} {
	ids, err := m.store.LeasedIDs()
	if err != nil {
		m.log.Warn("webhook: read lease members for reconcile", "error", err)
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// reconcileLeases is the failover-aware eager reconcile (doc-05 §"failover-aware
// eager reconcile") — the primary, not periodic, recovery path. sweepOnce's other
// passes re-wake only idle subs, so a sub stuck live/waking whose lease-ZSET entry
// a failover dropped (the L3 dropLeaseTail fault) is invisible to the lease worker
// and, with the floor raised off 2s, would otherwise wait seconds-to-minutes for a
// floor tick. This diffs the durable sub set against the lease-ZSET membership and,
// for each sub the pure DecideLeaseReconcile flags as stranded, re-derives its
// dropped schedule entries from the durable hash (RestoreLease): the lease entry so
// the fast lease worker drives its expiry, and the due mark when there is pending
// work so the dueWorker re-fires once idle. Idempotent and fence-safe (a re-ZADD of
// a present entry rewrites the same score; the restore is conditioned on the hash
// still being live/waking). Returns the number restored.
func (m *Manager) reconcileLeases(subs []Subscription, tails map[string]string, leased map[string]struct{}, now time.Time) int {
	restored := 0
	for _, sub := range subs {
		_, inLease := leased[sub.ID]
		if DecideLeaseReconcile(sub.Phase, sub.LeaseUntilNs, inLease) != LeaseStranded {
			continue
		}
		owed := HasPendingWorkFrom(sub.Links, tails)
		status, err := m.store.RestoreLease(sub.ID, owed, now)
		if err != nil {
			m.log.Warn("webhook: restore stranded lease", "sub", sub.ID, "error", err)
			continue
		}
		if status == "RESTORED" {
			restored++
		}
	}
	return restored
}

// sweepWindow optionally bounds a sweep tick to sweepBatch subscriptions,
// advancing a rolling cursor so every id is covered over successive ticks. With
// sweepBatch <= 0 (the default) it returns every id and the sweep is unbounded.
// Ids are sorted when a cap is active so the rolling window is stable across the
// unordered SMEMBERS result. Recovery latency for any one subscription becomes up
// to ceil(K/sweepBatch) ticks, traded for a bounded per-tick cost.
func (m *Manager) sweepWindow(ids []string) []string {
	if m.sweepBatch <= 0 || len(ids) <= m.sweepBatch {
		m.sweepCursor = 0
		return ids
	}
	sort.Strings(ids)
	start := m.sweepCursor
	if start >= len(ids) {
		start = 0
	}
	end := start + m.sweepBatch
	if end <= len(ids) {
		m.sweepCursor = end % len(ids)
		return ids[start:end]
	}
	window := make([]string, 0, m.sweepBatch)
	window = append(window, ids[start:]...)
	window = append(window, ids[:end-len(ids)]...)
	m.sweepCursor = end - len(ids)
	return window
}

// distinctLinkPaths is the deduplicated set of stream paths linked by any of subs
// — the input to one batched tail read for the whole sweep.
func distinctLinkPaths(subs []Subscription) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(subs))
	for _, sub := range subs {
		for _, l := range sub.Links {
			if _, ok := seen[l.Path]; ok {
				continue
			}
			seen[l.Path] = struct{}{}
			out = append(out, l.Path)
		}
	}
	return out
}

// RunSweep runs one recovery sweep immediately (used at startup and in tests).
func (m *Manager) RunSweep() { m.reconcile(scopeFloor) }

// ---- route-level operations (called by routes.go) ----

// seedLinks builds the explicit stream links for a new subscription: each is
// linked at the stream's current tail if it exists (no replay of history,
// PROTOCOL §6.2), else at the beginning so a later first append wakes it.
func (m *Manager) seedLinks(cfg Config) []StreamLink {
	links := make([]StreamLink, 0, len(cfg.Streams))
	for _, path := range cfg.Streams {
		off := m.streams.BeginningOffset()
		if tail, ok := m.tailOf(path); ok {
			off = tail
		}
		links = append(links, StreamLink{Path: path, LinkType: LinkExplicit, AckedOffset: off})
	}
	return links
}

// backfill eagerly links existing streams matching a new subscription's pattern
// at their current tail (PROTOCOL §6.2): no replay of history at create time.
// Best-effort — it needs a StreamLister, and the reconcile loop recovers any link
// a crash in this path drops.
func (m *Manager) backfill(id string, cfg Config) {
	if m.lister == nil || cfg.Pattern == "" {
		return
	}
	streams, err := m.lister.ListStreams()
	if err != nil {
		m.log.Warn("webhook: backfill list", "sub", id, "error", err)
		return
	}
	for _, st := range streams {
		if !GlobMatch(cfg.Pattern, st.Path) {
			continue
		}
		if err := m.store.Link(id, st.Path, LinkGlob, st.Tail); err != nil {
			m.log.Warn("webhook: backfill link", "sub", id, "path", st.Path, "error", err)
		}
	}
}

// reconcilePatternLinks recovers glob links missed when OnStreamCreated or the
// initial backfill was lost to a crash. A missed glob link does not self-heal: a
// later append to an unlinked stream has no subscriber in the fan-out to wake,
// and the sweep only re-evaluates existing links. So it lists streams once and,
// for each pattern subscription, links any matching stream it is missing — at the
// beginning offset when the stream was created after the subscription (a missed
// OnStreamCreated, so its data should wake) or at the current tail when it
// predates the subscription (a missed pre-existing backfill, no replay). This is
// O(pattern subs × streams); it runs on the slow reconcile loop, not the 2s sweep.
func (m *Manager) reconcilePatternLinks() {
	if m.lister == nil {
		return
	}
	ids, err := m.store.List()
	if err != nil {
		return
	}
	streams, err := m.lister.ListStreams()
	if err != nil || len(streams) == 0 {
		return
	}
	begin := m.streams.BeginningOffset()
	for _, id := range ids {
		sub, ok, err := m.store.Get(id)
		if err != nil || !ok || sub.Config.Pattern == "" {
			continue
		}
		linked := make(map[string]struct{}, len(sub.Links))
		for _, l := range sub.Links {
			linked[l.Path] = struct{}{}
		}
		subCreatedNs := sub.CreatedAt.UnixNano()
		relinked := false
		for _, st := range streams {
			if _, ok := linked[st.Path]; ok {
				continue
			}
			if !GlobMatch(sub.Config.Pattern, st.Path) {
				continue
			}
			offset := st.Tail
			if st.CreatedAtNs > subCreatedNs {
				offset = begin // created during the outage: deliver from the start
			}
			if err := m.store.Link(id, st.Path, LinkGlob, offset); err != nil {
				m.log.Warn("webhook: reconcile link", "sub", id, "path", st.Path, "error", err)
				continue
			}
			relinked = true
		}
		if relinked {
			m.maybeWake(id, "")
		}
	}
}

// reconcileLoop runs the slow reconciliation backstop (pattern link recovery and,
// from slice 4, fan-out index repair): once at start, then on the reconcile
// interval. It is deliberately separate from the fast 2s sweep because it scans
// the whole stream keyspace.
func (m *Manager) reconcileLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.reconcileInterval)
	defer ticker.Stop()
	m.reconcileOnce()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.reconcileOnce()
		}
	}
}

func (m *Manager) reconcileOnce() {
	if err := m.store.ReconcileIndexes(); err != nil {
		m.log.Warn("webhook: reconcile fan-out indexes", "error", err)
	}
	m.reconcilePatternLinks()
}

// RunReconcile runs one reconciliation pass immediately (startup and tests).
func (m *Manager) RunReconcile() { m.reconcileOnce() }

// validateWebhookURL applies the SSRF rules and returns the rejection reason, or
// "" when the URL is acceptable.
func (m *Manager) validateWebhookURL(rawURL string) string {
	if ok, reason := ClassifyWebhookURL(rawURL, m.resolver, m.allowPrivate); !ok {
		return reason
	}
	return ""
}

// applyAck fences and applies an ack/callback, returning the HTTP-facing outcome:
// fenced (409 FENCED), or ok with the next_wake flag. The done case releases the
// lease and re-wakes if pending; the heartbeat case extends the lease.
func (m *Manager) applyAck(id string, req CallbackRequest, tokenGeneration int64) (fenced, gone bool, nextWake bool, err error) {
	sub, ok, gerr := m.store.Get(id)
	if gerr != nil {
		return false, false, false, gerr
	}
	if !ok {
		return false, true, false, nil
	}
	done := req.Done != nil && *req.Done
	if done {
		if err := m.revokeWriteFencesIfCurrent(id, req.Generation, req.WakeID, tokenGeneration); err != nil {
			return false, false, false, err
		}
	}
	status, aerr := m.ackUnscoped(id, req.Generation, req.WakeID, tokenGeneration, done, req.Acks, time.Now(), sub.Config.LeaseTTLMs)
	if aerr != nil {
		return false, false, false, aerr
	}
	switch status {
	case "FENCED":
		return true, false, false, nil
	case "NOSUB":
		return false, true, false, nil
	}
	if done {
		nextWake = m.rewakeIfPendingUnscoped(id)
	}
	return false, false, nextWake, nil
}

// applyRelease fences and releases the lease, re-waking if pending (PROTOCOL §7.2).
func (m *Manager) applyRelease(id string, req ReleaseRequest, tokenGeneration int64) (fenced, gone bool, err error) {
	if err := m.revokeWriteFencesIfCurrent(id, req.Generation, req.WakeID, tokenGeneration); err != nil {
		return false, false, err
	}
	status, rerr := m.release(id, req.Generation, req.WakeID, tokenGeneration)
	if rerr != nil {
		return false, false, rerr
	}
	switch status {
	case "FENCED":
		return true, false, nil
	case "NOSUB":
		return false, true, nil
	}
	m.rewakeIfPendingUnscoped(id)
	return false, false, nil
}
