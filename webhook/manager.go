package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
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
	TailOffsets(paths []string) map[string]string
	// BeginningOffset is the canonical "start of stream" cursor (store.ZeroOffset);
	// a stream linked here has no pending work until its first append.
	BeginningOffset() string
	// AppendWakeEvent appends a JSON wake event to a pull-wake wake stream.
	AppendWakeEvent(wakeStream string, data []byte) error
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
}

// Manager orchestrates the subscription control plane: stream hooks, webhook
// delivery and callbacks, pull-wake claim/ack/release, the lease and retry
// worker ticks, and the recovery sweep that closes the restart gap
// (docs/research/07 §8). It is the imperative shell over the pure core and the
// durable Store.
type Manager struct {
	store             Store
	streams           Streams
	lister            StreamLister
	streamRootURL     string // normalized in NewManager to end in exactly one "/"
	client            *http.Client
	resolver          IPResolver
	signing           SigningKey
	tokenKey          []byte
	log               *slog.Logger
	workerTick        time.Duration
	sweepInterval     time.Duration
	reconcileInterval time.Duration
	sweepBatch        int
	sweepCursor       int // rolling start index when sweepBatch caps a tick
	allowPrivate      bool
	metrics           Metrics

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

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewManager builds a Manager and loads (or installs) the persisted signing and
// token keys, so the kid is stable and tokens validate across restarts.
func NewManager(store Store, streams Streams, opts ManagerOptions) (*Manager, error) {
	now := time.Now()
	signing, err := store.LoadSigningKey(now)
	if err != nil {
		return nil, err
	}
	tokenKey, err := store.LoadTokenKey()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		store:                 store,
		streams:               streams,
		lister:                opts.Lister,
		streamRootURL:         normalizeStreamRootURL(opts.StreamRootURL),
		client:                opts.HTTPClient,
		resolver:              opts.Resolver,
		signing:               signing,
		tokenKey:              tokenKey,
		log:                   opts.Logger,
		workerTick:            opts.WorkerTick,
		sweepInterval:         opts.SweepInterval,
		reconcileInterval:     opts.ReconcileInterval,
		sweepBatch:            opts.SweepBatch,
		allowPrivate:          opts.AllowPrivateWebhookTargets,
		metrics:               opts.Metrics,
		memberLeaseTTL:        opts.MemberLeaseTTL,
		heartbeatInterval:     opts.HeartbeatInterval,
		slotLeaseTTL:          opts.SlotLeaseTTL,
		slotReconcileInterval: opts.SlotReconcileInterval,
		held:                  map[SlotID]OwnerEpoch{},
		reconcileC:            make(chan scope, 1),
		stop:                  make(chan struct{}),
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
	if err := m.initOwnership(opts); err != nil {
		return nil, err
	}
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
	return &SigningView{Alg: "ed25519", Kid: m.signing.Kid, JWKSURL: m.JWKSURL()}
}

// JWKS returns the active key set served at __ds/jwks.json.
func (m *Manager) JWKS() (JWKS, error) {
	keys, err := m.store.SigningKeys()
	if err != nil {
		return JWKS{}, err
	}
	if len(keys) == 0 {
		keys = []SigningKey{m.signing}
	}
	return BuildJWKS(keys), nil
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

// OnStreamAppend wakes every idle subscription with pending work on path
// (PROTOCOL §7). It is the best-effort low-latency path; the recovery sweep is
// the durability backstop if this is lost to a crash (docs/research/09 §2).
func (m *Manager) OnStreamAppend(path string) {
	// Under slot-homing the fan-out is S parallel pipelined SMEMBERS over the
	// stream's occupied slots (~max-node-RTT, not S serial), gated by the
	// occupied-slots bitmap so a sparse-wide stream probes only its occupied slots.
	// FanOut records the scatter-gather cost (duration, slots probed, subscribers
	// found) — the number gate #2 measures (05:490-500, 05:525).
	start := time.Now()
	ids, slotsProbed, err := m.store.StreamSubscribers(path)
	if err != nil {
		// The low-latency wake path failed: this append's subscribers could not be
		// read, so its wakes are lost. Trigger a recovery reconcile rather than wait
		// for the coarse floor (doc-05 correction #2, the append-error event).
		m.log.Warn("webhook: stream subscribers", "path", path, "error", err)
		m.triggerReconcile(scopeAppendError)
		return
	}
	m.metrics.FanOut(time.Since(start), slotsProbed, len(ids))
	for _, id := range ids {
		m.maybeWake(id, path)
	}
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

// issueWake arms a new wake generation and delivers it (webhook POST or pull-wake
// event). For webhook the lease is armed at issue; for pull-wake the lease waits
// for a claim (PROTOCOL §7.3).
func (m *Manager) issueWake(sub Subscription, triggerStream string) {
	wakeID, err := GenerateWakeID(rand.Reader)
	if err != nil {
		m.log.Warn("webhook: generate wake id", "error", err)
		return
	}
	armLease := sub.Config.Type == DispatchWebhook
	res, err := m.armWake(sub.ID, time.Now(), sub.Config.LeaseTTLMs, armLease, wakeID)
	if err != nil {
		m.log.Warn("webhook: arm wake", "sub", sub.ID, "error", err)
		return
	}
	if !res.Armed {
		return // already in flight (coalesced) or gone
	}
	// The arm→emit surgical window (07 honest-gap #2): the fence is minted but the
	// wake is not yet emitted. A no-op in production; a test failpoint can crash/stall
	// here to exercise the stranded-wake recovery the host nemesis cannot pin down.
	failpoint(fpArmedBeforeEmit)
	switch sub.Config.Type {
	case DispatchWebhook:
		go m.deliverWebhook(sub.ID, res.Generation, res.WakeID)
	case DispatchPullWake:
		m.writeWakeEvent(sub, triggerStream, res.Generation, res.WakeID)
	}
}

func (m *Manager) writeWakeEvent(sub Subscription, triggerStream string, generation int64, wakeID string) {
	if triggerStream == "" && len(sub.Links) > 0 {
		triggerStream = sub.Links[0].Path
	}
	data, err := NewWakeEvent(sub.ID, triggerStream, generation, time.Now())
	if err != nil {
		return
	}
	appendStart := time.Now()
	if err := m.streams.AppendWakeEvent(sub.Config.WakeStream, data); err != nil {
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

// deliverWebhook signs and POSTs a wake notification, then handles the response:
// a 2xx {done:true} auto-acks the snapshot and releases; any other 2xx clears
// the failure state and leaves the wake in flight for an async callback; a
// non-2xx or transport error schedules a retry (PROTOCOL §7.1).
func (m *Manager) deliverWebhook(id string, generation int64, wakeID string, owner ...OwnerScope) {
	// Owner-epoch fence for the EXTERNAL POST (issue #14): the retry worker drives
	// this for a slot it owns, so verify ownership via check_owner immediately
	// before the POST — the one schedule write that cannot inline the check, since
	// the side effect crosses the network. The append-path caller (issueWake)
	// passes no scope and proceeds: the (gen,wake_id) fence on the returned ack is
	// the guard and a duplicate POST coalesces (a double-wake is safe).
	if len(owner) > 0 && owner[0].active() {
		chk, cerr := m.store.CheckOwner(owner[0].SlotKey, owner[0].ReplicaID, owner[0].Epoch)
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
	body, err := json.Marshal(notif)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, sub.Config.WebhookURL, bytes.NewReader(body))
	if err != nil {
		m.recordFailure(id, owner...)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Webhook-Signature", SignWebhookPayload(m.signing, body, time.Now()))

	postStart := time.Now()
	resp, err := m.client.Do(req)
	if err != nil {
		m.metrics.WakeDelivery(time.Since(postStart), "error")
		m.recordFailure(id, owner...)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.metrics.WakeDelivery(time.Since(postStart), "failed")
		m.recordFailure(id, owner...)
		return
	}
	m.metrics.WakeDelivery(time.Since(postStart), "ok")

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var parsed struct {
		Done *bool `json:"done"`
	}
	_ = json.Unmarshal(respBody, &parsed)

	_ = m.store.RecordSuccess(id)
	if parsed.Done != nil && *parsed.Done {
		acks := acksFromSnapshot(snapshot)
		// The auto-ack(done) is a schedule/due-mutating write, so it carries the
		// retry worker's owner scope: a deposed owner's done-ack (it released/ZREMed
		// a slot it no longer owns) is FENCED inline, atomically with the write — the
		// same TOCTOU resolution the expire path uses. The append-path caller passes
		// no scope, so its auto-ack stays unfenced (the (gen,wake_id) fence guards it).
		status, err := m.ack(id, generation, wakeID, generation, true, acks, time.Now(), sub.Config.LeaseTTLMs, owner...)
		if err != nil {
			m.log.Warn("webhook: auto-ack done", "sub", id, "error", err)
			return
		}
		if status == "OK" {
			m.rewakeIfPending(id)
		}
	}
}

func (m *Manager) recordFailure(id string, owner ...OwnerScope) {
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
	// schedules nothing (no phantom retry on a slot it lost) — closing the retry
	// path's TOCTOU. The append-path caller passes no scope (unfenced).
	if _, err := m.store.ScheduleRetry(id, time.Now(), next, owner...); err != nil {
		m.log.Warn("webhook: schedule retry", "sub", id, "error", err)
	}
}

func (m *Manager) tokenTTL(sub Subscription) time.Duration {
	// A grace beyond the lease so an in-flight callback's token outlives a
	// just-extended lease.
	return time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + time.Hour
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

// rewakeIfPending re-issues a wake when work remains after a release or a done
// ack (PROTOCOL §7.2/§7.3). Returns whether a re-wake was issued (the next_wake
// flag).
func (m *Manager) rewakeIfPending(id string) bool {
	sub, ok, err := m.store.Get(id)
	if err != nil || !ok {
		return false
	}
	if sub.Phase != PhaseIdle || !HasPendingWork(sub.Links, m.tailOf) {
		return false
	}
	m.issueWake(sub, "")
	return true
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

// armWake arms a wake (arm_wake): the ARMED branch ZADDs the due mark.
func (m *Manager) armWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error) {
	res, err := m.store.ArmWake(id, now, leaseTTLMs, armLease, wakeID)
	if err == nil && res.Armed {
		m.metrics.DueSetMutation("arm")
	}
	return res, err
}

// ack fences and acks (ack): the done branch ZREMs the due mark; a heartbeat
// (done=false) does not, so only a done-ack records the mutation. An optional
// OwnerScope makes ack inline the owner-epoch fence (issue #14) for the
// owner-driven retry-worker auto-ack; the store records the inline fence on FENCED.
func (m *Manager) ack(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64, owner ...OwnerScope) (string, error) {
	status, err := m.store.Ack(id, reqGeneration, reqWakeID, tokenGeneration, done, acks, now, leaseTTLMs, owner...)
	if err == nil && done && status == "OK" {
		m.metrics.DueSetMutation("ack")
	}
	return status, err
}

// release voluntarily releases the lease (release): the idle-reset branch ZREMs
// the due mark (GAP3).
func (m *Manager) release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64) (string, error) {
	status, err := m.store.Release(id, reqGeneration, reqWakeID, tokenGeneration)
	if err == nil && status == "OK" {
		m.metrics.DueSetMutation("release")
	}
	return status, err
}

// expireLease clears an expired lease (expire_lease): the EXPIRED branch re-owes
// (ZADDs) the due mark so the dueWorker re-fires it. An optional OwnerScope makes
// the script inline the owner-epoch fence (issue #14); a FENCED result is a
// deposed owner's expiry suppressed atomically. The inline fence is recorded by
// the store (the single place the Lua reply is observed), so every owner-scoped
// script records OwnerFenced("inline") uniformly — not just the ones with a
// manager wrapper.
func (m *Manager) expireLease(id string, now time.Time, owner ...OwnerScope) (string, error) {
	status, err := m.store.ExpireLease(id, now, owner...)
	if err == nil && status == "EXPIRED" {
		m.metrics.DueSetMutation("expire")
	}
	return status, err
}

// ---- background loops ----

// Start launches the lease worker, retry worker, due-set worker, the recovery
// loop (the coarse floor + event-triggered reconciles), and the slow reconcile
// loop. It first runs the boot reconcile synchronously so anything owed is
// re-fired before the loops — and before serving — closing the restart gap
// (doc-05 correction #2, the boot event).
func (m *Manager) Start() {
	m.reconcile(scopeBoot)
	// Join membership and claim our owned slots BEFORE the fast workers tick, so a
	// fresh replica does not idle a whole slotReconcileInterval before owning work
	// (and so the boot owner is established before serving). Both are best-effort:
	// a failure here is retried by the loops below.
	if err := m.store.Heartbeat(m.replicaID.String(), time.Now(), m.memberLeaseTTL); err != nil {
		m.log.Warn("webhook: initial heartbeat", "replica", m.replicaID, "error", err)
	}
	m.slotReconcileOnce()
	m.wg.Add(7)
	go m.leaseWorker()
	go m.retryWorker()
	go m.dueWorker()
	go m.recoveryLoop()
	go m.reconcileLoop()
	go m.heartbeatLoop()
	go m.slotReconcileLoop()
}

// Stop signals the background loops and waits for them to drain. It does NOT
// explicitly release owned slots: lease expiry is the authoritative handoff
// (05:248), so a surviving replica reclaims this one's slots once its slot lease
// (and membership lease) lapse. The full sweep covers the interim coverage gap.
func (m *Manager) Stop() {
	close(m.stop)
	m.wg.Wait()
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
		case <-m.stop:
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
					_, _ = m.expireLease(id, now, scope) // EXPIRED re-owes the due-set; dueWorker re-fires
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
		case <-m.stop:
			return
		case <-ticker.C:
			// Work-sharded: a replica drains the due-set only for its owned slots
			// (issue #14, real S slots — #15). The directly-invokable drainDue
			// (RunDueWorker, tests) stays ungated, sweeping every slot.
			for _, h := range m.ownedSlots() {
				m.drainDue(h.Index())
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
	start := time.Now()
	ids, err := m.store.ClaimDue(h, start, dueClaimLimit, m.workerTick*2)
	if err != nil || len(ids) == 0 {
		return 0
	}
	fired := 0
	for _, id := range ids {
		if m.fireDue(id) {
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
	sub, ok, err := m.store.Get(id)
	if err != nil {
		return false
	}
	switch DecideDue(ok, sub.Phase, ok && HasPendingWork(sub.Links, m.tailOf)) {
	case DueFire:
		m.issueWake(sub, "")
		return true
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
		case <-m.stop:
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
					m.deliverWebhook(id, sub.Generation, sub.WakeID, scope)
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
	scopeBoot        scope = iota // process boot: re-fire anything owed before serving
	scopeReconnect                // a Redis reconnect: the connection that lost in-flight ops healed
	scopeAppendError              // an OnStreamAppend / wake-event append failed: the low-latency wake path errored
	scopeFloor                    // the coarse periodic floor: the one eventless case (an owed mark on an unowned, quiet slot)
	scopeEpochBump                // #16: a DR promotion / owner_epoch bump drives the eager reconcile (Manager.Promote)
	scopeNewOwnerCAS              // #14: a new-owner claim_shard CAS reconciles its freshly-claimed slot
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
	case scopeBoot, scopeReconnect, scopeAppendError, scopeFloor, scopeEpochBump, scopeNewOwnerCAS:
		m.sweepOnce()
	}
}

// triggerReconcile routes a recovery event to the single reconcile seam without
// blocking the caller (OnStreamAppend, a reconnect callback). The send is
// non-blocking onto the depth-1 reconcileC, so concurrent events coalesce into at
// most one queued reconcile while one runs: duplicate reconciles are claim-fence-
// safe, and a storm of append errors cannot pile up a reconcile per error.
func (m *Manager) triggerReconcile(s scope) {
	select {
	case m.reconcileC <- s:
	default:
		// a reconcile is already queued; this event coalesces into it.
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
		case <-m.stop:
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
		case <-m.stop:
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
		case <-m.stop:
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

func (m *Manager) sweepOnce() {
	start := time.Now()
	ids, err := m.store.List()
	if err != nil || len(ids) == 0 {
		return
	}
	ids = m.sweepWindow(ids)
	now := time.Now()
	// Batch the per-tick reads. The sweep is O(subscriptions x links) and the
	// naive form was one round trip per subscription (Get) plus one per link
	// (tail) — the poll backstop's scaling ceiling. GetMany pipelines the
	// subscription reads; TailOffsets pipelines every linked tail into one batch.
	subs, err := m.store.GetMany(ids)
	if err != nil {
		return
	}
	// Collect tails across all subs (not just idle ones) so a subscription that
	// lease expiry flips to idle below still has its tails in the batch.
	paths := distinctLinkPaths(subs)
	tails := m.streams.TailOffsets(paths)
	// The failover-aware eager reconcile runs first: re-derive any stranded lease
	// tail (a live/waking sub a failover dropped from the lease ZSET) from the
	// durable hash, so the fast lease worker — not the coarse floor — drives its
	// expiry. The pull-wake re-emit and idle-rewake passes below are unchanged.
	leased := m.leasedSet()
	m.reconcileLeases(subs, tails, leased, now)
	wakes := 0
	for _, sub := range subs {
		// Recover a pull-wake stranded by a crash between arming the wake and
		// appending its wake event: the event was never durably emitted (its
		// sent flag is still 0), so nothing in the schedule will deliver it and a
		// later append only coalesces against the phantom waking state. Re-emit
		// it; duplicate wake events are claim-fence-safe.
		if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs == 0 {
			m.writeWakeEvent(sub, "", sub.Generation, sub.WakeID)
			wakes++
			continue
		}
		// Recover a pull-wake stranded in waking AFTER a durable emit (T4, #24): the
		// wake event was written (its sent flag is set) but no consumer ever claimed
		// it — the origin or consumer crashed after emit. Pull-wake arms no lease, so
		// LeaseExpired never fires and the sub never flips back to idle; the due-set
		// coalesces it (DecideDue is DueSkip for a non-idle phase) and reconcileLeases
		// skips it (a pull-wake's lease_until_ns is 0, never LeaseStranded). Nothing
		// else delivers, so it strands forever. Once the emitted wake is stale (older
		// than a few floor ticks: a live consumer would have claimed it long before),
		// re-emit the SAME (generation, wakeID) so a restarted consumer can reclaim it;
		// the (gen, wake) fence makes the duplicate event claim-safe. Idempotent: it
		// re-fires at most once per floor tick while the sub remains stranded.
		if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs != 0 &&
			now.UnixNano()-sub.WakeEventSentNs > (3*m.sweepInterval).Nanoseconds() {
			m.writeWakeEvent(sub, "", sub.Generation, sub.WakeID)
			wakes++
			continue
		}
		if sub.Phase != PhaseIdle && LeaseExpired(sub.LeaseUntilNs, now) {
			if status, err := m.expireLease(sub.ID, now); err == nil && status == "EXPIRED" {
				sub.Phase = PhaseIdle
			}
		}
		if sub.Phase == PhaseIdle && HasPendingWorkFrom(sub.Links, tails) {
			m.issueWake(sub, "")
			// A backstop wake the UNGUARDED full sweep issued — the append-time/owner
			// wake was lost (e.g. a rebalance coverage gap where the sub's slot was
			// unowned). This is the coverage-gap sample gate #4 reads: when a non-owner
			// replica's sweep has to re-fire a wake the owned fast path missed. At S=1
			// the slot's subs are the whole keyspace; #15 refines this to deliver−append
			// for the subs whose slot was specifically unowned at append.
			m.metrics.CoverageGap(time.Since(start))
			wakes++
		}
	}
	m.metrics.SweepTick(time.Since(start), len(subs), len(paths), wakes)
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
func (m *Manager) RunSweep() { m.sweepOnce() }

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
		case <-m.stop:
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
	status, aerr := m.ack(id, req.Generation, req.WakeID, tokenGeneration, done, req.Acks, time.Now(), sub.Config.LeaseTTLMs)
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
		nextWake = m.rewakeIfPending(id)
	}
	return false, false, nextWake, nil
}

// applyRelease fences and releases the lease, re-waking if pending (PROTOCOL §7.2).
func (m *Manager) applyRelease(id string, req ReleaseRequest, tokenGeneration int64) (fenced, gone bool, err error) {
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
	m.rewakeIfPending(id)
	return false, false, nil
}
