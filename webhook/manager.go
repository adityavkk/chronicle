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
	webhookDeliveryTimeout   = 30 * time.Second
	defaultWorkerTick        = 250 * time.Millisecond
	defaultSweepInterval     = 2 * time.Second
	defaultReconcileInterval = 30 * time.Second
	dueClaimLimit            = 256
)

// ManagerOptions configures a Manager. Zero values fall back to sensible
// defaults; StreamRootURL is required to build absolute callback and JWKS URLs.
type ManagerOptions struct {
	// StreamRootURL is the public URL the protocol is served under, including
	// scheme and host, ending in "/" (e.g. "http://localhost:4437/v1/stream/").
	StreamRootURL string
	Lister        StreamLister
	Resolver      IPResolver
	HTTPClient    *http.Client
	Logger        *slog.Logger
	WorkerTick    time.Duration
	SweepInterval time.Duration
	// ReconcileInterval is how often the slow reconciliation loop runs (pattern
	// link recovery + fan-out index repair). Default 30s — it is O(streams), so
	// it is deliberately decoupled from the fast 2s sweep.
	ReconcileInterval time.Duration
	// AllowPrivateWebhookTargets relaxes SSRF validation to accept any http(s)
	// webhook URL (e.g. cluster-internal receivers on RFC1918 addresses). Off by
	// default; the operator opts in for trusted networks.
	AllowPrivateWebhookTargets bool
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
	streamRootURL     string
	client            *http.Client
	resolver          IPResolver
	signing           SigningKey
	tokenKey          []byte
	log               *slog.Logger
	workerTick        time.Duration
	sweepInterval     time.Duration
	reconcileInterval time.Duration
	allowPrivate      bool

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
		store:             store,
		streams:           streams,
		lister:            opts.Lister,
		streamRootURL:     opts.StreamRootURL,
		client:            opts.HTTPClient,
		resolver:          opts.Resolver,
		signing:           signing,
		tokenKey:          tokenKey,
		log:               opts.Logger,
		workerTick:        opts.WorkerTick,
		sweepInterval:     opts.SweepInterval,
		reconcileInterval: opts.ReconcileInterval,
		allowPrivate:      opts.AllowPrivateWebhookTargets,
		stop:              make(chan struct{}),
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
	return m, nil
}

// randReader is the package's randomness source for wake ids and tokens.
var randReader = rand.Reader

func defaultResolver(host string) ([]net.IP, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

func (m *Manager) tailOf(path string) (string, bool) { return m.streams.TailOffset(path) }

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
	ids, err := m.store.StreamSubscribers(path)
	if err != nil {
		m.log.Warn("webhook: stream subscribers", "path", path, "error", err)
		return
	}
	for _, id := range ids {
		m.maybeWake(id, path)
	}
}

// OnStreamDeleted unlinks a deleted stream from all its subscribers.
func (m *Manager) OnStreamDeleted(path string) {
	ids, err := m.store.StreamSubscribers(path)
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
	res, err := m.store.ArmWake(sub.ID, time.Now(), sub.Config.LeaseTTLMs, armLease, wakeID)
	if err != nil {
		m.log.Warn("webhook: arm wake", "sub", sub.ID, "error", err)
		return
	}
	if !res.Armed {
		return // already in flight (coalesced) or gone
	}
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
	if err := m.streams.AppendWakeEvent(sub.Config.WakeStream, data); err != nil {
		// Leave wake_event_sent_ns at 0 so the recovery sweep re-emits.
		m.log.Warn("webhook: write wake event", "sub", sub.ID, "wake_stream", sub.Config.WakeStream, "error", err)
		return
	}
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
func (m *Manager) deliverWebhook(id string, generation int64, wakeID string) {
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
	req, err := http.NewRequest(http.MethodPost, sub.Config.WebhookURL, bytes.NewReader(body))
	if err != nil {
		m.recordFailure(id)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Webhook-Signature", SignWebhookPayload(m.signing, body, time.Now()))

	resp, err := m.client.Do(req)
	if err != nil {
		m.recordFailure(id)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.recordFailure(id)
		return
	}

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var parsed struct {
		Done *bool `json:"done"`
	}
	_ = json.Unmarshal(respBody, &parsed)

	_ = m.store.RecordSuccess(id)
	if parsed.Done != nil && *parsed.Done {
		acks := acksFromSnapshot(snapshot)
		status, err := m.store.Ack(id, generation, wakeID, generation, true, acks, time.Now(), sub.Config.LeaseTTLMs)
		if err != nil {
			m.log.Warn("webhook: auto-ack done", "sub", id, "error", err)
			return
		}
		if status == "OK" {
			m.rewakeIfPending(id)
		}
	}
}

func (m *Manager) recordFailure(id string) {
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
	if _, err := m.store.ScheduleRetry(id, time.Now(), next); err != nil {
		m.log.Warn("webhook: schedule retry", "sub", id, "error", err)
	}
}

func (m *Manager) tokenTTL(sub Subscription) time.Duration {
	// A grace beyond the lease so an in-flight callback's token outlives a
	// just-extended lease.
	return time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + time.Hour
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

// ---- background loops ----

// Start launches the lease worker, retry worker, and recovery sweep.
func (m *Manager) Start() {
	m.wg.Add(4)
	go m.leaseWorker()
	go m.retryWorker()
	go m.recoverySweeper()
	go m.reconcileLoop()
}

// Stop signals the background loops and waits for them to drain.
func (m *Manager) Stop() {
	close(m.stop)
	m.wg.Wait()
}

// leaseWorker expires due leases and re-wakes subscriptions that still have
// pending work (PROTOCOL §7.3). Due members are re-scored forward, so a crash
// mid-handling leaves the lease to fall due again.
func (m *Manager) leaseWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.workerTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			now := time.Now()
			ids, err := m.store.DueLeases(now, dueClaimLimit, m.workerTick*2)
			if err != nil {
				continue
			}
			for _, id := range ids {
				status, err := m.store.ExpireLease(id, now)
				if err == nil && status == "EXPIRED" {
					m.rewakeIfPending(id)
				}
			}
		}
	}
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
			now := time.Now()
			ids, err := m.store.DueRetries(now, dueClaimLimit, m.workerTick*2)
			if err != nil {
				continue
			}
			for _, id := range ids {
				sub, ok, err := m.store.Get(id)
				if err != nil || !ok || sub.Phase != PhaseWaking {
					continue
				}
				m.deliverWebhook(id, sub.Generation, sub.WakeID)
			}
		}
	}
}

// recoverySweeper closes the restart gap (docs/research/07 §8): it re-evaluates
// every subscription against its durable cursor and re-fires anything owed, even
// an idle subscription whose append-time wake was lost to a crash. It also
// expires stale leases as a backstop to the lease worker.
func (m *Manager) recoverySweeper() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.sweepOnce()
		}
	}
}

func (m *Manager) sweepOnce() {
	ids, err := m.store.List()
	if err != nil {
		return
	}
	now := time.Now()
	for _, id := range ids {
		sub, ok, err := m.store.Get(id)
		if err != nil || !ok {
			continue
		}
		// Recover a pull-wake stranded by a crash between arming the wake and
		// appending its wake event: the event was never durably emitted (its
		// sent flag is still 0), so nothing in the schedule will deliver it and a
		// later append only coalesces against the phantom waking state. Re-emit
		// it; duplicate wake events are claim-fence-safe.
		if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs == 0 {
			m.writeWakeEvent(sub, "", sub.Generation, sub.WakeID)
			continue
		}
		if sub.Phase != PhaseIdle && LeaseExpired(sub.LeaseUntilNs, now) {
			if status, err := m.store.ExpireLease(id, now); err == nil && status == "EXPIRED" {
				sub.Phase = PhaseIdle
			}
		}
		if sub.Phase == PhaseIdle && HasPendingWork(sub.Links, m.tailOf) {
			m.issueWake(sub, "")
		}
	}
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
func (m *Manager) reconcilePatternLinks(now time.Time) {
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
	m.reconcilePatternLinks(time.Now())
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
	status, aerr := m.store.Ack(id, req.Generation, req.WakeID, tokenGeneration, done, req.Acks, time.Now(), sub.Config.LeaseTTLMs)
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
	status, rerr := m.store.Release(id, req.Generation, req.WakeID, tokenGeneration)
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
