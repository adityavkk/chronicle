package chronicle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
	"unsafe"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const (
	defaultSSEHubReplayBytes = 1 << 20
	defaultSSEHubBatchBytes  = 256 << 10
	defaultSSEHubPoll        = time.Second
	defaultSSEWriteTimeout   = 10 * time.Second
	sseHubRetryMin           = 100 * time.Millisecond
	sseHubRetryMax           = time.Second
)

var (
	errSSEHubLagged    = errors.New("SSE client fell behind the hub replay window")
	errSSEHubBehind    = errors.New("SSE hub has not reached the client catch-up boundary")
	errSSEHubRecreated = errors.New("stream was deleted and recreated")
)

// SSEMetrics is the data-plane metrics seam for the shared SSE hub. The
// Prometheus implementation lives in metrics; nil records nothing.
type SSEMetrics interface {
	store.NotificationMetrics
	SSEHubActive(delta int)
	SSEClientActive(delta int)
	SSEHubRead(messages int)
	SSEHubRingBytes(rawDelta, wireDelta, indexDelta int)
	SSEHubRefresh(cause string, pages, bytes int, duration time.Duration)
	SSEPage(phase string, bytes int)
	SSEWatcherLookup(steps int, miss bool)
	SSEReason(reason string)
	SSEClientLagged()
	SSEClientWriteTimeout()
	SSESubscriptionActive(delta int)
	SSESubscriptionEvent(event string)
}

type nopSSEMetrics struct{}

func (nopSSEMetrics) SSEHubActive(int)                              {}
func (nopSSEMetrics) SSEClientActive(int)                           {}
func (nopSSEMetrics) SSEHubRead(int)                                {}
func (nopSSEMetrics) SSEHubRingBytes(int, int, int)                 {}
func (nopSSEMetrics) SSEHubRefresh(string, int, int, time.Duration) {}
func (nopSSEMetrics) SSEPage(string, int)                           {}
func (nopSSEMetrics) SSEWatcherLookup(int, bool)                    {}
func (nopSSEMetrics) SSEReason(string)                              {}
func (nopSSEMetrics) SSEClientLagged()                              {}
func (nopSSEMetrics) SSEClientWriteTimeout()                        {}
func (nopSSEMetrics) SSESubscriptionActive(int)                     {}
func (nopSSEMetrics) SSESubscriptionEvent(string)                   {}
func (nopSSEMetrics) NotificationPhysicalConnection(string, int)    {}
func (nopSSEMetrics) NotificationEvent(string)                      {}

type sseHubRegistry struct {
	mu   sync.Mutex
	hubs map[string]*sseHubEntry
}

type sseHubEntry struct {
	hub  *sseHub
	refs int
}

type sseHubLease struct {
	once     sync.Once
	registry *sseHubRegistry
	path     string
	hub      *sseHub
	entry    *sseHubEntry
	watcher  *sseHubWatcher
	metrics  SSEMetrics
}

func (l *sseHubLease) waitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.hub.ready:
		l.hub.mu.Lock()
		err := l.hub.readyErr
		l.hub.mu.Unlock()
		return err
	}
}

func (l *sseHubLease) waitRegistered(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.hub.registered:
		l.hub.mu.Lock()
		err := l.hub.registeredErr
		l.hub.mu.Unlock()
		return err
	}
}

func (l *sseHubLease) progressedBeyond(offset store.Offset, closed bool) bool {
	l.hub.mu.Lock()
	defer l.hub.mu.Unlock()
	return offset.LessThan(l.hub.current) || (!closed && l.hub.closed)
}

func (l *sseHubLease) validateSnapshot(snapshot store.ReadSnapshot) error {
	if l == nil {
		return errors.New("SSE hub lease is nil")
	}
	return l.hub.validateSnapshot(snapshot)
}

func (l *sseHubLease) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.registry.mu.Lock()
		defer l.registry.mu.Unlock()

		entry := l.entry
		entry.hub.removeWatcher(l.watcher)
		entry.refs--
		l.metrics.SSEClientActive(-1)
		if entry.refs > 0 {
			return
		}

		if l.registry.hubs[l.path] == entry {
			delete(l.registry.hubs, l.path)
		}
		entry.hub.cancel()
		l.metrics.SSEHubActive(-1)
		go func(hub *sseHub) {
			<-hub.done
			hub.releaseReplay()
		}(entry.hub)
	})
}

func (h *Handler) acquireSSEHubRegistration(path string) *sseHubLease {
	metrics := h.sseMetrics()
	h.sseHubs.mu.Lock()
	defer h.sseHubs.mu.Unlock()

	if h.sseHubs.hubs == nil {
		h.sseHubs.hubs = make(map[string]*sseHubEntry)
	}
	entry := h.sseHubs.hubs[path]
	if entry == nil || entry.hub.isTerminal() {
		entry = h.newSSEHubEntry(path, metrics)
		h.sseHubs.hubs[path] = entry
	}

	return h.addSSEHubLeaseLocked(path, entry, metrics)
}

// acquireSSEHub is retained for focused hub tests and internal callers that
// already own a snapshot. Production SSE requests use
// acquireSSEHubRegistration so registration is acknowledged before the first
// page captures that snapshot.
func (h *Handler) acquireSSEHub(path string, snapshot store.ReadSnapshot, useBase64 bool) *sseHubLease {
	lease := h.acquireSSEHubRegistration(path)
	if err := lease.hub.initialize(snapshot, useBase64); err == nil {
		return lease
	}
	return h.replaceSSEHubLease(lease, snapshot, useBase64)
}

func (h *Handler) replaceSSEHubLease(
	old *sseHubLease,
	snapshot store.ReadSnapshot,
	useBase64 bool,
) *sseHubLease {
	path := old.path
	oldEntry := old.entry
	old.close()
	metrics := h.sseMetrics()
	h.sseHubs.mu.Lock()
	defer h.sseHubs.mu.Unlock()
	entry := h.sseHubs.hubs[path]
	if entry == nil || entry == oldEntry || entry.hub.isTerminal() {
		entry = h.newSSEHubEntry(path, metrics)
		h.sseHubs.hubs[path] = entry
	}
	lease := h.addSSEHubLeaseLocked(path, entry, metrics)
	if err := lease.hub.initialize(snapshot, useBase64); err != nil {
		lease.hub.fail(err)
	}
	return lease
}

func (h *Handler) newSSEHubEntry(path string, metrics SSEMetrics) *sseHubEntry {
	ctx, cancel := context.WithCancel(context.Background())
	replayLimit := positiveOr(h.SSEHubReplayBytes, defaultSSEHubReplayBytes)
	batchLimit := min(positiveOr(h.SSEHubBatchBytes, defaultSSEHubBatchBytes), replayLimit)
	hub := &sseHub{
		handler:       h,
		path:          path,
		replayLimit:   replayLimit,
		batchLimit:    batchLimit,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		registered:    make(chan struct{}),
		initialized:   make(chan struct{}),
		ready:         make(chan struct{}),
		firstSequence: 1,
		nextSequence:  1,
		watchers:      make(map[*sseHubWatcher]struct{}),
		metrics:       metrics,
	}
	metrics.SSEHubActive(1)
	go hub.run()
	return &sseHubEntry{hub: hub}
}

func (h *Handler) addSSEHubLeaseLocked(
	path string,
	entry *sseHubEntry,
	metrics SSEMetrics,
) *sseHubLease {
	watcher := entry.hub.addWatcher()
	entry.refs++
	metrics.SSEClientActive(1)
	return &sseHubLease{
		registry: &h.sseHubs,
		path:     path,
		hub:      entry.hub,
		entry:    entry,
		watcher:  watcher,
		metrics:  metrics,
	}
}

func (h *Handler) sseMetrics() SSEMetrics {
	if h.SSEMetrics != nil {
		return h.SSEMetrics
	}
	return nopSSEMetrics{}
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func capSSEHubPollForTTL(poll time.Duration, ttlSeconds *int64) time.Duration {
	if ttlSeconds == nil || *ttlSeconds <= 0 {
		return poll
	}

	// Build ttl/2 without first converting the protocol's full int64 seconds
	// to nanoseconds. The full protocol range is larger than time.Duration, and
	// a wrapped negative duration would turn the defensive poll into a tight
	// Redis read loop.
	const maxDuration = time.Duration(1<<63 - 1)
	halfSeconds := *ttlSeconds / 2
	if halfSeconds > int64(maxDuration/time.Second) {
		return poll
	}
	ttlRefresh := time.Duration(halfSeconds) * time.Second
	if *ttlSeconds%2 != 0 {
		const halfSecond = 500 * time.Millisecond
		if ttlRefresh > maxDuration-halfSecond {
			return poll
		}
		ttlRefresh += halfSecond
	}
	if ttlRefresh < poll {
		return ttlRefresh
	}
	return poll
}

type sseHub struct {
	handler        *Handler
	path           string
	contentType    string
	useBase64      bool
	replayLimit    int
	batchLimit     int
	pollInterval   time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	registered     chan struct{}
	registeredOnce sync.Once
	initialized    chan struct{}
	ready          chan struct{}
	readyOnce      sync.Once
	metrics        SSEMetrics
	refreshMu      sync.Mutex
	confirmationMu sync.Mutex
	confirmation   *sseHubConfirmation

	mu             sync.Mutex
	registeredErr  error
	isInitialized  bool
	readyErr       error
	current        store.Offset
	closed         bool
	incarnation    string
	createdAt      time.Time
	terminalErr    error
	events         []*sseHubEvent
	firstSequence  uint64
	nextSequence   uint64
	ringRawBytes   int
	ringWireBytes  int
	ringIndexBytes int
	replayBytes    int
	watchers       map[*sseHubWatcher]struct{}
}

type sseHubEvent struct {
	sequence   uint64
	from       store.Offset
	to         store.Offset
	data       []byte
	boundaries []sseHubBoundary
	upToDate   bool
	closed     bool
	rawBytes   int
	wireBytes  int
	indexBytes int
	memorySize int
}

type sseHubBoundary struct {
	offset  store.Offset
	wireEnd int
}

type sseHubConfirmation struct {
	done     chan struct{}
	snapshot store.ReadSnapshot
	err      error
}

const sseHubBoundaryBytes = int(unsafe.Sizeof(sseHubBoundary{}))

type sseHubWatcher struct {
	hub            *sseHub
	wake           chan struct{}
	attached       bool
	sequence       uint64
	offset         store.Offset
	firstWireStart int
}

func (h *sseHub) addWatcher() *sseHubWatcher {
	watcher := &sseHubWatcher{hub: h, wake: make(chan struct{}, 1)}
	h.mu.Lock()
	h.watchers[watcher] = struct{}{}
	h.mu.Unlock()
	return watcher
}

func (h *sseHub) removeWatcher(watcher *sseHubWatcher) {
	h.mu.Lock()
	delete(h.watchers, watcher)
	h.mu.Unlock()
}

func (h *sseHub) run() {
	defer close(h.done)
	defer h.markRegistered(h.ctx.Err())
	defer h.markReady(h.ctx.Err())

	var subscriber store.NotificationSubscriber
	var ok bool
	if provider, hasProvider := h.handler.Store.(store.NotificationSubscriberProvider); hasProvider {
		subscriber, ok = provider.NotificationSubscriber()
	} else {
		subscriber, ok = h.handler.Store.(store.NotificationSubscriber)
	}
	if setter, hasSetter := subscriber.(store.NotificationMetricsSetter); hasSetter {
		setter.SetNotificationMetrics(h.metrics)
	}
	if ok {
		h.runPersistent(subscriber)
		return
	}
	h.runWaitLoop()
}

func (h *sseHub) runPersistent(subscriber store.NotificationSubscriber) {
	retry := sseHubRetryMin
	firstConfirmedGeneration := true
	for h.ctx.Err() == nil {
		sub, err := subscriber.SubscribeNotifications(h.ctx, h.path)
		if err != nil {
			h.metrics.SSESubscriptionEvent("subscribe_error")
			h.logRetry("subscribe", err)
			if !sleepContext(h.ctx, retry) {
				return
			}
			retry = min(retry*2, sseHubRetryMax)
			continue
		}
		h.metrics.SSESubscriptionEvent("opened")
		h.metrics.SSESubscriptionActive(1)
		h.markRegistered(nil)
		var closeOnce sync.Once
		closeSub := func() {
			closeOnce.Do(func() {
				_ = sub.Close()
				h.metrics.SSESubscriptionActive(-1)
				h.metrics.SSESubscriptionEvent("closed")
			})
		}

		if !h.waitInitialized() {
			closeSub()
			return
		}
		if firstConfirmedGeneration {
			// Registration was acknowledged before the handler captured the
			// authoritative first page. Redis orders every append either into
			// that page's tail or into this subscription, so a duplicate initial
			// readiness refresh is neither necessary nor desirable.
			firstConfirmedGeneration = false
		} else {
			// A replacement subscription may have missed hints while the old
			// generation was unavailable. Refresh durable state before waiting.
			if err := h.refreshNoTouch(); err != nil {
				closeSub()
				if h.handleReadError(err) {
					h.markReady(err)
					return
				}
				h.metrics.SSESubscriptionEvent("read_error")
				if !sleepContext(h.ctx, retry) {
					return
				}
				retry = min(retry*2, sseHubRetryMax)
				continue
			}
		}
		retry = sseHubRetryMin
		h.markReady(nil)
		if h.isClosed() {
			closeSub()
			return
		}

		reconnect := false
		for h.ctx.Err() == nil && !h.isClosed() {
			waitCtx, cancel := context.WithTimeout(h.ctx, h.pollInterval)
			event, err := sub.Wait(waitCtx)
			cancel()

			if h.ctx.Err() != nil {
				closeSub()
				return
			}
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				h.metrics.SSESubscriptionEvent("reconnect")
				h.logRetry("wait", err)
				reconnect = true
				break
			}
			if event == store.NotificationReconnect {
				h.metrics.SSESubscriptionEvent("reconnect")
				h.metrics.SSEReason("notification_reconnect")
			}

			var refreshErr error
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				refreshErr = h.poll()
			case event == store.NotificationReconnect:
				// A reconnect begins a notification generation that may have
				// missed hints. Confirm durable state without extending the
				// stream's access lifetime.
				refreshErr = h.refreshPage("reconnect", true)
			default:
				refreshErr = h.refresh()
			}
			if refreshErr != nil {
				err = refreshErr
				if h.handleReadError(err) {
					closeSub()
					return
				}
				h.metrics.SSESubscriptionEvent("read_error")
				h.logRetry("read", err)
				reconnect = true
				break
			}
		}
		closeSub()
		if !reconnect || h.isClosed() {
			return
		}
		if !sleepContext(h.ctx, retry) {
			return
		}
		retry = min(retry*2, sseHubRetryMax)
	}
}

func (h *sseHub) runWaitLoop() {
	h.markRegistered(nil)
	if !h.waitInitialized() {
		return
	}
	retry := sseHubRetryMin
	initial := true
	for h.ctx.Err() == nil && !h.isClosed() {
		var err error
		if initial {
			err = h.refreshNoTouch()
		} else {
			err = h.refresh()
		}
		if err != nil {
			if h.ctx.Err() != nil {
				return
			}
			if h.handleReadError(err) {
				h.markReady(err)
				return
			}
			h.logRetry("read", err)
			if !sleepContext(h.ctx, retry) {
				return
			}
			retry = min(retry*2, sseHubRetryMax)
			continue
		}
		initial = false
		retry = sseHubRetryMin
		h.markReady(nil)
		if h.isClosed() {
			return
		}
		if !sleepContext(h.ctx, h.pollInterval) {
			return
		}
	}
}

func (h *sseHub) refresh() error {
	return h.refreshPage("hint", false)
}

func (h *sseHub) refreshNoTouch() error {
	return h.refreshPage("initial", true)
}

func (h *sseHub) refreshCause(cause string) error {
	return h.refreshPage(cause, false)
}

func (h *sseHub) refreshPage(cause string, noTouch bool) error {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	return h.refreshPageLocked(cause, noTouch)
}

func (h *sseHub) refreshTo(offset store.Offset) error {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	if !h.currentOffset().LessThan(offset) {
		return nil
	}
	return h.refreshPageLocked("attach", false)
}

func (h *sseHub) refreshPageLocked(cause string, noTouch bool) error {
	started := time.Now()
	pages := 0
	bytes := 0
	defer func() {
		h.metrics.SSEHubRefresh(cause, pages, bytes, time.Since(started))
	}()
	ctx := h.ctx
	if ctx == nil {
		// refresh is also exercised directly by focused tests and diagnostic
		// callers. Production hubs always install a cancellable context.
		ctx = context.Background()
	}
	reader, ok := h.handler.Store.(store.PageReader)
	if !ok {
		return errors.New("SSE hub requires a store.PageReader backend")
	}
	var snapshot *store.ReadSnapshot
	defer func() {
		if snapshot != nil {
			releaseReadSnapshot(reader, h.path, *snapshot)
		}
	}()
	next := h.currentOffset()
	opts := store.ReadPageOptions{
		TargetBytes: min(h.handler.readPageBytes(), h.batchLimit),
		MaxFrames:   store.DefaultReadPageFrames,
		NoTouch:     noTouch,
	}
	emptyPages := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		opts.Snapshot = snapshot
		page, err := reader.ReadPage(ctx, h.path, next, opts)
		if err != nil {
			return err
		}
		h.handler.observeReadPage(page)
		h.metrics.SSEHubRead(len(page.Messages))
		pages++
		bytes += page.Stats.ReturnedBytes

		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
			if err := h.validateSnapshot(captured); err != nil {
				return err
			}
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			emptyPages++
			if emptyPages >= 8 {
				return store.ErrReadDataMissing
			}
			continue
		}
		emptyPages = 0
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h.publish(
			next,
			page.Messages,
			page.UpToDate,
			snapshot.Closed,
			snapshot.Tail,
		); err != nil {
			return err
		}
		next = page.NextOffset
		if page.UpToDate {
			return nil
		}
	}
}

func (h *sseHub) confirmSnapshot(
	ctx context.Context,
	reader store.PageReader,
	path string,
) (store.ReadSnapshot, error) {
	h.confirmationMu.Lock()
	confirmation := h.confirmation
	if confirmation == nil {
		confirmation = &sseHubConfirmation{done: make(chan struct{})}
		h.confirmation = confirmation
		h.confirmationMu.Unlock()

		readCtx := h.ctx
		if readCtx == nil {
			readCtx = context.Background()
		}
		page, err := reader.ReadPage(readCtx, path, store.NowOffset, store.ReadPageOptions{
			TargetBytes: 1,
			MaxFrames:   1,
			NoTouch:     true,
		})
		if err == nil {
			h.handler.observeReadPage(page)
			h.metrics.SSEPage("confirm", page.Stats.ReturnedBytes)
			releaseReadSnapshot(reader, path, page.Snapshot)
			confirmation.snapshot = page.Snapshot
		}
		confirmation.err = err

		h.confirmationMu.Lock()
		if h.confirmation == confirmation {
			h.confirmation = nil
		}
		close(confirmation.done)
		h.confirmationMu.Unlock()
	} else {
		h.confirmationMu.Unlock()
		select {
		case <-ctx.Done():
			return store.ReadSnapshot{}, ctx.Err()
		case <-confirmation.done:
		}
	}
	return confirmation.snapshot, confirmation.err
}

func (h *sseHub) poll() error {
	// Read, rather than Get, is required even when the tail is unchanged:
	// protocol sliding TTL treats an active live reader as access. It also
	// remains the durable fallback for a lost Pub/Sub notification.
	return h.refreshCause("poll")
}

func (h *sseHub) validateSnapshot(snapshot store.ReadSnapshot) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminalErr != nil {
		return h.terminalErr
	}
	if h.incarnation != snapshot.Incarnation ||
		!store.ContentTypeMatches(h.contentType, snapshot.ContentType) {
		return errSSEHubRecreated
	}
	return nil
}

func (h *sseHub) publish(
	from store.Offset,
	messages []store.Message,
	upToDate bool,
	streamClosed bool,
	tail store.Offset,
) error {
	events, err := h.buildSSEHubEvents(from, messages)
	if err != nil {
		return err
	}
	if len(events) > 0 {
		last := events[len(events)-1]
		last.upToDate = upToDate
		last.closed = streamClosed && last.to.Equal(tail)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.current.Equal(from) {
		return fmt.Errorf("SSE hub offset changed during publish: have %s, read from %s", h.current, from)
	}

	if h.nextSequence == 0 {
		h.firstSequence = 1
		h.nextSequence = 1
	}
	previousRawBytes := h.ringRawBytes
	previousWireBytes := h.ringWireBytes
	previousIndexBytes := h.ringIndexBytes
	for _, event := range events {
		event.sequence = h.nextSequence
		h.nextSequence++
		h.events = append(h.events, event)
		h.ringRawBytes += event.rawBytes
		h.ringWireBytes += event.wireBytes
		h.ringIndexBytes += event.indexBytes
		h.replayBytes += event.memorySize
		h.current = event.to
	}
	if len(events) == 0 && h.current.Equal(tail) {
		h.closed = streamClosed
	}
	if len(events) > 0 && events[len(events)-1].closed {
		h.closed = true
	}

	for h.replayBytes > h.replayLimit && len(h.events) > 1 {
		evicted := h.events[0]
		h.ringRawBytes -= evicted.rawBytes
		h.ringWireBytes -= evicted.wireBytes
		h.ringIndexBytes -= evicted.indexBytes
		h.replayBytes -= evicted.memorySize
		h.events[0] = nil
		h.events = h.events[1:]
		h.firstSequence++
	}
	if len(h.events) == 0 {
		h.firstSequence = h.nextSequence
	}
	rawDelta := h.ringRawBytes - previousRawBytes
	wireDelta := h.ringWireBytes - previousWireBytes
	indexDelta := h.ringIndexBytes - previousIndexBytes
	if rawDelta != 0 || wireDelta != 0 || indexDelta != 0 {
		h.metrics.SSEHubRingBytes(rawDelta, wireDelta, indexDelta)
	}
	h.notifyLocked()
	return nil
}

func (h *sseHub) buildSSEHubEvents(
	from store.Offset,
	messages []store.Message,
) ([]*sseHubEvent, error) {
	var events []*sseHubEvent
	event := &sseHubEvent{from: from}
	finish := func() {
		if len(event.boundaries) == 0 {
			return
		}
		event.to = event.boundaries[len(event.boundaries)-1].offset
		event.wireBytes = len(event.data)
		event.indexBytes = len(event.boundaries) * sseHubBoundaryBytes
		event.memorySize = event.rawBytes + event.wireBytes + event.indexBytes
		events = append(events, event)
		event = &sseHubEvent{from: event.to}
	}
	for _, message := range messages {
		wire, err := h.handler.formatSSEDataEvent(
			h.path,
			[]store.Message{message},
			h.contentType,
			h.useBase64,
		)
		if err != nil {
			return nil, err
		}
		candidateBytes := len(event.data) + len(wire) +
			(len(event.boundaries)+1)*sseHubBoundaryBytes
		if len(event.boundaries) > 0 && candidateBytes > h.batchLimit {
			finish()
		}
		event.data = append(event.data, wire...)
		event.boundaries = append(event.boundaries, sseHubBoundary{
			offset:  message.Offset,
			wireEnd: len(event.data),
		})
	}
	finish()
	return events, nil
}

func (h *sseHub) currentOffset() store.Offset {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current
}

func (h *sseHub) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

func (h *sseHub) isTerminal() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.terminalErr != nil
}

func (h *sseHub) initialize(snapshot store.ReadSnapshot, useBase64 bool) error {
	h.mu.Lock()
	if h.terminalErr != nil {
		err := h.terminalErr
		h.mu.Unlock()
		return err
	}
	if h.isInitialized {
		current := store.ReadSnapshot{
			Incarnation: h.incarnation,
			ContentType: h.contentType,
			CreatedAt:   h.createdAt,
		}
		matches := store.SameReadStream(current, snapshot) && h.useBase64 == useBase64
		h.mu.Unlock()
		if !matches {
			return errSSEHubRecreated
		}
		return nil
	}
	h.contentType = snapshot.ContentType
	h.useBase64 = useBase64
	h.pollInterval = durationOr(h.handler.SSEHubPollInterval, defaultSSEHubPoll)
	h.pollInterval = capSSEHubPollForTTL(h.pollInterval, snapshot.TTLSeconds)
	h.current = snapshot.Tail
	h.closed = snapshot.Closed
	h.incarnation = snapshot.Incarnation
	h.createdAt = snapshot.CreatedAt
	h.isInitialized = true
	initialized := h.initialized
	h.mu.Unlock()
	close(initialized)
	return nil
}

func (h *sseHub) waitInitialized() bool {
	select {
	case <-h.ctx.Done():
		return false
	case <-h.initialized:
		return true
	}
}

func (h *sseHub) fail(err error) {
	h.mu.Lock()
	h.terminalErr = err
	h.notifyLocked()
	h.mu.Unlock()
}

func (h *sseHub) markReady(err error) {
	h.readyOnce.Do(func() {
		h.mu.Lock()
		h.readyErr = err
		h.mu.Unlock()
		close(h.ready)
	})
}

func (h *sseHub) markRegistered(err error) {
	h.registeredOnce.Do(func() {
		h.mu.Lock()
		h.registeredErr = err
		h.mu.Unlock()
		close(h.registered)
	})
}

func (h *sseHub) handleReadError(err error) bool {
	if errors.Is(err, store.ErrStreamNotFound) ||
		errors.Is(err, store.ErrStreamExpired) ||
		errors.Is(err, store.ErrStreamSoftDeleted) {
		h.metrics.SSEReason("terminal_state")
		h.fail(err)
		return true
	}
	if errors.Is(err, errSSEHubRecreated) || errors.Is(err, store.ErrReadSnapshotChanged) {
		h.metrics.SSEReason("incarnation_change")
		h.fail(err)
		return true
	}
	return false
}

func (h *sseHub) logRetry(op string, err error) {
	h.handler.logger().Warn(
		"SSE hub operation failed; retrying",
		"path", h.path,
		"operation", op,
		"error", err,
	)
}

func (h *sseHub) notifyLocked() {
	for watcher := range h.watchers {
		select {
		case watcher.wake <- struct{}{}:
		default:
		}
	}
}

func (h *sseHub) releaseReplay() {
	h.mu.Lock()
	rawBytes := h.ringRawBytes
	wireBytes := h.ringWireBytes
	indexBytes := h.ringIndexBytes
	h.ringRawBytes = 0
	h.ringWireBytes = 0
	h.ringIndexBytes = 0
	h.replayBytes = 0
	h.events = nil
	h.firstSequence = h.nextSequence
	h.mu.Unlock()
	if rawBytes != 0 || wireBytes != 0 || indexBytes != 0 {
		h.metrics.SSEHubRingBytes(-rawBytes, -wireBytes, -indexBytes)
	}
}

func (w *sseHubWatcher) attach(offset store.Offset) error {
	h := w.hub
	h.mu.Lock()
	if h.terminalErr != nil {
		err := h.terminalErr
		h.mu.Unlock()
		return err
	}
	if offset.Equal(h.current) {
		w.attached = true
		w.sequence = h.nextSequence
		w.offset = offset
		w.firstWireStart = 0
		h.mu.Unlock()
		h.metrics.SSEWatcherLookup(0, false)
		return nil
	}
	if h.current.LessThan(offset) {
		h.mu.Unlock()
		return errSSEHubBehind
	}

	eventIndex, steps := firstSSEEventAfter(h.events, offset)
	if eventIndex >= len(h.events) {
		h.mu.Unlock()
		w.recordReplayLag(true, steps)
		return errSSEHubLagged
	}
	event := h.events[eventIndex]
	wireStart := 0
	if !offset.Equal(event.from) {
		boundaryIndex, boundarySteps := findSSEBoundary(event.boundaries, offset)
		steps += boundarySteps
		if boundaryIndex < 0 {
			h.mu.Unlock()
			w.recordReplayLag(true, steps)
			return errSSEHubLagged
		}
		wireStart = event.boundaries[boundaryIndex].wireEnd
	}
	w.attached = true
	w.sequence = event.sequence
	w.offset = offset
	w.firstWireStart = wireStart
	h.mu.Unlock()
	h.metrics.SSEWatcherLookup(steps, false)
	return nil
}

func (w *sseHubWatcher) waitAttach(ctx context.Context, offset store.Offset) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := w.attach(offset)
		if !errors.Is(err, errSSEHubBehind) {
			return err
		}
		if err := w.hub.refreshTo(offset); err != nil {
			return err
		}
	}
}

func (w *sseHubWatcher) next(offset store.Offset) (*sseHubEvent, error) {
	h := w.hub
	h.mu.Lock()
	if h.terminalErr != nil {
		err := h.terminalErr
		h.mu.Unlock()
		return nil, err
	}
	if !w.attached || !offset.Equal(w.offset) {
		h.mu.Unlock()
		return nil, errors.New("SSE watcher used without exact attach boundary")
	}
	if offset.Equal(h.current) {
		if h.closed {
			event := &sseHubEvent{from: offset, to: offset, upToDate: true, closed: true}
			h.mu.Unlock()
			return event, nil
		}
		h.mu.Unlock()
		return nil, nil
	}
	if w.sequence < h.firstSequence {
		h.mu.Unlock()
		w.recordReplayLag(false, 1)
		return nil, errSSEHubLagged
	}
	if w.sequence >= h.nextSequence {
		h.mu.Unlock()
		return nil, nil
	}
	eventIndex := int(w.sequence - h.firstSequence)
	if eventIndex < 0 || eventIndex >= len(h.events) {
		h.mu.Unlock()
		w.recordReplayLag(false, 1)
		return nil, errSSEHubLagged
	}
	found := h.events[eventIndex]
	wireStart := w.firstWireStart
	h.mu.Unlock()
	h.metrics.SSEWatcherLookup(1, false)
	if wireStart == 0 {
		return found, nil
	}
	return &sseHubEvent{
		sequence:   found.sequence,
		from:       offset,
		to:         found.to,
		data:       found.data[wireStart:],
		upToDate:   found.upToDate,
		closed:     found.closed,
		memorySize: len(found.data) - wireStart,
	}, nil
}

func (w *sseHubWatcher) commit(event *sseHubEvent) {
	if event == nil || event.sequence == 0 {
		return
	}
	h := w.hub
	h.mu.Lock()
	if w.attached && w.sequence == event.sequence {
		w.sequence++
		w.offset = event.to
		w.firstWireStart = 0
	}
	h.mu.Unlock()
}

func (w *sseHubWatcher) recordReplayLag(beforeAttach bool, steps int) {
	w.hub.metrics.SSEClientLagged()
	w.hub.metrics.SSEWatcherLookup(steps, true)
	if beforeAttach {
		w.hub.metrics.SSEReason("replay_loss_before_attach")
		return
	}
	w.hub.metrics.SSEReason("replay_loss_after_attach")
}

func firstSSEEventAfter(events []*sseHubEvent, offset store.Offset) (int, int) {
	low, high, steps := 0, len(events), 0
	for low < high {
		steps++
		middle := low + (high-low)/2
		if offset.LessThan(events[middle].to) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low, steps
}

func findSSEBoundary(boundaries []sseHubBoundary, offset store.Offset) (int, int) {
	low, high, steps := 0, len(boundaries), 0
	for low < high {
		steps++
		middle := low + (high-low)/2
		comparison := store.Compare(boundaries[middle].offset, offset)
		if comparison < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= len(boundaries) || !boundaries[low].offset.Equal(offset) {
		return -1, steps
	}
	return low, steps
}

func (h *Handler) formatSSEDataEvent(
	_ string,
	messages []store.Message,
	contentType string,
	useBase64 bool,
) ([]byte, error) {
	var event bytes.Buffer
	event.WriteString("event: data\n")
	if useBase64 {
		event.WriteString("data:")
		encoder := base64.NewEncoder(base64.StdEncoding, &event)
		response := &catchupResponseWriter{
			w:    encoder,
			json: store.IsJSONContentType(contentType),
		}
		if err := response.writePage(messages); err != nil {
			_ = encoder.Close()
			return nil, err
		}
		if err := response.close(); err != nil {
			_ = encoder.Close()
			return nil, err
		}
		if err := encoder.Close(); err != nil {
			return nil, err
		}
		event.WriteString("\n\n")
		return event.Bytes(), nil
	}

	dataWriter := &sseDataWriter{w: &event, lineStart: true}
	response := &catchupResponseWriter{
		w:    dataWriter,
		json: store.IsJSONContentType(contentType),
	}
	if err := response.writePage(messages); err != nil {
		return nil, err
	}
	if err := response.close(); err != nil {
		return nil, err
	}
	if err := dataWriter.close(); err != nil {
		return nil, err
	}
	event.WriteByte('\n')
	return event.Bytes(), nil
}

// sseDataWriter normalizes CRLF, CR, and LF to SSE data lines without
// performing one underlying write per byte.
type sseDataWriter struct {
	w         io.Writer
	lineStart bool
	pendingCR bool
}

func (w *sseDataWriter) Write(p []byte) (int, error) {
	consumed := 0
	for len(p) > 0 {
		if w.pendingCR {
			w.pendingCR = false
			if p[0] == '\n' {
				p = p[1:]
				consumed++
				continue
			}
		}
		if w.lineStart {
			if _, err := io.WriteString(w.w, "data:"); err != nil {
				return consumed, err
			}
			w.lineStart = false
		}
		nextBreak := bytes.IndexAny(p, "\r\n")
		if nextBreak < 0 {
			n, err := writeAllCount(w.w, p)
			return consumed + n, err
		}
		if nextBreak > 0 {
			n, err := writeAllCount(w.w, p[:nextBreak])
			consumed += n
			if err != nil {
				return consumed, err
			}
			p = p[nextBreak:]
		}
		delimiter := p[0]
		if _, err := io.WriteString(w.w, "\n"); err != nil {
			return consumed, err
		}
		p = p[1:]
		consumed++
		w.lineStart = true
		w.pendingCR = delimiter == '\r'
	}
	return consumed, nil
}

func (w *sseDataWriter) close() error {
	if w.lineStart {
		if _, err := io.WriteString(w.w, "data:"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w.w, "\n")
	return err
}

func writeAllCount(w io.Writer, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func writeSSEUpdate(
	w http.ResponseWriter,
	data []byte,
	next store.Offset,
	cursor string,
	upToDate bool,
	closed bool,
	writeTimeout time.Duration,
) error {
	writer := sseClientFrameWriter{response: w, writeTimeout: writeTimeout}
	return writer.write(data, next, cursor, upToDate, closed)
}

type sseControlFrame struct {
	StreamClosed     bool   `json:"streamClosed,omitempty"`
	StreamCursor     string `json:"streamCursor,omitempty"`
	StreamNextOffset string `json:"streamNextOffset"`
	UpToDate         bool   `json:"upToDate,omitempty"`
}

type sseClientFrameWriter struct {
	response     http.ResponseWriter
	writeTimeout time.Duration
	control      bytes.Buffer
}

func (w *sseClientFrameWriter) write(
	data []byte,
	next store.Offset,
	cursor string,
	upToDate bool,
	closed bool,
) error {
	return withSSEWriteDeadline(w.response, w.writeTimeout, func(controller *http.ResponseController) error {
		if len(data) > 0 {
			if _, err := w.response.Write(data); err != nil {
				return err
			}
		}
		control := sseControlFrame{
			StreamClosed:     closed,
			StreamNextOffset: next.String(),
		}
		if !closed {
			control.StreamCursor = cursor
			control.UpToDate = upToDate
		}
		w.control.Reset()
		w.control.WriteString("event: control\ndata:")
		if err := json.NewEncoder(&w.control).Encode(control); err != nil {
			return err
		}
		// Encoder.Encode contributes the first event terminator newline.
		w.control.WriteByte('\n')
		if _, err := w.response.Write(w.control.Bytes()); err != nil {
			return err
		}
		return controller.Flush()
	})
}

func withSSEWriteDeadline(
	w http.ResponseWriter,
	writeTimeout time.Duration,
	action func(*http.ResponseController) error,
) (returnErr error) {
	controller := http.NewResponseController(w)
	err := controller.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	deadlineInstalled := err == nil
	if deadlineInstalled {
		defer func() {
			clearErr := controller.SetWriteDeadline(time.Time{})
			if clearErr != nil && !errors.Is(clearErr, http.ErrNotSupported) && returnErr == nil {
				returnErr = clearErr
			}
		}()
	}
	return action(controller)
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
