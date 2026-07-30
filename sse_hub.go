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
	errSSEHubRecreated = errors.New("stream was deleted and recreated")
)

// SSEMetrics is the data-plane metrics seam for the shared SSE hub. The
// Prometheus implementation lives in metrics; nil records nothing.
type SSEMetrics interface {
	SSEHubActive(delta int)
	SSEClientActive(delta int)
	SSEHubRead(messages int)
	SSEHubRingBytes(delta int)
	SSEClientLagged()
	SSEClientWriteTimeout()
	SSESubscriptionActive(delta int)
	SSESubscriptionEvent(event string)
}

type nopSSEMetrics struct{}

func (nopSSEMetrics) SSEHubActive(int)            {}
func (nopSSEMetrics) SSEClientActive(int)         {}
func (nopSSEMetrics) SSEHubRead(int)              {}
func (nopSSEMetrics) SSEHubRingBytes(int)         {}
func (nopSSEMetrics) SSEClientLagged()            {}
func (nopSSEMetrics) SSEClientWriteTimeout()      {}
func (nopSSEMetrics) SSESubscriptionActive(int)   {}
func (nopSSEMetrics) SSESubscriptionEvent(string) {}

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

func (h *Handler) acquireSSEHub(path string, meta *store.StreamMetadata, useBase64 bool) *sseHubLease {
	metrics := h.sseMetrics()
	h.sseHubs.mu.Lock()
	defer h.sseHubs.mu.Unlock()

	if h.sseHubs.hubs == nil {
		h.sseHubs.hubs = make(map[string]*sseHubEntry)
	}
	entry := h.sseHubs.hubs[path]
	if entry == nil || !entry.hub.canServe(meta) {
		ctx, cancel := context.WithCancel(context.Background())
		replayLimit := positiveOr(h.SSEHubReplayBytes, defaultSSEHubReplayBytes)
		batchLimit := min(positiveOr(h.SSEHubBatchBytes, defaultSSEHubBatchBytes), replayLimit)
		pollInterval := durationOr(h.SSEHubPollInterval, defaultSSEHubPoll)
		pollInterval = capSSEHubPollForTTL(pollInterval, meta.TTLSeconds)
		hub := &sseHub{
			handler:      h,
			path:         path,
			contentType:  meta.ContentType,
			useBase64:    useBase64,
			replayLimit:  replayLimit,
			batchLimit:   batchLimit,
			pollInterval: pollInterval,
			ctx:          ctx,
			cancel:       cancel,
			done:         make(chan struct{}),
			ready:        make(chan struct{}),
			current:      meta.CurrentOffset,
			closed:       meta.Closed,
			incarnation:  meta.Incarnation,
			createdAt:    meta.CreatedAt,
			watchers:     make(map[*sseHubWatcher]struct{}),
			metrics:      metrics,
		}
		entry = &sseHubEntry{hub: hub}
		h.sseHubs.hubs[path] = entry
		metrics.SSEHubActive(1)
		go hub.run()
	}

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
	handler      *Handler
	path         string
	contentType  string
	useBase64    bool
	replayLimit  int
	batchLimit   int
	pollInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	ready        chan struct{}
	readyOnce    sync.Once
	metrics      SSEMetrics

	mu          sync.Mutex
	readyErr    error
	current     store.Offset
	closed      bool
	incarnation string
	createdAt   time.Time
	terminalErr error
	events      []*sseHubEvent
	replayBytes int
	watchers    map[*sseHubWatcher]struct{}
}

type sseHubEvent struct {
	from       store.Offset
	to         store.Offset
	messages   []store.Message
	data       []byte
	upToDate   bool
	closed     bool
	memorySize int
}

type sseHubWatcher struct {
	hub  *sseHub
	wake chan struct{}
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
	defer h.markReady(h.ctx.Err())

	if subscriber, ok := h.handler.Store.(store.NotificationSubscriber); ok {
		h.runPersistent(subscriber)
		return
	}
	h.runWaitLoop()
}

func (h *sseHub) runPersistent(subscriber store.NotificationSubscriber) {
	retry := sseHubRetryMin
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
		var closeOnce sync.Once
		closeSub := func() {
			closeOnce.Do(func() {
				_ = sub.Close()
				h.metrics.SSESubscriptionActive(-1)
				h.metrics.SSESubscriptionEvent("closed")
			})
		}

		if err := h.refresh(); err != nil {
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
			}

			var refreshErr error
			if errors.Is(err, context.DeadlineExceeded) {
				refreshErr = h.poll()
			} else {
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
	retry := sseHubRetryMin
	for h.ctx.Err() == nil && !h.isClosed() {
		err := h.refresh()
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
	ctx := h.ctx
	if ctx == nil {
		// refresh is also exercised directly by focused tests and diagnostic
		// callers. Production hubs always install a cancellable context.
		ctx = context.Background()
	}
	reader := h.handler.pageReader()
	next := h.currentOffset()
	opts := store.ReadPageOptions{
		TargetBytes: min(h.handler.readPageBytes(), h.batchLimit),
		MaxFrames:   store.DefaultReadPageFrames,
	}
	var snapshot *store.ReadSnapshot
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

func (h *sseHub) poll() error {
	// Read, rather than Get, is required even when the tail is unchanged:
	// protocol sliding TTL treats an active live reader as access. It also
	// remains the durable fallback for a lost Pub/Sub notification.
	return h.refresh()
}

func (h *sseHub) checkIncarnation(meta *store.StreamMetadata) error {
	snapshot := &store.StreamMetadata{
		Incarnation: h.incarnation,
		CreatedAt:   h.createdAt,
	}
	if !snapshot.SameIncarnation(meta) {
		return errSSEHubRecreated
	}
	return nil
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
	chunks := splitSSEMessages(messages, h.batchLimit, h.contentType, h.useBase64)
	events := make([]*sseHubEvent, 0, len(chunks))
	next := from
	for i, chunk := range chunks {
		last := i == len(chunks)-1
		data, err := h.handler.formatSSEDataEvent(h.path, chunk, h.contentType, h.useBase64)
		if err != nil {
			return err
		}
		event := &sseHubEvent{
			from:     next,
			to:       chunk[len(chunk)-1].Offset,
			messages: chunk,
			data:     data,
			upToDate: last && upToDate,
		}
		event.closed = last && streamClosed && event.to.Equal(tail)
		event.memorySize = len(event.data)
		for _, message := range chunk {
			event.memorySize += len(message.Data)
		}
		events = append(events, event)
		next = event.to
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.current.Equal(from) {
		return fmt.Errorf("SSE hub offset changed during publish: have %s, read from %s", h.current, from)
	}

	previousReplayBytes := h.replayBytes
	for _, event := range events {
		h.events = append(h.events, event)
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
		h.replayBytes -= h.events[0].memorySize
		h.events[0] = nil
		h.events = h.events[1:]
	}
	if retainedDelta := h.replayBytes - previousReplayBytes; retainedDelta != 0 {
		h.metrics.SSEHubRingBytes(retainedDelta)
	}
	h.notifyLocked()
	return nil
}

func splitSSEMessages(
	messages []store.Message,
	limit int,
	contentType string,
	useBase64 bool,
) [][]store.Message {
	if len(messages) == 0 {
		return nil
	}

	jsonContent := store.IsJSONContentType(contentType)
	var chunks [][]store.Message
	start := 0
	rawBytes := 0
	size := newSSEEventSize(jsonContent)
	for i, message := range messages {
		candidate := size
		if jsonContent && i > start {
			candidate.addByte(',')
		}
		candidate.add(message.Data)
		candidateRawBytes := rawBytes + len(message.Data)
		if i > start && candidate.retainedBytes(candidateRawBytes, jsonContent, useBase64) > limit {
			chunks = append(chunks, append([]store.Message(nil), messages[start:i]...))
			start = i
			candidateRawBytes = len(message.Data)
			candidate = newSSEEventSize(jsonContent)
			candidate.add(message.Data)
		}
		rawBytes = candidateRawBytes
		size = candidate
	}
	return append(chunks, append([]store.Message(nil), messages[start:]...))
}

type sseEventSize struct {
	bodyBytes  int
	textBytes  int
	lineBreaks int
	previousCR bool
}

func newSSEEventSize(jsonContent bool) sseEventSize {
	var size sseEventSize
	if jsonContent {
		size.addByte('[')
	}
	return size
}

func (s *sseEventSize) add(data []byte) {
	for _, b := range data {
		s.addByte(b)
	}
}

func (s *sseEventSize) addByte(b byte) {
	s.bodyBytes++
	if b == '\n' && s.previousCR {
		s.previousCR = false
		return
	}
	s.previousCR = b == '\r'
	if b == '\r' || b == '\n' {
		s.lineBreaks++
		return
	}
	s.textBytes++
}

func (s sseEventSize) retainedBytes(rawBytes int, jsonContent, useBase64 bool) int {
	if jsonContent {
		s.addByte(']')
	}
	const eventPrefixBytes = len("event: data\n")
	if useBase64 {
		const framingBytes = eventPrefixBytes + len("data:") + len("\n\n")
		return rawBytes + framingBytes + base64.StdEncoding.EncodedLen(s.bodyBytes)
	}
	const (
		dataLineBytes = len("data:") + len("\n")
		finalNewline  = len("\n")
	)
	eventBytes := eventPrefixBytes + s.textBytes + dataLineBytes*(s.lineBreaks+1) + finalNewline
	return rawBytes + eventBytes
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

func (h *sseHub) canServe(meta *store.StreamMetadata) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := &store.StreamMetadata{
		Incarnation: h.incarnation,
		CreatedAt:   h.createdAt,
	}
	return h.terminalErr == nil && snapshot.SameIncarnation(meta)
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

func (h *sseHub) handleReadError(err error) bool {
	if errors.Is(err, store.ErrStreamNotFound) ||
		errors.Is(err, store.ErrStreamExpired) ||
		errors.Is(err, store.ErrStreamSoftDeleted) ||
		errors.Is(err, errSSEHubRecreated) {
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
	bytes := h.replayBytes
	h.replayBytes = 0
	h.events = nil
	h.mu.Unlock()
	if bytes != 0 {
		h.metrics.SSEHubRingBytes(-bytes)
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
	if offset.Equal(h.current) {
		if h.closed {
			event := &sseHubEvent{from: offset, to: offset, upToDate: true, closed: true}
			h.mu.Unlock()
			return event, nil
		}
		h.mu.Unlock()
		return nil, nil
	}
	if h.current.LessThan(offset) {
		h.mu.Unlock()
		return nil, nil
	}

	var found *sseHubEvent
	start := 0
	for _, event := range h.events {
		if offset.Equal(event.from) {
			found = event
			break
		}
		for i, message := range event.messages {
			if !offset.Equal(message.Offset) {
				continue
			}
			if i+1 < len(event.messages) {
				found = event
				start = i + 1
			}
			break
		}
		if found != nil {
			break
		}
	}
	h.mu.Unlock()

	if found == nil {
		h.metrics.SSEClientLagged()
		return nil, errSSEHubLagged
	}
	if start == 0 {
		return found, nil
	}

	messages := found.messages[start:]
	data, err := h.handler.formatSSEDataEvent(h.path, messages, h.contentType, h.useBase64)
	if err != nil {
		return nil, err
	}
	return &sseHubEvent{
		from:       offset,
		to:         messages[len(messages)-1].Offset,
		messages:   messages,
		data:       data,
		upToDate:   found.upToDate,
		closed:     found.closed,
		memorySize: len(data),
	}, nil
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
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	control := map[string]any{"streamNextOffset": next.String()}
	if closed {
		control["streamClosed"] = true
	} else {
		control["streamCursor"] = cursor
		if upToDate {
			control["upToDate"] = true
		}
	}
	controlJSON, err := json.Marshal(control)
	if err != nil {
		return err
	}
	var event bytes.Buffer
	event.Grow(len(controlJSON) + 32)
	event.WriteString("event: control\n")
	event.WriteString("data:")
	event.Write(controlJSON)
	event.WriteString("\n\n")
	if _, err := w.Write(event.Bytes()); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil {
		return err
	}
	if err := controller.SetWriteDeadline(time.Time{}); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
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
