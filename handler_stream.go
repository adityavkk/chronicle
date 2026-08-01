package chronicle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const liveReadPollInterval = time.Second

// ReadMetrics receives bounded catch-up observations. Implementations must use
// bounded-cardinality labels.
type ReadMetrics interface {
	ReadPage(targetBytes, fetchedBytes, returnedBytes, discardedBytes int, redisScriptTime time.Duration, redisScriptInvokes int)
	ReadResponse(responseBytes, pages int)
	ReadCancellation(phase string)
}

func (h *Handler) readPageBytes() int {
	if h.ReadPageBytes > 0 {
		return h.ReadPageBytes
	}
	return store.DefaultReadPageBytes
}

func (h *Handler) pageReader() store.PageReader {
	if reader, ok := h.Store.(store.PageReader); ok {
		return reader
	}
	return legacyPageReader{store: h.Store}
}

func (h *Handler) ssePageReader() (store.PageReader, error) {
	reader, ok := h.Store.(store.PageReader)
	if !ok {
		return nil, newHTTPError(
			http.StatusInternalServerError,
			"SSE requires a store.PageReader backend",
		)
	}
	return reader, nil
}

func (h *Handler) captureReadSnapshot(
	ctx context.Context,
	path string,
	noTouch bool,
) (store.ReadSnapshot, error) {
	reader := h.pageReader()
	page, err := reader.ReadPage(
		ctx,
		path,
		store.NowOffset,
		store.ReadPageOptions{
			TargetBytes: 1,
			MaxFrames:   1,
			NoTouch:     noTouch,
		},
	)
	if err != nil {
		return store.ReadSnapshot{}, err
	}
	releaseReadSnapshot(reader, path, page.Snapshot)
	return page.Snapshot, nil
}

// readValidationError preserves the protocol's authorization → existence →
// request-validation order without loading producer state. Invalid reads do
// not count as access and therefore use a no-touch projection.
func (h *Handler) readValidationError(
	ctx context.Context,
	path string,
	message string,
) error {
	if _, err := h.captureReadSnapshot(ctx, path, true); err != nil {
		return readPageError(err)
	}
	return newHTTPError(http.StatusBadRequest, message)
}

// legacyPageReader keeps third-party Store implementations source compatible.
// It prevents the handler's second whole-body allocation, but the backend
// cannot promise bounded storage work without implementing store.PageReader.
type legacyPageReader struct {
	store store.Store
}

func (r legacyPageReader) ReadPage(ctx context.Context, path string, offset store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	if err := ctx.Err(); err != nil {
		return store.ReadPage{}, err
	}
	opts = opts.Normalize()

	var (
		messages []store.Message
		snapshot store.ReadSnapshot
	)
	metaPtr, err := r.store.Get(path)
	if err != nil {
		return store.ReadPage{}, err
	}
	// Store.Get historically returns a pointer. Some compatible stores,
	// including MemoryStore, point it at live metadata. Copy it before Read so
	// an append cannot silently move the pre-read incarnation check.
	meta := *metaPtr
	incarnation := readIncarnation(&meta)
	if opts.Snapshot != nil && (opts.Snapshot.Incarnation != incarnation ||
		!store.ContentTypeMatches(opts.Snapshot.ContentType, meta.ContentType)) {
		return store.ReadPage{}, store.ErrReadSnapshotChanged
	}
	readOffset := offset
	if readOffset.IsNow() {
		readOffset = meta.CurrentOffset
	}
	messages, _, err = r.store.Read(path, readOffset)
	if err != nil {
		return store.ReadPage{}, err
	}
	afterPtr, err := r.store.Get(path)
	if err != nil {
		return store.ReadPage{}, err
	}
	after := *afterPtr
	if readIncarnation(&after) != incarnation ||
		!after.CreatedAt.Equal(meta.CreatedAt) ||
		!store.ContentTypeMatches(after.ContentType, meta.ContentType) {
		return store.ReadPage{}, store.ErrReadSnapshotChanged
	}

	if opts.Snapshot != nil {
		snapshot = *opts.Snapshot
	} else {
		// Capture the post-read tail. An append between Read and Get is then
		// inside this snapshot but not this page; the next page fetches it from
		// the unchanged offset. This preserves the old SSE race-closing
		// behavior without falsely checkpointing or skipping the append.
		snapshotMeta := &after
		if offset.IsNow() {
			snapshotMeta = &meta
			messages = nil
		}
		snapshot = store.ReadSnapshotFromMetadata(snapshotMeta)
		snapshot.Incarnation = incarnation
	}

	filtered := make([]store.Message, 0, min(len(messages), opts.MaxFrames))
	var fetched, returned int
	for _, message := range messages {
		if snapshot.Tail.LessThan(message.Offset) {
			break
		}
		fetched += len(message.Data)
	}
	for _, message := range messages {
		if snapshot.Tail.LessThan(message.Offset) {
			break
		}
		if len(filtered) >= opts.MaxFrames ||
			(len(filtered) > 0 && returned+len(message.Data) > opts.TargetBytes) {
			break
		}
		filtered = append(filtered, message)
		returned += len(message.Data)
	}
	next := snapshot.Tail
	if readOffset.LessThan(snapshot.Tail) {
		next = readOffset
	}
	if len(filtered) > 0 {
		next = filtered[len(filtered)-1].Offset
	}
	return store.ReadPage{
		Messages:   filtered,
		NextOffset: next,
		Snapshot:   snapshot,
		UpToDate:   next.Equal(snapshot.Tail),
		Stats: store.ReadPageStats{
			RequestedBytes: opts.TargetBytes,
			FetchedBytes:   fetched,
			ReturnedBytes:  returned,
			DiscardedBytes: fetched - returned,
		},
	}, nil
}

func readIncarnation(meta *store.StreamMetadata) string {
	if meta.Incarnation != "" {
		return meta.Incarnation
	}
	return strconv.FormatInt(meta.CreatedAt.UnixNano(), 10)
}

type catchupResponseWriter struct {
	w         io.Writer
	json      bool
	enveloped bool
	started   bool
	wrote     bool
	bytes     int
}

func (w *catchupResponseWriter) start() error {
	if w.started {
		return nil
	}
	w.started = true
	if w.json {
		return w.write([]byte("["))
	}
	return nil
}

func (w *catchupResponseWriter) writePage(messages []store.Message) error {
	if err := w.start(); err != nil {
		return err
	}
	for _, message := range messages {
		if w.json && w.wrote {
			if err := w.write([]byte(",")); err != nil {
				return err
			}
		}
		if w.enveloped {
			if err := w.write([]byte(`{"offset":`)); err != nil {
				return err
			}
			if err := w.write(strconv.AppendQuote(nil, message.Offset.String())); err != nil {
				return err
			}
			if err := w.write([]byte(`,"data":`)); err != nil {
				return err
			}
		}
		if err := w.write(message.Data); err != nil {
			return err
		}
		if w.enveloped {
			if err := w.write([]byte("}")); err != nil {
				return err
			}
		}
		w.wrote = true
	}
	return nil
}

func (w *catchupResponseWriter) close() error {
	if err := w.start(); err != nil {
		return err
	}
	if w.json {
		return w.write([]byte("]"))
	}
	return nil
}

func (w *catchupResponseWriter) write(p []byte) error {
	for len(p) > 0 {
		n, err := w.w.Write(p)
		w.bytes += n
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (h *Handler) observeReadPage(page store.ReadPage) {
	if h.ReadMetrics == nil {
		return
	}
	h.ReadMetrics.ReadPage(
		page.Stats.RequestedBytes,
		page.Stats.FetchedBytes,
		page.Stats.ReturnedBytes,
		page.Stats.DiscardedBytes,
		page.Stats.RedisScriptTime,
		page.Stats.RedisScriptInvokes,
	)
}

func (h *Handler) observeReadCancellation(phase string) {
	if h.ReadMetrics != nil {
		h.ReadMetrics.ReadCancellation(phase)
	}
}

func releaseReadSnapshot(reader store.PageReader, path string, snapshot store.ReadSnapshot) {
	if releaser, ok := reader.(store.PageSnapshotReleaser); ok {
		releaser.ReleaseReadSnapshot(path, snapshot)
	}
}

func (h *Handler) waitForPage(
	ctx context.Context,
	reader store.PageReader,
	path string,
	offset store.Offset,
	initial store.ReadSnapshot,
	timeout time.Duration,
	opts store.ReadPageOptions,
) (store.ReadWaitResult, error) {
	if waiter, ok := h.Store.(store.PageWaiter); ok {
		return waiter.WaitForPage(ctx, path, offset, initial, timeout, opts)
	}

	_, timedOut, _, err := h.Store.WaitForMessages(ctx, path, offset, timeout)
	if err != nil {
		return store.ReadWaitResult{}, err
	}
	recheckOpts := opts
	recheckOpts.Snapshot = nil
	recheckOpts.NoTouch = true
	page, err := reader.ReadPage(ctx, path, offset, recheckOpts)
	if err != nil {
		return store.ReadWaitResult{}, err
	}
	if !store.SameReadStream(initial, page.Snapshot) {
		releaseReadSnapshot(reader, path, page.Snapshot)
		return store.ReadWaitResult{}, store.ErrReadSnapshotChanged
	}
	return store.ReadWaitResult{Page: page, TimedOut: timedOut}, nil
}

func (h *Handler) subscribeNotifications(
	ctx context.Context,
	path string,
) (store.NotificationSubscription, bool, error) {
	var subscriber store.NotificationSubscriber
	var ok bool
	if provider, hasProvider := h.Store.(store.NotificationSubscriberProvider); hasProvider {
		subscriber, ok = provider.NotificationSubscriber()
	} else {
		subscriber, ok = h.Store.(store.NotificationSubscriber)
	}
	if !ok {
		return nil, false, nil
	}
	subscription, err := subscriber.SubscribeNotifications(ctx, path)
	if err != nil {
		return nil, true, err
	}
	return subscription, true, nil
}

// waitForRegisteredPage waits on a notification registration that was
// confirmed before the caller's authoritative first page. It deliberately has
// no immediate attach recheck: an append before that page is in its captured
// tail, while an append after it is covered by the registration. Wake, poll,
// and timeout results remain fresh no-touch durable pages.
func (h *Handler) waitForRegisteredPage(
	ctx context.Context,
	reader store.PageReader,
	path string,
	offset store.Offset,
	initial store.ReadSnapshot,
	timeout time.Duration,
	opts store.ReadPageOptions,
	subscription store.NotificationSubscription,
) (store.ReadWaitResult, error) {
	recheck := func() (store.ReadPage, bool, error) {
		recheckOpts := opts
		recheckOpts.Snapshot = nil
		recheckOpts.NoTouch = true
		page, err := reader.ReadPage(ctx, path, offset, recheckOpts)
		if err != nil {
			return store.ReadPage{}, false, err
		}
		if !store.SameReadStream(initial, page.Snapshot) {
			releaseReadSnapshot(reader, path, page.Snapshot)
			return store.ReadPage{}, false, store.ErrReadSnapshotChanged
		}
		done := len(page.Messages) > 0 ||
			(page.Snapshot.Closed && offset.Equal(page.Snapshot.Tail))
		return page, done, nil
	}

	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			page, done, err := recheck()
			if err != nil {
				return store.ReadWaitResult{}, err
			}
			return store.ReadWaitResult{Page: page, TimedOut: !done}, nil
		}
		waitFor := min(remaining, liveReadPollInterval)
		waitCtx, cancel := context.WithTimeout(ctx, waitFor)
		_, waitErr := subscription.Wait(waitCtx)
		cancel()
		if err := ctx.Err(); err != nil {
			return store.ReadWaitResult{}, err
		}
		if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) {
			return store.ReadWaitResult{}, waitErr
		}

		page, done, err := recheck()
		if err != nil {
			return store.ReadWaitResult{}, err
		}
		if done || !time.Now().Before(deadline) {
			return store.ReadWaitResult{Page: page, TimedOut: !done}, nil
		}
		releaseReadSnapshot(reader, path, page.Snapshot)
	}
}

// streamCatchupResponse writes each storage page directly to the response.
// Once headers are committed, an internal pagination failure must abort the
// transport. A clean EOF would otherwise let a client accept a partial opaque
// body together with the snapshot-tail response header.
func (h *Handler) streamCatchupResponse(
	w http.ResponseWriter,
	r *http.Request,
	reader store.PageReader,
	path string,
	first store.ReadPage,
	pageOpts store.ReadPageOptions,
	stopAfterFirst bool,
	jsonMode bool,
	enveloped bool,
) {
	writer := &catchupResponseWriter{w: w, json: jsonMode, enveloped: enveloped}
	pages := 0
	defer releaseReadSnapshot(reader, path, first.Snapshot)
	defer func() {
		if h.ReadMetrics != nil {
			h.ReadMetrics.ReadResponse(writer.bytes, pages)
		}
	}()

	flusher, _ := w.(http.Flusher)
	page := first
	for {
		if err := r.Context().Err(); err != nil {
			h.observeReadCancellation("between_pages")
			return
		}
		pages++
		h.observeReadPage(page)
		if err := writer.writePage(page.Messages); err != nil {
			h.observeReadCancellation("write")
			h.logger().Debug("catch-up response write stopped", "path", path, "error", err)
			return
		}
		if flusher != nil {
			flusher.Flush()
			if err := r.Context().Err(); err != nil {
				h.observeReadCancellation("flush")
				return
			}
		}

		if stopAfterFirst || page.UpToDate {
			break
		}
		next := page.NextOffset
		pageOpts.Snapshot = &first.Snapshot
		var err error
		page, err = reader.ReadPage(r.Context(), path, next, pageOpts)
		if err != nil {
			requestErr := r.Context().Err()
			if requestErr != nil &&
				(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				h.observeReadCancellation("storage")
				return
			}
			h.logger().Error("catch-up storage page failed after response started", "path", path, "offset", next, "error", err)
			panic(http.ErrAbortHandler)
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			h.logger().Error("catch-up storage page made no progress", "path", path, "offset", next)
			panic(http.ErrAbortHandler)
		}
	}
	if err := writer.close(); err != nil {
		h.observeReadCancellation("write")
		h.logger().Debug("catch-up response close stopped", "path", path, "error", err)
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
}

func readPageError(err error) error {
	switch {
	case errors.Is(err, store.ErrStreamNotFound):
		return newHTTPError(http.StatusNotFound, "stream not found")
	case errors.Is(err, store.ErrStreamSoftDeleted):
		return newHTTPError(http.StatusGone, "stream has been deleted")
	case errors.Is(err, store.ErrReadSnapshotChanged):
		return fmt.Errorf("stream changed during read: %w", err)
	default:
		return err
	}
}
