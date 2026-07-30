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
	meta, err := r.store.Get(path)
	if err != nil {
		return store.ReadPage{}, err
	}
	incarnation := readIncarnation(meta)
	if opts.Snapshot != nil && (opts.Snapshot.Incarnation != incarnation ||
		!store.ContentTypeMatches(opts.Snapshot.ContentType, meta.ContentType)) {
		return store.ReadPage{}, store.ErrReadSnapshotChanged
	}
	messages, _, err := r.store.Read(path, offset)
	if err != nil {
		return store.ReadPage{}, err
	}
	after, err := r.store.Get(path)
	if err != nil {
		return store.ReadPage{}, err
	}
	if readIncarnation(after) != incarnation ||
		!after.CreatedAt.Equal(meta.CreatedAt) ||
		!store.ContentTypeMatches(after.ContentType, meta.ContentType) {
		return store.ReadPage{}, store.ErrReadSnapshotChanged
	}
	snapshot := store.ReadSnapshot{
		Tail:        meta.CurrentOffset,
		ContentType: meta.ContentType,
		Closed:      meta.Closed,
		Incarnation: incarnation,
	}
	if opts.Snapshot != nil {
		snapshot = *opts.Snapshot
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
	case errors.Is(err, store.ErrReadSnapshotChanged):
		return fmt.Errorf("stream changed during read: %w", err)
	default:
		return err
	}
}
