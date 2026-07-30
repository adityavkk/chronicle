// Server-Sent Events streaming, ported from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go handleSSE @ 82f9963).
package chronicle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// handleSSE handles Server-Sent Events streaming.
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, path string, offset store.Offset, cursor string, useBase64 bool) error {
	reader := h.pageReader()
	pageOpts := store.ReadPageOptions{
		TargetBytes: h.readPageBytes(),
		MaxFrames:   store.DefaultReadPageFrames,
	}
	first, err := reader.ReadPage(r.Context(), path, offset, pageOpts)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			h.observeReadCancellation("before_first_page")
		}
		return readPageError(err)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	if useBase64 {
		w.Header().Set(protocol.HeaderStreamSSEDataEncoding, "base64")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return newHTTPError(http.StatusInternalServerError, "streaming not supported")
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	counter := &countingWriter{w: w}
	totalPages := 0
	defer func() {
		if h.ReadMetrics != nil {
			h.ReadMetrics.ReadResponse(counter.bytes, totalPages)
		}
	}()

	ctx := r.Context()
	reconnectTimer := time.NewTimer(h.SSEReconnectInterval)
	defer reconnectTimer.Stop()

	currentOffset, closed, pages, err := h.streamSSESnapshot(
		ctx,
		counter,
		flusher,
		reader,
		path,
		cursor,
		useBase64,
		first,
		pageOpts,
	)
	totalPages += pages
	if err != nil {
		h.logger().Debug("SSE initial catch-up stopped", "path", path, "error", err)
		return nil
	}
	if closed {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			h.observeReadCancellation("between_pages")
			return nil
		case <-reconnectTimer.C:
			return nil
		default:
		}

		// WaitForMessages is a wake primitive here. The next ReadPage captures
		// the exact bounded snapshot that will be emitted.
		waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, timedOut, streamClosed, waitErr := h.Store.WaitForMessages(waitCtx, path, currentOffset, 100*time.Millisecond)
		cancel()
		if ctx.Err() != nil {
			h.observeReadCancellation("wait")
			return nil
		}
		if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled) {
			h.logger().Debug("SSE wake wait failed", "path", path, "error", waitErr)
		}
		if timedOut && !streamClosed {
			continue
		}

		next, readErr := reader.ReadPage(ctx, path, currentOffset, pageOpts)
		if readErr != nil {
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				h.observeReadCancellation("storage")
			}
			h.logger().Error("SSE storage page failed after response started", "path", path, "offset", currentOffset, "error", readErr)
			return nil
		}
		currentOffset, closed, pages, err = h.streamSSESnapshot(
			ctx,
			counter,
			flusher,
			reader,
			path,
			cursor,
			useBase64,
			next,
			pageOpts,
		)
		totalPages += pages
		if err != nil {
			h.logger().Debug("SSE live page stopped", "path", path, "error", err)
			return nil
		}
		if closed {
			return nil
		}
	}
}

func (h *Handler) streamSSESnapshot(
	ctx context.Context,
	w io.Writer,
	flusher http.Flusher,
	reader store.PageReader,
	path string,
	cursor string,
	useBase64 bool,
	first store.ReadPage,
	opts store.ReadPageOptions,
) (store.Offset, bool, int, error) {
	page := first
	pages := 0
	for {
		if err := ctx.Err(); err != nil {
			h.observeReadCancellation("between_pages")
			return page.NextOffset, false, pages, err
		}
		if len(page.Messages) > 0 {
			if err := writeSSEDataEvent(w, page.Messages, page.Snapshot.ContentType, useBase64); err != nil {
				h.observeReadCancellation("write")
				return page.NextOffset, false, pages, err
			}
		}

		closed := page.Snapshot.Closed && page.UpToDate
		if err := writeSSEControlEvent(w, page.NextOffset, cursor, page.UpToDate, closed); err != nil {
			h.observeReadCancellation("write")
			return page.NextOffset, false, pages, err
		}
		pages++
		h.observeReadPage(page)
		flusher.Flush()
		if err := ctx.Err(); err != nil {
			h.observeReadCancellation("flush")
			return page.NextOffset, false, pages, err
		}
		if closed {
			return page.NextOffset, true, pages, nil
		}
		if page.UpToDate {
			return page.NextOffset, false, pages, nil
		}

		nextOffset := page.NextOffset
		opts.Snapshot = &first.Snapshot
		var err error
		page, err = reader.ReadPage(ctx, path, nextOffset, opts)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				h.observeReadCancellation("storage")
			}
			return nextOffset, false, pages, err
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			return nextOffset, false, pages, errors.New("SSE storage page made no progress")
		}
	}
}

func writeSSEDataEvent(w io.Writer, messages []store.Message, contentType string, useBase64 bool) error {
	if _, err := io.WriteString(w, "event: data\n"); err != nil {
		return err
	}
	if useBase64 {
		if _, err := io.WriteString(w, "data:"); err != nil {
			return err
		}
		encoder := base64.NewEncoder(base64.StdEncoding, w)
		for _, message := range messages {
			if err := writeAll(encoder, message.Data); err != nil {
				_ = encoder.Close()
				return err
			}
		}
		if err := encoder.Close(); err != nil {
			return err
		}
		_, err := io.WriteString(w, "\n\n")
		return err
	}

	dataWriter := &sseDataWriter{w: w, lineStart: true}
	response := &catchupResponseWriter{
		w:    dataWriter,
		json: store.IsJSONContentType(contentType),
	}
	if err := response.writePage(messages); err != nil {
		return err
	}
	if err := response.close(); err != nil {
		return err
	}
	if err := dataWriter.close(); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func writeSSEControlEvent(w io.Writer, offset store.Offset, cursor string, upToDate, closed bool) error {
	control := map[string]any{
		"streamNextOffset": offset.String(),
	}
	if closed {
		control["streamClosed"] = true
	} else {
		control["streamCursor"] = protocol.GenerateResponseCursor(cursor, time.Now())
		if upToDate {
			control["upToDate"] = true
		}
	}
	body, err := json.Marshal(control)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, "event: control\ndata:"); err != nil {
		return err
	}
	if err := writeAll(w, body); err != nil {
		return err
	}
	_, err = io.WriteString(w, "\n\n")
	return err
}

type countingWriter struct {
	w     io.Writer
	bytes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.bytes += n
	return n, err
}

// sseDataWriter normalizes CRLF, CR, and LF to SSE data lines. It does not add
// a space after "data:", so leading spaces in stream bytes remain intact.
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
			consumed += n
			if err != nil {
				return consumed, err
			}
			return consumed, nil
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

func writeAll(w io.Writer, p []byte) error {
	_, err := writeAllCount(w, p)
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
