// Server-Sent Events streaming, ported from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go handleSSE @ 82f9963).
package chronicle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// handleSSE catches one client up through bounded snapshot pages and then
// attaches it to the per-stream hub shared by every SSE client on this replica.
func (h *Handler) handleSSE(
	w http.ResponseWriter,
	r *http.Request,
	path string,
	meta *store.StreamMetadata,
	offset store.Offset,
	cursor string,
	useBase64 bool,
) (returnErr error) {
	streamStarted := false
	defer func() {
		if streamStarted && returnErr != nil {
			h.logger().Warn(
				"closing SSE connection after streaming error",
				"path", path,
				"error", returnErr,
			)
			returnErr = nil
		}
	}()

	ctx := r.Context()
	reader := h.pageReader()
	pageOpts := store.ReadPageOptions{
		TargetBytes: h.readPageBytes(),
		MaxFrames:   store.DefaultReadPageFrames,
	}

	// Acquire the hub before catch-up. Its confirmed notification subscription
	// and first durable refresh close the append race between catch-up and live
	// delivery. The first page is captured only after the hub is ready.
	lease := h.acquireSSEHub(path, meta, useBase64)
	defer lease.close()
	if err := lease.waitReady(ctx); err != nil {
		return err
	}

	first, err := reader.ReadPage(ctx, path, offset, pageOpts)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			h.observeReadCancellation("before_first_page")
		}
		return readPageError(err)
	}
	snapshot := first.Snapshot
	snapshotReleased := false
	defer func() {
		if !snapshotReleased {
			releaseReadSnapshot(reader, path, snapshot)
		}
	}()
	if err := lease.validateSnapshot(first.Snapshot); err != nil {
		return err
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	if useBase64 {
		w.Header().Set(protocol.HeaderStreamSSEDataEncoding, "base64")
	}
	if _, ok := w.(http.Flusher); !ok {
		return newHTTPError(http.StatusInternalServerError, "streaming not supported")
	}

	reconnectTimer := time.NewTimer(h.SSEReconnectInterval)
	defer reconnectTimer.Stop()
	writeTimeout := durationOr(h.SSEClientWriteTimeout, defaultSSEWriteTimeout)

	// The initial header flush can block on the client just like every later SSE
	// flush. Put it under the same per-client deadline before committing the 200.
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	w.WriteHeader(http.StatusOK)
	streamStarted = true
	if err := controller.Flush(); err != nil {
		return h.recordSSEWriteError(err)
	}
	if err := controller.SetWriteDeadline(time.Time{}); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}

	counted := &sseCountingResponseWriter{ResponseWriter: w}
	responsePages := 0
	defer func() {
		if h.ReadMetrics != nil {
			h.ReadMetrics.ReadResponse(counted.bytes, responsePages)
		}
	}()

	page := first
	currentOffset := offset
	emptyPages := 0
	for {
		select {
		case <-ctx.Done():
			h.observeReadCancellation("between_pages")
			return nil
		case <-reconnectTimer.C:
			return nil
		default:
		}

		if err := lease.validateSnapshot(snapshot); err != nil {
			return err
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			h.observeReadPage(page)
			emptyPages++
			if emptyPages >= 8 {
				return store.ErrReadDataMissing
			}
			pageOpts.Snapshot = &snapshot
			page, err = reader.ReadPage(ctx, path, currentOffset, pageOpts)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					h.observeReadCancellation("storage")
				}
				return err
			}
			continue
		}
		emptyPages = 0
		var data []byte
		if len(page.Messages) > 0 {
			data, err = h.formatSSEDataEvent(path, page.Messages, snapshot.ContentType, useBase64)
			if err != nil {
				return err
			}
		}
		closed := snapshot.Closed && page.UpToDate
		if err := writeSSEUpdate(
			counted,
			data,
			page.NextOffset,
			protocol.GenerateResponseCursor(cursor, time.Now()),
			page.UpToDate,
			closed,
			writeTimeout,
		); err != nil {
			h.observeReadCancellation("write")
			return h.recordSSEWriteError(err)
		}

		// Advance only after the page's data and control event have both been
		// flushed successfully. This is the only safe live-attach cursor.
		currentOffset = page.NextOffset
		responsePages++
		h.observeReadPage(page)
		if closed {
			return nil
		}
		if page.UpToDate {
			break
		}

		pageOpts.Snapshot = &snapshot
		page, err = reader.ReadPage(ctx, path, currentOffset, pageOpts)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				h.observeReadCancellation("storage")
			}
			return err
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			return errors.New("SSE catch-up page made no progress")
		}
	}

	// Close the delete/recreate window between the final page and hub attach.
	// Reading at the captured tail returns no payload but atomically validates
	// the fixed incarnation and snapshot boundary in the maintained stores.
	pageOpts.Snapshot = &snapshot
	confirmed, err := reader.ReadPage(ctx, path, currentOffset, pageOpts)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			h.observeReadCancellation("storage")
		}
		return err
	}
	h.observeReadPage(confirmed)
	if len(confirmed.Messages) != 0 || !confirmed.UpToDate ||
		!confirmed.NextOffset.Equal(snapshot.Tail) {
		return fmt.Errorf("SSE catch-up snapshot confirmation made unexpected progress")
	}
	if err := lease.validateSnapshot(snapshot); err != nil {
		return err
	}
	releaseReadSnapshot(reader, path, snapshot)
	snapshotReleased = true

	// Live delivery uses one shared bounded durable read and one shared
	// formatted data event. A lagged client disconnects and resumes from the
	// last control offset that was flushed above.
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reconnectTimer.C:
			return nil
		default:
		}

		event, nextErr := lease.watcher.next(currentOffset)
		if nextErr != nil {
			if errors.Is(nextErr, errSSEHubLagged) {
				h.logger().Warn(
					"disconnecting SSE client behind replay window",
					"path", path,
					"offset", currentOffset.String(),
				)
				return nil
			}
			return nextErr
		}
		if event != nil {
			if err := writeSSEUpdate(
				counted,
				event.data,
				event.to,
				protocol.GenerateResponseCursor(cursor, time.Now()),
				event.upToDate,
				event.closed,
				writeTimeout,
			); err != nil {
				return h.recordSSEWriteError(err)
			}
			currentOffset = event.to
			if event.closed {
				return nil
			}
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-reconnectTimer.C:
			return nil
		case <-lease.watcher.wake:
		}
	}
}

type sseCountingResponseWriter struct {
	http.ResponseWriter
	bytes int
}

func (w *sseCountingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func (w *sseCountingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (h *Handler) recordSSEWriteError(err error) error {
	var timeout interface {
		Timeout() bool
	}
	if errors.As(err, &timeout) && timeout.Timeout() {
		h.sseMetrics().SSEClientWriteTimeout()
	}
	return err
}
