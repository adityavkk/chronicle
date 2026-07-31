// Server-Sent Events streaming, ported from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go handleSSE @ 82f9963).
package chronicle

import (
	"context"
	"errors"
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
	reader store.PageReader,
	first store.ReadPage,
	pageOpts store.ReadPageOptions,
	offset store.Offset,
	cursor string,
	useBase64 bool,
	lease *sseHubLease,
) (returnErr error) {
	streamStarted := false
	defer func() {
		if streamStarted && returnErr != nil {
			h.logger().Warn(
				"aborting committed SSE connection after streaming error",
				"path", path,
				"error", returnErr,
			)
			returnErr = http.ErrAbortHandler
		}
	}()

	ctx := r.Context()

	// Registration was acknowledged before the first atomic page. Bind that
	// page's incarnation and tail to the provisional hub. The first confirmed
	// notification generation becomes ready from this authoritative page; a
	// later notification or final attach confirmation covers subsequent change.
	if err := lease.hub.initialize(first.Snapshot, useBase64); err != nil {
		lease = h.replaceSSEHubLease(lease, first.Snapshot, useBase64)
		if err := lease.waitRegistered(ctx); err != nil {
			lease.close()
			return err
		}
	}
	defer lease.close()
	if err := lease.waitReady(ctx); err != nil {
		return err
	}

	snapshot := first.Snapshot
	var err error
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
	if err := withSSEWriteDeadline(w, writeTimeout, func(controller *http.ResponseController) error {
		w.WriteHeader(http.StatusOK)
		streamStarted = true
		return controller.Flush()
	}); err != nil {
		return h.recordSSEWriteError(err)
	}

	counted := &sseCountingResponseWriter{ResponseWriter: w}
	frameWriter := &sseClientFrameWriter{response: counted, writeTimeout: writeTimeout}
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
			h.sseMetrics().SSEReason("protocol_reconnect")
			return nil
		default:
		}

		if err := lease.validateSnapshot(snapshot); err != nil {
			return err
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			h.observeReadPage(page)
			h.sseMetrics().SSEPage("catchup", page.Stats.ReturnedBytes)
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
		if len(page.Messages) == 0 && page.UpToDate && !snapshot.Closed &&
			lease.progressedBeyond(page.NextOffset, snapshot.Closed) {
			// The hub subscribed before this authoritative snapshot, so an append
			// after it is already covered by the notification feed. Do not emit an
			// empty checkpoint ahead of that data.
			h.observeReadPage(page)
			h.sseMetrics().SSEPage("catchup", page.Stats.ReturnedBytes)
			currentOffset = page.NextOffset
			break
		}
		var data []byte
		if len(page.Messages) > 0 {
			data, err = h.formatSSEDataEvent(path, page.Messages, snapshot.ContentType, useBase64)
			if err != nil {
				return err
			}
		}
		closed := snapshot.Closed && page.UpToDate
		if err := frameWriter.write(
			data,
			page.NextOffset,
			protocol.GenerateResponseCursor(cursor, time.Now()),
			page.UpToDate,
			closed,
		); err != nil {
			h.observeReadCancellation("write")
			return h.recordSSEWriteError(err)
		}

		// Advance only after the page's data and control event have both been
		// flushed successfully. This is the only safe live-attach cursor.
		currentOffset = page.NextOffset
		responsePages++
		h.observeReadPage(page)
		h.sseMetrics().SSEPage("catchup", page.Stats.ReturnedBytes)
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

	// Confirm the durable incarnation immediately before live attach. The tail
	// may advance, but the path must still name the captured stream. This is a
	// bounded PageReader call and deliberately does not renew access a second
	// time for the same request.
	if err := lease.validateSnapshot(snapshot); err != nil {
		return err
	}
	confirmation, err := lease.hub.confirmSnapshot(ctx, reader, path)
	if err != nil {
		return err
	}
	if !store.SameReadStream(snapshot, confirmation) {
		return store.ErrReadSnapshotChanged
	}
	releaseReadSnapshot(reader, path, snapshot)
	snapshotReleased = true
	if err := lease.watcher.waitAttach(ctx, currentOffset); err != nil {
		if errors.Is(err, errSSEHubLagged) {
			h.logger().Warn(
				"disconnecting SSE client whose attach boundary left the replay window",
				"path", path,
				"offset", currentOffset.String(),
			)
			return nil
		}
		return err
	}

	// Live delivery uses one shared bounded durable read and one shared
	// formatted data event. A lagged client disconnects and resumes from the
	// last control offset that was flushed above.
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reconnectTimer.C:
			h.sseMetrics().SSEReason("protocol_reconnect")
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
			if err := frameWriter.write(
				event.data,
				event.to,
				protocol.GenerateResponseCursor(cursor, time.Now()),
				event.upToDate,
				event.closed,
			); err != nil {
				return h.recordSSEWriteError(err)
			}
			lease.watcher.commit(event)
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
			h.sseMetrics().SSEReason("protocol_reconnect")
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
		h.sseMetrics().SSEReason("write_timeout")
	}
	return err
}
