// Server-Sent Events streaming, ported from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go handleSSE @ 82f9963).
package chronicle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// sseLineTerminators matches all valid SSE line terminators: CRLF, CR, or LF
// Per SSE spec, these are all valid line terminators that could be used for injection attacks
var sseLineTerminators = regexp.MustCompile(`\r\n|\r|\n`)

// handleSSE handles Server-Sent Events streaming
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, path string, offset store.Offset, cursor string, useBase64 bool) error {
	meta, err := h.Store.Get(path)
	if err != nil {
		return err
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Add encoding header when base64 encoding is used for binary streams
	if useBase64 {
		w.Header().Set(protocol.HeaderStreamSSEDataEncoding, "base64")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return newHTTPError(http.StatusInternalServerError, "streaming not supported")
	}

	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	reconnectTimer := time.NewTimer(h.SSEReconnectInterval)
	defer reconnectTimer.Stop()

	currentOffset := offset
	sentInitialControl := false

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reconnectTimer.C:
			// Close connection to allow CDN collapsing
			return nil
		default:
			// Read any available messages
			messages, upToDate, err := h.Store.Read(path, currentOffset)
			if err != nil {
				return err
			}

			// Re-fetch current metadata to check closed state
			currentMeta, _ := h.Store.Get(path)
			streamIsClosed := currentMeta != nil && currentMeta.Closed

			if len(messages) > 0 {
				// Send data event
				body, _ := h.formatResponse(path, messages, meta.ContentType)
				fmt.Fprintf(w, "event: data\n")

				if useBase64 {
					// Base64 encode the binary data for SSE delivery (Protocol Section 5.7)
					encoded := base64.StdEncoding.EncodeToString(body)
					fmt.Fprintf(w, "data:%s\n", encoded)
				} else {
					// Split on all SSE-valid line terminators (CRLF, CR, LF) to prevent injection
					// Note: Per SSE spec, we don't add a space after "data:" because clients
					// strip exactly one leading space. Adding one would cause data starting
					// with spaces to lose an extra space character.
					for _, line := range sseLineTerminators.Split(string(body), -1) {
						fmt.Fprintf(w, "data:%s\n", line)
					}
				}
				fmt.Fprintf(w, "\n")

				// Update current offset
				currentOffset = messages[len(messages)-1].Offset

				// Check if client is now at tail of closed stream
				clientAtTail := currentMeta != nil && currentOffset.Equal(currentMeta.CurrentOffset)

				// Build control event
				control := map[string]any{
					"streamNextOffset": currentOffset.String(),
				}

				if streamIsClosed && clientAtTail {
					// Final control event - stream is closed
					// streamCursor is omitted when streamClosed is true per protocol
					// upToDate is implied by streamClosed per protocol
					control["streamClosed"] = true
				} else {
					// Normal control event - include cursor
					control["streamCursor"] = protocol.GenerateResponseCursor(cursor, time.Now())
					if upToDate {
						control["upToDate"] = true
					}
				}

				controlJSON, _ := json.Marshal(control)
				fmt.Fprintf(w, "event: control\n")
				fmt.Fprintf(w, "data:%s\n\n", controlJSON)

				flusher.Flush()
				sentInitialControl = true

				// Close SSE connection after sending streamClosed
				if streamIsClosed && clientAtTail {
					return nil
				}
			} else if !sentInitialControl {
				// Send initial control event even for empty stream
				// Check if stream is already closed and client is at tail
				clientAtTail := currentMeta != nil && offset.Equal(currentMeta.CurrentOffset)

				control := map[string]any{
					"streamNextOffset": currentMeta.CurrentOffset.String(),
				}

				if streamIsClosed && clientAtTail {
					// Stream already closed at tail - send final control and exit
					control["streamClosed"] = true
				} else {
					// Normal initial control
					control["streamCursor"] = protocol.GenerateResponseCursor(cursor, time.Now())
					control["upToDate"] = true
				}

				controlJSON, _ := json.Marshal(control)
				fmt.Fprintf(w, "event: control\n")
				fmt.Fprintf(w, "data:%s\n\n", controlJSON)

				flusher.Flush()
				sentInitialControl = true

				// Close connection if stream is closed
				if streamIsClosed && clientAtTail {
					return nil
				}
			} else if streamIsClosed {
				// Initial control was already sent and the stream has since been
				// closed with no further data to deliver (e.g. a close-only
				// request). Emit the final control event with streamClosed and
				// close the connection. (Data appended atomically with a close is
				// handled by the len(messages) > 0 branch above on this same
				// iteration.)
				clientAtTail := currentMeta != nil && currentOffset.Equal(currentMeta.CurrentOffset)
				if clientAtTail {
					control := map[string]any{
						"streamNextOffset": currentOffset.String(),
						"streamClosed":     true,
					}
					controlJSON, _ := json.Marshal(control)
					fmt.Fprintf(w, "event: control\n")
					fmt.Fprintf(w, "data:%s\n\n", controlJSON)
					flusher.Flush()
					return nil
				}
			}

			// Wait for more data or stream closure, then loop back to the top
			// of the loop. We deliberately do NOT emit the closing control event
			// here: if the stream was closed with a final append, that data must
			// be drained by the Read at the top of the next iteration and sent as
			// a data event before the closing control event. Emitting it here
			// (with the stale currentOffset) would silently drop the final append
			// for a live reader that was caught up at the tail.
			timeout := 100 * time.Millisecond
			waitCtx, cancel := context.WithTimeout(ctx, timeout)
			h.Store.WaitForMessages(waitCtx, path, currentOffset, timeout) //nolint:errcheck // wake-or-timeout only; the next iteration re-reads
			cancel()
		}
	}
}
