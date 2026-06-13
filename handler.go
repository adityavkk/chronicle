// Package chronicle implements the Durable Streams protocol HTTP layer.
//
// Ported from the Durable Streams reference Caddy plugin
// (packages/caddy-plugin/handler.go @ 82f9963). Deviations from upstream:
// ServeHTTP is a stdlib http.Handler (no caddyhttp `next` middleware
// argument), logging is log/slog instead of zap, and the parsing/cursor
// helpers live in the pure protocol package. The reserved __ds subscription
// routes and the stream lifecycle hooks dispatch through the optional
// Subscriptions/SubHooks fields (PROTOCOL §6), implemented by the webhook
// package.
package chronicle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Handler serves the Durable Streams protocol over HTTP.
type Handler struct {
	// Store is the stream storage backend.
	Store store.Store

	// LongPollTimeout is the default timeout for long-poll requests.
	LongPollTimeout time.Duration

	// SSEReconnectInterval is how often SSE connections should reconnect.
	SSEReconnectInterval time.Duration

	// Logger receives debug/error logs; nil falls back to slog.Default().
	Logger *slog.Logger

	// Subscriptions, when set, handles the reserved __ds subscription routes
	// before normal stream handling (PROTOCOL §6). Nil disables the layer.
	Subscriptions SubscriptionRouter

	// SubHooks, when set, receives stream lifecycle events so the subscription
	// layer can wake subscribers after a durable write. Nil disables the hooks.
	SubHooks SubscriptionHooks
}

func (h *Handler) onStreamCreated(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamCreated(path)
	}
}

func (h *Handler) onStreamAppend(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamAppend(path)
	}
}

func (h *Handler) onStreamDeleted(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamDeleted(path)
	}
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, If-None-Match, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset, Authorization")
	w.Header().Set("Access-Control-Expose-Headers", "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, ETag, Location, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq")

	// Browser security headers (Protocol Section 10.7)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")

	// Handle preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Reserved __ds subscription routes are handled before normal stream
	// handling, mirroring the Caddy plugin (PROTOCOL §6).
	if h.Subscriptions != nil && h.Subscriptions.HandleRequest(w, r) {
		return
	}

	// Extract stream path from URL
	streamPath := r.URL.Path

	h.logger().Debug("handling request",
		"method", r.Method,
		"path", streamPath,
		"query", r.URL.RawQuery)

	var err error
	switch r.Method {
	case http.MethodPut:
		err = h.handleCreate(w, r, streamPath)
	case http.MethodHead:
		err = h.handleHead(w, r, streamPath)
	case http.MethodGet:
		err = h.handleRead(w, r, streamPath)
	case http.MethodPost:
		err = h.handleAppend(w, r, streamPath)
	case http.MethodDelete:
		err = h.handleDelete(w, r, streamPath)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err != nil {
		h.writeError(w, err)
	}
}

// handleCreate handles PUT requests to create a stream
func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request, path string) error {
	// Parse headers
	contentType := r.Header.Get("Content-Type")
	ttlStr := r.Header.Get(protocol.HeaderStreamTTL)
	expiresAtStr := r.Header.Get(protocol.HeaderStreamExpiresAt)
	closedStr := r.Header.Get(protocol.HeaderStreamClosed)

	// Parse Stream-Closed header
	createClosed := closedStr == "true"

	// Parse fork headers
	forkedFromStr := r.Header.Get(protocol.HeaderStreamForkedFrom)
	forkOffsetStr := r.Header.Get(protocol.HeaderStreamForkOffset)
	// Use Values() to distinguish "header present but empty" from "absent"
	forkSubOffsetVals := r.Header.Values(protocol.HeaderStreamForkSubOffset)
	forkSubOffsetPresent := len(forkSubOffsetVals) > 0
	forkSubOffsetStr := ""
	if forkSubOffsetPresent {
		forkSubOffsetStr = forkSubOffsetVals[0]
	}

	// Validate TTL and ExpiresAt aren't both provided
	if ttlStr != "" && expiresAtStr != "" {
		return newHTTPError(http.StatusBadRequest, "cannot specify both Stream-TTL and Stream-Expires-At")
	}

	// Parse TTL
	var ttlSeconds *int64
	if ttlStr != "" {
		ttl, err := protocol.ParseTTL(ttlStr)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, err.Error())
		}
		ttlSeconds = &ttl
	}

	// Parse ExpiresAt
	var expiresAt *time.Time
	if expiresAtStr != "" {
		t, err := time.Parse(time.RFC3339, expiresAtStr)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid Stream-Expires-At format")
		}
		expiresAt = &t
	}

	// Read optional initial body
	var initialData []byte
	if r.ContentLength > 0 {
		var err error
		initialData, err = io.ReadAll(r.Body)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "failed to read body")
		}
	}

	opts := store.CreateOptions{
		ContentType: contentType,
		TTLSeconds:  ttlSeconds,
		ExpiresAt:   expiresAt,
		InitialData: initialData,
		Closed:      createClosed,
		ForkedFrom:  forkedFromStr,
	}

	// Parse fork offset if provided
	if forkOffsetStr != "" {
		forkOffset, err := store.ParseOffset(forkOffsetStr)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid Stream-Fork-Offset format")
		}
		opts.ForkOffset = &forkOffset
	}

	// Parse fork sub-offset if header was present (including empty value)
	if forkSubOffsetPresent {
		if forkedFromStr == "" {
			return newHTTPError(http.StatusBadRequest, "Stream-Fork-Sub-Offset requires Stream-Forked-From")
		}
		subOffset, err := protocol.ParseSubOffset(forkSubOffsetStr)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, err.Error())
		}
		opts.ForkSubOffset = &subOffset
	}

	meta, wasCreated, err := h.Store.Create(path, opts)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "source stream not found")
		}
		if errors.Is(err, store.ErrInvalidForkOffset) {
			return newHTTPError(http.StatusBadRequest, "fork offset beyond source stream length")
		}
		if errors.Is(err, store.ErrInvalidForkSubOffset) {
			return newHTTPError(http.StatusBadRequest, "fork sub-offset overshoots or is invalid")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusConflict, "source stream was deleted but still has active forks")
		}
		if errors.Is(err, store.ErrStreamExists) {
			return newHTTPError(http.StatusConflict, "stream already exists")
		}
		if errors.Is(err, store.ErrConfigMismatch) {
			return newHTTPError(http.StatusConflict, "stream exists with different configuration")
		}
		if errors.Is(err, store.ErrContentTypeMismatch) {
			return newHTTPError(http.StatusConflict, "fork content type does not match source stream")
		}
		return err
	}

	// Check for soft-deleted existing stream
	if meta != nil && meta.SoftDeleted {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("stream was deleted but still has active forks — path cannot be reused until all forks are removed"))
		return nil
	}

	// Notify the subscription layer of a new stream (and its initial append, if
	// any) so matching subscriptions are linked and woken.
	if wasCreated {
		h.onStreamCreated(path)
		if len(initialData) > 0 {
			h.onStreamAppend(path)
		}
	}

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, meta.CurrentOffset.String())

	// Include Stream-Closed header if stream is closed
	if meta.Closed {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	if wasCreated {
		// Build full URL for Location header
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		// Check X-Forwarded-Proto header (for reverse proxies)
		if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
			scheme = proto
		}
		// Get the host from the request, preferring X-Forwarded-Host for proxies
		host := r.Host
		if fwdHost := r.Header.Get("X-Forwarded-Host"); fwdHost != "" {
			host = fwdHost
		}
		fullURL := fmt.Sprintf("%s://%s%s", scheme, host, r.URL.Path)
		w.Header().Set("Location", fullURL)
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	return nil
}

// handleHead handles HEAD requests for stream metadata
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, path string) error {
	meta, err := h.Store.Get(path)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "stream not found")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusGone, "stream has been deleted")
		}
		return err
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, meta.CurrentOffset.String())
	w.Header().Set("Cache-Control", "no-store")

	if meta.TTLSeconds != nil {
		w.Header().Set(protocol.HeaderStreamTTL, strconv.FormatInt(*meta.TTLSeconds, 10))
	}
	if meta.ExpiresAt != nil {
		w.Header().Set(protocol.HeaderStreamExpiresAt, meta.ExpiresAt.Format(time.RFC3339))
	}

	// Include Stream-Closed header if stream is closed
	if meta.Closed {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

// handleRead handles GET requests to read from a stream
func (h *Handler) handleRead(w http.ResponseWriter, r *http.Request, path string) error {
	// Check if stream exists
	meta, err := h.Store.Get(path)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "stream not found")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusGone, "stream has been deleted")
		}
		return err
	}

	// Check for explicit empty offset parameter (different from missing offset)
	query := r.URL.Query()
	offsetValues, offsetProvided := query["offset"]
	offsetStr := ""
	if offsetProvided {
		if len(offsetValues) > 1 {
			return newHTTPError(http.StatusBadRequest, "multiple offset parameters not allowed")
		}
		offsetStr = offsetValues[0]
		// Reject empty offset string when explicitly provided
		if offsetStr == "" {
			return newHTTPError(http.StatusBadRequest, "offset parameter cannot be empty")
		}
	}

	// Parse offset
	offset, err := store.ParseOffset(offsetStr)
	if err != nil {
		return newHTTPError(http.StatusBadRequest, "invalid offset")
	}

	// Check for live mode
	liveMode := query.Get("live")
	cursor := query.Get("cursor")
	// Validate long-poll requires offset
	if liveMode == "long-poll" && !offsetProvided {
		return newHTTPError(http.StatusBadRequest, "offset required for long-poll mode")
	}

	// Validate SSE requires offset
	if liveMode == "sse" && !offsetProvided {
		return newHTTPError(http.StatusBadRequest, "offset required for SSE mode")
	}

	// Handle SSE mode first (before reading)
	if liveMode == "sse" {
		// Auto-detect binary content types for base64 encoding
		ct := strings.ToLower(store.ExtractMediaType(meta.ContentType))
		isTextCompatible := strings.HasPrefix(ct, "text/") || ct == "application/json"
		useBase64 := !isTextCompatible

		// For SSE with offset=now, convert to actual tail offset
		sseOffset := offset
		if offset.IsNow() {
			sseOffset = meta.CurrentOffset
		}
		return h.handleSSE(w, r, path, sseOffset, cursor, useBase64)
	}

	// For offset=now, convert to actual tail offset
	// This allows long-poll to immediately start waiting for new data
	effectiveOffset := offset
	isNowOffset := offset.IsNow()
	if isNowOffset {
		effectiveOffset = meta.CurrentOffset
	}

	// Handle catch-up mode offset=now: return empty response with tail offset
	// For long-poll mode, we fall through to wait for new data instead
	if isNowOffset && liveMode != "long-poll" {
		w.Header().Set("Content-Type", meta.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, meta.CurrentOffset.String())
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")

		// Include Stream-Closed if stream is closed (client at tail, upToDate)
		if meta.Closed {
			w.Header().Set(protocol.HeaderStreamClosed, "true")
		}

		// Prevent caching - tail offset changes with each append
		w.Header().Set("Cache-Control", "no-store")

		// No ETag for offset=now responses - Cache-Control: no-store makes ETag unnecessary
		// and some CDNs may behave unexpectedly with both headers

		// For JSON mode, return empty array; otherwise empty body
		if store.IsJSONContentType(meta.ContentType) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		return nil
	}

	// Read messages
	messages, _, err := h.Store.Read(path, effectiveOffset)
	if err != nil {
		return err
	}

	// Calculate next offset
	nextOffset := effectiveOffset
	if len(messages) > 0 {
		nextOffset = messages[len(messages)-1].Offset
	} else {
		// No new messages, use current offset from metadata
		nextOffset = meta.CurrentOffset
	}

	// Handle long-poll mode - wait if no messages and either:
	// 1. Client used offset=now (wants to wait for future data)
	// 2. Client is caught up (at the tail)
	shouldWait := liveMode == "long-poll" && len(messages) == 0 && (isNowOffset || effectiveOffset.Equal(meta.CurrentOffset))
	if shouldWait {
		// If stream is closed and client is at tail, return immediately (don't wait)
		if meta.Closed {
			w.Header().Set("Content-Type", meta.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, meta.CurrentOffset.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.Header().Set(protocol.HeaderStreamClosed, "true")
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
			w.WriteHeader(http.StatusNoContent)
			return nil
		}

		// Client is caught up, wait for new data
		timeout := h.LongPollTimeout
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		var timedOut bool
		var streamClosed bool
		messages, timedOut, streamClosed, err = h.Store.WaitForMessages(ctx, path, effectiveOffset, timeout)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Timeout or client disconnect - return 204 with current offset
				w.Header().Set("Content-Type", meta.ContentType)
				w.Header().Set(protocol.HeaderStreamNextOffset, effectiveOffset.String())
				w.Header().Set(protocol.HeaderStreamUpToDate, "true")
				w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
				// Check if stream was closed during wait
				currentMeta, _ := h.Store.Get(path)
				if currentMeta != nil && currentMeta.Closed {
					w.Header().Set(protocol.HeaderStreamClosed, "true")
				}
				w.WriteHeader(http.StatusNoContent)
				return nil
			}
			return err
		}

		// If stream was closed during wait, return immediately with Stream-Closed
		if streamClosed {
			w.Header().Set("Content-Type", meta.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, effectiveOffset.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.Header().Set(protocol.HeaderStreamClosed, "true")
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
			w.WriteHeader(http.StatusNoContent)
			return nil
		}

		if timedOut {
			// Timeout - return 204 with current offset
			w.Header().Set("Content-Type", meta.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, effectiveOffset.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
			// Check if stream was closed during timeout
			currentMeta, _ := h.Store.Get(path)
			if currentMeta != nil && currentMeta.Closed {
				w.Header().Set(protocol.HeaderStreamClosed, "true")
			}
			w.WriteHeader(http.StatusNoContent)
			return nil
		}

		// Got new messages - update nextOffset
		if len(messages) > 0 {
			nextOffset = messages[len(messages)-1].Offset
		}
	}

	// Determine if we're up to date (at the tail of the stream)
	// Re-fetch current offset to check if we're at the tail
	currentMeta, _ := h.Store.Get(path)
	upToDate := nextOffset.Equal(currentMeta.CurrentOffset)

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())

	// Always set Stream-Up-To-Date when at tail
	if upToDate {
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
	}

	// Include Stream-Closed when stream is closed AND client is at tail AND upToDate
	if currentMeta.Closed && upToDate {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	// Generate Stream-Cursor for long-poll responses (CDN cache collision prevention)
	if liveMode == "long-poll" {
		responseCursor := protocol.GenerateResponseCursor(cursor, time.Now())
		w.Header().Set(protocol.HeaderStreamCursor, responseCursor)
	}

	// Set ETag for caching
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, nextOffset.String()))

	// Set caching headers for historical reads
	if !upToDate && len(messages) > 0 {
		w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
	}

	// Check If-None-Match for 304
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		expectedETag := fmt.Sprintf(`"%s"`, nextOffset.String())
		if ifNoneMatch == expectedETag {
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
	}

	// Format and write response
	body, err := h.formatResponse(path, messages, meta.ContentType)
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusOK)
	w.Write(body)
	return nil
}

// handleAppend handles POST requests to append to a stream
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, path string) error {
	// Check if stream exists
	meta, err := h.Store.Get(path)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "stream not found")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusGone, "stream has been deleted")
		}
		return err
	}

	// Parse Stream-Closed header
	closedStr := r.Header.Get(protocol.HeaderStreamClosed)
	closeStream := closedStr == "true"

	// Check for Content-Type header
	contentType := r.Header.Get("Content-Type")

	// Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return newHTTPError(http.StatusBadRequest, "failed to read body")
	}

	// Extract producer headers early (used for close-only and append)
	producerId := r.Header.Get(protocol.HeaderProducerId)
	producerEpochStr := r.Header.Get(protocol.HeaderProducerEpoch)
	producerSeqStr := r.Header.Get(protocol.HeaderProducerSeq)

	hasProducerHeaders := producerId != "" || producerEpochStr != "" || producerSeqStr != ""
	hasAllProducerHeaders := producerId != "" && producerEpochStr != "" && producerSeqStr != ""

	// Validate producer headers - all or none
	if hasProducerHeaders && !hasAllProducerHeaders {
		return newHTTPError(http.StatusBadRequest, "all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together")
	}

	var producerEpoch *int64
	var producerSeq *int64
	if hasAllProducerHeaders {
		// Validate Producer-Epoch
		if !protocol.IsValidIntegerString(producerEpochStr) {
			return newHTTPError(http.StatusBadRequest, "invalid Producer-Epoch: must be an integer")
		}
		epoch, err := strconv.ParseInt(producerEpochStr, 10, 64)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid Producer-Epoch: must be an integer")
		}
		producerEpoch = &epoch

		// Validate Producer-Seq
		if !protocol.IsValidIntegerString(producerSeqStr) {
			return newHTTPError(http.StatusBadRequest, "invalid Producer-Seq: must be an integer")
		}
		seq, err := strconv.ParseInt(producerSeqStr, 10, 64)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid Producer-Seq: must be an integer")
		}
		producerSeq = &seq
	}

	// Handle close-only request (empty body with Stream-Closed: true)
	if len(body) == 0 && closeStream {
		// Close-only - Content-Type validation is skipped per protocol Section 5.2
		if hasAllProducerHeaders {
			result, err := h.Store.CloseStreamWithProducer(path, store.CloseProducerOptions{
				ProducerId:    producerId,
				ProducerEpoch: *producerEpoch,
				ProducerSeq:   *producerSeq,
			})
			if err != nil {
				if errors.Is(err, store.ErrStreamNotFound) {
					return newHTTPError(http.StatusNotFound, "stream not found")
				}
				if errors.Is(err, store.ErrStaleEpoch) {
					w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(result.CurrentEpoch, 10))
					http.Error(w, "producer epoch is stale", http.StatusForbidden)
					return nil
				}
				if errors.Is(err, store.ErrInvalidEpochSeq) {
					return newHTTPError(http.StatusBadRequest, "new epoch must start at sequence 0")
				}
				if errors.Is(err, store.ErrProducerSeqGap) {
					w.Header().Set(protocol.HeaderProducerExpectedSeq, strconv.FormatInt(result.ExpectedSeq, 10))
					w.Header().Set(protocol.HeaderProducerReceivedSeq, strconv.FormatInt(result.ReceivedSeq, 10))
					http.Error(w, "producer sequence gap detected", http.StatusConflict)
					return nil
				}
				if errors.Is(err, store.ErrStreamClosed) {
					w.Header().Set(protocol.HeaderStreamClosed, "true")
					http.Error(w, "stream is closed", http.StatusConflict)
					return nil
				}
				return err
			}

			w.Header().Set(protocol.HeaderStreamNextOffset, result.FinalOffset.String())
			w.Header().Set(protocol.HeaderStreamClosed, "true")
			w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(*producerEpoch, 10))
			w.Header().Set(protocol.HeaderProducerSeq, strconv.FormatInt(result.LastSeq, 10))
			w.WriteHeader(http.StatusNoContent)
			return nil
		}

		result, err := h.Store.CloseStream(path)
		if err != nil {
			if errors.Is(err, store.ErrStreamNotFound) {
				return newHTTPError(http.StatusNotFound, "stream not found")
			}
			return err
		}

		w.Header().Set(protocol.HeaderStreamNextOffset, result.FinalOffset.String())
		w.Header().Set(protocol.HeaderStreamClosed, "true")
		w.WriteHeader(http.StatusNoContent)
		return nil
	}

	// Empty body without Stream-Closed is an error
	if len(body) == 0 {
		return newHTTPError(http.StatusBadRequest, "empty body not allowed")
	}

	// Content-Type is required for requests with body
	if contentType == "" {
		return newHTTPError(http.StatusBadRequest, "Content-Type header is required")
	}

	// Check if content type matches stream (must validate before processing)
	if !store.ContentTypeMatches(meta.ContentType, contentType) {
		return newHTTPError(http.StatusConflict, "content type mismatch")
	}

	opts := store.AppendOptions{
		Seq:         r.Header.Get(protocol.HeaderStreamSeq),
		ContentType: contentType,
		Close:       closeStream,
	}

	if hasAllProducerHeaders {
		opts.ProducerId = producerId
		opts.ProducerEpoch = producerEpoch
		opts.ProducerSeq = producerSeq
	}

	result, err := h.Store.Append(path, body, opts)
	if err != nil {
		if errors.Is(err, store.ErrStreamClosed) {
			w.Header().Set(protocol.HeaderStreamClosed, "true")
			w.Header().Set(protocol.HeaderStreamNextOffset, result.Offset.String())
			http.Error(w, "stream is closed", http.StatusConflict)
			return nil
		}
		if errors.Is(err, store.ErrSequenceConflict) {
			return newHTTPError(http.StatusConflict, "sequence number conflict")
		}
		if errors.Is(err, store.ErrContentTypeMismatch) {
			return newHTTPError(http.StatusConflict, "content type mismatch")
		}
		if errors.Is(err, store.ErrInvalidJSON) {
			return newHTTPError(http.StatusBadRequest, "invalid JSON")
		}
		if errors.Is(err, store.ErrEmptyJSONArray) {
			return newHTTPError(http.StatusBadRequest, "empty JSON array not allowed")
		}
		if errors.Is(err, store.ErrPartialProducer) {
			return newHTTPError(http.StatusBadRequest, "all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together")
		}
		if errors.Is(err, store.ErrStaleEpoch) {
			// 403 Forbidden - stale epoch (zombie fencing)
			w.Header().Set(protocol.HeaderStreamNextOffset, result.Offset.String())
			w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(result.CurrentEpoch, 10))
			http.Error(w, "producer epoch is stale", http.StatusForbidden)
			return nil
		}
		if errors.Is(err, store.ErrInvalidEpochSeq) {
			return newHTTPError(http.StatusBadRequest, "new epoch must start at sequence 0")
		}
		if errors.Is(err, store.ErrProducerSeqGap) {
			// 409 Conflict - sequence gap
			w.Header().Set(protocol.HeaderStreamNextOffset, result.Offset.String())
			w.Header().Set(protocol.HeaderProducerExpectedSeq, strconv.FormatInt(result.ExpectedSeq, 10))
			w.Header().Set(protocol.HeaderProducerReceivedSeq, strconv.FormatInt(result.ReceivedSeq, 10))
			http.Error(w, "producer sequence gap detected", http.StatusConflict)
			return nil
		}
		return err
	}

	// Wake subscribers off the durable append (best-effort; the recovery sweep
	// is the backstop if this is lost to a crash).
	h.onStreamAppend(path)

	w.Header().Set(protocol.HeaderStreamNextOffset, result.Offset.String())

	// Include Stream-Closed header if stream was closed
	if result.StreamClosed {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	// Echo Producer-Epoch and Producer-Seq on success when producer headers were provided
	if opts.ProducerEpoch != nil {
		w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(*opts.ProducerEpoch, 10))
		// Return highest accepted seq (per PROTOCOL.md)
		w.Header().Set(protocol.HeaderProducerSeq, strconv.FormatInt(result.LastSeq, 10))
	}

	// Handle duplicate detection (204 No Content)
	if result.ProducerResult == store.ProducerResultDuplicate {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}

	// For non-producer appends, return 204 No Content
	// For producer appends (new writes), return 200 OK to distinguish from duplicates
	if opts.ProducerId != "" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
	return nil
}

// handleDelete handles DELETE requests to delete a stream
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, path string) error {
	err := h.Store.Delete(path)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "stream not found")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusGone, "stream has been deleted")
		}
		return err
	}

	h.onStreamDeleted(path)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// formatResponse formats messages based on content type
func (h *Handler) formatResponse(path string, messages []store.Message, contentType string) ([]byte, error) {
	if store.IsJSONContentType(contentType) {
		return store.FormatJSONResponse(messages), nil
	}

	// Non-JSON: concatenate raw data
	var total int
	for _, msg := range messages {
		total += len(msg.Data)
	}
	result := make([]byte, 0, total)
	for _, msg := range messages {
		result = append(result, msg.Data...)
	}
	return result, nil
}

// HTTP error handling
type httpError struct {
	status  int
	message string
}

func (e *httpError) Error() string {
	return e.message
}

func newHTTPError(status int, message string) *httpError {
	return &httpError{status: status, message: message}
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	var httpErr *httpError
	if errors.As(err, &httpErr) {
		http.Error(w, httpErr.message, httpErr.status)
		return
	}

	h.logger().Error("internal error", "error", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}
