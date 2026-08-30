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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// AppendMetrics records work between a durable append commit and the HTTP
// response. It lives in the HTTP package because only the handler can observe
// both boundaries exactly.
type AppendMetrics interface {
	AppendSubscriptionHook(dur time.Duration)
}

// FenceMetrics records data-plane write-fence rejections by reason (#183):
// credential, shard, producer_required, principal, wake_token, precheck,
// marker, sealed, epoch, bound, or store. metrics.Prometheus implements it.
type FenceMetrics interface {
	AppendFenceRejection(reason string)
}

// Handler serves the Durable Streams protocol over HTTP.
type Handler struct {
	// Store is the stream storage backend.
	Store store.Store

	// LongPollTimeout is the default timeout for long-poll requests.
	LongPollTimeout time.Duration

	// SSEReconnectInterval is how often SSE connections should reconnect.
	SSEReconnectInterval time.Duration

	// ReadPageBytes is the returned page payload target for HTTP and SSE catch-up.
	// Zero uses store.DefaultReadPageBytes.
	ReadPageBytes int

	// ReadMetrics receives bounded catch-up observations. Nil disables them.
	ReadMetrics ReadMetrics

	// SSEHubReplayBytes bounds the shared replay window retained for each active
	// stream. A slow client that falls behind this window reconnects from its
	// last durable offset. Zero uses the 1 MiB default.
	SSEHubReplayBytes int

	// SSEHubBatchBytes bounds each shared live SSE data batch by formatted wire
	// plus boundary-index bytes when message boundaries permit. One oversized
	// message remains whole. Zero uses the 256 KiB default.
	SSEHubBatchBytes int

	// SSEHubPollInterval is the durable fallback cadence when a Redis
	// notification is lost. Zero uses one second.
	SSEHubPollInterval time.Duration

	// SSEClientWriteTimeout bounds one data-and-control flush to a client.
	// Zero uses the 10 second default.
	SSEClientWriteTimeout time.Duration

	// SSEMetrics records shared fanout state. Nil records nothing.
	SSEMetrics SSEMetrics

	sseHubs sseHubRegistry

	// Logger receives debug/error logs; nil falls back to slog.Default().
	Logger *slog.Logger

	// Subscriptions, when set, handles the reserved __ds subscription routes
	// before normal stream handling (PROTOCOL §6). Nil disables the layer.
	Subscriptions SubscriptionRouter

	// SubHooks, when set, receives stream lifecycle events so the subscription
	// layer can wake subscribers after a durable write. Nil disables the hooks.
	SubHooks SubscriptionHooks

	// AppendMetrics receives the end-to-end synchronous subscription-hook time
	// after a committed append. Nil disables it.
	AppendMetrics AppendMetrics

	// AuthMode selects authorization enforcement (issue #126). The zero value
	// ModeInsecure evaluates decisions for telemetry only, so a base-protocol
	// client is never broken by default; ModeEnforce fails closed with a
	// 401/403 before any store access.
	AuthMode auth.Mode

	// AppendAuth authorizes data-plane appends with the claim-scoped write
	// token (issue #126). Nil means no authorizer is available, which in
	// ModeEnforce denies every append (fail closed) and in ModeInsecure only
	// logs.
	AppendAuth AppendAuthorizer

	// ServiceAuth authenticates SPIFFE or compatibility-bearer service
	// principals and evaluates their explicit action and namespace policy.
	// Nil disables service authentication.
	ServiceAuth *ServiceAuth
	// ServiceMetrics records service authentication, authorization, and
	// delegated-gateway outcomes. Nil disables these counters.
	ServiceMetrics ServiceMetrics
	// FenceMetrics records write-fence rejections on the append path, exactly
	// once per rejected request (#183). Nil disables the counter.
	FenceMetrics FenceMetrics

	// ReadAuth authorizes data-plane reads with the chronicle
	// read-capability JWS (issue #126 TB5). Nil means no capability
	// verifier is available: in ModeEnforce a read without another accepted
	// credential is denied (fail closed).
	ReadAuth ReadAuthorizer

	// CallerAuth authorizes namespace-scoped creates/deletes with the
	// chronicle caller token (issue #126 TB5, completing the data-plane
	// action matrix). Nil fails closed the same way.
	CallerAuth CallerAuthorizer

	// UserAuth verifies IdP (PingFed) access tokens into user principals
	// (issue #126 TB5, the multi-issuer widening). Nil disables the OIDC
	// issuer route.
	UserAuth *OIDCUserAuth

	// EntityAuth authorizes a woken entity acting as itself with its
	// wake_token (issue #126 TB6b): reads and appends within its own entity
	// subtree, never creates or deletes. Nil fails closed for wake-typed
	// bearers.
	EntityAuth EntityAuthorizer
}

// subStreamPath maps a store path ("/events/abc") to the stream-root-relative
// path the subscription layer and protocol wire use ("events/abc"): the store
// keys paths with a leading slash (Mount keeps it when stripping the root), but
// PROTOCOL §6 paths have no leading slash.
func subStreamPath(path string) string { return strings.TrimPrefix(path, "/") }

func (h *Handler) onStreamCreated(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamCreated(subStreamPath(path))
	}
}

func (h *Handler) onStreamAppend(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamAppend(subStreamPath(path))
	}
}

func (h *Handler) observeAppendSubscriptionHook(start time.Time) {
	if h.SubHooks != nil && h.AppendMetrics != nil {
		h.AppendMetrics.AppendSubscriptionHook(time.Since(start))
	}
}

func (h *Handler) onStreamDeleted(path string) {
	if h.SubHooks != nil {
		h.SubHooks.OnStreamDeleted(subStreamPath(path))
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
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, If-None-Match, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset, Authorization, electric-claim-token, Write-Fence, Write-Token")
	w.Header().Set("Access-Control-Expose-Headers", "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, Stream-Envelope, ETag, Location, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, Write-Fence, Write-Fence-Sealed-Generation, Write-Fence-Sealed-Offset")

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

	// The __ds namespace is reserved. With the subscription layer disabled
	// these paths are unimplemented rather than application streams, so they
	// must not reach the store (PROTOCOL §6).
	if p := r.URL.Path; p == "/__ds" || strings.HasPrefix(p, "/__ds/") {
		http.Error(w, "subscription APIs are not implemented", http.StatusNotImplemented)
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
		if errors.Is(err, http.ErrAbortHandler) {
			// The response was already committed. Preserve net/http's abort
			// semantics so no HTTP error payload is appended to an SSE stream.
			panic(http.ErrAbortHandler)
		}
		h.writeError(w, err)
	}
}

// handleCreate handles PUT requests to create a stream
func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request, path string) error {
	// The create gate runs before any parsing or store access (issue #126
	// TB5): a denied create neither reads nor writes stream state.
	if err := h.authorizeMutate(r, path, auth.ActionCreate); err != nil {
		return err
	}

	// Parse headers
	contentType := r.Header.Get("Content-Type")
	ttlStr := r.Header.Get(protocol.HeaderStreamTTL)
	expiresAtStr := r.Header.Get(protocol.HeaderStreamExpiresAt)
	closedStr := r.Header.Get(protocol.HeaderStreamClosed)

	// Parse Stream-Closed header
	createClosed := closedStr == "true"

	// Write-Fence: true creates the stream write-fenced (#183). The fence lives
	// in the store's stream slot, so a store without the capability cannot
	// honor the opt-in; refusing here keeps a base client's create unchanged.
	writeFence := r.Header.Get(protocol.HeaderWriteFence) == "true"
	if writeFence {
		if _, ok := h.Store.(store.WriteFenceStore); !ok {
			return newHTTPError(http.StatusNotImplemented, store.ErrWriteFenceUnsupported.Error())
		}
	}

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

	if forkedFromStr != "" {
		sourcePath, err := auth.NormalizeStreamPath(forkedFromStr)
		if err != nil {
			return newHTTPError(http.StatusBadRequest, "invalid Stream-Forked-From path")
		}
		// The store uses root-relative paths with one leading slash. Rebuild that
		// canonical spelling from the same normalized value the read decision
		// evaluates, so authorization and dereference cannot name different keys.
		forkedFromStr = "/" + sourcePath.String()
		if _, err := h.authorizeRead(r, forkedFromStr); err != nil {
			return err
		}
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
		WriteFence:  writeFence,
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
	createReturnedAt := time.Now()
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
		if errors.Is(err, store.ErrWriteFenceUnsupported) {
			return newHTTPError(http.StatusNotImplemented, err.Error())
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
		if opts.WriteFence && h.AppendAuth == nil {
			// The fenced class needs the write token; without an authorizer every
			// fenced write fails closed (design K.16), which is worth saying once.
			h.logger().Warn("write-fenced stream created with no append authorizer configured", "path", path)
		}
		h.onStreamCreated(path)
		if len(initialData) > 0 {
			h.onStreamAppend(path)
			h.observeAppendSubscriptionHook(createReturnedAt)
		}
	}

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, meta.CurrentOffset.String())

	// Include Stream-Closed header if stream is closed
	if meta.Closed {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}
	if meta.WriteFence {
		w.Header().Set(protocol.HeaderWriteFence, "true")
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
	// The read gate runs before the store lookup: a 401 must not reveal
	// whether the stream exists (issue #126 TB5, §12.2). HEAD responses are
	// already no-store, so the private posture needs no extra handling here.
	if _, err := h.authorizeRead(r, path); err != nil {
		return err
	}

	snapshot, err := h.captureReadSnapshot(r.Context(), path, true)
	if err != nil {
		if errors.Is(err, store.ErrStreamNotFound) {
			return newHTTPError(http.StatusNotFound, "stream not found")
		}
		if errors.Is(err, store.ErrStreamSoftDeleted) {
			return newHTTPError(http.StatusGone, "stream has been deleted")
		}
		return err
	}

	w.Header().Set("Content-Type", snapshot.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, snapshot.Tail.String())
	w.Header().Set("Cache-Control", "no-store")

	if snapshot.TTLSeconds != nil {
		w.Header().Set(protocol.HeaderStreamTTL, strconv.FormatInt(*snapshot.TTLSeconds, 10))
	}
	if snapshot.ExpiresAt != nil {
		w.Header().Set(protocol.HeaderStreamExpiresAt, snapshot.ExpiresAt.Format(time.RFC3339))
	}

	// Include Stream-Closed header if stream is closed
	if snapshot.Closed {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	// Write-fence echo and the most recent seal (#183 B.2): a successor reads
	// the predecessor's definite last fenced offset from HEAD.
	if snapshot.WriteFence {
		w.Header().Set(protocol.HeaderWriteFence, "true")
		if snapshot.SealedGeneration > 0 && snapshot.SealedOffset != nil {
			w.Header().Set(protocol.HeaderWriteFenceSealedGeneration, strconv.FormatInt(snapshot.SealedGeneration, 10))
			w.Header().Set(protocol.HeaderWriteFenceSealedOffset, snapshot.SealedOffset.String())
		}
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

// handleRead handles GET requests to read from a stream
func (h *Handler) handleRead(w http.ResponseWriter, r *http.Request, path string) error {
	// The read gate runs before the store lookup (401 never leaks stream
	// existence, §12.2) and covers every read mode — plain, long-poll, and
	// SSE all dispatch below this point. authorizedPrivate is the Q3 cache
	// posture: a credentialed read is answered Cache-Control: private with
	// no ETag, so no shared cache can serve one principal's bytes to
	// another; uncredentialed reads (the insecure default for base clients)
	// keep today's headers byte for byte.
	authorizedPrivate, err := h.authorizeRead(r, path)
	if err != nil {
		return err
	}

	// Check for explicit empty offset parameter (different from missing offset)
	query := r.URL.Query()
	offsetValues, offsetProvided := query["offset"]
	offsetStr := ""
	if offsetProvided {
		if len(offsetValues) > 1 {
			return h.readValidationError(r.Context(), path, "multiple offset parameters not allowed")
		}
		offsetStr = offsetValues[0]
		// Reject empty offset string when explicitly provided
		if offsetStr == "" {
			return h.readValidationError(r.Context(), path, "offset parameter cannot be empty")
		}
	}

	// Parse offset
	offset, err := store.ParseOffset(offsetStr)
	if err != nil {
		return h.readValidationError(r.Context(), path, "invalid offset")
	}

	// Optional read batch limit — the concrete lever behind §5.6's
	// "server-defined maximum chunk size". A positive ?limit caps a catch-up
	// read to N messages (framed/JSON streams only), up to the storage page
	// frame cap; the response then omits
	// Stream-Up-To-Date and its Stream-Next-Offset lands mid-stream, so a
	// client pages forward by re-reading from that cursor. Absent, empty, or
	// non-positive means unlimited (the historical behaviour), so no existing
	// client or conformance test is affected — the cap is strictly opt-in.
	limit := 0
	if v := query.Get("limit"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			limit = n
		}
	}
	if limit > store.DefaultReadPageFrames {
		return h.readValidationError(
			r.Context(),
			path,
			fmt.Sprintf("limit cannot exceed %d", store.DefaultReadPageFrames),
		)
	}
	envelope := false
	if v := query.Get("envelope"); v == "1" || strings.EqualFold(v, "true") {
		envelope = true
	}

	// Check for live mode
	liveMode := query.Get("live")
	cursor := query.Get("cursor")
	// Validate long-poll requires offset
	if liveMode == "long-poll" && !offsetProvided {
		return h.readValidationError(r.Context(), path, "offset required for long-poll mode")
	}

	// Validate SSE requires offset
	if liveMode == "sse" && !offsetProvided {
		return h.readValidationError(r.Context(), path, "offset required for SSE mode")
	}

	isNowOffset := offset.IsNow()

	reader := h.pageReader()
	var sseLease *sseHubLease
	if liveMode == "sse" {
		reader, err = h.ssePageReader()
		if err != nil {
			return err
		}
		// Registration acknowledgement is the attach-race barrier. It must be
		// established before the first page captures an incarnation and tail.
		sseLease = h.acquireSSEHubRegistration(path)
		if err := sseLease.waitRegistered(r.Context()); err != nil {
			sseLease.close()
			return err
		}
	}
	pageOpts := store.ReadPageOptions{
		TargetBytes: h.readPageBytes(),
		MaxFrames:   store.DefaultReadPageFrames,
	}
	if limit > 0 && limit < pageOpts.MaxFrames {
		pageOpts.MaxFrames = limit
	}
	if err := r.Context().Err(); err != nil {
		if sseLease != nil {
			sseLease.close()
		}
		h.observeReadCancellation("before_first_page")
		return err
	}

	var liveSubscription store.NotificationSubscription
	if liveMode == "long-poll" && isNowOffset {
		// offset=now has no historical page to inspect. Confirm registration
		// before the authoritative first page and reuse it while blocked.
		liveSubscription, _, err = h.subscribeNotifications(r.Context(), path)
		if err != nil {
			return err
		}
	}
	defer func() {
		if liveSubscription != nil {
			_ = liveSubscription.Close()
		}
	}()
	ownsSSELease := sseLease != nil
	defer func() {
		if ownsSSELease {
			sseLease.close()
		}
	}()

	firstPage, err := reader.ReadPage(r.Context(), path, offset, pageOpts)
	if err != nil {
		if sseLease != nil {
			sseLease.close()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			h.observeReadCancellation("storage")
		}
		return readPageError(err)
	}
	ownsSnapshot := true
	defer func() {
		if ownsSnapshot {
			releaseReadSnapshot(reader, path, firstPage.Snapshot)
		}
	}()

	if liveMode == "sse" {
		ct := strings.ToLower(store.ExtractMediaType(firstPage.Snapshot.ContentType))
		useBase64 := !strings.HasPrefix(ct, "text/") && ct != "application/json"
		sseOffset := offset
		if sseOffset.IsNow() {
			sseOffset = firstPage.Snapshot.Tail
		}
		ownsSnapshot = false
		ownsSSELease = false
		return h.handleSSE(
			w,
			r,
			path,
			reader,
			firstPage,
			pageOpts,
			sseOffset,
			cursor,
			useBase64,
			sseLease,
		)
	}

	effectiveOffset := offset
	if isNowOffset {
		effectiveOffset = firstPage.Snapshot.Tail
	}

	// offset=now captures and touches one authoritative root snapshot while
	// deliberately returning no historical frames.
	if isNowOffset && liveMode != "long-poll" {
		h.observeReadPage(firstPage)
		w.Header().Set("Content-Type", firstPage.Snapshot.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, firstPage.Snapshot.Tail.String())
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
		if firstPage.Snapshot.Closed {
			w.Header().Set(protocol.HeaderStreamClosed, "true")
		}
		w.Header().Set("Cache-Control", "no-store")
		if store.IsJSONContentType(firstPage.Snapshot.ContentType) {
			if envelope {
				w.Header().Set(protocol.HeaderStreamEnvelope, "offsets")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		return nil
	}

	// Handle long-poll mode - wait if no messages and either:
	// 1. Client used offset=now (wants to wait for future data)
	// 2. Client is caught up (at the tail)
	shouldWait := liveMode == "long-poll" && len(firstPage.Messages) == 0 && (isNowOffset || effectiveOffset.Equal(firstPage.Snapshot.Tail))
	if shouldWait {
		// If stream is closed and client is at tail, return immediately (don't wait)
		if firstPage.Snapshot.Closed {
			h.observeReadPage(firstPage)
			w.Header().Set("Content-Type", firstPage.Snapshot.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, firstPage.Snapshot.Tail.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.Header().Set(protocol.HeaderStreamClosed, "true")
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
			w.WriteHeader(http.StatusNoContent)
			return nil
		}

		// Client is caught up. Subscription/registration happens before a
		// no-touch durable recheck, and the final page owns every response
		// header needed for wake, close, or timeout.
		var waited store.ReadWaitResult
		var waitErr error
		if liveSubscription != nil {
			waited, waitErr = h.waitForRegisteredPage(
				r.Context(),
				reader,
				path,
				effectiveOffset,
				firstPage.Snapshot,
				h.LongPollTimeout,
				pageOpts,
				liveSubscription,
			)
		} else {
			waited, waitErr = h.waitForPage(
				r.Context(),
				reader,
				path,
				effectiveOffset,
				firstPage.Snapshot,
				h.LongPollTimeout,
				pageOpts,
			)
		}
		h.observeReadPage(firstPage)
		releaseReadSnapshot(reader, path, firstPage.Snapshot)
		ownsSnapshot = false
		err = waitErr
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				h.observeReadCancellation("wait")
				return err
			}
			return readPageError(err)
		}

		firstPage = waited.Page
		ownsSnapshot = true
		closedAtTail := firstPage.Snapshot.Closed &&
			effectiveOffset.Equal(firstPage.Snapshot.Tail) &&
			len(firstPage.Messages) == 0
		if waited.TimedOut || closedAtTail {
			h.observeReadPage(firstPage)
			w.Header().Set("Content-Type", firstPage.Snapshot.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, firstPage.Snapshot.Tail.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateResponseCursor(cursor, time.Now()))
			if closedAtTail {
				w.Header().Set(protocol.HeaderStreamClosed, "true")
			}
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
	}

	stopAfterFirst := limit > 0 && store.IsJSONContentType(firstPage.Snapshot.ContentType)
	nextOffset := firstPage.Snapshot.Tail
	upToDate := true
	if stopAfterFirst {
		nextOffset = firstPage.NextOffset
		upToDate = firstPage.UpToDate
	}

	// Set response headers
	enveloped := envelope && store.IsJSONContentType(firstPage.Snapshot.ContentType)
	w.Header().Set("Content-Type", firstPage.Snapshot.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())

	// Always set Stream-Up-To-Date when at tail
	if upToDate {
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
	}

	// Include Stream-Closed when stream is closed AND client is at tail AND upToDate
	if firstPage.Snapshot.Closed && upToDate {
		w.Header().Set(protocol.HeaderStreamClosed, "true")
	}

	// Generate Stream-Cursor for long-poll responses (CDN cache collision prevention)
	if liveMode == "long-poll" {
		responseCursor := protocol.GenerateResponseCursor(cursor, time.Now())
		w.Header().Set(protocol.HeaderStreamCursor, responseCursor)
	}

	// Cache posture (issue #126, the Q3 decision): a credentialed read is
	// never shared-cacheable — Cache-Control: private, no ETag, and no
	// conditional handling (nothing to revalidate against) — so the §12.7
	// credential-keying problem cannot arise. Uncredentialed reads keep the
	// base protocol's caching headers unchanged.
	if authorizedPrivate {
		w.Header().Set("Cache-Control", "private")
	} else {
		// Set ETag for caching
		etag := fmt.Sprintf(`"%s"`, nextOffset.String())
		if enveloped {
			etag = fmt.Sprintf(`"%s~env"`, nextOffset.String())
		}
		w.Header().Set("ETag", etag)

		// Set caching headers for historical reads
		if !upToDate && len(firstPage.Messages) > 0 {
			w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
		}

		// Check If-None-Match for 304
		if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
			if ifNoneMatch == etag {
				h.observeReadPage(firstPage)
				if h.ReadMetrics != nil {
					h.ReadMetrics.ReadResponse(0, 1)
				}
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
		}
	}

	if enveloped {
		w.Header().Set(protocol.HeaderStreamEnvelope, "offsets")
	}

	w.WriteHeader(http.StatusOK)
	ownsSnapshot = false
	h.streamCatchupResponse(
		w,
		r,
		reader,
		path,
		firstPage,
		pageOpts,
		stopAfterFirst,
		store.IsJSONContentType(firstPage.Snapshot.ContentType),
		enveloped,
	)
	return nil
}

// handleAppend handles POST requests to append to a stream
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, path string) error {
	// Buffer the body before the live claim fence. This keeps a slow request body
	// from stretching the deposed-holder window; the residual fence-to-append
	// race is the #169 same-commit fence follow-up.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return newHTTPError(http.StatusBadRequest, "failed to read body")
	}

	// Credential preflight runs before stream store access so an invalid token
	// still cannot probe stream existence. The live fence is repeated below,
	// immediately before the mutation.
	cred, err := h.authorizeAppendCredential(r, path)
	if err != nil {
		return err
	}

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

	// On a fenced stream every POST — append, append-and-close, close-only —
	// is classed once, here, before any branch (#183 E.1 step c1). A stream
	// that never opted in is never classed and takes today's path.
	var class writeClass
	if meta.WriteFence {
		if class, err = h.classifyFencedStream(path, cred); err != nil {
			return err
		}
	}

	// Parse Stream-Closed header
	closedStr := r.Header.Get(protocol.HeaderStreamClosed)
	closeStream := closedStr == "true"

	// Check for Content-Type header
	contentType := r.Header.Get("Content-Type")

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

	// The fenced class binds Producer-Epoch to the claim generation, so the
	// producer headers are mandatory on it (#183 E.1 step g1; the Lua rung is
	// the backstop). The disclosure a 409 would carry needs the same headers.
	if class == writeClassFenced && !hasAllProducerHeaders {
		h.countFenceRejection(string(store.FenceProducerRequired))
		return newHTTPError(http.StatusBadRequest, "fenced write requires Producer-Id, Producer-Epoch, and Producer-Seq")
	}
	var reqSeq int64
	if hasAllProducerHeaders {
		reqSeq = *producerSeq
	}

	// Handle close-only request (empty body with Stream-Closed: true)
	if len(body) == 0 && closeStream {
		// Close-only - Content-Type validation is skipped per protocol Section 5.2
		if hasAllProducerHeaders {
			fence, err := h.authorizeAppendFence(r, path, cred, class)
			if err != nil {
				return producerDisclosure(err, true, reqSeq)
			}
			result, err := h.Store.CloseStreamWithProducer(path, store.CloseProducerOptions{
				ProducerId:    producerId,
				ProducerEpoch: *producerEpoch,
				ProducerSeq:   *producerSeq,
				Fence:         fence,
			})
			if err != nil {
				if errors.Is(err, store.ErrAppendFenced) {
					var d store.CloseProducerResult
					if result != nil {
						d = *result
					}
					return fencedError(d.FenceReason, d.FenceGeneration, d.FenceHolder, true, reqSeq)
				}
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

		fence, err := h.authorizeAppendFence(r, path, cred, class)
		if err != nil {
			return producerDisclosure(err, false, 0)
		}
		var result *store.CloseResult
		if fence == nil {
			result, err = h.Store.CloseStream(path)
		} else if closer, ok := h.Store.(store.FencedCloser); ok {
			result, err = closer.CloseStreamFenced(path, *fence)
		} else {
			return fenceDenied(auth.Deny(auth.ReasonFenced, "atomic append fence unavailable"), reasonStore)
		}
		if err != nil {
			if errors.Is(err, store.ErrAppendFenced) {
				var d store.CloseResult
				if result != nil {
					d = *result
				}
				return fencedError(d.FenceReason, d.FenceGeneration, d.FenceHolder, false, 0)
			}
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

	// Body IO, metadata lookup, and framing are complete. The authorization
	// result now carries the live claim identity into Store.Append, whose Redis
	// script compares it with the same-slot lease marker in the atomic commit.
	fence, err := h.authorizeAppendFence(r, path, cred, class)
	if err != nil {
		return producerDisclosure(err, hasAllProducerHeaders, reqSeq)
	}
	opts.Fence = fence
	result, err := h.Store.Append(path, body, opts)
	appendReturnedAt := time.Now()
	if err != nil {
		if errors.Is(err, store.ErrAppendFenced) {
			return fencedError(result.FenceReason, result.FenceGeneration, result.FenceHolder, hasAllProducerHeaders, reqSeq)
		}
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

	// Wake subscribers off the durable append (best-effort; the recovery sweep
	// is the backstop if this is lost to a crash). This fires only for a
	// genuinely new append — a deduplicated producer retry wrote no new data,
	// so waking subscribers for it would be spurious.
	h.onStreamAppend(path)
	h.observeAppendSubscriptionHook(appendReturnedAt)

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
	// The delete gate runs before the store call (issue #126 TB5): a denied
	// delete removes nothing and never fires the deletion hooks.
	if err := h.authorizeMutate(r, path, auth.ActionDelete); err != nil {
		return err
	}

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

// fenceFaultNoPair suppresses writeError's terminal gap pair on 409 FENCED
// when built with `-tags fence_fault_nopair` (fence_fault_nopair.go) — the
// fault-injection control proving the extension conformance client test
// detects a server that omits the pair. Always false in untagged builds.
var fenceFaultNoPair bool

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	// Authorization denials use the JSON error envelope shared with the
	// control plane (issue #126) rather than the base protocol's plaintext
	// errors: they are an extension surface, so their shape is ours to pin.
	var authErr *authError
	if errors.As(err, &authErr) {
		if authErr.status == http.StatusUnauthorized {
			// RFC 6750 §3: challenge the client for a Bearer credential.
			w.Header().Set("WWW-Authenticate", `Bearer realm="chronicle"`)
		}
		detail := webhook.ErrorDetail{Code: authErr.code, Message: authErr.msg}
		if f := authErr.fence; f != nil {
			// A write-fence rejection (#183 B.2/B.4). On a 409 with producer
			// headers, Producer-Epoch echoes the current generation when the
			// stream slot knew one, and the terminal gap pair Expected == Received
			// == the request's seq stops the pinned Electric producer on its first
			// response. Never Stream-Next-Offset: a deposed writer does not resume.
			// This is the single counting site, so every rejection counts once.
			if authErr.status == http.StatusConflict && f.HasProducer {
				if f.Generation != 0 {
					w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(f.Generation, 10))
				}
				if !fenceFaultNoPair {
					seq := strconv.FormatInt(f.ReqSeq, 10)
					w.Header().Set(protocol.HeaderProducerExpectedSeq, seq)
					w.Header().Set(protocol.HeaderProducerReceivedSeq, seq)
				}
			}
			detail.Reason, detail.CurrentHolder, detail.Generation = f.Reason, f.Holder, f.Generation
			h.countFenceRejection(f.Reason)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(authErr.status)
		_ = json.NewEncoder(w).Encode(webhook.ErrorBody{Error: detail})
		return
	}

	var httpErr *httpError
	if errors.As(err, &httpErr) {
		http.Error(w, httpErr.message, httpErr.status)
		return
	}

	h.logger().Error("internal error", "error", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}
