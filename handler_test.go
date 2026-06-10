package chronicle

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func testHandler(longPoll, sseReconnect time.Duration) *Handler {
	return &Handler{
		Store:                store.NewMemoryStore(),
		LongPollTimeout:      longPoll,
		SSEReconnectInterval: sseReconnect,
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// do builds a request, runs it through the handler, and returns the recorder.
func do(h http.Handler, method, target string, headers map[string]string, body []byte) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func mustCreate(t *testing.T, h *Handler, path, contentType string, body []byte) {
	t.Helper()
	rec := do(h, http.MethodPut, path, map[string]string{"Content-Type": contentType}, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create %s: status = %d, body %q", path, rec.Code, rec.Body.String())
	}
}

func mustAppend(t *testing.T, h *Handler, path, contentType string, body []byte) {
	t.Helper()
	rec := do(h, http.MethodPost, path, map[string]string{"Content-Type": contentType}, body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append %s: status = %d, body %q", path, rec.Code, rec.Body.String())
	}
}

// off renders the canonical offset string for a byte position.
func off(n uint64) string {
	return store.Offset{ByteOffset: n}.String()
}

func assertSecurityHeaders(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := rec.Header().Get("Cross-Origin-Resource-Policy"); got != "cross-origin" {
		t.Errorf("Cross-Origin-Resource-Policy = %q, want cross-origin", got)
	}
}

// --- Create (PUT) ---

func TestCreate(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/test", map[string]string{"Content-Type": "text/plain"}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "http://example.com/test" {
		t.Errorf("Location = %q", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(0) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(0))
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q", got)
	}
	assertSecurityHeaders(t, rec)

	// Idempotent re-create with the same config returns 200, no Location
	rec = do(h, http.MethodPut, "/test", map[string]string{"Content-Type": "text/plain"}, nil)
	if rec.Code != http.StatusOK {
		t.Errorf("idempotent create: status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "" {
		t.Errorf("idempotent create: Location = %q, want empty", got)
	}

	// Different config conflicts
	rec = do(h, http.MethodPut, "/test", map[string]string{"Content-Type": "application/json"}, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("config mismatch: status = %d, want 409", rec.Code)
	}
	assertSecurityHeaders(t, rec)
}

func TestCreateWithInitialData(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/test", map[string]string{"Content-Type": "text/plain"}, []byte("hello"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}
}

func TestCreateClosed(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
}

func TestCreateLocationForwardedHeaders(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/test", map[string]string{
		"Content-Type":      "text/plain",
		"X-Forwarded-Proto": "https",
		"X-Forwarded-Host":  "streams.example.org",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "https://streams.example.org/test" {
		t.Errorf("Location = %q", got)
	}
}

func TestCreateTTLExpiresAtConflict(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/test", map[string]string{
		"Content-Type":                 "text/plain",
		protocol.HeaderStreamTTL:       "60",
		protocol.HeaderStreamExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339),
	}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestCreateInvalidTTL(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	for _, ttl := range []string{"01", "-1", "1.5", "abc"} {
		rec := do(h, http.MethodPut, "/test", map[string]string{
			"Content-Type":           "text/plain",
			protocol.HeaderStreamTTL: ttl,
		}, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("TTL %q: status = %d, want 400", ttl, rec.Code)
		}
	}
}

func TestCreateForkHeaderValidation(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/src", "text/plain", []byte("data"))

	// Sub-offset without Stream-Forked-From is rejected, even when "0"
	rec := do(h, http.MethodPut, "/fork", map[string]string{
		"Content-Type":                     "text/plain",
		protocol.HeaderStreamForkSubOffset: "0",
	}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("sub-offset without forked-from: status = %d, want 400", rec.Code)
	}

	// Present-but-empty sub-offset is distinguished from absent and rejected
	req := httptest.NewRequest(http.MethodPut, "/fork", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set(protocol.HeaderStreamForkedFrom, "/src")
	req.Header.Set(protocol.HeaderStreamForkSubOffset, "")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("empty sub-offset: status = %d, want 400", rec.Code)
	}

	// Invalid fork offset format
	rec = do(h, http.MethodPut, "/fork", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/src",
		protocol.HeaderStreamForkOffset: "not-an-offset",
	}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid fork offset: status = %d, want 400", rec.Code)
	}

	// Fork of a missing source
	rec = do(h, http.MethodPut, "/fork", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/missing",
	}, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("missing source: status = %d, want 404", rec.Code)
	}

	// Valid fork inherits source data
	rec = do(h, http.MethodPut, "/fork", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/src",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("valid fork: status = %d, want 201", rec.Code)
	}
	rec = do(h, http.MethodGet, "/fork", nil, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "data" {
		t.Errorf("fork read: status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

// --- HEAD ---

func TestHead(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	rec := do(h, http.MethodHead, "/test", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}

	// Missing stream
	rec = do(h, http.MethodHead, "/missing", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("missing: status = %d, want 404", rec.Code)
	}
}

func TestHeadTTLHeaders(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPut, "/ttl", map[string]string{
		"Content-Type":           "text/plain",
		protocol.HeaderStreamTTL: "60",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d", rec.Code)
	}
	rec = do(h, http.MethodHead, "/ttl", nil, nil)
	if got := rec.Header().Get(protocol.HeaderStreamTTL); got != "60" {
		t.Errorf("Stream-TTL = %q, want 60", got)
	}

	expiresAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	rec = do(h, http.MethodPut, "/exp", map[string]string{
		"Content-Type":                 "text/plain",
		protocol.HeaderStreamExpiresAt: expiresAt.Format(time.RFC3339),
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d", rec.Code)
	}
	rec = do(h, http.MethodHead, "/exp", nil, nil)
	if got := rec.Header().Get(protocol.HeaderStreamExpiresAt); got != expiresAt.Format(time.RFC3339) {
		t.Errorf("Stream-Expires-At = %q, want %q", got, expiresAt.Format(time.RFC3339))
	}
}

func TestHeadClosedStream(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodHead, "/test", nil, nil)
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
}

// --- Append (POST) ---

func TestAppend(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "text/plain"}, []byte("hello"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}
}

func TestAppendEmptyBody(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "text/plain"}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	assertSecurityHeaders(t, rec)
}

func TestAppendMissingContentType(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", nil, []byte("hello"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAppendContentTypeMismatch(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "application/json"}, []byte(`{"a":1}`))
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rec.Code)
	}
}

func TestAppendNotFound(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPost, "/missing", map[string]string{"Content-Type": "text/plain"}, []byte("x"))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestAppendToClosed(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "text/plain"}, []byte("more"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want final offset %q", got, off(5))
	}
	assertSecurityHeaders(t, rec)
}

// --- Idempotent producers ---

func producerHeaders(id, epoch, seq string) map[string]string {
	return map[string]string{
		"Content-Type":               "text/plain",
		protocol.HeaderProducerId:    id,
		protocol.HeaderProducerEpoch: epoch,
		protocol.HeaderProducerSeq:   seq,
	}
}

func TestAppendProducerFlow(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	// New producer write: 200 OK with echoed epoch/seq
	rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "0"), []byte("a"))
	if rec.Code != http.StatusOK {
		t.Fatalf("seq 0: status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != "1" {
		t.Errorf("Producer-Epoch = %q, want 1", got)
	}
	if got := rec.Header().Get(protocol.HeaderProducerSeq); got != "0" {
		t.Errorf("Producer-Seq = %q, want 0", got)
	}

	rec = do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "1"), []byte("b"))
	if rec.Code != http.StatusOK {
		t.Fatalf("seq 1: status = %d, want 200", rec.Code)
	}

	// Duplicate: 204 with echoed epoch and the highest accepted seq
	rec = do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "0"), []byte("a"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("duplicate: status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != "1" {
		t.Errorf("duplicate Producer-Epoch = %q, want 1", got)
	}
	if got := rec.Header().Get(protocol.HeaderProducerSeq); got != "1" {
		t.Errorf("duplicate Producer-Seq = %q, want highest accepted 1", got)
	}

	// The duplicate appended nothing
	rec = do(h, http.MethodGet, "/test", nil, nil)
	if rec.Body.String() != "ab" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "ab")
	}
}

func TestAppendProducerStaleEpoch(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	if rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "5", "0"), []byte("a")); rec.Code != http.StatusOK {
		t.Fatalf("setup append: status = %d", rec.Code)
	}

	rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "4", "0"), []byte("b"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != "5" {
		t.Errorf("Producer-Epoch = %q, want current epoch 5", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(1) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(1))
	}
	assertSecurityHeaders(t, rec)
}

func TestAppendProducerSeqGap(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	if rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "0"), []byte("a")); rec.Code != http.StatusOK {
		t.Fatalf("setup append: status = %d", rec.Code)
	}

	rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "5"), []byte("b"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerExpectedSeq); got != "1" {
		t.Errorf("Producer-Expected-Seq = %q, want 1", got)
	}
	if got := rec.Header().Get(protocol.HeaderProducerReceivedSeq); got != "5" {
		t.Errorf("Producer-Received-Seq = %q, want 5", got)
	}

	// First contact must start at seq 0
	rec = do(h, http.MethodPost, "/test", producerHeaders("p2", "1", "3"), []byte("c"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("first-contact gap: status = %d, want 409", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerExpectedSeq); got != "0" {
		t.Errorf("first-contact Producer-Expected-Seq = %q, want 0", got)
	}
}

func TestAppendProducerNewEpochMustStartAtZero(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	if rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "1", "0"), []byte("a")); rec.Code != http.StatusOK {
		t.Fatalf("setup append: status = %d", rec.Code)
	}

	rec := do(h, http.MethodPost, "/test", producerHeaders("p1", "2", "7"), []byte("b"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAppendPartialProducerHeaders(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":            "text/plain",
		protocol.HeaderProducerId: "p1",
	}, []byte("a"))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAppendInvalidProducerIntegers(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	for _, bad := range []string{"abc", "-1", "1.5", "+2"} {
		rec := do(h, http.MethodPost, "/test", producerHeaders("p1", bad, "0"), []byte("a"))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("epoch %q: status = %d, want 400", bad, rec.Code)
		}
		rec = do(h, http.MethodPost, "/test", producerHeaders("p1", "1", bad), []byte("a"))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("seq %q: status = %d, want 400", bad, rec.Code)
		}
	}
}

// --- Close ---

func TestCloseOnly(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	// Close-only requests skip Content-Type validation entirely
	rec := do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}

	// Closing an already-closed stream is idempotent
	rec = do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("repeat close: status = %d, want 204", rec.Code)
	}
}

func TestCloseOnlyWithProducer(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	headers := map[string]string{
		protocol.HeaderStreamClosed:  "true",
		protocol.HeaderProducerId:    "p1",
		protocol.HeaderProducerEpoch: "1",
		protocol.HeaderProducerSeq:   "0",
	}
	rec := do(h, http.MethodPost, "/test", headers, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != "1" {
		t.Errorf("Producer-Epoch = %q, want 1", got)
	}
	if got := rec.Header().Get(protocol.HeaderProducerSeq); got != "0" {
		t.Errorf("Producer-Seq = %q, want 0", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}

	// Duplicate of the closing request: idempotent 204
	rec = do(h, http.MethodPost, "/test", headers, nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("duplicate close: status = %d, want 204", rec.Code)
	}

	// A different producer tuple hits the closed stream: 409
	rec = do(h, http.MethodPost, "/test", map[string]string{
		protocol.HeaderStreamClosed:  "true",
		protocol.HeaderProducerId:    "p2",
		protocol.HeaderProducerEpoch: "1",
		protocol.HeaderProducerSeq:   "0",
	}, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("other producer close: status = %d, want 409", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("other producer close: Stream-Closed = %q, want true", got)
	}
}

func TestAppendWithClose(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("final"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}
}

// --- Delete ---

func TestDelete(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodDelete, "/test", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}

	rec = do(h, http.MethodDelete, "/test", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("repeat delete: status = %d, want 404", rec.Code)
	}
}

func TestSoftDeletedStream(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/src", "text/plain", []byte("data"))
	rec := do(h, http.MethodPut, "/fork", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/src",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("fork create: status = %d", rec.Code)
	}

	// Deleting a source with live forks soft-deletes it
	if rec := do(h, http.MethodDelete, "/src", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete source: status = %d, want 204", rec.Code)
	}

	if rec := do(h, http.MethodGet, "/src", nil, nil); rec.Code != http.StatusGone {
		t.Errorf("GET soft-deleted: status = %d, want 410", rec.Code)
	}
	if rec := do(h, http.MethodHead, "/src", nil, nil); rec.Code != http.StatusGone {
		t.Errorf("HEAD soft-deleted: status = %d, want 410", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/src", map[string]string{"Content-Type": "text/plain"}, []byte("x")); rec.Code != http.StatusGone {
		t.Errorf("POST soft-deleted: status = %d, want 410", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/src", nil, nil); rec.Code != http.StatusGone {
		t.Errorf("DELETE soft-deleted: status = %d, want 410", rec.Code)
	}
	if rec := do(h, http.MethodPut, "/src", map[string]string{"Content-Type": "text/plain"}, nil); rec.Code != http.StatusConflict {
		t.Errorf("PUT soft-deleted: status = %d, want 409", rec.Code)
	}

	// Fork still reads through the soft-deleted source
	if rec := do(h, http.MethodGet, "/fork", nil, nil); rec.Code != http.StatusOK || rec.Body.String() != "data" {
		t.Errorf("fork read: status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

// --- Read (GET catch-up) ---

func TestReadCatchUp(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)
	mustAppend(t, h, "/test", "text/plain", []byte("hello"))
	mustAppend(t, h, "/test", "text/plain", []byte(" world"))

	rec := do(h, http.MethodGet, "/test", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "hello world" {
		t.Errorf("body = %q", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(11) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(11))
	}
	if got := rec.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("Stream-Up-To-Date = %q, want true", got)
	}
	wantETag := fmt.Sprintf("%q", off(11))
	if got := rec.Header().Get("ETag"); got != wantETag {
		t.Errorf("ETag = %q, want %q", got, wantETag)
	}
	assertSecurityHeaders(t, rec)

	// Offset-suffix read returns only the remainder
	rec = do(h, http.MethodGet, "/test?offset="+off(5), nil, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != " world" {
		t.Errorf("suffix read: status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestReadEmptyStream(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodGet, "/test", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("Stream-Up-To-Date = %q, want true", got)
	}
	wantETag := fmt.Sprintf("%q", off(0))
	if got := rec.Header().Get("ETag"); got != wantETag {
		t.Errorf("ETag = %q, want %q", got, wantETag)
	}
}

func TestReadIfNoneMatch(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	etag := fmt.Sprintf("%q", off(5))
	rec := do(h, http.MethodGet, "/test", map[string]string{"If-None-Match": etag}, nil)
	if rec.Code != http.StatusNotModified {
		t.Errorf("status = %d, want 304", rec.Code)
	}

	// Stale ETag still returns data
	rec = do(h, http.MethodGet, "/test", map[string]string{"If-None-Match": fmt.Sprintf("%q", off(2))}, nil)
	if rec.Code != http.StatusOK {
		t.Errorf("stale etag: status = %d, want 200", rec.Code)
	}
}

func TestReadOffsetNow(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("history"))

	rec := do(h, http.MethodGet, "/test?offset=now", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(7) {
		t.Errorf("Stream-Next-Offset = %q, want tail %q", got, off(7))
	}
	if got := rec.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("Stream-Up-To-Date = %q, want true", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	if got := rec.Header().Get("ETag"); got != "" {
		t.Errorf("ETag = %q, want unset for offset=now", got)
	}
}

func TestReadOffsetNowJSON(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "application/json", []byte(`[1,2]`))

	rec := do(h, http.MethodGet, "/test?offset=now", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "[]" {
		t.Errorf("body = %q, want []", rec.Body.String())
	}
}

func TestReadOffsetNowClosedStream(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("x"))
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodGet, "/test?offset=now", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
}

func TestReadOffsetValidation(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)

	// Explicit empty offset parameter
	if rec := do(h, http.MethodGet, "/test?offset=", nil, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("empty offset: status = %d, want 400", rec.Code)
	}
	// Multiple offset parameters
	if rec := do(h, http.MethodGet, "/test?offset="+off(0)+"&offset="+off(1), nil, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("multiple offsets: status = %d, want 400", rec.Code)
	}
	// Invalid offset format
	if rec := do(h, http.MethodGet, "/test?offset=bogus", nil, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("invalid offset: status = %d, want 400", rec.Code)
	}
	// Live modes require an offset
	if rec := do(h, http.MethodGet, "/test?live=long-poll", nil, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("long-poll without offset: status = %d, want 400", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/test?live=sse", nil, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("sse without offset: status = %d, want 400", rec.Code)
	}
}

func TestReadNotFound(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodGet, "/missing", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	assertSecurityHeaders(t, rec)
}

// --- JSON mode ---

func TestJSONModeFlatten(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "application/json", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "application/json"}, []byte(`[1,2,3]`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append: status = %d, want 204", rec.Code)
	}
	// Flattened: offsets advance by element byte length, not array length
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(3) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(3))
	}

	rec = do(h, http.MethodGet, "/test", nil, nil)
	if rec.Body.String() != "[1,2,3]" {
		t.Errorf("body = %q, want [1,2,3]", rec.Body.String())
	}

	// Suffix read re-wraps remaining elements
	rec = do(h, http.MethodGet, "/test?offset="+off(1), nil, nil)
	if rec.Body.String() != "[2,3]" {
		t.Errorf("suffix body = %q, want [2,3]", rec.Body.String())
	}
}

func TestJSONModeRejectsEmptyArrayAppend(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	// Empty array is allowed as PUT initial data...
	mustCreate(t, h, "/test", "application/json", []byte(`[]`))

	// ...but rejected on POST
	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "application/json"}, []byte(`[]`))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestJSONModeRejectsInvalidJSON(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "application/json", nil)

	rec := do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "application/json"}, []byte(`{broken`))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// --- Long-poll ---

func TestLongPollImmediateData(t *testing.T) {
	h := testHandler(2*time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	rec := do(h, http.MethodGet, "/test?offset=-1&live=long-poll", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Errorf("body = %q", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamCursor); got == "" {
		t.Error("Stream-Cursor missing on long-poll 200")
	}
}

func TestLongPollTimeout(t *testing.T) {
	h := testHandler(50*time.Millisecond, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	rec := do(h, http.MethodGet, "/test?offset="+off(5)+"&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(5) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(5))
	}
	if got := rec.Header().Get(protocol.HeaderStreamUpToDate); got != "true" {
		t.Errorf("Stream-Up-To-Date = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamCursor); got == "" {
		t.Error("Stream-Cursor missing on long-poll 204")
	}
}

func TestLongPollReceivesAppend(t *testing.T) {
	h := testHandler(2*time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	go func() {
		time.Sleep(100 * time.Millisecond)
		do(h, http.MethodPost, "/test", map[string]string{"Content-Type": "text/plain"}, []byte(" world"))
	}()

	start := time.Now()
	rec := do(h, http.MethodGet, "/test?offset="+off(5)+"&live=long-poll", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != " world" {
		t.Errorf("body = %q", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(11) {
		t.Errorf("Stream-Next-Offset = %q, want %q", got, off(11))
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("long-poll did not wake on append (took %v)", elapsed)
	}
}

func TestLongPollClosedAtTail(t *testing.T) {
	h := testHandler(2*time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	start := time.Now()
	rec := do(h, http.MethodGet, "/test?offset="+off(5)+"&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamCursor); got == "" {
		t.Error("Stream-Cursor missing on closed long-poll 204")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("closed-at-tail long-poll should not wait (took %v)", elapsed)
	}
}

func TestLongPollClosedDuringWait(t *testing.T) {
	h := testHandler(2*time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	go func() {
		time.Sleep(100 * time.Millisecond)
		do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)
	}()

	rec := do(h, http.MethodGet, "/test?offset="+off(5)+"&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamClosed); got != "true" {
		t.Errorf("Stream-Closed = %q, want true", got)
	}
	if got := rec.Header().Get(protocol.HeaderStreamCursor); got == "" {
		t.Error("Stream-Cursor missing on 204")
	}
}

func TestLongPollOffsetNowWaitsAtTail(t *testing.T) {
	h := testHandler(50*time.Millisecond, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("history"))

	// offset=now long-poll skips history and waits at the tail
	rec := do(h, http.MethodGet, "/test?offset=now&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != off(7) {
		t.Errorf("Stream-Next-Offset = %q, want tail %q", got, off(7))
	}
}

func TestLongPollEchoedCursorAdvances(t *testing.T) {
	h := testHandler(50*time.Millisecond, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("x"))

	// Send a cursor far in the future; the response cursor must be strictly greater
	rec := do(h, http.MethodGet, "/test?offset="+off(1)+"&live=long-poll&cursor=99999999", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	got, err := strconv.ParseInt(rec.Header().Get(protocol.HeaderStreamCursor), 10, 64)
	if err != nil || got <= 99999999 {
		t.Errorf("Stream-Cursor = %v (err %v), want > 99999999", got, err)
	}
}

// --- SSE ---

func TestSSEDataAndControlFraming(t *testing.T) {
	h := testHandler(time.Second, 200*time.Millisecond)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", got)
	}
	if !rec.Flushed {
		t.Error("SSE response was not flushed")
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: data\ndata:hello\n\n") {
		t.Errorf("missing data event, body = %q", body)
	}
	if !strings.Contains(body, "event: control\n") {
		t.Errorf("missing control event, body = %q", body)
	}
	if !strings.Contains(body, `"streamNextOffset":"`+off(5)+`"`) {
		t.Errorf("control missing streamNextOffset, body = %q", body)
	}
	if !strings.Contains(body, `"streamCursor":"`) {
		t.Errorf("control missing streamCursor, body = %q", body)
	}
	if !strings.Contains(body, `"upToDate":true`) {
		t.Errorf("control missing upToDate, body = %q", body)
	}
}

func TestSSEClosedStreamFinalControl(t *testing.T) {
	// Long reconnect interval: the connection must close because of the
	// streamClosed control event, not the reconnect timer.
	h := testHandler(time.Second, 10*time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("hello"))
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	start := time.Now()
	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("SSE on closed stream did not terminate promptly (took %v)", elapsed)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: data\ndata:hello\n\n") {
		t.Errorf("missing data event, body = %q", body)
	}
	if !strings.Contains(body, `"streamClosed":true`) {
		t.Errorf("missing streamClosed control, body = %q", body)
	}
	// Final control omits streamCursor per protocol
	if strings.Contains(body, "streamCursor") {
		t.Errorf("final control must omit streamCursor, body = %q", body)
	}
}

func TestSSEEmptyStreamInitialControl(t *testing.T) {
	h := testHandler(time.Second, 150*time.Millisecond)
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	body := rec.Body.String()
	if strings.Contains(body, "event: data\n") {
		t.Errorf("unexpected data event for empty stream, body = %q", body)
	}
	if !strings.Contains(body, "event: control\n") {
		t.Fatalf("missing initial control event, body = %q", body)
	}
	if !strings.Contains(body, `"streamNextOffset":"`+off(0)+`"`) {
		t.Errorf("control missing zero offset, body = %q", body)
	}
	if !strings.Contains(body, `"upToDate":true`) {
		t.Errorf("control missing upToDate, body = %q", body)
	}
}

func TestSSEEmptyClosedStream(t *testing.T) {
	h := testHandler(time.Second, 10*time.Second)
	mustCreate(t, h, "/test", "text/plain", nil)
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	body := rec.Body.String()
	if !strings.Contains(body, `"streamClosed":true`) {
		t.Errorf("missing streamClosed in initial control, body = %q", body)
	}
}

func TestSSEBase64Binary(t *testing.T) {
	h := testHandler(time.Second, 10*time.Second)
	mustCreate(t, h, "/test", "application/octet-stream", []byte{0x01, 0x02, 0xff})
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	if got := rec.Header().Get(protocol.HeaderStreamSSEDataEncoding); got != "base64" {
		t.Errorf("Stream-SSE-Data-Encoding = %q, want base64", got)
	}
	body := rec.Body.String()
	// base64([0x01 0x02 0xff]) = "AQL/"
	if !strings.Contains(body, "event: data\ndata:AQL/\n\n") {
		t.Errorf("missing base64 data event, body = %q", body)
	}
}

func TestSSECRLFInjectionSafety(t *testing.T) {
	h := testHandler(time.Second, 10*time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("a\r\nb\rc\nd"))
	do(h, http.MethodPost, "/test", map[string]string{protocol.HeaderStreamClosed: "true"}, nil)

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "event: data\ndata:a\ndata:b\ndata:c\ndata:d\n\n") {
		t.Errorf("payload line terminators not split safely, body = %q", body)
	}
	// No raw CR may survive into the event stream
	if strings.Contains(body, "\r") {
		t.Errorf("raw CR leaked into SSE stream, body = %q", body)
	}
}

// TestSSEDrainsFinalAppendBeforeClose exercises the close sequencing: data
// appended together with a close must be delivered as a data event before the
// streamClosed control event, even for a reader already waiting at the tail.
func TestSSEDrainsFinalAppendBeforeClose(t *testing.T) {
	h := testHandler(time.Second, 10*time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("first"))

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()

	// Let the SSE loop deliver the catch-up data and settle at the tail
	time.Sleep(150 * time.Millisecond)

	// Append the final message and close in one request
	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("last"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("close-append: status = %d", rec.Code)
	}

	// The server must emit data:last, then the streamClosed control, then EOF
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read sse body: %v", err)
	}
	s := string(body)
	dataIdx := strings.Index(s, "data:last")
	closedIdx := strings.Index(s, `"streamClosed":true`)
	if dataIdx == -1 {
		t.Fatalf("final append not delivered, body = %q", s)
	}
	if closedIdx == -1 {
		t.Fatalf("streamClosed control not delivered, body = %q", s)
	}
	if dataIdx > closedIdx {
		t.Errorf("final data event must precede streamClosed control, body = %q", s)
	}
}

// --- Dispatch, CORS, security ---

func TestOptionsPreflight(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodOptions, "/test", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "PUT") {
		t.Errorf("Access-Control-Allow-Methods = %q", got)
	}
	assertSecurityHeaders(t, rec)
}

func TestMethodNotAllowed(t *testing.T) {
	h := testHandler(time.Second, time.Second)

	rec := do(h, http.MethodPatch, "/test", nil, nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
	assertSecurityHeaders(t, rec)
}

func TestCORSHeadersOnAllResponses(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/test", "text/plain", []byte("x"))

	cases := []struct {
		name string
		rec  *httptest.ResponseRecorder
	}{
		{"success read", do(h, http.MethodGet, "/test", nil, nil)},
		{"not found", do(h, http.MethodGet, "/missing", nil, nil)},
		{"bad request", do(h, http.MethodGet, "/test?offset=bogus", nil, nil)},
	}
	for _, tc := range cases {
		if got := tc.rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("%s: Access-Control-Allow-Origin = %q, want *", tc.name, got)
		}
		if got := tc.rec.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, "Stream-Next-Offset") {
			t.Errorf("%s: Access-Control-Expose-Headers = %q", tc.name, got)
		}
		assertSecurityHeaders(t, tc.rec)
	}
}

// --- Mount (stream root prefix handling) ---

func TestMount(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mux, err := Mount("/v1/stream/", h)
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}

	// Requests outside the stream root are rejected
	rec := do(mux, http.MethodGet, "/other/path", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("outside root: status = %d, want 404", rec.Code)
	}
	assertSecurityHeaders(t, rec)

	// The reserved __ds namespace is not implemented yet
	rec = do(mux, http.MethodGet, "/v1/stream/__ds/subscriptions/abc", nil, nil)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("__ds: status = %d, want 501", rec.Code)
	}

	// Create through the mount: Location carries the full wire path
	rec = do(mux, http.MethodPut, "/v1/stream/abc", map[string]string{"Content-Type": "text/plain"}, []byte("data"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "http://example.com/v1/stream/abc" {
		t.Errorf("Location = %q, want full wire path", got)
	}

	// The store sees root-relative paths
	if !h.Store.Has("/abc") {
		t.Error("store should hold the root-relative path /abc")
	}
	if h.Store.Has("/v1/stream/abc") {
		t.Error("store should not hold the full wire path")
	}

	// Reads resolve through the same translation
	rec = do(mux, http.MethodGet, "/v1/stream/abc", nil, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "data" {
		t.Errorf("read: status = %d, body = %q", rec.Code, rec.Body.String())
	}

	// Stream-Forked-From wire paths are translated to store paths
	rec = do(mux, http.MethodPut, "/v1/stream/fork", map[string]string{
		"Content-Type":                  "text/plain",
		protocol.HeaderStreamForkedFrom: "/v1/stream/abc",
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("fork create: status = %d, want 201, body %q", rec.Code, rec.Body.String())
	}
	rec = do(mux, http.MethodGet, "/v1/stream/fork", nil, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "data" {
		t.Errorf("fork read: status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestMountValidatesRoot(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	for _, root := range []string{"", "v1/stream/", "/v1/stream"} {
		if _, err := Mount(root, h); err == nil {
			t.Errorf("Mount(%q) should fail", root)
		}
	}
	if _, err := Mount("/", h); err != nil {
		t.Errorf("Mount(\"/\") should succeed, got %v", err)
	}
}
