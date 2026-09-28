package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// This file is the dsui binary's built-in WEBHOOK-CAPTURE endpoint. It is a tool
// feature, NOT part of the Durable Streams protocol: the chronicle server signs
// and POSTs wake notifications to a subscription's webhook_url, but a browser
// cannot host an inbound HTTP endpoint to receive them. So the dsui binary hosts
// a plain receiver that a webhook subscription's webhook_url can point at, buffers
// the deliveries in memory, and relays them to the browser over Server-Sent
// Events. This touches no protocol/server code — it is exactly how a webhook
// receiver is meant to work.
//
// Three routes, all under /__hooks/{id} (the {id} is an arbitrary capture-bucket
// name the UI chooses, typically the subscription id):
//
//   POST /__hooks/{id}         receive one delivery; buffer it; respond 200 fast.
//   GET  /__hooks/{id}/stream  SSE: replay the buffer then stream new deliveries.
//   GET  /__hooks/{id}         JSON list of recent deliveries (debug / non-SSE).

// captureBufferCap is the per-bucket ring-buffer size. Oldest deliveries are
// evicted once a bucket exceeds this. Kept small: the UI tails live and only the
// recent window matters.
const captureBufferCap = 200

// captureBucketCap bounds bucket metadata, including buckets created by SSE
// subscribers before any delivery arrives.
const captureBucketCap = 1024

// captureBucketTTL bounds how long an idle bucket retains deliveries. Reaping is
// lazy on bucket creation, avoiding a background goroutine for this tiny tool.
const captureBucketTTL = time.Hour

// captureSubscriberChanSize bounds a single SSE subscriber's pending queue. If a
// subscriber falls this far behind it is dropped rather than blocking the POST
// handler (a slow browser must never wall the chronicle server's delivery).
const captureSubscriberChanSize = 64

// Delivery is one captured webhook POST, as the UI consumes it over SSE / JSON.
// All fields are JSON-serialized; the body is captured as a raw string so the
// browser can show (and, if it wants, verify) the exact bytes that arrived —
// signature verification requires the unmodified body, so it is never reparsed.
type Delivery struct {
	// Seq is a monotonic per-bucket sequence number (1-based), stamped on receipt.
	Seq uint64 `json:"seq"`
	// ReceivedAt is the capture time in Unix milliseconds.
	ReceivedAt int64 `json:"receivedAt"`
	// Method is the HTTP method of the delivery (normally POST).
	Method string `json:"method"`
	// Signature is the Webhook-Signature header value, captured for display. It is
	// NOT required and NOT verified here; an empty string means none was sent.
	Signature string `json:"signature"`
	// ContentType is the delivery's Content-Type header (normally application/json).
	ContentType string `json:"contentType"`
	// Headers contains only display-safe webhook protocol headers. Credentials and
	// proxy metadata are never retained or relayed to the browser.
	Headers map[string]string `json:"headers"`
	// Body is the exact raw request body bytes as a string.
	Body string `json:"body"`
}

// captureBucket holds one bucket's ring buffer and its live SSE subscribers.
type captureBucket struct {
	seq         uint64
	deliveries  []Delivery
	subscribers map[chan Delivery]struct{}
	updatedAt   time.Time
}

// captureStore is the concurrency-safe owner of all capture buckets. A single
// mutex guards the whole structure; the work under the lock is tiny (append to a
// slice, fan-out to channels) so a single lock keeps it simple and race-free.
type captureStore struct {
	mu      sync.Mutex
	buckets map[string]*captureBucket
	now     func() time.Time
}

func newCaptureStore() *captureStore {
	return &captureStore{buckets: make(map[string]*captureBucket), now: time.Now}
}

// bucketLocked returns the bucket for id, creating it on first use. The caller
// must hold s.mu.
func (s *captureStore) bucketLocked(id string) (*captureBucket, bool) {
	b := s.buckets[id]
	if b == nil {
		now := s.now()
		s.reapExpiredLocked(now)
		if len(s.buckets) >= captureBucketCap {
			return nil, false
		}
		b = &captureBucket{subscribers: make(map[chan Delivery]struct{}), updatedAt: now}
		s.buckets[id] = b
	}
	return b, true
}

func (s *captureStore) reapExpiredLocked(now time.Time) {
	for id, bucket := range s.buckets {
		if len(bucket.subscribers) == 0 && now.Sub(bucket.updatedAt) >= captureBucketTTL {
			delete(s.buckets, id)
		}
	}
}

// record stamps a delivery into the bucket's ring buffer (evicting the oldest
// past the cap) and fans it out to every live subscriber. A subscriber whose
// queue is full is skipped for this delivery rather than blocking the POST.
func (s *captureStore) record(id string, d Delivery) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucketLocked(id)
	if !ok {
		return false
	}

	b.seq++
	b.updatedAt = s.now()
	d.Seq = b.seq
	b.deliveries = append(b.deliveries, d)
	if len(b.deliveries) > captureBufferCap {
		// Drop the oldest. Re-slice onto a fresh backing array so the evicted
		// element can be garbage-collected (a plain re-slice would retain it).
		trimmed := make([]Delivery, captureBufferCap)
		copy(trimmed, b.deliveries[len(b.deliveries)-captureBufferCap:])
		b.deliveries = trimmed
	}

	for ch := range b.subscribers {
		select {
		case ch <- d:
		default:
			// Subscriber is too far behind; skip it for this delivery. It still
			// has the buffered replay and will recover on its next read.
		}
	}
	return true
}

// list returns a snapshot copy of a bucket's buffered deliveries (oldest first).
func (s *captureStore) list(id string) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[id]
	if b == nil {
		return []Delivery{}
	}
	out := make([]Delivery, len(b.deliveries))
	copy(out, b.deliveries)
	return out
}

// subscribe registers a new SSE subscriber and returns its channel plus the
// current buffered backlog (to replay before streaming). The caller MUST call
// unsubscribe with the same channel when done.
func (s *captureStore) subscribe(id string) (chan Delivery, []Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucketLocked(id)
	if !ok {
		return nil, nil, false
	}
	ch := make(chan Delivery, captureSubscriberChanSize)
	b.subscribers[ch] = struct{}{}
	b.updatedAt = s.now()
	backlog := make([]Delivery, len(b.deliveries))
	copy(backlog, b.deliveries)
	return ch, backlog, true
}

// unsubscribe removes a subscriber and closes its channel. Safe to call once.
func (s *captureStore) unsubscribe(id string, ch chan Delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[id]
	if b == nil {
		return
	}
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
		b.updatedAt = s.now()
	}
}

// registerCaptureRoutes wires the capture endpoint onto a mux. It is split out of
// main so it can be unit-tested against a stand-alone mux + httptest server with
// no chronicle and no embedded UI. A nil verifier records deliveries unverified.
func registerCaptureRoutes(mux *http.ServeMux, store *captureStore, verifier captureVerifier) {
	// Go 1.22+ pattern routing gives us the {id} wildcard and method matching, so
	// the stream and list/post routes split cleanly without manual path parsing.
	mux.HandleFunc("POST /__hooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleCapturePost(store, w, r, verifier)
	})
	mux.HandleFunc("GET /__hooks/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
		handleCaptureStream(store, w, r)
	})
	mux.HandleFunc("GET /__hooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleCaptureList(store, w, r)
	})
}

// maxCaptureBody bounds a single captured body so a hostile or runaway delivery
// cannot exhaust memory. 2 MiB is far above any realistic wake notification.
const (
	maxCaptureBody    = 2 << 20
	maxCaptureIDBytes = 256
)

func captureID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing capture id", http.StatusBadRequest)
		return "", false
	}
	if len(id) > maxCaptureIDBytes {
		http.Error(w, "capture id too long", http.StatusBadRequest)
		return "", false
	}
	return id, true
}

func handleCapturePost(store *captureStore, w http.ResponseWriter, r *http.Request, verifier captureVerifier) {
	id, ok := captureID(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCaptureBody+1))
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	if len(body) > maxCaptureBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if verifier != nil {
		if err := verifier.Verify(r.Header.Get("Webhook-Signature"), body); err != nil {
			if errors.Is(err, errJWKSUnavailable) {
				http.Error(w, "webhook verification unavailable", http.StatusServiceUnavailable)
				return
			}
			http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
			return
		}
	}
	d := Delivery{
		ReceivedAt:  time.Now().UnixMilli(),
		Method:      r.Method,
		Signature:   r.Header.Get("Webhook-Signature"),
		ContentType: r.Header.Get("Content-Type"),
		Headers:     flattenHeaders(r.Header),
		Body:        string(body),
	}
	if ok := store.record(id, d); !ok {
		http.Error(w, "capture capacity reached", http.StatusServiceUnavailable)
		return
	}
	// Respond fast and minimally. We deliberately do NOT echo {"done":true}: that
	// would tell chronicle to auto-ack and release the lease, which is a decision
	// for the operator driving the UI, not for the passive capture sink. The
	// browser drives ack/callback explicitly through the subscription controls.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"captured":true}`))
}

func handleCaptureList(store *captureStore, w http.ResponseWriter, r *http.Request) {
	id, ok := captureID(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":         id,
		"deliveries": store.list(id),
	})
}

func handleCaptureStream(store *captureStore, w http.ResponseWriter, r *http.Request) {
	id, ok := captureID(w, r)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch, backlog, ok := store.subscribe(id)
	if !ok {
		http.Error(w, "capture capacity reached", http.StatusServiceUnavailable)
		return
	}
	defer store.unsubscribe(id, ch)

	// An opening comment lets the EventSource fire `open` immediately and flushes
	// any proxy buffering before the first real event.
	_, _ = io.WriteString(w, ": connected\n\n")
	flusher.Flush()

	// Replay the buffered backlog first, then stream live deliveries. A delivery
	// that arrived between the subscribe snapshot and now is still delivered live
	// (the subscriber channel was already registered), so none are missed; the
	// browser dedupes on the monotonic Seq if a replay and a live event overlap.
	for _, d := range backlog {
		if !writeDeliveryEvent(w, d) {
			return
		}
		flusher.Flush()
	}

	ctx := r.Context()
	// A periodic comment keeps the connection alive through idle-timeout proxies.
	keepalive := time.NewTicker(25 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			// Client disconnected; the deferred unsubscribe cleans up.
			return
		case d, open := <-ch:
			if !open {
				return
			}
			if !writeDeliveryEvent(w, d) {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// writeDeliveryEvent writes one named SSE `delivery` event carrying the JSON
// record. Returns false if the write failed (client gone), so the caller stops.
func writeDeliveryEvent(w io.Writer, d Delivery) bool {
	payload, err := json.Marshal(d)
	if err != nil {
		return true // Skip an unencodable record rather than tearing down the stream.
	}
	if _, err := io.WriteString(w, "event: delivery\ndata: "); err != nil {
		return false
	}
	if _, err := w.Write(payload); err != nil {
		return false
	}
	// A JSON object is single-line (no embedded newlines), so one data: line is
	// correct SSE framing; terminate the event with a blank line.
	if _, err := io.WriteString(w, "\n\n"); err != nil {
		return false
	}
	return true
}

// flattenHeaders retains only protocol metadata that is safe and useful to show
// in the browser. In particular, credentials, cookies, and forwarding headers
// must never enter the in-memory capture store.
func flattenHeaders(h http.Header) map[string]string {
	const (
		contentTypeHeader = "Content-Type"
		signatureHeader   = "Webhook-Signature"
	)
	out := make(map[string]string, 2)
	for _, name := range [...]string{contentTypeHeader, signatureHeader} {
		if values := h.Values(name); len(values) > 0 {
			out[name] = strings.Join(values, ", ")
		}
	}
	return out
}
