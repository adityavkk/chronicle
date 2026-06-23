package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newCaptureServer spins up an httptest server hosting only the capture routes —
// no chronicle, no embedded UI — which is the whole point: the POST -> buffer ->
// SSE/list relay is testable in isolation.
func newCaptureServer(t *testing.T) (*httptest.Server, *captureStore) {
	t.Helper()
	store := newCaptureStore()
	mux := http.NewServeMux()
	registerCaptureRoutes(mux, store)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, store
}

// postDelivery POSTs one webhook delivery and asserts a 200.
func postDelivery(t *testing.T, base, id, body, signature string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/__hooks/"+id, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if signature != "" {
		req.Header.Set("Webhook-Signature", signature)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST delivery: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", res.StatusCode)
	}
}

// getDeliveries fetches the JSON list for a bucket.
func getDeliveries(t *testing.T, base, id string) []Delivery {
	t.Helper()
	res, err := http.Get(base + "/__hooks/" + id)
	if err != nil {
		t.Fatalf("GET list: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET list status = %d, want 200", res.StatusCode)
	}
	var payload struct {
		ID         string     `json:"id"`
		Deliveries []Delivery `json:"deliveries"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if payload.ID != id {
		t.Fatalf("list id = %q, want %q", payload.ID, id)
	}
	return payload.Deliveries
}

// readDeliveryEvent reads SSE lines from r until one full `delivery` event is
// parsed, then returns the decoded Delivery. It ignores comment/keepalive lines.
func readDeliveryEvent(t *testing.T, r *bufio.Reader) Delivery {
	t.Helper()
	var event, data string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			// End of an event block. Only return on the named delivery event.
			if event == "delivery" && data != "" {
				var d Delivery
				if err := json.Unmarshal([]byte(data), &d); err != nil {
					t.Fatalf("decode SSE delivery data %q: %v", data, err)
				}
				return d
			}
			event, data = "", ""
		case strings.HasPrefix(line, ":"):
			// Comment / keepalive — ignore.
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

// TestCapturePostThenListAndSSE is the headline test the task asks for: POST a
// delivery, then assert BOTH the GET list and an SSE subscriber receive it.
func TestCapturePostThenListAndSSE(t *testing.T) {
	srv, _ := newCaptureServer(t)
	const id = "sub-abc"
	const body = `{"subscription_id":"sub-abc","wake_id":"w1","generation":3}`
	const sig = "t=1699564800,kid=ds_test,ed25519=AAAA"

	// --- POST a delivery, then assert the GET list saw it. ---
	postDelivery(t, srv.URL, id, body, sig)

	list := getDeliveries(t, srv.URL, id)
	if len(list) != 1 {
		t.Fatalf("list len = %d, want 1", len(list))
	}
	got := list[0]
	if got.Body != body {
		t.Errorf("list body = %q, want %q", got.Body, body)
	}
	if got.Signature != sig {
		t.Errorf("list signature = %q, want %q", got.Signature, sig)
	}
	if got.Method != http.MethodPost {
		t.Errorf("list method = %q, want POST", got.Method)
	}
	if got.ContentType != "application/json" {
		t.Errorf("list contentType = %q, want application/json", got.ContentType)
	}
	if got.Seq != 1 {
		t.Errorf("list seq = %d, want 1", got.Seq)
	}
	if got.ReceivedAt == 0 {
		t.Errorf("list receivedAt not stamped")
	}

	// --- An SSE subscriber must replay the buffered delivery. ---
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/__hooks/"+id+"/stream", nil)
	if err != nil {
		t.Fatalf("build SSE req: %v", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("SSE Content-Type = %q, want text/event-stream", ct)
	}
	reader := bufio.NewReader(res.Body)

	replayed := readDeliveryEvent(t, reader)
	if replayed.Body != body {
		t.Errorf("SSE replay body = %q, want %q", replayed.Body, body)
	}
	if replayed.Seq != 1 {
		t.Errorf("SSE replay seq = %d, want 1", replayed.Seq)
	}

	// --- A NEW delivery, posted while the subscriber is live, must stream. ---
	const body2 = `{"subscription_id":"sub-abc","wake_id":"w2","generation":4}`
	postDelivery(t, srv.URL, id, body2, "")

	live := readDeliveryEvent(t, reader)
	if live.Body != body2 {
		t.Errorf("SSE live body = %q, want %q", live.Body, body2)
	}
	if live.Seq != 2 {
		t.Errorf("SSE live seq = %d, want 2", live.Seq)
	}
	if live.Signature != "" {
		t.Errorf("SSE live signature = %q, want empty", live.Signature)
	}
}

// TestCaptureRingBufferEviction asserts the bounded buffer evicts the oldest and
// keeps the cap, with sequence numbers continuing to climb monotonically.
func TestCaptureRingBufferEviction(t *testing.T) {
	srv, _ := newCaptureServer(t)
	const id = "evict"
	total := captureBufferCap + 25
	for i := 0; i < total; i++ {
		postDelivery(t, srv.URL, id, fmt.Sprintf(`{"n":%d}`, i), "")
	}

	list := getDeliveries(t, srv.URL, id)
	if len(list) != captureBufferCap {
		t.Fatalf("buffered len = %d, want cap %d", len(list), captureBufferCap)
	}
	// Oldest retained is total-cap (0-indexed posts); seq is 1-based so the first
	// retained seq is (total-cap)+1 and they ascend by one.
	wantFirstSeq := uint64(total-captureBufferCap) + 1
	if list[0].Seq != wantFirstSeq {
		t.Errorf("first retained seq = %d, want %d", list[0].Seq, wantFirstSeq)
	}
	if list[len(list)-1].Seq != uint64(total) {
		t.Errorf("last seq = %d, want %d", list[len(list)-1].Seq, total)
	}
	for i := 1; i < len(list); i++ {
		if list[i].Seq != list[i-1].Seq+1 {
			t.Fatalf("seq not monotonic at %d: %d then %d", i, list[i-1].Seq, list[i].Seq)
		}
	}
}

// TestCaptureBucketsAreIsolated asserts deliveries to one id never bleed into
// another bucket's list.
func TestCaptureBucketsAreIsolated(t *testing.T) {
	srv, _ := newCaptureServer(t)
	postDelivery(t, srv.URL, "alpha", `{"x":1}`, "")
	postDelivery(t, srv.URL, "beta", `{"y":2}`, "")
	postDelivery(t, srv.URL, "beta", `{"y":3}`, "")

	if got := getDeliveries(t, srv.URL, "alpha"); len(got) != 1 {
		t.Errorf("alpha len = %d, want 1", len(got))
	}
	if got := getDeliveries(t, srv.URL, "beta"); len(got) != 2 {
		t.Errorf("beta len = %d, want 2", len(got))
	}
	if got := getDeliveries(t, srv.URL, "never-posted"); len(got) != 0 {
		t.Errorf("empty bucket len = %d, want 0", len(got))
	}
}

// TestCaptureUnsubscribeOnDisconnect asserts a client disconnect tears the
// subscriber down so it does not leak. We cancel the request context and then
// verify the bucket has no remaining subscribers.
func TestCaptureUnsubscribeOnDisconnect(t *testing.T) {
	srv, store := newCaptureServer(t)
	const id = "leak"

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/__hooks/"+id+"/stream", nil)
	if err != nil {
		t.Fatalf("build SSE req: %v", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	reader := bufio.NewReader(res.Body)
	// Read the opening comment so we know the handler has registered the subscriber.
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read SSE open: %v", err)
	}

	// Confirm the subscriber is registered.
	if n := subscriberCount(store, id); n != 1 {
		t.Fatalf("subscriber count = %d, want 1 while connected", n)
	}

	cancel()
	_ = res.Body.Close()

	// The handler's deferred unsubscribe runs once the context is observed. Poll
	// briefly for it rather than sleeping a fixed time.
	deadline := time.Now().Add(2 * time.Second)
	for subscriberCount(store, id) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("subscriber not cleaned up after disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func subscriberCount(store *captureStore, id string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	b := store.buckets[id]
	if b == nil {
		return 0
	}
	return len(b.subscribers)
}

// TestCaptureMissingIdRejected guards the empty-id edge (defensive; pattern
// routing normally supplies a non-empty {id}).
func TestCaptureMissingIdRejected(t *testing.T) {
	store := newCaptureStore()
	// Call the handler directly with no path value set.
	req := httptest.NewRequest(http.MethodPost, "/__hooks/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	handleCapturePost(store, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing-id POST status = %d, want 400", rec.Code)
	}
}
