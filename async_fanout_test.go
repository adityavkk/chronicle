package chronicle

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// blockingTailStore is a synchronization barrier at the first linked-tail read.
// The durable append itself does not use GetCurrentOffset, so reaching entered
// proves downstream wake evaluation has begun.
type blockingTailStore struct {
	store.Store
	enabled atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingTailStore) GetCurrentOffset(path string) (store.Offset, error) {
	if s.enabled.Load() {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Store.GetCurrentOffset(path)
}

func TestHTTPAppendReturnsWhileSubscriptionTailReadIsBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in short mode")
	}
	client := itestRedis(t)
	memory := store.NewMemoryStore()
	blocked := &blockingTailStore{
		Store:   memory,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router, service, _, err := NewSubscriptions(client, blocked, nil,
		"http://example.test/v1/stream/", false,
		SubscriptionTuning{SweepInterval: time.Hour, ReconcileInterval: time.Hour}, logger)
	if err != nil {
		t.Fatal(err)
	}
	h := testHandler(time.Second, time.Second)
	h.Store = blocked
	h.Subscriptions = router
	h.SubHooks = service

	mustCreate(t, h, "/events/a", "text/plain", nil)
	mustCreate(t, h, "/wake/async", "application/json", nil)
	createSub := do(h, http.MethodPut, "/__ds/subscriptions/async", nil, []byte(`{
		"type":"pull-wake",
		"streams":["events/a"],
		"wake_stream":"wake/async",
		"lease_ttl_ms":30000
	}`))
	if createSub.Code != http.StatusCreated {
		t.Fatalf("create subscription = %d: %s", createSub.Code, createSub.Body.String())
	}

	service.Start()
	defer service.Stop()
	blocked.enabled.Store(true)

	appendDone := make(chan *responseResult, 1)
	go func() {
		rec := do(h, http.MethodPost, "/events/a", map[string]string{"Content-Type": "text/plain"}, []byte("x"))
		appendDone <- &responseResult{code: rec.Code, body: rec.Body.String()}
	}()

	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("async worker did not reach the linked-tail barrier")
	}

	select {
	case result := <-appendDone:
		if result.code != http.StatusNoContent || result.body != "" {
			t.Fatalf("append response = %d %q, want 204 with empty body", result.code, result.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP append waited for blocked subscription tail evaluation")
	}

	close(blocked.release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messages, timedOut, _, err := memory.WaitForMessages(ctx, "/wake/async", store.ZeroOffset, 2*time.Second)
	if err != nil || timedOut || len(messages) != 1 {
		t.Fatalf("eventual wake = messages:%d timeout:%v err:%v", len(messages), timedOut, err)
	}
	var event webhook.WakeEvent
	if err := json.Unmarshal(messages[0].Data, &event); err != nil {
		t.Fatal(err)
	}
	if event.SubscriptionID != "async" || event.Stream != "events/a" {
		t.Fatalf("wake event = %+v", event)
	}
}

type responseResult struct {
	code int
	body string
}
