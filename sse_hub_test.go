package chronicle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/store/segments"
)

type hubTestStore struct {
	store.Store
	wake          chan store.NotificationEvent
	onSubscribe   func()
	dropWake      atomic.Bool
	reads         atomic.Int64
	subscriptions atomic.Int64
	waits         atomic.Int64
	closes        atomic.Int64
	failNextWait  atomic.Bool
	afterReadAt   int64
	afterRead     func()
	gets          atomic.Int64
	afterGetAt    int64
	afterGet      func()
}

func newHubTestStore() *hubTestStore {
	return newHubTestStoreWithStore(store.NewMemoryStore())
}

func newHubTestStoreWithStore(st store.Store) *hubTestStore {
	return &hubTestStore{
		Store: st,
		wake:  make(chan store.NotificationEvent, 1),
	}
}

func (s *hubTestStore) Get(path string) (*store.StreamMetadata, error) {
	get := s.gets.Add(1)
	meta, err := s.Store.Get(path)
	if s.afterGet != nil && get == s.afterGetAt {
		s.afterGet()
	}
	return meta, err
}

func (s *hubTestStore) Read(path string, offset store.Offset) ([]store.Message, bool, error) {
	read := s.reads.Add(1)
	messages, upToDate, err := s.Store.Read(path, offset)
	if s.afterRead != nil && read == s.afterReadAt {
		s.afterRead()
	}
	return messages, upToDate, err
}

func (s *hubTestStore) Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error) {
	result, err := s.Store.Append(path, data, opts)
	if err == nil {
		s.signal(store.NotificationAppend)
	}
	return result, err
}

func (s *hubTestStore) CloseStream(path string) (*store.CloseResult, error) {
	result, err := s.Store.CloseStream(path)
	if err == nil {
		s.signal(store.NotificationClose)
	}
	return result, err
}

func (s *hubTestStore) CloseStreamWithProducer(
	path string,
	opts store.CloseProducerOptions,
) (*store.CloseProducerResult, error) {
	result, err := s.Store.CloseStreamWithProducer(path, opts)
	if err == nil {
		s.signal(store.NotificationClose)
	}
	return result, err
}

func (s *hubTestStore) Delete(path string) error {
	err := s.Store.Delete(path)
	if err == nil {
		s.signal(store.NotificationDelete)
	}
	return err
}

func (s *hubTestStore) SubscribeNotifications(
	context.Context,
	string,
) (store.NotificationSubscription, error) {
	s.subscriptions.Add(1)
	if s.onSubscribe != nil {
		s.onSubscribe()
	}
	return &hubTestSubscription{owner: s}, nil
}

func (s *hubTestStore) signal(event store.NotificationEvent) {
	if s.dropWake.Load() {
		return
	}
	select {
	case s.wake <- event:
	default:
	}
}

type hubTestSubscription struct {
	owner *hubTestStore
}

func (s *hubTestSubscription) Wait(ctx context.Context) (store.NotificationEvent, error) {
	s.owner.waits.Add(1)
	if s.owner.failNextWait.CompareAndSwap(true, false) {
		return store.NotificationAppend, store.ErrNotificationSubscriptionClosed
	}
	select {
	case event := <-s.owner.wake:
		return event, nil
	case <-ctx.Done():
		return store.NotificationAppend, ctx.Err()
	}
}

func (s *hubTestSubscription) Close() error {
	s.owner.closes.Add(1)
	return nil
}

func newHubTestHandler(st store.Store) *Handler {
	return &Handler{
		Store:                st,
		LongPollTimeout:      time.Second,
		SSEReconnectInterval: 10 * time.Second,
		SSEHubReplayBytes:    1 << 20,
		SSEHubBatchBytes:     256 << 10,
		SSEHubPollInterval:   5 * time.Second,
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestSSEHubSharesOneSubscriptionAndOneLiveRead(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()

	const clients = 24
	bodies := make([]io.ReadCloser, 0, clients)
	for range clients {
		resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
		if err != nil {
			t.Fatalf("GET sse: %v", err)
		}
		body := resp.Body
		defer body.Close() //nolint:errcheck // test cleanup; each body is also closed after its assertion
		bodies = append(bodies, body)
	}

	waitForCount(t, &st.subscriptions, 1)
	waitForAtLeast(t, &st.reads, clients)
	waitForStableCount(t, &st.reads)
	beforeAppend := st.reads.Load()

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("shared"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d body=%q", rec.Code, rec.Body.String())
	}

	for i, stream := range bodies {
		body, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		text := string(body)
		if !strings.Contains(text, "event: data\ndata:shared\n\n") {
			t.Fatalf("client %d missed shared data: %q", i, text)
		}
		if !strings.Contains(text, `"streamClosed":true`) {
			t.Fatalf("client %d missed close: %q", i, text)
		}
	}

	if got := st.subscriptions.Load(); got != 1 {
		t.Fatalf("subscriptions = %d, want one per active stream", got)
	}
	if got := st.reads.Load() - beforeAppend; got != 1 {
		t.Fatalf("live reads after append = %d, want one shared read", got)
	}
	waitForCount(t, &st.closes, 1)
}

func TestLongPollNowRegisterFirstSkipsAttachRecheck(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	h.LongPollTimeout = 20 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", nil)

	rec := do(h, http.MethodGet, "/test?offset=now&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %q", rec.Code, rec.Body.String())
	}
	if got := st.subscriptions.Load(); got != 1 {
		t.Fatalf("subscriptions = %d, want 1", got)
	}
	if got := st.waits.Load(); got != 1 {
		t.Fatalf("notification waits = %d, want 1", got)
	}
	if got := st.reads.Load(); got != 2 {
		t.Fatalf("reads = %d, want first page plus timeout page with no attach recheck", got)
	}
	if got := st.closes.Load(); got != 1 {
		t.Fatalf("subscription closes = %d, want 1", got)
	}
}

func TestLongPollNumericKeepsPreReadPath(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	h.LongPollTimeout = 20 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", nil)

	offset := store.ZeroOffset.String()
	rec := do(h, http.MethodGet, "/test?offset="+offset+"&live=long-poll", nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %q", rec.Code, rec.Body.String())
	}
	if got := st.subscriptions.Load(); got != 0 {
		t.Fatalf("handler notification subscriptions = %d, want numeric PageWaiter path", got)
	}
}

func TestSegmentSSEKeepsFinalSnapshotForLiveAttach(t *testing.T) {
	backend, err := segments.NewFileBackend(
		segments.ModeLocalFiles,
		t.TempDir(),
		1<<20,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	segmented, err := segments.New(store.NewMemoryStore(), segments.Options{
		Backend:      backend,
		InitialState: segments.StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = segmented.Close() })

	h := newHubTestHandler(segmented)
	h.SSEHubPollInterval = 10 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", []byte("seed"))

	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test cleanup

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("live"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d body=%q", rec.Code, rec.Body.String())
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "event: data\ndata:seed\n\n") ||
		!strings.Contains(text, "event: data\ndata:live\n\n") ||
		!strings.Contains(text, `"streamClosed":true`) {
		t.Fatalf("segment SSE failed across live attach:\n%s", text)
	}
}

func TestSSEHubSubscribeThenReadClosesAttachRace(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	var appendErr atomic.Value
	st.onSubscribe = func() {
		_, err := st.Store.Append("/test", []byte("raced"), store.AppendOptions{Close: true})
		if err != nil {
			appendErr.Store(err)
		}
	}

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	if err, _ := appendErr.Load().(error); err != nil {
		t.Fatalf("append during subscribe: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: data\ndata:raced\n\n") {
		t.Fatalf("attach race lost data: %q", body)
	}
	if !strings.Contains(body, `"streamClosed":true`) {
		t.Fatalf("attach race lost close: %q", body)
	}
	if got := st.subscriptions.Load(); got != 1 {
		t.Fatalf("subscriptions = %d, want 1", got)
	}
}

func TestSSEHubEmptyCatchupNeverCheckpointsUndeliveredAppend(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	// The subscription is confirmed before this client's first read. Append
	// after that read but before its metadata lookup.
	st.afterReadAt = 1
	st.afterRead = func() {
		if _, err := st.Append(
			"/test",
			[]byte("raced"),
			store.AppendOptions{ContentType: "text/plain", Close: true},
		); err != nil {
			t.Errorf("append during empty read/metadata race: %v", err)
		}
	}

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	body := rec.Body.String()
	dataAt := strings.Index(body, "event: data\ndata:raced\n\n")
	controlAt := strings.Index(body, "event: control")
	if dataAt < 0 {
		t.Fatalf("raced append was not delivered: %q", body)
	}
	if controlAt < dataAt {
		t.Fatalf("control checkpointed raced data before delivery: %q", body)
	}
	if !strings.Contains(body, `"streamClosed":true`) {
		t.Fatalf("raced close was not delivered: %q", body)
	}
}

func TestSSEHubCatchupDoesNotCrossStreamIncarnation(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	// The subscription is confirmed before this client's first read. Replace
	// the stream after that read but before its metadata lookup, so the request
	// must not emit data from the new incarnation.
	st.afterReadAt = 1
	st.afterRead = func() {
		if err := st.Store.Delete("/test"); err != nil {
			t.Errorf("delete old incarnation: %v", err)
			return
		}
		if _, _, err := st.Create(
			"/test",
			store.CreateOptions{ContentType: "text/plain"},
		); err != nil {
			t.Errorf("create new incarnation: %v", err)
			return
		}
		if _, err := st.Store.Append(
			"/test",
			[]byte("new-incarnation"),
			store.AppendOptions{ContentType: "text/plain", Close: true},
		); err != nil {
			t.Errorf("append new incarnation: %v", err)
		}
	}

	rec := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	body := rec.Body.String()
	if strings.Contains(body, "new-incarnation") {
		t.Fatalf("catch-up crossed stream incarnation: %q", body)
	}
	if strings.Contains(body, "stream was deleted and recreated") {
		t.Fatalf("ordinary HTTP error leaked into SSE framing: %q", body)
	}
}

func TestSSEHubPollRecoversDroppedNotification(t *testing.T) {
	st := newHubTestStore()
	st.dropWake.Store(true)
	h := newHubTestHandler(st)
	h.SSEHubPollInterval = 20 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	waitForCount(t, &st.subscriptions, 1)
	waitForAtLeast(t, &st.reads, 2)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("polled"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d", rec.Code)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE: %v", err)
	}
	if !strings.Contains(string(body), "data:polled") ||
		!strings.Contains(string(body), `"streamClosed":true`) {
		t.Fatalf("poll fallback lost data or close: %q", body)
	}
}

func TestSSEHubActiveReaderRenewsSlidingTTL(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	h.SSEHubPollInterval = 20 * time.Millisecond
	ttl := int64(1)
	if _, _, err := st.Create(
		"/test",
		store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl},
	); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(h)
	defer srv.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	waitForCount(t, &st.subscriptions, 1)
	time.Sleep(2200 * time.Millisecond)

	if _, err := st.Get("/test"); err != nil {
		t.Fatalf("active SSE reader did not keep sliding-TTL stream alive: %v", err)
	}
	if _, err := st.CloseStream("/test"); err != nil {
		t.Fatalf("close live stream: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE close: %v", err)
	}
	if !strings.Contains(string(body), `"streamClosed":true`) {
		t.Fatalf("active SSE reader missed close: %q", body)
	}

	time.Sleep(1100 * time.Millisecond)
	if _, err := st.Get("/test"); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("stream after reader release and TTL = %v, want not found", err)
	}
}

func TestSSEHubPollIntervalDoesNotOverflowAtMaxTTL(t *testing.T) {
	ttl := int64(1<<63 - 1)
	if got := capSSEHubPollForTTL(time.Second, &ttl); got != time.Second {
		t.Fatalf("poll interval = %s, want 1s for max TTL", got)
	}

	ttl = 3
	if got := capSSEHubPollForTTL(5*time.Second, &ttl); got != 1500*time.Millisecond {
		t.Fatalf("poll interval = %s, want 1.5s for TTL=3", got)
	}
}

func TestSSEHubPollTerminatesDeletedStreamIncarnation(t *testing.T) {
	st := newHubTestStore()
	st.dropWake.Store(true)
	h := newHubTestHandler(st)
	h.SSEHubPollInterval = 20 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	old, err := client.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET old SSE: %v", err)
	}
	defer old.Body.Close()
	waitForCount(t, &st.subscriptions, 1)

	deleted := do(h, http.MethodDelete, "/test", nil, nil)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d body=%q", deleted.Code, deleted.Body.String())
	}
	mustCreate(t, h, "/test", "text/plain", []byte("new-incarnation"))

	oldBody, err := io.ReadAll(old.Body)
	if err != nil {
		t.Fatalf("read old SSE: %v", err)
	}
	if strings.Contains(string(oldBody), "new-incarnation") {
		t.Fatalf("old hub crossed stream incarnation: %q", oldBody)
	}
	if strings.Contains(string(oldBody), "stream was deleted") ||
		strings.Contains(string(oldBody), "stream not found") {
		t.Fatalf("ordinary HTTP error leaked into SSE framing: %q", oldBody)
	}

	closed := do(h, http.MethodPost, "/test", map[string]string{
		protocol.HeaderStreamClosed: "true",
	}, nil)
	if closed.Code != http.StatusNoContent {
		t.Fatalf("close recreated stream: status = %d", closed.Code)
	}
	fresh := do(h, http.MethodGet, "/test?offset=-1&live=sse", nil, nil)
	if !strings.Contains(fresh.Body.String(), "data:new-incarnation") ||
		!strings.Contains(fresh.Body.String(), `"streamClosed":true`) {
		t.Fatalf("fresh hub missed recreated stream: %q", fresh.Body.String())
	}
}

func TestSSEHubRecreatedStreamReplacesHubWithOldLeaseStillOpen(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(1_765_000_000, 123))
	st := newHubTestStoreWithStore(store.NewMemoryStore(store.WithClock(clock)))
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)
	oldMeta, err := st.Get("/test")
	if err != nil {
		t.Fatal(err)
	}
	oldLease := h.acquireSSEHub("/test", store.ReadSnapshotFromMetadata(oldMeta), false, nil)
	defer oldLease.close()
	if err := oldLease.waitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := st.Delete("/test"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Create("/test", store.CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	newMeta, err := st.Get("/test")
	if err != nil {
		t.Fatal(err)
	}
	if !oldMeta.CreatedAt.Equal(newMeta.CreatedAt) {
		t.Fatalf("frozen CreatedAt changed: old=%s new=%s", oldMeta.CreatedAt, newMeta.CreatedAt)
	}
	if oldMeta.Incarnation == newMeta.Incarnation {
		t.Fatalf("recreated stream reused incarnation %q", oldMeta.Incarnation)
	}
	newLease := h.acquireSSEHub("/test", store.ReadSnapshotFromMetadata(newMeta), false, nil)
	defer newLease.close()
	if newLease.hub == oldLease.hub {
		t.Fatal("recreated stream reused the old incarnation's hub")
	}
	if err := newLease.waitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSSEAttachRejectsReplacementBetweenMetadataAndHub(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(1_765_000_000, 123))
	st := newHubTestStoreWithStore(store.NewMemoryStore(store.WithClock(clock)))
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "application/octet-stream", []byte("old"))

	st.afterGetAt = 1
	st.afterGet = func() {
		if err := st.Store.Delete("/test"); err != nil {
			t.Errorf("delete old stream: %v", err)
			return
		}
		if _, _, err := st.Create(
			"/test",
			store.CreateOptions{
				ContentType: "text/plain",
				InitialData: []byte("new-text"),
				Closed:      true,
			},
		); err != nil {
			t.Errorf("create replacement stream: %v", err)
		}
	}

	rec := do(h, http.MethodGet, "/test?offset=now&live=sse", nil, nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("replacement attach returned 200: %q", rec.Body.String())
	}
	if got := rec.Header().Get(protocol.HeaderStreamSSEDataEncoding); got != "" {
		t.Fatalf("replacement attach kept old binary encoding %q", got)
	}
	if strings.Contains(rec.Body.String(), "new-text") {
		t.Fatalf("replacement attach emitted new incarnation: %q", rec.Body.String())
	}
}

func TestSSEHubDuplicateNotificationDoesNotDuplicateData(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "application/json", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	waitForCount(t, &st.subscriptions, 1)
	waitForStableCount(t, &st.reads)

	before := st.reads.Load()
	mustAppend(t, h, "/test", "application/json", []byte(`{"n":1}`))
	waitForAtLeast(t, &st.reads, before+1)

	// Redis Pub/Sub is a hint, so duplicate hints may cause another durable
	// read but must not produce another data frame.
	st.signal(store.NotificationAppend)
	waitForAtLeast(t, &st.reads, before+2)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "application/json",
		protocol.HeaderStreamClosed: "true",
	}, []byte(`{"n":2}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d body=%q", rec.Code, rec.Body.String())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE: %v", err)
	}
	text := string(body)
	for _, marker := range []string{`"n":1}`, `"n":2}`} {
		if got := strings.Count(text, marker); got != 1 {
			t.Fatalf("%s occurrences = %d, want 1 in %q", marker, got, text)
		}
	}
}

func TestSSEHubSpuriousDeleteNotificationIsOnlyAWakeHint(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	waitForCount(t, &st.subscriptions, 1)
	waitForStableCount(t, &st.reads)

	before := st.reads.Load()
	st.signal(store.NotificationDelete)
	waitForAtLeast(t, &st.reads, before+1)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("still-live"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d body=%q", rec.Code, rec.Body.String())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "event: data\ndata:still-live\n\n") ||
		!strings.Contains(text, `"streamClosed":true`) {
		t.Fatalf("spurious delete stopped a valid hub: %q", text)
	}
}

func TestSSEHubReconnectsNotificationSubscription(t *testing.T) {
	st := newHubTestStore()
	st.failNextWait.Store(true)
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	waitForCount(t, &st.subscriptions, 2)

	rec := do(h, http.MethodPost, "/test", map[string]string{
		"Content-Type":              "text/plain",
		protocol.HeaderStreamClosed: "true",
	}, []byte("after-reconnect"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append and close: status = %d body=%q", rec.Code, rec.Body.String())
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read SSE: %v", err)
	}
	if !strings.Contains(string(body), "data:after-reconnect") ||
		!strings.Contains(string(body), `"streamClosed":true`) {
		t.Fatalf("subscription reconnect lost data or close: %q", body)
	}
	if got := st.subscriptions.Load(); got != 2 {
		t.Fatalf("subscriptions = %d, want one reconnect", got)
	}
}

func TestSSEHubDoesNotAdvertiseStaleUpToDate(t *testing.T) {
	st := newHubTestStore()
	if _, _, err := st.Create("/test", store.CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	first, err := st.Store.Append("/test", []byte("a"), store.AppendOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := st.Get("/test")
	if err != nil {
		t.Fatal(err)
	}
	st.afterRead = func() {
		if _, appendErr := st.Store.Append(
			"/test",
			[]byte("b"),
			store.AppendOptions{ContentType: "text/plain"},
		); appendErr != nil {
			t.Errorf("append during read/metadata race: %v", appendErr)
		}
	}
	st.afterReadAt = 1

	h := newHubTestHandler(st)
	hub := &sseHub{
		handler:     h,
		path:        "/test",
		contentType: "text/plain",
		replayLimit: 1 << 20,
		batchLimit:  1 << 20,
		current:     store.ZeroOffset,
		incarnation: meta.Incarnation,
		createdAt:   meta.CreatedAt,
		watchers:    make(map[*sseHubWatcher]struct{}),
		metrics:     nopSSEMetrics{},
	}
	watcher := hub.addWatcher()
	if err := hub.refresh(); err != nil {
		t.Fatal(err)
	}
	event, err := watcher.next(store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	if event == nil || !event.to.Equal(first.Offset) {
		t.Fatalf("first event = %#v", event)
	}
	if event.upToDate {
		t.Fatal("event advertised upToDate after metadata advanced beyond its offset")
	}
}

func TestSSEHubConcurrentClientsReceiveOrderedCompleteStream(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	mustCreate(t, h, "/test", "text/plain", nil)

	srv := httptest.NewServer(h)
	defer srv.Close()

	const (
		clients  = 32
		messages = 200
	)
	bodies := make([]io.ReadCloser, 0, clients)
	for range clients {
		resp, err := http.Get(srv.URL + "/test?offset=-1&live=sse")
		if err != nil {
			t.Fatalf("GET sse: %v", err)
		}
		body := resp.Body
		defer body.Close() //nolint:errcheck // test cleanup; each body is also closed after its assertion
		bodies = append(bodies, body)
	}
	waitForCount(t, &st.subscriptions, 1)
	waitForStableCount(t, &st.reads)
	before := st.reads.Load()

	for n := range messages {
		mustAppend(t, h, "/test", "text/plain", []byte(fmt.Sprintf("|%04d|", n)))
	}
	rec := do(h, http.MethodPost, "/test", map[string]string{
		protocol.HeaderStreamClosed: "true",
	}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("close: status = %d body=%q", rec.Code, rec.Body.String())
	}

	for i, stream := range bodies {
		body, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		text := string(body)
		previous := -1
		for n := range messages {
			marker := fmt.Sprintf("|%04d|", n)
			if got := strings.Count(text, marker); got != 1 {
				t.Fatalf("client %d marker %s occurrences = %d", i, marker, got)
			}
			index := strings.Index(text, marker)
			if index <= previous {
				t.Fatalf("client %d marker %s reordered at byte %d after %d", i, marker, index, previous)
			}
			previous = index
		}
		if !strings.Contains(text, `"streamClosed":true`) {
			t.Fatalf("client %d missed close", i)
		}
	}

	if got := st.subscriptions.Load(); got != 1 {
		t.Fatalf("subscriptions = %d, want one per active stream", got)
	}
	if got := st.reads.Load() - before; got > messages+1 {
		t.Fatalf("live reads = %d, want at most one per append plus close", got)
	}
}

func TestSSEReconnectTimerIsNotStarvedByReplayBacklog(t *testing.T) {
	st := newHubTestStore()
	h := newHubTestHandler(st)
	h.SSEHubBatchBytes = 1
	h.SSEReconnectInterval = 20 * time.Millisecond
	mustCreate(t, h, "/test", "text/plain", nil)

	const messages = 100
	// The client's authoritative empty page is the first read. Fill the durable
	// stream after its final metadata lookup and wake the already-confirmed hub,
	// so the hub's second read publishes one retained event per message.
	st.afterGetAt = 2
	st.afterGet = func() {
		for n := range messages {
			if _, err := st.Store.Append(
				"/test",
				[]byte(fmt.Sprintf("|%03d|", n)),
				store.AppendOptions{ContentType: "text/plain"},
			); err != nil {
				t.Errorf("append backlog message %d: %v", n, err)
				return
			}
		}
		st.signal(store.NotificationAppend)
	}

	recorder := &deadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		writeDelay:       2 * time.Millisecond,
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"/test?offset=-1&live=sse",
		nil,
	)
	start := time.Now()
	h.ServeHTTP(recorder, request)
	elapsed := time.Since(start)

	delivered := strings.Count(recorder.Body.String(), "event: data")
	if delivered == 0 {
		t.Fatal("replay backlog delivered no data before reconnect")
	}
	if delivered >= messages {
		t.Fatalf("reconnect timer was starved through all %d retained events", delivered)
	}
	if elapsed > time.Second {
		t.Fatalf("reconnect took %s with continuously available events", elapsed)
	}
}

func TestWriteSSEUpdateSetsAndClearsPerClientDeadline(t *testing.T) {
	recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	if err := writeSSEUpdate(
		recorder,
		[]byte("event: data\ndata:x\n\n"),
		store.Offset{ByteOffset: 1},
		"cursor",
		true,
		false,
		50*time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	if len(recorder.deadlines) != 2 {
		t.Fatalf("write deadlines = %v, want set then clear", recorder.deadlines)
	}
	if recorder.deadlines[0].IsZero() {
		t.Fatal("first write deadline was not set")
	}
	if !recorder.deadlines[1].IsZero() {
		t.Fatalf("final write deadline = %s, want cleared", recorder.deadlines[1])
	}
}

func TestSSEInitialHeaderFlushSetsAndClearsPerClientDeadline(t *testing.T) {
	for _, mounted := range []bool{false, true} {
		t.Run(fmt.Sprintf("mounted=%t", mounted), func(t *testing.T) {
			st := newHubTestStore()
			h := newHubTestHandler(st)
			h.SSEClientWriteTimeout = 50 * time.Millisecond
			mustCreate(t, h, "/test", "text/plain", nil)
			if _, err := st.Store.CloseStream("/test"); err != nil {
				t.Fatal(err)
			}

			var handler http.Handler = h
			target := "/test?offset=-1&live=sse"
			if mounted {
				var err error
				handler, err = Mount("/v1/stream/", h)
				if err != nil {
					t.Fatal(err)
				}
				target = "/v1/stream/test?offset=-1&live=sse"
			}
			recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
			request := httptest.NewRequest(http.MethodGet, target, nil)
			handler.ServeHTTP(recorder, request)

			if len(recorder.deadlines) < 4 {
				t.Fatalf("write deadlines = %v, want initial and control set/clear pairs", recorder.deadlines)
			}
			if recorder.deadlines[0].IsZero() {
				t.Fatal("initial header flush deadline was not set")
			}
			if !recorder.deadlines[1].IsZero() {
				t.Fatalf("initial header flush deadline = %s after flush, want cleared", recorder.deadlines[1])
			}
		})
	}
}

func TestSSEInitialHeaderFlushTimeoutReleasesHub(t *testing.T) {
	for _, mounted := range []bool{false, true} {
		t.Run(fmt.Sprintf("mounted=%t", mounted), func(t *testing.T) {
			st := newHubTestStore()
			metrics := &recordingSSEMetrics{}
			h := newHubTestHandler(st)
			h.SSEMetrics = metrics
			h.SSEClientWriteTimeout = 50 * time.Millisecond
			mustCreate(t, h, "/test", "text/plain", nil)

			var handler http.Handler = h
			target := "/test?offset=-1&live=sse"
			if mounted {
				var err error
				handler, err = Mount("/v1/stream/", h)
				if err != nil {
					t.Fatal(err)
				}
				target = "/v1/stream/test?offset=-1&live=sse"
			}
			recorder := &failingFlushRecorder{
				deadlineRecorder: &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()},
			}
			request := httptest.NewRequest(http.MethodGet, target, nil)
			handler.ServeHTTP(recorder, request)

			if got := metrics.writeTimeouts.Load(); got != 1 {
				t.Fatalf("write timeout metric = %d, want 1", got)
			}
			waitForCount(t, &st.closes, 1)
			if got := metrics.clients.Load(); got != 0 {
				t.Fatalf("active clients = %d, want 0 after initial flush timeout", got)
			}
			if got := metrics.hubs.Load(); got != 0 {
				t.Fatalf("active hubs = %d, want 0 after initial flush timeout", got)
			}
		})
	}
}

func TestWriteSSEUpdateDeadlinePassesThroughLocationRewriter(t *testing.T) {
	recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	wrapped := &locationRewriter{ResponseWriter: recorder, prefix: "/v1/stream"}
	if err := writeSSEUpdate(
		wrapped,
		[]byte("event: data\ndata:x\n\n"),
		store.Offset{ByteOffset: 1},
		"cursor",
		true,
		false,
		50*time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	if len(recorder.deadlines) != 2 {
		t.Fatalf("wrapped write deadlines = %v, want set then clear", recorder.deadlines)
	}
}

func TestSSEHubReplaySupportsInteriorOffset(t *testing.T) {
	h := newHubTestHandler(store.NewMemoryStore())
	hub := &sseHub{
		handler:     h,
		path:        "/test",
		contentType: "text/plain",
		replayLimit: 1 << 20,
		batchLimit:  1 << 20,
		current:     store.ZeroOffset,
		watchers:    make(map[*sseHubWatcher]struct{}),
		metrics:     nopSSEMetrics{},
	}
	watcher := hub.addWatcher()
	messages := []store.Message{
		{Data: []byte("a"), Offset: store.Offset{ByteOffset: 1}},
		{Data: []byte("b"), Offset: store.Offset{ByteOffset: 2}},
		{Data: []byte("c"), Offset: store.Offset{ByteOffset: 3}},
	}
	if err := hub.publish(store.ZeroOffset, messages, true, false, messages[2].Offset); err != nil {
		t.Fatal(err)
	}

	event, err := watcher.next(messages[0].Offset)
	if err != nil {
		t.Fatal(err)
	}
	if event == nil || event.to != messages[2].Offset {
		t.Fatalf("partial replay event = %#v", event)
	}
	if got := string(event.data); got != "event: data\ndata:bc\n\n" {
		t.Fatalf("partial replay data = %q", got)
	}
}

func TestSSEHubReplaySplitsByRetainedBytes(t *testing.T) {
	h := newHubTestHandler(store.NewMemoryStore())
	hub := &sseHub{
		handler:     h,
		path:        "/test",
		contentType: "text/plain",
		replayLimit: 200,
		batchLimit:  100,
		current:     store.ZeroOffset,
		watchers:    make(map[*sseHubWatcher]struct{}),
		metrics:     nopSSEMetrics{},
	}
	messages := []store.Message{
		{Data: []byte("012345678901234567890"), Offset: store.Offset{ByteOffset: 21}},
		{Data: []byte("123456789012345678901"), Offset: store.Offset{ByteOffset: 42}},
	}
	if err := hub.publish(store.ZeroOffset, messages, true, false, messages[1].Offset); err != nil {
		t.Fatal(err)
	}
	if len(hub.events) != 2 {
		t.Fatalf("replay events = %d, want 2 retained-byte-bounded events", len(hub.events))
	}
	for i, event := range hub.events {
		if event.memorySize > hub.batchLimit {
			t.Fatalf("event %d retained bytes = %d, batch limit = %d", i, event.memorySize, hub.batchLimit)
		}
	}
	if hub.replayBytes > hub.replayLimit {
		t.Fatalf("replay bytes = %d, limit = %d", hub.replayBytes, hub.replayLimit)
	}
}

func TestSSEHubRingBytesTracksRetainedBytesThroughEvictionAndCleanup(t *testing.T) {
	st := newHubTestStore()
	metrics := &recordingSSEMetrics{}
	h := newHubTestHandler(st)
	h.SSEMetrics = metrics
	h.SSEHubReplayBytes = 64
	h.SSEHubBatchBytes = 1
	mustCreate(t, h, "/test", "text/plain", nil)

	meta, err := st.Get("/test")
	if err != nil {
		t.Fatal(err)
	}
	lease := h.acquireSSEHub("/test", store.ReadSnapshotFromMetadata(meta), false, nil)
	if err := lease.waitReady(t.Context()); err != nil {
		t.Fatal(err)
	}

	messages := make([]store.Message, 8)
	for i := range messages {
		messages[i] = store.Message{
			Data:   []byte("0123456789"),
			Offset: store.Offset{ByteOffset: uint64((i + 1) * 10)},
		}
	}
	if err := lease.hub.publish(
		store.ZeroOffset,
		messages,
		true,
		false,
		messages[len(messages)-1].Offset,
	); err != nil {
		t.Fatal(err)
	}

	lease.hub.mu.Lock()
	retainedBytes := lease.hub.replayBytes
	retainedEvents := append([]*sseHubEvent(nil), lease.hub.events...)
	lease.hub.mu.Unlock()
	var summedBytes int
	for _, event := range retainedEvents {
		summedBytes += event.memorySize
	}
	if retainedBytes != summedBytes {
		t.Fatalf("retained bytes = %d, event sum = %d", retainedBytes, summedBytes)
	}
	if got := metrics.ringBytes.Load(); got != int64(retainedBytes) {
		t.Fatalf("ring-byte metric = %d, retained bytes = %d", got, retainedBytes)
	}
	if len(retainedEvents) >= len(messages) {
		t.Fatalf("retained events = %d, want eviction from %d published events", len(retainedEvents), len(messages))
	}

	lease.close()
	waitForCount(t, &metrics.ringBytes, 0)
	if got := metrics.hubs.Load(); got != 0 {
		t.Fatalf("active hubs = %d, want 0 after last-client cleanup", got)
	}
	if got := metrics.clients.Load(); got != 0 {
		t.Fatalf("active clients = %d, want 0 after last-client cleanup", got)
	}
}

func TestSSEHubDisconnectsClientOutsideReplayWindow(t *testing.T) {
	metrics := &recordingSSEMetrics{}
	h := newHubTestHandler(store.NewMemoryStore())
	h.SSEMetrics = metrics
	hub := &sseHub{
		handler:     h,
		path:        "/test",
		contentType: "text/plain",
		replayLimit: 64,
		batchLimit:  1,
		current:     store.ZeroOffset,
		watchers:    make(map[*sseHubWatcher]struct{}),
		metrics:     metrics,
	}
	watcher := hub.addWatcher()
	messages := make([]store.Message, 8)
	for i := range messages {
		messages[i] = store.Message{
			Data:   []byte("0123456789"),
			Offset: store.Offset{ByteOffset: uint64((i + 1) * 10)},
		}
	}
	if err := hub.publish(store.ZeroOffset, messages, true, false, messages[len(messages)-1].Offset); err != nil {
		t.Fatal(err)
	}

	if _, err := watcher.next(store.ZeroOffset); !errors.Is(err, errSSEHubLagged) {
		t.Fatalf("next outside replay window = %v, want %v", err, errSSEHubLagged)
	}
	if got := metrics.lagged.Load(); got != 1 {
		t.Fatalf("lagged metric = %d, want 1", got)
	}
}

type recordingSSEMetrics struct {
	lagged        atomic.Int64
	writeTimeouts atomic.Int64
	clients       atomic.Int64
	hubs          atomic.Int64
	ringBytes     atomic.Int64
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines  []time.Time
	writeDelay time.Duration
}

func (r *deadlineRecorder) Write(body []byte) (int, error) {
	if r.writeDelay > 0 {
		time.Sleep(r.writeDelay)
	}
	return r.ResponseRecorder.Write(body)
}

func (r *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	r.deadlines = append(r.deadlines, deadline)
	return nil
}

type failingFlushRecorder struct {
	*deadlineRecorder
}

func (r *failingFlushRecorder) Flush() {
	_ = r.FlushError()
}

func (r *failingFlushRecorder) FlushError() error {
	if len(r.deadlines) == 0 || r.deadlines[len(r.deadlines)-1].IsZero() {
		return errors.New("flush attempted without a write deadline")
	}
	return timeoutTestError{}
}

type timeoutTestError struct{}

func (timeoutTestError) Error() string { return "test write timeout" }
func (timeoutTestError) Timeout() bool { return true }

func (m *recordingSSEMetrics) SSEHubActive(delta int) {
	m.hubs.Add(int64(delta))
}

func (m *recordingSSEMetrics) SSEClientActive(delta int) {
	m.clients.Add(int64(delta))
}

func (*recordingSSEMetrics) SSEHubRead(int) {}
func (m *recordingSSEMetrics) SSEHubRingBytes(delta int) {
	m.ringBytes.Add(int64(delta))
}
func (m *recordingSSEMetrics) SSEClientLagged()          { m.lagged.Add(1) }
func (m *recordingSSEMetrics) SSEClientWriteTimeout()    { m.writeTimeouts.Add(1) }
func (*recordingSSEMetrics) SSESubscriptionActive(int)   {}
func (*recordingSSEMetrics) SSESubscriptionEvent(string) {}

func waitForCount(t *testing.T, value *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := value.Load(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("count = %d, want %d", value.Load(), want)
}

func waitForAtLeast(t *testing.T, value *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if value.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("count = %d, want at least %d", value.Load(), want)
}

func waitForStableCount(t *testing.T, value *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	last := value.Load()
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		current := value.Load()
		if current != last {
			last = current
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= 30*time.Millisecond {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("counter did not stabilize")
}
