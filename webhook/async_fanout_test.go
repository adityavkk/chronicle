package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type observedBlockingTransport struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (t *observedBlockingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	t.once.Do(func() { close(t.entered) })
	<-t.release
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: &observedBody{
			Reader: strings.NewReader(`{}`),
			done:   t.done,
		},
	}, nil
}

func (t *observedBlockingTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

type observedBody struct {
	io.Reader
	done chan struct{}
	once sync.Once
}

func (b *observedBody) Close() error {
	b.once.Do(func() { close(b.done) })
	return nil
}

type countingBatchStreams struct {
	inner *fakeStreams
	mu    sync.Mutex
	calls int
	paths []string
}

func (s *countingBatchStreams) TailOffset(path string) (string, bool) {
	return s.inner.TailOffset(path)
}

func (s *countingBatchStreams) TailOffsets(paths []string) (map[string]string, error) {
	s.mu.Lock()
	s.calls++
	s.paths = append([]string(nil), paths...)
	s.mu.Unlock()
	return s.inner.TailOffsets(paths)
}

func (s *countingBatchStreams) BeginningOffset() string { return s.inner.BeginningOffset() }

func (s *countingBatchStreams) AppendWakeEvent(path string, data []byte) error {
	return s.inner.AppendWakeEvent(path, data)
}

type blockingFanoutStore struct {
	Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingFanoutStore) StreamSubscribers(path string) ([]string, int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Store.StreamSubscribers(path)
}

type failOnceFanoutStore struct {
	Store
	mu     sync.Mutex
	failed bool
}

func (s *failOnceFanoutStore) StreamSubscribers(path string) ([]string, int, error) {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		s.mu.Unlock()
		return nil, 0, errors.New("injected subscriber lookup failure")
	}
	s.mu.Unlock()
	return s.Store.StreamSubscribers(path)
}

type blockingListStore struct {
	Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingListStore) List() ([]string, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Store.List()
}

type dirtyRecordingMetrics struct {
	NopMetrics
	mu             sync.Mutex
	enqueues       map[string]int
	overflows      int
	processErrors  map[string]int
	reconcile      map[string]int
	processedSubs  int
	processedWakes int
}

func (m *dirtyRecordingMetrics) DirtyEnqueue(result string, _, _ int, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enqueues == nil {
		m.enqueues = map[string]int{}
	}
	m.enqueues[result]++
}

func (m *dirtyRecordingMetrics) DirtyOverflow() {
	m.mu.Lock()
	m.overflows++
	m.mu.Unlock()
}

func (m *dirtyRecordingMetrics) DirtyProcessingError(stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processErrors == nil {
		m.processErrors = map[string]int{}
	}
	m.processErrors[stage]++
}

func (m *dirtyRecordingMetrics) ReconcileRequest(scope, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reconcile == nil {
		m.reconcile = map[string]int{}
	}
	m.reconcile[scope+"/"+result]++
}

func (m *dirtyRecordingMetrics) DirtyProcess(_ time.Duration, subs, wakes, _ int, _ string) {
	m.mu.Lock()
	m.processedSubs += subs
	m.processedWakes += wakes
	m.mu.Unlock()
}

func TestOnStreamAppendIsOnlyBoundedHandoff(t *testing.T) {
	base, _ := newTestStore(t)
	barrier := &blockingFanoutStore{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(barrier, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}

	returned := make(chan struct{})
	go func() {
		mgr.OnStreamAppend("events/a")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("OnStreamAppend blocked")
	}
	select {
	case <-barrier.entered:
		t.Fatal("OnStreamAppend performed subscriber lookup")
	default:
	}

	processed := make(chan int, 1)
	go func() { processed <- mgr.RunDirtyWorker() }()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("async worker did not reach subscriber lookup")
	}
	close(barrier.release)
	if got := <-processed; got != 1 {
		t.Fatalf("processed = %d, want 1", got)
	}
}

func TestManagerDirtyBurstCoalesces(t *testing.T) {
	mgr, _, _ := newTestManager(t)
	mgr.dirtyMu.Lock()
	mgr.dirty = newDirtyQueue(8)
	mgr.dirtyMu.Unlock()

	for range 100 {
		mgr.OnStreamAppend("events/hot")
	}
	mgr.dirtyMu.Lock()
	stats := mgr.dirty.stats(mgr.now())
	mgr.dirtyMu.Unlock()
	if stats.depth != 1 {
		t.Fatalf("burst queue depth = %d, want 1", stats.depth)
	}
}

func TestDirtyFanoutBatchesHydrationAndDistinctLinkedTails(t *testing.T) {
	base, _ := newTestStore(t)
	inner := &fakeStreams{tails: map[string]string{
		"events/a":      "0000000000000001_0000000000000000",
		"events/shared": "0000000000000001_0000000000000000",
		"events/unique": "0000000000000001_0000000000000000",
	}}
	streams := &countingBatchStreams{inner: inner}
	mgr, err := NewManager(base, streams, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	begin := inner.BeginningOffset()
	for _, tc := range []struct {
		id    string
		links []string
	}{
		{"s1", []string{"events/a", "events/shared"}},
		{"s2", []string{"events/a", "events/shared", "events/unique"}},
	} {
		cfg := pullWakeCfg()
		cfg.Pattern = ""
		cfg.Streams = append([]string(nil), tc.links...)
		cfg.WakeStream = "wake/" + tc.id
		if _, err := base.CreateOrConfirm(tc.id, cfg, nil, now); err != nil {
			t.Fatal(err)
		}
		for _, path := range tc.links {
			if err := base.Link(tc.id, path, LinkExplicit, begin); err != nil {
				t.Fatal(err)
			}
		}
	}

	mgr.OnStreamAppend("events/a")
	if got := mgr.RunDirtyWorker(); got != 1 {
		t.Fatalf("processed streams = %d, want 1", got)
	}
	streams.mu.Lock()
	calls, paths := streams.calls, append([]string(nil), streams.paths...)
	streams.mu.Unlock()
	if calls != 1 || len(paths) != 3 {
		t.Fatalf("TailOffsets calls/paths = %d/%v, want one batch with 3 distinct paths", calls, paths)
	}
	if inner.count() != 2 {
		t.Fatalf("wakes = %d, want one per subscription", inner.count())
	}
}

func TestDirtyFanoutPreservesWebhookDispatch(t *testing.T) {
	base, _ := newTestStore(t)
	streams := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	post := &observedBlockingTransport{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	mgr, err := NewManager(base, streams, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		HTTPClient:    &http.Client{Transport: post},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, streams.BeginningOffset()); err != nil {
		t.Fatal(err)
	}

	mgr.OnStreamAppend("events/a")
	mgr.RunDirtyWorker()
	select {
	case <-post.entered:
	case <-time.After(time.Second):
		t.Fatal("async dirty fan-out did not dispatch webhook")
	}
	if got := post.count(); got != 1 {
		t.Fatalf("webhook calls = %d, want 1", got)
	}
	close(post.release)
	select {
	case <-post.done:
	case <-time.After(time.Second):
		t.Fatal("webhook delivery did not finish")
	}
}

func TestConcurrentDirtyAppendsCoalesceRaceFree(t *testing.T) {
	mgr, _, _ := newTestManager(t)
	const appends = 256
	var wg sync.WaitGroup
	for range appends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.OnStreamAppend("events/a")
		}()
	}
	wg.Wait()
	mgr.dirtyMu.Lock()
	stats := mgr.dirty.stats(mgr.now())
	mgr.dirtyMu.Unlock()
	if stats.depth != 1 {
		t.Fatalf("concurrent queue depth = %d, want 1", stats.depth)
	}
}

func TestDeletedStreamHintDoesNotWake(t *testing.T) {
	mgr, base, fs := newTestManager(t)
	if _, err := base.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	fs.tails["events/a"] = "0000000000000001_0000000000000000"
	fs.mu.Unlock()

	mgr.OnStreamAppend("events/a")
	mgr.OnStreamDeleted("events/a")
	mgr.RunDirtyWorker()
	if fs.count() != 0 {
		t.Fatalf("deleted stream emitted %d wakes", fs.count())
	}
}

func TestDirtyFanoutSurvivesConcurrentOwnerTransfer(t *testing.T) {
	base, _ := newTestStore(t)
	barrier := &blockingFanoutStore{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	fs := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	mgr, err := NewManager(barrier, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}
	h := slotOf("s1")
	t0 := time.Unix(1_700_000_000, 0)
	if claim, err := base.ClaimSlot(slotKey(h), "owner-a", t0, slotTTL); err != nil || !claim.Granted() {
		t.Fatalf("owner A claim = %+v err=%v", claim, err)
	}

	mgr.OnStreamAppend("events/a")
	done := make(chan int, 1)
	go func() { done <- mgr.RunDirtyWorker() }()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("dirty fan-out did not reach subscriber lookup")
	}
	if claim, err := base.ClaimSlot(slotKey(h), "owner-b", t0.Add(slotTTL+time.Second), slotTTL); err != nil || !claim.Transferred() {
		t.Fatalf("owner B transfer = %+v err=%v", claim, err)
	}
	close(barrier.release)
	if processed := <-done; processed != 1 || fs.count() != 1 {
		t.Fatalf("after transfer processed/wakes = %d/%d, want 1/1", processed, fs.count())
	}
}

func TestDirtyOverflowUsesRecoveryWithoutLosingWake(t *testing.T) {
	base, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{
		"events/a": "0000000000000001_0000000000000000",
		"events/b": "0000000000000001_0000000000000000",
	}}
	metrics := &dirtyRecordingMetrics{}
	mgr, err := NewManager(base, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	mgr.dirtyMu.Lock()
	mgr.dirty = newDirtyQueue(1)
	mgr.dirtyMu.Unlock()

	now := time.Now()
	begin := fs.BeginningOffset()
	for _, tc := range []struct{ id, path string }{{"a", "events/a"}, {"b", "events/b"}} {
		cfg := pullWakeCfg()
		cfg.Pattern = ""
		cfg.Streams = []string{tc.path}
		cfg.WakeStream = "wake/" + tc.id
		if _, err := base.CreateOrConfirm(tc.id, cfg, nil, now); err != nil {
			t.Fatal(err)
		}
		if err := base.Link(tc.id, tc.path, LinkExplicit, begin); err != nil {
			t.Fatal(err)
		}
	}

	mgr.OnStreamAppend("events/a")
	mgr.OnStreamAppend("events/b") // capacity one: represented by overflow epoch
	mgr.reconcile(scopeDirtyOverflow)

	if fs.count() != 2 {
		t.Fatalf("overflow recovery emitted %d wakes, want 2", fs.count())
	}
	mgr.dirtyMu.Lock()
	overflowPending := mgr.dirty.hasPendingOverflow()
	mgr.dirtyMu.Unlock()
	if overflowPending {
		t.Fatal("successful recovery did not clear overflow epoch")
	}
	metrics.mu.Lock()
	overflows := metrics.overflows
	metrics.mu.Unlock()
	if overflows != 1 {
		t.Fatalf("overflow metric = %d, want 1", overflows)
	}

	// The retained queued hint is duplicate work after the recovery wake. Its
	// generation fence must prevent a second event.
	mgr.RunDirtyWorker()
	if fs.count() != 2 {
		t.Fatalf("duplicate queued work emitted another wake: %d", fs.count())
	}
}

func TestDirtyWorkerErrorRetriesAndRequestsRecovery(t *testing.T) {
	base, _ := newTestStore(t)
	failing := &failOnceFanoutStore{Store: base}
	fs := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	metrics := &dirtyRecordingMetrics{}
	mgr, err := NewManager(failing, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := base.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}

	mgr.OnStreamAppend("events/a")
	if got := mgr.RunDirtyWorker(); got != 1 {
		t.Fatalf("failed pass processed = %d", got)
	}
	if fs.count() != 0 {
		t.Fatal("failed subscriber lookup emitted a wake")
	}
	mgr.dirtyMu.Lock()
	ready := mgr.dirty.hasReady()
	mgr.dirtyMu.Unlock()
	if !ready {
		t.Fatal("failed dirty work was not requeued")
	}

	if got := mgr.RunDirtyWorker(); got != 1 || fs.count() != 1 {
		t.Fatalf("retry processed=%d wakes=%d, want 1/1", got, fs.count())
	}
	metrics.mu.Lock()
	lookupErrors := metrics.processErrors["lookup"]
	reconcileRequests := metrics.reconcile["append-error/enqueued"]
	metrics.mu.Unlock()
	if lookupErrors != 1 || reconcileRequests != 1 {
		t.Fatalf("error/reconcile metrics = %d/%d, want 1/1", lookupErrors, reconcileRequests)
	}
}

func TestManagerStopDuringDirtyWorkIsIdempotent(t *testing.T) {
	base, _ := newTestStore(t)
	barrier := &blockingFanoutStore{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(barrier, fs, ManagerOptions{
		StreamRootURL:     "http://x/v1/stream/",
		SweepInterval:     time.Hour,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.Start()
	mgr.Start()
	mgr.OnStreamAppend("events/a")
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("dirty worker did not start")
	}

	stopped := make(chan struct{})
	go func() {
		mgr.Stop()
		mgr.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while Manager-owned dirty work was still running")
	default:
	}
	close(barrier.release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not drain after dirty work was released")
	}

	// A stopped manager rejects later low-latency hints without panicking. The
	// next process boot sweep remains the durable repair path.
	mgr.OnStreamAppend("events/after-stop")
}

func TestStopRacingStartDoesNotLeakWorker(t *testing.T) {
	base, _ := newTestStore(t)
	barrier := &blockingListStore{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(barrier, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	go func() {
		mgr.Start()
		close(started)
	}()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach boot reconcile")
	}
	stopped := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopped)
	}()
	close(barrier.release)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not finish after cancellation")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop racing Start did not finish")
	}
}

func TestLostDirtyHintRecoversAfterManagerRestart(t *testing.T) {
	base, _ := newTestStore(t)
	fs := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	now := time.Now()
	if _, err := base.CreateOrConfirm("s1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}

	first, err := NewManager(base, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	first.OnStreamAppend("events/a")
	first.Stop() // crash boundary: queued latency hint is abandoned
	if fs.count() != 0 {
		t.Fatal("stopped manager unexpectedly processed its dirty hint")
	}

	second, err := NewManager(base, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	second.RunSweep()
	if fs.count() != 1 {
		t.Fatalf("restart recovery emitted %d wakes, want 1", fs.count())
	}
}

func TestRedisDisconnectReconnectRecoversLostDirtyHint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis fault test in short mode")
	}
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/14"
	}
	opts, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	opts.PoolSize = 1
	client := goredis.NewClient(opts)
	controlOpts := *opts
	controlOpts.PoolSize = 1
	control := goredis.NewClient(&controlOpts)
	t.Cleanup(func() {
		_ = client.Close()
		_ = control.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable at %s: %v", rawURL, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	store := NewRedisStore(client)
	fs := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOrConfirm("s1", pullWakeCfg(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Link("s1", "events/a", LinkGlob, fs.BeginningOffset()); err != nil {
		t.Fatal(err)
	}
	mgr.OnStreamAppend("events/a")

	oldID, err := client.ClientID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	killed, err := control.ClientKillByFilter(ctx, "ID", strconv.FormatInt(oldID, 10)).Result()
	if err != nil || killed != 1 {
		t.Fatalf("kill Redis connection %d = %d/%v", oldID, killed, err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis client did not reconnect: %v", err)
	}
	newID, err := client.ClientID(ctx).Result()
	if err != nil || newID == oldID {
		t.Fatalf("Redis connection id after reconnect = %d/%v, old %d", newID, err, oldID)
	}

	// The connection callback is a bounded signal. Drive the queued recovery
	// deterministically and prove the append whose process hint was abandoned is
	// still derived from durable cursor and tail state.
	mgr.OnRedisReconnect()
	select {
	case recoveryScope := <-mgr.reconcileC:
		mgr.reconcile(recoveryScope)
	case <-time.After(time.Second):
		t.Fatal("Redis reconnect did not request eager recovery")
	}
	if fs.count() != 1 {
		t.Fatalf("reconnect recovery emitted %d wakes, want 1", fs.count())
	}
}
