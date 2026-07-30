package chronicle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestCatchupFlushesBoundedPagesIncrementally(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,22,333]`),
	}); err != nil {
		t.Fatal(err)
	}
	counted := &countingPageStore{Store: s, reader: s}
	h := &Handler{Store: counted, ReadPageBytes: 2}
	w := newFlushRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil))

	if w.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.status)
	}
	if got := w.body.String(); got != `[1,22,333]` {
		t.Fatalf("body = %q", got)
	}
	if counted.calls.Load() != 3 {
		t.Fatalf("ReadPage calls = %d, want 3", counted.calls.Load())
	}
	if len(w.flushBodies) < 3 {
		t.Fatalf("flushes = %d, want at least 3", len(w.flushBodies))
	}
	if got := w.flushBodies[0]; got != `[1` {
		t.Fatalf("first flushed body = %q, want first complete record only", got)
	}
	if w.maxWrite >= w.body.Len() {
		t.Fatalf("largest Write = %d, complete body = %d: response was buffered", w.maxWrite, w.body.Len())
	}
	if got := w.header.Get(protocol.HeaderStreamNextOffset); got != (store.Offset{ByteOffset: 6}).String() {
		t.Fatalf("next offset = %q", got)
	}
}

func TestCatchupSnapshotExcludesAppendAfterFirstFlush(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2,3]`),
	}); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: s, ReadPageBytes: 1}
	w := newFlushRecorder()
	w.onFirstFlush = func() {
		if _, err := s.Append("/stream", []byte(`[4]`), store.AppendOptions{ContentType: "application/json"}); err != nil {
			t.Fatal(err)
		}
	}
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil))
	if got := w.body.String(); got != `[1,2,3]` {
		t.Fatalf("captured body = %q, want pre-append snapshot", got)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream?offset=0_3", nil))
	if got := rec.Body.String(); got != `[4]` {
		t.Fatalf("next response = %q, want concurrent append", got)
	}
}

func TestCatchupCancellationBetweenPagesStopsStorageWork(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("aa"),
	}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"bb", "cc"} {
		if _, err := s.Append("/stream", []byte(body), store.AppendOptions{ContentType: "application/octet-stream"}); err != nil {
			t.Fatal(err)
		}
	}
	counted := &countingPageStore{Store: s, reader: s}
	metrics := &readMetricsRecorder{}
	h := &Handler{Store: counted, ReadPageBytes: 2, ReadMetrics: metrics}
	ctx, cancel := context.WithCancel(context.Background())
	w := newFlushRecorder()
	w.onFirstFlush = cancel
	req := httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil).WithContext(ctx)
	h.ServeHTTP(w, req)

	if got := w.body.String(); got != "aa" {
		t.Fatalf("body after cancellation = %q, want one complete frame", got)
	}
	if counted.calls.Load() != 1 {
		t.Fatalf("ReadPage calls = %d, want 1", counted.calls.Load())
	}
	if metrics.cancellations["flush"] != 1 {
		t.Fatalf("flush cancellations = %d, want 1", metrics.cancellations["flush"])
	}
}

func TestCatchupCancellationBeforeFirstPageSkipsStorageWork(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("x"),
	}); err != nil {
		t.Fatal(err)
	}
	counted := &countingPageStore{Store: s, reader: s}
	metrics := &readMetricsRecorder{}
	h := &Handler{Store: counted, ReadPageBytes: 1, ReadMetrics: metrics}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil).WithContext(ctx)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if counted.calls.Load() != 0 {
		t.Fatalf("ReadPage calls = %d, want 0", counted.calls.Load())
	}
	if metrics.cancellations["before_first_page"] != 1 {
		t.Fatalf("before-first-page cancellations = %d, want 1", metrics.cancellations["before_first_page"])
	}
}

func TestLegacyPageReaderRejectsDeleteRecreateDuringRead(t *testing.T) {
	t.Parallel()

	base := store.NewMemoryStore()
	if _, _, err := base.Create("/stream", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("old"),
	}); err != nil {
		t.Fatal(err)
	}
	legacy := &recreatingLegacyStore{Store: base, path: "/stream"}
	_, err := (legacyPageReader{store: legacy}).ReadPage(
		context.Background(),
		"/stream",
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("error = %v, want ErrReadSnapshotChanged", err)
	}
}

func TestLegacyPageReaderHonorsFrameAndByteBounds(t *testing.T) {
	t.Parallel()

	base := store.NewMemoryStore()
	if _, _, err := base.Create("/stream", store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,22,333]`),
	}); err != nil {
		t.Fatal(err)
	}
	reader := legacyPageReader{store: legacyOnlyStore{Store: base}}
	page, err := reader.ReadPage(
		context.Background(),
		"/stream",
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 2, MaxFrames: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || string(page.Messages[0].Data) != "1" {
		t.Fatalf("messages = %+v, want first whole frame only", page.Messages)
	}
	if page.UpToDate {
		t.Fatal("bounded compatibility page incorrectly reported up-to-date")
	}
	if page.Stats.FetchedBytes != 6 || page.Stats.ReturnedBytes != 1 || page.Stats.DiscardedBytes != 5 {
		t.Fatalf("stats = %+v", page.Stats)
	}
}

func TestLegacyPageReaderStopsBeforeNonFittingFrame(t *testing.T) {
	t.Parallel()

	base := store.NewMemoryStore()
	want := []byte("aaaaaabbbbbbbbbbc")
	if _, _, err := base.Create("/stream", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: want[:6],
	}); err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{want[6:16], want[16:]} {
		if _, err := base.Append("/stream", body, store.AppendOptions{
			ContentType: "application/octet-stream",
		}); err != nil {
			t.Fatal(err)
		}
	}

	reader := legacyPageReader{store: legacyOnlyStore{Store: base}}
	offset := store.ZeroOffset
	var snapshot *store.ReadSnapshot
	var got []byte
	var pageSizes []int
	for {
		page, err := reader.ReadPage(
			context.Background(),
			"/stream",
			offset,
			store.ReadPageOptions{
				TargetBytes: 8,
				MaxFrames:   store.DefaultReadPageFrames,
				Snapshot:    snapshot,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot == nil {
			snapshot = &page.Snapshot
		}
		for _, message := range page.Messages {
			got = append(got, message.Data...)
			pageSizes = append(pageSizes, len(message.Data))
		}
		if page.UpToDate {
			break
		}
		if len(page.Messages) == 0 || page.NextOffset.Equal(offset) {
			t.Fatalf("page made no progress: %+v", page)
		}
		offset = page.NextOffset
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("concatenated pages = %q, want %q", got, want)
	}
	if gotSizes, wantSizes := pageSizes, []int{6, 10, 1}; !slices.Equal(gotSizes, wantSizes) {
		t.Fatalf("page frame sizes = %v, want %v", gotSizes, wantSizes)
	}
}

func TestCatchupCancellationWhileStorageQueuedReturns(t *testing.T) {
	base := store.NewMemoryStore()
	if _, _, err := base.Create("/stream", store.CreateOptions{ContentType: "application/octet-stream", InitialData: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingPageStore{
		Store:   base,
		entered: make(chan struct{}),
	}
	metrics := &readMetricsRecorder{}
	h := &Handler{Store: blocked, ReadPageBytes: 1, ReadMetrics: metrics}
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()

	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("ReadPage did not enter")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler goroutine did not exit after storage cancellation")
	}
	if metrics.cancellations["storage"] != 1 {
		t.Fatalf("storage cancellations = %d, want 1", metrics.cancellations["storage"])
	}
}

func TestCatchupSocketWriteFailureStopsAtFrameBoundary(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("aa"),
	}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"bb", "cc"} {
		if _, err := s.Append("/stream", []byte(body), store.AppendOptions{ContentType: "application/octet-stream"}); err != nil {
			t.Fatal(err)
		}
	}
	counted := &countingPageStore{Store: s, reader: s}
	metrics := &readMetricsRecorder{}
	h := &Handler{Store: counted, ReadPageBytes: 2, ReadMetrics: metrics}
	w := newFlushRecorder()
	w.failWriteCall = 2
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream?offset=-1", nil))

	if got := w.body.String(); got != "aa" {
		t.Fatalf("body = %q, want first complete frame", got)
	}
	if counted.calls.Load() != 2 {
		t.Fatalf("ReadPage calls = %d, want current and failed page only", counted.calls.Load())
	}
	if metrics.cancellations["write"] != 1 {
		t.Fatalf("write cancellations = %d, want 1", metrics.cancellations["write"])
	}
}

func TestCatchupInternalPageFailureAbortsCommittedTransport(t *testing.T) {
	t.Parallel()

	for _, protocolTest := range []struct {
		name      string
		start     func(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client)
		wantMajor int
	}{
		{
			name: "http1",
			start: func(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
				t.Helper()
				server := httptest.NewServer(handler)
				return server, server.Client()
			},
			wantMajor: 1,
		},
		{
			name: "http2",
			start: func(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
				t.Helper()
				server := httptest.NewUnstartedServer(handler)
				server.EnableHTTP2 = true
				server.StartTLS()
				return server, server.Client()
			},
			wantMajor: 2,
		},
	} {
		t.Run(protocolTest.name, func(t *testing.T) {
			t.Parallel()
			for _, failure := range []string{"read-error", "no-progress"} {
				t.Run(failure, func(t *testing.T) {
					base := store.NewMemoryStore()
					if _, _, err := base.Create("/stream", store.CreateOptions{
						ContentType: "application/octet-stream",
						InitialData: []byte("aa"),
					}); err != nil {
						t.Fatal(err)
					}
					if _, err := base.Append("/stream", []byte("bb"), store.AppendOptions{}); err != nil {
						t.Fatal(err)
					}
					broken := &secondPageFailureStore{
						Store:   base,
						reader:  base,
						failure: failure,
					}
					handler := &Handler{
						Store:         broken,
						ReadPageBytes: 2,
						Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
					}
					server, client := protocolTest.start(t, handler)
					t.Cleanup(server.Close)

					resp, err := client.Get(server.URL + "/stream?offset=-1")
					if err != nil {
						t.Fatalf("GET before response headers: %v", err)
					}
					body, readErr := io.ReadAll(resp.Body)
					_ = resp.Body.Close()

					if resp.ProtoMajor != protocolTest.wantMajor {
						t.Fatalf("protocol = %s, want HTTP/%d", resp.Proto, protocolTest.wantMajor)
					}
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("status = %d, want 200", resp.StatusCode)
					}
					if got := resp.Header.Get(protocol.HeaderStreamNextOffset); got != (store.Offset{ByteOffset: 4}).String() {
						t.Fatalf("advertised tail = %q, want byte offset 4", got)
					}
					if got := resp.Header.Get(protocol.HeaderStreamUpToDate); got != "true" {
						t.Fatalf("Stream-Up-To-Date = %q, want true", got)
					}
					if got := string(body); got != "aa" {
						t.Fatalf("partial body = %q, want first page %q", got, "aa")
					}
					if readErr == nil {
						t.Fatal("partial body ended with clean EOF; client could accept the advertised tail")
					}
					t.Logf(
						"proto=%s status=%d tail=%q up_to_date=%q body=%q read_error=%v",
						resp.Proto,
						resp.StatusCode,
						resp.Header.Get(protocol.HeaderStreamNextOffset),
						resp.Header.Get(protocol.HeaderStreamUpToDate),
						body,
						readErr,
					)
				})
			}
		})
	}
}

func TestSSEDataWriterBatchesOrdinaryBytes(t *testing.T) {
	t.Parallel()

	sink := &writeCallRecorder{}
	writer := &sseDataWriter{w: sink, lineStart: true}
	payload := bytes.Repeat([]byte("x"), 1<<20)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.close(); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 3 {
		t.Fatalf("underlying writes = %d, want 3 (prefix, span, close)", sink.calls)
	}
	if sink.bytes != len(payload)+len("data:\n") {
		t.Fatalf("underlying bytes = %d, want %d", sink.bytes, len(payload)+len("data:\n"))
	}
	t.Logf("payload_bytes=%d underlying_writes=%d", len(payload), sink.calls)
}

func TestSSEDataWriterNormalizesLineEndingsAcrossWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{name: "lf", writes: []string{"a\nb"}, want: "data:a\ndata:b\n"},
		{name: "cr", writes: []string{"a\rb"}, want: "data:a\ndata:b\n"},
		{name: "crlf", writes: []string{"a\r\nb"}, want: "data:a\ndata:b\n"},
		{name: "split-crlf", writes: []string{"a\r", "\nb"}, want: "data:a\ndata:b\n"},
		{name: "empty-final-line", writes: []string{"a\n"}, want: "data:a\ndata:\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			writer := &sseDataWriter{w: &out, lineStart: true}
			for _, part := range tc.writes {
				n, err := writer.Write([]byte(part))
				if err != nil {
					t.Fatal(err)
				}
				if n != len(part) {
					t.Fatalf("consumed = %d, want %d", n, len(part))
				}
			}
			if err := writer.close(); err != nil {
				t.Fatal(err)
			}
			if got := out.String(); got != tc.want {
				t.Fatalf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSSEInitialCatchupUsesBoundedPages(t *testing.T) {
	s := store.NewMemoryStore()
	if _, _, err := s.Create("/stream", store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,22,333]`),
		Closed:      true,
	}); err != nil {
		t.Fatal(err)
	}
	counted := &countingPageStore{Store: s, reader: s}
	h := &Handler{
		Store:                counted,
		ReadPageBytes:        2,
		SSEReconnectInterval: time.Minute,
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream?offset=-1&live=sse", nil))

	body := rec.Body.String()
	if got := strings.Count(body, "event: data\n"); got != 3 {
		t.Fatalf("data events = %d, want 3\n%s", got, body)
	}
	if got := strings.Count(body, "event: control\n"); got != 3 {
		t.Fatalf("control events = %d, want 3\n%s", got, body)
	}
	if got := strings.Count(body, `"streamClosed":true`); got != 1 {
		t.Fatalf("closed controls = %d, want 1\n%s", got, body)
	}
	if !strings.Contains(body, `"streamNextOffset":"0000000000000000_0000000000000006"`) {
		t.Fatalf("final control offset missing:\n%s", body)
	}
	if counted.calls.Load() != 3 {
		t.Fatalf("ReadPage calls = %d, want 3", counted.calls.Load())
	}
}

type countingPageStore struct {
	store.Store
	reader store.PageReader
	calls  atomic.Int64
}

func (s *countingPageStore) ReadPage(ctx context.Context, path string, offset store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	s.calls.Add(1)
	return s.reader.ReadPage(ctx, path, offset, opts)
}

type blockingPageStore struct {
	store.Store
	once    sync.Once
	entered chan struct{}
}

type legacyOnlyStore struct {
	store.Store
}

type recreatingLegacyStore struct {
	store.Store
	path string
	once sync.Once
}

func (s *recreatingLegacyStore) Read(path string, offset store.Offset) ([]store.Message, bool, error) {
	base := s.Store
	s.once.Do(func() {
		_ = base.Delete(s.path)
		_, _, _ = base.Create(s.path, store.CreateOptions{
			ContentType: "application/octet-stream",
			InitialData: []byte("new"),
		})
	})
	return base.Read(path, offset)
}

type secondPageFailureStore struct {
	store.Store
	reader  store.PageReader
	failure string
	calls   atomic.Int64
}

func (s *secondPageFailureStore) ReadPage(ctx context.Context, path string, offset store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	if s.calls.Add(1) == 1 {
		return s.reader.ReadPage(ctx, path, offset, opts)
	}
	if s.failure == "no-progress" {
		return store.ReadPage{NextOffset: offset}, nil
	}
	return store.ReadPage{}, store.ErrReadDataMissing
}

func (s *blockingPageStore) ReadPage(ctx context.Context, _ string, _ store.Offset, _ store.ReadPageOptions) (store.ReadPage, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return store.ReadPage{}, ctx.Err()
}

type readMetricsRecorder struct {
	mu            sync.Mutex
	cancellations map[string]int
}

func (m *readMetricsRecorder) ReadPage(_, _, _, _ int, _ time.Duration, _ int) {}
func (m *readMetricsRecorder) ReadResponse(_, _ int)                           {}
func (m *readMetricsRecorder) ReadCancellation(phase string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancellations == nil {
		m.cancellations = make(map[string]int)
	}
	m.cancellations[phase]++
}

type flushRecorder struct {
	header        http.Header
	status        int
	body          strings.Builder
	flushBodies   []string
	maxWrite      int
	writeCalls    int
	failWriteCall int
	onFirstFlush  func()
}

type writeCallRecorder struct {
	calls int
	bytes int
}

func (w *writeCallRecorder) Write(p []byte) (int, error) {
	w.calls++
	w.bytes += len(p)
	return len(p), nil
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{header: make(http.Header)}
}

func (w *flushRecorder) Header() http.Header { return w.header }

func (w *flushRecorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *flushRecorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.writeCalls++
	if w.failWriteCall > 0 && w.writeCalls == w.failWriteCall {
		return 0, errors.New("socket write failed")
	}
	if len(p) > w.maxWrite {
		w.maxWrite = len(p)
	}
	return w.body.Write(p)
}

func (w *flushRecorder) Flush() {
	w.flushBodies = append(w.flushBodies, w.body.String())
	if len(w.flushBodies) == 1 && w.onFirstFlush != nil {
		w.onFirstFlush()
	}
}
