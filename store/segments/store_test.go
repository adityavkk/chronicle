package segments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func newFileSegmentStore(t *testing.T, mode Mode, primary store.Store, state MigrationState) (*Store, *FileBackend) {
	t.Helper()
	backend, err := NewFileBackend(mode, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  64,
		IndexStride:  2,
		InitialState: state,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seg, backend
}

type readInterleavingStore struct {
	store.Store
	mu    sync.Mutex
	armed bool
	hook  func()
}

func (s *readInterleavingStore) arm(hook func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hook = hook
	s.armed = true
}

func (s *readInterleavingStore) Read(path string, offset store.Offset) ([]store.Message, bool, error) {
	s.runHook()
	return s.Store.Read(path, offset)
}

func (s *readInterleavingStore) runHook() {
	s.mu.Lock()
	var hook func()
	if s.armed {
		s.armed = false
		hook = s.hook
	}
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (s *readInterleavingStore) ReadPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	opts store.ReadPageOptions,
) (store.ReadPage, error) {
	s.runHook()
	return s.Store.(store.PageReader).ReadPage(ctx, path, offset, opts)
}

type blockingPublishBackend struct {
	Backend
	generation uint64
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

type notificationPrimary struct {
	*store.MemoryStore
	subscriber store.NotificationSubscriber
}

type getCountingPrimary struct {
	*store.MemoryStore
	gets atomic.Int64
}

func (p *getCountingPrimary) Get(path string) (*store.StreamMetadata, error) {
	p.gets.Add(1)
	return p.MemoryStore.Get(path)
}

func (p *notificationPrimary) NotificationSubscriber() (store.NotificationSubscriber, bool) {
	return p.subscriber, true
}

type fakeNotificationSubscriber struct{}

func (*fakeNotificationSubscriber) SubscribeNotifications(
	context.Context,
	string,
) (store.NotificationSubscription, error) {
	return nil, errors.New("not used")
}

func (b *blockingPublishBackend) Publish(ctx context.Context, path, expectedToken string, manifest *Manifest) (string, error) {
	if manifest.Generation == b.generation {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.Backend.Publish(ctx, path, expectedToken, manifest)
}

func mustSegmentCreate(t *testing.T, s store.Store, path, contentType string) {
	t.Helper()
	if _, created, err := s.Create(path, store.CreateOptions{ContentType: contentType}); err != nil || !created {
		t.Fatalf("Create: created=%v err=%v", created, err)
	}
}

func mustSegmentAppend(t *testing.T, s store.Store, path string, data []byte, opts store.AppendOptions) store.AppendResult {
	t.Helper()
	result, err := s.Append(path, data, opts)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return result
}

func assertMessages(t *testing.T, got []store.Message, want ...[]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("message count = %d, want %d: %#v", len(got), len(want), got)
	}
	var offset uint64
	for i := range want {
		offset += uint64(len(want[i]))
		if !bytes.Equal(got[i].Data, want[i]) || got[i].Offset.ByteOffset != offset {
			t.Fatalf("message %d = (%x,%s), want (%x,%d)", i, got[i].Data, got[i].Offset, want[i], offset)
		}
	}
}

func TestNotificationSubscriberDelegatesThroughSegmentWrapper(t *testing.T) {
	subscriber := &fakeNotificationSubscriber{}
	primary := &notificationPrimary{
		MemoryStore: store.NewMemoryStore(),
		subscriber:  subscriber,
	}
	backend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := New(primary, Options{Backend: backend}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := seg.NotificationSubscriber()
	if !ok || got != subscriber {
		t.Fatalf("NotificationSubscriber() = (%T, %v), want delegated subscriber", got, ok)
	}
}

func TestImmutableReadAndControlWorkDoesNotLoadProducerMetadata(t *testing.T) {
	primary := &getCountingPrimary{MemoryStore: store.NewMemoryStore()}
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateShadow)
	path := "/no-producer-metadata"
	mustSegmentCreate(t, seg, path, "text/plain")
	mustSegmentAppend(t, seg, path, []byte("payload"), store.AppendOptions{})
	primary.gets.Store(0)

	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	page, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	seg.ReleaseReadSnapshot(path, page.Snapshot)
	pin, err := seg.PinSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	pin.Release()
	if _, err := seg.Transition(path, StateServing); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Repair(path); err != nil {
		t.Fatal(err)
	}

	if got := primary.gets.Load(); got != 0 {
		t.Fatalf("full metadata Get calls = %d, want 0", got)
	}
}

func TestFileCandidatesServeSealedPrefixAndPrimaryTail(t *testing.T) {
	for _, mode := range []Mode{ModeLocalFiles, ModeObjectCache} {
		t.Run(string(mode), func(t *testing.T) {
			primary := store.NewMemoryStore()
			seg, backend := newFileSegmentStore(t, mode, primary, StateServing)
			path := "/sealed-tail"
			mustSegmentCreate(t, seg, path, "application/octet-stream")
			a := []byte{0, 0xff, '|', 'a'}
			b := []byte("second")
			c := []byte("hot-tail")
			mustSegmentAppend(t, seg, path, a, store.AppendOptions{})
			mustSegmentAppend(t, seg, path, b, store.AppendOptions{})
			manifest, err := seg.Seal(path)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Generation != 1 || len(manifest.Segments) == 0 {
				t.Fatalf("unexpected manifest: %+v", manifest)
			}
			mustSegmentAppend(t, seg, path, c, store.AppendOptions{})

			got, upToDate, err := seg.Read(path, store.ZeroOffset)
			if err != nil || !upToDate {
				t.Fatalf("Read: upToDate=%v err=%v", upToDate, err)
			}
			assertMessages(t, got, a, b, c)
			if stats := seg.Stats(); stats.SegmentReads != 1 || stats.BytesServed != uint64(len(a)+len(b)) {
				t.Fatalf("unexpected stats: %+v", stats)
			}
			if mode == ModeObjectCache {
				stats := backend.Stats()
				if stats.OriginReads == 0 || stats.CacheMisses == 0 {
					t.Fatalf("object mode did not exercise origin/cache: %+v", stats)
				}
			}
		})
	}
}

func TestReadPageBoundsFixedSnapshotAndExplicitPinRelease(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/bounded-snapshot"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	want := make([][]byte, 10)
	for i := range want {
		want[i] = bytes.Repeat([]byte{byte(i + 1)}, 16)
		mustSegmentAppend(t, seg, path, want[i], store.AppendOptions{})
	}
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	opts := store.ReadPageOptions{TargetBytes: 32, MaxFrames: 2}
	first, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.StoreToken == "" || first.UpToDate {
		t.Fatalf("first page has no immutable lease or is unexpectedly final: %+v", first)
	}
	secondReader, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, opts)
	if err != nil {
		t.Fatal(err)
	}
	if secondReader.Snapshot.StoreToken == first.Snapshot.StoreToken {
		t.Fatal("independent readers shared an immutable lease token")
	}
	seg.ReleaseReadSnapshot(path, secondReader.Snapshot)

	postSnapshot := []byte("not-in-first-snapshot")
	mustSegmentAppend(t, primary, path, postSnapshot, store.AppendOptions{})

	got := append([]store.Message(nil), first.Messages...)
	page := first
	for !page.UpToDate {
		nextOpts := opts
		nextOpts.Snapshot = &first.Snapshot
		page, err = seg.ReadPage(context.Background(), path, page.NextOffset, nextOpts)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Messages) == 0 && !page.UpToDate {
			t.Fatalf("non-final page made no progress: %+v", page)
		}
		if len(page.Messages) > opts.MaxFrames || page.Stats.ReturnedBytes > opts.TargetBytes {
			t.Fatalf("page exceeded bounds: %+v", page)
		}
		got = append(got, page.Messages...)
	}
	if len(got) != len(want) {
		t.Fatalf("captured messages = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Data, want[i]) {
			t.Fatalf("message %d = %x, want %x", i, got[i].Data, want[i])
		}
	}
	confirmationOpts := opts
	confirmationOpts.Snapshot = &first.Snapshot
	confirmation, err := seg.ReadPage(
		context.Background(),
		path,
		first.Snapshot.Tail,
		confirmationOpts,
	)
	if err != nil {
		t.Fatalf("final snapshot confirmation: %v", err)
	}
	if len(confirmation.Messages) != 0 || !confirmation.UpToDate ||
		!confirmation.NextOffset.Equal(first.Snapshot.Tail) {
		t.Fatalf("final snapshot confirmation = %+v", confirmation)
	}
	seg.ReleaseReadSnapshot(path, first.Snapshot)
	seg.leasesMu.Lock()
	activeLeases := len(seg.leases)
	seg.leasesMu.Unlock()
	seg.pinsMu.Lock()
	activePins := len(seg.pins[path])
	seg.pinsMu.Unlock()
	if activeLeases != 0 || activePins != 0 {
		t.Fatalf("explicit final release retained leases=%d pins=%d", activeLeases, activePins)
	}
	nextOpts := opts
	nextOpts.Snapshot = &first.Snapshot
	if _, err := seg.ReadPage(context.Background(), path, page.NextOffset, nextOpts); !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("released snapshot continuation error = %v", err)
	}
}

func TestReadPageMakesExactProgressAcrossMoreThanSegmentFrameLimit(t *testing.T) {
	primary := store.NewMemoryStore()
	backend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  2 << 20,
		IndexStride:  32,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const records = SegmentMaxFrames + 76
	var initial bytes.Buffer
	initial.WriteByte('[')
	for i := 0; i < records; i++ {
		if i > 0 {
			initial.WriteByte(',')
		}
		initial.WriteByte('0')
	}
	initial.WriteByte(']')
	path := "/frame-limit-progress"
	if _, created, err := seg.Create(path, store.CreateOptions{
		ContentType: "application/json",
		InitialData: initial.Bytes(),
	}); err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	manifest, _, err := backend.Load(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Segments) != 2 ||
		manifest.Segments[0].Records != SegmentMaxFrames ||
		manifest.Segments[1].Records != 76 {
		t.Fatalf("segment frame bounds = %+v", manifest.Segments)
	}

	opts := store.ReadPageOptions{TargetBytes: 2 << 20, MaxFrames: 100}
	var (
		snapshot *store.ReadSnapshot
		next     = store.ZeroOffset
		got      []store.Message
		pages    int
	)
	for {
		opts.Snapshot = snapshot
		page, err := seg.ReadPage(context.Background(), path, next, opts)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
		}
		if len(page.Messages) > opts.MaxFrames ||
			(len(page.Messages) == 0 && !page.UpToDate) {
			t.Fatalf("invalid page %d: %+v", pages, page)
		}
		got = append(got, page.Messages...)
		if page.UpToDate {
			break
		}
		if page.NextOffset.Equal(next) {
			t.Fatalf("page %d did not advance from %s", pages, next)
		}
		next = page.NextOffset
	}
	if pages != 12 || len(got) != records {
		t.Fatalf("pages=%d records=%d, want 12 and %d", pages, len(got), records)
	}
	for i, message := range got {
		if string(message.Data) != "0" ||
			(i > 0 && !got[i-1].Offset.LessThan(message.Offset)) {
			t.Fatalf("message %d = %+v", i, message)
		}
	}
}

func TestReadPageReturnsOversizedFirstFrameAlone(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/oversized-first"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	large := bytes.Repeat([]byte("x"), SegmentBlockBytes+123)
	mustSegmentAppend(t, seg, path, large, store.AppendOptions{})
	mustSegmentAppend(t, seg, path, []byte("later"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	page, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{
		TargetBytes: 16,
		MaxFrames:   8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || !bytes.Equal(page.Messages[0].Data, large) ||
		page.Stats.ReturnedBytes != len(large) || page.UpToDate {
		t.Fatalf("oversized page = %+v", page)
	}
	seg.ReleaseReadSnapshot(path, page.Snapshot)
}

func TestReadPageDeleteRecreateDuringContinuationFailsClosed(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(45, 0))
	primary := store.NewMemoryStore(store.WithClock(clock))
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/paged-recreate"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	for _, payload := range [][]byte{[]byte("old-one"), []byte("old-two")} {
		mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	}
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	first, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{
		TargetBytes: 1,
		MaxFrames:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.UpToDate || first.Snapshot.StoreToken == "" {
		t.Fatalf("first page = %+v", first)
	}
	if err := primary.Delete(path); err != nil {
		t.Fatal(err)
	}
	if _, created, err := primary.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil || !created {
		t.Fatalf("recreate: created=%v err=%v", created, err)
	}
	mustSegmentAppend(t, primary, path, []byte("new-incarnation-is-longer"), store.AppendOptions{})
	opts := store.ReadPageOptions{
		TargetBytes: 1,
		MaxFrames:   1,
		Snapshot:    &first.Snapshot,
	}
	if _, err := seg.ReadPage(context.Background(), path, first.NextOffset, opts); !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("continuation error = %v, want ErrReadSnapshotChanged", err)
	}
	seg.leasesMu.Lock()
	active := len(seg.leases)
	seg.leasesMu.Unlock()
	if active != 0 {
		t.Fatalf("failed continuation retained %d leases", active)
	}
}

func TestSegmentReadPageContinuationDoesNotRenewSlidingTTL(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(46, 0))
	primary := store.NewMemoryStore(store.WithClock(clock))
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	ttl := int64(10)
	path := "/segment-page-ttl"
	if _, _, err := seg.Create(path, store.CreateOptions{
		ContentType: "application/octet-stream",
		TTLSeconds:  &ttl,
	}); err != nil {
		t.Fatal(err)
	}
	mustSegmentAppend(t, seg, path, []byte("a"), store.AppendOptions{})
	mustSegmentAppend(t, seg, path, []byte("b"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	clock.Advance(9 * time.Second)
	first, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{
		TargetBytes: 1,
		MaxFrames:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	second, err := seg.ReadPage(context.Background(), path, first.NextOffset, store.ReadPageOptions{
		TargetBytes: 1,
		MaxFrames:   1,
		Snapshot:    &first.Snapshot,
	})
	if err != nil || len(second.Messages) != 1 || string(second.Messages[0].Data) != "b" {
		t.Fatalf("continuation page=%+v err=%v", second, err)
	}
	clock.Advance(2 * time.Second)
	if _, err := primary.Get(path); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("segment continuation extended TTL: %v", err)
	}
}

type cancelRangeBackend struct {
	Backend
	entered chan struct{}
	once    sync.Once
}

func (b *cancelRangeBackend) ReadDataRange(
	ctx context.Context,
	_ SegmentRef,
	_, _ int64,
) ([]byte, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestReadPageCancellationReturnsPromptlyAndReleasesPin(t *testing.T) {
	primary := store.NewMemoryStore()
	fileBackend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &cancelRangeBackend{Backend: fileBackend, entered: make(chan struct{})}
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  64,
		IndexStride:  2,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/cancel-range"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("payload"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, readErr := seg.ReadPage(ctx, path, store.ZeroOffset, store.ReadPageOptions{})
		result <- readErr
	}()
	<-backend.entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled range read did not return promptly")
	}
	seg.leasesMu.Lock()
	activeLeases := len(seg.leases)
	seg.leasesMu.Unlock()
	seg.pinsMu.Lock()
	activePins := len(seg.pins[path])
	seg.pinsMu.Unlock()
	if activeLeases != 0 || activePins != 0 {
		t.Fatalf("cancellation retained leases=%d pins=%d", activeLeases, activePins)
	}
}

func TestCorruptRangeFallsBackBoundedAndLeaseStaysPrimaryOnly(t *testing.T) {
	primary := store.NewMemoryStore()
	fileBackend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &rangeRecordingBackend{Backend: fileBackend}
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  64,
		IndexStride:  1,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/bounded-fallback"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	for _, payload := range [][]byte{[]byte("first"), []byte("second")} {
		mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	}
	manifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	dataPath, _ := fileBackend.DebugPaths(path, manifest.Segments[0])
	original, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), original...)
	corrupt[0] ^= 0xff
	if err := os.WriteFile(dataPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	opts := store.ReadPageOptions{TargetBytes: 5, MaxFrames: 1}
	first, err := seg.ReadPage(context.Background(), path, store.ZeroOffset, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Messages) != 1 || string(first.Messages[0].Data) != "first" ||
		first.Stats.ReturnedBytes > opts.TargetBytes {
		t.Fatalf("fallback page = %+v", first)
	}
	if first.Stats.FetchedBytes <= first.Stats.ReturnedBytes ||
		first.Stats.DiscardedBytes == 0 {
		t.Fatalf("fallback omitted immutable or snapshot work: %+v", first.Stats)
	}
	rangesAfterFailure := len(backend.ranges)
	if rangesAfterFailure == 0 {
		t.Fatal("corrupt immutable range was not attempted")
	}
	if err := os.WriteFile(dataPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	nextOpts := opts
	nextOpts.Snapshot = &first.Snapshot
	second, err := seg.ReadPage(context.Background(), path, first.NextOffset, nextOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Messages) != 1 || string(second.Messages[0].Data) != "second" ||
		len(backend.ranges) != rangesAfterFailure {
		t.Fatalf("primary-only continuation page=%+v ranges=%v", second, backend.ranges)
	}
	stats := seg.Stats()
	if stats.ChecksumFailures != 1 || stats.PrimaryFallbacks != 1 {
		t.Fatalf("fallback stats = %+v", stats)
	}
}

func TestJSONBoundariesAndResumeOffsets(t *testing.T) {
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, store.NewMemoryStore(), StateServing)
	path := "/json"
	mustSegmentCreate(t, seg, path, "application/json")
	mustSegmentAppend(t, seg, path, []byte(`[{"n":1},{"n":2}]`), store.AppendOptions{})
	mustSegmentAppend(t, seg, path, []byte(`{"n":3}`), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	all, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("messages = %d, want 3", len(all))
	}
	resumed, upToDate, err := seg.Read(path, all[0].Offset)
	if err != nil || !upToDate || len(resumed) != 2 ||
		string(resumed[0].Data) != `{"n":2}` || string(resumed[1].Data) != `{"n":3}` {
		t.Fatalf("resumed = %#v upToDate=%v err=%v", resumed, upToDate, err)
	}
}

func TestSealedSnapshotMatchesPrimaryAndResumeStaysExact(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/snapshot"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	for _, payload := range [][]byte{[]byte("one"), []byte("two")} {
		mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	}
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	snapshot, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	primarySnapshot, _, err := primary.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	if !messagesEqual(snapshot, primarySnapshot) {
		t.Fatalf("segment snapshot differs from primary: segment=%#v primary=%#v", snapshot, primarySnapshot)
	}

	mustSegmentAppend(t, primary, path, []byte("three"), store.AppendOptions{})
	if len(snapshot) != 2 || string(snapshot[1].Data) != "two" {
		t.Fatalf("captured snapshot changed after append: %#v", snapshot)
	}
	resumed, upToDate, err := seg.Read(path, snapshot[len(snapshot)-1].Offset)
	if err != nil || !upToDate {
		t.Fatalf("resume: upToDate=%v err=%v", upToDate, err)
	}
	assertMessagesFromOffset(t, resumed, snapshot[len(snapshot)-1].Offset, []byte("three"))
}

func TestReadRejectsDeleteRecreateBetweenManifestAndTail(t *testing.T) {
	primary := store.NewMemoryStore(store.WithClock(store.NewFakeClock(time.Unix(42, 0))))
	interleaved := &readInterleavingStore{Store: primary}
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, interleaved, StateServing)
	path := "/read-incarnation-race"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("old-prefix"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	interleaved.arm(func() {
		if err := primary.Delete(path); err != nil {
			t.Fatalf("delete old incarnation: %v", err)
		}
		if _, created, err := primary.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil || !created {
			t.Fatalf("create new incarnation: created=%v err=%v", created, err)
		}
		if _, err := primary.Append(path, []byte("new-incarnation-payload"), store.AppendOptions{}); err != nil {
			t.Fatalf("append new incarnation: %v", err)
		}
	})

	got, upToDate, err := seg.Read(path, store.ZeroOffset)
	if err != nil || !upToDate {
		t.Fatalf("Read: upToDate=%v err=%v", upToDate, err)
	}
	assertMessages(t, got, []byte("new-incarnation-payload"))
	if stats := seg.Stats(); stats.SegmentReads != 0 || stats.PrimaryFallbacks != 1 {
		t.Fatalf("incarnation change did not force primary fallback: %+v", stats)
	}
}

func TestSealReplacesPriorIncarnationFromMixedVersionNode(t *testing.T) {
	primary := store.NewMemoryStore(store.WithClock(store.NewFakeClock(time.Unix(43, 0))))
	seg, backend := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/mixed-version-recreate"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("old"), store.AppendOptions{})
	oldManifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := primary.Delete(path); err != nil {
		t.Fatal(err)
	}
	if _, created, err := primary.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil || !created {
		t.Fatalf("direct primary recreate: created=%v err=%v", created, err)
	}
	mustSegmentAppend(t, primary, path, []byte("new"), store.AppendOptions{})
	newManifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	if newManifest.Incarnation == oldManifest.Incarnation {
		t.Fatal("delete and recreate reused the immutable incarnation ID")
	}
	visible, _, err := backend.Load(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if visible.Incarnation != newManifest.Incarnation || len(visible.Segments) != 1 {
		t.Fatalf("stale incarnation remained visible: old=%+v new=%+v visible=%+v", oldManifest, newManifest, visible)
	}
	got, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, []byte("new"))
}

func TestPinSnapshotRejectsDeleteRecreateInterleaving(t *testing.T) {
	primary := store.NewMemoryStore(store.WithClock(store.NewFakeClock(time.Unix(44, 0))))
	interleaved := &readInterleavingStore{Store: primary}
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, interleaved, StateServing)
	path := "/pin-incarnation-race"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("old"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	interleaved.arm(func() {
		if err := primary.Delete(path); err != nil {
			t.Fatal(err)
		}
		if _, created, err := primary.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil || !created {
			t.Fatalf("recreate: created=%v err=%v", created, err)
		}
	})
	if _, err := seg.PinSnapshot(path); err == nil {
		t.Fatal("PinSnapshot accepted a manifest from the prior incarnation")
	}
}

func assertMessagesFromOffset(t *testing.T, got []store.Message, start store.Offset, want ...[]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("message count = %d, want %d: %#v", len(got), len(want), got)
	}
	offset := start.ByteOffset
	for i := range want {
		offset += uint64(len(want[i]))
		if !bytes.Equal(got[i].Data, want[i]) || got[i].Offset.ByteOffset != offset {
			t.Fatalf("message %d = (%x,%s), want (%x,%d)", i, got[i].Data, got[i].Offset, want[i], offset)
		}
	}
}

func messagesEqual(a, b []store.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Data, b[i].Data) || !a[i].Offset.Equal(b[i].Offset) {
			return false
		}
	}
	return true
}

func TestAutoSealReadSkipsReconciliationWhenTailIsKnown(t *testing.T) {
	backend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := New(store.NewMemoryStore(), Options{
		Backend:      backend,
		AutoSealRead: true,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/auto-seal"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("one"), store.AppendOptions{})
	for i := 0; i < 2; i++ {
		if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
			t.Fatal(err)
		}
	}
	if got := seg.Stats().Seals; got != 1 {
		t.Fatalf("seals after unchanged reads = %d, want 1", got)
	}
	mustSegmentAppend(t, seg, path, []byte("two"), store.AppendOptions{})
	if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
		t.Fatal(err)
	}
	if got := seg.Stats().Seals; got != 2 {
		t.Fatalf("seals after tail advance = %d, want 2", got)
	}
}

func TestChecksumFailureFallsBackWithoutCorruptBytes(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, backend := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	path := "/checksum"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	payload := []byte("acknowledged")
	mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	manifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	dataPath, _ := backend.DebugPaths(path, manifest.Segments[0])
	if err := os.WriteFile(dataPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, upToDate, err := seg.Read(path, store.ZeroOffset)
	if err != nil || !upToDate {
		t.Fatalf("Read fallback: upToDate=%v err=%v", upToDate, err)
	}
	assertMessages(t, got, payload)
	stats := seg.Stats()
	if stats.ChecksumFailures != 1 || stats.PrimaryFallbacks != 1 {
		t.Fatalf("fallback not recorded: %+v", stats)
	}
}

func TestObjectCacheRejectsStaleBytesAndRefetches(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, backend := newFileSegmentStore(t, ModeObjectCache, primary, StateServing)
	path := "/stale-cache"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	payload := []byte("origin-is-good")
	mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	manifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
		t.Fatal(err)
	}
	cacheDir := backend.cacheDir(path)
	cacheData := filepath.Join(cacheDir, manifest.Segments[0].Checksum+".block-000000")
	if err := os.WriteFile(cacheData, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, payload)
	if stats := backend.Stats(); stats.OriginReads < 2 || stats.CacheMisses < 2 {
		t.Fatalf("stale cache was not refetched: %+v", stats)
	}
}

func TestObjectCacheFaultFallsBackToPrimary(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, backend := newFileSegmentStore(t, ModeObjectCache, primary, StateServing)
	path := "/cache-fault"
	payload := []byte("primary-remains-complete")
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}

	backend.faults = FailOnce(FaultCache)
	got, upToDate, err := seg.Read(path, store.ZeroOffset)
	if err != nil || !upToDate {
		t.Fatalf("Read fallback: upToDate=%v err=%v", upToDate, err)
	}
	assertMessages(t, got, payload)
	if stats := seg.Stats(); stats.PrimaryFallbacks != 1 || stats.SegmentReads != 0 {
		t.Fatalf("cache fault did not use primary-only fallback: %+v", stats)
	}
}

func TestObjectCacheEnforcesByteBoundAndRefetchesEvictedData(t *testing.T) {
	const cacheLimit = 256
	primary := store.NewMemoryStore()
	backend, err := NewFileBackend(ModeObjectCache, t.TempDir(), cacheLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := New(primary, Options{
		Backend:      backend,
		TargetBytes:  1024,
		IndexStride:  2,
		InitialState: StateServing,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 128)
	for _, path := range []string{"/cache-a", "/cache-b"} {
		mustSegmentCreate(t, seg, path, "application/octet-stream")
		mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
		if _, err := seg.Seal(path); err != nil {
			t.Fatal(err)
		}
		if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
			t.Fatal(err)
		}
	}
	stats := backend.BackendStats()
	if stats.CacheBytes > cacheLimit || stats.CacheEvictions == 0 {
		t.Fatalf("cache bound not enforced: %+v", stats)
	}

	got, _, err := seg.Read("/cache-a", store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, payload)
	if stats = backend.BackendStats(); stats.OriginReads < 3 {
		t.Fatalf("evicted data was not refetched: %+v", stats)
	}
}

func TestForkSurvivesSourceSoftDelete(t *testing.T) {
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, store.NewMemoryStore(), StateServing)
	source, fork := "/source", "/fork"
	mustSegmentCreate(t, seg, source, "application/octet-stream")
	a, b := []byte("source-a"), []byte("source-b")
	mustSegmentAppend(t, seg, source, a, store.AppendOptions{})
	mustSegmentAppend(t, seg, source, b, store.AppendOptions{})
	if _, err := seg.Seal(source); err != nil {
		t.Fatal(err)
	}
	if _, created, err := seg.Create(fork, store.CreateOptions{ForkedFrom: source}); err != nil || !created {
		t.Fatalf("fork Create: created=%v err=%v", created, err)
	}
	c := []byte("fork-own")
	mustSegmentAppend(t, seg, fork, c, store.AppendOptions{})
	manifest, err := seg.Seal(fork)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Fork == nil || manifest.Fork.SourcePath != source {
		t.Fatalf("fork reference missing: %+v", manifest)
	}
	if err := seg.Delete(source); err != nil {
		t.Fatal(err)
	}
	got, _, err := seg.Read(fork, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, a, b, c)
}

func TestTTLClosureProducerAndDeleteRecreateParity(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(10, 0))
	primary := store.NewMemoryStore(store.WithClock(clock))
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	ttl := int64(1)
	path := "/lifecycle"
	if _, _, err := seg.Create(path, store.CreateOptions{
		ContentType: "application/octet-stream",
		TTLSeconds:  &ttl,
	}); err != nil {
		t.Fatal(err)
	}
	epoch, seq := int64(3), int64(0)
	opts := store.AppendOptions{ProducerId: "p", ProducerEpoch: &epoch, ProducerSeq: &seq}
	first := mustSegmentAppend(t, seg, path, []byte("once"), opts)
	duplicate := mustSegmentAppend(t, seg, path, []byte("ignored"), opts)
	if first.ProducerResult != store.ProducerResultAccepted ||
		duplicate.ProducerResult != store.ProducerResultDuplicate ||
		!duplicate.Offset.Equal(first.Offset) {
		t.Fatalf("producer results first=%+v duplicate=%+v", first, duplicate)
	}
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.CloseStream(path); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Append(path, []byte("late"), store.AppendOptions{}); !errors.Is(err, store.ErrStreamClosed) {
		t.Fatalf("closed append error = %v", err)
	}
	clock.Advance(2 * time.Second)
	if _, _, err := seg.Read(path, store.ZeroOffset); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("expired read error = %v", err)
	}

	// Recreate through the same wrapper. Tombstoning prevents the previous
	// generation from aliasing even with a deterministic test clock.
	if err := seg.Delete(path); err != nil {
		t.Fatal(err)
	}
	if _, created, err := seg.Create(path, store.CreateOptions{}); err != nil || !created {
		t.Fatalf("recreate: created=%v err=%v", created, err)
	}
	replacement := []byte("replacement")
	mustSegmentAppend(t, seg, path, replacement, store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	got, _, err := seg.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, replacement)
}

func TestInternalSealingDoesNotRefreshSlidingTTL(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action func(*testing.T, *Store, string)
	}{
		{name: "manual-seal", action: func(t *testing.T, seg *Store, path string) {
			t.Helper()
			if _, err := seg.Seal(path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "close-and-seal", action: func(t *testing.T, seg *Store, path string) {
			t.Helper()
			if _, err := seg.CloseStream(path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := store.NewFakeClock(time.Unix(100, 0))
			primary := store.NewMemoryStore(store.WithClock(clock))
			seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
			ttl := int64(10)
			path := "/ttl-neutral-" + tc.name
			if _, _, err := seg.Create(path, store.CreateOptions{TTLSeconds: &ttl}); err != nil {
				t.Fatal(err)
			}
			mustSegmentAppend(t, seg, path, []byte("payload"), store.AppendOptions{})
			clock.Advance(9 * time.Second)
			tc.action(t, seg, path)
			clock.Advance(2 * time.Second)
			if _, err := primary.Get(path); !errors.Is(err, store.ErrStreamNotFound) {
				t.Fatalf("internal action extended sliding TTL: %v", err)
			}
		})
	}
}

func TestClientSegmentReadRefreshesSlidingTTL(t *testing.T) {
	clock := store.NewFakeClock(time.Unix(200, 0))
	primary := store.NewMemoryStore(store.WithClock(clock))
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	ttl := int64(10)
	path := "/ttl-client-read"
	if _, _, err := seg.Create(path, store.CreateOptions{TTLSeconds: &ttl}); err != nil {
		t.Fatal(err)
	}
	mustSegmentAppend(t, seg, path, []byte("payload"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := primary.Get(path); err != nil {
		t.Fatalf("client read did not refresh sliding TTL: %v", err)
	}
	clock.Advance(9 * time.Second)
	if _, err := primary.Get(path); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("stream survived past refreshed TTL: %v", err)
	}
}

func TestInterruptedSealNeverAdvancesVisibleGeneration(t *testing.T) {
	cases := []struct {
		mode  Mode
		point FaultPoint
	}{
		{ModeLocalFiles, FaultCreate},
		{ModeLocalFiles, FaultWrite},
		{ModeLocalFiles, FaultSync},
		{ModeLocalFiles, FaultRename},
		{ModeLocalFiles, FaultChecksum},
		{ModeLocalFiles, FaultManifest},
		{ModeObjectCache, FaultUpload},
	}
	for _, tc := range cases {
		t.Run(string(tc.mode)+"/"+string(tc.point), func(t *testing.T) {
			primary := store.NewMemoryStore()
			seg, backend := newFileSegmentStore(t, tc.mode, primary, StateServing)
			path := "/crash"
			mustSegmentCreate(t, seg, path, "application/octet-stream")
			first, second := []byte("first"), []byte("second-generation")
			mustSegmentAppend(t, seg, path, first, store.AppendOptions{})
			before, err := seg.Seal(path)
			if err != nil {
				t.Fatal(err)
			}
			mustSegmentAppend(t, seg, path, second, store.AppendOptions{})
			backend.faults = FailOnce(tc.point)
			if _, err := seg.Seal(path); err == nil {
				t.Fatalf("Seal unexpectedly succeeded at %s", tc.point)
			}
			backend.faults = nil
			visible, _, err := backend.Load(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if visible.Generation != before.Generation || visible.SealedThrough != before.SealedThrough {
				t.Fatalf("partial generation became visible: before=%+v after=%+v", before, visible)
			}
			got, _, err := seg.Read(path, store.ZeroOffset)
			if err != nil {
				t.Fatal(err)
			}
			assertMessages(t, got, first, second)
		})
	}
}

func TestMigrationRollbackCutoverAndFaults(t *testing.T) {
	seg, backend := newFileSegmentStore(t, ModeLocalFiles, store.NewMemoryStore(), StateShadow)
	path := "/migration"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	payload := []byte("migrate-me")
	mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
		t.Fatal(err)
	}
	if seg.Stats().SegmentReads != 0 {
		t.Fatal("shadow state served segment bytes")
	}

	seg.opts.Faults = FailOnce(FaultMigration)
	if _, err := seg.Transition(path, StateServing); err == nil {
		t.Fatal("migration fault did not stop transition")
	}
	seg.opts.Faults = nil
	if _, err := seg.Transition(path, StateServing); err != nil {
		t.Fatal(err)
	}
	if _, _, err := seg.Read(path, store.ZeroOffset); err != nil {
		t.Fatal(err)
	}
	if seg.Stats().SegmentReads != 1 {
		t.Fatal("serving state did not use immutable bytes")
	}

	seg.opts.Faults = FailOnce(FaultRollback)
	if _, err := seg.Transition(path, StateShadow); err == nil {
		t.Fatal("rollback fault did not stop transition")
	}
	seg.opts.Faults = nil
	if _, err := seg.Transition(path, StateShadow); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Transition(path, StateServing); err != nil {
		t.Fatal(err)
	}
	seg.opts.Faults = FailOnce(FaultCutover)
	if _, err := seg.Transition(path, StateCutover); err == nil {
		t.Fatal("cutover fault did not stop transition")
	}
	seg.opts.Faults = nil
	if _, err := seg.Transition(path, StateCutover); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.Transition(path, StateShadow); !errors.Is(err, ErrCutover) {
		t.Fatalf("post-cutover rollback error = %v", err)
	}

	backend.faults = FailOnce(FaultGC)
	if _, err := seg.GC(path, GCRetention{}); err == nil {
		t.Fatal("GC fault did not abort cleanup")
	}
}

func TestValidateManifestRejectsUnknownMigrationState(t *testing.T) {
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, store.NewMemoryStore(), StateShadow)
	path := "/unknown-state"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("one"), store.AppendOptions{})
	manifest, err := seg.Seal(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = MigrationState("future-state")
	if err := validateManifest(manifest, ModeLocalFiles, path, manifest.Incarnation); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown state validation error = %v, want ErrCorrupt", err)
	}
}

func TestShadowPromotionRejectsMissingOrCorruptObjects(t *testing.T) {
	for _, mode := range []Mode{ModeLocalFiles, ModeObjectCache} {
		for _, mutate := range []struct {
			name string
			fn   func(string) error
		}{
			{name: "missing", fn: os.Remove},
			{name: "corrupt", fn: func(path string) error {
				return os.WriteFile(path, []byte("corrupt"), 0o600)
			}},
		} {
			t.Run(string(mode)+"/"+mutate.name, func(t *testing.T) {
				seg, backend := newFileSegmentStore(t, mode, store.NewMemoryStore(), StateShadow)
				path := "/verify-before-serving-" + string(mode) + "-" + mutate.name
				mustSegmentCreate(t, seg, path, "application/octet-stream")
				mustSegmentAppend(t, seg, path, []byte("verified"), store.AppendOptions{})
				manifest, err := seg.Seal(path)
				if err != nil {
					t.Fatal(err)
				}
				dataPath, _ := backend.DebugPaths(path, manifest.Segments[0])
				if err := mutate.fn(dataPath); err != nil {
					t.Fatal(err)
				}
				if _, err := seg.Seal(path); err == nil {
					t.Fatal("no-op shadow seal accepted an unreadable generation")
				}
				if _, err := seg.Transition(path, StateServing); err == nil {
					t.Fatal("shadow promotion accepted an unreadable generation")
				}
				repaired, err := seg.Repair(path)
				if err != nil {
					t.Fatalf("Repair: %v", err)
				}
				if repaired.Generation != manifest.Generation+1 {
					t.Fatalf("repair generation = %d, want %d", repaired.Generation, manifest.Generation+1)
				}
				if _, err := seg.Transition(path, StateServing); err != nil {
					t.Fatalf("promote repaired generation: %v", err)
				}
				visible, _, err := backend.Load(context.Background(), path)
				if err != nil {
					t.Fatal(err)
				}
				if visible.State != StateServing {
					t.Fatalf("repaired promotion state = %q", visible.State)
				}
				got, _, err := seg.Read(path, store.ZeroOffset)
				if err != nil {
					t.Fatal(err)
				}
				assertMessages(t, got, []byte("verified"))
			})
		}
	}
}

func TestMixedVersionReadsAndWritesRemainReversible(t *testing.T) {
	primary := store.NewMemoryStore()
	newNode, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	oldNode := primary
	path := "/mixed"
	mustSegmentCreate(t, oldNode, path, "application/octet-stream")
	a, b, c := []byte("old-a"), []byte("new-b"), []byte("old-c")
	mustSegmentAppend(t, oldNode, path, a, store.AppendOptions{})
	if _, err := newNode.Seal(path); err != nil {
		t.Fatal(err)
	}
	mustSegmentAppend(t, newNode, path, b, store.AppendOptions{})
	mustSegmentAppend(t, oldNode, path, c, store.AppendOptions{})
	for name, reader := range map[string]store.Store{"old": oldNode, "new": newNode} {
		got, _, err := reader.Read(path, store.ZeroOffset)
		if err != nil {
			t.Fatalf("%s Read: %v", name, err)
		}
		assertMessages(t, got, a, b, c)
	}
	if _, err := newNode.Transition(path, StateShadow); err != nil {
		t.Fatal(err)
	}
	got, _, err := newNode.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, a, b, c)
}

func TestConcurrentAppendSealAndManifestPublishRace(t *testing.T) {
	primary := store.NewMemoryStore()
	root := t.TempDir()
	backendA, err := NewFileBackend(ModeLocalFiles, root, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	backendB, err := NewFileBackend(ModeLocalFiles, root, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	newNode := func(backend Backend) *Store {
		s, err := New(primary, Options{
			Backend:      backend,
			TargetBytes:  96,
			IndexStride:  4,
			InitialState: StateServing,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := newNode(backendA), newNode(backendB)
	path := "/race"
	mustSegmentCreate(t, a, path, "application/octet-stream")

	const count = 100
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			mustSegmentAppend(t, a, path, []byte(fmt.Sprintf("%04d", i)), store.AppendOptions{})
		}
	}()
	for _, node := range []*Store{a, b} {
		go func(node *Store) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_, _ = node.Seal(path)
			}
		}(node)
	}
	wg.Wait()
	if _, err := a.Seal(path); err != nil {
		t.Fatal(err)
	}
	got, upToDate, err := b.Read(path, store.ZeroOffset)
	if err != nil || !upToDate || len(got) != count {
		t.Fatalf("Read after race: messages=%d upToDate=%v err=%v", len(got), upToDate, err)
	}
	for i, message := range got {
		if string(message.Data) != fmt.Sprintf("%04d", i) {
			t.Fatalf("message %d = %q", i, message.Data)
		}
	}
}

func TestConcurrentForkAndSealMatchesPrimary(t *testing.T) {
	primary := store.NewMemoryStore()
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, primary, StateServing)
	source := "/fork-race-source"
	mustSegmentCreate(t, seg, source, "application/octet-stream")
	for i := 0; i < 20; i++ {
		mustSegmentAppend(t, seg, source, []byte(fmt.Sprintf("seed-%03d", i)), store.AppendOptions{})
	}
	if _, err := seg.Seal(source); err != nil {
		t.Fatal(err)
	}

	const forks = 12
	errs := make(chan error, forks+2)
	var wg sync.WaitGroup
	wg.Add(2 + forks)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := primary.Append(source, []byte(fmt.Sprintf("tail-%03d", i)), store.AppendOptions{}); err != nil {
				errs <- fmt.Errorf("append %d: %w", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := seg.Seal(source); err != nil {
				errs <- fmt.Errorf("seal %d: %w", i, err)
				return
			}
		}
	}()
	for i := 0; i < forks; i++ {
		go func(i int) {
			defer wg.Done()
			path := fmt.Sprintf("/fork-race-%02d", i)
			if _, created, err := seg.Create(path, store.CreateOptions{ForkedFrom: source}); err != nil {
				errs <- fmt.Errorf("create %s: %w", path, err)
				return
			} else if !created {
				errs <- fmt.Errorf("create %s: stream already existed", path)
				return
			}
			if _, err := seg.Seal(path); err != nil {
				errs <- fmt.Errorf("seal %s: %w", path, err)
				return
			}
			got, _, err := seg.Read(path, store.ZeroOffset)
			if err != nil {
				errs <- fmt.Errorf("segment read %s: %w", path, err)
				return
			}
			want, _, err := primary.Read(path, store.ZeroOffset)
			if err != nil {
				errs <- fmt.Errorf("primary read %s: %w", path, err)
				return
			}
			if !messagesEqual(got, want) {
				errs <- fmt.Errorf("fork %s differs: segment=%#v primary=%#v", path, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestRestartRecoveryAndConservativeGC(t *testing.T) {
	root := t.TempDir()
	primary := store.NewMemoryStore()
	backend, err := NewFileBackend(ModeLocalFiles, root, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := New(primary, Options{Backend: backend, InitialState: StateServing}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/restart"
	mustSegmentCreate(t, first, path, "application/octet-stream")
	for _, payload := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		mustSegmentAppend(t, first, path, payload, store.AppendOptions{})
		if _, err := first.Seal(path); err != nil {
			t.Fatal(err)
		}
	}

	reopenedBackend, err := NewFileBackend(ModeLocalFiles, root, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(primary, Options{Backend: reopenedBackend, InitialState: StateServing}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := reopened.Read(path, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	assertMessages(t, got, []byte("one"), []byte("two"), []byte("three"))

	result, err := reopened.GC(path, GCRetention{KeepGenerations: 2, Now: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestsKept < 2 || result.ManifestsDeferred < 1 ||
		result.ManifestsDeleted != 0 || result.SegmentsDeleted != 0 {
		t.Fatalf("unexpected conservative GC result: %+v", result)
	}
}

func TestFileAndObjectGCKeepUnpublishedObjectsRacingWithPublish(t *testing.T) {
	for _, mode := range []Mode{ModeLocalFiles, ModeObjectCache} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			primary := store.NewMemoryStore()
			writerBackend, err := NewFileBackend(mode, root, 1<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			blocking := &blockingPublishBackend{
				Backend:    writerBackend,
				generation: 2,
				entered:    make(chan struct{}),
				release:    make(chan struct{}),
			}
			writer, err := New(primary, Options{
				Backend:      blocking,
				TargetBytes:  64,
				IndexStride:  2,
				InitialState: StateServing,
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			path := "/publisher-gc-race"
			mustSegmentCreate(t, writer, path, "application/octet-stream")
			mustSegmentAppend(t, writer, path, []byte("one"), store.AppendOptions{})
			if _, err := writer.Seal(path); err != nil {
				t.Fatal(err)
			}
			mustSegmentAppend(t, writer, path, []byte("two"), store.AppendOptions{})

			sealResult := make(chan struct {
				manifest *Manifest
				err      error
			}, 1)
			go func() {
				manifest, sealErr := writer.Seal(path)
				sealResult <- struct {
					manifest *Manifest
					err      error
				}{manifest: manifest, err: sealErr}
			}()
			<-blocking.entered

			gcBackend, err := NewFileBackend(mode, root, 1<<20, nil)
			if err != nil {
				close(blocking.release)
				t.Fatal(err)
			}
			collector, err := New(primary, Options{Backend: gcBackend, InitialState: StateServing}, nil)
			if err != nil {
				close(blocking.release)
				t.Fatal(err)
			}
			gcResult, gcErr := collector.GC(path, GCRetention{
				KeepGenerations: 1,
				Now:             time.Now().Add(time.Hour),
			})
			close(blocking.release)
			sealed := <-sealResult
			if gcErr != nil {
				t.Fatal(gcErr)
			}
			if sealed.err != nil {
				t.Fatal(sealed.err)
			}
			if gcResult.SegmentsDeleted != 0 {
				t.Fatalf("GC deleted %d objects while generation 2 was between Put and Publish: %+v", gcResult.SegmentsDeleted, gcResult)
			}
			if gcResult.SegmentsDeferred < 2 {
				t.Fatalf("publisher staging objects were not classified as deferred: %+v", gcResult)
			}
			for _, ref := range sealed.manifest.Segments {
				if _, _, err := writerBackend.Read(context.Background(), ref); err != nil {
					t.Fatalf("published generation %d references deleted segment %q: %v", sealed.manifest.Generation, ref.ID, err)
				}
			}
		})
	}
}

func TestActiveSnapshotPinProtectsManifestFromGC(t *testing.T) {
	seg, _ := newFileSegmentStore(t, ModeLocalFiles, store.NewMemoryStore(), StateServing)
	path := "/snapshot-pin"
	mustSegmentCreate(t, seg, path, "application/octet-stream")
	mustSegmentAppend(t, seg, path, []byte("one"), store.AppendOptions{})
	if _, err := seg.Seal(path); err != nil {
		t.Fatal(err)
	}
	pin, err := seg.PinSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if pin.Manifest.Generation != 1 || pin.Token == "" {
		t.Fatalf("unexpected pin: %+v", pin)
	}

	for _, payload := range [][]byte{[]byte("two"), []byte("three")} {
		mustSegmentAppend(t, seg, path, payload, store.AppendOptions{})
		if _, err := seg.Seal(path); err != nil {
			t.Fatal(err)
		}
	}
	result, err := seg.GC(path, GCRetention{KeepGenerations: 1, Now: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestsKept != 2 || result.ManifestsDeferred != 1 || result.ManifestsDeleted != 0 {
		t.Fatalf("GC with active pin = %+v, want two kept and one deferred", result)
	}

	pin.Release()
	pin.Release()
	result, err = seg.GC(path, GCRetention{KeepGenerations: 1, Now: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestsKept != 1 || result.ManifestsDeferred != 2 || result.ManifestsDeleted != 0 {
		t.Fatalf("GC after pin release = %+v, want one kept and two deferred", result)
	}
}
