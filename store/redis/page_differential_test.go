package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestReadPageDifferentialBoundaries(t *testing.T) {
	subject := newTestStore(t)
	tests := []struct {
		name        string
		contentType string
		initial     []byte
		target      int
		maxFrames   int
	}{
		{name: "empty", contentType: "application/octet-stream", target: 4, maxFrames: 8},
		{name: "one frame", contentType: "application/octet-stream", initial: []byte("abcd"), target: 4, maxFrames: 8},
		{name: "oversized frame", contentType: "application/octet-stream", initial: []byte("abcde"), target: 4, maxFrames: 8},
		{name: "exact boundary", contentType: "application/json", initial: []byte(`[1,22,333]`), target: 3, maxFrames: 8},
		{name: "one byte over", contentType: "application/json", initial: []byte(`[1,22,333]`), target: 2, maxFrames: 8},
		{name: "frame cap", contentType: "application/json", initial: []byte(`[1,2,3,4,5]`), target: 1024, maxFrames: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oracle := store.NewMemoryStore()
			path := testPath("page-" + tc.name)
			opts := store.CreateOptions{ContentType: tc.contentType, InitialData: tc.initial}
			if _, _, err := oracle.Create(path, opts); err != nil {
				t.Fatal(err)
			}
			mustCreate(t, subject, path, opts)

			assertPagedStoresEqual(t, oracle, subject, path, store.ZeroOffset, tc.target, tc.maxFrames, nil)
		})
	}
}

func TestReadPageDifferentialTargets(t *testing.T) {
	subject := newTestStore(t)
	for _, target := range []int{256 << 10, 1 << 20, 4 << 20} {
		t.Run(fmt.Sprintf("%d", target), func(t *testing.T) {
			oracle := store.NewMemoryStore()
			path := testPath("page-target")
			frame := bytes.Repeat([]byte("x"), 64<<10)
			values := make([]string, 80)
			for i := range values {
				values[i] = string(frame)
			}
			body, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			opts := store.CreateOptions{ContentType: "application/json", InitialData: body}
			if _, _, err := oracle.Create(path, opts); err != nil {
				t.Fatal(err)
			}
			mustCreate(t, subject, path, opts)

			assertPagedStoresEqual(t, oracle, subject, path, store.ZeroOffset, target, store.DefaultReadPageFrames, nil)
		})
	}
}

func TestReadPageOversizedCandidatesDoNotAmplifyFetchedBytes(t *testing.T) {
	subject := newTestStore(t)
	const oversizedBytes = 64 << 10

	tests := []struct {
		name          string
		first         []byte
		target        int
		wantReturned  int
		wantFetched   int
		wantDiscarded int
	}{
		{
			name:         "oversized first frame",
			first:        bytes.Repeat([]byte("a"), oversizedBytes),
			target:       16,
			wantReturned: oversizedBytes,
			wantFetched:  oversizedBytes,
		},
		{
			name:         "first non-fitting frame after prefix",
			first:        bytes.Repeat([]byte("a"), 8),
			target:       16,
			wantReturned: 8,
			wantFetched:  8,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := testPath("page-oversized-amplification")
			create := store.CreateOptions{
				ContentType: "application/octet-stream",
				InitialData: tc.first,
			}
			mustCreate(t, subject, path, create)
			for i := 0; i < 3; i++ {
				mustAppend(
					t,
					subject,
					path,
					bytes.Repeat([]byte{byte('b' + i)}, oversizedBytes),
					store.AppendOptions{},
				)
			}

			next := store.ZeroOffset
			var snapshot *store.ReadSnapshot
			pages := 0
			for {
				page, err := subject.ReadPage(
					context.Background(),
					path,
					next,
					store.ReadPageOptions{
						TargetBytes: tc.target,
						MaxFrames:   store.DefaultReadPageFrames,
						Snapshot:    snapshot,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if pages == 0 && (page.Stats.ReturnedBytes != tc.wantReturned ||
					page.Stats.FetchedBytes != tc.wantFetched ||
					page.Stats.DiscardedBytes != tc.wantDiscarded) {
					t.Fatalf(
						"first-page stats = %+v, want returned=%d fetched=%d discarded=%d",
						page.Stats,
						tc.wantReturned,
						tc.wantFetched,
						tc.wantDiscarded,
					)
				}
				if page.Stats.FetchedBytes != page.Stats.ReturnedBytes || page.Stats.DiscardedBytes != 0 {
					t.Fatalf(
						"page %d fetched-byte amplification: %+v; every oversized-first page must fetch exactly what it returns",
						pages,
						page.Stats,
					)
				}
				t.Logf(
					"page=%d returned=%d fetched=%d discarded=%d",
					pages,
					page.Stats.ReturnedBytes,
					page.Stats.FetchedBytes,
					page.Stats.DiscardedBytes,
				)
				pages++
				if page.UpToDate {
					break
				}
				next = page.NextOffset
				if snapshot == nil {
					snapshot = &page.Snapshot
				}
			}
			if pages != 4 {
				t.Fatalf("pages = %d, want 4 one-frame progress pages", pages)
			}
		})
	}
}

func TestReadPageOneFrameUsesOneRangeCall(t *testing.T) {
	subject := newTestStore(t)
	path := testPath("page-one-frame-commandstats")
	mustCreate(t, subject, path, store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("one frame"),
	})
	ctx := context.Background()
	if err := subject.client.ConfigResetStat(ctx).Err(); err != nil {
		t.Fatalf("reset Redis commandstats: %v", err)
	}

	page, err := subject.ReadPage(
		ctx,
		path,
		store.ZeroOffset,
		store.ReadPageOptions{
			TargetBytes: store.DefaultReadPageBytes,
			MaxFrames:   store.DefaultReadPageFrames,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || !page.UpToDate {
		t.Fatalf("page = %+v, want one complete frame", page)
	}
	if page.Stats.RedisScriptInvokes != 1 {
		t.Fatalf("Redis script invokes = %d, want 1", page.Stats.RedisScriptInvokes)
	}
	if calls := redisCommandCalls(t, subject.client, "zrangebylex"); calls != 1 {
		t.Fatalf("ZRANGEBYLEX calls = %d, want 1 for a one-frame page", calls)
	} else {
		t.Logf("one-frame page ZRANGEBYLEX calls=%d", calls)
	}
}

func BenchmarkReadPageRootOwnedOneFrame(b *testing.B) {
	subject := testStoreFor(b)
	path := testPath("benchmark-root-owned-one-frame")
	mustCreateBenchmark(b, subject, path, store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("one frame"),
	})
	ctx := context.Background()
	readOpts := store.ReadPageOptions{
		TargetBytes: store.DefaultReadPageBytes,
		MaxFrames:   store.DefaultReadPageFrames,
	}
	if _, err := subject.ReadPage(ctx, path, store.ZeroOffset, readOpts); err != nil {
		b.Fatal(err)
	}

	var scripts int64
	b.ResetTimer()
	for b.Loop() {
		page, err := subject.ReadPage(ctx, path, store.ZeroOffset, readOpts)
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Messages) != 1 || !page.UpToDate {
			b.Fatalf("page = %+v, want one complete frame", page)
		}
		scripts += int64(page.Stats.RedisScriptInvokes)
	}
	b.StopTimer()
	b.ReportMetric(float64(scripts)/float64(b.N), "redis_scripts/op")
	if scripts != int64(b.N) {
		b.Fatalf("Redis script invokes = %d over %d reads, want one per read", scripts, b.N)
	}
}

// BenchmarkReadPageShapes covers the bounded-page shapes whose command count
// can change when root validation and frame selection are fused. Setup is kept
// outside the timed loop; every case reports both client-observed script calls
// and Redis's cumulative EVALSHA execution time.
func BenchmarkReadPageShapes(b *testing.B) {
	type readCase struct {
		path         string
		offset       store.Offset
		opts         store.ReadPageOptions
		wantMessages int
		wantScripts  int64
	}

	for _, tc := range []struct {
		name  string
		setup func(*testing.B, *Store) readCase
	}{
		{
			name: "near-byte-target",
			setup: func(b *testing.B, subject *Store) readCase {
				const target = store.DefaultReadPageBytes
				path := testPath("benchmark-near-byte-target")
				mustCreateBenchmark(b, subject, path, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: bytes.Repeat([]byte("x"), target-128),
				})
				return readCase{
					path:         path,
					opts:         store.ReadPageOptions{TargetBytes: target},
					wantMessages: 1,
					wantScripts:  1,
				}
			},
		},
		{
			name: "many-small-frames-at-cap",
			setup: func(b *testing.B, subject *Store) readCase {
				const frameCap = 128
				values := make([]string, frameCap)
				for i := range values {
					values[i] = "x"
				}
				body, err := json.Marshal(values)
				if err != nil {
					b.Fatal(err)
				}
				path := testPath("benchmark-many-small-frames")
				mustCreateBenchmark(b, subject, path, store.CreateOptions{
					ContentType: "application/json",
					InitialData: body,
				})
				return readCase{
					path: path,
					opts: store.ReadPageOptions{
						TargetBytes: store.DefaultReadPageBytes,
						MaxFrames:   frameCap,
					},
					wantMessages: frameCap,
					wantScripts:  1,
				}
			},
		},
		{
			name: "empty-tail",
			setup: func(b *testing.B, subject *Store) readCase {
				path := testPath("benchmark-empty-tail")
				meta := mustCreateBenchmark(b, subject, path, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("frame"),
				})
				return readCase{path: path, offset: meta.CurrentOffset, wantScripts: 1}
			},
		},
		{
			name: "root-owned-fork",
			setup: func(b *testing.B, subject *Store) readCase {
				sourcePath := testPath("benchmark-root-fork-source")
				source := mustCreateBenchmark(b, subject, sourcePath, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("source"),
				})
				forkPath := testPath("benchmark-root-fork")
				forkOffset := source.CurrentOffset
				mustCreateBenchmark(b, subject, forkPath, store.CreateOptions{
					ContentType: "application/octet-stream",
					ForkedFrom:  sourcePath,
					ForkOffset:  &forkOffset,
					InitialData: []byte("root"),
				})
				return readCase{
					path:         forkPath,
					offset:       forkOffset,
					wantMessages: 1,
					wantScripts:  1,
				}
			},
		},
		{
			name: "inherited-fork",
			setup: func(b *testing.B, subject *Store) readCase {
				sourcePath := testPath("benchmark-inherited-source")
				source := mustCreateBenchmark(b, subject, sourcePath, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("source"),
				})
				forkPath := testPath("benchmark-inherited-fork")
				forkOffset := source.CurrentOffset
				mustCreateBenchmark(b, subject, forkPath, store.CreateOptions{
					ContentType: "application/octet-stream",
					ForkedFrom:  sourcePath,
					ForkOffset:  &forkOffset,
				})
				return readCase{
					path:         forkPath,
					wantMessages: 1,
					wantScripts:  2,
				}
			},
		},
		{
			name: "sliding-first-page",
			setup: func(b *testing.B, subject *Store) readCase {
				ttl := int64(300)
				path := testPath("benchmark-sliding-first")
				mustCreateBenchmark(b, subject, path, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("frame"),
					TTLSeconds:  &ttl,
				})
				return readCase{path: path, wantMessages: 1, wantScripts: 1}
			},
		},
		{
			name: "sliding-continuation",
			setup: func(b *testing.B, subject *Store) readCase {
				ttl := int64(300)
				path := testPath("benchmark-sliding-continuation")
				mustCreateBenchmark(b, subject, path, store.CreateOptions{
					ContentType: "application/json",
					InitialData: []byte(`["first","second"]`),
					TTLSeconds:  &ttl,
				})
				first, err := subject.ReadPage(
					context.Background(),
					path,
					store.ZeroOffset,
					store.ReadPageOptions{MaxFrames: 1},
				)
				if err != nil {
					b.Fatal(err)
				}
				return readCase{
					path:         path,
					offset:       first.NextOffset,
					opts:         store.ReadPageOptions{MaxFrames: 1, Snapshot: &first.Snapshot},
					wantMessages: 1,
					wantScripts:  1,
				}
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			subject := testStoreFor(b)
			read := tc.setup(b, subject)
			ctx := context.Background()
			if _, err := subject.ReadPage(ctx, read.path, read.offset, read.opts); err != nil {
				b.Fatal(err)
			}
			beforeUsec := redisCommandUsec(b, subject.client, "evalsha")
			var scripts int64
			b.ResetTimer()
			for b.Loop() {
				page, err := subject.ReadPage(ctx, read.path, read.offset, read.opts)
				if err != nil {
					b.Fatal(err)
				}
				if len(page.Messages) != read.wantMessages {
					b.Fatalf("messages = %d, want %d", len(page.Messages), read.wantMessages)
				}
				scripts += int64(page.Stats.RedisScriptInvokes)
			}
			b.StopTimer()
			afterUsec := redisCommandUsec(b, subject.client, "evalsha")
			b.ReportMetric(float64(scripts)/float64(b.N), "redis_scripts/op")
			b.ReportMetric(float64(afterUsec-beforeUsec)/float64(b.N), "redis_evalsha_usec/op")
			if scripts != read.wantScripts*int64(b.N) {
				b.Fatalf(
					"Redis script invokes = %d over %d reads, want %d per read",
					scripts,
					b.N,
					read.wantScripts,
				)
			}
		})
	}
}

func mustCreateBenchmark(
	b *testing.B,
	subject *Store,
	path string,
	opts store.CreateOptions,
) *store.StreamMetadata {
	b.Helper()
	meta, created, err := subject.Create(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	if !created {
		b.Fatalf("stream %q already exists", path)
	}
	return meta
}

func redisCommandCalls(t *testing.T, client goredis.UniversalClient, command string) int64 {
	t.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatalf("Redis commandstats: %v", err)
	}
	prefix := "cmdstat_" + strings.ToLower(command) + ":"
	for _, line := range strings.Split(info, "\r\n") {
		rest, ok := strings.CutPrefix(line, prefix)
		if !ok {
			continue
		}
		for _, field := range strings.Split(rest, ",") {
			raw, ok := strings.CutPrefix(field, "calls=")
			if !ok {
				continue
			}
			calls, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil {
				t.Fatalf("parse %s calls %q: %v", command, raw, parseErr)
			}
			return calls
		}
	}
	return 0
}

func redisCommandUsec(tb testing.TB, client goredis.UniversalClient, command string) int64 {
	tb.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		tb.Fatalf("Redis commandstats: %v", err)
	}
	prefix := "cmdstat_" + strings.ToLower(command) + ":"
	for _, line := range strings.Split(info, "\r\n") {
		rest, ok := strings.CutPrefix(line, prefix)
		if !ok {
			continue
		}
		for _, field := range strings.Split(rest, ",") {
			raw, ok := strings.CutPrefix(field, "usec=")
			if !ok {
				continue
			}
			usec, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil {
				tb.Fatalf("parse %s usec %q: %v", command, raw, parseErr)
			}
			return usec
		}
	}
	return 0
}

func TestReadPageRedisScriptZeroCandidatesFailsLoudly(t *testing.T) {
	subject := newTestStore(t)
	path := testPath("page-zero-candidates")
	mustCreate(t, subject, path, store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("x"),
	})
	// Keep metadata with a non-zero tail while removing the sorted-set member.
	// This forces read.lua through fetchFrames with a nonempty requested range
	// and zero candidates, rather than letting ReadPage skip an empty segment.
	if err := subject.client.Del(context.Background(), msgKey(path)).Err(); err != nil {
		t.Fatalf("remove message range: %v", err)
	}

	page, err := subject.ReadPage(
		context.Background(),
		path,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if !errors.Is(err, store.ErrReadDataMissing) {
		t.Fatalf("zero-candidate error = %v, want ErrReadDataMissing; page=%+v", err, page)
	}
}

func TestReadPageDifferentialSnapshotAndFork(t *testing.T) {
	subject := newTestStore(t)
	oracle := store.NewMemoryStore()
	source := testPath("page-source")
	fork := testPath("page-fork")
	create := store.CreateOptions{ContentType: "application/json", InitialData: []byte(`[1,22,333]`)}
	if _, _, err := oracle.Create(source, create); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, source, create)
	sourceMessages, _, err := oracle.Read(source, store.ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	forkOffset := sourceMessages[1].Offset
	forkCreate := store.CreateOptions{
		ContentType: "application/json",
		ForkedFrom:  source,
		ForkOffset:  &forkOffset,
		InitialData: []byte(`[4444,55555]`),
	}
	if _, _, err := oracle.Create(fork, forkCreate); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, fork, forkCreate)

	afterFirst := func() {
		appendOpts := store.AppendOptions{ContentType: "application/json"}
		if _, err := oracle.Append(fork, []byte(`[6,7]`), appendOpts); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, subject, fork, []byte(`[6,7]`), appendOpts)
	}
	got := assertPagedStoresEqual(t, oracle, subject, fork, store.ZeroOffset, 3, 2, afterFirst)
	if got, want := string(bytes.Join(messagePayloads(got), []byte(","))), "1,22,4444,55555"; got != want {
		t.Fatalf("captured fork bytes = %q, want %q", got, want)
	}
}

func TestReadPageForkDoesNotLeapOverNonFittingInheritedFrame(t *testing.T) {
	subject := newTestStore(t)
	oracle := store.NewMemoryStore()
	const oversizedBytes = 64 << 10
	source := testPath("page-nonfit-source")
	fork := testPath("page-nonfit-fork")
	createSource := store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: bytes.Repeat([]byte("a"), 8),
	}
	if _, _, err := oracle.Create(source, createSource); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, source, createSource)
	oversized := bytes.Repeat([]byte("b"), oversizedBytes)
	if _, err := oracle.Append(source, oversized, store.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, subject, source, oversized, store.AppendOptions{})
	sourceMeta, err := oracle.Get(source)
	if err != nil {
		t.Fatal(err)
	}
	forkOffset := sourceMeta.CurrentOffset
	createFork := store.CreateOptions{
		ContentType: "application/octet-stream",
		ForkedFrom:  source,
		ForkOffset:  &forkOffset,
		InitialData: []byte("own"),
	}
	if _, _, err := oracle.Create(fork, createFork); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, fork, createFork)

	opts := store.ReadPageOptions{TargetBytes: 16, MaxFrames: store.DefaultReadPageFrames}
	oPage, err := oracle.ReadPage(context.Background(), fork, store.ZeroOffset, opts)
	if err != nil {
		t.Fatal(err)
	}
	sPage, err := subject.ReadPage(context.Background(), fork, store.ZeroOffset, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(bytes.Join(messagePayloads(oPage.Messages), nil)); got != "aaaaaaaa" {
		t.Fatalf("oracle first page = %q, want inherited prefix only", got)
	}
	if got := string(bytes.Join(messagePayloads(sPage.Messages), nil)); got != "aaaaaaaa" {
		t.Fatalf("Redis first page = %q, want inherited prefix only", got)
	}
	if !oPage.NextOffset.Equal(sPage.NextOffset) || oPage.UpToDate != sPage.UpToDate {
		t.Fatalf("first page mismatch: oracle=%+v Redis=%+v", oPage, sPage)
	}

	oNext, err := oracle.ReadPage(
		context.Background(),
		fork,
		oPage.NextOffset,
		store.ReadPageOptions{TargetBytes: 16, MaxFrames: store.DefaultReadPageFrames, Snapshot: &oPage.Snapshot},
	)
	if err != nil {
		t.Fatal(err)
	}
	sNext, err := subject.ReadPage(
		context.Background(),
		fork,
		sPage.NextOffset,
		store.ReadPageOptions{TargetBytes: 16, MaxFrames: store.DefaultReadPageFrames, Snapshot: &sPage.Snapshot},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(oNext.Messages) != 1 || len(sNext.Messages) != 1 ||
		len(oNext.Messages[0].Data) != oversizedBytes ||
		len(sNext.Messages[0].Data) != oversizedBytes {
		t.Fatalf("second page did not retry inherited oversized frame: oracle=%+v Redis=%+v", oNext, sNext)
	}
}

func TestReadPageDifferentialSnapshotChanged(t *testing.T) {
	subjectBase := newTestStore(t)
	clock := store.NewFakeClock(time.Unix(100, 0))
	subject := New(subjectBase.client, Options{Clock: clock})
	oracle := store.NewMemoryStore(store.WithClock(clock))
	path := testPath("page-incarnation")
	create := store.CreateOptions{ContentType: "application/json", InitialData: []byte(`[1,2]`)}
	if _, _, err := oracle.Create(path, create); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, path, create)

	oPage, err := oracle.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1})
	if err != nil {
		t.Fatal(err)
	}
	sPage, err := subject.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Delete(path); err != nil {
		t.Fatal(err)
	}
	if err := subject.Delete(path); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Nanosecond)
	if _, _, err := oracle.Create(path, create); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, path, create)

	_, oErr := oracle.ReadPage(context.Background(), path, oPage.NextOffset, store.ReadPageOptions{Snapshot: &oPage.Snapshot})
	_, sErr := subject.ReadPage(context.Background(), path, sPage.NextOffset, store.ReadPageOptions{Snapshot: &sPage.Snapshot})
	if !errors.Is(oErr, store.ErrReadSnapshotChanged) || !errors.Is(sErr, store.ErrReadSnapshotChanged) {
		t.Fatalf("snapshot errors: oracle=%v subject=%v", oErr, sErr)
	}
}

func TestReadPageLegacyMetadataFallbackAcrossPagesAndRecreate(t *testing.T) {
	subject := newTestStore(t)
	path := testPath("page-legacy-incarnation")
	create := store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2,3]`),
	}
	mustCreate(t, subject, path, create)
	if err := subject.client.HDel(context.Background(), metaKey(path), fIncarnation).Err(); err != nil {
		t.Fatalf("remove incarnation field: %v", err)
	}

	first, err := subject.ReadPage(
		context.Background(),
		path,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.Incarnation == "" {
		t.Fatal("legacy metadata did not fall back to its creation timestamp")
	}
	second, err := subject.ReadPage(
		context.Background(),
		path,
		first.NextOffset,
		store.ReadPageOptions{
			TargetBytes: 1,
			MaxFrames:   1,
			Snapshot:    &first.Snapshot,
		},
	)
	if err != nil {
		t.Fatalf("continue legacy snapshot: %v", err)
	}
	if len(second.Messages) != 1 || string(second.Messages[0].Data) != "2" {
		t.Fatalf("second page = %+v, want message 2", second.Messages)
	}

	if err := subject.Delete(path); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, path, create)
	_, err = subject.ReadPage(
		context.Background(),
		path,
		second.NextOffset,
		store.ReadPageOptions{Snapshot: &first.Snapshot},
	)
	if !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("recreated legacy snapshot error = %v, want ErrReadSnapshotChanged", err)
	}
}

func TestReadPageDifferentialSlidingTTLAndInheritedSource(t *testing.T) {
	subjectBase := newTestStore(t)
	clock := store.NewFakeClock(time.Unix(200, 0))
	subject := New(subjectBase.client, Options{Clock: clock})
	oracle := store.NewMemoryStore(store.WithClock(clock))
	ttl := int64(10)
	source := testPath("page-ttl-source")
	fork := testPath("page-ttl-fork")
	sourceCreate := store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
		TTLSeconds:  &ttl,
	}
	if _, _, err := oracle.Create(source, sourceCreate); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, source, sourceCreate)
	forkCreate := store.CreateOptions{ContentType: "application/json", ForkedFrom: source}
	if _, _, err := oracle.Create(fork, forkCreate); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, fork, forkCreate)

	clock.Advance(9 * time.Second)
	assertPagedStoresEqual(t, oracle, subject, fork, store.ZeroOffset, 1, 1, nil)
	clock.Advance(2 * time.Second)

	// Reading inherited bytes renews the fork but not its source.
	if _, err := oracle.ReadPage(context.Background(), source, store.ZeroOffset, store.ReadPageOptions{}); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("oracle source error = %v", err)
	}
	if _, err := subject.ReadPage(context.Background(), source, store.ZeroOffset, store.ReadPageOptions{}); !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("subject source error = %v", err)
	}
	if _, err := oracle.ReadPage(context.Background(), fork, store.ZeroOffset, store.ReadPageOptions{}); err != nil {
		t.Fatalf("oracle fork expired: %v", err)
	}
	if _, err := subject.ReadPage(context.Background(), fork, store.ZeroOffset, store.ReadPageOptions{}); err != nil {
		t.Fatalf("subject fork expired: %v", err)
	}
}

func TestReadPageDifferentialContinuationDoesNotRenewSlidingTTL(t *testing.T) {
	subjectBase := newTestStore(t)
	clock := store.NewFakeClock(time.Unix(250, 0))
	subject := New(subjectBase.client, Options{Clock: clock})
	oracle := store.NewMemoryStore(store.WithClock(clock))
	ttl := int64(10)
	path := testPath("page-continuation-ttl")
	create := store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
		TTLSeconds:  &ttl,
	}
	if _, _, err := oracle.Create(path, create); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, subject, path, create)

	clock.Advance(9 * time.Second)
	oracleFirst, err := oracle.ReadPage(
		context.Background(),
		path,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	subjectFirst, err := subject.ReadPage(
		context.Background(),
		path,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	for name, candidate := range map[string]struct {
		reader   store.PageReader
		snapshot store.ReadSnapshot
	}{
		"oracle":  {reader: oracle, snapshot: oracleFirst.Snapshot},
		"subject": {reader: subject, snapshot: subjectFirst.Snapshot},
	} {
		page, err := candidate.reader.ReadPage(
			context.Background(),
			path,
			store.Offset{ByteOffset: 1},
			store.ReadPageOptions{
				TargetBytes: 1,
				MaxFrames:   1,
				Snapshot:    &candidate.snapshot,
			},
		)
		if err != nil || len(page.Messages) != 1 {
			t.Fatalf("%s continuation page=%+v err=%v", name, page, err)
		}
	}

	clock.Advance(2 * time.Second)
	for name, candidate := range map[string]struct {
		reader   store.PageReader
		snapshot store.ReadSnapshot
	}{
		"oracle":  {reader: oracle, snapshot: oracleFirst.Snapshot},
		"subject": {reader: subject, snapshot: subjectFirst.Snapshot},
	} {
		_, err := candidate.reader.ReadPage(
			context.Background(),
			path,
			store.Offset{ByteOffset: 2},
			store.ReadPageOptions{Snapshot: &candidate.snapshot},
		)
		if !errors.Is(err, store.ErrStreamNotFound) {
			t.Fatalf("%s continuation extended TTL: %v", name, err)
		}
	}
}

func TestReadPageDifferentialProperty(t *testing.T) {
	subject := newTestStore(t)
	rapid.Check(t, func(rt *rapid.T) {
		oracle := store.NewMemoryStore()
		path := testPath("page-property")
		jsonMode := rapid.Bool().Draw(rt, "json")
		count := rapid.IntRange(0, 32).Draw(rt, "count")
		if jsonMode {
			values := make([]string, count)
			for i := range values {
				values[i] = rapid.StringOfN(rapid.RuneFrom([]rune("ab| \n☃")), 0, 256, 256).Draw(rt, fmt.Sprintf("value-%d", i))
			}
			body, err := json.Marshal(values)
			if err != nil {
				rt.Fatal(err)
			}
			create := store.CreateOptions{ContentType: "application/json", InitialData: body}
			if _, _, err := oracle.Create(path, create); err != nil {
				rt.Fatal(err)
			}
			if _, _, err := subject.Create(path, create); err != nil {
				rt.Fatal(err)
			}
		} else {
			create := store.CreateOptions{ContentType: "application/octet-stream"}
			if _, _, err := oracle.Create(path, create); err != nil {
				rt.Fatal(err)
			}
			if _, _, err := subject.Create(path, create); err != nil {
				rt.Fatal(err)
			}
			for i := 0; i < count; i++ {
				payload := []byte(rapid.StringOfN(
					rapid.RuneFrom([]rune{0, 1, 'a', '|', '\n', '☃'}),
					1,
					256,
					256,
				).Draw(rt, fmt.Sprintf("frame-%d", i)))
				if _, err := oracle.Append(path, payload, store.AppendOptions{}); err != nil {
					rt.Fatal(err)
				}
				if _, err := subject.Append(path, payload, store.AppendOptions{}); err != nil {
					rt.Fatal(err)
				}
			}
		}
		target := rapid.SampledFrom([]int{1, 2, 255, 256 << 10, 1 << 20, 4 << 20}).Draw(rt, "target")
		maxFrames := rapid.IntRange(1, 32).Draw(rt, "maxFrames")
		assertPagedStoresEqualRT(rt, oracle, subject, path, store.ZeroOffset, target, maxFrames)
	})
}

func TestReadPageCancellationReleasesRedisPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in short mode")
	}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/15"
	}
	clientOpts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	clientOpts.PoolSize = 1
	clientOpts.MaxActiveConns = 1
	clientOpts.PoolTimeout = 10 * time.Second
	client := goredis.NewClient(clientOpts)
	t.Cleanup(func() { _ = client.Close() })
	subject := New(client, Options{})
	path := testPath("page-cancel-pool")
	mustCreate(t, subject, path, store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("frame"),
	})

	blockCtx, stopBlock := context.WithCancel(context.Background())
	blockDone := make(chan error, 1)
	go func() {
		blockDone <- client.BLPop(blockCtx, 30*time.Second, path+":blocked").Err()
	}()
	deadline := time.Now().Add(time.Second)
	for client.PoolStats().IdleConns != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if client.PoolStats().IdleConns != 0 {
		stopBlock()
		t.Fatal("blocking Redis command did not occupy the only pool connection")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, readErr := subject.ReadPage(ctx, path, store.ZeroOffset, store.ReadPageOptions{})
		done <- readErr
	}()

	select {
	case readErr := <-done:
		cancel()
		stopBlock()
		t.Fatalf("ReadPage returned before cancellation: %v", readErr)
	case <-time.After(50 * time.Millisecond):
		// The only connection is still occupied, so ReadPage is queued.
	}
	cancel()
	select {
	case readErr := <-done:
		if !errors.Is(readErr, context.Canceled) {
			t.Fatalf("queued ReadPage error = %v, want context.Canceled", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("queued ReadPage goroutine did not exit after cancellation")
	}

	if err := testClient.LPush(context.Background(), path+":blocked", "release").Err(); err != nil {
		t.Fatal(err)
	}
	stopBlock()
	select {
	case <-blockDone:
	case <-time.After(time.Second):
		t.Fatal("blocking Redis command did not exit")
	}
	if _, err := subject.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{}); err != nil {
		t.Fatalf("Redis connection was not reusable after cancellation: %v", err)
	}
}

func assertPagedStoresEqual(
	t *testing.T,
	oracle store.PageReader,
	subject store.PageReader,
	path string,
	offset store.Offset,
	target, maxFrames int,
	afterFirst func(),
) []store.Message {
	t.Helper()
	var out []store.Message
	assertPagedStoresEqualCore(t, oracle, subject, path, offset, target, maxFrames, afterFirst, func(messages []store.Message) {
		out = append(out, messages...)
	})
	return out
}

func assertPagedStoresEqualRT(
	t *rapid.T,
	oracle store.PageReader,
	subject store.PageReader,
	path string,
	offset store.Offset,
	target, maxFrames int,
) {
	assertPagedStoresEqualCore(t, oracle, subject, path, offset, target, maxFrames, nil, func([]store.Message) {})
}

type pageTestingT interface {
	Fatalf(format string, args ...any)
}

func assertPagedStoresEqualCore(
	t pageTestingT,
	oracle store.PageReader,
	subject store.PageReader,
	path string,
	offset store.Offset,
	target, maxFrames int,
	afterFirst func(),
	collect func([]store.Message),
) {
	var oSnapshot, sSnapshot *store.ReadSnapshot
	next := offset
	for pageNum := 0; ; pageNum++ {
		oPage, oErr := oracle.ReadPage(context.Background(), path, next, store.ReadPageOptions{
			TargetBytes: target,
			MaxFrames:   maxFrames,
			Snapshot:    oSnapshot,
		})
		sPage, sErr := subject.ReadPage(context.Background(), path, next, store.ReadPageOptions{
			TargetBytes: target,
			MaxFrames:   maxFrames,
			Snapshot:    sSnapshot,
		})
		if !samePageError(oErr, sErr) {
			t.Fatalf("page %d errors differ: oracle=%v subject=%v", pageNum, oErr, sErr)
		}
		if oErr != nil {
			return
		}
		if oSnapshot == nil {
			oCaptured, sCaptured := oPage.Snapshot, sPage.Snapshot
			oSnapshot, sSnapshot = &oCaptured, &sCaptured
			if afterFirst != nil {
				afterFirst()
			}
		}
		if !oPage.Snapshot.Tail.Equal(sPage.Snapshot.Tail) ||
			oPage.Snapshot.ContentType != sPage.Snapshot.ContentType ||
			oPage.Snapshot.Closed != sPage.Snapshot.Closed ||
			!oPage.NextOffset.Equal(sPage.NextOffset) ||
			oPage.UpToDate != sPage.UpToDate {
			t.Fatalf("page %d metadata differs:\noracle=%+v\nsubject=%+v", pageNum, oPage, sPage)
		}
		if oPage.Stats.ReturnedBytes != payloadBytes(oPage.Messages) ||
			sPage.Stats.ReturnedBytes != payloadBytes(sPage.Messages) {
			t.Fatalf("page %d returned byte accounting mismatch: oracle=%+v subject=%+v", pageNum, oPage.Stats, sPage.Stats)
		}
		if len(oPage.Messages) != len(sPage.Messages) {
			t.Fatalf("page %d message count differs: oracle=%d subject=%d", pageNum, len(oPage.Messages), len(sPage.Messages))
		}
		for i := range oPage.Messages {
			if !oPage.Messages[i].Offset.Equal(sPage.Messages[i].Offset) ||
				!bytes.Equal(oPage.Messages[i].Data, sPage.Messages[i].Data) {
				t.Fatalf("page %d message %d differs: oracle=%+v subject=%+v", pageNum, i, oPage.Messages[i], sPage.Messages[i])
			}
		}
		collect(oPage.Messages)
		if oPage.UpToDate {
			return
		}
		if len(oPage.Messages) == 0 || oPage.NextOffset.Equal(next) {
			t.Fatalf("page %d made no progress: %+v", pageNum, oPage)
		}
		next = oPage.NextOffset
		if pageNum > 10000 {
			t.Fatalf("too many pages")
		}
	}
}

func samePageError(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	for _, target := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		store.ErrStreamNotFound,
		store.ErrReadSnapshotChanged,
	} {
		if errors.Is(a, target) || errors.Is(b, target) {
			return errors.Is(a, target) && errors.Is(b, target)
		}
	}
	return a.Error() == b.Error()
}

func messagePayloads(messages []store.Message) [][]byte {
	out := make([][]byte, len(messages))
	for i := range messages {
		out[i] = messages[i].Data
	}
	return out
}

func payloadBytes(messages []store.Message) int {
	var total int
	for _, message := range messages {
		total += len(message.Data)
	}
	return total
}
