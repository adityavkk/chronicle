package redis

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func BenchmarkReadPageRootFusion(b *testing.B) {
	if os.Getenv("REDIS_URL") == "" {
		b.Skip("REDIS_URL is required for Redis page benchmarks")
	}
	subject := testStoreFor(b)
	ctx := context.Background()

	type benchmarkCase struct {
		path   string
		offset store.Offset
		opts   store.ReadPageOptions
	}
	cases := make(map[string]benchmarkCase)

	small := testPath("bench-small-root")
	mustBenchmarkCreate(b, subject, small, store.CreateOptions{InitialData: []byte("small")})
	cases["small-unforked"] = benchmarkCase{path: small, opts: store.ReadPageOptions{}}

	nearTarget := testPath("bench-near-target")
	mustBenchmarkCreate(b, subject, nearTarget, store.CreateOptions{})
	frame := bytes.Repeat([]byte("x"), 64<<10)
	for range 16 {
		if _, err := subject.Append(nearTarget, frame, store.AppendOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	cases["near-byte-target"] = benchmarkCase{
		path: nearTarget,
		opts: store.ReadPageOptions{TargetBytes: 1 << 20},
	}

	frameCap := testPath("bench-frame-cap")
	jsonFrames := bytes.Repeat([]byte(`"x",`), store.DefaultReadPageFrames)
	jsonFrames = append([]byte{'['}, jsonFrames...)
	jsonFrames[len(jsonFrames)-1] = ']'
	mustBenchmarkCreate(b, subject, frameCap, store.CreateOptions{
		ContentType: "application/json",
		InitialData: jsonFrames,
	})
	cases["many-small-frames"] = benchmarkCase{
		path: frameCap,
		opts: store.ReadPageOptions{TargetBytes: 1 << 20, MaxFrames: store.DefaultReadPageFrames},
	}

	empty := testPath("bench-empty-tail")
	mustBenchmarkCreate(b, subject, empty, store.CreateOptions{})
	cases["empty-tail"] = benchmarkCase{path: empty, opts: store.ReadPageOptions{}}

	source := testPath("bench-source")
	mustBenchmarkCreate(b, subject, source, store.CreateOptions{InitialData: []byte("source")})
	fork := testPath("bench-fork")
	forkMeta := mustBenchmarkCreate(b, subject, fork, store.CreateOptions{ForkedFrom: source})
	if _, err := subject.Append(fork, []byte("root"), store.AppendOptions{}); err != nil {
		b.Fatal(err)
	}
	cases["root-owned-fork"] = benchmarkCase{path: fork, offset: forkMeta.ForkOffset}
	cases["inherited-fork"] = benchmarkCase{path: fork, offset: store.ZeroOffset}

	ttl := int64(3600)
	sliding := testPath("bench-sliding")
	mustBenchmarkCreate(b, subject, sliding, store.CreateOptions{
		InitialData: []byte("sliding"),
		TTLSeconds:  &ttl,
	})
	cases["sliding-first-page"] = benchmarkCase{path: sliding}

	continuationPath := testPath("bench-continuation")
	mustBenchmarkCreate(b, subject, continuationPath, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
	})
	first, err := subject.ReadPage(ctx, continuationPath, store.ZeroOffset, store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1})
	if err != nil {
		b.Fatal(err)
	}
	cases["continuation"] = benchmarkCase{
		path:   continuationPath,
		offset: first.NextOffset,
		opts: store.ReadPageOptions{
			TargetBytes: 1,
			MaxFrames:   1,
			Snapshot:    &first.Snapshot,
		},
	}

	for name, tc := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			resetBenchmarkCommandStats(b, subject.client)
			b.ResetTimer()
			var invokes int64
			for range b.N {
				page, err := subject.ReadPage(ctx, tc.path, tc.offset, tc.opts)
				if err != nil {
					b.Fatal(err)
				}
				invokes += int64(page.Stats.RedisScriptInvokes)
			}
			b.StopTimer()
			b.ReportMetric(float64(invokes)/float64(b.N), "evalsha/op")
			b.ReportMetric(benchmarkCommandUsec(b, subject.client, "evalsha")/float64(b.N), "redis-usec/op")
		})
	}
}

func BenchmarkReadPageForkPlan(b *testing.B) {
	if os.Getenv("REDIS_URL") == "" {
		b.Skip("REDIS_URL is required for Redis page benchmarks")
	}
	subject := testStoreFor(b)

	var body bytes.Buffer
	body.WriteByte('[')
	for i := range 64 {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteByte('1')
	}
	body.WriteByte(']')

	for _, depth := range []int{1, 4} {
		source := testPath("bench-plan-source-" + strconv.Itoa(depth))
		mustBenchmarkCreate(b, subject, source, store.CreateOptions{
			ContentType: "application/json",
			InitialData: append([]byte(nil), body.Bytes()...),
		})
		path := source
		for level := range depth {
			fork := testPath("bench-plan-fork-" + strconv.Itoa(depth) + "-" + strconv.Itoa(level))
			mustBenchmarkCreate(b, subject, fork, store.CreateOptions{ForkedFrom: path})
			path = fork
		}

		for _, sessionEnabled := range []bool{false, true} {
			name := "baseline"
			if sessionEnabled {
				name = "response-session"
			}
			b.Run("depth-"+strconv.Itoa(depth)+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				resetBenchmarkCommandStats(b, subject.client)
				b.ResetTimer()
				var totalPages int
				for range b.N {
					reader := store.PageReader(subject)
					var session store.PageReaderSession
					if sessionEnabled {
						session = subject.NewPageReaderSession(path)
						reader = session
					}
					totalPages += benchmarkForkResponse(b, reader, path)
					if session != nil {
						session.Close()
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(totalPages)/float64(b.N), "pages/response")
				b.ReportMetric(benchmarkCommandCalls(b, subject.client, "hgetall")/float64(b.N), "hgetall/response")
				b.ReportMetric(benchmarkCommandCalls(b, subject.client, "evalsha")/float64(b.N), "evalsha/response")
			})
		}
	}
}

func benchmarkForkResponse(b *testing.B, reader store.PageReader, path string) int {
	b.Helper()
	var snapshot *store.ReadSnapshot
	next := store.ZeroOffset
	pages := 0
	for {
		page, err := reader.ReadPage(
			context.Background(),
			path,
			next,
			store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1, Snapshot: snapshot},
		)
		if err != nil {
			b.Fatal(err)
		}
		pages++
		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
		}
		if page.UpToDate {
			return pages
		}
		if !next.LessThan(page.NextOffset) {
			b.Fatalf("fork response made no progress at %s", next)
		}
		next = page.NextOffset
	}
}

func resetBenchmarkCommandStats(b *testing.B, client goredis.UniversalClient) {
	b.Helper()
	if err := client.Do(context.Background(), "CONFIG", "RESETSTAT").Err(); err != nil {
		b.Fatalf("CONFIG RESETSTAT: %v", err)
	}
}

func benchmarkCommandUsec(b *testing.B, client goredis.UniversalClient, command string) float64 {
	b.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		b.Fatalf("INFO commandstats: %v", err)
	}
	prefix := "cmdstat_" + command + ":"
	for _, line := range strings.Split(info, "\n") {
		values, ok := strings.CutPrefix(strings.TrimSpace(line), prefix)
		if !ok {
			continue
		}
		for _, field := range strings.Split(values, ",") {
			value, ok := strings.CutPrefix(field, "usec=")
			if !ok {
				continue
			}
			result, err := strconv.ParseFloat(value, 64)
			if err != nil {
				b.Fatalf("parse %s usec %q: %v", command, value, err)
			}
			return result
		}
	}
	return 0
}

func benchmarkCommandCalls(b *testing.B, client goredis.UniversalClient, command string) float64 {
	b.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		b.Fatalf("INFO commandstats: %v", err)
	}
	prefix := "cmdstat_" + command + ":"
	for _, line := range strings.Split(info, "\n") {
		values, ok := strings.CutPrefix(strings.TrimSpace(line), prefix)
		if !ok {
			continue
		}
		for _, field := range strings.Split(values, ",") {
			value, ok := strings.CutPrefix(field, "calls=")
			if !ok {
				continue
			}
			result, err := strconv.ParseFloat(value, 64)
			if err != nil {
				b.Fatalf("parse %s calls %q: %v", command, value, err)
			}
			return result
		}
	}
	return 0
}

func mustBenchmarkCreate(
	b *testing.B,
	subject *Store,
	path string,
	opts store.CreateOptions,
) *store.StreamMetadata {
	b.Helper()
	meta, created, err := subject.Create(path, opts)
	if err != nil || !created {
		b.Fatalf("Create(%s): created=%v err=%v", path, created, err)
	}
	return meta
}
