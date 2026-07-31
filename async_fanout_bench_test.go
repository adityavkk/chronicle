package chronicle

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// BenchmarkSubscriptionFanout measures both sides of the async boundary with
// genuinely owed work. It is source-compatible with the synchronous baseline:
// old managers finish fan-out in OnStreamAppend, while managers with
// RunDirtyWorker report the bounded hook separately from full completion.
func BenchmarkSubscriptionFanout(b *testing.B) {
	cases := []struct {
		subs  int
		links int
	}{
		{1, 1},
		{1, 4},
		{4, 1},
		{4, 4},
		{64, 1},
		{64, 4},
		{256, 1},
		{256, 4},
		{1000, 1},
		{1000, 4},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("S%d_P%d", tc.subs, tc.links), func(b *testing.B) {
			client := benchRedis(b)
			defer client.Close()
			streams := redisstore.New(client, redisstore.Options{})
			subs := webhook.NewRedisStore(client)
			mgr, err := webhook.NewManager(subs, streamAdapter{st: streams, rs: streams}, webhook.ManagerOptions{
				StreamRootURL: "http://bench.invalid/v1/stream/",
			})
			if err != nil {
				b.Fatal(err)
			}

			paths := make([]string, tc.links)
			links := make([]webhook.StreamLink, tc.links)
			for i := range tc.links {
				paths[i] = fmt.Sprintf("bench/fanout/%d", i)
				if _, _, err := streams.Create("/"+paths[i], store.CreateOptions{ContentType: "text/plain"}); err != nil {
					b.Fatalf("create stream %s: %v", paths[i], err)
				}
				links[i] = webhook.StreamLink{Path: paths[i], LinkType: webhook.LinkExplicit, AckedOffset: store.ZeroOffset.String()}
			}
			if _, _, err := streams.Create("/bench/wake", store.CreateOptions{ContentType: "application/json"}); err != nil {
				b.Fatalf("create wake stream: %v", err)
			}
			cfg := webhook.Config{
				Type:       webhook.DispatchPullWake,
				Streams:    paths,
				WakeStream: "bench/wake",
				LeaseTTLMs: 30000,
			}
			for i := range tc.subs {
				id := fmt.Sprintf("bench-sub-%d", i)
				if _, err := subs.CreateOrConfirm(id, cfg, links, time.Now()); err != nil {
					b.Fatalf("create subscription %s: %v", id, err)
				}
			}

			type dirtyRunner interface{ RunDirtyWorker() int }
			runner, asynchronous := any(mgr).(dirtyRunner)
			hookSamples := make([]time.Duration, 0, b.N)
			appendSamples := make([]time.Duration, 0, b.N)
			completionSamples := make([]time.Duration, 0, b.N)
			var redisCommands int64
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for iteration := range b.N {
				before := totalRedisCommandCalls(b, client)
				b.StartTimer()
				appendStarted := time.Now()
				appendResult, err := streams.Append("/"+paths[0], []byte("x"), store.AppendOptions{ContentType: "text/plain"})
				if err != nil {
					b.Fatalf("append source stream: %v", err)
				}
				committed := time.Now()
				mgr.OnStreamAppend(paths[0])
				hookFinished := time.Now()
				hookSamples = append(hookSamples, hookFinished.Sub(committed))
				appendSamples = append(appendSamples, hookFinished.Sub(appendStarted))
				if asynchronous {
					if processed := runner.RunDirtyWorker(); processed != 1 {
						b.Fatalf("dirty worker processed %d streams, want 1", processed)
					}
				}
				completionSamples = append(completionSamples, time.Since(committed))
				b.StopTimer()
				after := totalRedisCommandCalls(b, client)
				redisCommands += after - before

				ack := []webhook.Ack{{Stream: paths[0], Offset: appendResult.Offset.String()}}
				for i := range tc.subs {
					id := fmt.Sprintf("bench-sub-%d", i)
					sub, ok, err := subs.Get(id)
					if err != nil || !ok || sub.Phase != webhook.PhaseWaking || sub.Generation != int64(iteration+1) {
						b.Fatalf("missing owed wake for sub %d: ok=%v phase=%s generation=%d err=%v", i, ok, sub.Phase, sub.Generation, err)
					}
					status, err := subs.AckUnscoped(id, sub.Generation, sub.WakeID, sub.Generation, true, ack, time.Now(), sub.Config.LeaseTTLMs)
					if err != nil || status != "OK" {
						b.Fatalf("reset subscription %d: status=%s err=%v", i, status, err)
					}
				}
			}

			hookP50, hookP99 := durationQuantiles(hookSamples)
			appendP50, appendP99 := durationQuantiles(appendSamples)
			completionP50, completionP99 := durationQuantiles(completionSamples)
			b.ReportMetric(float64(hookP50.Nanoseconds()), "hook-p50-ns")
			b.ReportMetric(float64(hookP99.Nanoseconds()), "hook-p99-ns")
			b.ReportMetric(float64(appendP50.Nanoseconds()), "append-p50-ns")
			b.ReportMetric(float64(appendP99.Nanoseconds()), "append-p99-ns")
			b.ReportMetric(float64(completionP50.Nanoseconds()), "completion-p50-ns")
			b.ReportMetric(float64(completionP99.Nanoseconds()), "completion-p99-ns")
			b.ReportMetric(float64(redisCommands)/float64(b.N), "redis-cmds/op")
			b.ReportMetric(float64(time.Second)/float64(appendP50), "append-requests/s")
			b.ReportMetric(float64(tc.subs), "wakes/op")
		})
	}
}

func durationQuantiles(samples []time.Duration) (time.Duration, time.Duration) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[(len(samples)-1)/2], samples[(len(samples)-1)*99/100]
}

type redisInfoClient interface {
	Info(context.Context, ...string) *redis.StringCmd
}

func totalRedisCommandCalls(b *testing.B, client redisInfoClient) int64 {
	b.Helper()
	info, err := client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		b.Fatal(err)
	}
	var total int64
	for _, line := range strings.Split(info, "\n") {
		if !strings.HasPrefix(line, "cmdstat_") {
			continue
		}
		_, stats, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		for _, field := range strings.Split(stats, ",") {
			value, ok := strings.CutPrefix(field, "calls=")
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err == nil {
				total += n
			}
		}
	}
	return total
}
