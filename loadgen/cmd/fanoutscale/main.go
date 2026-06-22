// Command fanoutscale is the gate #2 driver: it seeds S subscriptions all
// watching one sentinel stream, drives appends at a steady rate, and reports
// chronicle_fanout_seconds p99 scraped from the server's /metrics endpoint.
//
// The fan-out path (OnStreamAppend → SMEMBERS → maybeWake × S) is what the
// current sweepscale driver never exercises — sweepscale seeds subscriptions
// but drives no appends, so chronicle_fanout_seconds stays at _count=0.
//
// Usage (local):
//
//	fanoutscale -subs 8 -rate 5/s -warmup 20s -measure 60s \
//	  -base-url http://localhost:4437 -metrics-url http://localhost:9090/metrics
//
// Usage (GKE, via rendered Job manifest):
//
//	fanoutscale -subs 256 -rate 10/s -warmup 30s -measure 120s -slo-p99-ms 50 \
//	  -base-url http://chronicle.chronicle-gate2:4437 \
//	  -metrics-url http://chronicle.chronicle-gate2:9090/metrics
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
)

func main() { os.Exit(run()) }

func run() int {
	subsN := flag.Int("subs", 4, "S — number of subscriptions linked to the sentinel stream")
	rateStr := flag.String("rate", "10/s", "append rate (e.g. 10/s, 0.5/s)")
	streamName := flag.String("stream", "gate2/sentinel", "sentinel stream name")
	dispatch := flag.String("dispatch", "pull-wake", "subscription dispatch: pull-wake or webhook")
	warmup := flag.Duration("warmup", 20*time.Second, "settle time after seeding before measurement")
	measure := flag.Duration("measure", 60*time.Second, "metric sampling window")
	sloP99 := flag.Float64("slo-p99-ms", 0, "fail (exit 1) when fanout total p99 exceeds this (ms); 0 disables")
	baseURL := flag.String("base-url", "http://localhost:4437", "chronicle base URL")
	root := flag.String("root", "/v1/stream/", "stream root path")
	metricsURL := flag.String("metrics-url", "http://localhost:9090/metrics", "chronicle /metrics URL")
	flag.Parse()

	rate, err := parseRate(*rateStr)
	if err != nil {
		return fail(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli := dsclient.New(*baseURL, *root)
	hc := &http.Client{Timeout: 15 * time.Second}

	// 1. Create the sentinel stream.
	fmt.Fprintf(os.Stderr, "creating sentinel stream %q ...\n", *streamName)
	if resp, err := cli.Create(ctx, *streamName, "application/json"); err != nil || (resp.Status != 201 && resp.Status != 200) {
		if err != nil {
			return fail(fmt.Errorf("create sentinel stream: %w", err))
		}
		return fail(fmt.Errorf("create sentinel stream: status %d", resp.Status))
	}

	// 2. Seed S subscriptions, all linked to the sentinel stream.
	fmt.Fprintf(os.Stderr, "seeding S=%d subscriptions (dispatch=%s) ...\n", *subsN, *dispatch)
	seeded, seedErrs := seedSubscriptions(ctx, cli, *subsN, *streamName, *dispatch)
	fmt.Fprintf(os.Stderr, "seeded %d/%d (%d errors)\n", seeded, *subsN, seedErrs)
	if seedErrs > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d seed errors — fan-out test may be incomplete\n", seedErrs)
	}

	// 3. Start append driver.
	var appends, appendErrs atomic.Int64
	appendCtx, stopAppends := context.WithCancel(ctx)
	defer stopAppends()
	go appendLoop(appendCtx, cli, *streamName, rate, &appends, &appendErrs)

	// 4. Warmup.
	fmt.Fprintf(os.Stderr, "warming up for %s ...\n", *warmup)
	if !sleepCtx(ctx, *warmup) {
		return fail(ctx.Err())
	}

	// 5. Scrape before snapshot. The epic (#15) exposes three UNLABELED fan-out
	// histograms: chronicle_fanout_seconds (the deciding p99), chronicle_fanout_subs
	// (subscribers found per fan-out), and chronicle_fanout_slots_probed (the
	// occupied-slots bitmap effect — a small constant for a sparse-wide stream, not S).
	names := []string{"chronicle_fanout_seconds", "chronicle_fanout_subs", "chronicle_fanout_slots_probed"}
	before, err := scrape(ctx, hc, *metricsURL, names...)
	if err != nil {
		return fail(fmt.Errorf("scrape before: %w", err))
	}
	appends.Store(0)
	appendErrs.Store(0)

	// 6. Measurement window.
	fmt.Fprintf(os.Stderr, "measuring for %s ...\n", *measure)
	if !sleepCtx(ctx, *measure) {
		return fail(ctx.Err())
	}

	// 7. Scrape after snapshot.
	after, err := scrape(ctx, hc, *metricsURL, names...)
	if err != nil {
		return fail(fmt.Errorf("scrape after: %w", err))
	}
	stopAppends()

	// 8. Compute deltas over the measurement window.
	total := after["chronicle_fanout_seconds"].sub(before["chronicle_fanout_seconds"])
	subs := after["chronicle_fanout_subs"].sub(before["chronicle_fanout_subs"])
	slots := after["chronicle_fanout_slots_probed"].sub(before["chronicle_fanout_slots_probed"])

	rep := Report{
		Subs:            *subsN,
		Dispatch:        *dispatch,
		AppendRate:      rate,
		Warmup:          measure.String(),
		Measure:         measure.String(),
		WindowSeconds:   measure.Seconds(),
		Appends:         appends.Load(),
		AppendErrors:    appendErrs.Load(),
		FanoutTicks:     total.count,
		FanoutMeanMs:    total.mean() * 1000,
		FanoutP50Ms:     total.quantile(0.5) * 1000,
		FanoutP99Ms:     total.quantile(0.99) * 1000,
		FanoutP999Ms:    total.quantile(0.999) * 1000,
		MeanSubsPerFan:  subs.mean(),
		MeanSlotsProbed: slots.mean(),
		P99SlotsProbed:  slots.quantile(0.99),
	}

	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	fmt.Fprintf(os.Stderr,
		"\nS=%d %s | rate=%.1f/s | fan-out over %.0fs: mean=%.2fms p50=%.2fms p99=%.2fms p999=%.2fms | slots probed mean=%.1f p99=%.1f | %.0f fan-outs | appends=%d errs=%d\n",
		rep.Subs, rep.Dispatch, rate, rep.WindowSeconds,
		rep.FanoutMeanMs, rep.FanoutP50Ms, rep.FanoutP99Ms, rep.FanoutP999Ms,
		rep.MeanSlotsProbed, rep.P99SlotsProbed, rep.FanoutTicks, rep.Appends, rep.AppendErrors)

	if *sloP99 > 0 && rep.FanoutP99Ms > *sloP99 {
		fmt.Fprintf(os.Stderr, "SLO FAIL: fanout total p99 %.2fms > %.2fms\n", rep.FanoutP99Ms, *sloP99)
		return 1
	}
	if rep.FanoutTicks == 0 {
		fmt.Fprintln(os.Stderr, "WARN: chronicle_fanout_seconds _count=0 — fan-out path not exercised (check subscriptions + appends)")
	}
	return 0
}

// Report is the structured output of one fanoutscale run.
type Report struct {
	Subs            int     `json:"subs"`
	Dispatch        string  `json:"dispatch"`
	AppendRate      float64 `json:"append_rate_per_s"`
	Warmup          string  `json:"warmup"`
	Measure         string  `json:"measure"`
	WindowSeconds   float64 `json:"window_seconds"`
	Appends         int64   `json:"appends"`
	AppendErrors    int64   `json:"append_errors"`
	FanoutTicks     float64 `json:"fanout_ticks"`
	FanoutMeanMs    float64 `json:"fanout_mean_ms"`
	FanoutP50Ms     float64 `json:"fanout_p50_ms"`
	FanoutP99Ms     float64 `json:"fanout_p99_ms"`
	FanoutP999Ms    float64 `json:"fanout_p999_ms"`
	MeanSubsPerFan  float64 `json:"mean_subs_per_fanout"`
	MeanSlotsProbed float64 `json:"mean_slots_probed"`
	P99SlotsProbed  float64 `json:"p99_slots_probed"`
}

// seedSubscriptions creates S pull-wake subscriptions all linked to stream.
func seedSubscriptions(ctx context.Context, cli *dsclient.Client, s int, stream, dispatch string) (seeded, errs int) {
	const concurrency = 32
	jobs := make(chan int, concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				body := subBody(i, stream, dispatch)
				resp, err := cli.CreateSubscription(ctx, fmt.Sprintf("gate2-%d", i), body)
				mu.Lock()
				if err != nil || resp.Status >= 300 {
					errs++
				} else {
					seeded++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < s; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	return
}

func subBody(i int, stream, dispatch string) []byte {
	m := map[string]any{
		"type":         dispatch,
		"streams":      []string{stream},
		"lease_ttl_ms": 5000,
	}
	if dispatch == "pull-wake" {
		m["wake_stream"] = fmt.Sprintf("gate2/wake/%d", i)
	} else {
		// Webhook dispatch: point at a local sink (for GKE, operator provides a real URL).
		m["webhook"] = map[string]string{"url": "http://127.0.0.1:19999/noop"}
	}
	b, _ := json.Marshal(m)
	return b
}

// appendLoop drives appends at `rate` per second until ctx is cancelled.
func appendLoop(ctx context.Context, cli *dsclient.Client, stream string, rate float64, ok, errs *atomic.Int64) {
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seq := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			body := []byte(fmt.Sprintf(`{"seq":%d}`, seq))
			seq++
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := cli.Append(reqCtx, stream, "application/json", body, nil)
			cancel()
			if err != nil || resp.Status/100 != 2 {
				errs.Add(1)
			} else {
				ok.Add(1)
			}
		}
	}
}

// parseRate parses "10/s" or "0.5/s" → events per second.
func parseRate(s string) (float64, error) {
	var v float64
	var unit string
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%f/%s", &v, &unit); err != nil {
		return 0, fmt.Errorf("invalid rate %q: want <number>/<s|m>", s)
	}
	switch unit {
	case "s":
		return v, nil
	case "m":
		return v / 60, nil
	default:
		return 0, fmt.Errorf("invalid rate unit %q: want s or m", unit)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, "fanoutscale:", err)
	return 1
}

// ---- Prometheus scraper (minimal, mirrors sweep/prom.go) ----

type hist struct {
	count   float64
	sum     float64
	buckets []bucket
}

type bucket struct {
	le    float64
	count float64
}

func (h hist) mean() float64 {
	if h.count == 0 {
		return 0
	}
	return h.sum / h.count
}

func (h hist) quantile(q float64) float64 {
	if h.count == 0 || len(h.buckets) == 0 {
		return 0
	}
	rank := q * h.count
	prevLe, prevCount := 0.0, 0.0
	for _, b := range h.buckets {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				return prevLe
			}
			if b.count == prevCount {
				return b.le
			}
			return prevLe + (b.le-prevLe)*((rank-prevCount)/(b.count-prevCount))
		}
		prevLe, prevCount = b.le, b.count
	}
	return prevLe
}

func (h hist) sub(start hist) hist {
	out := hist{count: h.count - start.count, sum: h.sum - start.sum}
	startCounts := make(map[float64]float64, len(start.buckets))
	for _, b := range start.buckets {
		startCounts[b.le] = b.count
	}
	for _, b := range h.buckets {
		out.buckets = append(out.buckets, bucket{le: b.le, count: b.count - startCounts[b.le]})
	}
	return out
}

// scrape fetches and parses named Prometheus histograms. Unlike sweep/prom.go,
// this version also handles labeled series (for chronicle_fanout_seconds{stage=...}).
// The key in the returned map is "basename" (unlabeled) or "basename{label=value}".
func scrape(ctx context.Context, hc *http.Client, metricsURL string, names ...string) (map[string]hist, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: status %d", resp.StatusCode)
	}

	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	out := make(map[string]hist)

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		series := fields[0]
		// Extract base name and label fragment (e.g. {stage="total"}).
		base, labels := splitSeries(series)
		switch {
		case strings.HasSuffix(base, "_bucket"):
			baseName := strings.TrimSuffix(base, "_bucket")
			if !want[baseName] {
				continue
			}
			key := keyFor(baseName, labels)
			h := out[key]
			h.buckets = append(h.buckets, bucket{le: leFromLabels(labels), count: val})
			out[key] = h
		case strings.HasSuffix(base, "_sum"):
			baseName := strings.TrimSuffix(base, "_sum")
			if !want[baseName] {
				continue
			}
			key := keyFor(baseName, labels)
			h := out[key]
			h.sum = val
			out[key] = h
		case strings.HasSuffix(base, "_count"):
			baseName := strings.TrimSuffix(base, "_count")
			if !want[baseName] {
				continue
			}
			key := keyFor(baseName, labels)
			h := out[key]
			h.count = val
			out[key] = h
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for k, h := range out {
		sort.Slice(h.buckets, func(i, j int) bool { return h.buckets[i].le < h.buckets[j].le })
		out[k] = h
	}
	return out, nil
}

// splitSeries splits "base_name{labels}" into ("base_name", "{labels}").
func splitSeries(s string) (string, string) {
	i := strings.IndexByte(s, '{')
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i:]
}

// keyFor builds the map key: "basename" when no label fragment, or
// "basename{stage=value}" using only the stage label for fanout metrics.
func keyFor(base, labels string) string {
	if labels == "" {
		return base
	}
	// Extract stage label value.
	if v := labelValue(labels, "stage"); v != "" {
		return base + "{stage=" + v + "}"
	}
	return base
}

func labelValue(labels, key string) string {
	needle := key + `="`
	i := strings.Index(labels, needle)
	if i < 0 {
		return ""
	}
	rest := labels[i+len(needle):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func leFromLabels(labels string) float64 {
	if v := labelValue(labels, "le"); v != "" && v != "+Inf" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return math.Inf(1)
}
