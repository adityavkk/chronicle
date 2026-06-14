package sweep

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSpecPrepared(t *testing.T) {
	if _, err := (Spec{Subscriptions: 0}).Prepared(); err == nil {
		t.Fatal("expected error for 0 subscriptions")
	}
	s, err := (Spec{Subscriptions: 100}).Prepared()
	if err != nil {
		t.Fatal(err)
	}
	if s.LinksPerSub != 1 || s.Dispatch != "pull-wake" || s.Warmup == 0 || s.Measure == 0 {
		t.Fatalf("defaults not applied: %+v", s)
	}
	if _, err := (Spec{Subscriptions: 1, Dispatch: "webhook"}).Prepared(); err == nil {
		t.Fatal("webhook dispatch without url should error")
	}
	if _, err := (Spec{Subscriptions: 1, Dispatch: "bogus"}).Prepared(); err == nil {
		t.Fatal("unknown dispatch should error")
	}
}

func TestHistMeanQuantileSub(t *testing.T) {
	h := hist{count: 10, sum: 1.0, buckets: []bucket{{0.005, 2}, {0.01, 5}, {0.025, 10}, {math.Inf(1), 10}}}
	if got := h.mean(); math.Abs(got-0.1) > 1e-9 {
		t.Fatalf("mean = %v, want 0.1", got)
	}
	if got := h.quantile(0.5); got <= 0.005 || got > 0.025 {
		t.Fatalf("p50 = %v, want within (0.005, 0.025]", got)
	}
	d := h.sub(hist{count: 4, sum: 0.4, buckets: []bucket{{0.005, 1}, {0.01, 2}, {0.025, 4}, {math.Inf(1), 4}}})
	if d.count != 6 || math.Abs(d.sum-0.6) > 1e-9 {
		t.Fatalf("window delta = count %v sum %v, want 6 / 0.6", d.count, d.sum)
	}
}

func TestScrapeParsesHistogram(t *testing.T) {
	body := `# HELP chronicle_sweep_tick_seconds x
# TYPE chronicle_sweep_tick_seconds histogram
chronicle_sweep_tick_seconds_bucket{le="0.01"} 3
chronicle_sweep_tick_seconds_bucket{le="0.005"} 1
chronicle_sweep_tick_seconds_bucket{le="+Inf"} 4
chronicle_sweep_tick_seconds_sum 0.08
chronicle_sweep_tick_seconds_count 4
other_metric 99
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	got, err := scrape(context.Background(), srv.Client(), srv.URL, "chronicle_sweep_tick_seconds")
	if err != nil {
		t.Fatal(err)
	}
	h := got["chronicle_sweep_tick_seconds"]
	if h.count != 4 || math.Abs(h.sum-0.08) > 1e-9 || len(h.buckets) != 3 {
		t.Fatalf("parsed = %+v", h)
	}
	// Buckets must come back sorted ascending by le, +Inf last.
	if h.buckets[0].le != 0.005 || h.buckets[1].le != 0.01 || !math.IsInf(h.buckets[2].le, 1) {
		t.Fatalf("buckets not sorted: %+v", h.buckets)
	}
}
