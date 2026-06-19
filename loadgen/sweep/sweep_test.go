package sweep

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestSpecPreparedSharedStreamFanOut(t *testing.T) {
	s, err := (Spec{
		Subscriptions:  4,
		SharedStream:   "loadtest/fanout/hot",
		FanOutAppends:  10,
		FanOutInterval: Dur(100 * time.Millisecond),
	}).Prepared()
	if err != nil {
		t.Fatal(err)
	}
	if s.OccupiedSlots != 4 {
		t.Fatalf("occupied_slots = %d, want 4", s.OccupiedSlots)
	}
	if s.Dispatch != "pull-wake" {
		t.Fatalf("dispatch = %q, want pull-wake", s.Dispatch)
	}
}

func TestSpecPreparedRejectsImpossibleFanOut(t *testing.T) {
	for _, spec := range []Spec{
		{Subscriptions: 1, OccupiedSlots: 1},
		{Subscriptions: 1, SharedStream: "x", OccupiedSlots: 2},
		{Subscriptions: 300, SharedStream: "x", OccupiedSlots: 257},
		{Subscriptions: 1, FanOutAppends: 1},
	} {
		if _, err := spec.Prepared(); err == nil {
			t.Fatalf("Prepared(%+v) succeeded, want error", spec)
		}
	}
}

func TestSubscriptionIDForSlot(t *testing.T) {
	for slot := 0; slot < 16; slot++ {
		id := subscriptionIDForSlot(slot, 0)
		if got := int(fnv32a(id) % subSlots); got != slot {
			t.Fatalf("subscriptionIDForSlot(%d) hashes to %d with id %q", slot, got, id)
		}
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

func TestScrapeMetricActivity(t *testing.T) {
	body := `# TYPE chronicle_fanout_seconds histogram
chronicle_fanout_seconds_bucket{le="0.01"} 1
chronicle_fanout_seconds_bucket{le="+Inf"} 2
chronicle_fanout_seconds_sum 0.03
chronicle_fanout_seconds_count 2
chronicle_due_set_mutations_total{op="arm"} 3
chronicle_due_set_mutations_total{op="ack"} 4
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	got, err := scrapeMetricActivity(context.Background(), srv.Client(), srv.URL,
		"chronicle_fanout_seconds",
		"chronicle_due_set_mutations_total",
		"chronicle_owner_fenced_total",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got["chronicle_fanout_seconds"] != 2 {
		t.Fatalf("fanout activity = %v, want histogram count 2", got["chronicle_fanout_seconds"])
	}
	if got["chronicle_due_set_mutations_total"] != 7 {
		t.Fatalf("due mutation activity = %v, want sum 7", got["chronicle_due_set_mutations_total"])
	}
	if got["chronicle_owner_fenced_total"] != 0 {
		t.Fatalf("missing metric activity = %v, want 0", got["chronicle_owner_fenced_total"])
	}
}
