package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store/segments"
)

type fakeSegmentStats struct{}

func (fakeSegmentStats) Stats() segments.Stats {
	return segments.Stats{
		Seals:                      2,
		SealDurationNanoseconds:    uint64(3 * time.Second),
		SealDurationMaxNanoseconds: uint64(2 * time.Second),
		SegmentReads:               3,
		PrimaryFallbacks:           1,
		ChecksumFailures:           1,
		BytesServed:                1024,
		Backend: segments.BackendStats{
			OriginReads: 4,
			OriginBytes: 4096,
			CacheHits:   5,
			CacheMisses: 6,
			CacheBytes:  2048,
			RedisReads:  7,
		},
	}
}

func TestMuxEndpoints(t *testing.T) {
	p := New()
	p.SweepTick(5*time.Millisecond, 1000, 500, 3)
	p.WakeDelivery(2*time.Millisecond, "ok")
	p.WakeEvent(time.Millisecond, "ok")
	p.WorkerTick("lease", 7)
	p.ReadPage(1<<20, 2048, 1024, 1024, 2*time.Millisecond, 2)
	p.ReadResponse(1200, 2)
	p.ReadCancellation("between_pages")
	// Horizontal-scale golden signals (GAP2): exercise each appended method so its
	// series appears in the exposition (a CounterVec emits nothing until a label
	// value is observed).
	p.FanOut(3*time.Millisecond, 4, 12)
	p.DueSetMutation("arm")
	p.DueWorkerTick(time.Millisecond, 2)
	p.SlotOwnership("claimed", 7)
	p.CoverageGap(8 * time.Millisecond)
	p.OwnerFenced("check_owner")
	p.ClaimContention("already_claimed", "agent-handler")
	p.DurabilityShort("WAITAOF")
	p.SSEHubActive(1)
	p.SSEClientActive(1)
	p.SSEHubRead(7)
	p.SSEHubRingBytes(4096)
	p.SSEClientLagged()
	p.SSEClientWriteTimeout()
	p.SSESubscriptionActive(1)
	p.SSESubscriptionEvent("opened")
	mux := p.Mux(func() error { return nil })

	get := func(path string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}

	if rr := get("/healthz"); rr.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rr.Code)
	}
	if rr := get("/readyz"); rr.Code != http.StatusOK {
		t.Fatalf("/readyz (ready) = %d, want 200", rr.Code)
	}
	if rr := get("/debug/pprof/"); rr.Code != http.StatusNotFound {
		t.Fatalf("/debug/pprof/ (disabled) = %d, want 404", rr.Code)
	}

	body := get("/metrics").Body.String()
	for _, name := range []string{
		"chronicle_sweep_tick_seconds",
		"chronicle_sweep_subs_evaluated",
		"chronicle_sweep_tails_batched",
		"chronicle_sweep_wakes_total",
		"chronicle_wake_delivery_seconds",
		"chronicle_worker_due_items",
		"chronicle_read_page_target_bytes",
		"chronicle_read_fetched_bytes_total",
		"chronicle_read_returned_bytes_total",
		"chronicle_read_discarded_bytes_total",
		"chronicle_read_pages_total",
		"chronicle_read_pages_per_response",
		"chronicle_read_redis_script_seconds",
		"chronicle_read_redis_script_invocations_total",
		"chronicle_read_response_bytes_total",
		"chronicle_read_cancellations_total",
		"chronicle_fanout_seconds",
		"chronicle_fanout_slots_probed",
		"chronicle_fanout_subs",
		"chronicle_due_set_mutations_total",
		"chronicle_due_worker_tick_seconds",
		"chronicle_due_worker_fired",
		"chronicle_slot_ownership_events_total",
		"chronicle_coverage_gap_seconds",
		"chronicle_owner_fenced_total",
		"chronicle_claim_contention_total",
		"chronicle_durability_short_total",
		"chronicle_sse_hubs",
		"chronicle_sse_clients",
		"chronicle_sse_hub_reads_total",
		"chronicle_sse_hub_messages_total",
		"chronicle_sse_hub_ring_bytes",
		"chronicle_sse_lagged_disconnects_total",
		"chronicle_sse_write_timeouts_total",
		"chronicle_sse_subscriptions",
		"chronicle_sse_subscription_events_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics output missing %q", name)
		}
	}
}

func TestMuxPprofIsExplicitlyEnabled(t *testing.T) {
	p := New()
	mux := p.Mux(nil, true)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(
		rr,
		httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil),
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("/debug/pprof/goroutine = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "goroutine profile") {
		t.Fatalf("goroutine profile response did not contain profile data")
	}
}

func TestReadyzReportsNotReady(t *testing.T) {
	p := New()
	mux := p.Mux(func() error { return errors.New("redis unreachable") })
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz (not ready) = %d, want 503", rr.Code)
	}
}

func TestSegmentMetricsAreFeatureGated(t *testing.T) {
	without := New()
	rr := httptest.NewRecorder()
	without.Mux(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(rr.Body.String(), "chronicle_segment_reads_total") {
		t.Fatal("prototype segment metrics appeared without an enabled source")
	}

	with := New()
	with.RegisterSegments(fakeSegmentStats{})
	rr = httptest.NewRecorder()
	with.Mux(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, sample := range []string{
		"chronicle_segment_reads_total 3",
		"chronicle_segment_seal_seconds_total 3",
		"chronicle_segment_seal_seconds_max 2",
		"chronicle_segment_primary_fallbacks_total 1",
		"chronicle_segment_checksum_failures_total 1",
		"chronicle_segment_cache_hits_total 5",
		"chronicle_segment_cache_bytes 2048",
		"chronicle_segment_origin_bytes_total 4096",
		"chronicle_segment_redis_reads_total 7",
	} {
		if !strings.Contains(body, sample) {
			t.Errorf("metrics output missing %q", sample)
		}
	}
}
