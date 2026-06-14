package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMuxEndpoints(t *testing.T) {
	p := New()
	p.SweepTick(5*time.Millisecond, 1000, 500, 3)
	p.WakeDelivery(2*time.Millisecond, "ok")
	p.WakeEvent(time.Millisecond, "ok")
	p.WorkerTick("lease", 7)
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

	body := get("/metrics").Body.String()
	for _, name := range []string{
		"chronicle_sweep_tick_seconds",
		"chronicle_sweep_subs_evaluated",
		"chronicle_sweep_tails_batched",
		"chronicle_sweep_wakes_total",
		"chronicle_wake_delivery_seconds",
		"chronicle_worker_due_items",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics output missing %q", name)
		}
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
