package webhook

import (
	"fmt"
	"testing"
	"time"
)

func TestWakeCorrelationIdleTTL(t *testing.T) {
	t0 := time.Unix(1_000, 0)
	c := newWakeCorrelation(8)
	c.remember("w1", appendOrigin{requestID: "req-1"}, time.Minute, t0)

	if got, ok := c.lookup("w1", t0.Add(59*time.Second)); !ok || got.requestID != "req-1" {
		t.Fatalf("lookup inside the window = %q/%v", got.requestID, ok)
	}
	// The use at t0+59s refreshed the window, so t0+60s+1 is still inside it.
	if got, ok := c.lookup("w1", t0.Add(61*time.Second)); !ok || got.requestID != "req-1" {
		t.Fatalf("lookup after a refresh = %q/%v, want the entry kept", got.requestID, ok)
	}
	if dropped := c.sweep(t0.Add(2*time.Minute + 2*time.Second)); dropped != 1 || c.size() != 0 {
		t.Fatalf("sweep past the refreshed window dropped %d, size %d; want 1 and 0", dropped, c.size())
	}
	if _, ok := c.lookup("w1", t0.Add(3*time.Minute)); ok {
		t.Fatal("an expired entry must not be returned")
	}
	c.remember("w2", appendOrigin{requestID: "req-2"}, time.Minute, t0)
	if _, ok := c.lookup("w2", t0.Add(time.Minute+time.Nanosecond)); ok {
		t.Fatal("lookup must not return an entry whose window has lapsed even before a sweep")
	}
	c.forget("w2")
	if c.size() != 0 {
		t.Fatalf("size after forget = %d, want 0", c.size())
	}
}

func TestWakeCorrelationCapacityEvictsNearestExpiry(t *testing.T) {
	t0 := time.Unix(1_000, 0)
	c := newWakeCorrelation(3)
	for i, ttl := range []time.Duration{3 * time.Minute, time.Minute, 2 * time.Minute} {
		if evicted := c.remember(fmt.Sprintf("w%d", i), appendOrigin{requestID: fmt.Sprintf("req-%d", i)}, ttl, t0); evicted {
			t.Fatalf("remember %d evicted while under capacity", i)
		}
	}
	// Full, nothing expired: the entry nearest its expiry (w1, one minute) goes.
	if evicted := c.remember("w3", appendOrigin{requestID: "req-3"}, time.Minute, t0); !evicted || c.size() != 3 {
		t.Fatalf("remember at capacity evicted=%v size=%d, want true and 3", evicted, c.size())
	}
	if _, ok := c.lookup("w1", t0); ok {
		t.Fatal("the entry nearest expiry must be the one evicted")
	}
	for _, id := range []string{"w0", "w2", "w3"} {
		if _, ok := c.lookup(id, t0); !ok {
			t.Fatalf("%s must survive the eviction", id)
		}
	}
	// Full with an expired entry: expiry is reclaimed first and nothing live is evicted.
	if evicted := c.remember("w4", appendOrigin{requestID: "req-4"}, time.Minute, t0.Add(90*time.Second)); evicted || c.size() != 3 {
		t.Fatalf("remember with an expired entry present evicted=%v size=%d, want false and 3", evicted, c.size())
	}
	// Re-remembering a known wake never evicts.
	if evicted := c.remember("w4", appendOrigin{requestID: "req-4b"}, time.Minute, t0.Add(90*time.Second)); evicted {
		t.Fatal("remembering a known wake evicted another")
	}
}

func TestWakeCorrelationCapacityMustBePositive(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("zero capacity must panic")
		}
	}()
	newWakeCorrelation(0)
}

func TestManagerWakeMemoryIsBounded(t *testing.T) {
	mgr, _, _ := newTestManager(t)
	for i := range wakeCorrelationCapacity + 1 {
		mgr.rememberWakeOrigin(fmt.Sprintf("w_%d", i), appendOrigin{requestID: fmt.Sprintf("req-%d", i)}, DefaultLeaseTTLMs)
	}
	if got := mgr.wakeCorrelation.size(); got != wakeCorrelationCapacity {
		t.Fatalf("remembered %d wakes, want the capacity %d", got, wakeCorrelationCapacity)
	}
	if got := mgr.requestIDForWake("w_0"); got != "wake-w_0" {
		t.Fatalf("the oldest wake must have been evicted, got %q", got)
	}
}
