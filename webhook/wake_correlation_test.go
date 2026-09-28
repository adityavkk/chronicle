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

func TestWakeCorrelationCapacityEvictsTheLeastRecentlyUsed(t *testing.T) {
	t0 := time.Unix(1_000, 0)
	c := newWakeCorrelation(3)
	// w0 has the longest lease but is the least recently used; w1 and w2 are
	// fresher with short leases.
	for i, ttl := range []time.Duration{10 * time.Minute, time.Minute, time.Minute} {
		at := t0.Add(time.Duration(i) * time.Second)
		if evicted := c.remember(fmt.Sprintf("w%d", i), appendOrigin{requestID: fmt.Sprintf("req-%d", i)}, ttl, at); evicted {
			t.Fatalf("remember %d evicted while under capacity", i)
		}
	}
	// Full, nothing expired: the entry used least recently goes, however long
	// its lease; the policy is recency of use, not remaining time.
	if evicted := c.remember("w3", appendOrigin{requestID: "req-3"}, time.Minute, t0.Add(3*time.Second)); !evicted || c.size() != 3 {
		t.Fatalf("remember at capacity evicted=%v size=%d, want true and 3", evicted, c.size())
	}
	if _, ok := c.lookup("w0", t0.Add(3*time.Second)); ok {
		t.Fatal("the least recently used entry must be the one evicted")
	}
	// A use is what protects an entry: w1 is touched, so the next eviction takes w2.
	if _, ok := c.lookup("w1", t0.Add(4*time.Second)); !ok {
		t.Fatal("w1 must survive the eviction")
	}
	if evicted := c.remember("w4", appendOrigin{requestID: "req-4"}, time.Minute, t0.Add(5*time.Second)); !evicted {
		t.Fatal("remember at capacity must evict")
	}
	if _, ok := c.lookup("w2", t0.Add(5*time.Second)); ok {
		t.Fatal("w2, unused since it was remembered, must be the one evicted")
	}
	// Full with expired entries: they are reclaimed first and nothing live is
	// evicted or counted.
	if evicted := c.remember("w5", appendOrigin{requestID: "req-5"}, time.Minute, t0.Add(90*time.Second)); evicted || c.size() != 1 {
		t.Fatalf("remember with expired entries present evicted=%v size=%d, want false and 1", evicted, c.size())
	}
	// Re-remembering a known wake updates it in place and never evicts.
	c.remember("w6", appendOrigin{requestID: "req-6"}, time.Minute, t0.Add(90*time.Second))
	c.remember("w7", appendOrigin{requestID: "req-7"}, time.Minute, t0.Add(90*time.Second))
	if evicted := c.remember("w5", appendOrigin{requestID: "req-5b"}, time.Minute, t0.Add(90*time.Second)); evicted || c.size() != 3 {
		t.Fatalf("remembering a known wake at capacity evicted=%v size=%d, want false and 3", evicted, c.size())
	}
	if got, ok := c.lookup("w5", t0.Add(91*time.Second)); !ok || got.requestID != "req-5b" {
		t.Fatalf("re-remembered origin = %q/%v, want req-5b", got.requestID, ok)
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
