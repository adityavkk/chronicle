package webhook

import (
	"sync"
	"time"
)

// wakeCorrelation is the Manager's bounded, process-local memory of which
// append armed each in-flight wake (its request id and its trace), so the
// wake's delivery, retries and acknowledgement log and send the id of the
// append that caused it and its delivery spans join the caller's trace. It is
// a hint cache, never state: a miss falls back to the stable wake-<id> and a
// trace of the delivery's own, which is what any other replica does anyway.
//
// Two bounds keep it from growing with wakes whose end this replica never
// sees (a callback that lands on another replica, a lease that lapses, a slot
// that changes owner): an idle TTL per entry, refreshed on every use and swept
// on the dirty worker's tick, and a hard capacity that evicts the entry
// nearest its expiry. Like dirtyQueue it is a pure structure; the Manager
// performs every external action after releasing its mutex.
type wakeCorrelation struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]wakeCorrelationEntry
}

type wakeCorrelationEntry struct {
	origin    appendOrigin
	ttl       time.Duration
	expiresAt time.Time
}

func newWakeCorrelation(capacity int) wakeCorrelation {
	if capacity <= 0 {
		panic("webhook: wake correlation capacity must be positive")
	}
	return wakeCorrelation{capacity: capacity, entries: make(map[string]wakeCorrelationEntry)}
}

// remember stores origin for wakeID until it goes unused for ttl. It reports
// whether a live entry had to be evicted to stay within capacity; expired
// entries are reclaimed first and do not count.
func (c *wakeCorrelation) remember(wakeID string, origin appendOrigin, ttl time.Duration, now time.Time) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, known := c.entries[wakeID]; !known && len(c.entries) >= c.capacity {
		if c.sweepLocked(now) == 0 {
			c.evictNearestExpiryLocked()
			evicted = true
		}
	}
	c.entries[wakeID] = wakeCorrelationEntry{origin: origin, ttl: ttl, expiresAt: now.Add(ttl)}
	return evicted
}

// lookup returns the origin remembered for wakeID and refreshes its idle
// window. An entry whose window has lapsed is a miss even before a sweep
// removes it; a miss is the zero origin.
func (c *wakeCorrelation) lookup(wakeID string, now time.Time) (appendOrigin, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[wakeID]
	if !ok || !entry.expiresAt.After(now) {
		return appendOrigin{}, false
	}
	entry.expiresAt = now.Add(entry.ttl)
	c.entries[wakeID] = entry
	return entry.origin, true
}

// forget drops wakeID once its outcome is final on this replica.
func (c *wakeCorrelation) forget(wakeID string) {
	c.mu.Lock()
	delete(c.entries, wakeID)
	c.mu.Unlock()
}

// sweep drops every entry whose idle window has lapsed and returns how many.
func (c *wakeCorrelation) sweep(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepLocked(now)
}

func (c *wakeCorrelation) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *wakeCorrelation) sweepLocked(now time.Time) int {
	dropped := 0
	for wakeID, entry := range c.entries {
		if !entry.expiresAt.After(now) {
			delete(c.entries, wakeID)
			dropped++
		}
	}
	return dropped
}

// evictNearestExpiryLocked removes the live entry that would expire first.
// It scans the map, which only happens once the cache is full of live wakes,
// a load where one scan per arm is cheap next to the arm's own Redis write.
func (c *wakeCorrelation) evictNearestExpiryLocked() {
	var victim string
	var nearest time.Time
	for wakeID, entry := range c.entries {
		if victim == "" || entry.expiresAt.Before(nearest) {
			victim, nearest = wakeID, entry.expiresAt
		}
	}
	delete(c.entries, victim)
}
