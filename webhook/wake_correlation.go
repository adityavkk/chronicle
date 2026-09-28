package webhook

import (
	"container/list"
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
// on the dirty worker's tick, and a hard capacity that evicts the entry used
// least recently. Entries sit in one list ordered by last use, so every
// operation, including eviction at capacity, is constant time under the
// mutex that every delivery and acknowledgement lookup shares. Like dirtyQueue
// it is a pure structure; the Manager performs every external action after
// releasing its mutex.
type wakeCorrelation struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*list.Element // wake id -> its element in order
	order    *list.List               // *wakeCorrelationEntry, least recently used at the front
}

type wakeCorrelationEntry struct {
	wakeID    string
	origin    appendOrigin
	ttl       time.Duration
	expiresAt time.Time
}

func newWakeCorrelation(capacity int) wakeCorrelation {
	if capacity <= 0 {
		panic("webhook: wake correlation capacity must be positive")
	}
	return wakeCorrelation{capacity: capacity, entries: make(map[string]*list.Element), order: list.New()}
}

// remember stores origin for wakeID until it goes unused for ttl. It reports
// whether a live entry had to be evicted to stay within capacity; expired
// entries are reclaimed first and do not count.
func (c *wakeCorrelation) remember(wakeID string, origin appendOrigin, ttl time.Duration, now time.Time) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := &wakeCorrelationEntry{wakeID: wakeID, origin: origin, ttl: ttl, expiresAt: now.Add(ttl)}
	if el, known := c.entries[wakeID]; known {
		el.Value = entry
		c.order.MoveToBack(el)
		return false
	}
	if len(c.entries) >= c.capacity && c.sweepLocked(now) == 0 {
		c.dropLocked(c.order.Front())
		evicted = true
	}
	c.entries[wakeID] = c.order.PushBack(entry)
	return evicted
}

// lookup returns the origin remembered for wakeID and refreshes its idle
// window. An entry whose window has lapsed is a miss even before a sweep
// removes it; a miss is the zero origin.
func (c *wakeCorrelation) lookup(wakeID string, now time.Time) (appendOrigin, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[wakeID]
	if !ok {
		return appendOrigin{}, false
	}
	entry := el.Value.(*wakeCorrelationEntry)
	if !entry.expiresAt.After(now) {
		return appendOrigin{}, false
	}
	entry.expiresAt = now.Add(entry.ttl)
	c.order.MoveToBack(el)
	return entry.origin, true
}

// forget drops wakeID once its outcome is final on this replica.
func (c *wakeCorrelation) forget(wakeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[wakeID]; ok {
		c.dropLocked(el)
	}
}

// sweep drops every lapsed entry at the least recently used end and returns
// how many.
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

// sweepLocked pops lapsed entries from the front of the order and stops at
// the first live one. An entry with a shorter lease behind a longer-lived one
// waits there until the front lapses or is used; that costs memory within the
// capacity and nothing else, because lookup checks expiry itself.
func (c *wakeCorrelation) sweepLocked(now time.Time) int {
	dropped := 0
	for el := c.order.Front(); el != nil && !el.Value.(*wakeCorrelationEntry).expiresAt.After(now); el = c.order.Front() {
		c.dropLocked(el)
		dropped++
	}
	return dropped
}

func (c *wakeCorrelation) dropLocked(el *list.Element) {
	delete(c.entries, c.order.Remove(el).(*wakeCorrelationEntry).wakeID)
}
