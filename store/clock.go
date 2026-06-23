package store

import (
	"sync"
	"time"
)

// Clock is the time source consulted by the stores for lazy-expiry,
// sliding-TTL touches, and producer-state stamping. Production code uses
// the real wall clock; tests inject a FakeClock so TTL/expiry decisions are
// reproducible at a frozen instant.
//
// The seam exists because naive MemoryStore-vs-Redis equivalence breaks on
// time nondeterminism: the two backends must never be asserted to expire on
// the same independently-sampled wall-clock instant. With one shared,
// controllable Clock driving both, is_expired / lazy-expiry / sliding-TTL
// become deterministic and can be diffed step-for-step (issue #26).
type Clock interface {
	// Now returns the current time. The two stores agree on expiry only when
	// they consult the same Clock at the same logical instant.
	Now() time.Time
}

// realClock is the production default: it reads the wall clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RealClock returns the wall-clock Clock used by default in production. It is
// stateless and safe for concurrent use.
func RealClock() Clock { return realClock{} }

// FakeClock is a controllable Clock for tests. Its time only advances when
// Advance/Set is called, so frozen-clock expiry boundaries (notably
// now == expiry => not expired, a strict ">" on both backends) are exactly
// reproducible. It is safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock anchored at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the clock's current frozen time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d (d may be negative to wind back).
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to an absolute time t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
