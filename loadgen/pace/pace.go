// Package pace computes open-loop arrival schedules. A Pacer maps a hit
// index to the instant (relative to start) at which that hit should fire,
// independent of how long previous hits took — the property that makes a
// load test immune to coordinated omission. Pure math, no clocks.
package pace

import (
	"math"
	"time"
)

// Pacer yields the send schedule for one open-loop workload.
type Pacer interface {
	// At returns the time since start at which hit n (0-based) fires.
	// At must be monotonically non-decreasing in n.
	At(n uint64) time.Duration
	// Rate is the instantaneous target rate (hits/sec) at elapsed.
	Rate(elapsed time.Duration) float64
}

// Constant fires at a fixed rate (hits per second).
type Constant struct{ PerSec float64 }

// At implements Pacer.
func (c Constant) At(n uint64) time.Duration {
	if c.PerSec <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(float64(n) / c.PerSec * float64(time.Second))
}

// Rate implements Pacer.
func (c Constant) Rate(time.Duration) float64 { return c.PerSec }

// Linear ramps the rate linearly from From to To hits/sec over Period,
// holding To thereafter.
type Linear struct {
	From, To float64
	Period   time.Duration
}

// Rate implements Pacer.
func (l Linear) Rate(elapsed time.Duration) float64 {
	if elapsed >= l.Period || l.Period <= 0 {
		return l.To
	}
	frac := float64(elapsed) / float64(l.Period)
	return l.From + (l.To-l.From)*frac
}

// At implements Pacer. The cumulative hit count by time t (within the
// ramp) is a*t + (b-a)/(2T) * t²; At inverts that closed form, then
// continues at the terminal rate beyond the ramp.
func (l Linear) At(n uint64) time.Duration {
	a, b := l.From, l.To
	T := l.Period.Seconds()
	if T <= 0 || a == b {
		return Constant{PerSec: b}.At(n)
	}
	c := (b - a) / (2 * T)
	hitsInRamp := a*T + c*T*T // cumulative hits when the ramp ends
	x := float64(n)
	if x <= hitsInRamp {
		// Solve c·t² + a·t − x = 0 for t ≥ 0.
		if c == 0 {
			return secs(x / a)
		}
		t := (-a + math.Sqrt(a*a+4*c*x)) / (2 * c)
		return secs(t)
	}
	if b <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return secs(T + (x-hitsInRamp)/b)
}

func secs(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}
