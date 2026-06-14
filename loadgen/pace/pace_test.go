package pace

import (
	"math"
	"testing"
	"time"
)

func TestConstantSchedule(t *testing.T) {
	p := Constant{PerSec: 10}
	if got := p.At(0); got != 0 {
		t.Errorf("At(0) = %v", got)
	}
	if got := p.At(10); got != time.Second {
		t.Errorf("At(10) = %v, want 1s", got)
	}
	if got := p.Rate(5 * time.Second); got != 10 {
		t.Errorf("Rate = %v", got)
	}
}

func TestConstantZeroNeverFires(t *testing.T) {
	p := Constant{PerSec: 0}
	if got := p.At(1); got != time.Duration(math.MaxInt64) {
		t.Errorf("At(1) = %v, want max", got)
	}
}

// The linear ramp's closed form must invert its cumulative-hits curve:
// hits(At(n)) == n, and the schedule must be monotonic.
func TestLinearRampInvertsCumulativeHits(t *testing.T) {
	p := Linear{From: 5, To: 50, Period: 30 * time.Second}
	cumulative := func(d time.Duration) float64 {
		ts := d.Seconds()
		T := p.Period.Seconds()
		if ts <= T {
			return p.From*ts + (p.To-p.From)/(2*T)*ts*ts
		}
		rampHits := p.From*T + (p.To-p.From)/2*T
		return rampHits + (ts-T)*p.To
	}
	prev := time.Duration(-1)
	for _, n := range []uint64{0, 1, 7, 100, 500, 824, 2000} {
		at := p.At(n)
		if at <= prev {
			t.Errorf("At(%d) = %v not after previous %v", n, at, prev)
		}
		prev = at
		if got := cumulative(at); math.Abs(got-float64(n)) > 1e-6*float64(n)+1e-6 {
			t.Errorf("cumulative(At(%d)) = %f, want %d", n, got, n)
		}
	}
	// hits(30s) = 5*30 + 45/60*900/... = 825 total during ramp; hit 825 onwards paced at 50/s.
	atEnd := p.At(825)
	if math.Abs(atEnd.Seconds()-30) > 0.01 {
		t.Errorf("At(825) = %v, want ~30s (ramp end)", atEnd)
	}
	after := p.At(875)
	if math.Abs(after.Seconds()-31) > 0.01 {
		t.Errorf("At(875) = %v, want ~31s (50/s after ramp)", after)
	}
}

func TestLinearRampRate(t *testing.T) {
	p := Linear{From: 0, To: 100, Period: 10 * time.Second}
	if got := p.Rate(5 * time.Second); got != 50 {
		t.Errorf("Rate(5s) = %v, want 50", got)
	}
	if got := p.Rate(20 * time.Second); got != 100 {
		t.Errorf("Rate(20s) = %v, want 100", got)
	}
}

func TestLinearDegeneratesToConstant(t *testing.T) {
	p := Linear{From: 10, To: 10, Period: 10 * time.Second}
	if got := p.At(20); got != 2*time.Second {
		t.Errorf("At(20) = %v, want 2s", got)
	}
}
