package webhook

import "time"

// Metrics is the optional observability seam the Manager records to. It is the
// instrumentation analogue of the Streams and Store seams: a nil Metrics (or the
// NopMetrics default) records nothing, so the pure core and the Redis paths stay
// independent of any metrics library, and tests need no instrumentation. The
// chronicle binary wires in a Prometheus implementation; see package metrics.
//
// These are exactly the signals a load test needs to find the sweep's fault
// lines: per-tick sweep cost (the scaling ceiling), wake-delivery latency, and
// how much work the lease/retry workers claim each tick.
type Metrics interface {
	// SweepTick records one recovery-sweep pass: its wall-clock duration, how
	// many subscriptions it evaluated, the distinct linked tails it batched, and
	// how many wakes it issued.
	SweepTick(dur time.Duration, subs, tails, wakes int)
	// WakeDelivery records a webhook delivery attempt: the round-trip duration and
	// its outcome ("ok", "failed", or "error").
	WakeDelivery(dur time.Duration, outcome string)
	// WakeEvent records a pull-wake event append to a wake stream: the duration
	// and its outcome ("ok" or "error").
	WakeEvent(dur time.Duration, outcome string)
	// WorkerTick records one lease/retry worker pass: the kind ("lease" or
	// "retry") and how many due items it claimed this tick.
	WorkerTick(kind string, due int)
}

// NopMetrics is the no-op Metrics used when none is configured. The Manager
// defaults to it, so instrumentation is strictly opt-in.
type NopMetrics struct{}

// SweepTick implements Metrics.
func (NopMetrics) SweepTick(time.Duration, int, int, int) {}

// WakeDelivery implements Metrics.
func (NopMetrics) WakeDelivery(time.Duration, string) {}

// WakeEvent implements Metrics.
func (NopMetrics) WakeEvent(time.Duration, string) {}

// WorkerTick implements Metrics.
func (NopMetrics) WorkerTick(string, int) {}
