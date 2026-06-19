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

	// The methods below are the golden signals for the horizontal-scale
	// mechanisms (docs/specs/horizontal-scale/research/05 "New metrics"). The
	// interface is APPEND-ONLY: each new mechanism ships its method here with a
	// NopMetrics no-op AND a metrics.Prometheus implementation AND a
	// metrics/metrics_test.go golden entry in the same change, or CI breaks for
	// every downstream slice (epic #9, GAP2). They are wired to real call sites by
	// the issues that add the mechanism (#12–#15); they are no-ops until then.

	// FanOut records one OnStreamAppend fan-out under slot-homing: its wall-clock
	// duration, how many slots it probed, and how many subscribers it found.
	// Feeds gate #2 (fan-out p99 regression). Wired at OnStreamAppend in #15.
	FanOut(dur time.Duration, slotsProbed, subs int)
	// DueSetMutation records one mutation of a per-subscription due-set: op is
	// "arm", "ack", "expire", or "release" (GAP3). Feeds gate #3 (due write
	// amplification). Wired at the arm/ack/expire/release call sites in #12.
	DueSetMutation(op string)
	// DueWorkerTick records one due-worker pass over an owned slot: its duration
	// and how many owed subscriptions it fired. Feeds gate #3. Wired at dueWorker
	// in #12.
	DueWorkerTick(dur time.Duration, fired int)
	// SlotOwnership records a slot-ownership lifecycle event for slot: event is
	// "claimed", "renewed", "busy", or "released". Feeds gate #4 (churn,
	// double-grant). Wired at claim_shard / the reconcile loop in #14.
	SlotOwnership(event string, slot int)
	// CoverageGap records the latency of a wake the full sweep issued for a
	// subscription whose slot was unowned at append time — the rebalance coverage
	// gap. Feeds gate #4 (coverage-gap latency). Wired at sweepOnce in #14.
	CoverageGap(dur time.Duration)
	// OwnerFenced records an owner-epoch fence firing: scope is "check_owner"
	// (the external webhook POST) or "inline" (a schedule/due write). Feeds gate
	// #4/#5 (fence firing). Wired at check_owner / the inlined checks in #14.
	OwnerFenced(scope string)

	// ClaimContention records one claim/ack/release outcome on a subscription's
	// lease, the golden signal for the per-type claim-contention axis (the third
	// axis, 05 §"A third axis: per-type claim contention"; gate #6). status is the
	// op outcome, drawn from a fixed vocabulary so the gate can compute the
	// contention SLIs as rates over it:
	//   - claim site (claim.lua): "claimed" | "already_claimed" | "nosub"
	//   - ack/release site (ack.lua/release.lua): "ok" | "fenced" | "nosub"
	// The earliest collapse indicator is "already_claimed"/op (contenders bouncing
	// off a live lease); the tipping point is "fenced"/op (leases lapsing into
	// takeover, the fence storm). subID identifies the contended subscription for
	// call-site tracing; the recorder aggregates by status only (subID is
	// type-cardinality), the same discipline SlotOwnership uses for its slot arg.
	// Wired at the RedisStore claim/ack/release call sites in #11.
	ClaimContention(status, subID string)
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

// FanOut implements Metrics.
func (NopMetrics) FanOut(time.Duration, int, int) {}

// DueSetMutation implements Metrics.
func (NopMetrics) DueSetMutation(string) {}

// DueWorkerTick implements Metrics.
func (NopMetrics) DueWorkerTick(time.Duration, int) {}

// SlotOwnership implements Metrics.
func (NopMetrics) SlotOwnership(string, int) {}

// CoverageGap implements Metrics.
func (NopMetrics) CoverageGap(time.Duration) {}

// OwnerFenced implements Metrics.
func (NopMetrics) OwnerFenced(string) {}

// ClaimContention implements Metrics.
func (NopMetrics) ClaimContention(string, string) {}
