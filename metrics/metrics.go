// Package metrics is the Prometheus implementation of webhook.Metrics plus the
// observability HTTP surface (/metrics, /healthz, /readyz) for the chronicle
// server. It is wired in only by the binary; the webhook package stays free of
// any metrics dependency behind the webhook.Metrics seam, so this is the one
// place the Prometheus client lives.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// Prometheus implements webhook.Metrics against a dedicated registry. The
// metric set is chosen to expose the sweep's scaling fault lines: per-tick
// duration and the K and U (subscriptions, unique tails) that drive it, plus
// wake-delivery latency and per-worker backlog.
type Prometheus struct {
	reg          *prometheus.Registry
	sweepSeconds prometheus.Histogram
	sweepSubs    prometheus.Histogram
	sweepTails   prometheus.Histogram
	sweepWakes   prometheus.Counter
	delivery     *prometheus.HistogramVec
	wakeEvent    *prometheus.HistogramVec
	workerDue    *prometheus.HistogramVec

	// Horizontal-scale golden signals (docs/specs/horizontal-scale/research/05
	// "New metrics"). Appended after the original set; see webhook.Metrics for the
	// append-only contract (GAP2). No-ops until the matching mechanism (#12–#15)
	// wires them to real call sites.
	fanoutSeconds     prometheus.Histogram
	fanoutSlotsProbed prometheus.Histogram
	fanoutSubs        prometheus.Histogram
	dueSetMutations   *prometheus.CounterVec
	dueWorkerSeconds  prometheus.Histogram
	dueWorkerFired    prometheus.Histogram
	slotOwnership     *prometheus.CounterVec
	coverageGap       prometheus.Histogram
	ownerFenced       *prometheus.CounterVec
	claimContention   *prometheus.CounterVec
}

var _ webhook.Metrics = (*Prometheus)(nil)

// New builds a Prometheus recorder with its own registry, including the standard
// Go-runtime and process collectors so a load test also sees GC pauses,
// goroutine count, and RSS — the host-side pressure that explains tail latency.
func New() *Prometheus {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	p := &Prometheus{
		reg: reg,
		sweepSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_sweep_tick_seconds",
			Help:    "Recovery sweep tick wall-clock duration.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16), // 0.5ms .. ~16s
		}),
		sweepSubs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_sweep_subs_evaluated",
			Help:    "Subscriptions evaluated per recovery sweep tick.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 18), // 1 .. ~131k
		}),
		sweepTails: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_sweep_tails_batched",
			Help:    "Distinct stream tails read (batched) per recovery sweep tick.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 20),
		}),
		sweepWakes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sweep_wakes_total",
			Help: "Wakes issued by the recovery sweep.",
		}),
		delivery: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_wake_delivery_seconds",
			Help:    "Webhook POST round-trip duration by outcome (ok|failed|error).",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		wakeEvent: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_wake_event_seconds",
			Help:    "Pull-wake event append duration by outcome (ok|error).",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		workerDue: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_worker_due_items",
			Help:    "Due items claimed per lease/retry worker tick, by kind.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{"kind"}),
		fanoutSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_seconds",
			Help:    "OnStreamAppend fan-out wall-clock duration under slot-homing (gate #2).",
			Buckets: prometheus.DefBuckets,
		}),
		fanoutSlotsProbed: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_slots_probed",
			Help:    "Slots probed per OnStreamAppend fan-out (occupied-slots bitmap effect).",
			Buckets: prometheus.ExponentialBuckets(1, 2, 9), // 1 .. 256
		}),
		fanoutSubs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_subs",
			Help:    "Subscribers found per OnStreamAppend fan-out.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}),
		dueSetMutations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_due_set_mutations_total",
			Help: "Per-subscription due-set mutations by op (arm|ack|expire) — gate #3 write amplification.",
		}, []string{"op"}),
		dueWorkerSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_due_worker_tick_seconds",
			Help:    "Due-worker tick wall-clock duration over one owned slot.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}),
		dueWorkerFired: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_due_worker_fired",
			Help:    "Owed subscriptions fired per due-worker tick.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		slotOwnership: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_slot_ownership_events_total",
			Help: "Slot-ownership lifecycle events by event (claimed|renewed|busy|released) — gate #4 churn/double-grant.",
		}, []string{"event"}),
		coverageGap: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_coverage_gap_seconds",
			Help:    "Latency of a sweep wake for a subscription whose slot was unowned at append (gate #4).",
			Buckets: prometheus.DefBuckets,
		}),
		ownerFenced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_owner_fenced_total",
			Help: "Owner-epoch fence firings by scope (check_owner|inline) — gate #4/#5.",
		}, []string{"scope"}),
		claimContention: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_claim_contention_total",
			Help: "Claim/ack lease outcomes by status (claimed|already_claimed|fenced|ok|nosub) — gate #6 per-type claim contention. already_claimed/op is the earliest collapse signal; fenced/op the tipping point.",
		}, []string{"status"}),
	}
	reg.MustRegister(p.sweepSeconds, p.sweepSubs, p.sweepTails, p.sweepWakes, p.delivery, p.wakeEvent, p.workerDue)
	reg.MustRegister(
		p.fanoutSeconds, p.fanoutSlotsProbed, p.fanoutSubs,
		p.dueSetMutations, p.dueWorkerSeconds, p.dueWorkerFired,
		p.slotOwnership, p.coverageGap, p.ownerFenced, p.claimContention,
	)
	return p
}

// SweepTick implements webhook.Metrics.
func (p *Prometheus) SweepTick(dur time.Duration, subs, tails, wakes int) {
	p.sweepSeconds.Observe(dur.Seconds())
	p.sweepSubs.Observe(float64(subs))
	p.sweepTails.Observe(float64(tails))
	p.sweepWakes.Add(float64(wakes))
}

// WakeDelivery implements webhook.Metrics.
func (p *Prometheus) WakeDelivery(dur time.Duration, outcome string) {
	p.delivery.WithLabelValues(outcome).Observe(dur.Seconds())
}

// WakeEvent implements webhook.Metrics.
func (p *Prometheus) WakeEvent(dur time.Duration, outcome string) {
	p.wakeEvent.WithLabelValues(outcome).Observe(dur.Seconds())
}

// WorkerTick implements webhook.Metrics.
func (p *Prometheus) WorkerTick(kind string, due int) {
	p.workerDue.WithLabelValues(kind).Observe(float64(due))
}

// FanOut implements webhook.Metrics.
func (p *Prometheus) FanOut(dur time.Duration, slotsProbed, subs int) {
	p.fanoutSeconds.Observe(dur.Seconds())
	p.fanoutSlotsProbed.Observe(float64(slotsProbed))
	p.fanoutSubs.Observe(float64(subs))
}

// DueSetMutation implements webhook.Metrics.
func (p *Prometheus) DueSetMutation(op string) {
	p.dueSetMutations.WithLabelValues(op).Inc()
}

// DueWorkerTick implements webhook.Metrics.
func (p *Prometheus) DueWorkerTick(dur time.Duration, fired int) {
	p.dueWorkerSeconds.Observe(dur.Seconds())
	p.dueWorkerFired.Observe(float64(fired))
}

// SlotOwnership implements webhook.Metrics. The affected slot is part of the
// seam (the call site logs it for tracing), but the recorder aggregates by
// event only: a per-slot label would be 256-cardinality and the gate-#4 signal
// is the event rate, not the per-slot breakdown.
func (p *Prometheus) SlotOwnership(event string, _ int) {
	p.slotOwnership.WithLabelValues(event).Inc()
}

// CoverageGap implements webhook.Metrics.
func (p *Prometheus) CoverageGap(dur time.Duration) {
	p.coverageGap.Observe(dur.Seconds())
}

// OwnerFenced implements webhook.Metrics.
func (p *Prometheus) OwnerFenced(scope string) {
	p.ownerFenced.WithLabelValues(scope).Inc()
}

// ClaimContention implements webhook.Metrics. Like SlotOwnership's slot arg, the
// subID is part of the call-site seam (logged for tracing) but the recorder
// aggregates by status only: a per-subID label would be type-cardinality and the
// gate-#6 signal is the status rate (already_claimed/op, fenced/op), not the
// per-subscription breakdown.
func (p *Prometheus) ClaimContention(status, _ string) {
	p.claimContention.WithLabelValues(status).Inc()
}

// Mux returns the observability HTTP surface: /metrics (Prometheus exposition),
// /healthz (liveness — 200 while the process serves), and /readyz (readiness —
// 200 when ready() returns nil, else 503). ready is typically a Redis ping, so
// a load-test harness and Kubernetes both hold traffic until the store is up.
func (p *Prometheus) Mux(ready func() error) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil {
			if err := ready(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}
