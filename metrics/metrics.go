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
	reg           *prometheus.Registry
	sweepSeconds  prometheus.Histogram
	sweepSubs     prometheus.Histogram
	sweepTails    prometheus.Histogram
	sweepWakes    prometheus.Counter
	delivery      *prometheus.HistogramVec
	wakeEvent     *prometheus.HistogramVec
	workerDue     *prometheus.HistogramVec
	fanoutSeconds *prometheus.HistogramVec // label: stage (probe|total)
	fanoutSubs    prometheus.Histogram
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
		// 22 buckets from 1ms → ~4194s covers K=10k sweeps that were hitting the
		// old 16384ms (2^14 ms) ceiling and reporting a flat artifact.
		sweepSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_sweep_tick_seconds",
			Help:    "Recovery sweep tick wall-clock duration.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 22), // 1ms .. ~4194s
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
		// gate #2: OnStreamAppend fan-out latency, split by stage.
		// stage=probe  → Redis SMEMBERS index-lookup only.
		// stage=total  → probe + wake issuance for all S subscribers.
		fanoutSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_seconds",
			Help:    "OnStreamAppend fan-out duration by stage (probe|total).",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms .. ~52s
		}, []string{"stage"}),
		fanoutSubs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_subs",
			Help:    "Subscriber count (S) per OnStreamAppend fan-out.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1 .. 512
		}),
	}
	reg.MustRegister(
		p.sweepSeconds, p.sweepSubs, p.sweepTails, p.sweepWakes,
		p.delivery, p.wakeEvent, p.workerDue,
		p.fanoutSeconds, p.fanoutSubs,
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
func (p *Prometheus) FanOut(probeDur, totalDur time.Duration, subs int) {
	p.fanoutSeconds.WithLabelValues("probe").Observe(probeDur.Seconds())
	p.fanoutSeconds.WithLabelValues("total").Observe(totalDur.Seconds())
	p.fanoutSubs.Observe(float64(subs))
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
