// Package metrics is the Prometheus implementation of webhook.Metrics plus the
// observability HTTP surface (/metrics, /healthz, /readyz) for the chronicle
// server. It is wired in only by the binary; the webhook package stays free of
// any metrics dependency behind the webhook.Metrics seam, so this is the one
// place the Prometheus client lives.
package metrics

import (
	"net/http"
	httppprof "net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
	"gecgithub01.walmart.com/auk000v/chronicle/store/segments"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// Prometheus implements webhook.Metrics against a dedicated registry. The
// metric set is chosen to expose the sweep's scaling fault lines: per-tick
// duration and the K and U (subscriptions, unique tails) that drive it, plus
// wake-delivery latency and per-worker backlog.
type Prometheus struct {
	reg                  *prometheus.Registry
	sweepSeconds         prometheus.Histogram
	sweepSubs            prometheus.Histogram
	sweepTails           prometheus.Histogram
	sweepWakes           prometheus.Counter
	delivery             *prometheus.HistogramVec
	wakeEvent            *prometheus.HistogramVec
	workerDue            *prometheus.HistogramVec
	readTarget           prometheus.Histogram
	readFetched          prometheus.Counter
	readReturned         prometheus.Counter
	readDiscarded        prometheus.Counter
	readPages            prometheus.Counter
	readPagesPerResponse prometheus.Histogram
	readScript           prometheus.Histogram
	readScriptInvokes    prometheus.Counter
	readResponse         prometheus.Counter
	readCanceled         *prometheus.CounterVec

	// Horizontal-scale golden signals (docs/specs/horizontal-scale/research/05
	// "New metrics"). Appended after the original set; see webhook.Metrics for the
	// append-only contract (GAP2). No-ops until the matching mechanism (#12–#15)
	// wires them to real call sites.
	fanoutSeconds     prometheus.Histogram
	fanoutSlotsProbed prometheus.Histogram
	fanoutSubs        prometheus.Histogram
	appendHookSeconds prometheus.Histogram
	dirtyEnqueues     *prometheus.CounterVec
	dirtyDepth        prometheus.Gauge
	dirtyCapacity     prometheus.Gauge
	dirtyOldestAge    prometheus.Gauge
	dirtyProcess      *prometheus.HistogramVec
	dirtyProcessSubs  prometheus.Counter
	dirtyProcessWakes prometheus.Counter
	dirtyDuplicates   prometheus.Counter
	dirtyOverflows    prometheus.Counter
	reconcileRequests *prometheus.CounterVec
	dirtyErrors       *prometheus.CounterVec
	dirtyRecovery     prometheus.Histogram
	dueSetMutations   *prometheus.CounterVec
	dueWorkerSeconds  prometheus.Histogram
	dueWorkerFired    prometheus.Histogram
	slotOwnership     *prometheus.CounterVec
	coverageGap       prometheus.Histogram
	ownerFenced       *prometheus.CounterVec
	claimContention   *prometheus.CounterVec
	durabilityShort   *prometheus.CounterVec
	serviceAccess     *prometheus.CounterVec

	appendFenceRejections    *prometheus.CounterVec
	appendFenceSeals         *prometheus.CounterVec
	appendFenceGrantFailures *prometheus.CounterVec

	sseHubs                prometheus.Gauge
	sseClients             prometheus.Gauge
	sseHubReads            prometheus.Counter
	sseHubMessages         prometheus.Counter
	sseHubRingBytes        prometheus.Gauge
	sseHubRingRawBytes     prometheus.Gauge
	sseHubRingWireBytes    prometheus.Gauge
	sseHubRingIndexBytes   prometheus.Gauge
	sseHubRefreshes        *prometheus.CounterVec
	sseHubRefreshPages     *prometheus.CounterVec
	sseHubRefreshBytes     *prometheus.CounterVec
	sseHubRefreshSeconds   *prometheus.HistogramVec
	ssePages               *prometheus.CounterVec
	ssePageBytes           *prometheus.CounterVec
	sseWatcherLookupSteps  prometheus.Counter
	sseWatcherLookupMisses prometheus.Counter
	sseLaggedDisconnects   prometheus.Counter
	sseWriteTimeouts       prometheus.Counter
	sseSubscriptions       prometheus.Gauge
	ssePhysicalConnections *prometheus.GaugeVec
	sseSubscriptionEvents  *prometheus.CounterVec
	sseReasons             *prometheus.CounterVec
}

// SegmentStatsSource is implemented by the feature-gated immutable read plane.
type SegmentStatsSource interface {
	Stats() segments.Stats
}

var (
	_ webhook.Metrics          = (*Prometheus)(nil)
	_ chronicle.AppendMetrics  = (*Prometheus)(nil)
	_ chronicle.ReadMetrics    = (*Prometheus)(nil)
	_ chronicle.ServiceMetrics = (*Prometheus)(nil)
	_ chronicle.FenceMetrics   = (*Prometheus)(nil)
	_ chronicle.SSEMetrics     = (*Prometheus)(nil)
)

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
		readTarget: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_read_page_target_bytes",
			Help:    "Requested storage page byte target for catch-up reads.",
			Buckets: prometheus.ExponentialBuckets(64<<10, 2, 8),
		}),
		readFetched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_fetched_bytes_total",
			Help: "Message payload bytes fetched by bounded storage pages.",
		}),
		readReturned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_returned_bytes_total",
			Help: "Message payload bytes returned by bounded storage pages.",
		}),
		readDiscarded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_discarded_bytes_total",
			Help: "Fetched payload bytes discarded at a storage page boundary.",
		}),
		readPages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_pages_total",
			Help: "Bounded storage pages evaluated for HTTP and SSE catch-up.",
		}),
		readPagesPerResponse: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_read_pages_per_response",
			Help:    "Bounded storage pages evaluated per HTTP or SSE response.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		readScript: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_read_redis_script_seconds",
			Help:    "Mean client-observed wall time per Redis script invocation within one bounded storage page, including pool and server queueing.",
			Buckets: prometheus.ExponentialBuckets(0.00005, 2, 18),
		}),
		readScriptInvokes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_redis_script_invocations_total",
			Help: "Redis script invocations used by bounded storage pages.",
		}),
		readResponse: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_read_response_bytes_total",
			Help: "HTTP and SSE catch-up response body bytes written.",
		}),
		readCanceled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_read_cancellations_total",
			Help: "Catch-up cancellations by fixed processing phase.",
		}, []string{"phase"}),
		fanoutSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_seconds",
			Help:    "Async fan-out subscriber lookup duration under slot-homing (gate #2 component).",
			Buckets: prometheus.DefBuckets,
		}),
		fanoutSlotsProbed: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_slots_probed",
			Help:    "Slots probed per async subscriber lookup (occupied-slots bitmap effect).",
			Buckets: prometheus.ExponentialBuckets(1, 2, 9), // 1 .. 256
		}),
		fanoutSubs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_fanout_subs",
			Help:    "Subscribers found per async fan-out lookup.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}),
		appendHookSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_append_subscription_hook_seconds",
			Help:    "Wall time from a successful append commit through all synchronous subscription hook work before the HTTP response.",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		dirtyEnqueues: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_enqueues_total",
			Help: "Committed append dirty-handoff outcomes by bounded result.",
		}, []string{"result"}),
		dirtyDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_subscription_dirty_queue_depth",
			Help: "Current process-local dirty stream entries, including work in progress.",
		}),
		dirtyCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_subscription_dirty_queue_capacity",
			Help: "Fixed process-local dirty stream capacity.",
		}),
		dirtyOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_subscription_dirty_oldest_age_seconds",
			Help: "Age of the oldest queued, processing, or overflowed dirty hint.",
		}),
		dirtyProcess: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_subscription_dirty_processing_seconds",
			Help:    "Async dirty batch processing duration by outcome (ok|error).",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18),
		}, []string{"outcome"}),
		dirtyProcessSubs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_subscribers_evaluated_total",
			Help: "Subscriptions hydrated and evaluated by async dirty fan-out.",
		}),
		dirtyProcessWakes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_wakes_armed_total",
			Help: "Wakes successfully armed by async dirty fan-out.",
		}),
		dirtyDuplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_duplicate_work_total",
			Help: "Async fan-out candidates already absent or in flight when evaluated.",
		}),
		dirtyOverflows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_overflow_total",
			Help: "Process-local dirty queue overflow epochs that requested eager recovery.",
		}),
		reconcileRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_subscription_reconcile_requests_total",
			Help: "Eager recovery requests by bounded reason and enqueue result.",
		}, []string{"reason", "result"}),
		dirtyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_subscription_dirty_processing_errors_total",
			Help: "Async dirty processing failures by fixed stage.",
		}, []string{"stage"}),
		dirtyRecovery: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronicle_subscription_dirty_recovery_delay_seconds",
			Help:    "Delay from a committed append hint or overflow epoch to successful async evaluation or recovery.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20),
		}),
		dueSetMutations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_due_set_mutations_total",
			Help: "Per-subscription due-set mutations by op (arm|ack|expire|release) — gate #3 write amplification.",
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
		durabilityShort: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_durability_short_total",
			Help: "Tier B fence-minting writes that reached the primary but could not prove durability within the WAIT/WAITAOF timeout, by command (WAITAOF|WAIT) — the RPO-exposure signal (issue #43, INV-DUR-01). Durability only: carries no holder/generation/exclusivity.",
		}, []string{"cmd"}),
		serviceAccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_service_access_total",
			Help: "Service authentication and authorization events by result: spiffe_authenticated, bearer_authenticated, authentication_failure, authorization_failure, or delegated_gateway.",
		}, []string{"result"}),
		appendFenceRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_append_fence_rejections_total",
			Help: "Data-plane write-fence rejections by reason (credential|shard|producer_required|principal|wake_token|precheck|marker|sealed|epoch|bound|store) — the primary zombie-writer signal (#183, ADR-0003 c8).",
		}, []string{"reason"}),
		appendFenceSeals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_append_fence_seals_total",
			Help: "Per-stream write-fence seals at done/release/delete/unlink by outcome (sealed|already|stale|notfound|unfenced|error) (#183).",
		}, []string{"outcome"}),
		appendFenceGrantFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_append_fence_grant_failures_total",
			Help: "Claim-marker grant failures by site (claim|heartbeat|webhook); the webhook site is the fail-open-delivery signal (#183).",
		}, []string{"site"}),
		sseHubs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_hubs",
			Help: "Active per-stream SSE fanout hubs on this Chronicle replica.",
		}),
		sseClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_clients",
			Help: "SSE clients attached to shared fanout hubs on this Chronicle replica.",
		}),
		sseHubReads: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_hub_reads_total",
			Help: "Durable stream reads performed by shared SSE hubs.",
		}),
		sseHubMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_hub_messages_total",
			Help: "Durable messages read once and offered to local SSE clients through a shared hub.",
		}),
		sseHubRingBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_hub_ring_bytes",
			Help: "Exact raw plus wire plus boundary-index bytes retained in all shared SSE replay rings on this replica.",
		}),
		sseHubRingRawBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_hub_ring_raw_bytes",
			Help: "Raw payload bytes retained in shared SSE replay rings on this replica.",
		}),
		sseHubRingWireBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_hub_ring_wire_bytes",
			Help: "Formatted SSE wire bytes retained in shared replay rings on this replica.",
		}),
		sseHubRingIndexBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_hub_ring_index_bytes",
			Help: "Offset-boundary index bytes retained in shared SSE replay rings on this replica.",
		}),
		sseHubRefreshes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_hub_refreshes_total",
			Help: "Bounded durable SSE hub refreshes by cause.",
		}, []string{"cause"}),
		sseHubRefreshPages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_hub_refresh_pages_total",
			Help: "Storage pages consumed by bounded SSE hub refreshes, by cause.",
		}, []string{"cause"}),
		sseHubRefreshBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_hub_refresh_bytes_total",
			Help: "Payload bytes consumed by bounded SSE hub refreshes, by cause.",
		}, []string{"cause"}),
		sseHubRefreshSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronicle_sse_hub_refresh_seconds",
			Help:    "Duration of one exact-tail SSE hub refresh, by cause.",
			Buckets: prometheus.DefBuckets,
		}, []string{"cause"}),
		ssePages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_pages_total",
			Help: "Bounded SSE storage pages by phase (catchup or confirm).",
		}, []string{"phase"}),
		ssePageBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_page_bytes_total",
			Help: "Returned payload bytes from bounded SSE storage pages by phase.",
		}, []string{"phase"}),
		sseWatcherLookupSteps: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_watcher_lookup_steps_total",
			Help: "Indexed replay-boundary comparisons performed by watcher lookup.",
		}),
		sseWatcherLookupMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_watcher_lookup_misses_total",
			Help: "Exact replay boundaries absent from the retained indexed ring.",
		}),
		sseLaggedDisconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_lagged_disconnects_total",
			Help: "SSE clients disconnected after falling behind the bounded shared replay window.",
		}),
		sseWriteTimeouts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronicle_sse_write_timeouts_total",
			Help: "SSE clients disconnected after one data-and-control flush exceeded its write deadline.",
		}),
		sseSubscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronicle_sse_subscriptions",
			Help: "Active logical stream notification registrations on this Chronicle replica.",
		}),
		ssePhysicalConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "chronicle_sse_notification_connections",
			Help: "Physical Redis Pub/Sub connections owned by the SSE notification multiplexer, by bounded topology.",
		}, []string{"topology"}),
		sseReasons: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_events_total",
			Help: "Bounded SSE recovery and disconnect events by reason.",
		}, []string{"reason"}),
		sseSubscriptionEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronicle_sse_subscription_events_total",
			Help: "Shared Redis notification subscription lifecycle events by event.",
		}, []string{"event"}),
	}
	reg.MustRegister(
		p.sweepSeconds, p.sweepSubs, p.sweepTails, p.sweepWakes,
		p.delivery, p.wakeEvent, p.workerDue,
		p.readTarget, p.readFetched, p.readReturned, p.readDiscarded,
		p.readPages, p.readPagesPerResponse, p.readScript,
		p.readScriptInvokes, p.readResponse, p.readCanceled,
	)
	reg.MustRegister(
		p.fanoutSeconds, p.fanoutSlotsProbed, p.fanoutSubs, p.appendHookSeconds,
		p.dirtyEnqueues, p.dirtyDepth, p.dirtyCapacity, p.dirtyOldestAge,
		p.dirtyProcess, p.dirtyProcessSubs, p.dirtyProcessWakes,
		p.dirtyDuplicates, p.dirtyOverflows, p.reconcileRequests, p.dirtyErrors,
		p.dirtyRecovery,
		p.dueSetMutations, p.dueWorkerSeconds, p.dueWorkerFired,
		p.slotOwnership, p.coverageGap, p.ownerFenced, p.claimContention,
		p.durabilityShort, p.serviceAccess,
		p.appendFenceRejections, p.appendFenceSeals, p.appendFenceGrantFailures,
		p.sseHubs, p.sseClients, p.sseHubReads, p.sseHubMessages,
		p.sseHubRingBytes, p.sseHubRingRawBytes, p.sseHubRingWireBytes,
		p.sseHubRingIndexBytes, p.sseHubRefreshes, p.sseHubRefreshPages,
		p.sseHubRefreshBytes, p.sseHubRefreshSeconds, p.ssePages,
		p.ssePageBytes, p.sseWatcherLookupSteps, p.sseWatcherLookupMisses,
		p.sseLaggedDisconnects, p.sseWriteTimeouts, p.sseSubscriptions,
		p.ssePhysicalConnections, p.sseSubscriptionEvents, p.sseReasons,
	)
	return p
}

// RegisterSegments adds issue-6 read-plane counters to this registry. It is
// called only when a segment mode is explicitly enabled, so the default
// observability surface stays byte-for-byte free of prototype-only series.
func (p *Prometheus) RegisterSegments(source SegmentStatsSource) {
	counter := func(name, help string, value func(segments.Stats) uint64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 {
			return float64(value(source.Stats()))
		})
	}
	seconds := func(name, help string, value func(segments.Stats) uint64, gauge bool) prometheus.Collector {
		read := func() float64 { return float64(value(source.Stats())) / float64(time.Second) }
		if gauge {
			return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, read)
		}
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, read)
	}
	gauge := func(name, help string, value func(segments.Stats) uint64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
			return float64(value(source.Stats()))
		})
	}
	p.reg.MustRegister(
		counter("chronicle_segment_seals_total", "Durable immutable manifest generations published.",
			func(s segments.Stats) uint64 { return s.Seals }),
		seconds("chronicle_segment_seal_seconds_total", "Wall time spent completing successful immutable durability barriers.",
			func(s segments.Stats) uint64 { return s.SealDurationNanoseconds }, false),
		seconds("chronicle_segment_seal_seconds_max", "Longest successful immutable durability barrier observed by this process.",
			func(s segments.Stats) uint64 { return s.SealDurationMaxNanoseconds }, true),
		counter("chronicle_segment_reads_total", "Reads which served at least one record from an immutable segment.",
			func(s segments.Stats) uint64 { return s.SegmentReads }),
		counter("chronicle_segment_primary_fallbacks_total", "Reads which used the complete primary Redis copy.",
			func(s segments.Stats) uint64 { return s.PrimaryFallbacks }),
		counter("chronicle_segment_checksum_failures_total", "Checksum or immutable-format failures rejected before serving bytes.",
			func(s segments.Stats) uint64 { return s.ChecksumFailures }),
		counter("chronicle_segment_bytes_served_total", "Payload bytes served from immutable segments.",
			func(s segments.Stats) uint64 { return s.BytesServed }),
		counter("chronicle_segment_origin_reads_total", "Data+index pairs fetched from the object origin.",
			func(s segments.Stats) uint64 { return s.Backend.OriginReads }),
		counter("chronicle_segment_origin_bytes_total", "Data and index bytes fetched from the object origin.",
			func(s segments.Stats) uint64 { return s.Backend.OriginBytes }),
		counter("chronicle_segment_cache_hits_total", "Checksum-verified local object-cache hits.",
			func(s segments.Stats) uint64 { return s.Backend.CacheHits }),
		counter("chronicle_segment_cache_misses_total", "Local object-cache misses, including rejected stale entries.",
			func(s segments.Stats) uint64 { return s.Backend.CacheMisses }),
		counter("chronicle_segment_cache_evictions_total", "Files removed to enforce the local object-cache byte bound.",
			func(s segments.Stats) uint64 { return s.Backend.CacheEvictions }),
		gauge("chronicle_segment_cache_bytes", "Current local object-cache occupancy in bytes.",
			func(s segments.Stats) uint64 { return s.Backend.CacheBytes }),
		counter("chronicle_segment_backend_bytes_read_total", "Encoded data and index bytes read by the candidate backend.",
			func(s segments.Stats) uint64 { return s.Backend.BytesRead }),
		counter("chronicle_segment_backend_bytes_written_total", "Encoded data and index bytes durably written by the candidate backend.",
			func(s segments.Stats) uint64 { return s.Backend.BytesWritten }),
		counter("chronicle_segment_redis_reads_total", "Logical immutable-segment reads from Redis strings.",
			func(s segments.Stats) uint64 { return s.Backend.RedisReads }),
		counter("chronicle_segment_redis_writes_total", "Logical immutable-segment writes to Redis strings.",
			func(s segments.Stats) uint64 { return s.Backend.RedisWrites }),
	)
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

// ReadPage records one bounded storage page. RedisScriptTime is zero for the
// memory backend. When a page used more than one script, the histogram records
// the mean while the counter preserves the invocation count.
func (p *Prometheus) ReadPage(targetBytes, fetchedBytes, returnedBytes, discardedBytes int, redisScriptTime time.Duration, redisScriptInvokes int) {
	p.readTarget.Observe(float64(targetBytes))
	p.readFetched.Add(float64(fetchedBytes))
	p.readReturned.Add(float64(returnedBytes))
	p.readDiscarded.Add(float64(discardedBytes))
	p.readPages.Inc()
	if redisScriptInvokes > 0 {
		p.readScript.Observe(redisScriptTime.Seconds() / float64(redisScriptInvokes))
		p.readScriptInvokes.Add(float64(redisScriptInvokes))
	}
}

// ReadResponse records the bytes written and the number of storage pages
// evaluated for one HTTP or SSE response.
func (p *Prometheus) ReadResponse(responseBytes, pages int) {
	p.readResponse.Add(float64(responseBytes))
	p.readPagesPerResponse.Observe(float64(pages))
}

// ReadCancellation records one fixed cancellation phase. Call sites pass only
// the bounded set documented by handler_stream.go and handler_sse.go.
func (p *Prometheus) ReadCancellation(phase string) {
	p.readCanceled.WithLabelValues(phase).Inc()
}

// FanOut implements webhook.Metrics.
func (p *Prometheus) FanOut(dur time.Duration, slotsProbed, subs int) {
	p.fanoutSeconds.Observe(dur.Seconds())
	p.fanoutSlotsProbed.Observe(float64(slotsProbed))
	p.fanoutSubs.Observe(float64(subs))
}

// AppendSubscriptionHook implements chronicle.AppendMetrics.
func (p *Prometheus) AppendSubscriptionHook(dur time.Duration) {
	p.appendHookSeconds.Observe(dur.Seconds())
}

// DirtyEnqueue implements webhook.Metrics.
func (p *Prometheus) DirtyEnqueue(result string, depth, capacity int, oldestAge time.Duration) {
	p.dirtyEnqueues.WithLabelValues(result).Inc()
	p.DirtyQueue(depth, capacity, oldestAge)
}

// DirtyQueue implements webhook.Metrics.
func (p *Prometheus) DirtyQueue(depth, capacity int, oldestAge time.Duration) {
	p.dirtyDepth.Set(float64(depth))
	p.dirtyCapacity.Set(float64(capacity))
	p.dirtyOldestAge.Set(oldestAge.Seconds())
}

// DirtyProcess implements webhook.Metrics.
func (p *Prometheus) DirtyProcess(dur time.Duration, subs, wakes, duplicates int, outcome string) {
	p.dirtyProcess.WithLabelValues(outcome).Observe(dur.Seconds())
	p.dirtyProcessSubs.Add(float64(subs))
	p.dirtyProcessWakes.Add(float64(wakes))
	p.dirtyDuplicates.Add(float64(duplicates))
}

// DirtyOverflow implements webhook.Metrics.
func (p *Prometheus) DirtyOverflow() { p.dirtyOverflows.Inc() }

// ReconcileRequest implements webhook.Metrics.
func (p *Prometheus) ReconcileRequest(scope, result string) {
	p.reconcileRequests.WithLabelValues(scope, result).Inc()
}

// DirtyProcessingError implements webhook.Metrics.
func (p *Prometheus) DirtyProcessingError(stage string) {
	p.dirtyErrors.WithLabelValues(stage).Inc()
}

// DirtyRecoveryDelay implements webhook.Metrics.
func (p *Prometheus) DirtyRecoveryDelay(dur time.Duration) {
	p.dirtyRecovery.Observe(dur.Seconds())
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

// DurabilityShort implements webhook.Metrics. cmd is the WAIT/WAITAOF barrier that
// fell short; the counter conveys durability only (the RPO-exposure rate), never a
// holder/generation/ack count — correction #3.
func (p *Prometheus) DurabilityShort(cmd string) {
	p.durabilityShort.WithLabelValues(cmd).Inc()
}

// ServiceSPIFFEAuthentication implements webhook.Metrics and
// chronicle.ServiceMetrics.
func (p *Prometheus) ServiceSPIFFEAuthentication() {
	p.serviceAccess.WithLabelValues("spiffe_authenticated").Inc()
}

// ServiceBearerAuthentication implements webhook.Metrics and
// chronicle.ServiceMetrics.
func (p *Prometheus) ServiceBearerAuthentication() {
	p.serviceAccess.WithLabelValues("bearer_authenticated").Inc()
}

// ServiceAuthenticationFailure implements webhook.Metrics and
// chronicle.ServiceMetrics.
func (p *Prometheus) ServiceAuthenticationFailure() {
	p.serviceAccess.WithLabelValues("authentication_failure").Inc()
}

// ServiceAuthorizationFailure implements webhook.Metrics and
// chronicle.ServiceMetrics.
func (p *Prometheus) ServiceAuthorizationFailure() {
	p.serviceAccess.WithLabelValues("authorization_failure").Inc()
}

// ServiceDelegatedGateway implements webhook.Metrics and
// chronicle.ServiceMetrics.
func (p *Prometheus) ServiceDelegatedGateway() {
	p.serviceAccess.WithLabelValues("delegated_gateway").Inc()
}

// AppendFenceRejection implements webhook.Metrics and chronicle.FenceMetrics.
func (p *Prometheus) AppendFenceRejection(reason string) {
	p.appendFenceRejections.WithLabelValues(reason).Inc()
}

// AppendFenceSeal implements webhook.Metrics.
func (p *Prometheus) AppendFenceSeal(outcome string) {
	p.appendFenceSeals.WithLabelValues(outcome).Inc()
}

// AppendFenceGrantFailed implements webhook.Metrics.
func (p *Prometheus) AppendFenceGrantFailed(site string) {
	p.appendFenceGrantFailures.WithLabelValues(site).Inc()
}

// SSEHubActive implements chronicle.SSEMetrics.
func (p *Prometheus) SSEHubActive(delta int) {
	p.sseHubs.Add(float64(delta))
}

// SSEClientActive implements chronicle.SSEMetrics.
func (p *Prometheus) SSEClientActive(delta int) {
	p.sseClients.Add(float64(delta))
}

// SSEHubRead implements chronicle.SSEMetrics.
func (p *Prometheus) SSEHubRead(messages int) {
	p.sseHubReads.Inc()
	p.sseHubMessages.Add(float64(messages))
}

// SSEHubRingBytes implements chronicle.SSEMetrics.
func (p *Prometheus) SSEHubRingBytes(rawDelta, wireDelta, indexDelta int) {
	p.sseHubRingRawBytes.Add(float64(rawDelta))
	p.sseHubRingWireBytes.Add(float64(wireDelta))
	p.sseHubRingIndexBytes.Add(float64(indexDelta))
	p.sseHubRingBytes.Add(float64(rawDelta + wireDelta + indexDelta))
}

// SSEHubRefresh implements chronicle.SSEMetrics.
func (p *Prometheus) SSEHubRefresh(cause string, pages, bytes int, duration time.Duration) {
	p.sseHubRefreshes.WithLabelValues(cause).Inc()
	p.sseHubRefreshPages.WithLabelValues(cause).Add(float64(pages))
	p.sseHubRefreshBytes.WithLabelValues(cause).Add(float64(bytes))
	p.sseHubRefreshSeconds.WithLabelValues(cause).Observe(duration.Seconds())
}

// SSEPage implements chronicle.SSEMetrics.
func (p *Prometheus) SSEPage(phase string, bytes int) {
	p.ssePages.WithLabelValues(phase).Inc()
	p.ssePageBytes.WithLabelValues(phase).Add(float64(bytes))
}

// SSEWatcherLookup implements chronicle.SSEMetrics.
func (p *Prometheus) SSEWatcherLookup(steps int, miss bool) {
	p.sseWatcherLookupSteps.Add(float64(steps))
	if miss {
		p.sseWatcherLookupMisses.Inc()
	}
}

// SSEReason implements chronicle.SSEMetrics.
func (p *Prometheus) SSEReason(reason string) {
	p.sseReasons.WithLabelValues(reason).Inc()
}

// SSEClientLagged implements chronicle.SSEMetrics.
func (p *Prometheus) SSEClientLagged() {
	p.sseLaggedDisconnects.Inc()
}

// SSEClientWriteTimeout implements chronicle.SSEMetrics.
func (p *Prometheus) SSEClientWriteTimeout() {
	p.sseWriteTimeouts.Inc()
}

// SSESubscriptionActive implements chronicle.SSEMetrics.
func (p *Prometheus) SSESubscriptionActive(delta int) {
	p.sseSubscriptions.Add(float64(delta))
}

// SSESubscriptionEvent implements chronicle.SSEMetrics.
func (p *Prometheus) SSESubscriptionEvent(event string) {
	p.sseSubscriptionEvents.WithLabelValues(event).Inc()
}

// NotificationPhysicalConnection implements store.NotificationMetrics.
func (p *Prometheus) NotificationPhysicalConnection(topology string, delta int) {
	p.ssePhysicalConnections.WithLabelValues(topology).Add(float64(delta))
}

// NotificationEvent implements store.NotificationMetrics.
func (p *Prometheus) NotificationEvent(event string) {
	p.sseSubscriptionEvents.WithLabelValues(event).Inc()
}

// EnableRuntimeProfiles turns on the block and mutex sampling needed for useful
// /debug/pprof/block and /debug/pprof/mutex profiles. CPU, heap, allocation, and
// goroutine profiles are available without it. The returned function restores
// the previous sampling configuration.
func EnableRuntimeProfiles() func() {
	runtime.SetBlockProfileRate(1)
	previousMutexFraction := runtime.SetMutexProfileFraction(1)
	return func() {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(previousMutexFraction)
	}
}

// Mux returns the observability HTTP surface: /metrics (Prometheus exposition),
// /healthz (liveness — 200 while the process serves), and /readyz (readiness —
// 200 when ready() returns nil, else 503). ready is typically a Redis ping, so
// a load-test harness and Kubernetes both hold traffic until the store is up.
// When enablePprof is true, Go runtime profiles are also exposed under
// /debug/pprof/. Keep that surface on a protected listener.
func (p *Prometheus) Mux(ready func() error, enablePprof ...bool) *http.ServeMux {
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
	if len(enablePprof) > 0 && enablePprof[0] {
		mux.HandleFunc("/debug/pprof/", httppprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	}
	return mux
}
