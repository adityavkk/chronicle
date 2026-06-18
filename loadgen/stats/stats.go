// Package stats records and summarizes benchmark observations.
// Recording uses HDR histograms (lossless to 3 significant figures,
// losslessly mergeable across workers) — the authoritative source for
// percentiles. A per-second Series captures throughput shape over time.
//
// Recorders are handed out per worker to keep lock contention trivial;
// summarization is pure computation over merged histograms.
package stats

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// Metric names one latency distribution.
type Metric string

// The latency distributions dsload records.
const (
	// Append: intended send time → response fully read. Measuring from
	// the *scheduled* time (not the actual send) keeps queueing delay
	// visible when the SUT can't keep up (anti-coordinated-omission).
	Append Metric = "append"
	// DeliverySSE / DeliveryLongPoll: writer's wall-clock send timestamp
	// (embedded in the payload) → tailer receipt. End-to-end fan-out
	// latency, the protocol's headline signal.
	DeliverySSE      Metric = "delivery_sse"
	DeliveryLongPoll Metric = "delivery_long_poll"
	// CatchupTTFB / CatchupTotal: cold full-stream read; first byte and
	// complete body.
	CatchupTTFB  Metric = "catchup_ttfb"
	CatchupTotal Metric = "catchup_total"
)

// Histogram bounds: 1µs .. 120s at 3 significant figures.
const (
	histMin = 1
	histMax = 120_000_000 // µs
	histSig = 3
)

func newHist() *hdrhistogram.Histogram { return hdrhistogram.New(histMin, histMax, histSig) }

// Recorder accumulates observations for one worker. Safe for concurrent
// use; intended to be shared by at most a handful of goroutines.
type Recorder struct {
	gate *atomic.Bool
	mu   sync.Mutex
	hist map[Metric]*hdrhistogram.Histogram
	cnt  map[string]int64
}

// Record adds a latency observation if measurement is open.
func (r *Recorder) Record(m Metric, d time.Duration) {
	if !r.gate.Load() {
		return
	}
	us := d.Microseconds()
	if us < histMin {
		us = histMin
	}
	if us > histMax {
		us = histMax
	}
	r.mu.Lock()
	h, ok := r.hist[m]
	if !ok {
		h = newHist()
		r.hist[m] = h
	}
	h.RecordValue(us) //nolint:errcheck // value is clamped to bounds above
	r.mu.Unlock()
}

// Count increments a named counter if measurement is open.
func (r *Recorder) Count(name string, delta int64) {
	if !r.gate.Load() {
		return
	}
	r.mu.Lock()
	r.cnt[name] += delta
	r.mu.Unlock()
}

// CountError increments the error counter for an operation/class pair,
// e.g. ("append", "status=409") or ("sse", "transport").
func (r *Recorder) CountError(op, class string) {
	r.Count("err:"+op+":"+class, 1)
}

// Collector owns the measurement gate, the worker recorders, and the
// per-second series.
type Collector struct {
	recording atomic.Bool
	Series    *Series

	mu        sync.Mutex
	recorders []*Recorder
}

// NewCollector sizes the per-second series for a run of the given length.
func NewCollector(runSeconds int) *Collector {
	return &Collector{Series: newSeries(runSeconds + 120)}
}

// NewRecorder registers and returns a recorder for one worker.
func (c *Collector) NewRecorder() *Recorder {
	r := &Recorder{gate: &c.recording, hist: map[Metric]*hdrhistogram.Histogram{}, cnt: map[string]int64{}}
	c.mu.Lock()
	c.recorders = append(c.recorders, r)
	c.mu.Unlock()
	return r
}

// SetRecording opens or closes the measurement window. The Series is not
// gated — it always records, so warmup remains visible in the timeline.
func (c *Collector) SetRecording(on bool) { c.recording.Store(on) }

// Merged returns the merged histograms and summed counters across all
// recorders. Call after workers have stopped.
func (c *Collector) Merged() (map[Metric]*hdrhistogram.Histogram, map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hists := map[Metric]*hdrhistogram.Histogram{}
	counts := map[string]int64{}
	for _, r := range c.recorders {
		r.mu.Lock()
		for m, h := range r.hist {
			dst, ok := hists[m]
			if !ok {
				dst = newHist()
				hists[m] = dst
			}
			dst.Merge(h)
		}
		for k, v := range r.cnt {
			counts[k] += v
		}
		r.mu.Unlock()
	}
	return hists, counts
}

// Series is a set of per-second counters (timeline columns).
type Series struct {
	mu   sync.Mutex
	cols map[string][]int64
	size int
}

func newSeries(size int) *Series {
	return &Series{cols: map[string][]int64{}, size: size}
}

// Add increments column col at second sec.
func (s *Series) Add(col string, sec int, delta int64) {
	if sec < 0 || sec >= s.size {
		return
	}
	s.mu.Lock()
	c, ok := s.cols[col]
	if !ok {
		c = make([]int64, s.size)
		s.cols[col] = c
	}
	c[sec] += delta
	s.mu.Unlock()
}

// Snapshot returns the columns trimmed to the last non-zero second.
func (s *Series) Snapshot() map[string][]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := 0
	for _, c := range s.cols {
		for i := len(c) - 1; i >= 0; i-- {
			if c[i] != 0 {
				if i+1 > last {
					last = i + 1
				}
				break
			}
		}
	}
	out := map[string][]int64{}
	for k, c := range s.cols {
		out[k] = append([]int64(nil), c[:last]...)
	}
	return out
}

// Quantiles is a latency summary in milliseconds.
type Quantiles struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min_ms"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P75   float64 `json:"p75_ms"`
	P90   float64 `json:"p90_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

// Summarize reduces a histogram (µs values) to millisecond quantiles.
func Summarize(h *hdrhistogram.Histogram) Quantiles {
	ms := func(us int64) float64 { return float64(us) / 1000 }
	return Quantiles{
		Count: h.TotalCount(),
		Min:   ms(h.Min()),
		Mean:  h.Mean() / 1000,
		P50:   ms(h.ValueAtQuantile(50)),
		P75:   ms(h.ValueAtQuantile(75)),
		P90:   ms(h.ValueAtQuantile(90)),
		P95:   ms(h.ValueAtQuantile(95)),
		P99:   ms(h.ValueAtQuantile(99)),
		P999:  ms(h.ValueAtQuantile(99.9)),
		Max:   ms(h.Max()),
	}
}

// PercentileCurve renders an hdr-plot-compatible percentile distribution
// (value_ms percentile count) for archival alongside the summary.
func PercentileCurve(h *hdrhistogram.Histogram) []string {
	dist := h.CumulativeDistribution()
	lines := make([]string, 0, len(dist))
	for _, b := range dist {
		lines = append(lines, fmt.Sprintf("%.3f %.6f %d", float64(b.ValueAt)/1000, b.Quantile/100, b.Count))
	}
	return lines
}

// SortedKeys returns map keys in stable order — small helper for
// deterministic report rendering.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
