package sweep

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// hist is a snapshot of a Prometheus histogram: the total observation count, the
// sum of observed values, and cumulative bucket counts. It is enough to recover
// the mean and (by the histogram_quantile method) approximate quantiles over a
// window by diffing two snapshots.
type hist struct {
	count   float64
	sum     float64
	buckets []bucket // sorted ascending by le; the last is +Inf
}

type bucket struct {
	le    float64
	count float64 // cumulative
}

func (h hist) mean() float64 {
	if h.count == 0 {
		return 0
	}
	return h.sum / h.count
}

// quantile approximates the q-quantile (0..1) by linear interpolation within the
// bucket where the cumulative count crosses q*count — the same method PromQL's
// histogram_quantile uses, with the same coarseness.
func (h hist) quantile(q float64) float64 {
	if h.count == 0 || len(h.buckets) == 0 {
		return 0
	}
	rank := q * h.count
	prevLe, prevCount := 0.0, 0.0
	for _, b := range h.buckets {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				return prevLe
			}
			if b.count == prevCount {
				return b.le
			}
			return prevLe + (b.le-prevLe)*((rank-prevCount)/(b.count-prevCount))
		}
		prevLe, prevCount = b.le, b.count
	}
	return prevLe
}

// sub returns the per-window delta histogram (h - start): counts and sum
// subtracted so mean/quantile reflect only observations made during the window.
func (h hist) sub(start hist) hist {
	out := hist{count: h.count - start.count, sum: h.sum - start.sum}
	startCounts := make(map[float64]float64, len(start.buckets))
	for _, b := range start.buckets {
		startCounts[b.le] = b.count
	}
	for _, b := range h.buckets {
		out.buckets = append(out.buckets, bucket{le: b.le, count: b.count - startCounts[b.le]})
	}
	return out
}

// scrape fetches the Prometheus exposition at metricsURL and parses the named
// histograms (base names, e.g. "chronicle_sweep_tick_seconds"), returning base
// name -> hist. Labeled series are ignored, which is correct for the unlabeled
// sweep histograms this rig reads.
func scrape(ctx context.Context, hc *http.Client, metricsURL string, names ...string) (map[string]hist, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read side
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: status %d", resp.StatusCode)
	}
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	out := make(map[string]hist, len(names))
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		series, labels := fields[0], ""
		if brace := strings.IndexByte(fields[0], '{'); brace >= 0 {
			series, labels = fields[0][:brace], fields[0][brace:]
		}
		switch {
		case strings.HasSuffix(series, "_bucket"):
			base := strings.TrimSuffix(series, "_bucket")
			if !want[base] {
				continue
			}
			h := out[base]
			h.buckets = append(h.buckets, bucket{le: leFromLabels(labels), count: val})
			out[base] = h
		case strings.HasSuffix(series, "_sum"):
			base := strings.TrimSuffix(series, "_sum")
			if want[base] {
				h := out[base]
				h.sum = val
				out[base] = h
			}
		case strings.HasSuffix(series, "_count"):
			base := strings.TrimSuffix(series, "_count")
			if want[base] {
				h := out[base]
				h.count = val
				out[base] = h
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for base, h := range out {
		sort.Slice(h.buckets, func(i, j int) bool { return h.buckets[i].le < h.buckets[j].le })
		out[base] = h
	}
	return out, nil
}

func leFromLabels(labels string) float64 {
	i := strings.Index(labels, `le="`)
	if i < 0 {
		return math.Inf(1)
	}
	rest := labels[i+4:]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return math.Inf(1)
	}
	if v := rest[:j]; v != "+Inf" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return math.Inf(1)
}
