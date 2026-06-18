// Package report renders a run.Result into a human-readable markdown
// summary. Pure: Result in, string out.
package report

import (
	"fmt"
	"sort"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// metricOrder fixes the latency table ordering.
var metricOrder = []string{
	string(stats.Append),
	string(stats.DeliverySSE),
	string(stats.DeliveryLongPoll),
	string(stats.CatchupTTFB),
	string(stats.CatchupTotal),
}

var metricLabels = map[string]string{
	string(stats.Append):           "Append (scheduled→response)",
	string(stats.DeliverySSE):      "Delivery, SSE (write→receipt)",
	string(stats.DeliveryLongPoll): "Delivery, long-poll (write→receipt)",
	string(stats.CatchupTTFB):      "Catch-up TTFB",
	string(stats.CatchupTotal):     "Catch-up full read",
}

// Markdown renders the summary document.
func Markdown(r *run.Result) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	sc := r.Scenario
	w("# %s — %s\n\n", sc.Name, r.Label)
	if sc.Description != "" {
		w("%s\n\n", strings.TrimSpace(sc.Description))
	}
	w("- **Target**: `%s` (stream root `%s`)\n", r.BaseURL, sc.Target.StreamRoot)
	w("- **Run**: %s → %s (measured window %.1fs after %s warmup)\n",
		r.MeasureStart.Format("2006-01-02 15:04:05"), r.MeasureEnd.Format("15:04:05"),
		r.MeasureSeconds(), sc.Warmup)
	w("- **Host**: %s, %s/%s, %d CPUs, %s\n\n", r.Env.Hostname, r.Env.GOOS, r.Env.GOARCH, r.Env.NumCPU, r.Env.GoVersion)

	w("## Workload\n\n")
	w("- Streams: %d (`%s-*`, %s)\n", sc.Streams.Count, sc.Streams.Prefix, sc.Streams.ContentType)
	if sc.Streams.Prefill.Messages > 0 {
		w("- Prefill: %d × %dB messages per stream\n", sc.Streams.Prefill.Messages, sc.Streams.Prefill.MessageBytes)
	}
	if sc.Writers.PerStream > 0 {
		w("- Writers: %d (%d/stream) @ %s each, %dB messages, batch %d, producer %s\n",
			sc.TotalWriters(), sc.Writers.PerStream, sc.Writers.Rate, sc.Writers.MessageBytes, sc.Writers.Batch, sc.Writers.Producer)
	}
	if sc.Tailers.SSEPerStream > 0 || sc.Tailers.LongPollPerStream > 0 {
		w("- Tailers: %d SSE + %d long-poll per stream (%d total), from `%s`\n",
			sc.Tailers.SSEPerStream, sc.Tailers.LongPollPerStream, sc.TotalTailers(), sc.Tailers.From)
	}
	if !sc.Catchup.Rate.IsZero() {
		w("- Catch-up reads: %s (offset=-1, random stream)\n", sc.Catchup.Rate)
	}
	w("\n")

	w("## Throughput (measured window)\n\n")
	w("| Signal | Total | Per second |\n|---|---:|---:|\n")
	secs := r.MeasureSeconds()
	row := func(label, counter string, scale float64, unit string) {
		v := r.Counters[counter]
		if v == 0 {
			return
		}
		w("| %s | %s | %s%s |\n", label, fmtCount(float64(v), scale, unit), fmtCount(float64(v)/secs, scale, unit), "/s")
	}
	row("Appends (requests OK)", "appends_ok", 1, "")
	row("Messages appended", "msgs_appended", 1, "")
	row("Bytes appended", "bytes_appended", 1024*1024, " MiB")
	row("Messages delivered (SSE)", "msgs_sse", 1, "")
	row("Bytes delivered (SSE)", "bytes_sse", 1024*1024, " MiB")
	row("Messages delivered (long-poll)", "msgs_long_poll", 1, "")
	row("Bytes delivered (long-poll)", "bytes_long_poll", 1024*1024, " MiB")
	row("Long-poll requests (200)", "long_poll_200", 1, "")
	row("Long-poll requests (204 timeout)", "long_poll_204", 1, "")
	row("Catch-up reads OK", "catchup_ok", 1, "")
	row("Catch-up bytes", "catchup_bytes", 1024*1024, " MiB")
	row("SSE reconnects", "sse_reconnects", 1, "")
	row("Appends dropped (client cap)", "appends_dropped", 1, "")
	row("Catch-ups dropped (client cap)", "catchup_dropped", 1, "")
	w("\n")

	w("## Latency (ms)\n\n")
	w("| Metric | Count | Min | Mean | p50 | p90 | p95 | p99 | p99.9 | Max |\n")
	w("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range metricOrder {
		q, ok := r.Metrics[m]
		if !ok {
			continue
		}
		label := metricLabels[m]
		if label == "" {
			label = m
		}
		w("| %s | %d | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
			label, q.Count, q.Min, q.Mean, q.P50, q.P90, q.P95, q.P99, q.P999, q.Max)
	}
	w("\n")

	if errs := errorRows(r.Counters); len(errs) > 0 {
		w("## Errors\n\n| Operation : class | Count |\n|---|---:|\n")
		for _, e := range errs {
			w("| %s | %d |\n", e.key, e.n)
		}
		w("\n")
	} else {
		w("## Errors\n\nNone observed in the measured window.\n\n")
	}

	if rows := resourceRows(r); len(rows) > 0 {
		w("## Resources (sampled 1s; CPU%% from cumulative deltas)\n\n")
		w("| Process | RSS mean | RSS max | CPU mean | CPU max |\n|---|---:|---:|---:|---:|\n")
		for _, x := range rows {
			w("| %s | %.1f MiB | %.1f MiB | %.0f%% | %.0f%% |\n", x.name, x.rssMean, x.rssMax, x.cpuMean, x.cpuMax)
		}
		w("\n")
	}

	if len(r.Notes) > 0 {
		w("## Notes\n\n")
		for _, n := range r.Notes {
			w("- %s\n", n)
		}
		w("\n")
	}
	return b.String()
}

func fmtCount(v, scale float64, unit string) string {
	v /= scale
	switch {
	case unit != "":
		return fmt.Sprintf("%.2f%s", v, unit)
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.1f", v)
	}
}

type errRow struct {
	key string
	n   int64
}

func errorRows(counters map[string]int64) []errRow {
	var rows []errRow
	for k, v := range counters {
		if rest, ok := strings.CutPrefix(k, "err:"); ok {
			rows = append(rows, errRow{strings.ReplaceAll(rest, ":", " : "), v})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].key < rows[j].key })
	return rows
}

type resRow struct {
	name            string
	rssMean, rssMax float64
	cpuMean, cpuMax float64
}

// resourceRows reduces raw samples per process: RSS straight averages,
// CPU% via per-interval deltas of cumulative CPU seconds.
func resourceRows(r *run.Result) []resRow {
	byName := map[string][]run.ResourceSample{}
	for _, s := range r.Resources {
		byName[s.Name] = append(byName[s.Name], s)
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	rows := make([]resRow, 0, len(names))
	for _, name := range names {
		ss := byName[name]
		sort.Slice(ss, func(i, j int) bool { return ss[i].Sec < ss[j].Sec })
		var rssSum, rssMax float64
		for _, s := range ss {
			mb := float64(s.RSSBytes) / (1024 * 1024)
			rssSum += mb
			if mb > rssMax {
				rssMax = mb
			}
		}
		var cpuSum, cpuMax float64
		intervals := 0
		for i := 1; i < len(ss); i++ {
			dt := float64(ss[i].Sec - ss[i-1].Sec)
			if dt <= 0 {
				continue
			}
			pct := (ss[i].CPUSeconds - ss[i-1].CPUSeconds) / dt * 100
			if pct < 0 {
				continue
			}
			cpuSum += pct
			intervals++
			if pct > cpuMax {
				cpuMax = pct
			}
		}
		row := resRow{name: name, rssMax: rssMax}
		if len(ss) > 0 {
			row.rssMean = rssSum / float64(len(ss))
		}
		if intervals > 0 {
			row.cpuMean = cpuSum / float64(intervals)
		}
		row.cpuMax = cpuMax
		rows = append(rows, row)
	}
	return rows
}
