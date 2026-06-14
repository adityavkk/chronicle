package report

import (
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

func TestMarkdownRendersAllSections(t *testing.T) {
	sc, err := scenario.Parse([]byte(`
name: demo
duration: 30s
streams: {count: 2}
writers: {per_stream: 1, rate: 10/s}
tailers: {sse_per_stream: 1}
`))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	r := &run.Result{
		Scenario:     sc,
		Label:        "chronicle-redis",
		BaseURL:      "http://localhost:4437",
		MeasureStart: start,
		MeasureEnd:   start.Add(30 * time.Second),
		Metrics: map[string]stats.Quantiles{
			string(stats.Append):      {Count: 600, Mean: 2.5, P50: 2, P99: 9, Max: 12},
			string(stats.DeliverySSE): {Count: 600, Mean: 3.1, P50: 3, P99: 8, Max: 14},
		},
		Counters: map[string]int64{
			"appends_ok":            600,
			"msgs_appended":         600,
			"bytes_appended":        72000,
			"msgs_sse":              600,
			"err:append:status=409": 2,
		},
		Resources: []run.ResourceSample{
			{Sec: 1, Name: "chronicle", RSSBytes: 100 << 20, CPUSeconds: 1.0},
			{Sec: 2, Name: "chronicle", RSSBytes: 110 << 20, CPUSeconds: 1.5},
			{Sec: 3, Name: "chronicle", RSSBytes: 120 << 20, CPUSeconds: 2.5},
		},
		Notes: []string{"example note"},
	}
	md := Markdown(r)
	for _, want := range []string{
		"# demo — chronicle-redis",
		"Append (scheduled→response)",
		"Delivery, SSE (write→receipt)",
		"| Appends (requests OK) | 600 | 20.0/s |",
		"append : status=409",
		"| chronicle | 110.0 MiB | 120.0 MiB | 75% | 100% |",
		"example note",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown missing %q\n---\n%s", want, md)
		}
	}
}
