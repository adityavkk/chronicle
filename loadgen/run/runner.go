// Package run is the imperative shell: it wires a validated scenario to
// real clocks, sockets, and goroutines, and produces a Result. All
// decisions about *what* the workload looks like live in the pure
// packages (scenario, pace, payload, stats); this package only executes.
package run

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/pace"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// Options configures one run.
type Options struct {
	Scenario    scenario.Scenario
	Label       string            // SUT label, e.g. "caddy-mem", "chronicle-redis"
	SamplePIDs  map[string]int    // process name → pid to sample (RSS, CPU)
	SampleRedis map[string]string // name → host:port to sample via INFO
	KeepStreams bool              // skip deleting streams at teardown
	Logf        func(format string, args ...any)
}

// Result is the complete outcome of one run; it serializes to results.json.
type Result struct {
	Scenario     scenario.Scenario          `json:"scenario"`
	Label        string                     `json:"label"`
	BaseURL      string                     `json:"base_url"`
	Env          Env                        `json:"env"`
	StartedAt    time.Time                  `json:"started_at"`
	MeasureStart time.Time                  `json:"measure_start"`
	MeasureEnd   time.Time                  `json:"measure_end"`
	EndedAt      time.Time                  `json:"ended_at"`
	Metrics      map[string]stats.Quantiles `json:"metrics"`
	Counters     map[string]int64           `json:"counters"`
	Series       map[string][]int64         `json:"series"`
	Resources    []ResourceSample           `json:"resources,omitempty"`
	HDRCurves    map[string][]string        `json:"hdr_curves,omitempty"`
	Notes        []string                   `json:"notes,omitempty"`
}

// Env captures where the run happened.
type Env struct {
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	GoVersion string `json:"go_version"`
	Hostname  string `json:"hostname"`
}

// MeasureSeconds is the throughput denominator: the recorded window.
func (r *Result) MeasureSeconds() float64 {
	return r.MeasureEnd.Sub(r.MeasureStart).Seconds()
}

type runner struct {
	sc         scenario.Scenario
	cl         *dsclient.Client
	col        *stats.Collector
	start      time.Time // run anchor for series/stagger timing
	paceStart  time.Time // writers/catchup pacing anchor: set when they start
	appendSem  chan struct{}
	catchupSem chan struct{}
	logf       func(string, ...any)

	mu    sync.Mutex
	notes []string
}

func (r *runner) sec() int { return int(time.Since(r.start).Seconds()) }

func (r *runner) note(format string, args ...any) {
	r.mu.Lock()
	r.notes = append(r.notes, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

// drainGrace is how long after writers stop the tailers keep recording,
// so deliveries of the final appends are observed rather than truncated.
const drainGrace = 2 * time.Second

// Run executes the scenario and returns the result.
func Run(ctx context.Context, opts Options) (*Result, error) {
	sc := opts.Scenario
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	cl := dsclient.New(sc.Target.BaseURL, sc.Target.StreamRoot)

	if err := preflight(ctx, cl, sc); err != nil {
		return nil, fmt.Errorf("preflight against %s failed: %w", sc.Target.BaseURL, err)
	}
	logf("preflight ok: %s (root %s)", sc.Target.BaseURL, sc.Target.StreamRoot)

	totalSec := int((sc.Warmup.Duration + sc.Duration.Duration).Seconds())
	r := &runner{
		sc:         sc,
		cl:         cl,
		col:        stats.NewCollector(totalSec),
		appendSem:  make(chan struct{}, sc.Limits.MaxInFlightAppends),
		catchupSem: make(chan struct{}, sc.Limits.MaxInFlightCatchup),
		logf:       logf,
	}

	startedAt := time.Now()
	if err := r.createStreams(ctx); err != nil {
		return nil, err
	}
	if sc.Streams.Prefill.Messages > 0 {
		if err := r.prefill(ctx); err != nil {
			return nil, err
		}
	}

	// Sampler runs for the whole workload.
	samplerCtx, stopSampler := context.WithCancel(ctx)
	defer stopSampler()
	sampler := newSampler(opts.SamplePIDs, opts.SampleRedis, logf)
	var samplerWG sync.WaitGroup

	// Tailers attach before writers start so offset=now sees everything.
	tailerCtx, cancelTailers := context.WithCancel(ctx)
	defer cancelTailers()
	var tailerWG sync.WaitGroup
	var connected atomic.Int64
	r.start = time.Now() // anchor: tailer stagger and pacing share this
	sampler.start(samplerCtx, &samplerWG, r.start)
	r.startTailers(tailerCtx, &tailerWG, &connected)
	r.awaitTailers(ctx, &connected)

	writerCtx, cancelWriters := context.WithCancel(ctx)
	defer cancelWriters()
	var writerWG sync.WaitGroup
	// Pacing anchors *here*, after tailer attachment: schedules must not
	// inherit a backlog from setup time, or the first seconds become an
	// artificial burst.
	r.paceStart = time.Now()
	r.startWriters(writerCtx, &writerWG)
	r.startCatchup(writerCtx, &writerWG)

	if err := sleepCtx(ctx, sc.Warmup.Duration); err != nil {
		return nil, err
	}
	r.col.SetRecording(true)
	measureStart := time.Now()
	logf("measurement window open (%s)", sc.Duration)

	err := sleepCtx(ctx, sc.Duration.Duration)
	cancelWriters()
	writerWG.Wait() // includes in-flight appends (bounded by request timeout)
	measureEnd := time.Now()
	if err != nil {
		r.note("run interrupted: %v", err)
	}

	_ = sleepCtx(ctx, drainGrace)
	r.col.SetRecording(false)
	cancelTailers()
	tailerWG.Wait()
	stopSampler()
	samplerWG.Wait()
	logf("measurement window closed; tearing down")

	if sc.Writers.CloseStreams {
		r.closeStreams(context.WithoutCancel(ctx))
	}
	if !opts.KeepStreams {
		r.deleteStreams(context.WithoutCancel(ctx))
	}

	hists, counts := r.col.Merged()
	metrics := map[string]stats.Quantiles{}
	curves := map[string][]string{}
	for m, h := range hists {
		metrics[string(m)] = stats.Summarize(h)
		curves[string(m)] = stats.PercentileCurve(h)
	}
	host, _ := os.Hostname()
	return &Result{
		Scenario:     sc,
		Label:        opts.Label,
		BaseURL:      sc.Target.BaseURL,
		Env:          Env{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(), Hostname: host},
		StartedAt:    startedAt,
		MeasureStart: measureStart,
		MeasureEnd:   measureEnd,
		EndedAt:      time.Now(),
		Metrics:      metrics,
		Counters:     counts,
		Series:       r.col.Series.Snapshot(),
		Resources:    sampler.samples(),
		HDRCurves:    curves,
		Notes:        r.notes,
	}, nil
}

// preflight verifies the target speaks the protocol before any load.
func preflight(ctx context.Context, cl *dsclient.Client, sc scenario.Scenario) error {
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	probe := sc.Streams.Prefix + "-preflight"
	if resp, err := cl.Create(pctx, probe, sc.Streams.ContentType); err != nil {
		return fmt.Errorf("create probe stream: %w", err)
	} else if resp.Status != 201 && resp.Status != 200 {
		return fmt.Errorf("create probe stream: status %d", resp.Status)
	}
	body := []byte(`{"i":0,"t":0,"p":"preflight"}`)
	if sc.Streams.ContentType != "application/json" {
		body = append(body, '\n')
	}
	if resp, err := cl.Append(pctx, probe, sc.Streams.ContentType, body, nil); err != nil {
		return fmt.Errorf("append to probe stream: %w", err)
	} else if resp.Status/100 != 2 {
		return fmt.Errorf("append to probe stream: status %d", resp.Status)
	}
	resp, err := cl.Read(pctx, probe, "-1", "", "")
	if err != nil {
		return fmt.Errorf("read probe stream: %w", err)
	}
	if resp.Status != 200 || resp.NextOffset == "" {
		return fmt.Errorf("read probe stream: status %d, Stream-Next-Offset %q", resp.Status, resp.NextOffset)
	}
	if _, err := cl.Delete(pctx, probe); err != nil {
		return fmt.Errorf("delete probe stream: %w", err)
	}
	return nil
}

func pacerFromRate(rate scenario.Rate, period time.Duration) pace.Pacer {
	if rate.IsRamp() {
		return pace.Linear{From: rate.From, To: rate.To, Period: period}
	}
	return pace.Constant{PerSec: rate.From}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func classify(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			return "timeout"
		}
		return "transport"
	}
}
