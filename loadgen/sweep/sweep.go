// Package sweep is a sweep-scale experiment runner: it seeds K subscriptions
// (each with P explicit links) against a running chronicle, lets the recovery
// sweep settle, then scrapes the chronicle_sweep_* metrics over a window to
// report how a single sweep tick scales with the subscription keyspace.
//
// The batched sweep reads every linked stream's tail whether the stream exists
// or not, so the links need not point at real streams to exercise the read
// cost — seeding is K subscription PUTs, not K*P stream creations. That keeps
// the experiment about the sweep, and lets it run to very large K cheaply.
package sweep

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
)

// Dur is a time.Duration that (un)marshals as a Go duration string ("30s") in
// both YAML and JSON, so scenario files and reports read naturally.
type Dur time.Duration

// D returns the underlying time.Duration.
func (d Dur) D() time.Duration { return time.Duration(d) }

// String renders the duration (e.g. "30s").
func (d Dur) String() string { return time.Duration(d).String() }

// UnmarshalYAML parses a duration string.
func (d *Dur) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	p, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Dur(p)
	return nil
}

// MarshalYAML renders the duration as a string.
func (d Dur) MarshalYAML() (any, error) { return d.String(), nil }

// UnmarshalJSON parses a quoted duration string.
func (d *Dur) UnmarshalJSON(b []byte) error {
	p, err := time.ParseDuration(strings.Trim(string(b), `"`))
	if err != nil {
		return err
	}
	*d = Dur(p)
	return nil
}

// MarshalJSON renders the duration as a quoted string.
func (d Dur) MarshalJSON() ([]byte, error) { return []byte(`"` + d.String() + `"`), nil }

// Spec is a declarative sweep-scale experiment.
type Spec struct {
	Name            string `yaml:"name" json:"name"`
	Subscriptions   int    `yaml:"subscriptions" json:"subscriptions"` // K
	LinksPerSub     int    `yaml:"links_per_sub" json:"links_per_sub"` // P
	Dispatch        string `yaml:"dispatch" json:"dispatch"`           // "pull-wake" (default) or "webhook"
	WebhookURL      string `yaml:"webhook_url" json:"webhook_url"`     // required for dispatch=webhook
	SharedStream    string `yaml:"shared_stream" json:"shared_stream"`
	OccupiedSlots   int    `yaml:"occupied_slots" json:"occupied_slots"`
	FanOutAppends   int    `yaml:"fanout_appends" json:"fanout_appends"`
	FanOutInterval  Dur    `yaml:"fanout_interval" json:"fanout_interval"`
	SeedConcurrency int    `yaml:"seed_concurrency" json:"seed_concurrency"`
	Warmup          Dur    `yaml:"warmup" json:"warmup"`   // settle time after seeding
	Measure         Dur    `yaml:"measure" json:"measure"` // metric sampling window
}

// Decode parses a YAML spec without applying defaults (so CLI flags can override
// before Prepared validates).
func Decode(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Spec{}, err
	}
	return s, nil
}

// Prepared returns a copy with defaults applied and the spec validated.
func (s Spec) Prepared() (Spec, error) {
	if s.LinksPerSub <= 0 {
		s.LinksPerSub = 1
	}
	if s.Dispatch == "" {
		s.Dispatch = "pull-wake"
	}
	if s.SeedConcurrency <= 0 {
		s.SeedConcurrency = 64
	}
	if s.Warmup == 0 {
		s.Warmup = Dur(5 * time.Second)
	}
	if s.Measure == 0 {
		s.Measure = Dur(30 * time.Second)
	}
	if s.Subscriptions <= 0 {
		return Spec{}, fmt.Errorf("subscriptions must be > 0")
	}
	if s.OccupiedSlots < 0 {
		return Spec{}, fmt.Errorf("occupied_slots must be >= 0")
	}
	if s.OccupiedSlots > 0 {
		if s.SharedStream == "" {
			return Spec{}, fmt.Errorf("shared_stream is required when occupied_slots is set")
		}
		if s.OccupiedSlots > subSlots {
			return Spec{}, fmt.Errorf("occupied_slots must be <= %d", subSlots)
		}
		if s.Subscriptions < s.OccupiedSlots {
			return Spec{}, fmt.Errorf("subscriptions must be >= occupied_slots")
		}
	}
	if s.SharedStream != "" && s.OccupiedSlots == 0 {
		s.OccupiedSlots = min(s.Subscriptions, subSlots)
	}
	if s.FanOutAppends < 0 {
		return Spec{}, fmt.Errorf("fanout_appends must be >= 0")
	}
	if s.FanOutAppends > 0 {
		if s.SharedStream == "" {
			return Spec{}, fmt.Errorf("shared_stream is required when fanout_appends is set")
		}
		if s.FanOutInterval == 0 {
			s.FanOutInterval = Dur(s.Measure.D() / time.Duration(s.FanOutAppends))
			if s.FanOutInterval == 0 {
				s.FanOutInterval = Dur(time.Millisecond)
			}
		}
	}
	switch s.Dispatch {
	case "pull-wake":
	case "webhook":
		if s.WebhookURL == "" {
			return Spec{}, fmt.Errorf("webhook_url required for dispatch=webhook")
		}
	default:
		return Spec{}, fmt.Errorf("dispatch must be pull-wake or webhook, got %q", s.Dispatch)
	}
	return s, nil
}

// Report is the result of one experiment.
type Report struct {
	Spec                   Spec               `json:"spec"`
	Seeded                 int                `json:"seeded"`
	SeedErrors             int                `json:"seed_errors"`
	SeedSeconds            float64            `json:"seed_seconds"`
	WindowSeconds          float64            `json:"window_seconds"`
	SweepTicks             float64            `json:"sweep_ticks"` // ticks observed during the window
	SweepMeanMs            float64            `json:"sweep_mean_ms"`
	SweepP50Ms             float64            `json:"sweep_p50_ms"`
	SweepP99Ms             float64            `json:"sweep_p99_ms"`
	MeanSubs               float64            `json:"mean_subs_evaluated"`
	MeanTails              float64            `json:"mean_tails_batched"`
	FanOutAppended         int                `json:"fanout_appended"`
	FanOutAppendErrors     int                `json:"fanout_append_errors"`
	FanOutCount            float64            `json:"fanout_count"`
	FanOutMeanMs           float64            `json:"fanout_mean_ms"`
	FanOutP50Ms            float64            `json:"fanout_p50_ms"`
	FanOutP99Ms            float64            `json:"fanout_p99_ms"`
	MeanFanOutSlots        float64            `json:"mean_fanout_slots_probed"`
	MeanFanOutSubscribers  float64            `json:"mean_fanout_subscribers"`
	ProposedMetricActivity map[string]float64 `json:"proposed_metric_activity"`
}

// Run executes the experiment against a chronicle base URL, scraping metricsURL.
// The caller's context bounds the whole run (seed + warmup + measure).
func Run(ctx context.Context, baseURL, root, metricsURL string, spec Spec) (Report, error) {
	cli := dsclient.New(baseURL, root)
	hc := &http.Client{Timeout: 15 * time.Second}

	if spec.SharedStream != "" {
		if resp, err := cli.Create(ctx, spec.SharedStream, "text/plain"); err != nil {
			return Report{}, fmt.Errorf("create shared stream: %w", err)
		} else if resp.Status >= http.StatusBadRequest && resp.Status != http.StatusConflict {
			return Report{}, fmt.Errorf("create shared stream: status %d", resp.Status)
		}
	}

	seedStart := time.Now()
	seeded, seedErrs := seedSubscriptions(ctx, cli, spec)
	seedDur := time.Since(seedStart)
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	defer cleanupSeeded(cli, spec)

	if !sleepCtx(ctx, spec.Warmup.D()) {
		return Report{}, ctx.Err()
	}

	names := []string{
		"chronicle_sweep_tick_seconds",
		"chronicle_sweep_subs_evaluated",
		"chronicle_sweep_tails_batched",
		"chronicle_fanout_seconds",
		"chronicle_fanout_slots_probed",
		"chronicle_fanout_subscribers",
	}
	before, err := scrape(ctx, hc, metricsURL, names...)
	if err != nil {
		return Report{}, fmt.Errorf("scrape before: %w", err)
	}
	appendDone := make(chan appendStats, 1)
	appendCancel := func() {}
	if spec.FanOutAppends > 0 {
		appendCtx, cancel := context.WithCancel(ctx)
		appendCancel = cancel
		go func() { appendDone <- appendFanOut(appendCtx, cli, spec) }()
	}
	if !sleepCtx(ctx, spec.Measure.D()) {
		appendCancel()
		return Report{}, ctx.Err()
	}
	appendCancel()
	after, err := scrape(ctx, hc, metricsURL, names...)
	if err != nil {
		return Report{}, fmt.Errorf("scrape after: %w", err)
	}
	appends := appendStats{}
	if spec.FanOutAppends > 0 {
		appends = <-appendDone
	}
	activity, err := scrapeMetricActivity(ctx, hc, metricsURL, proposedMetricNames...)
	if err != nil {
		return Report{}, fmt.Errorf("scrape proposed metrics: %w", err)
	}

	tick := after[names[0]].sub(before[names[0]])
	subs := after[names[1]].sub(before[names[1]])
	tails := after[names[2]].sub(before[names[2]])
	fanOut := after[names[3]].sub(before[names[3]])
	fanOutSlots := after[names[4]].sub(before[names[4]])
	fanOutSubs := after[names[5]].sub(before[names[5]])

	return Report{
		Spec:                   spec,
		Seeded:                 seeded,
		SeedErrors:             seedErrs,
		SeedSeconds:            seedDur.Seconds(),
		WindowSeconds:          spec.Measure.D().Seconds(),
		SweepTicks:             tick.count,
		SweepMeanMs:            tick.mean() * 1000,
		SweepP50Ms:             tick.quantile(0.5) * 1000,
		SweepP99Ms:             tick.quantile(0.99) * 1000,
		MeanSubs:               subs.mean(),
		MeanTails:              tails.mean(),
		FanOutAppended:         appends.appended,
		FanOutAppendErrors:     appends.errs,
		FanOutCount:            fanOut.count,
		FanOutMeanMs:           fanOut.mean() * 1000,
		FanOutP50Ms:            fanOut.quantile(0.5) * 1000,
		FanOutP99Ms:            fanOut.quantile(0.99) * 1000,
		MeanFanOutSlots:        fanOutSlots.mean(),
		MeanFanOutSubscribers:  fanOutSubs.mean(),
		ProposedMetricActivity: activity,
	}, nil
}

var proposedMetricNames = []string{
	"chronicle_fanout_seconds",
	"chronicle_due_set_mutations_total",
	"chronicle_due_worker_tick_seconds",
	"chronicle_slot_ownership_events_total",
	"chronicle_coverage_gap_seconds",
	"chronicle_owner_fenced_total",
}

// seedSubscriptions creates K subscriptions with a bounded worker pool.
func seedSubscriptions(ctx context.Context, cli *dsclient.Client, spec Spec) (seeded, errs int) {
	jobs := make(chan int, spec.SeedConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < spec.SeedConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				resp, err := cli.CreateSubscription(ctx, subscriptionID(spec, i), subscriptionBody(spec, i))
				mu.Lock()
				if err != nil || resp.Status >= 300 {
					errs++
				} else {
					seeded++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < spec.Subscriptions; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	return
}

func subscriptionBody(spec Spec, i int) []byte {
	streams := make([]string, spec.LinksPerSub)
	for j := range streams {
		if spec.SharedStream != "" {
			streams[j] = spec.SharedStream
		} else {
			streams[j] = fmt.Sprintf("loadtest/s/%d/%d", i, j)
		}
	}
	m := map[string]any{
		"type":         spec.Dispatch,
		"streams":      streams,
		"lease_ttl_ms": 30000,
	}
	if spec.Dispatch == "webhook" {
		m["webhook"] = map[string]string{"url": spec.WebhookURL}
	} else {
		m["wake_stream"] = fmt.Sprintf("loadtest/wake/%d", i)
	}
	b, _ := json.Marshal(m)
	return b
}

func subscriptionID(spec Spec, i int) string {
	if spec.OccupiedSlots <= 0 {
		return fmt.Sprintf("loadtest-%d", i)
	}
	slot := i % spec.OccupiedSlots
	return subscriptionIDForSlot(slot, i/spec.OccupiedSlots)
}

const subSlots = 256

func subscriptionIDForSlot(slot, ordinal int) string {
	for nonce := 0; ; nonce++ {
		id := fmt.Sprintf("loadtest-slot-%03d-%d-%d", slot, ordinal, nonce)
		if int(fnv32a(id)%subSlots) == slot {
			return id
		}
	}
}

func fnv32a(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

type appendStats struct {
	appended int
	errs     int
}

func appendFanOut(ctx context.Context, cli *dsclient.Client, spec Spec) appendStats {
	var appended, errs atomic.Int64
	interval := spec.FanOutInterval.D()
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; i < spec.FanOutAppends; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return appendStats{appended: int(appended.Load()), errs: int(errs.Load())}
			case <-ticker.C:
			}
		}
		resp, err := cli.Append(ctx, spec.SharedStream, "text/plain", []byte(fmt.Sprintf("fanout-%d", i)), nil)
		if err != nil || resp.Status >= http.StatusBadRequest {
			errs.Add(1)
			continue
		}
		appended.Add(1)
	}
	return appendStats{appended: int(appended.Load()), errs: int(errs.Load())}
}

func cleanupSeeded(cli *dsclient.Client, spec Spec) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	jobs := make(chan int, spec.SeedConcurrency)
	var wg sync.WaitGroup
	for w := 0; w < spec.SeedConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				_, _ = cli.DeleteSubscription(ctx, subscriptionID(spec, i))
			}
		}()
	}
	for i := 0; i < spec.Subscriptions; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	if spec.SharedStream != "" {
		_, _ = cli.Delete(ctx, spec.SharedStream)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
