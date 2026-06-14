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
	"net/http"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
)

// Spec is a declarative sweep-scale experiment.
type Spec struct {
	Name            string        `yaml:"name" json:"name"`
	Subscriptions   int           `yaml:"subscriptions" json:"subscriptions"` // K
	LinksPerSub     int           `yaml:"links_per_sub" json:"links_per_sub"` // P
	Dispatch        string        `yaml:"dispatch" json:"dispatch"`           // "pull-wake" (default) or "webhook"
	WebhookURL      string        `yaml:"webhook_url" json:"webhook_url"`     // required for dispatch=webhook
	SeedConcurrency int           `yaml:"seed_concurrency" json:"seed_concurrency"`
	Warmup          time.Duration `yaml:"warmup" json:"warmup"`   // settle time after seeding
	Measure         time.Duration `yaml:"measure" json:"measure"` // metric sampling window
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
		s.Warmup = 5 * time.Second
	}
	if s.Measure == 0 {
		s.Measure = 30 * time.Second
	}
	if s.Subscriptions <= 0 {
		return Spec{}, fmt.Errorf("subscriptions must be > 0")
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
	Spec          Spec    `json:"spec"`
	Seeded        int     `json:"seeded"`
	SeedErrors    int     `json:"seed_errors"`
	SeedSeconds   float64 `json:"seed_seconds"`
	WindowSeconds float64 `json:"window_seconds"`
	SweepTicks    float64 `json:"sweep_ticks"` // ticks observed during the window
	SweepMeanMs   float64 `json:"sweep_mean_ms"`
	SweepP50Ms    float64 `json:"sweep_p50_ms"`
	SweepP99Ms    float64 `json:"sweep_p99_ms"`
	MeanSubs      float64 `json:"mean_subs_evaluated"`
	MeanTails     float64 `json:"mean_tails_batched"`
}

// Run executes the experiment against a chronicle base URL, scraping metricsURL.
// The caller's context bounds the whole run (seed + warmup + measure).
func Run(ctx context.Context, baseURL, root, metricsURL string, spec Spec) (Report, error) {
	cli := dsclient.New(baseURL, root)
	hc := &http.Client{Timeout: 15 * time.Second}

	seedStart := time.Now()
	seeded, seedErrs := seedSubscriptions(ctx, cli, spec)
	seedDur := time.Since(seedStart)
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}

	if !sleepCtx(ctx, spec.Warmup) {
		return Report{}, ctx.Err()
	}

	names := []string{
		"chronicle_sweep_tick_seconds",
		"chronicle_sweep_subs_evaluated",
		"chronicle_sweep_tails_batched",
	}
	before, err := scrape(ctx, hc, metricsURL, names...)
	if err != nil {
		return Report{}, fmt.Errorf("scrape before: %w", err)
	}
	if !sleepCtx(ctx, spec.Measure) {
		return Report{}, ctx.Err()
	}
	after, err := scrape(ctx, hc, metricsURL, names...)
	if err != nil {
		return Report{}, fmt.Errorf("scrape after: %w", err)
	}

	tick := after[names[0]].sub(before[names[0]])
	subs := after[names[1]].sub(before[names[1]])
	tails := after[names[2]].sub(before[names[2]])

	return Report{
		Spec:          spec,
		Seeded:        seeded,
		SeedErrors:    seedErrs,
		SeedSeconds:   seedDur.Seconds(),
		WindowSeconds: spec.Measure.Seconds(),
		SweepTicks:    tick.count,
		SweepMeanMs:   tick.mean() * 1000,
		SweepP50Ms:    tick.quantile(0.5) * 1000,
		SweepP99Ms:    tick.quantile(0.99) * 1000,
		MeanSubs:      subs.mean(),
		MeanTails:     tails.mean(),
	}, nil
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
				resp, err := cli.CreateSubscription(ctx, fmt.Sprintf("loadtest-%d", i), subscriptionBody(spec, i))
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
		streams[j] = fmt.Sprintf("loadtest/s/%d/%d", i, j)
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
