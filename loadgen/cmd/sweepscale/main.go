// Command sweepscale runs a sweep-scale experiment against a running chronicle:
// it seeds K subscriptions, lets the recovery sweep settle, and scrapes the
// chronicle_sweep_* metrics to report how one sweep tick scales with K.
//
//	sweepscale -scenario scenarios/sweep-scale.yaml \
//	  -base-url http://localhost:4437 -metrics-url http://localhost:9090/metrics
//
// Flags override scenario fields, so a single scenario can be swept across K:
//
//	for k in 1000 5000 10000; do sweepscale -scenario s.yaml -subscriptions $k; done
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/sweep"
)

func main() { os.Exit(run()) }

func run() int {
	scenario := flag.String("scenario", "", "path to a sweep-scale scenario YAML")
	baseURL := flag.String("base-url", "http://localhost:4437", "chronicle base URL")
	root := flag.String("root", "/v1/stream/", "stream root path")
	metricsURL := flag.String("metrics-url", "http://localhost:9090/metrics", "chronicle /metrics URL")
	out := flag.String("out", "", "write the JSON report to this path (default: stdout)")
	subs := flag.Int("subscriptions", 0, "override: K subscriptions")
	links := flag.Int("links-per-sub", 0, "override: P links per subscription")
	dispatch := flag.String("dispatch", "", "override: pull-wake or webhook")
	webhookURL := flag.String("webhook-url", "", "override: webhook receiver URL for dispatch=webhook")
	sharedStream := flag.String("shared-stream", "", "override: link every subscription to this stream")
	occupiedSlots := flag.Int("occupied-slots", 0, "override: exact occupied subscription slots for shared-stream fan-out")
	fanOutAppends := flag.Int("fanout-appends", 0, "override: append count against shared-stream during measure")
	fanOutInterval := flag.Duration("fanout-interval", 0, "override: interval between fan-out appends")
	warmup := flag.Duration("warmup", 0, "override: settle time after seeding")
	measure := flag.Duration("measure", 0, "override: metric sampling window")
	sloP99 := flag.Float64("slo-p99-ms", 0, "fail (exit 1) if sweep tick p99 exceeds this; 0 disables")
	maxSeedErrs := flag.Int("max-seed-errors", -1, "fail (exit 1) if seed errors exceed this; -1 disables")
	flag.Parse()

	spec := sweep.Spec{}
	if *scenario != "" {
		data, err := os.ReadFile(*scenario)
		if err != nil {
			return fail(err)
		}
		if spec, err = sweep.Decode(data); err != nil {
			return fail(err)
		}
	}
	if *subs > 0 {
		spec.Subscriptions = *subs
	}
	if *links > 0 {
		spec.LinksPerSub = *links
	}
	if *dispatch != "" {
		spec.Dispatch = *dispatch
	}
	if *webhookURL != "" {
		spec.WebhookURL = *webhookURL
	}
	if *sharedStream != "" {
		spec.SharedStream = *sharedStream
	}
	if *occupiedSlots > 0 {
		spec.OccupiedSlots = *occupiedSlots
	}
	if *fanOutAppends > 0 {
		spec.FanOutAppends = *fanOutAppends
	}
	if *fanOutInterval > 0 {
		spec.FanOutInterval = sweep.Dur(*fanOutInterval)
	}
	if *warmup > 0 {
		spec.Warmup = sweep.Dur(*warmup)
	}
	if *measure > 0 {
		spec.Measure = sweep.Dur(*measure)
	}
	spec, err := spec.Prepared()
	if err != nil {
		return fail(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "seeding K=%d subscriptions (P=%d, %s) against %s ...\n",
		spec.Subscriptions, spec.LinksPerSub, spec.Dispatch, *baseURL)
	rep, err := sweep.Run(ctx, *baseURL, *root, *metricsURL, spec)
	if err != nil {
		return fail(err)
	}

	b, _ := json.MarshalIndent(rep, "", "  ")
	if *out != "" {
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			return fail(err)
		}
	} else {
		fmt.Println(string(b))
	}
	fmt.Fprintf(os.Stderr,
		"\nK=%d P=%d %s | seeded %d/%d in %.1fs | sweep tick over %.0fs: mean=%.1fms p50=%.1fms p99=%.1fms | subs/tick=%.0f tails/tick=%.0f | %.0f ticks\n",
		spec.Subscriptions, spec.LinksPerSub, spec.Dispatch, rep.Seeded, spec.Subscriptions, rep.SeedSeconds,
		rep.WindowSeconds, rep.SweepMeanMs, rep.SweepP50Ms, rep.SweepP99Ms, rep.MeanSubs, rep.MeanTails, rep.SweepTicks)
	if spec.FanOutAppends > 0 {
		fmt.Fprintf(os.Stderr,
			"fanout stream %q occupied_slots=%d | appends %d/%d errors=%d | fanout count=%.0f mean=%.2fms p50=%.2fms p99=%.2fms | slots=%.1f subs=%.1f\n",
			spec.SharedStream, spec.OccupiedSlots, rep.FanOutAppended, spec.FanOutAppends, rep.FanOutAppendErrors,
			rep.FanOutCount, rep.FanOutMeanMs, rep.FanOutP50Ms, rep.FanOutP99Ms, rep.MeanFanOutSlots, rep.MeanFanOutSubscribers)
	}
	if rep.SeedErrors > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d seed errors\n", rep.SeedErrors)
	}
	if len(rep.ProposedMetricActivity) > 0 {
		fmt.Fprintf(os.Stderr, "proposed metric activity: %v\n", rep.ProposedMetricActivity)
	}

	// SLO gate: a non-zero exit makes this usable as an on-demand pass/fail check.
	failed := false
	if *sloP99 > 0 && rep.SweepP99Ms > *sloP99 {
		fmt.Fprintf(os.Stderr, "SLO FAIL: sweep tick p99 %.1fms > %.1fms\n", rep.SweepP99Ms, *sloP99)
		failed = true
	}
	if *maxSeedErrs >= 0 && rep.SeedErrors > *maxSeedErrs {
		fmt.Fprintf(os.Stderr, "SLO FAIL: seed errors %d > %d\n", rep.SeedErrors, *maxSeedErrs)
		failed = true
	}
	if rep.FanOutAppendErrors > 0 {
		fmt.Fprintf(os.Stderr, "SLO FAIL: fanout append errors %d\n", rep.FanOutAppendErrors)
		failed = true
	}
	if failed {
		return 1
	}
	return 0
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, "sweepscale:", err)
	return 1
}
