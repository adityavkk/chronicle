// Command rediscpu prints the managed-Redis (Memorystore) CPU utilization over a
// recent window, via Cloud Monitoring. It is the gate-#1 reader the rig orchestrator
// (ltctl gate1) calls at each replica level to plot Redis CPU vs N at fixed K —
// the O(N*K) sweep-redundancy premise (docs/specs/horizontal-scale/research/05
// experiment 1).
//
//	rediscpu -project $PROJECT -instance chronicle-loadtest-redis -window 2m
//
// Prints one line "redis_cpu max=<f> mean=<f> samples=<n>" to stdout, so ltctl can
// capture it into the gate-#1 table. Requires an authenticated gcloud.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/redismon"
)

func main() {
	project := flag.String("project", "", "GCP project id (required)")
	instance := flag.String("instance", "", "Memorystore instance id (required)")
	window := flag.Duration("window", 2*time.Minute, "look-back window for the CPU series")
	flag.Parse()

	if *project == "" || *instance == "" {
		fmt.Fprintln(os.Stderr, "rediscpu: -project and -instance are required")
		os.Exit(2)
	}

	r := redismon.Reader{Project: *project, Instance: *instance}
	s, err := r.ReadCPU(time.Now(), *window)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rediscpu:", err)
		os.Exit(1)
	}
	fmt.Printf("redis_cpu max=%.3f mean=%.3f samples=%d\n", s.Max(), s.Mean(), len(s.Points))
}
