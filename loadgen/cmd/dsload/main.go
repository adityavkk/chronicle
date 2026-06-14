// dsload is a load generator for Durable Streams servers.
//
//	dsload run      -scenario scenarios/fanout.yaml -label chronicle-redis -out results/
//	dsload validate -scenario scenarios/fanout.yaml
//
// See README.md for the scenario schema and methodology.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/report"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "validate":
		err = cmdValidate(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("dsload: %v", err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  dsload run      -scenario <file> [-label <sut>] [-out <dir>] [-base-url <url>]
                  [-duration <d>] [-warmup <d>] [-sample-pid name=pid ...]
                  [-sample-redis name=host:port ...] [-keep-streams]
  dsload validate -scenario <file>`)
}

type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, ",") }
func (r *repeated) Set(v string) error { *r = append(*r, v); return nil }

func cmdValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	path := fs.String("scenario", "", "scenario YAML file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sc, err := loadScenario(*path)
	if err != nil {
		return err
	}
	fmt.Printf("ok: %s — %d stream(s), %d writer(s), %d tailer(s), duration %s + %s warmup\n",
		sc.Name, sc.Streams.Count, sc.TotalWriters(), sc.TotalTailers(), sc.Duration, sc.Warmup)
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("scenario", "", "scenario YAML file")
	label := fs.String("label", "sut", "label for the system under test (used in output paths)")
	out := fs.String("out", "results", "output directory")
	baseURL := fs.String("base-url", "", "override target.base_url")
	duration := fs.Duration("duration", 0, "override scenario duration")
	warmup := fs.Duration("warmup", -1, "override scenario warmup")
	keep := fs.Bool("keep-streams", false, "do not delete streams at teardown")
	var samplePids, sampleRedis repeated
	fs.Var(&samplePids, "sample-pid", "name=pid of a SUT process to sample (repeatable)")
	fs.Var(&sampleRedis, "sample-redis", "name=host:port of a Redis to sample via INFO (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	sc, err := loadScenario(*path)
	if err != nil {
		return err
	}
	if *baseURL != "" {
		sc.Target.BaseURL = strings.TrimSuffix(*baseURL, "/")
	}
	if *duration > 0 {
		sc.Duration.Duration = *duration
	}
	if *warmup >= 0 {
		sc.Warmup.Duration = *warmup
	}
	if err := sc.Validate(); err != nil {
		return err
	}

	pids, err := parsePids(samplePids)
	if err != nil {
		return err
	}
	redis := map[string]string{}
	for _, kv := range sampleRedis {
		name, addr, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("invalid -sample-redis %q: want name=host:port", kv)
		}
		redis[name] = addr
	}

	raiseFDLimit(sc)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("scenario %s against %s [%s]", sc.Name, sc.Target.BaseURL, *label)
	result, err := run.Run(ctx, run.Options{
		Scenario:    sc,
		Label:       *label,
		SamplePIDs:  pids,
		SampleRedis: redis,
		KeepStreams: *keep,
		Logf:        log.Printf,
	})
	if err != nil {
		return err
	}

	dir := filepath.Join(*out, *label, sc.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "results.json"), data, 0o644); err != nil {
		return err
	}
	md := report.Markdown(result)
	if err := os.WriteFile(filepath.Join(dir, "summary.md"), []byte(md), 0o644); err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(md)
	log.Printf("results written to %s", dir)
	return nil
}

func loadScenario(path string) (scenario.Scenario, error) {
	if path == "" {
		return scenario.Scenario{}, fmt.Errorf("-scenario is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return scenario.Scenario{}, err
	}
	return scenario.Parse(data)
}

func parsePids(kvs []string) (map[string]int, error) {
	out := map[string]int{}
	for _, kv := range kvs {
		name, pidStr, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid -sample-pid %q: want name=pid", kv)
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			return nil, fmt.Errorf("invalid -sample-pid %q: %w", kv, err)
		}
		out[name] = pid
	}
	return out, nil
}

// raiseFDLimit lifts RLIMIT_NOFILE toward the hard limit: every SSE
// tailer and in-flight request is a socket.
func raiseFDLimit(sc scenario.Scenario) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	need := uint64(sc.TotalTailers()+sc.Limits.MaxInFlightAppends+sc.Limits.MaxInFlightCatchup) + 256
	if lim.Cur >= need {
		return
	}
	lim.Cur = lim.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		log.Printf("WARNING: fd limit %d < %d needed and raise failed: %v", lim.Cur, need, err)
		return
	}
	if lim.Cur < need {
		log.Printf("WARNING: fd limit raised to %d but scenario may need %d", lim.Cur, need)
	}
}
