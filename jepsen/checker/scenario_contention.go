package main

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type contentionAccumulator struct {
	mu        sync.Mutex
	ops       int
	busy      int
	fenced    int
	ok        int
	errors    int
	latencyMs []float64
}

func (a *contentionAccumulator) recordClaim(status int, code string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ops++
	if err != nil {
		a.errors++
		return
	}
	if status == http.StatusConflict && code == "ALREADY_CLAIMED" {
		a.busy++
		return
	}
	if status == http.StatusConflict && code == statusFenced {
		a.fenced++
	}
}

func (a *contentionAccumulator) recordAck(status int, code string, dur time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.errors++
		return
	}
	if status == http.StatusOK {
		a.ok++
		a.latencyMs = append(a.latencyMs, float64(dur.Microseconds())/1000)
		return
	}
	if status == http.StatusConflict && code == statusFenced {
		a.fenced++
	}
}

func runLiveContentionContract(c config) error {
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}
	claimants, err := parseClaimantList(c.contentionClaimants)
	if err != nil {
		return err
	}
	if c.claimShards < 1 || c.claimShards > 16 {
		return fmt.Errorf("-claim-shards must be in [1,16], got %d", c.claimShards)
	}
	fmt.Printf("== contention-contract: claimants=%v workload=%dms hold=%s lease_ttl_ms=%d ==\n",
		claimants, c.workloadMs, c.contentionHold, c.contentionLeaseTTLMs)

	baseline, err := runContentionSeries(c, claimants, 1)
	if err != nil {
		return err
	}
	printContentionSeries("G=1 baseline", baseline)
	if c.claimShards == 1 {
		fmt.Println("PASS: G=1 baseline measured; run with -claim-shards >1 to evaluate C3")
		return nil
	}

	sharded, err := runContentionSeries(c, claimants, c.claimShards)
	if err != nil {
		return err
	}
	printContentionSeries(fmt.Sprintf("G=%d sharded", c.claimShards), sharded)

	baseKnee := contentionKnee(baseline)
	shardedKnee := contentionKnee(sharded)
	fmt.Println("---- contention verdict ----")
	if baseKnee == 0 {
		return fmt.Errorf("G=1 baseline did not show a contention knee in %v; increase -contention-hold or -contention-claimants", claimants)
	}
	fmt.Printf("C1/C2 baseline knee: G=1 first violated at %d claimants\n", baseKnee)
	if shardedKnee == 0 {
		fmt.Printf("C2 sharded knee:     no knee observed through %d claimants\n", claimants[len(claimants)-1])
	} else {
		fmt.Printf("C2 sharded knee:     first violated at %d claimants\n", shardedKnee)
	}
	fixed := CheckContentionC3FixedN(baseline[len(baseline)-1], sharded[len(sharded)-1], c.claimShards, 1.50)
	if len(fixed) > 0 {
		for _, v := range fixed {
			fmt.Printf("  VIOLATION %s\n", v)
		}
		return fmt.Errorf("C3 fixed-N contention reduction failed")
	}
	fmt.Printf("PASS: C3 fixed-N BUSY/op dropped enough at %d claimants for G=%d\n",
		claimants[len(claimants)-1], c.claimShards)
	return nil
}

func runContentionSeries(c config, claimants []int, shards int) ([]contentionPoint, error) {
	points := make([]contentionPoint, 0, len(claimants))
	for _, n := range claimants {
		p, err := runContentionPoint(c, n, shards)
		if err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, nil
}

func runContentionPoint(c config, claimants, shards int) (contentionPoint, error) {
	stamp := time.Now().UnixNano()
	subID := fmt.Sprintf("jepsen-contention-%d-g%d-n%d", stamp, shards, claimants)
	pattern := fmt.Sprintf("events/contention/%d/*", stamp)
	wakeStream := fmt.Sprintf("events/contention/%d/wake", stamp)
	if err := createPullWakeSubscription(c.base, subID, pattern, wakeStream, c.contentionLeaseTTLMs); err != nil {
		return contentionPoint{}, err
	}
	defer deleteSubscription(c.base, subID)

	deadline := time.Now().Add(time.Duration(c.workloadMs) * time.Millisecond)
	start := time.Now()
	var acc contentionAccumulator
	var wg sync.WaitGroup
	for i := 0; i < claimants; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardValue := workerID % shards
			shard := &shardValue
			worker := fmt.Sprintf("worker-%02d-g%02d", workerID, shardValue)
			for time.Now().Before(deadline) {
				opStart := time.Now()
				status, code, res, err := claimOnceHTTP(c.base, subID, worker, shard)
				acc.recordClaim(status, code, err)
				if err != nil || status != http.StatusOK {
					sleep(contentionBackoff(c))
					continue
				}
				sleep(c.contentionHold)
				ackStatus, ackCode, ackErr := ackPullWakeShard(c.base, subID, res.Token, res.WakeID, res.Generation, shard)
				acc.recordAck(ackStatus, ackCode, time.Since(opStart), ackErr)
				if ackErr != nil || ackStatus != http.StatusOK {
					sleep(contentionBackoff(c))
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	acc.mu.Lock()
	defer acc.mu.Unlock()
	return contentionPoint{
		Claimants:           claimants,
		Ops:                 acc.ops,
		Busy:                acc.busy,
		Fenced:              acc.fenced,
		LeaseLapses:         0,
		ThroughputPerWorker: float64(acc.ok) / elapsed / float64(claimants),
		WakeP99Ms:           percentile(acc.latencyMs, 0.99),
		CPUPercent:          0,
	}, nil
}

func contentionBackoff(c config) time.Duration {
	if c.contentionHold > 0 {
		return c.contentionHold
	}
	return 20 * time.Millisecond
}

func parseClaimantList(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]bool{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid claimant count %q", p)
		}
		if !seen[n] {
			out = append(out, n)
			seen[n] = true
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no claimant counts configured")
	}
	sort.Ints(out)
	return out, nil
}

func printContentionSeries(label string, points []contentionPoint) {
	fmt.Println("---- " + label + " ----")
	fmt.Println("claimants ops busy/op fenced/op throughput_per_worker wake_p99_ms")
	for _, p := range points {
		busyRate := 0.0
		fencedRate := 0.0
		if p.Ops > 0 {
			busyRate = float64(p.Busy) / float64(p.Ops)
			fencedRate = float64(p.Fenced) / float64(p.Ops)
		}
		fmt.Printf("%9d %5d %.4f %.4f %.3f %.1f\n",
			p.Claimants, p.Ops, busyRate, fencedRate, p.ThroughputPerWorker, p.WakeP99Ms)
	}
}

func contentionKnee(points []contentionPoint) int {
	th := contentionThresholds{
		MaxBusyPerOp:          0.85,
		MaxFencedPerOp:        0.001,
		MaxWakeP99Ms:          5000,
		MaxThroughputDropFrac: 0.35,
	}
	violations := CheckContentionC1C2(points, th)
	for _, v := range violations {
		if v.Property == "C2" {
			return v.At
		}
	}
	return 0
}

func percentile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	idx := int(q*float64(len(cp)-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}
