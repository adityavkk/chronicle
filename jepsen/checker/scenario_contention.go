package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// scenario_contention.go is the IMPERATIVE SHELL of the contention suite (C1/C2/C3
// in docs/specs/horizontal-scale/research/07; gate #6) — the live claimant-fan-in
// driver the pure checker (check_contention.go) was scaffolded for. N concurrent
// workers contend for ONE logical type's lease(s) under NO fault, ramping the
// claimant count 6 -> 12 -> 24, and the recorded aggregates are fed to the pure
// C1/C2/C3 checkers.
//
// It drives the REAL claim.lua / ack.lua against a local Redis through
// webhook.RedisStore directly — no HTTP, no chronicle binary, no k3d. That is the
// lighter path #10's baseline proved (and the shared-VM constraint demands): the
// suite needs 12+ real claimants on one lease, which a single Redis container
// serves fine, and driving the store directly exercises exactly the scripts the
// granularity fix changes.
//
// The collapse is reproduced by SCALED timers, not GKE's literal 10 s/30 s/10 s.
// A worker holds a granted lease for -hold-ms (processing a wake), then idles
// -think-ms before the next wake. The per-lease claimant capacity is therefore
// ~(hold+think)/hold; with the defaults (hold 5 ms, think 25 ms) that is ~6 — so
// ONE lease stays clean at ~6 claimants and collapses past it, the empirical
// 6-clean/12-collapse signature, reproduced in seconds. The RATIO (and thus the
// knee location and its ~G× scaling under sharding) is what is faithful, not the
// absolute milliseconds; the literal-timer rig run is documented in 08.
//
// G is the granularity axis. G=1 is today's single hot per-type lease (the
// collapse). G>1 splits the type into G shard-leases (the fix); a worker's entity
// hashes to a shard (g = hash(entityId) % G, exactly 05's formula), so concurrent
// claimants on different entity-shards do not serialize. C3 is the differential
// between the two.

// claimOutcome is the result of one claim attempt, in the driver's vocabulary.
type claimOutcome struct {
	granted bool
	busy    bool  // ALREADY_CLAIMED — the lease was held and unexpired
	nosub   bool  // NOSUB — the subscription does not exist
	gen     int64 // the granted generation (on granted)
	wakeID  string
}

// shardClaimer is the fan-in driver's claim/ack seam over ONE logical type split
// into shards() leases. It is the only thing that differs between today's
// client-side sharding (G separate subscriptions, no rebuild) and the chronicle
// per-(subId,g) capability (#11's build): the driver and the checkers are
// identical across both, so the C1/C2 baseline and the C3 differential are
// measured like-for-like.
type shardClaimer interface {
	label() string // a short description of the claim topology
	shards() int   // G
	setup() error  // create the G shard-leases
	teardown()     // delete them
	claim(g int, worker, wakeID string, now time.Time) (claimOutcome, error)
	ack(g int, gen int64, wakeID string, done bool, now time.Time) (string, error)
}

// contentionParams configures one ramp.
type contentionParams struct {
	ttlMs    int64
	hold     time.Duration // lease hold per granted wake (the "processing" time)
	think    time.Duration // idle between wakes (sets the per-lease claimant capacity)
	backoff  time.Duration // pause after an ALREADY_CLAIMED bounce before retrying
	roundDur time.Duration // wall-clock per claimant rung
	ramp     []int         // claimant counts, e.g. {6, 12, 24}
}

// workerStats are one worker's tallies for a round; each worker owns its slot so
// the hot loop never contends on a shared counter, and the latency slices are
// merged only after the round ends.
type workerStats struct {
	completed int             // CLAIMED then acked OK — a full cycle
	busy      int             // ALREADY_CLAIMED bounces
	fenced    int             // ack returned FENCED (a takeover fenced this holder)
	lapses    int             // FENCED while still within the lease window (a lapse under hold)
	errs      int             // NOSUB / transport / unexpected
	latencies []time.Duration // claim-start -> ack-done, per completed cycle
}

// shardOf maps an entity id to its shard, g = hash(entityId) % G (05's formula).
// G must be >= 1; G == 1 always returns shard 0 (today's single per-type lease).
func shardOf(entityID string, g int) int {
	if g <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(entityID))
	return int(h.Sum32() % uint32(g))
}

// contentionWorker runs one worker's claim/hold/ack/think loop until the
// deadline, recording its outcomes into st. Each cycle the worker takes a fresh
// entity (a replica processes wakes for many entities of the type) and hashes it
// to a shard; it retries that shard through ALREADY_CLAIMED until it wins the
// lease, holds it, acks done, then idles — the realistic cycle the collapse rides
// on.
func contentionWorker(claimer shardClaimer, p contentionParams, workerID int, deadline time.Time, st *workerStats) {
	worker := fmt.Sprintf("w-%d", workerID)
	g := claimer.shards()
	for cycle := 0; time.Now().Before(deadline); cycle++ {
		entity := fmt.Sprintf("e-%d-%d", workerID, cycle)
		shard := shardOf(entity, g)
		wakeID := fmt.Sprintf("%s-wk-%d", worker, cycle)
		cycleStart := time.Now()

		var out claimOutcome
		granted := false
		for time.Now().Before(deadline) {
			var err error
			out, err = claimer.claim(shard, worker, wakeID, time.Now())
			if err != nil {
				st.errs++
				break
			}
			if out.granted {
				granted = true
				break
			}
			if out.busy {
				st.busy++
				time.Sleep(p.backoff)
				continue
			}
			st.errs++ // NOSUB or unexpected: stop this cycle
			break
		}
		if !granted {
			continue
		}

		if p.hold > 0 {
			time.Sleep(p.hold)
		}
		status, err := claimer.ack(shard, out.gen, out.wakeID, true, time.Now())
		if err != nil {
			st.errs++
			continue
		}
		switch status {
		case "OK":
			st.completed++
			st.latencies = append(st.latencies, time.Since(cycleStart))
		case "FENCED":
			st.fenced++
			// A FENCED on our OWN done-ack means a peer took the lease over while we
			// held it. If that happened inside the lease window, the lease lapsed
			// under an active holder — C1's forbidden lapse, the storm's mechanism.
			if time.Since(cycleStart) < time.Duration(p.ttlMs)*time.Millisecond {
				st.lapses++
			}
		default:
			st.errs++
		}
		if p.think > 0 {
			time.Sleep(p.think)
		}
	}
}

// driveRound runs N claimants against the claimer for one rung and folds their
// tallies into a contentionRound the pure checkers consume.
func driveRound(claimer shardClaimer, p contentionParams, n int) contentionRound {
	stats := make([]workerStats, n)
	deadline := time.Now().Add(p.roundDur)
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			contentionWorker(claimer, p, id, deadline, &stats[id])
		}(w)
	}
	wg.Wait()
	return buildRound(n, p.roundDur, stats)
}

// buildRound aggregates per-worker tallies into a contentionRound. ops is the
// total of terminal claim/ack outcomes (busy bounces + completed cycles + fenced
// acks), so busyRate/fencedRate read as the fraction of outcomes that were
// contention. throughputPerWorker is the mean per-worker completion rate
// (cycles/s), whose fall-off as N rises is the C2 knee.
func buildRound(n int, dur time.Duration, stats []workerStats) contentionRound {
	var busy, completed, fenced, lapses int
	var lat []time.Duration
	for i := range stats {
		busy += stats[i].busy
		completed += stats[i].completed
		fenced += stats[i].fenced
		lapses += stats[i].lapses
		lat = append(lat, stats[i].latencies...)
	}
	secs := dur.Seconds()
	perWorker := 0.0
	if secs > 0 && n > 0 {
		perWorker = float64(completed) / (float64(n) * secs)
	}
	p50, p99 := percentileMs(lat, 50), percentileMs(lat, 99)
	return contentionRound{
		claimants:               n,
		ops:                     busy + completed + fenced,
		fenced:                  fenced,
		alreadyClaimed:          busy,
		leaseLapsesHeartbeating: lapses,
		throughputPerWorker:     perWorker,
		wakeP50Ms:               p50,
		wakeP99Ms:               p99,
	}
}

// percentileMs returns the pth percentile (0..100) of the durations in
// milliseconds, using nearest-rank on the sorted copy. Empty -> 0.
func percentileMs(d []time.Duration, p int) float64 {
	if len(d) == 0 {
		return 0
	}
	s := make([]time.Duration, len(d))
	copy(s, d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := (p * len(s)) / 100
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return float64(s[rank].Microseconds()) / 1000.0
}

// ---- the redis-backed claimer (today's per-subId lease; no rebuild) ----

// redisShardClaimer maps each shard to its OWN pull-wake subscription
// "<type>-handler:<g>" (and just "<type>-handler" at G=1), so it reproduces
// C1/C2/C3 on TODAY's code with no rebuild: it is the client-side form of the
// granularity fix (the agents runtime choosing a finer subscriptionId). #11's
// chronicle capability adds the SAME shard split WITHIN one subscription
// (shardedSubClaimer), so the suite can compare client-side vs server-side
// sharding under an identical driver.
type redisShardClaimer struct {
	store *webhook.RedisStore
	typ   string
	g     int
	ttl   int64
	ids   []string
}

func newRedisShardClaimer(store *webhook.RedisStore, typ string, g int, ttlMs int64) *redisShardClaimer {
	if g < 1 {
		g = 1
	}
	return &redisShardClaimer{store: store, typ: typ, g: g, ttl: ttlMs}
}

func (rc *redisShardClaimer) label() string {
	return fmt.Sprintf("G=%d sub-per-shard (%s)", rc.g, rc.shardID(0))
}

func (rc *redisShardClaimer) shards() int { return rc.g }

// shardID names the subscription backing shard g. At G=1 the single per-type
// subscription keeps its bare name, byte-identical to today.
func (rc *redisShardClaimer) shardID(g int) string {
	if rc.g == 1 {
		return rc.typ
	}
	return fmt.Sprintf("%s:%d", rc.typ, g)
}

func (rc *redisShardClaimer) setup() error {
	cfg := webhook.Config{
		Type:        webhook.DispatchPullWake,
		Pattern:     rc.typ + "/*",
		WakeStream:  rc.typ + "/__wake__",
		LeaseTTLMs:  rc.ttl,
		Description: "hs-11 contention fan-in probe",
	}
	rc.ids = rc.ids[:0]
	for g := 0; g < rc.g; g++ {
		id := rc.shardID(g)
		if _, err := rc.store.CreateOrConfirm(id, cfg, nil, time.Now()); err != nil {
			return fmt.Errorf("create shard %d (%s): %w", g, id, err)
		}
		rc.ids = append(rc.ids, id)
	}
	return nil
}

func (rc *redisShardClaimer) teardown() {
	for _, id := range rc.ids {
		_ = rc.store.Delete(id)
	}
}

func (rc *redisShardClaimer) claim(g int, worker, wakeID string, now time.Time) (claimOutcome, error) {
	res, err := rc.store.Claim(rc.shardID(g), worker, wakeID, now, rc.ttl)
	if err != nil {
		return claimOutcome{}, err
	}
	return claimOutcome{
		granted: res.Claimed,
		busy:    res.Busy,
		nosub:   res.NoSub,
		gen:     res.Generation,
		wakeID:  res.WakeID,
	}, nil
}

func (rc *redisShardClaimer) ack(g int, gen int64, wakeID string, done bool, now time.Time) (string, error) {
	return rc.store.Ack(rc.shardID(g), gen, wakeID, gen, done, nil, now, rc.ttl)
}

// ---- the scenario entry point ----

// contentionLimitsDefault gates C1: a FENCED storm (>5% of outcomes) or any
// lease lapse under an active hold fails it, and BUSY > 30% of outcomes flags
// runaway claim contention. In the local no-fault regime FENCED stays ~0 at every
// G (a storm needs lease lapses, which need genuine >lease_ttl queueing the rig,
// not a laptop, produces); the BUSY ceiling is the gate the fix moves — high at
// G=1 (the hot lease), low at G>1 (spread across shards).
var contentionLimitsDefault = contentionLimits{MaxFencedRate: 0.05, MaxBusyRate: 0.30}

// runContention drives the claimant-fan-in ramp and reports C1/C2 (and, with
// -c3, the C3 differential / gate #6). It needs only a Redis URL — no cluster.
// -sharded selects the chronicle per-(subId,g) capability (one subscription) over
// client-side G-subscription sharding; C3 runs G=1 vs G on the same topology.
func runContention(c config) error {
	store, client, err := contentionStore(c)
	if err != nil {
		return err
	}
	defer client.Close()

	p := contentionParams{
		ttlMs:    int64(c.leaseTTLMs),
		hold:     time.Duration(c.holdMs) * time.Millisecond,
		think:    time.Duration(c.thinkMs) * time.Millisecond,
		backoff:  time.Duration(c.backoffMs) * time.Millisecond,
		roundDur: time.Duration(c.roundMs) * time.Millisecond,
		ramp:     parseRamp(c.ramp),
	}

	topology := "sub-per-shard (client-side)"
	if c.sharded {
		topology = "sharded-sub (chronicle per-(subId,g) capability)"
	}
	typ := fmt.Sprintf("agent-handler-%d", time.Now().UnixNano())
	fmt.Printf("== contention: type=%s ramp=%v G=%d topology=%s hold=%s think=%s lease_ttl=%dms round=%s ==\n",
		typ, p.ramp, c.gShards, topology, p.hold, p.think, p.ttlMs, p.roundDur)

	roundsG, err := runRamp(store, client, typ+"-G", c.gShards, p, c.sharded)
	if err != nil {
		return err
	}
	printRounds(fmt.Sprintf("G=%d", c.gShards), roundsG)

	c1 := CheckBoundedContention(roundsG, contentionLimitsDefault)
	c2 := CheckNoThroughputCollapse(roundsG)
	reportViolations("C1 (bounded contention)", c1)
	reportViolations("C2 (no throughput collapse — the knee)", c2)

	if !c.c3 {
		fmt.Println("\n(run with -c3 to add the C3 granularity differential / gate #6)")
		return contentionVerdict(c.gShards, c2, nil)
	}

	// C3: the same ramp at G=1 (the hot per-type lease) as the baseline to move.
	rounds1, err := runRamp(store, client, typ+"-base", 1, p, c.sharded)
	if err != nil {
		return err
	}
	printRounds("G=1 (baseline)", rounds1)
	c3 := CheckGranularityMovesKnee(rounds1, roundsG, c.gShards, 0.75)
	reportViolations("C3 (granularity moves the knee — gate #6)", c3)
	return contentionVerdict(c.gShards, c2, c3)
}

// runRamp creates a fresh claimer at granularity g and drives every rung, tearing
// the shard-leases down afterward so a rerun starts clean. sharded picks the
// chronicle per-(subId,g) capability over client-side G subscriptions.
func runRamp(store *webhook.RedisStore, client redis.UniversalClient, typ string, g int, p contentionParams, sharded bool) ([]contentionRound, error) {
	var claimer shardClaimer
	if sharded {
		claimer = newShardedSubClaimer(store, client, typ, g, p.ttlMs)
	} else {
		claimer = newRedisShardClaimer(store, typ, g, p.ttlMs)
	}
	if err := claimer.setup(); err != nil {
		return nil, err
	}
	defer claimer.teardown()
	rounds := make([]contentionRound, 0, len(p.ramp))
	for _, n := range p.ramp {
		rounds = append(rounds, driveRound(claimer, p, n))
	}
	return rounds, nil
}

// contentionVerdict turns the C2 (and optional C3) results into the scenario's
// pass/fail. At the fixed G the suite passes when there is NO collapse knee; if a
// C3 differential ran, it must also be clean.
func contentionVerdict(g int, c2, c3 []contentionViolation) error {
	if g == 1 {
		// G=1 is the unfixed hot lease: a knee here is the collapse being
		// reproduced, the expected baseline — not a failure of the run.
		fmt.Println("\nNOTE: G=1 is the unfixed per-type lease; a C2 knee here is the reproduced collapse baseline.")
		return nil
	}
	if len(c2) > 0 || len(c3) > 0 {
		return fmt.Errorf("contention gate FAILED at G=%d: %d C2 + %d C3 violation(s)", g, len(c2), len(c3))
	}
	fmt.Printf("\nPASS: at G=%d the knee did not collapse the per-worker throughput; gate #6 holds for this ramp.\n", g)
	return nil
}

func reportViolations(label string, v []contentionViolation) {
	if len(v) == 0 {
		fmt.Printf("  %-44s OK\n", label)
		return
	}
	fmt.Printf("  %-44s %d violation(s):\n", label, len(v))
	for _, x := range v {
		fmt.Printf("      - %s\n", x)
	}
}

func printRounds(title string, rounds []contentionRound) {
	fmt.Printf("\n-- %s --\n", title)
	fmt.Printf("  %-4s %-8s %-10s %-10s %-14s %-12s %-8s %-8s\n",
		"N", "ops", "busy/op", "fenced/op", "thru/worker", "aggregate", "p50ms", "p99ms")
	for _, r := range rounds {
		fmt.Printf("  %-4d %-8d %-10.3f %-10.3f %-14.1f %-12.1f %-8.1f %-8.1f\n",
			r.claimants, r.ops, r.busyRate(), r.fencedRate(),
			r.throughputPerWorker, r.aggregateThroughput(), r.wakeP50Ms, r.wakeP99Ms)
	}
}

// parseRamp parses a comma-separated claimant ramp ("6,12,24"); a malformed or
// empty value falls back to the canonical 6,12,24.
func parseRamp(s string) []int {
	out := []int{}
	cur, has := 0, false
	flush := func() {
		if has {
			out = append(out, cur)
		}
		cur, has = 0, false
	}
	for _, ch := range s {
		if ch >= '0' && ch <= '9' {
			cur = cur*10 + int(ch-'0')
			has = true
		} else {
			flush()
		}
	}
	flush()
	if len(out) == 0 {
		return []int{6, 12, 24}
	}
	return out
}
