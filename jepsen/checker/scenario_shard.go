package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// scenario_shard.go drives the chronicle per-(subId,g) claim-granularity
// capability (design 08): the shardedSubClaimer used by C3 (one subscription,
// many shard-leases via ClaimShard/AckShard) and the shard-linz scenario that
// proves T1 (single-holder) holds PER (subId, g).
//
// Both drive webhook.RedisStore directly against a local Redis — no cluster.

// pullWakeContentionCfg is the subscription config the contention/shard drivers
// create: a pull-wake sub (claim.lua grants on the lease alone, no streams
// needed) with the runtime's 30 s lease TTL by default.
func pullWakeContentionCfg(typ string, ttlMs int64) webhook.Config {
	return webhook.Config{
		Type:        webhook.DispatchPullWake,
		Pattern:     typ + "/*",
		WakeStream:  typ + "/__wake__",
		LeaseTTLMs:  ttlMs,
		Description: "hs-11 claim-granularity probe",
	}
}

// shardedSubClaimer maps every shard to ONE subscription's per-(subId,g) fence
// (RedisStore.ClaimShard/AckShard) — the chronicle capability #11 builds, as
// opposed to redisShardClaimer's G separate subscriptions. C3 runs the SAME
// driver over both so the differential is measured like-for-like; this one is the
// acceptance-gate topology (one config + one cursor set per type, sharded fence).
type shardedSubClaimer struct {
	store  *webhook.RedisStore
	client redis.UniversalClient
	id     string
	g      int
	ttl    int64
}

func newShardedSubClaimer(store *webhook.RedisStore, client redis.UniversalClient, id string, g int, ttlMs int64) *shardedSubClaimer {
	if g < 1 {
		g = 1
	}
	return &shardedSubClaimer{store: store, client: client, id: id, g: g, ttl: ttlMs}
}

func (c *shardedSubClaimer) label() string {
	return fmt.Sprintf("G=%d sharded-sub (%s, per-(subId,g) fence)", c.g, c.id)
}

func (c *shardedSubClaimer) shards() int { return c.g }

func (c *shardedSubClaimer) setup() error {
	_, err := c.store.CreateOrConfirm(c.id, pullWakeContentionCfg(c.id, c.ttl), nil, time.Now())
	return err
}

func (c *shardedSubClaimer) teardown() {
	_ = c.store.Delete(c.id)
	// Delete the g>0 shard fence hashes the capability lazily created (Delete only
	// removes the main hash + links). Shard 0 lives in the main hash, already gone.
	ctx := context.Background()
	for g := 1; g < c.g; g++ {
		c.client.Del(ctx, fmt.Sprintf("ds:{__ds}:sub:%s:g:%d", c.id, g))
	}
}

func (c *shardedSubClaimer) claim(g int, worker, wakeID string, now time.Time) (claimOutcome, error) {
	res, err := c.store.ClaimShard(c.id, g, worker, wakeID, now, c.ttl)
	if err != nil {
		return claimOutcome{}, err
	}
	return claimOutcome{granted: res.Claimed, busy: res.Busy, nosub: res.NoSub, gen: res.Generation, wakeID: res.WakeID}, nil
}

func (c *shardedSubClaimer) ack(g int, gen int64, wakeID string, done bool, now time.Time) (string, error) {
	return c.store.AckShard(c.id, g, gen, wakeID, gen, done, nil, now, c.ttl)
}

// ---- T1 per (subId, g) ----

// runShardLinz drives contending workers across G shards of ONE subscription via
// ClaimShard/AckShard, with a gcPause nemesis forcing intra-shard takeovers, and
// checks the recorded history for linearizability against the unchanged leaseModel
// — PARTITIONED PER (subId, g) by recording each op under sub "id#g<g>". A pass
// proves the single-holder fence still holds within every shard (07 T1 per
// (subId,g)); the granularity split did not weaken it.
func runShardLinz(c config) error {
	store, client, err := contentionStore(c)
	if err != nil {
		return err
	}
	defer client.Close()

	id := fmt.Sprintf("agent-handler-linz-%d", time.Now().UnixNano())
	G := c.gShards
	if G < 1 {
		G = 1
	}
	ttlMs := int64(c.leaseTTLMs)
	if ttlMs <= 0 {
		ttlMs = 1000 // short by default so a gcPause reliably outlives the lease
	}
	if _, err := store.CreateOrConfirm(id, pullWakeContentionCfg(id, ttlMs), nil, time.Now()); err != nil {
		return fmt.Errorf("create sub: %w", err)
	}
	defer func() {
		_ = store.Delete(id)
		for g := 1; g < G; g++ {
			client.Del(context.Background(), fmt.Sprintf("ds:{__ds}:sub:%s:g:%d", id, g))
		}
	}()

	fmt.Printf("== shard-linz: sub=%s G=%d workers=%d lease_ttl=%dms for %dms ==\n",
		id, G, c.workers, ttlMs, c.workloadMs)

	rec := newRecorder()
	deadline := time.Now().Add(time.Duration(c.workloadMs) * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < c.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			shardLeaseWorker(store, id, G, wid, ttlMs, deadline, rec)
		}(w)
	}
	wg.Wait()

	history := rec.history()
	parts := len(partitionBySub(history))
	fmt.Printf("operations: %d across %d (subId,g) partitions\n", len(history), parts)

	result, info := porcupine.CheckOperationsVerbose(leaseModel(), history, 20*time.Second)
	switch result {
	case porcupine.Ok:
		fmt.Println("PASS: T1 single-holder linearizable PER (subId,g) — the per-shard fence held under concurrency + GC pauses")
		return nil
	case porcupine.Illegal:
		const path = "shard-linz-counterexample.html"
		if verr := porcupine.VisualizePath(leaseModel(), info, path); verr == nil {
			fmt.Printf("counterexample: %s\n", path)
		}
		return fmt.Errorf("NOT linearizable: two holders of one (subId,g) shard held a fence-valid token — T1 violated per shard")
	default:
		return fmt.Errorf("linearizability UNKNOWN: history too concurrent (reduce -workers/-workload-ms; 07 gap #1)")
	}
}

// shardLeaseWorker claims a shard (chosen by hashing a fresh entity), optionally
// GC-pauses past the lease so a peer on the same shard takes over, then acks —
// recording every claim/ack into the per-(subId,g) history.
func shardLeaseWorker(store *webhook.RedisStore, id string, G, workerID int, ttlMs int64, deadline time.Time, rec *recorder) {
	worker := fmt.Sprintf("w-%d", workerID)
	backoff := time.Duration(20+workerID*11) * time.Millisecond
	grants := 0
	for cycle := 0; time.Now().Before(deadline); cycle++ {
		entity := fmt.Sprintf("e-%d-%d", workerID, cycle)
		g := webhook.ShardIndex(entity, G)
		sub := fmt.Sprintf("%s#g%d", id, g)
		wake := fmt.Sprintf("%s-wk-%d", worker, cycle)

		callNs := rec.now()
		res, err := store.ClaimShard(id, g, worker, wake, time.Now(), ttlMs)
		if err != nil {
			sleep(backoff)
			continue
		}
		out := fenceOutput{status: statusBusy}
		switch {
		case res.Claimed:
			out = fenceOutput{status: statusClaimed, gen: res.Generation, wake: res.WakeID}
		case res.NoSub:
			out.status = statusNoSub
		}
		rec.record(workerID, fenceInput{sub: sub, op: opClaim, worker: worker}, out, callNs)
		if !res.Claimed {
			sleep(backoff)
			continue
		}
		grants++
		// gcPause on ~every third grant: stall past the lease so a same-shard peer
		// takes over (rotating that shard's generation) and this ack races in stale.
		if grants%3 == 0 {
			sleep(gcPause(time.Duration(ttlMs) * time.Millisecond))
		}
		callNs = rec.now()
		st, aerr := store.AckShard(id, g, res.Generation, res.WakeID, res.Generation, true, nil, time.Now(), ttlMs)
		if aerr != nil {
			continue
		}
		astatus := statusOK
		switch st {
		case "OK":
			astatus = statusOK
		case "FENCED":
			astatus = statusFenced
		default:
			astatus = st
		}
		rec.record(workerID, fenceInput{
			sub: sub, op: opAck, worker: worker,
			reqGen: res.Generation, reqWake: res.WakeID, tokenGen: res.Generation, done: true,
		}, fenceOutput{status: astatus}, callNs)
	}
}

// contentionStore builds a RedisStore + client from -redis-url / $REDIS_URL, the
// shared setup for the contention and shard-linz scenarios.
func contentionStore(c config) (*webhook.RedisStore, redis.UniversalClient, error) {
	url := c.redisURL
	if url == "" {
		url = os.Getenv("REDIS_URL")
	}
	if url == "" {
		return nil, nil, fmt.Errorf("need -redis-url or $REDIS_URL (e.g. redis://localhost:6379/14)")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, nil, fmt.Errorf("parse redis url %q: %w", url, err)
	}
	client := redis.NewClient(opt)
	return webhook.NewRedisStore(client).WithMetrics(webhook.NopMetrics{}), client, nil
}
