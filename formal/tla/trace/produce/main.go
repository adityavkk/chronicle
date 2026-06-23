//go:build subtrace

// Command produce drives the chronicle subscription fence against a live Redis
// through the webhook.TracingStore seam (issue #39) to PRODUCE a real JSONL
// trace of fence linearization points, then leaves the file for the Go->TLA
// converter + TLC to validate against SubscriptionFence (#37).
//
// It is the imperative driver of the "smart casual verification" recipe
// (research/01): rather than mock the engine, it issues the SHIPPED Lua scripts
// (arm_wake/claim/ack/release/expire_lease/record_wake_sent) via the real
// RedisStore so every recorded line is a genuine Lua commit, and exercises the
// exact concurrent races SubscriptionFence reasons about:
//
//	clean       arm -> stamp (pull-wake emit split, T2) -> claim -> ack(done)
//	contended   one wake, two workers claim; the loser coalesces or is BUSY
//	takeover    worker A claims, lease expires, worker B takes over (rotate),
//	            A's late ack arrives FENCED (Kleppmann deposed-but-resumed)
//	heartbeat   claim -> ack(done=false) heartbeat -> ack(done)
//	release     claim -> release (voluntary)
//
// Build/run (needs Redis on $CHRONICLE_REDIS_ADDR, default localhost:6379):
//
//	go run -tags subtrace ./formal/tla/trace/produce -out trace.jsonl
//
// All keys are namespaced under a unique run id so the driver never collides
// with another worktree's data on a shared Redis, and every subscription it
// creates is deleted on exit.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
	"github.com/redis/go-redis/v9"
)

func main() {
	out := flag.String("out", "trace.jsonl", "JSONL trace output path")
	addr := flag.String("addr", env("CHRONICLE_REDIS_ADDR", "localhost:6379"), "redis address")
	flag.Parse()

	if err := run(*out, *addr); err != nil {
		fmt.Fprintln(os.Stderr, "produce:", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func run(outPath, addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	rec, closeRec, err := webhook.NewTraceRecorder(outPath)
	if err != nil {
		return fmt.Errorf("open trace: %w", err)
	}
	defer closeRec()

	inner := webhook.NewRedisStore(rdb)
	st := webhook.NewTracingStore(inner, rec)

	// Unique run prefix so keys never collide with a sibling worktree's data.
	runID := fmt.Sprintf("t39-%d", time.Now().UnixNano())
	const leaseTTLMs = int64(1000)
	created := []string{}
	mk := func(suffix string) (string, error) {
		id := runID + "-" + suffix
		_, err := st.CreateOrConfirm(id, webhook.Config{
			Type:       webhook.DispatchPullWake,
			WakeStream: "events/__wake__",
			LeaseTTLMs: leaseTTLMs,
		}, nil, time.Now())
		if err != nil {
			return "", err
		}
		created = append(created, id)
		return id, nil
	}
	defer func() {
		for _, id := range created {
			_ = inner.Delete(id)
		}
	}()

	now := time.Now()

	// --- Scenario 1: clean pull-wake lifecycle ----------------------------
	// arm (ARMED) -> stamp (OK, the emit half) -> claim (CLAIMED, coalesce on
	// the in-flight wake) -> ack(done) (OK, back to idle).
	s1, err := mk("clean")
	if err != nil {
		return err
	}
	arm, err := st.ArmWake(s1, now, leaseTTLMs, false, "w-"+s1+"-1")
	if err != nil {
		return err
	}
	if err := st.RecordWakeEventSent(s1, arm.Generation, arm.WakeID, now); err != nil {
		return err
	}
	c1, err := st.Claim(s1, "A", "w-"+s1+"-claimA", now, leaseTTLMs)
	if err != nil {
		return err
	}
	if _, err := st.Ack(s1, c1.Generation, c1.WakeID, c1.Generation, true, nil, now, leaseTTLMs); err != nil {
		return err
	}

	// --- Scenario 2: contended claim --------------------------------------
	// arm -> claim A (coalesce) -> claim B while A holds an unexpired lease
	// (BUSY) -> A acks done.
	s2, err := mk("contended")
	if err != nil {
		return err
	}
	a2, err := st.ArmWake(s2, now, leaseTTLMs, false, "w-"+s2+"-1")
	if err != nil {
		return err
	}
	_ = a2
	ca, err := st.Claim(s2, "A", "w-"+s2+"-A", now, leaseTTLMs)
	if err != nil {
		return err
	}
	if _, err := st.Claim(s2, "B", "w-"+s2+"-B", now, leaseTTLMs); err != nil { // expect BUSY
		return err
	}
	if _, err := st.Ack(s2, ca.Generation, ca.WakeID, ca.Generation, true, nil, now, leaseTTLMs); err != nil {
		return err
	}

	// --- Scenario 3: expired-lease takeover + deposed late ack ------------
	// arm -> claim A -> (let the lease expire) -> claim B takes over (ROTATE,
	// fresh gen+wake) -> A's late ack arrives FENCED (old gen) -> B acks done.
	// This is the central single-holder race the fence exists to make safe.
	s3, err := mk("takeover")
	if err != nil {
		return err
	}
	if _, err := st.ArmWake(s3, now, leaseTTLMs, false, "w-"+s3+"-1"); err != nil {
		return err
	}
	ta, err := st.Claim(s3, "A", "w-"+s3+"-A", now, leaseTTLMs)
	if err != nil {
		return err
	}
	// Advance the clock past the lease so B's claim takes over by rotating.
	later := now.Add(time.Duration(leaseTTLMs+10) * time.Millisecond)
	tb, err := st.Claim(s3, "B", "w-"+s3+"-B", later, leaseTTLMs)
	if err != nil {
		return err
	}
	// A's late ack: old generation/wake -> FENCED (no-op). Deposed-but-resumed.
	if _, err := st.Ack(s3, ta.Generation, ta.WakeID, ta.Generation, true, nil, later, leaseTTLMs); err != nil {
		return err
	}
	// B acks done with its rotated token -> OK.
	if _, err := st.Ack(s3, tb.Generation, tb.WakeID, tb.Generation, true, nil, later, leaseTTLMs); err != nil {
		return err
	}

	// --- Scenario 4: heartbeat then done ----------------------------------
	s4, err := mk("heartbeat")
	if err != nil {
		return err
	}
	if _, err := st.ArmWake(s4, now, leaseTTLMs, false, "w-"+s4+"-1"); err != nil {
		return err
	}
	c4, err := st.Claim(s4, "A", "w-"+s4+"-A", now, leaseTTLMs)
	if err != nil {
		return err
	}
	if _, err := st.Ack(s4, c4.Generation, c4.WakeID, c4.Generation, false, nil, now, leaseTTLMs); err != nil { // heartbeat
		return err
	}
	if _, err := st.Ack(s4, c4.Generation, c4.WakeID, c4.Generation, true, nil, now, leaseTTLMs); err != nil { // done
		return err
	}

	// --- Scenario 5: voluntary release ------------------------------------
	s5, err := mk("release")
	if err != nil {
		return err
	}
	if _, err := st.ArmWake(s5, now, leaseTTLMs, false, "w-"+s5+"-1"); err != nil {
		return err
	}
	c5, err := st.Claim(s5, "A", "w-"+s5+"-A", now, leaseTTLMs)
	if err != nil {
		return err
	}
	if _, err := st.Release(s5, c5.Generation, c5.WakeID, c5.Generation); err != nil {
		return err
	}

	// --- Scenario 6: server-side expire then re-arm -----------------------
	// arm with a lease armed (webhook-style arm_lease=true) -> let it expire ->
	// expire_lease (EXPIRED, idles, gen UNCHANGED — INV-FENCE-04) -> re-arm.
	s6, err := mk("expire")
	if err != nil {
		return err
	}
	if _, err := st.ArmWake(s6, now, leaseTTLMs, true, "w-"+s6+"-1"); err != nil {
		return err
	}
	exLater := now.Add(time.Duration(leaseTTLMs+10) * time.Millisecond)
	if _, err := st.ExpireLease(s6, exLater); err != nil {
		return err
	}
	if _, err := st.ArmWake(s6, exLater, leaseTTLMs, true, "w-"+s6+"-2"); err != nil {
		return err
	}

	fmt.Printf("produced trace at %s (run %s, %d subscriptions)\n", outPath, runID, len(created))
	return nil
}
