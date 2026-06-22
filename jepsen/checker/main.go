// Command jepsen-checker drives a Jepsen-style durability test of the chronicle
// __ds subscription layer against a live Kubernetes deployment.
//
// It embeds a webhook receiver (reachable from the cluster via
// host.k3d.internal), creates a webhook subscription, appends a known set of
// messages across many streams while a nemesis injects faults (killing the
// origin mid-flight, restarting Redis), then verifies the durability property:
// after the faults settle, every stream's subscription cursor has advanced to
// its tail — i.e. every durably-appended message was eventually delivered, even
// the ones whose append-time wake was lost to a crash and had to be re-fired by
// the recovery sweep. Delivery is at-least-once, so duplicate deliveries are
// reported but are not failures (the generation fence makes them safe).
//
// This is the empirical counterpart to docs/research/07's crash-window analysis.
//
// Beyond the original baseline/origin-restart/redis-restart scenarios, four
// scenarios exercise the crash windows closed by the subscription-hardening
// slices (docs/research/10): pull-wake-arm-crash (slice 1, durable pull-wake
// recovery), expired-lease-takeover (slice 2, fence rotation on lease takeover),
// glob-create-crash (slice 3, glob-link reconciliation), and index-repair
// (slice 4, fan-out index repair). Each asserts the property the matching slice
// promises — see the per-scenario comments and docs/jepsen/results.md.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"
)

type config struct {
	base       string
	recvHost   string
	recvPort   int
	cluster    string
	kctx       string // kubectl context override; defaults to "k3d-" + cluster
	namespace  string
	streams    int
	msgs       int
	scenario   string
	settle     time.Duration
	workloadMs int
	workers    int
}

func main() {
	var c config
	flag.StringVar(&c.base, "base", "http://localhost:4438", "chronicle base URL (via k3d loadbalancer)")
	flag.StringVar(&c.recvHost, "recv-host", "host.k3d.internal", "hostname the cluster uses to reach this receiver")
	flag.IntVar(&c.recvPort, "recv-port", 8099, "local webhook receiver port")
	flag.StringVar(&c.cluster, "cluster", "chronicle-jepsen", "k3d cluster name (for kubectl context)")
	flag.StringVar(&c.kctx, "context", "", "kubectl context override (default: k3d-<cluster>; use GKE context for cloud runs)")
	flag.StringVar(&c.namespace, "namespace", "chronicle-jepsen", "kubernetes namespace")
	flag.IntVar(&c.streams, "streams", 8, "number of event streams")
	flag.IntVar(&c.msgs, "msgs", 40, "messages appended per stream")
	flag.StringVar(&c.scenario, "scenario", "origin-restart", "baseline|origin-restart|redis-restart|pull-wake-arm-crash|expired-lease-takeover|glob-create-crash|index-repair|single-holder-linz|cursor-monotonic")
	flag.DurationVar(&c.settle, "settle", 25*time.Second, "post-fault settle time for the recovery sweep")
	flag.IntVar(&c.workers, "workers", 4, "contending workers for the single-holder-linz scenario")
	flag.IntVar(&c.workloadMs, "workload-ms", 8000, "workload duration in ms for the single-holder-linz scenario")
	flag.Parse()

	r := newReceiver()
	srv := r.serve(c.recvPort)
	defer srv.Close()

	if err := run(c, r); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func run(c config, r *receiver) error {
	ctx := c.kctx
	if ctx == "" {
		ctx = fmt.Sprintf("k3d-%s", c.cluster)
	}

	// single-holder-linz is a pure fence-contention linearizability test over the
	// claim/ack API: N workers + an in-process GC-pause nemesis, checked with
	// porcupine. It needs neither the webhook receiver nor the kubectl nemesis, so
	// it runs its own self-contained flow (scenario_lease.go).
	if c.scenario == "single-holder-linz" {
		return runSingleHolderLinz(c)
	}

	// cursor-monotonic drives the webhook delivery workload under origin churn
	// while sampling cursors, then checks forward-only advance with the pure
	// CheckCursorMonotonic (T2, scenario_cursor.go). It reuses the receiver.
	if c.scenario == "cursor-monotonic" {
		return runCursorMonotonic(c, r)
	}

	fmt.Printf("== scenario %q: %d streams x %d msgs ==\n", c.scenario, c.streams, c.msgs)
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}

	// expired-lease-takeover asserts a fence-rotation property over the pull-wake
	// claim/ack API rather than the webhook delivery property the other scenarios
	// share, so it runs its own end-to-end flow (no webhook receiver, no
	// cursor-reaches-tail verify).
	if c.scenario == "expired-lease-takeover" {
		nem := &nemesis{ctx: ctx, ns: c.namespace, scenario: c.scenario}
		return runExpiredLeaseTakeover(c, nem)
	}

	// pull-wake-arm-crash drives a pull-wake subscription drained by a worker
	// loop, not the webhook receiver, so it too runs its own flow.
	if c.scenario == "pull-wake-arm-crash" {
		nem := &nemesis{ctx: ctx, ns: c.namespace, scenario: c.scenario}
		return runPullWakeArmCrash(c, nem)
	}

	subID := fmt.Sprintf("jepsen-sub-%d", time.Now().UnixNano())
	webhookURL := fmt.Sprintf("http://%s:%d/webhook", c.recvHost, c.recvPort)

	// Most scenarios drive a glob (events/*) webhook subscription. glob-create-crash
	// is the exception: it creates the subscription FIRST and then creates matching
	// streams under a crash, so the reconcile loop (not create-time backfill) is
	// what has to link them.
	pattern := "events/*"

	// 1. Create the webhook subscription. The receiver returns {done:true}, so
	//    the server auto-acks each wake's snapshot.
	if err := createSubscription(c.base, subID, webhookURL, pattern); err != nil {
		return err
	}
	fmt.Printf("created subscription %s -> %s (pattern %q)\n", subID, webhookURL, pattern)

	// 2. Run the nemesis concurrently with the workload.
	nem := &nemesis{ctx: ctx, ns: c.namespace, scenario: c.scenario}
	var wg sync.WaitGroup
	stopNemesis := make(chan struct{})
	wg.Add(1)
	go func() { defer wg.Done(); nem.run(stopNemesis) }()

	// 3. Workload: append a known set of messages, recording each stream's tail.
	expected := map[string]string{} // stream -> final tail offset
	for i := 0; i < c.streams; i++ {
		stream := fmt.Sprintf("events/e-%d", i)

		// glob-create-crash: kill all origins the instant the stream's first
		// message creates it, approximating a crash before the best-effort
		// OnStreamCreated hook / create-time backfill links the new stream. The
		// canonical link is then never written by create-time paths, so the only
		// way the stream gets linked is the slow reconcile loop re-matching the
		// glob. We create the stream alone, crash, then append the rest after the
		// origin returns so the to-deliver work straddles the missed link.
		if c.scenario == "glob-create-crash" {
			tail, err := createStream(c.base, stream)
			if err != nil {
				close(stopNemesis)
				wg.Wait()
				return fmt.Errorf("workload create %s: %w", stream, err)
			}
			nem.killAllOrigins()
			if err := waitReady(c.base, 90*time.Second); err != nil {
				close(stopNemesis)
				wg.Wait()
				return fmt.Errorf("chronicle did not recover mid-workload: %w", err)
			}
			if t2, err := appendRest(c.base, stream, 1, c.msgs); err == nil && t2 != "" {
				tail = t2
			}
			expected[stream] = tail
			continue
		}

		tail, err := appendStream(c.base, stream, c.msgs)
		if err != nil {
			close(stopNemesis)
			wg.Wait()
			return fmt.Errorf("workload %s: %w", stream, err)
		}
		expected[stream] = tail

		// index-repair: after each stream is created and linked, delete its
		// fan-out index entry (ds:{__ds}:stream:<path>) out from under the
		// running webhook workload. The canonical link survives, so the property
		// is that ReconcileIndexes rebuilds the SADD from links and the later
		// appends still wake — the stream reaches tail rather than being stranded
		// at sweep latency forever.
		if c.scenario == "index-repair" {
			nem.deleteStreamIndex(stream)
		}
	}
	fmt.Printf("workload done: appended %d messages\n", c.streams*c.msgs)

	// 4. The decisive fault, per scenario.
	switch c.scenario {
	case "origin-restart":
		// Kill ALL origins after the last append, so the final wake can only come
		// from the recovery sweep on a restarted origin.
		nem.killAllOrigins()
		fmt.Println("nemesis: killed all origins after final append")
	case "index-repair":
		// Delete every stream's index entry once more after the final append, then
		// append one more message per stream so the wake for it depends on the
		// repaired index (OnStreamAppend reads ds:{__ds}:stream:<path>).
		for i := 0; i < c.streams; i++ {
			stream := fmt.Sprintf("events/e-%d", i)
			nem.deleteStreamIndex(stream)
		}
		for i := 0; i < c.streams; i++ {
			stream := fmt.Sprintf("events/e-%d", i)
			tail, err := appendOne(c.base, stream, c.msgs)
			if err == nil && tail != "" {
				expected[stream] = tail
			}
		}
		fmt.Println("nemesis: dropped fan-out index entries; appended past the gap")
	}
	close(stopNemesis)
	wg.Wait()

	// 5. Let the origin come back and the recovery sweep / reconcile loop run.
	if err := waitReady(c.base, 90*time.Second); err != nil {
		return fmt.Errorf("chronicle did not recover: %w", err)
	}
	fmt.Printf("settling %s for the recovery sweep / reconcile loop...\n", c.settle)
	sleep(c.settle)

	// 6. Verify durability: every stream's cursor advanced to its tail.
	return verify(c, subID, expected, r, nem)
}

// runExpiredLeaseTakeover exercises slice 2 (fence rotation on expired-lease
// takeover, commit 457bd69). Worker A claims a pull-wake subscription and stalls
// past lease_ttl_ms; worker B then claims (taking over the expired lease) and
// acks; worker A's later ack with its now-stale (generation, wake_id) token MUST
// return 409 FENCED because B's takeover minted a fresh generation. Before the
// slice, B reused A's generation and both tokens stayed valid (split-brain).
//
// This is a deterministic property over the claim/ack HTTP API: no pod kill is
// required to reproduce it, so the nemesis is idle here. We DELETE the
// subscription on exit to keep the keyspace clean between scenarios.
func runExpiredLeaseTakeover(c config, nem *nemesis) error {
	subID := fmt.Sprintf("jepsen-pull-%d", time.Now().UnixNano())
	const leaseTTLMs = 1500

	// A short-lived stream + a pull-wake subscription with a wake_stream, plus
	// one appended message so a claim has pending work to snapshot.
	stream := "events/lease-0"
	if _, err := appendStream(c.base, stream, 4); err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	if err := createPullWakeSubscription(c.base, subID, "events/*", "events/wake-0", leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created pull-wake subscription %s (lease_ttl_ms=%d)\n", subID, leaseTTLMs)

	// Worker A claims and then deliberately stalls past the lease.
	a, err := claim(c.base, subID, "worker-A")
	if err != nil {
		return fmt.Errorf("worker A claim: %w", err)
	}
	fmt.Printf("worker A claimed: generation=%d wake_id=%s\n", a.Generation, short(a.WakeID))
	stall := time.Duration(leaseTTLMs)*time.Millisecond + 1*time.Second
	fmt.Printf("worker A stalling %s past the lease...\n", stall)
	sleep(stall)

	// Worker B takes over the expired lease. With slice 2 this mints a FRESH
	// generation (and wake_id), rotating the fence.
	b, err := claim(c.base, subID, "worker-B")
	if err != nil {
		return fmt.Errorf("worker B claim (takeover): %w", err)
	}
	fmt.Printf("worker B claimed (takeover): generation=%d wake_id=%s\n", b.Generation, short(b.WakeID))
	if b.Generation == a.Generation {
		return fmt.Errorf("fence did NOT rotate on lease takeover: worker B reused generation %d (split-brain — slice 2 regression)", a.Generation)
	}

	// Worker B acks under the new fence; this must succeed.
	status, _, err := ackPullWake(c.base, subID, b.Token, b.WakeID, b.Generation)
	if err != nil {
		return fmt.Errorf("worker B ack: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("worker B ack returned %d, want 200 (the live holder must ack)", status)
	}
	fmt.Println("worker B ack: 200 OK")

	// The decisive assertion: worker A's late ack with its stale token is FENCED.
	status, code, err := ackPullWake(c.base, subID, a.Token, a.WakeID, a.Generation)
	if err != nil {
		return fmt.Errorf("worker A late ack: %w", err)
	}
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("A generation:      %d\n", a.Generation)
	fmt.Printf("B generation:      %d (rotated)\n", b.Generation)
	fmt.Printf("A late-ack status: %d %s\n", status, code)
	if status != http.StatusConflict || code != "FENCED" {
		return fmt.Errorf("worker A's late ack returned %d %q, want 409 FENCED — the deposed worker was NOT fenced (slice 2 regression)", status, code)
	}
	fmt.Println("PASS: the deposed worker's late ack was fenced (409 FENCED); the fence rotated on lease takeover")
	return nil
}

// runPullWakeArmCrash exercises slice 1 (durable pull-wake recovery, commit
// 19c3af8). A pull-wake arm writes the durable waking phase and the wake event
// in two non-atomic steps; a crash in between leaves the subscription armed
// (phase=waking, wake_event_sent_ns=0) with no event, permanently stranded
// before the slice. The sweep now re-emits any pull-wake stuck in waking with
// sent_ns==0.
//
// APPROXIMATION: precisely timing a kill "after arm, before wake-event emit"
// from outside the process is not feasible from this host driver — the window is
// a few microseconds inside issueWake. We approximate it by appending under
// aggressive origin churn (kill every ~2 s, plus a kill-all after the final
// append) so that some appends' wake-event emits are lost to a crash, and assert
// the strictly stronger end-to-end property the slice guarantees: a worker
// draining the wake stream eventually sees every stream reach its tail. If the
// arm/emit window were NOT recovered, at least one stream would stay stuck in
// waking with no event and its cursor would never advance — which this catches.
// A future, more surgical version could pause an origin via a fault-injection
// hook between arm_wake.lua and record_wake_sent.lua; that needs a server-side
// seam this harness does not have.
func runPullWakeArmCrash(c config, nem *nemesis) error {
	subID := fmt.Sprintf("jepsen-pull-%d", time.Now().UnixNano())
	const leaseTTLMs = 2000
	wakeStream := "events/__wake__"

	if err := createPullWakeSubscription(c.base, subID, "events/*", wakeStream, leaseTTLMs); err != nil {
		return err
	}
	defer deleteSubscription(c.base, subID)
	fmt.Printf("created pull-wake subscription %s -> wake_stream %s\n", subID, wakeStream)

	// A worker loop continuously claims, acks every pending offset, and releases —
	// draining each stream's cursor toward its tail. It runs until stopWorker.
	stopWorker := make(chan struct{})
	var wwg sync.WaitGroup
	wwg.Add(1)
	go func() { defer wwg.Done(); drainWorker(c.base, subID, wakeStream, stopWorker) }()

	// Nemesis: light churn during the workload to lose some arm-time emits.
	nem.scenario = "pull-wake-arm-crash"
	stopNemesis := make(chan struct{})
	var nwg sync.WaitGroup
	nwg.Add(1)
	go func() { defer nwg.Done(); nem.run(stopNemesis) }()

	expected := map[string]string{}
	for i := 0; i < c.streams; i++ {
		stream := fmt.Sprintf("events/e-%d", i)
		tail, err := appendStream(c.base, stream, c.msgs)
		if err != nil {
			close(stopNemesis)
			nwg.Wait()
			close(stopWorker)
			wwg.Wait()
			return fmt.Errorf("workload %s: %w", stream, err)
		}
		expected[stream] = tail
	}
	fmt.Printf("workload done: appended %d messages\n", c.streams*c.msgs)

	// Decisive: kill all origins after the final append so any arm-without-emit
	// can only be recovered by the sweep on a restarted origin.
	nem.killAllOrigins()
	fmt.Println("nemesis: killed all origins after final append")
	close(stopNemesis)
	nwg.Wait()

	if err := waitReady(c.base, 90*time.Second); err != nil {
		close(stopWorker)
		wwg.Wait()
		return fmt.Errorf("chronicle did not recover: %w", err)
	}
	fmt.Printf("settling %s for the recovery sweep (re-emits stranded wakes)...\n", c.settle)
	sleep(c.settle)
	close(stopWorker)
	wwg.Wait()

	// Verify: every stream's cursor advanced to its tail despite lost arm-time
	// emits. The worker has drained whatever the sweep re-emitted.
	view, err := getSubscription(c.base, subID)
	if err != nil {
		return err
	}
	acked := map[string]string{}
	for _, s := range view.Streams {
		acked[s.Path] = s.AckedOffset
	}
	var lagging []string
	streams := make([]string, 0, len(expected))
	for s := range expected {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	for _, s := range streams {
		if acked[s] != expected[s] {
			lagging = append(lagging, fmt.Sprintf("%s acked=%s want=%s", s, short(acked[s]), short(expected[s])))
		}
	}
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("messages appended: %d\n", c.streams*c.msgs)
	fmt.Printf("streams at tail:   %d/%d\n", c.streams-len(lagging), c.streams)
	if len(lagging) > 0 {
		for _, l := range lagging {
			fmt.Printf("  LAGGING %s\n", l)
		}
		return fmt.Errorf("%d/%d streams never reached their tail — a pull-wake arm was stranded and the sweep did not re-emit it", len(lagging), c.streams)
	}
	fmt.Println("PASS: every pull-wake reached its tail; arm-without-emit windows were recovered by the sweep")
	return nil
}

func verify(c config, subID string, expected map[string]string, r *receiver, nem *nemesis) error {
	view, err := getSubscription(c.base, subID)
	if err != nil {
		return err
	}
	acked := map[string]string{}
	for _, s := range view.Streams {
		acked[s.Path] = s.AckedOffset
	}

	var lagging []string
	streams := make([]string, 0, len(expected))
	for s := range expected {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	for _, s := range streams {
		if acked[s] != expected[s] {
			lagging = append(lagging, fmt.Sprintf("%s acked=%s want=%s", s, short(acked[s]), short(expected[s])))
		}
	}

	delivered, dupFactor := r.stats()
	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Printf("messages appended: %d\n", c.streams*c.msgs)
	fmt.Printf("wakes delivered:   %d\n", delivered)
	fmt.Printf("duplicate factor:  %.2f (at-least-once; duplicates fenced-safe)\n", dupFactor)
	fmt.Printf("streams at tail:   %d/%d\n", c.streams-len(lagging), c.streams)
	if len(lagging) > 0 {
		for _, l := range lagging {
			fmt.Printf("  LAGGING %s\n", l)
		}
		return fmt.Errorf("%d/%d streams never reached their tail — a wake was lost and not recovered", len(lagging), c.streams)
	}
	fmt.Println("PASS: every durably-appended message was delivered and acked despite faults")
	return nil
}

// ---- chronicle HTTP client ----

func createSubscription(base, id, webhookURL, pattern string) error {
	body, _ := json.Marshal(map[string]any{
		"type":         "webhook",
		"pattern":      pattern,
		"webhook":      map[string]string{"url": webhookURL},
		"lease_ttl_ms": 2000,
		"description":  "jepsen durability probe",
	})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, id)
	return retry(20, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			return fmt.Errorf("create subscription status %d", resp.StatusCode)
		}
		return nil
	})
}

// createPullWakeSubscription creates a pull-wake subscription with a wake_stream.
// Wakes are written as events to wake_stream for workers to claim (PROTOCOL §7.2),
// rather than pushed to a webhook.
func createPullWakeSubscription(base, id, pattern, wakeStream string, leaseTTLMs int64) error {
	body, _ := json.Marshal(map[string]any{
		"type":         "pull-wake",
		"pattern":      pattern,
		"wake_stream":  wakeStream,
		"lease_ttl_ms": leaseTTLMs,
		"description":  "jepsen pull-wake probe",
	})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, id)
	return retry(20, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("create pull-wake subscription status %d: %s", resp.StatusCode, b)
		}
		return nil
	})
}

func deleteSubscription(base, id string) {
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, id)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// claimResult is the decoded subset of a successful claim we need to drive an ack.
type claimResult struct {
	WakeID     string            `json:"wake_id"`
	Generation int64             `json:"generation"`
	Token      string            `json:"token"`
	Streams    []claimStreamSnap `json:"streams"`
}

type claimStreamSnap struct {
	Path       string `json:"path"`
	TailOffset string `json:"tail_offset"`
	HasPending bool   `json:"has_pending"`
}

// claim POSTs a pull-wake claim for worker. On a successful 200 it returns the
// minted (wake_id, generation, token) and the snapshot. A 409 ALREADY_CLAIMED
// (the lease is held and unexpired) is surfaced as an error so callers can retry.
func claim(base, id, worker string) (claimResult, error) {
	var out claimResult
	body, _ := json.Marshal(ClaimBody{Worker: worker})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s/claim", base, id)
	err := retry(40, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return json.NewDecoder(resp.Body).Decode(&out)
		}
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("claim status %d: %s", resp.StatusCode, b)
	})
	return out, err
}

// ackPullWake POSTs a pull-wake ack with done=true under the holder's token and
// fence. streams is the claim snapshot; each pending stream is acked to its
// current tail so the subscription cursor advances. Callers that only need fence
// verification (expired-lease-takeover) pass no streams.
// It returns the HTTP status and (on a 4xx error envelope) the error code,
// without retrying — the caller asserts on the exact status. status 200 means the
// ack landed; 409 with code "FENCED" means the fence rotated under this token.
func ackPullWake(base, id, token, wakeID string, generation int64, streams ...claimStreamSnap) (status int, code string, err error) {
	done := true
	var acks []ackEntry
	for _, s := range streams {
		if s.HasPending && s.TailOffset != "" {
			acks = append(acks, ackEntry{Stream: s.Path, Offset: s.TailOffset})
		}
	}
	body, _ := json.Marshal(CallbackBody{WakeID: wakeID, Generation: generation, Acks: acks, Done: &done})
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s/ack", base, id)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	if status >= 400 {
		var env errEnvelope
		if json.NewDecoder(resp.Body).Decode(&env) == nil {
			code = env.Error.Code
		}
	}
	return status, code, nil
}

// drainWorker continuously claims, acks every pending stream up to its tail with
// done=true, and lets the next claim proceed — draining all pull-wakes toward
// their tails. It runs until stop is closed. Errors (no pending work, transient
// origin churn) are expected and simply retried after a short pause.
func drainWorker(base, id, wakeStream string, stop <-chan struct{}) {
	worker := fmt.Sprintf("drain-%d", time.Now().UnixNano())
	for {
		select {
		case <-stop:
			return
		default:
		}
		res, err := claim(base, id, worker)
		if err != nil || res.Token == "" {
			// No pending work or origin churn: back off briefly and retry.
			sleep(200 * time.Millisecond)
			continue
		}
		ackPullWake(base, id, res.Token, res.WakeID, res.Generation, res.Streams...)
	}
}

// ClaimBody and CallbackBody mirror the request wire shapes (webhook/wire.go
// ClaimRequest / CallbackRequest) so the harness stays self-contained.
type ClaimBody struct {
	Worker string `json:"worker"`
}

type ackEntry struct {
	Stream string `json:"stream"`
	Offset string `json:"offset"`
}

type CallbackBody struct {
	WakeID     string     `json:"wake_id"`
	Generation int64      `json:"generation"`
	Acks       []ackEntry `json:"acks,omitempty"`
	Done       *bool      `json:"done"`
}

type errEnvelope struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// appendStream creates a stream with its first message then appends the rest,
// returning the final tail offset. Each request retries through origin churn.
func appendStream(base, stream string, msgs int) (string, error) {
	if _, err := createStream(base, stream); err != nil {
		return "", err
	}
	return appendRest(base, stream, 1, msgs)
}

// createStream PUTs the first message ({"n":0}), creating the stream, and returns
// its tail. Retries through origin churn.
func createStream(base, stream string) (string, error) {
	var tail string
	createURL := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	err := retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPut, createURL, bytes.NewReader([]byte(`{"n":0}`)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("create %s status %d: %s", stream, resp.StatusCode, b)
		}
		tail = resp.Header.Get("Stream-Next-Offset")
		return nil
	})
	return tail, err
}

// appendRest POSTs messages [from, msgs) to an existing stream, returning the
// final tail. Retries through origin churn.
func appendRest(base, stream string, from, msgs int) (string, error) {
	var tail string
	appendURL := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	for n := from; n < msgs; n++ {
		payload := []byte(fmt.Sprintf(`{"n":%d}`, n))
		err := retry(160, 500*time.Millisecond, func() error {
			req, _ := http.NewRequest(http.MethodPost, appendURL, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				return fmt.Errorf("append %s status %d", stream, resp.StatusCode)
			}
			if v := resp.Header.Get("Stream-Next-Offset"); v != "" {
				tail = v
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	return tail, nil
}

// appendOne POSTs a single message numbered seq to an existing stream, returning
// the new tail. Used by index-repair to push work past a dropped index entry.
func appendOne(base, stream string, seq int) (string, error) {
	return appendRest(base, stream, seq, seq+1)
}

type subscriptionView struct {
	Streams []struct {
		Path        string `json:"path"`
		AckedOffset string `json:"acked_offset"`
	} `json:"streams"`
}

func getSubscription(base, id string) (subscriptionView, error) {
	var v subscriptionView
	url := fmt.Sprintf("%s/v1/stream/__ds/subscriptions/%s", base, id)
	err := retry(20, 500*time.Millisecond, func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("get subscription status %d", resp.StatusCode)
		}
		return json.NewDecoder(resp.Body).Decode(&v)
	})
	return v, err
}

func waitReady(base string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	probe := base + "/v1/stream/__jepsen_health__"
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPut, probe, bytes.NewReader([]byte("x")))
		req.Header.Set("Content-Type", "text/plain")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				dreq, _ := http.NewRequest(http.MethodDelete, probe, nil)
				if dresp, derr := http.DefaultClient.Do(dreq); derr == nil {
					dresp.Body.Close()
				}
				return nil
			}
		}
		sleep(time.Second)
	}
	return fmt.Errorf("not ready after %s", timeout)
}

// ---- embedded webhook receiver ----

type receiver struct {
	mu        sync.Mutex
	delivered int
	perOffset map[string]int // stream+":"+tail -> count, for the duplicate factor
}

func newReceiver() *receiver { return &receiver{perOffset: map[string]int{}} }

func (r *receiver) serve(port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, req *http.Request) {
		var notif struct {
			Streams []struct {
				Path       string `json:"path"`
				TailOffset string `json:"tail_offset"`
				HasPending bool   `json:"has_pending"`
			} `json:"streams"`
		}
		body, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(body, &notif)
		r.mu.Lock()
		r.delivered++
		for _, s := range notif.Streams {
			if s.HasPending {
				r.perOffset[s.Path+":"+s.TailOffset]++
			}
		}
		r.mu.Unlock()
		// Synchronously finish: the server auto-acks the snapshot and releases.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"done":true}`))
	})
	srv := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", port), Handler: mux}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "receiver listen:", err)
		os.Exit(1)
	}
	go func() { _ = srv.Serve(ln) }()
	return srv
}

func (r *receiver) stats() (delivered int, dupFactor float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	unique := len(r.perOffset)
	total := 0
	for _, n := range r.perOffset {
		total += n
	}
	if unique == 0 {
		return r.delivered, 1
	}
	return r.delivered, float64(total) / float64(unique)
}

// ---- nemesis (kubectl fault injection) ----

type nemesis struct {
	ctx, ns, scenario string
	mu                sync.Mutex
	log               []string
}

func (n *nemesis) record(action string) {
	n.mu.Lock()
	n.log = append(n.log, action)
	n.mu.Unlock()
}

func (n *nemesis) run(stop <-chan struct{}) {
	switch n.scenario {
	case "origin-restart", "index-repair":
		// Continuously churn one origin so appends and wakes race pod loss.
		// index-repair runs the same origin churn while it also drops fan-out
		// index entries from the workload goroutine.
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n.killOneOrigin()
			}
		}
	case "glob-create-crash", "pull-wake-arm-crash":
		// These scenarios drive their own decisive crashes inline (right after a
		// stream create / a wake arm) from the workload goroutine, so the
		// background nemesis only adds light churn to widen the race window.
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n.killOneOrigin()
			}
		}
	case "redis-restart":
		// A single Redis restart mid-workload: the PVC-backed AOF must replay
		// so the log and the subscription control plane survive, and the
		// reconnected origins re-fire owed wakes.
		select {
		case <-stop:
		case <-time.After(1 * time.Second):
			n.killRedis()
			<-stop
		}
	default: // baseline
		<-stop
	}
}

func (n *nemesis) killOneOrigin() {
	out, _ := n.kubectl("get", "pods", "-l", "app=chronicle", "-o", "jsonpath={.items[0].metadata.name}")
	pod := string(bytes.TrimSpace(out))
	if pod == "" {
		return
	}
	n.kubectl("delete", "pod", pod, "--grace-period=0", "--force")
	n.record("kill-origin")
}

func (n *nemesis) killAllOrigins() {
	n.kubectl("delete", "pods", "-l", "app=chronicle", "--grace-period=0", "--force")
	n.record("kill-all-origins")
}

func (n *nemesis) killRedis() {
	n.kubectl("delete", "pods", "-l", "app=redis", "--grace-period=0", "--force")
	n.record("kill-redis")
}

// deleteStreamIndex removes one stream's fan-out index SET
// (ds:{__ds}:stream:<path>) directly in Redis, simulating a crash between the
// canonical Lua link write and the Go-side SADD that maintains the index. The
// link survives; only the index entry is dropped. ReconcileIndexes must rebuild
// it from the canonical links. The deployment uses Redis DB 0 (deploy.yaml
// REDIS_URL .../0); run.sh flushes the same DB between scenarios.
func (n *nemesis) deleteStreamIndex(path string) {
	key := fmt.Sprintf("ds:{__ds}:stream:%s", path)
	n.redisCLI("del", key)
	n.record("drop-stream-index")
}

// redisCLI runs redis-cli inside the redis pod against DB 0 (the deployment DB).
func (n *nemesis) redisCLI(args ...string) ([]byte, error) {
	full := append([]string{"exec", "deploy/redis", "--", "redis-cli", "-n", "0"}, args...)
	return n.kubectl(full...)
}

func (n *nemesis) kubectl(args ...string) ([]byte, error) {
	full := append([]string{"--context", n.ctx, "-n", n.ns}, args...)
	return exec.Command("kubectl", full...).CombinedOutput()
}

// ---- small helpers ----

func retry(attempts int, delay time.Duration, f func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}
		sleep(delay)
	}
	return err
}

func sleep(d time.Duration) { <-time.After(d) }

func short(o string) string {
	if len(o) > 12 {
		return o[len(o)-12:]
	}
	return o
}

func join(ss []string) string {
	counts := map[string]int{}
	for _, s := range ss {
		counts[s]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s×%d", k, counts[k])
	}
	return out
}
