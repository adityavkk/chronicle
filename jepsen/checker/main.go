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
	namespace  string
	streams    int
	msgs       int
	scenario   string
	settle     time.Duration
	workloadMs int
}

func main() {
	var c config
	flag.StringVar(&c.base, "base", "http://localhost:4438", "chronicle base URL (via k3d loadbalancer)")
	flag.StringVar(&c.recvHost, "recv-host", "host.k3d.internal", "hostname the cluster uses to reach this receiver")
	flag.IntVar(&c.recvPort, "recv-port", 8099, "local webhook receiver port")
	flag.StringVar(&c.cluster, "cluster", "chronicle-jepsen", "k3d cluster name (for kubectl context)")
	flag.StringVar(&c.namespace, "namespace", "chronicle-jepsen", "kubernetes namespace")
	flag.IntVar(&c.streams, "streams", 8, "number of event streams")
	flag.IntVar(&c.msgs, "msgs", 40, "messages appended per stream")
	flag.StringVar(&c.scenario, "scenario", "origin-restart", "baseline|origin-restart|redis-restart")
	flag.DurationVar(&c.settle, "settle", 25*time.Second, "post-fault settle time for the recovery sweep")
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
	ctx := fmt.Sprintf("k3d-%s", c.cluster)
	subID := fmt.Sprintf("jepsen-sub-%d", time.Now().UnixNano())
	webhookURL := fmt.Sprintf("http://%s:%d/webhook", c.recvHost, c.recvPort)

	fmt.Printf("== scenario %q: %d streams x %d msgs ==\n", c.scenario, c.streams, c.msgs)
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}

	// 1. Create the webhook subscription. The receiver returns {done:true}, so
	//    the server auto-acks each wake's snapshot.
	if err := createSubscription(c.base, subID, webhookURL); err != nil {
		return err
	}
	fmt.Printf("created subscription %s -> %s\n", subID, webhookURL)

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
		tail, err := appendStream(c.base, stream, c.msgs)
		if err != nil {
			close(stopNemesis)
			wg.Wait()
			return fmt.Errorf("workload %s: %w", stream, err)
		}
		expected[stream] = tail
	}
	fmt.Printf("workload done: appended %d messages\n", c.streams*c.msgs)

	// 4. The decisive fault: kill ALL origins after the last append, so the
	//    final wake can only come from the recovery sweep on a restarted origin.
	if c.scenario == "origin-restart" {
		nem.killAllOrigins()
		fmt.Println("nemesis: killed all origins after final append")
	}
	close(stopNemesis)
	wg.Wait()

	// 5. Let the origin come back and the recovery sweep run.
	if err := waitReady(c.base, 90*time.Second); err != nil {
		return fmt.Errorf("chronicle did not recover: %w", err)
	}
	fmt.Printf("settling %s for the recovery sweep...\n", c.settle)
	sleep(c.settle)

	// 6. Verify durability: every stream's cursor advanced to its tail.
	return verify(c, subID, expected, r, nem)
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

func createSubscription(base, id, webhookURL string) error {
	body, _ := json.Marshal(map[string]any{
		"type":         "webhook",
		"pattern":      "events/*",
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

// appendStream creates a stream with its first message then appends the rest,
// returning the final tail offset. Each request retries through origin churn.
func appendStream(base, stream string, msgs int) (string, error) {
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
	if err != nil {
		return "", err
	}
	for n := 1; n < msgs; n++ {
		payload := []byte(fmt.Sprintf(`{"n":%d}`, n))
		err := retry(160, 500*time.Millisecond, func() error {
			req, _ := http.NewRequest(http.MethodPost, createURL, bytes.NewReader(payload))
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
	case "origin-restart":
		// Continuously churn one origin so appends and wakes race pod loss.
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
