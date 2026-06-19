package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

// nemesis.go enriches the kubectl/redis-cli nemesis in main.go with the
// proposed-mechanism faults from docs/specs/horizontal-scale/research/07 step 4:
// an in-process gcPause, a toxiproxy partition/latency seam, killSlotOwner,
// dropLeaseTail (the exact L3 fault), a best-effort clock skew, and randomized
// start/stop windows replacing the fixed time.Ticker. The pure pieces (gcPause,
// the replica→pod mapping, the random-window draw, the clock-skew command, the
// toxiproxy request contract) are split out so each is unit-tested without a
// cluster; the methods on *nemesis are the thin shell that issues the command
// through the injectable runner (main.go).
//
// Honest bounds carried from 07: the harness drives from OUTSIDE the process
// (honest-gap #2), so these are coarse pod-kill / proxy-toggle faults, not
// surgical failpoints; the gofail seam is deferred to #16. The recorder clock is
// pinned to the driver host (honest-gap #5), so even the clock-skew nemesis
// cannot corrupt the porcupine history's [Call,Return] ordering.

// ---- gcPause: in-process, no infrastructure (07's highest-ROI T1/T3 nemesis) ----

// gcPauseMargin is how far past the lease a gcPause holds, so the lease is
// reliably expired (and a peer has taken over) before the paused holder resumes.
const gcPauseMargin = 250 * time.Millisecond

// gcPause returns how long an in-process GC-pause nemesis stalls a worker that
// has claimed: strictly past the lease TTL, so a peer takes over (rotating the
// fence) and the paused holder's later ack/heartbeat arrives with a now-stale
// token — Kleppmann's deposed-but-resumed process, the exact case the fence
// exists to make safe. No infrastructure: a sleep in the worker goroutine.
func gcPause(leaseTTL time.Duration) time.Duration { return leaseTTL + gcPauseMargin }

// ---- randomized fault windows (replace the fixed time.Ticker) ----

// nemesisWindow draws a fault start/stop interval uniformly from [min, max]. The
// rng is injected so a seeded run is reproducible and the draw is unit-testable.
// Randomizing the cadence (vs a fixed ticker) widens the set of interleavings the
// faults explore across seeds (07's approximation for the missing failpoint seam).
func nemesisWindow(rng *rand.Rand, min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rng.Int63n(int64(max-min)))
}

// churn fires action on a randomized [min, max] cadence until stop is closed —
// the randomized-window replacement for main.go's fixed-ticker nemesis loops.
func (n *nemesis) churn(stop <-chan struct{}, rng *rand.Rand, min, max time.Duration, action func()) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(nemesisWindow(rng, min, max)):
			action()
		}
	}
}

// ---- dropLeaseTail: the exact L3 fault ----

// leaseSchedZKey is the global lease schedule ZSET on today's single-{__ds}-slot
// code (webhook/keys.go leaseZKey). The harness stays self-contained — it mirrors
// the key literal rather than importing the webhook package, the same way
// deleteStreamIndex mirrors the fan-out index key.
const leaseSchedZKey = "ds:{__ds}:sched:lease"

// dropLeaseTail ZREMs a subscription's lease-schedule entry while leaving its sub
// hash intact — the exact L3 fault (07 line 60): a failover that lost the
// schedule tail. The lease WORKER can no longer see the sub as due (its ZADD is
// gone), so only the cursor-reading recovery sweep can recover it. ONLY the
// schedule entry is removed — never the sub hash — which is what proves the
// reconciler re-derived the tail from the durable cursor, not the schedule. (The
// per-subscription due ZSET is #12; on today's code the lease ZSET is the only
// schedule entry to drop.)
func (n *nemesis) dropLeaseTail(subID string) {
	_, _ = n.redisCLI("zrem", leaseSchedZKey, subID)
	n.record("drop-lease-tail")
}

// ---- killSlotOwner: read the slot owner, kill that pod ----

// ownershipSlotKey is the ownership CAS record for slot h (doc-05 keyspace). It
// has its own {ownership} hash tag, deliberately not slot-homed.
func ownershipSlotKey(slot int) string { return fmt.Sprintf("ds:{ownership}:slot:%d", slot) }

// killSlotOwner reads owner_id from ds:{ownership}:slot:<h> and force-deletes that
// pod — the L2/L4 fault (07 line 59). On today's code the ownership record does
// not exist yet (claim_shard.lua lands in #14), so an empty owner is a clean
// no-op rather than an error: the primitive is the executable spec the #14 driver
// reuses.
func (n *nemesis) killSlotOwner(slot int) {
	out, err := n.redisCLI("hget", ownershipSlotKey(slot), "owner_id")
	owner := strings.TrimSpace(string(out))
	if err != nil || owner == "" {
		n.record("kill-slot-owner-noop") // slot unowned — ownership lands in #14
		return
	}
	pod := ownerPodFromReplicaID(owner)
	if pod == "" {
		n.record("kill-slot-owner-unparsed") // a local/dev UUID replica_id, not pod-derived
		return
	}
	_, _ = n.kubectl("delete", "pod", pod, "--grace-period=0", "--force")
	n.record("kill-slot-owner")
}

// ownerPodFromReplicaID recovers the Kubernetes pod name from a replica_id of the
// form "<podName>-<startNonce>", where startNonce is 32 lowercase hex chars
// (crypto/rand 16 bytes) minted per process start (doc-05 membership protocol).
// Returns "" when the id carries no recognisable nonce suffix (e.g. a local/dev
// UUID), so killSlotOwner skips rather than kill the wrong pod.
func ownerPodFromReplicaID(replicaID string) string {
	i := strings.LastIndexByte(replicaID, '-')
	if i <= 0 {
		return ""
	}
	nonce := replicaID[i+1:]
	if len(nonce) != 32 || !isHex(nonce) {
		return ""
	}
	return replicaID[:i]
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return len(s) > 0
}

// ---- clock skew (best-effort; the recorder clock stays on the host) ----

// clockSkewShell builds the shell command that advances a pod's wall clock by
// offset, to stress the lease-TTL math (07: clock skew on the membership TTL).
// It is relative arithmetic evaluated IN the pod (`date -s @$(( now + offset ))`),
// so it is pure in its input and works with both GNU and busybox `date`.
func clockSkewShell(offset time.Duration) string {
	return fmt.Sprintf("date -s @$(( $(date +%%s) + %d ))", int64(offset.Seconds()))
}

// clockSkew advances the wall clock of the first chronicle pod by offset. It is
// BEST-EFFORT: most clusters reject `date -s` without a privileged container, so
// a failure is recorded as unsupported rather than fatal — and it never threatens
// correctness, because the porcupine recorder reads the driver host's monotonic
// clock, never a node's (07 honest-gap #5). A real clock-skew substrate is its
// own rig (honest-gap #3).
func (n *nemesis) clockSkew(offset time.Duration) {
	out, _ := n.kubectl("get", "pods", "-l", "app=chronicle", "-o", "jsonpath={.items[0].metadata.name}")
	pod := strings.TrimSpace(string(out))
	if pod == "" {
		n.record("clock-skew-noop")
		return
	}
	if _, err := n.kubectl("exec", pod, "--", "sh", "-c", clockSkewShell(offset)); err != nil {
		n.record("clock-skew-unsupported") // no privileged date; documented best-effort
		return
	}
	n.record("clock-skew")
}

// ---- toxiproxy: partition / latency in front of Redis without killing pods ----

// toxiproxyClient drives a Shopify/toxiproxy admin API (07 step 4). The proxy
// sits between chronicle and Redis; disabling it is a clean partition and a
// latency toxic injects RTT — neither kills a pod, so the SUT keeps its in-memory
// state while Redis is unreachable/slow. The toxiproxy sidecar is a rig
// deployment concern; this client is the seam, pinned by an httptest unit test so
// the request contract holds without a live proxy.
type toxiproxyClient struct {
	base  string // admin base URL, e.g. http://localhost:8474
	proxy string // the proxy fronting Redis, e.g. "redis-claude"
	hc    *http.Client
}

func newToxiproxy(base, proxy string) *toxiproxyClient {
	return &toxiproxyClient{base: base, proxy: proxy, hc: &http.Client{Timeout: 5 * time.Second}}
}

// partition cuts the Redis link by disabling the proxy (a full partition).
func (t *toxiproxyClient) partition() error {
	return t.post(fmt.Sprintf("/proxies/%s", t.proxy), map[string]any{"enabled": false})
}

// heal restores the Redis link by re-enabling the proxy.
func (t *toxiproxyClient) heal() error {
	return t.post(fmt.Sprintf("/proxies/%s", t.proxy), map[string]any{"enabled": true})
}

// addLatency injects ms of downstream latency on every Redis round trip.
func (t *toxiproxyClient) addLatency(ms int) error {
	return t.post(fmt.Sprintf("/proxies/%s/toxics", t.proxy), map[string]any{
		"name":       "latency-claude",
		"type":       "latency",
		"stream":     "downstream",
		"attributes": map[string]any{"latency": ms},
	})
}

func (t *toxiproxyClient) post(path string, body map[string]any) error {
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, t.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("toxiproxy %s: status %d", path, resp.StatusCode)
	}
	return nil
}
