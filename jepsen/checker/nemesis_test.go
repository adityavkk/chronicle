package main

import (
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// errKill is a test sentinel for a failed kubectl delete in the redisFailover tests.
var errKill = errors.New("kubectl delete pod failed")

// One unit test per nemesis primitive (nemesis.go), all cluster-free: the pure
// pieces are tested directly, and the shell methods are tested through a
// recording runner injected on the nemesis, so the exact kubectl/redis-cli
// command is asserted without shelling out.

// recordingNemesis returns a nemesis whose external commands are captured rather
// than executed, plus a pointer to the captured command slice.
func recordingNemesis(reply map[string]string) (*nemesis, *[][]string) {
	var mu sync.Mutex
	cmds := &[][]string{}
	n := &nemesis{ctx: "k3d-x", ns: "ns", runner: func(name string, args ...string) ([]byte, error) {
		mu.Lock()
		*cmds = append(*cmds, append([]string{name}, args...))
		mu.Unlock()
		// reply keyed by a substring of the joined command (e.g. "hget").
		joined := strings.Join(args, " ")
		for k, v := range reply {
			if strings.Contains(joined, k) {
				return []byte(v), nil
			}
		}
		return nil, nil
	}}
	return n, cmds
}

// gcPause holds strictly past the lease TTL.
func TestGCPause_HoldsPastTTL(t *testing.T) {
	ttl := 1500 * time.Millisecond
	if got := gcPause(ttl); got <= ttl {
		t.Fatalf("gcPause(%s) = %s, must hold past the TTL", ttl, got)
	}
}

// nemesisWindow draws within [min, max] for every seed, and collapses to min when
// the range is empty.
func TestNemesisWindow_InRange(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	min, max := 2*time.Second, 8*time.Second
	for i := 0; i < 1000; i++ {
		w := nemesisWindow(rng, min, max)
		if w < min || w >= max {
			t.Fatalf("window %s out of [%s,%s)", w, min, max)
		}
	}
	if w := nemesisWindow(rng, 5*time.Second, 5*time.Second); w != 5*time.Second {
		t.Fatalf("empty range should return min, got %s", w)
	}
}

// dropLeaseTail ZREMs ONLY the lease schedule entry — never the sub hash.
func TestDropLeaseTail_ZremsOnlyScheduleEntry(t *testing.T) {
	n, cmds := recordingNemesis(nil)
	n.dropLeaseTail("sub-x")

	if len(*cmds) != 1 {
		t.Fatalf("expected exactly one command, got %v", *cmds)
	}
	got := strings.Join((*cmds)[0], " ")
	if !strings.Contains(got, "zrem") || !strings.Contains(got, leaseSchedZKey) || !strings.Contains(got, "sub-x") {
		t.Fatalf("expected ZREM of the lease ZSET for sub-x, got %q", got)
	}
	// The sub hash must be untouched — that is the whole point of the L3 fault.
	for _, c := range *cmds {
		j := strings.Join(c, " ")
		if strings.Contains(j, "ds:{__ds}:sub:sub-x") || strings.Contains(j, "del ") {
			t.Fatalf("dropLeaseTail must not touch the sub hash, saw %q", j)
		}
	}
}

// ownerPodFromReplicaID strips a 32-hex start nonce; a non-pod id yields "".
func TestOwnerPodFromReplicaID(t *testing.T) {
	nonce := "0123456789abcdef0123456789abcdef" // 32 hex
	cases := map[string]string{
		"chronicle-7d9f-" + nonce:       "chronicle-7d9f",
		"chronicle-0-" + nonce:          "chronicle-0",
		"local-dev-uuid":                "", // no 32-hex suffix
		nonce:                           "", // no separator
		"pod-" + strings.ToUpper(nonce): "", // uppercase is not our lowercase hex
		"pod-" + nonce[:31]:             "", // 31 chars, not 32
	}
	for id, want := range cases {
		if got := ownerPodFromReplicaID(id); got != want {
			t.Errorf("ownerPodFromReplicaID(%q) = %q, want %q", id, got, want)
		}
	}
}

// killSlotOwner reads owner_id and kills the derived pod.
func TestKillSlotOwner_KillsOwnerPod(t *testing.T) {
	nonce := "0123456789abcdef0123456789abcdef"
	n, cmds := recordingNemesis(map[string]string{"hget": "chronicle-abc-" + nonce})
	n.killSlotOwner(7)

	var sawHGet, sawDelete bool
	for _, c := range *cmds {
		j := strings.Join(c, " ")
		if strings.Contains(j, "hget") && strings.Contains(j, ownershipSlotKey(7)) {
			sawHGet = true
		}
		if strings.Contains(j, "delete pod chronicle-abc") {
			sawDelete = true
		}
	}
	if !sawHGet || !sawDelete {
		t.Fatalf("expected HGET slot:7 then delete pod chronicle-abc, got %v", *cmds)
	}
}

// killSlotOwner is a clean no-op when the slot is unowned (today's code — the
// ownership record lands in #14).
func TestKillSlotOwner_NoopWhenUnowned(t *testing.T) {
	n, cmds := recordingNemesis(nil) // hget returns empty
	n.killSlotOwner(3)
	for _, c := range *cmds {
		if strings.Contains(strings.Join(c, " "), "delete pod") {
			t.Fatalf("must not kill any pod when the slot is unowned, got %v", *cmds)
		}
	}
	if n.log[len(n.log)-1] != "kill-slot-owner-noop" {
		t.Fatalf("expected kill-slot-owner-noop, got %v", n.log)
	}
}

// clockSkewShell builds relative date arithmetic carrying the offset in seconds.
func TestClockSkewShell(t *testing.T) {
	got := clockSkewShell(30 * time.Second)
	if !strings.Contains(got, "date -s") || !strings.Contains(got, "+ 30") {
		t.Fatalf("clockSkewShell(30s) = %q, want a +30s date -s arithmetic", got)
	}
}

// toxiproxy partition/latency hit the right admin endpoints with the right bodies.
func TestToxiproxy_RequestContract(t *testing.T) {
	type call struct{ method, path, body string }
	var got []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		got = append(got, call{r.Method, r.URL.Path, string(buf)})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tp := newToxiproxy(srv.URL, "redis-claude")
	if err := tp.partition(); err != nil {
		t.Fatal(err)
	}
	if err := tp.addLatency(120); err != nil {
		t.Fatal(err)
	}
	if err := tp.heal(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 admin calls, got %d (%+v)", len(got), got)
	}
	if got[0].path != "/proxies/redis-claude" || !strings.Contains(got[0].body, `"enabled":false`) {
		t.Errorf("partition: %+v", got[0])
	}
	if got[1].path != "/proxies/redis-claude/toxics" || !strings.Contains(got[1].body, `"latency":120`) {
		t.Errorf("addLatency: %+v", got[1])
	}
	if got[2].path != "/proxies/redis-claude" || !strings.Contains(got[2].body, `"enabled":true`) {
		t.Errorf("heal: %+v", got[2])
	}
}

// toxiproxy surfaces a non-2xx admin reply as an error.
func TestToxiproxy_NonOKIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no such proxy", http.StatusNotFound)
	}))
	defer srv.Close()
	if err := newToxiproxy(srv.URL, "missing").partition(); err == nil {
		t.Fatal("expected an error on a 404 admin reply")
	}
}

// ---- redisFailover (#43, gate #5) ----

// promoteReplicaCmd is exactly REPLICAOF NO ONE — the standalone-promotion command.
func TestPromoteReplicaCmd(t *testing.T) {
	got := strings.Join(promoteReplicaCmd(), " ")
	if got != "REPLICAOF NO ONE" {
		t.Fatalf("promoteReplicaCmd() = %q, want %q", got, "REPLICAOF NO ONE")
	}
}

// flipEndpointPatch repoints the stable `redis` Service selector to the given role,
// so chronicle's unchanged redis URL reconnects to the promoted node.
func TestFlipEndpointPatch(t *testing.T) {
	got := flipEndpointPatch("replica")
	for _, want := range []string{`"selector"`, `"app":"redis"`, `"role":"replica"`} {
		if !strings.Contains(got, want) {
			t.Errorf("flipEndpointPatch missing %q in %q", want, got)
		}
	}
}

// parseMasterReplOffset reads master_repl_offset; absent/garbage -> 0.
func TestParseMasterReplOffset(t *testing.T) {
	cases := []struct {
		name string
		info string
		want int64
	}{
		{"absent", "role:master\r\nconnected_slaves:1\r\n", 0},
		{"present", "role:master\r\nmaster_repl_offset:123456\r\nconnected_slaves:1\r\n", 123456},
		{"zero", "master_repl_offset:0\n", 0},
		{"garbage", "master_repl_offset:not-a-number\n", 0},
		{"empty", "", 0},
	}
	for _, tc := range cases {
		if got := parseMasterReplOffset(tc.info); got != tc.want {
			t.Errorf("%s: parseMasterReplOffset = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// redisFailover issues the full sequence (kill primary -> REPLICAOF NO ONE on the
// replica -> patch the redis Service) and computes the RPO from the sampled
// offsets. With the primary 200 bytes ahead of the replica at promotion time, the
// empirical RPO is 200 dropped replication bytes.
func TestRedisFailover_SequenceAndRPO(t *testing.T) {
	n, cmds := recordingNemesis(map[string]string{
		// The pre-kill primary sample and the post-promotion replica sample. The
		// recordingNemesis matches by a command substring; we key on the pod the exec
		// targets so the two INFO replication reads return different offsets.
		"deploy/redis-primary": "master_repl_offset:1200\r\nconnected_slaves:1\r\n",
		"deploy/redis-replica": "master_repl_offset:1000\r\nrole:master\r\n",
	})
	rpo := n.redisFailover()
	if rpo != 200 {
		t.Fatalf("empirical RPO = %d, want 200 (primary 1200 - promoted 1000)", rpo)
	}

	var sawKill, sawPromote, sawFlip bool
	for _, c := range *cmds {
		j := strings.Join(c, " ")
		if strings.Contains(j, "delete pod") && strings.Contains(j, "role=primary") {
			sawKill = true
		}
		if strings.Contains(j, "deploy/redis-replica") && strings.Contains(j, "REPLICAOF NO ONE") {
			sawPromote = true
		}
		if strings.Contains(j, "patch service redis") && strings.Contains(j, `"role":"replica"`) {
			sawFlip = true
		}
	}
	if !sawKill || !sawPromote || !sawFlip {
		t.Fatalf("redisFailover must kill the primary, REPLICAOF NO ONE the replica, and flip the endpoint; got %v", *cmds)
	}
	// The promotion fault must NOT issue any WAIT/WAITAOF or read a lease — safety is
	// the fence's job, asserted by the scenario, never inferred from a durability
	// count here (correction #3).
	for _, c := range *cmds {
		j := strings.ToUpper(strings.Join(c, " "))
		if strings.Contains(j, "WAITAOF") || strings.Contains(j, "WAIT ") {
			t.Fatalf("redisFailover must not issue a WAIT/WAITAOF (correction #3), saw %q", j)
		}
	}
}

// A negative RPO (the promoted node advanced past the pre-kill primary sample) is
// clamped to 0 — no bytes were dropped.
func TestRedisFailover_NegativeRPOClampsToZero(t *testing.T) {
	n, _ := recordingNemesis(map[string]string{
		"deploy/redis-primary": "master_repl_offset:1000\r\n",
		"deploy/redis-replica": "master_repl_offset:1500\r\n",
	})
	if rpo := n.redisFailover(); rpo != 0 {
		t.Fatalf("a caught-up replica must yield RPO 0, got %d", rpo)
	}
}

// When the primary kill fails, redisFailover returns -1 (injection failed) and does
// NOT proceed to promote — the scenario reports an honest failure, not a 0 RPO.
func TestRedisFailover_KillFailureReturnsMinusOne(t *testing.T) {
	var n *nemesis
	n = &nemesis{ctx: "k3d-x", ns: "ns", runner: func(name string, args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "delete pod") {
			return nil, errKill
		}
		return nil, nil
	}}
	if rpo := n.redisFailover(); rpo != -1 {
		t.Fatalf("a failed primary kill must return -1, got %d", rpo)
	}
	if n.log[len(n.log)-1] != "redis-failover-kill-failed" {
		t.Fatalf("expected redis-failover-kill-failed, got %v", n.log)
	}
}

// churn fires the action on its randomized cadence and stops cleanly.
func TestChurn_FiresAndStops(t *testing.T) {
	n := &nemesis{}
	rng := rand.New(rand.NewSource(7))
	var mu sync.Mutex
	fired := 0
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		n.churn(stop, rng, time.Millisecond, 3*time.Millisecond, func() { mu.Lock(); fired++; mu.Unlock() })
		close(done)
	}()
	time.Sleep(40 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("churn did not stop after stop was closed")
	}
	mu.Lock()
	defer mu.Unlock()
	if fired == 0 {
		t.Fatal("churn never fired the action")
	}
}
