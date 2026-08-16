package webhook

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// These integration tests cover the #16 Tier B durability path against a live
// Redis (REDIS_URL or redis://localhost:6379/14), skipped under -short like the
// rest of redis_store_test.go. The Tier B WAITAOF barrier needs an AOF server, so
// they skip when appendonly is off. They assert the correction-#3 separation
// directly: WAITAOF is durability, the (gen,wake_id) fence is exclusivity.

type fakeDurabilityClient struct {
	goredis.UniversalClient
	mint           *fakeDurabilityConn
	other          *fakeDurabilityConn
	poolBarrier    string
	poolBarrierOff int64
	poolDoCalls    int
	slots          []goredis.ClusterSlot
	nodes          map[string]*fakeDurabilityConn
	lastPinnedAddr string
}

func newFakeDurabilityClient(reply []any) *fakeDurabilityClient {
	return &fakeDurabilityClient{
		mint:  &fakeDurabilityConn{name: "mint", offset: 40, reply: reply},
		other: &fakeDurabilityConn{name: "other", offset: 7, reply: reply},
	}
}

func (f *fakeDurabilityClient) PinnedConnForKeys(_ context.Context, keys []string) (pinnedRedisConn, error) {
	if len(f.slots) == 0 {
		return f.mint, nil
	}
	addr, err := clusterMasterAddrForKey(f.slots, keys[0])
	if err != nil {
		return nil, err
	}
	f.lastPinnedAddr = addr
	conn := f.nodes[addr]
	if conn == nil {
		return nil, fmt.Errorf("missing fake node %s", addr)
	}
	return conn, nil
}

func (f *fakeDurabilityClient) Eval(_ context.Context, _ string, _ []string, _ ...interface{}) *goredis.Cmd {
	return f.mint.eval()
}

func (f *fakeDurabilityClient) EvalSha(_ context.Context, _ string, _ []string, _ ...interface{}) *goredis.Cmd {
	return f.mint.eval()
}

func (f *fakeDurabilityClient) EvalRO(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return f.mint.eval()
}

func (f *fakeDurabilityClient) EvalShaRO(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return f.mint.eval()
}

func (f *fakeDurabilityClient) ScriptExists(context.Context, ...string) *goredis.BoolSliceCmd {
	return goredis.NewBoolSliceResult(nil, nil)
}

func (f *fakeDurabilityClient) ScriptLoad(context.Context, string) *goredis.StringCmd {
	return goredis.NewStringResult("", nil)
}

func (f *fakeDurabilityClient) Do(_ context.Context, args ...interface{}) *goredis.Cmd {
	f.poolDoCalls++
	f.poolBarrier = fmt.Sprint(args[0])
	f.poolBarrierOff = f.other.offset
	return f.other.Do(context.Background(), args...)
}

type fakeDurabilityConn struct {
	name           string
	offset         int64
	postEvalOffset int64
	barrierOffset  int64
	barrierCommand string
	barrierCalls   int
	closed         bool
	reply          []any
}

func (c *fakeDurabilityConn) eval() *goredis.Cmd {
	c.offset += 10
	c.postEvalOffset = c.offset
	return goredis.NewCmdResult(c.reply, nil)
}

func (c *fakeDurabilityConn) Eval(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return c.eval()
}

func (c *fakeDurabilityConn) EvalSha(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return c.eval()
}

func (c *fakeDurabilityConn) EvalRO(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return c.eval()
}

func (c *fakeDurabilityConn) EvalShaRO(context.Context, string, []string, ...interface{}) *goredis.Cmd {
	return c.eval()
}

func (c *fakeDurabilityConn) ScriptExists(context.Context, ...string) *goredis.BoolSliceCmd {
	return goredis.NewBoolSliceResult(nil, nil)
}

func (c *fakeDurabilityConn) ScriptLoad(context.Context, string) *goredis.StringCmd {
	return goredis.NewStringResult("", nil)
}

func (c *fakeDurabilityConn) Do(_ context.Context, args ...interface{}) *goredis.Cmd {
	c.barrierCalls++
	c.barrierCommand = fmt.Sprint(args[0])
	c.barrierOffset = c.offset
	switch c.barrierCommand {
	case "WAITAOF":
		return goredis.NewCmdResult([]any{int64(1), int64(1)}, nil)
	case "WAIT":
		return goredis.NewCmdResult(int64(1), nil)
	default:
		return goredis.NewCmdResult(nil, fmt.Errorf("unexpected command %s", c.barrierCommand))
	}
}

func (c *fakeDurabilityConn) Close() error {
	c.closed = true
	return nil
}

func TestTierBDurabilityBarrierPinnedToMintConnection(t *testing.T) {
	tests := []struct {
		name  string
		reply []any
		run   func(*RedisStore) error
	}{
		{
			name:  "arm wake",
			reply: []any{"ARMED", int64(7), "wake-a"},
			run: func(s *RedisStore) error {
				_, err := s.ArmWakeUnscoped("s1", time.Now(), 1000, true, "wake-a")
				return err
			},
		},
		{
			name:  "claim shard",
			reply: []any{"CLAIMED", int64(8), "wake-c", "worker-1"},
			run: func(s *RedisStore) error {
				_, err := s.claimShardAuthorized("s1", 0, "worker-1", "wake-c", SubscriptionExpectation{}, time.Now(), 1000)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeDurabilityClient(tc.reply)
			s := (&RedisStore{client: client, metrics: NopMetrics{}}).WithConsistency(TierB, 1, 1000)
			if err := tc.run(s); err != nil {
				t.Fatalf("operation failed: %v", err)
			}
			if client.poolDoCalls != 0 {
				t.Fatalf("barrier used pooled client %d times; want pinned mint connection", client.poolDoCalls)
			}
			if client.mint.barrierCalls != 1 || client.mint.barrierCommand != "WAITAOF" {
				t.Fatalf("mint conn barrier = %d %q, want one WAITAOF", client.mint.barrierCalls, client.mint.barrierCommand)
			}
			if client.mint.barrierOffset < client.mint.postEvalOffset {
				t.Fatalf("barrier observed offset %d before mint offset %d", client.mint.barrierOffset, client.mint.postEvalOffset)
			}
			if !client.mint.closed {
				t.Fatal("pinned connection was not closed")
			}
		})
	}
}

func TestTierBClusterPinnedConnTargetsMintSlotOwner(t *testing.T) {
	key := subKey("cluster-sub")
	slot := redisClusterSlot(key)
	slots := make([]goredis.ClusterSlot, 0, 3)
	if slot > 0 {
		slots = append(slots, goredis.ClusterSlot{Start: 0, End: slot - 1, Nodes: []goredis.ClusterNode{{Addr: "node-a:6379"}}})
	}
	slots = append(slots, goredis.ClusterSlot{Start: slot, End: slot, Nodes: []goredis.ClusterNode{{Addr: "node-b:6379"}}})
	if slot < 16383 {
		slots = append(slots, goredis.ClusterSlot{Start: slot + 1, End: 16383, Nodes: []goredis.ClusterNode{{Addr: "node-c:6379"}}})
	}
	reply := []any{"ARMED", int64(7), "wake-a"}
	client := newFakeDurabilityClient(reply)
	client.slots = slots
	client.nodes = map[string]*fakeDurabilityConn{
		"node-a:6379": {name: "node-a", offset: 100, reply: reply},
		"node-b:6379": {name: "node-b", offset: 200, reply: reply},
		"node-c:6379": {name: "node-c", offset: 300, reply: reply},
	}
	s := (&RedisStore{client: client, metrics: NopMetrics{}}).WithConsistency(TierB, 1, 1000)
	if _, err := s.ArmWakeUnscoped("cluster-sub", time.Now(), 1000, true, "wake-a"); err != nil {
		t.Fatalf("ArmWakeUnscoped: %v", err)
	}
	if client.lastPinnedAddr != "node-b:6379" {
		t.Fatalf("pinned addr = %q, want mint slot owner node-b:6379", client.lastPinnedAddr)
	}
	if got := client.nodes["node-b:6379"].barrierCalls; got != 1 {
		t.Fatalf("slot owner barrier calls = %d, want 1", got)
	}
	if got := client.nodes["node-a:6379"].barrierCalls + client.nodes["node-c:6379"].barrierCalls; got != 0 {
		t.Fatalf("non-owner barrier calls = %d, want 0", got)
	}
}

// requireAOF skips when the test Redis is not running with appendonly yes —
// WAITAOF errors without AOF and the Tier B path is only meaningful against one.
func requireAOF(t *testing.T, client goredis.UniversalClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	vals, err := client.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		t.Skipf("CONFIG GET appendonly: %v", err)
	}
	if vals["appendonly"] != "yes" {
		t.Skip("Tier B durability test needs Redis with appendonly yes (point REDIS_URL at an AOF server)")
	}
}

// TestStoreTierBArmDurableLocalFsync: with numReplicas=0 (the single-Redis local
// rig) WAITAOF 1 0 is satisfied by the local AOF fsync, so the arm is durable.
func TestStoreTierBArmDurableLocalFsync(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 0, 3000) // local AOF fsync only; appendfsync everysec can straddle 1s
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	res, err := s.ArmWakeUnscoped("s1", now, 1000, true, "w_a")
	if err != nil {
		t.Fatalf("Tier B arm with local fsync must be durable: %v", err)
	}
	if !res.Armed || res.Generation != 1 {
		t.Fatalf("arm result = %+v, want Armed gen=1", res)
	}
}

// TestStoreTierBArmShortReplicaSurfacedAsError: requiring a replica on a
// single-Redis rig yields the [1,0] short reply, surfaced as a *DurabilityShort
// Error and NEVER swallowed — yet the fence is still minted on the primary. This
// is correction #3 end to end: WAITAOF is durability, the fence is exclusivity.
func TestStoreTierBArmShortReplicaSurfacedAsError(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 1, 200) // require 1 replica; the local rig has none
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)

	_, err := s.ArmWakeUnscoped("s1", now, 1000, true, "w_a")
	if err == nil {
		t.Fatal("Tier B arm requiring a replica must surface the short WAITAOF reply as an error")
	}
	var de *DurabilityShortError
	if !errors.As(err, &de) {
		t.Fatalf("want *DurabilityShortError, got %T: %v", err, err)
	}
	if de.GotReplicas != 0 || de.WantReplicas != 1 {
		t.Errorf("short error counts = %+v, want replicas 0/1", de)
	}
	// The durability miss did NOT prevent the fence from being minted on the
	// primary: generation advanced and the phase moved to waking. The fence — not
	// the WAIT count — is what governs exclusivity.
	sub, ok, gerr := s.Get("s1")
	if gerr != nil || !ok {
		t.Fatalf("get after arm: ok=%v err=%v", ok, gerr)
	}
	if sub.Generation != 1 || sub.Phase != PhaseWaking {
		t.Errorf("fence must be minted on the primary despite the durability miss: gen=%d phase=%v", sub.Generation, sub.Phase)
	}
}

// TestStoreTierAIssuesNoWait: Tier A never blocks the write — even configured
// with a replica requirement it issues no WAIT and never reports a durability
// error (the fast, at-least-once default path).
func TestStoreTierAIssuesNoWait(t *testing.T) {
	base, _ := newTestStore(t)
	s := base.WithConsistency(TierA, 1, 200)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	res, err := s.ArmWakeUnscoped("s1", now, 1000, true, "w_a")
	if err != nil {
		t.Fatalf("Tier A must issue no WAIT and never error on durability: %v", err)
	}
	if !res.Armed {
		t.Fatal("Tier A arm should succeed")
	}
}

// TestRecordDurabilityShortIncrementsMetric is the pure (no-cluster) metric-seam
// spec for issue #43: a short WAIT/WAITAOF verdict (a *DurabilityShortError)
// increments chronicle_durability_short_total labeled by the command that fell
// short, while a satisfied barrier (nil) records nothing. The verdict is returned
// unchanged either way — the metric makes the short reply OBSERVABLE, it never
// swallows the error that stops dispatch. The double captures only the command
// name, mirroring the correction-#3 durability-only contract (no holder/gen/count).
func TestRecordDurabilityShortIncrementsMetric(t *testing.T) {
	fm := &fakeMetrics{}
	s := &RedisStore{metrics: fm}

	// A satisfied barrier records nothing and returns nil.
	if got := s.recordDurabilityShort(nil, "WAITAOF"); got != nil {
		t.Errorf("nil verdict must pass through unchanged, got %v", got)
	}
	if n := fm.durabilityShorts("WAITAOF"); n != 0 {
		t.Errorf("satisfied barrier must record no DurabilityShort, got %d", n)
	}

	// A short WAITAOF reply increments the WAITAOF counter and is returned unchanged.
	short := &DurabilityShortError{WantLocal: 1, GotLocal: 1, WantReplicas: 1, GotReplicas: 0, UseAOF: true}
	if got := s.recordDurabilityShort(short, "WAITAOF"); !errors.Is(got, short) {
		t.Errorf("short verdict must be returned unchanged (never swallowed), got %v", got)
	}
	if n := fm.durabilityShorts("WAITAOF"); n != 1 {
		t.Errorf("short WAITAOF reply must increment WAITAOF counter once, got %d", n)
	}
	// A short plain-WAIT reply is labeled separately so the operator gauge splits
	// the two barrier commands.
	if got := s.recordDurabilityShort(short, "WAIT"); !errors.Is(got, short) {
		t.Errorf("short WAIT verdict must be returned unchanged, got %v", got)
	}
	if n := fm.durabilityShorts("WAIT"); n != 1 {
		t.Errorf("short WAIT reply must increment WAIT counter once, got %d", n)
	}

	// A non-durability error (e.g. a transport/Lua failure) is NOT a DurabilityShort
	// — it passes through without touching the RPO-exposure counter.
	other := errors.New("webhook: WAITAOF: connection reset")
	if got := s.recordDurabilityShort(other, "WAITAOF"); !errors.Is(got, other) {
		t.Errorf("non-DurabilityShort error must pass through, got %v", got)
	}
	if n := fm.durabilityShorts("WAITAOF"); n != 1 {
		t.Errorf("non-DurabilityShort error must not increment the counter, got %d", n)
	}
}

// TestStoreTierBClaimDurable: the pull-wake claim grant (claim.lua fence rotation)
// also takes the Tier B barrier; with local fsync it is durable.
func TestStoreTierBClaimDurable(t *testing.T) {
	base, client := newTestStore(t)
	requireAOF(t, client)
	s := base.WithConsistency(TierB, 0, 3000)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")
	cfg.Type = DispatchPullWake
	cfg.WakeStream = "wake/pool"
	_, _ = s.CreateOrConfirm("p1", cfg, nil, now)
	c, err := s.Claim("p1", "worker-1", "w_a", now, 1000)
	if err != nil {
		t.Fatalf("Tier B claim with local fsync must be durable: %v", err)
	}
	if !c.Claimed {
		t.Fatalf("claim result = %+v, want Claimed", c)
	}
}
