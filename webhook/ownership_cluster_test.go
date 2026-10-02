package webhook

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ownership_cluster_test.go runs the batched claim and drain passes against a REAL
// Redis Cluster, where a ClusterClient pipeline is split per master and standalone
// Redis would hide CROSSSLOT, MOVED and per-node stalls. Opt-in: set
// CHRONICLE_TEST_CLUSTER_ADDRS to a comma-separated list of master addresses of a
// throwaway cluster started with --enable-debug-command yes (the stall case uses
// DEBUG SLEEP). Every master is FLUSHed.

// nodePartCounter counts the per-master pipeline parts a ClusterClient pipeline
// fans out into (hooks on the node clients, not the cluster client).
type nodePartCounter struct {
	mu    sync.Mutex
	parts int
}

func (h *nodePartCounter) DialHook(next goredis.DialHook) goredis.DialHook          { return next }
func (h *nodePartCounter) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook { return next }
func (h *nodePartCounter) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		h.mu.Lock()
		h.parts++
		h.mu.Unlock()
		return next(ctx, cmds)
	}
}

func (h *nodePartCounter) take() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.parts
	h.parts = 0
	return n
}

func newClusterStore(t *testing.T) (*RedisStore, *goredis.ClusterClient, *slotTripCounter, *nodePartCounter) {
	t.Helper()
	addrs := os.Getenv("CHRONICLE_TEST_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("set CHRONICLE_TEST_CLUSTER_ADDRS=host:port,... to run against a real Redis Cluster")
	}
	cc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
	parts := &nodePartCounter{}
	cc.OnNewNode(func(node *goredis.Client) { node.AddHook(parts) })
	hook := newSlotTripCounter()
	cc.AddHook(hook)
	ctx := context.Background()
	if err := cc.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error { return c.FlushDB(ctx).Err() }); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return NewRedisStore(cc), cc, hook, parts
}

func allSlotKeys() []string {
	keys := make([]string, subSlots)
	for h := range keys {
		keys[h] = slotKey(h)
	}
	return keys
}

// TestClaimSlotsOnCluster: the reserve and mirror phases hash to the one
// {ownership} master while the claim phase fans out to every master in parallel;
// a SCRIPT FLUSH on every master is healed by one EVAL pipeline per phase; a 4 s
// stall of one master fails or delays only that master's slots, never produces
// BUSY or a transfer, and the next pass renews all 256 with no epoch bump.
func TestClaimSlotsOnCluster(t *testing.T) {
	s, cc, hook, parts := newClusterStore(t)
	ctx := context.Background()
	keys := allSlotKeys()
	var masters atomic.Int32 // ForEachMaster runs its callback concurrently per master
	if err := cc.ForEachMaster(ctx, func(context.Context, *goredis.Client) error { masters.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
	warm := s.ClaimSlots(keys, "warm", time.Unix(1_700_000_000, 0), slotTTL)
	for _, r := range warm {
		if r.Err != nil {
			t.Fatalf("warm: %v", r.Err)
		}
	}
	if err := cc.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error { return c.FlushDB(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	hook.reset()
	parts.take()
	epochs := make([]OwnerEpoch, subSlots)
	for h, r := range s.ClaimSlots(keys, "rA", t0, time.Minute) {
		if r.Err != nil || r.Claim.Status != SlotClaimed {
			t.Fatalf("cold slot %d: %+v/%v", h, r.Claim, r.Err)
		}
		epochs[h] = r.Claim.Epoch
	}
	if singles, pipes := hook.counts(); singles != 0 || pipes != 3 {
		t.Fatalf("cold pass: %d singles, %d pipelines, want 0 and 3", singles, pipes)
	}
	// reserve: 1 part; claim: one part per master; mirror: 1 part.
	if n, want := parts.take(), 2+int(masters.Load()); n != want {
		t.Fatalf("cold pass fanned out into %d node parts, want %d (1 + %d masters + 1)", n, want, masters.Load())
	}

	if err := cc.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error { return c.ScriptFlush(ctx).Err() }); err != nil {
		t.Fatal(err)
	}
	hook.reset()
	for h, r := range s.ClaimSlots(keys, "rA", t0.Add(time.Second), time.Minute) {
		if r.Err != nil || r.Claim.Status != SlotRenewed || r.Claim.Epoch != epochs[h] {
			t.Fatalf("after SCRIPT FLUSH slot %d: %+v/%v, want RENEWED at epoch %v", h, r.Claim, r.Err, epochs[h])
		}
	}
	if singles, pipes := hook.counts(); singles != 0 || pipes != 5 {
		t.Fatalf("healing pass: %d singles, %d pipelines, want 0 and 5 (3 phases + 2 EVAL batches)", singles, pipes)
	}

	// Stall one master for 4 s (past the default 3 s read timeout) while a pass
	// runs. DEBUG SLEEP needs a cluster started with --enable-debug-command yes;
	// without it the case is skipped rather than passing with no stall at all.
	t.Run("one-master stall", func(t *testing.T) {
		stalled, err := cc.MasterForKey(ctx, slotKey(0))
		if err != nil {
			t.Fatal(err)
		}
		if err := stalled.Do(ctx, "DEBUG", "SLEEP", "0").Err(); err != nil {
			t.Skipf("DEBUG SLEEP is unavailable (start the cluster with --enable-debug-command yes): %v", err)
		}
		onStalled := map[int]bool{}
		for h, k := range keys {
			m, err := cc.MasterForKey(ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			onStalled[h] = m.Options().Addr == stalled.Options().Addr
		}
		go func() { _ = stalled.Do(ctx, "DEBUG", "SLEEP", "4").Err() }() // the node client's read times out at 3 s; the server sleeps on
		time.Sleep(200 * time.Millisecond)
		start := time.Now()
		for h, r := range s.ClaimSlots(keys, "rA", start, time.Minute) {
			switch {
			case r.Err != nil:
				if !onStalled[h] {
					t.Fatalf("slot %d on a healthy master errored during the stall: %v", h, r.Err)
				}
			case r.Claim.Status == SlotRenewed && r.Claim.Epoch == epochs[h]:
			default:
				t.Fatalf("slot %d during the stall: %+v, want RENEWED at epoch %v or an error on the stalled master", h, r.Claim, epochs[h])
			}
		}
		took := time.Since(start)
		// Nothing on the stalled master is answered before its sleep ends, so a
		// pass that finished inside the read timeout was never stalled.
		if took < 3*time.Second {
			t.Fatalf("pass during the stall of %s took %v, so the master was not stalled", stalled.Options().Addr, took)
		}
		t.Logf("pass during a 4 s stall of %s took %v", stalled.Options().Addr, took)
		time.Sleep(4*time.Second - took + 200*time.Millisecond)
		for h, r := range s.ClaimSlots(keys, "rA", time.Now(), time.Minute) {
			if r.Err != nil || r.Claim.Status != SlotRenewed || r.Claim.Epoch != epochs[h] {
				t.Fatalf("after the stall slot %d: %+v/%v, want RENEWED at epoch %v (no churn)", h, r.Claim, r.Err, epochs[h])
			}
		}
	})
}

// TestClaimDueSlotsOnCluster: one claim_due pipeline over all 256 per-slot ZSETs
// fans out to every master with no CROSSSLOT (every command is single-key).
func TestClaimDueSlotsOnCluster(t *testing.T) {
	s, cc, hook, parts := newClusterStore(t)
	ctx := context.Background()
	now := time.Now()
	slots := make([]int, subSlots)
	seeded := map[int]string{}
	for h := range slots {
		slots[h] = h
		if h%3 == 0 {
			id := idInSlot(h)
			seeded[h] = id
			if err := cc.ZAdd(ctx, dueZKey(h), goredis.Z{Score: float64(now.Add(-time.Second).UnixNano()), Member: id}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Warm claim_due on every master (an empty schedule drains nothing), so the
	// counted pass sees no NOSCRIPT heal.
	for h, d := range s.ClaimDueSlots(ScheduleLease, slots, now, 1, time.Second) {
		if d.Err != nil {
			t.Fatalf("warm slot %d: %v", h, d.Err)
		}
	}
	hook.reset()
	parts.take()
	drains := s.ClaimDueSlots(ScheduleDue, slots, now, dueClaimLimit, time.Second)
	for h, d := range drains {
		if d.Err != nil {
			t.Fatalf("slot %d: %v", h, d.Err)
		}
		if id, ok := seeded[h]; ok && (len(d.IDs) != 1 || d.IDs[0] != id) {
			t.Fatalf("slot %d: drained %v, want [%s]", h, d.IDs, id)
		} else if !ok && len(d.IDs) != 0 {
			t.Fatalf("slot %d: drained %v from an empty schedule", h, d.IDs)
		}
	}
	if singles, pipes := hook.counts(); singles != 0 || pipes != 1 {
		t.Fatalf("%d singles, %d pipelines for 256 slots, want 0 and 1", singles, pipes)
	}
	t.Logf("one claim_due pipeline fanned out into %d node parts", parts.take())
}
