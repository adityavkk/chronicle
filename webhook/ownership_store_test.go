package webhook

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ownership_store_test.go is the golden table for the {ownership} Lua scripts
// (claim_shard.lua / check_owner.lua) and the membership ZSET, against live Redis
// (skipped under -short). Lease expiry is driven deterministically by passing a
// later `now` rather than sleeping, so the tests are fast and flake-free. These
// match the model_shard.go semantics the jepsen T3 gate checks.

const slotTTL = 1 * time.Second

func TestClaimShardGoldenTable(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	// First claim of an unowned slot: CLAIMED, owner_epoch minted to 1.
	c, err := s.ClaimSlot(key, "A", t0, slotTTL)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if c.Status != SlotClaimed || c.Owner.String() != "A" || c.Epoch.Value() != 1 {
		t.Fatalf("first claim = %+v, want CLAIMED owner=A epoch=1", c)
	}
	if !c.Granted() || !c.Transferred() {
		t.Fatalf("first claim: granted=%v transferred=%v, want true/true", c.Granted(), c.Transferred())
	}

	// Same owner re-claims before expiry: RENEWED, epoch UNCHANGED (1).
	c, err = s.ClaimSlot(key, "A", t0.Add(100*time.Millisecond), slotTTL)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if c.Status != SlotRenewed || c.Epoch.Value() != 1 {
		t.Fatalf("renew = %+v, want RENEWED epoch=1 (bump-on-transfer-only)", c)
	}
	if c.Transferred() {
		t.Fatal("renew must not be a transfer")
	}

	// A foreign claim while A's lease is live: BUSY, reports A as the live owner.
	c, err = s.ClaimSlot(key, "B", t0.Add(200*time.Millisecond), slotTTL)
	if err != nil {
		t.Fatalf("busy claim: %v", err)
	}
	if c.Status != SlotBusy || c.Owner.String() != "A" || c.Epoch.Value() != 1 {
		t.Fatalf("busy claim = %+v, want BUSY owner=A epoch=1", c)
	}
	if c.Granted() {
		t.Fatal("BUSY must not be a grant")
	}

	// After A's lease expires (now past lease_expiry_ns), B takes over: CLAIMED,
	// epoch bumped 1 -> 2 (transfer). This is the rotate-on-takeover that fences A.
	expired := t0.Add(slotTTL + time.Second)
	c, err = s.ClaimSlot(key, "B", expired, slotTTL)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if c.Status != SlotClaimed || c.Owner.String() != "B" || c.Epoch.Value() != 2 {
		t.Fatalf("takeover = %+v, want CLAIMED owner=B epoch=2", c)
	}
	if !c.Transferred() {
		t.Fatal("takeover must be a transfer (fires reconcile)")
	}
}

// The load-bearing property: bump-on-transfer-only means a deposed-then-resumed
// owner carries a STALE epoch and is FENCED by check_owner, while the new owner
// is OWNER. This is the Kleppmann deposed-but-resumed case at the ownership layer.
func TestClaimShardDeposedResumedIsFenced(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	a, _ := s.ClaimSlot(key, "A", t0, slotTTL) // A owns at epoch 1
	expired := t0.Add(slotTTL + time.Second)
	b, _ := s.ClaimSlot(key, "B", expired, slotTTL) // B takes over at epoch 2

	// A resumes after its GC pause still believing it holds epoch 1: FENCED.
	chk, err := s.CheckOwner(key, "A", a.Epoch.String())
	if err != nil {
		t.Fatalf("check A: %v", err)
	}
	if chk != OwnerCheckFenced {
		t.Fatalf("deposed A check = %v, want FENCED", chk)
	}
	// B at the current epoch 2 is OWNER.
	chk, err = s.CheckOwner(key, "B", b.Epoch.String())
	if err != nil {
		t.Fatalf("check B: %v", err)
	}
	if chk != OwnerCheckOwner {
		t.Fatalf("current B check = %v, want OWNER", chk)
	}
}

func TestCheckOwnerStates(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	// UNOWNED: no claim yet.
	if chk, err := s.CheckOwner(key, "A", "1"); err != nil || chk != OwnerCheckUnowned {
		t.Fatalf("fresh slot check = %v/%v, want UNOWNED", chk, err)
	}
	c, _ := s.ClaimSlot(key, "A", t0, slotTTL)
	// OWNER: current owner, current epoch.
	if chk, _ := s.CheckOwner(key, "A", c.Epoch.String()); chk != OwnerCheckOwner {
		t.Fatalf("owner check = %v, want OWNER", chk)
	}
	// FENCED: right owner, wrong epoch.
	if chk, _ := s.CheckOwner(key, "A", "999"); chk != OwnerCheckFenced {
		t.Fatalf("stale-epoch check = %v, want FENCED", chk)
	}
	// FENCED: wrong owner, even at the live epoch.
	if chk, _ := s.CheckOwner(key, "C", c.Epoch.String()); chk != OwnerCheckFenced {
		t.Fatalf("foreign check = %v, want FENCED", chk)
	}
}

func TestClaimSlotDefersToLiveLegacyOwnerDuringRollout(t *testing.T) {
	s, client := newTestStore(t)
	key := slotKey(7)
	legacyKey := legacyOwnershipSlotKey(7)
	t0 := time.Unix(1_700_000_000, 0)
	expires := t0.Add(slotTTL).UnixNano()
	if err := client.HSet(s.ctx(), legacyKey,
		"owner_id", "old-pod", "owner_epoch", "41", "lease_expiry_ns", strconv.FormatInt(expires, 10),
	).Err(); err != nil {
		t.Fatal(err)
	}

	c, err := s.ClaimSlot(key, "new-pod", t0, slotTTL)
	if err != nil {
		t.Fatalf("claim with live legacy owner: %v", err)
	}
	if c.Status != SlotBusy || c.Owner.String() != "old-pod" || c.Epoch.String() != "41" {
		t.Fatalf("claim = %+v, want BUSY on old-pod epoch 41", c)
	}
	if n, err := client.Exists(s.ctx(), key).Result(); err != nil || n != 0 {
		t.Fatalf("new key must not be claimed while legacy owner is live: exists=%d err=%v", n, err)
	}
}

func TestClaimSlotMirrorsLegacyKeyToBlockOldPodsDuringRollout(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(9)
	legacyKey := legacyOwnershipSlotKey(9)
	t0 := time.Unix(1_700_000_000, 0)

	c, err := s.ClaimSlot(key, "new-pod", t0, slotTTL)
	if err != nil || !c.Granted() {
		t.Fatalf("new claim = %+v err=%v, want grant", c, err)
	}
	old, err := s.ClaimSlot(legacyKey, "old-pod", t0.Add(100*time.Millisecond), slotTTL)
	if err != nil {
		t.Fatalf("old claim against mirrored legacy key: %v", err)
	}
	if old.Status != SlotBusy || old.Owner.String() != "new-pod" || old.Epoch.String() != c.Epoch.String() {
		t.Fatalf("old claim = %+v, want BUSY on mirrored new-pod epoch %s", old, c.Epoch.String())
	}
	if chk, err := s.CheckOwner(legacyKey, "old-pod", "1"); err != nil || chk != OwnerCheckFenced {
		t.Fatalf("old owner check = %v/%v, want FENCED by mirrored legacy key", chk, err)
	}
}

func TestMembershipHeartbeatAndLiveMembers(t *testing.T) {
	s, _ := newTestStore(t)
	ttl := 9 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	if err := s.Heartbeat("r1", t0, ttl); err != nil {
		t.Fatalf("heartbeat r1: %v", err)
	}
	if err := s.Heartbeat("r2", t0, ttl); err != nil {
		t.Fatalf("heartbeat r2: %v", err)
	}
	live, err := s.LiveMembers(t0.Add(time.Second))
	if err != nil {
		t.Fatalf("live members: %v", err)
	}
	if len(live) != 2 {
		t.Fatalf("live = %v, want [r1 r2]", live)
	}

	// r2 keeps heartbeating; r1 goes silent. After r1's lease lapses, a heartbeat
	// from r2 at a later now evicts r1 via ZREMRANGEBYSCORE, and LiveMembers shows
	// only r2 — the dead-member age-out the slot-reconcile loop relies on.
	later := t0.Add(ttl + time.Second)
	if err := s.Heartbeat("r2", later, ttl); err != nil {
		t.Fatalf("heartbeat r2 later: %v", err)
	}
	live, err = s.LiveMembers(later)
	if err != nil {
		t.Fatalf("live members later: %v", err)
	}
	if len(live) != 1 || live[0] != "r2" {
		t.Fatalf("live after r1 aged out = %v, want [r2]", live)
	}
}

// ---- batched claims (ClaimSlots / ClaimDueSlots) ----
//
// The slot-reconcile pass and the three workers issue their per-slot Lua steps
// as one pipeline per phase. The tests below pin the two things that make that
// safe: the outcome equals the serial one-round-trip-per-step form (differential
// against a verbatim transcription of it), and the round-trip count is constant
// in the number of slots (a counting redis.Hook).

// tripCounter is a redis.Hook that counts single commands and pipelines, by
// command name, and can stall one pipeline (the stability test's injected
// read-timeout). Commands inside a pipeline are counted under pipeCmds, never as
// singles, so "pipes" is the number of round trips a batch cost.
type tripCounter struct {
	mu       sync.Mutex
	singles  map[string]int
	pipes    int
	pipeCmds map[string]int
	// stallPipe is called before every pipeline with its first command; it may
	// sleep. Nil means no stall.
	stallPipe func(first goredis.Cmder)
}

func newTripCounter() *tripCounter {
	return &tripCounter{singles: map[string]int{}, pipeCmds: map[string]int{}}
}

func (h *tripCounter) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.singles, h.pipes, h.pipeCmds = map[string]int{}, 0, map[string]int{}
}

func (h *tripCounter) counts() (singles, pipes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, n := range h.singles {
		singles += n
	}
	return singles, h.pipes
}

func (h *tripCounter) pipelined(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pipeCmds[name]
}

func (h *tripCounter) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *tripCounter) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		h.mu.Lock()
		h.singles[cmd.Name()]++
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (h *tripCounter) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		h.mu.Lock()
		h.pipes++
		for _, c := range cmds {
			h.pipeCmds[c.Name()]++
		}
		stall := h.stallPipe
		h.mu.Unlock()
		if stall != nil && len(cmds) > 0 {
			stall(cmds[0])
		}
		return next(ctx, cmds)
	}
}

// newCountingStore is newTestStore with a tripCounter attached (counting starts
// after the caller's warm-up, via reset).
func newCountingStore(t *testing.T) (*RedisStore, goredis.UniversalClient, *tripCounter) {
	t.Helper()
	s, client := newTestStore(t)
	hook := newTripCounter()
	client.AddHook(hook)
	return s, client, hook
}

// warmScripts loads every ownership script into the server's cache so a
// NOSCRIPT->EVAL fallback never pollutes a trip count, then wipes the data.
func warmScripts(t *testing.T, s *RedisStore, client goredis.UniversalClient) {
	t.Helper()
	keys := make([]string, subSlots)
	for h := range keys {
		keys[h] = slotKey(h)
	}
	for _, r := range s.ClaimSlots(keys, "warm", time.Unix(1_700_000_000, 0), slotTTL) {
		if r.Err != nil {
			t.Fatalf("warm claim: %v", r.Err)
		}
	}
	for _, sched := range []Schedule{ScheduleLease, ScheduleRetry, ScheduleDue} {
		if d := s.ClaimDueSlots(sched, []int{0}, time.Now(), 1, time.Second); d[0].Err != nil {
			t.Fatalf("warm claim_due: %v", d[0].Err)
		}
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

// idInSlot returns a (nonexistent) subscription id homed in slot h.
func idInSlot(h int) string {
	for i := 0; ; i++ {
		if id := "owed-" + strconv.Itoa(i); slotOf(id) == h {
			return id
		}
	}
}

// serialClaimSlot is a verbatim transcription of the pre-batch per-slot claim —
// reserve_legacy_slot, then claim_shard, then the legacy mirror HSET or the
// HMGET+DEL reservation release, one round trip each — built from the untouched
// single-call primitives. It is the oracle ClaimSlots must equal slot for slot.
func serialClaimSlot(client goredis.UniversalClient, key, replicaID string, now time.Time, ttl time.Duration) (SlotClaim, error) {
	ctx := context.Background()
	args := []any{replicaID, nsArg(now), strconv.FormatInt(ttl.Milliseconds(), 10)}
	claimShard := func() (SlotClaim, error) {
		reply, err := claimShardScript.run(ctx, client, newClaimShardKeys(key), args...)
		if err != nil {
			return SlotClaim{}, err
		}
		return reply.toSlotClaim(), nil
	}
	h, ok := ownershipSlotIndex(key)
	if !ok {
		return claimShard()
	}
	legacyKey := legacyOwnershipSlotKey(h)
	reply, err := reserveLegacySlotScript.run(ctx, client, newReserveLegacySlotKeys(legacyKey), args...)
	if err != nil {
		return SlotClaim{}, err
	}
	reserved, isReserved := reply.(reserveLegacySlotReserved)
	if !isReserved {
		return reply.toSlotClaim(), nil
	}
	release := func() {
		fields, err := client.HMGet(ctx, legacyKey, "owner_id", "lease_expiry_ns").Result()
		if err != nil || len(fields) != 2 || fields[0] != replicaID || parseLeaseUntilNs(fmt.Sprint(fields[1])) != reserved.ExpiryNs {
			return
		}
		_ = client.Del(ctx, legacyKey).Err()
	}
	claim, err := claimShard()
	if err != nil {
		release()
		return SlotClaim{}, err
	}
	if !claim.Granted() {
		release()
		return claim, nil
	}
	if err := client.HSet(ctx, legacyKey,
		"owner_id", claim.Owner.String(),
		"owner_epoch", claim.Epoch.String(),
		"lease_expiry_ns", strconv.FormatInt(claim.ExpiryNs, 10),
	).Err(); err != nil {
		return SlotClaim{}, err
	}
	return claim, nil
}

// seedSlotState writes one of the pre-states a claim can meet: nothing; our own
// live lease (mirrored); a live or expired foreign owner on the new key (mirrored);
// a live pre-rollout owner on the legacy key only; a legacy key another new pod
// reserved but never mirrored. Deterministic in (kind, h).
func seedSlotState(t *testing.T, client goredis.UniversalClient, h, kind int, me string, now time.Time, ttl time.Duration) {
	t.Helper()
	ctx := context.Background()
	live := strconv.FormatInt(now.Add(ttl).UnixNano(), 10)
	expired := strconv.FormatInt(now.Add(-ttl).UnixNano(), 10)
	write := func(key, owner, epoch, exp string) {
		if err := client.HSet(ctx, key, "owner_id", owner, "owner_epoch", epoch, "lease_expiry_ns", exp).Err(); err != nil {
			t.Fatal(err)
		}
	}
	key, legacy := slotKey(h), legacyOwnershipSlotKey(h)
	switch kind % 7 {
	case 0: // absent
	case 1: // self, live, mirrored
		write(key, me, "3", live)
		write(legacy, me, "3", live)
	case 2: // live foreign on the new key, mirrored
		write(key, "rival", "5", live)
		write(legacy, "rival", "5", live)
	case 3: // expired foreign on the new key, mirrored
		write(key, "rival", "5", expired)
		write(legacy, "rival", "5", expired)
	case 4: // a live pre-rollout owner holds only the legacy key
		write(legacy, "old-pod", "41", live)
	case 5: // an expired pre-rollout owner on the legacy key only
		write(legacy, "old-pod", "41", expired)
	case 6: // another new pod reserved the legacy key (epoch 0) but has not mirrored
		write(legacy, "other-new-pod", "0", live)
	}
}

// dumpSlots is the byte-level state the differential compares: every slot's new
// and legacy hash.
func dumpSlots(t *testing.T, client goredis.UniversalClient, hs []int) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	for _, h := range hs {
		for _, k := range []string{slotKey(h), legacyOwnershipSlotKey(h)} {
			m, err := client.HGetAll(context.Background(), k).Result()
			if err != nil {
				t.Fatal(err)
			}
			out[k] = m
		}
	}
	return out
}

// TestClaimSlotsMatchesSerialReference is the commutativity proof obligation
// discharged on live Redis: for the same now, replica and pre-state, the batched
// phases leave every slot's hashes and every per-slot SlotClaim byte-identical to
// the serial reserve -> claim -> mirror/release loop, over two passes with foreign
// interference in between. Slots are independent keys, so cross-slot order never
// mattered; the phase barriers keep the per-slot order.
func TestClaimSlotsMatchesSerialReference(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const me = "rA"
	t0 := time.Unix(1_700_000_000, 0)
	for seed := int64(1); seed <= 4; seed++ {
		rng := rand.New(rand.NewSource(seed))
		hs := rng.Perm(subSlots)[:48]
		kinds := make([]int, len(hs))
		for i := range kinds {
			kinds[i] = rng.Intn(7)
		}
		// Two unguarded keys (not real ownership slot keys) take the one-trip branch.
		keys := make([]string, 0, len(hs)+2)
		for _, h := range hs {
			keys = append(keys, slotKey(h))
		}
		keys = append(keys, "ds:{__ds:7}:ownership:probe:1", "ds:{__ds:9}:ownership:probe:2")
		// Between the passes a rival takes some slots over (live) and some legacy
		// mirrors vanish, so pass 2 meets RENEW, BUSY and takeover cases at once.
		interfere := make([]int, len(hs))
		for i := range interfere {
			interfere[i] = rng.Intn(5)
		}
		t1 := t0.Add(slotTTL / 2)

		type pass struct {
			results []SlotClaimResult
			state   map[string]map[string]string
		}
		run := func(batched bool) [2]pass {
			if err := client.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			for i, h := range hs {
				seedSlotState(t, client, h, kinds[i], me, t0, slotTTL)
			}
			claim := func(now time.Time) []SlotClaimResult {
				if batched {
					return s.ClaimSlots(keys, me, now, slotTTL)
				}
				out := make([]SlotClaimResult, len(keys))
				for i, k := range keys {
					c, err := serialClaimSlot(client, k, me, now, slotTTL)
					out[i] = SlotClaimResult{Claim: c, Err: err}
				}
				return out
			}
			var out [2]pass
			out[0] = pass{claim(t0), dumpSlots(t, client, hs)}
			for i, h := range hs {
				switch interfere[i] {
				case 0:
					seedSlotState(t, client, h, 2, me, t1, slotTTL) // rival live
				case 1:
					if err := client.Del(ctx, legacyOwnershipSlotKey(h)).Err(); err != nil {
						t.Fatal(err)
					}
				}
			}
			out[1] = pass{claim(t1), dumpSlots(t, client, hs)}
			return out
		}
		want, got := run(false), run(true)
		for p := range want {
			for i := range keys {
				w, g := want[p].results[i], got[p].results[i]
				if (w.Err != nil) != (g.Err != nil) || w.Claim != g.Claim {
					t.Fatalf("seed %d pass %d key %s: batched %+v/%v, serial %+v/%v", seed, p, keys[i], g.Claim, g.Err, w.Claim, w.Err)
				}
			}
			if !reflect.DeepEqual(want[p].state, got[p].state) {
				t.Fatalf("seed %d pass %d: slot/legacy hashes differ\nbatched: %v\nserial:  %v", seed, p, got[p].state, want[p].state)
			}
		}
	}
}

// TestClaimSlotsRoundTripsAreConstant pins the whole point: a claim pass costs the
// same three pipelines (reserve, claim, mirror) for 1, 85 or 256 slots, cold or
// steady, with zero single commands; an all-legacy-BUSY pass stops after the
// reserve pipeline; an all-new-key-BUSY pass adds the HMGET probe and DEL release.
func TestClaimSlotsRoundTripsAreConstant(t *testing.T) {
	s, client, hook := newCountingStore(t)
	warmScripts(t, s, client)
	ctx := context.Background()
	t0 := time.Unix(1_700_000_000, 0)
	keysFor := func(n int) []string {
		keys := make([]string, n)
		for h := range keys {
			keys[h] = slotKey(h)
		}
		return keys
	}
	assertTrips := func(label string, wantPipes int) {
		t.Helper()
		singles, pipes := hook.counts()
		if singles != 0 || pipes != wantPipes {
			t.Fatalf("%s: %d single commands and %d pipelines, want 0 and %d", label, singles, pipes, wantPipes)
		}
		hook.reset()
	}
	for _, n := range []int{1, 85, 256} {
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		hook.reset()
		for _, r := range s.ClaimSlots(keysFor(n), "rA", t0, slotTTL) {
			if r.Err != nil || r.Claim.Status != SlotClaimed {
				t.Fatalf("cold claim over %d: %+v/%v", n, r.Claim, r.Err)
			}
		}
		assertTrips(fmt.Sprintf("cold pass over %d slots", n), 3)
		for _, r := range s.ClaimSlots(keysFor(n), "rA", t0.Add(time.Millisecond), slotTTL) {
			if r.Err != nil || r.Claim.Status != SlotRenewed {
				t.Fatalf("steady claim over %d: %+v/%v", n, r.Claim, r.Err)
			}
		}
		assertTrips(fmt.Sprintf("steady pass over %d slots", n), 3)
	}

	// Every legacy key held by a live pre-rollout owner: BUSY after one pipeline.
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	for h := 0; h < 85; h++ {
		seedSlotState(t, client, h, 4, "rA", t0, slotTTL)
	}
	hook.reset()
	for _, r := range s.ClaimSlots(keysFor(85), "rA", t0, slotTTL) {
		if r.Err != nil || r.Claim.Status != SlotBusy || r.Claim.Owner.String() != "old-pod" {
			t.Fatalf("legacy busy: %+v/%v", r.Claim, r.Err)
		}
	}
	assertTrips("all-legacy-BUSY pass", 1)

	// Every new key held by a live rival with no legacy mirror: reserve, claim BUSY,
	// probe the reservation, release it.
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	for h := 0; h < 85; h++ {
		if err := client.HSet(ctx, slotKey(h), "owner_id", "rival", "owner_epoch", "5",
			"lease_expiry_ns", strconv.FormatInt(t0.Add(slotTTL).UnixNano(), 10)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	hook.reset()
	busy := s.ClaimSlots(keysFor(85), "rA", t0, slotTTL)
	assertTrips("all-new-key-BUSY pass", 4)
	for i, r := range busy {
		if r.Err != nil || r.Claim.Status != SlotBusy || r.Claim.Owner.String() != "rival" {
			t.Fatalf("new-key busy: %+v/%v", r.Claim, r.Err)
		}
		if n, _ := client.Exists(ctx, legacyOwnershipSlotKey(i)).Result(); n != 0 {
			t.Fatalf("slot %d: a BUSY claim must release its legacy reservation", i)
		}
	}
}

// TestClaimSlotsHealsNoscript: after a script-cache flush the EVALSHA pipelines
// come back NOSCRIPT; exactly those commands are re-issued as EVAL in one more
// pipeline per phase (+2 round trips for the whole pass, not +1 per slot), every
// slot is still granted, and the next pass is back to three EVALSHA pipelines.
func TestClaimSlotsHealsNoscript(t *testing.T) {
	s, client, hook := newCountingStore(t)
	warmScripts(t, s, client)
	ctx := context.Background()
	t0 := time.Unix(1_700_000_000, 0)
	keys := make([]string, 32)
	for h := range keys {
		keys[h] = slotKey(h)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	hook.reset()
	for _, r := range s.ClaimSlots(keys, "rA", t0, slotTTL) {
		if r.Err != nil || r.Claim.Status != SlotClaimed {
			t.Fatalf("claim after SCRIPT FLUSH: %+v/%v", r.Claim, r.Err)
		}
	}
	singles, pipes := hook.counts()
	if singles != 0 || pipes != 5 || hook.pipelined("eval") != 64 || hook.pipelined("evalsha") != 64 {
		t.Fatalf("healing pass: singles=%d pipes=%d eval=%d evalsha=%d, want 0/5/64/64",
			singles, pipes, hook.pipelined("eval"), hook.pipelined("evalsha"))
	}
	hook.reset()
	for _, r := range s.ClaimSlots(keys, "rA", t0.Add(time.Millisecond), slotTTL) {
		if r.Err != nil || r.Claim.Status != SlotRenewed {
			t.Fatalf("renew after heal: %+v/%v", r.Claim, r.Err)
		}
	}
	singles, pipes = hook.counts()
	if singles != 0 || pipes != 3 || hook.pipelined("eval") != 0 {
		t.Fatalf("healed pass: singles=%d pipes=%d eval=%d, want 0/3/0", singles, pipes, hook.pipelined("eval"))
	}
}

// TestClaimSlotsIsolatesPerSlotErrors: a failing slot (here a WRONGTYPE on its
// legacy or its new key) reports Err for that index only; every other slot in the
// same batch is granted, and a slot whose claim failed after reserving the legacy
// key has that reservation released, exactly as the serial form did.
func TestClaimSlotsIsolatesPerSlotErrors(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1_700_000_000, 0)
	const legacyBroken, newBroken = 3, 11
	if err := client.Set(ctx, legacyOwnershipSlotKey(legacyBroken), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, slotKey(newBroken), "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 16)
	for h := range keys {
		keys[h] = slotKey(h)
	}
	for h, r := range s.ClaimSlots(keys, "rA", t0, slotTTL) {
		switch h {
		case legacyBroken, newBroken:
			if r.Err == nil || r.Claim != (SlotClaim{}) {
				t.Fatalf("slot %d: want an error and a zero claim, got %+v/%v", h, r.Claim, r.Err)
			}
		default:
			if r.Err != nil || r.Claim.Status != SlotClaimed {
				t.Fatalf("slot %d: want CLAIMED, got %+v/%v", h, r.Claim, r.Err)
			}
		}
	}
	if n, _ := client.Exists(ctx, legacyOwnershipSlotKey(newBroken)).Result(); n != 0 {
		t.Fatal("a claim that failed after reserving the legacy key must release the reservation")
	}
	if n, _ := client.Exists(ctx, slotKey(legacyBroken)).Result(); n != 0 {
		t.Fatal("a slot whose reserve failed must not have been claimed")
	}
}

// TestClaimDueSlotsMatchesPerSlot: one claim_due pipeline over every slot returns
// the same ids and leaves the same re-scored ZSETs as the per-slot calls, for
// each of the three schedules, in one round trip for all 256 slots.
func TestClaimDueSlotsMatchesPerSlot(t *testing.T) {
	s, client, hook := newCountingStore(t)
	warmScripts(t, s, client)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))
	now := time.Unix(1_700_000_000, 0)
	slots := make([]int, subSlots)
	for h := range slots {
		slots[h] = h
	}
	members := make([]string, 300)
	for i := range members {
		members[i] = "d-" + strconv.Itoa(rng.Intn(100000))
	}
	for _, sched := range []Schedule{ScheduleLease, ScheduleRetry, ScheduleDue} {
		seed := func() {
			if err := client.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			for i, m := range members {
				score := float64(now.Add(-time.Duration(i) * time.Millisecond).UnixNano())
				if err := client.ZAdd(ctx, sched.zkey(slotOf(m)), goredis.Z{Score: score, Member: m}).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
		dump := func() map[int][]goredis.Z {
			out := map[int][]goredis.Z{}
			for _, h := range slots {
				zs, err := client.ZRangeWithScores(ctx, sched.zkey(h), 0, -1).Result()
				if err != nil {
					t.Fatal(err)
				}
				if len(zs) > 0 {
					out[h] = zs
				}
			}
			return out
		}
		perSlot := func(h int) ([]string, error) {
			switch sched {
			case ScheduleLease:
				return s.DueLeases(h, now, dueClaimLimit, time.Second)
			case ScheduleRetry:
				return s.DueRetries(h, now, dueClaimLimit, time.Second)
			default:
				return s.ClaimDue(h, now, dueClaimLimit, time.Second)
			}
		}
		seed()
		want := make([]DueDrain, subSlots)
		for _, h := range slots {
			ids, err := perSlot(h)
			want[h] = DueDrain{IDs: ids, Err: err}
		}
		wantState := dump()

		seed()
		hook.reset()
		got := s.ClaimDueSlots(sched, slots, now, dueClaimLimit, time.Second)
		if singles, pipes := hook.counts(); singles != 0 || pipes != 1 {
			t.Fatalf("schedule %v: %d singles and %d pipelines for %d slots, want 0 and 1", sched, singles, pipes, subSlots)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("schedule %v: batched drain differs from per-slot\nbatched: %v\nper-slot: %v", sched, got, want)
		}
		if gotState := dump(); !reflect.DeepEqual(wantState, gotState) {
			t.Fatalf("schedule %v: re-scored ZSETs differ\nbatched: %v\nper-slot: %v", sched, gotState, wantState)
		}
	}
}
