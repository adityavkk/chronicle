package webhook

import (
	"context"
	"testing"
	"time"
)

// These exercise the per-(subId,g) claim-granularity capability against live
// Redis (skipped under -short). They prove the two properties the design (08 §4)
// promises beyond T1: a subscription is claimable by MULTIPLE concurrent holders
// over disjoint shards, and a holder of shard g cannot disturb shard g' (the
// fence is per-(subId,g)).

func pullWakeShardCfg() Config {
	return Config{Type: DispatchPullWake, Pattern: "agents/*", WakeStream: "agents/__wake__", LeaseTTLMs: 30000}
}

// TestShardMultiHolderDisjoint: different shards of one subscription are claimed
// concurrently (no cross-shard BUSY), while a second claimant on the SAME shard
// is BUSY — the contention that collapsed is now per-shard, not per-type.
func TestShardMultiHolderDisjoint(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	const id = "agent-handler"
	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), nil, now); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Hold shard 0.
	r0, err := s.ClaimShard(id, 0, "w-a", "wake-0", now, 30000)
	if err != nil || !r0.Claimed {
		t.Fatalf("claim shard 0 = %+v, %v; want CLAIMED", r0, err)
	}
	// A different worker on shard 0 is BUSY (single-holder WITHIN the shard).
	if busy, _ := s.ClaimShard(id, 0, "w-b", "wake-0b", now, 30000); !busy.Busy {
		t.Fatalf("second claim on shard 0 = %+v; want BUSY", busy)
	}
	// But shard 1 is free — claimed concurrently while shard 0 is held.
	r1, err := s.ClaimShard(id, 1, "w-c", "wake-1", now, 30000)
	if err != nil || !r1.Claimed {
		t.Fatalf("claim shard 1 while shard 0 held = %+v, %v; want CLAIMED", r1, err)
	}
	// Shards mint independent generations (separate fence registers).
	if r1.Generation == 0 && r0.Generation == 0 {
		// both first-claims should have advanced their own register from -1/0
		t.Logf("gen0=%d gen1=%d", r0.Generation, r1.Generation)
	}

	// Both holders can ack their own shard.
	if st, _ := s.AckShardUnscoped(id, 0, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("ack shard 0 = %q; want OK", st)
	}
	if st, _ := s.AckShardUnscoped(id, 1, r1.Generation, r1.WakeID, r1.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("ack shard 1 = %q; want OK", st)
	}
}

// TestShardFenceIsolation: a token minted for shard g is FENCED against any other
// shard (a holder of g cannot ack/release g'), but valid against its own shard.
func TestShardFenceIsolation(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	const id = "agent-handler"
	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), nil, now); err != nil {
		t.Fatalf("create: %v", err)
	}

	r0, err := s.ClaimShard(id, 0, "w-a", "wake-0", now, 30000)
	if err != nil || !r0.Claimed {
		t.Fatalf("claim shard 0: %+v %v", r0, err)
	}
	r3, err := s.ClaimShard(id, 3, "w-b", "wake-3", now, 30000)
	if err != nil || !r3.Claimed {
		t.Fatalf("claim shard 3: %+v %v", r3, err)
	}

	// Shard 0's token applied to shard 3 must be FENCED — independent registers.
	if st, _ := s.AckShardUnscoped(id, 3, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "FENCED" {
		t.Fatalf("shard-0 token acking shard 3 = %q; want FENCED (a holder of g must not ack g')", st)
	}
	// Shard 3 is untouched: its own holder still acks OK.
	if st, _ := s.AckShardUnscoped(id, 3, r3.Generation, r3.WakeID, r3.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("shard-3 own ack after a foreign fenced attempt = %q; want OK", st)
	}
	// And shard 0's own token still acks shard 0 OK (the foreign attempt did not
	// consume it).
	if st, _ := s.AckShardUnscoped(id, 0, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("shard-0 own ack = %q; want OK", st)
	}
}

// TestShardZeroIsByteIdenticalToClaim: ClaimShard(id,0)/AckShard(id,0) and the
// bare Claim/Ack operate on the same keyspace (shard 0 lives in the main hash),
// so a claim via one path is seen (and fenced) by the other.
func TestShardZeroIsByteIdenticalToClaim(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	const id = "agent-handler"
	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), nil, now); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Claim via the bare API...
	r, err := s.Claim(id, "w", "wake-x", now, 30000)
	if err != nil || !r.Claimed {
		t.Fatalf("Claim: %+v %v", r, err)
	}
	// ...a shard-0 claim by another worker sees it as BUSY (same lease).
	if busy, _ := s.ClaimShard(id, 0, "w2", "wake-y", now, 30000); !busy.Busy {
		t.Fatalf("ClaimShard(id,0) after Claim = %+v; want BUSY (same shard-0 lease)", busy)
	}
	// ...and AckShard(id,0) with the bare claim's token acks OK.
	if st, _ := s.AckShardUnscoped(id, 0, r.Generation, r.WakeID, r.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("AckShard(id,0) with bare-Claim token = %q; want OK", st)
	}
}

// TestShardDeleteRecreateFencesStaleAck is the issue #142 regression: a g>0
// shard claim from a deleted incarnation must not be able to ack the recreated
// subscription's links. Delete removes registered shard state; the incarnation
// stamp is the backstop for shard hashes left by older versions.
func TestShardDeleteRecreateFencesStaleAck(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	const id = "agent-handler-delete-recreate"
	const path = "/p"
	initial := "0000000000000000_0000000000000000"
	tail := "0000000000000001_0000000000000000"
	links := []StreamLink{{Path: path, LinkType: LinkExplicit, AckedOffset: initial}}
	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), links, now); err != nil {
		t.Fatalf("create: %v", err)
	}

	claim, err := s.ClaimShard(id, 1, "worker-old", "wake-old", now, 30000)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim shard 1 = %+v, %v; want CLAIMED", claim, err)
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if exists, err := client.Exists(context.Background(), subShardKey(id, 1)).Result(); err != nil || exists != 0 {
		t.Fatalf("delete must remove shard hash: exists=%d err=%v", exists, err)
	}

	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), links, now.Add(time.Second)); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	st, err := s.AckShardUnscoped(id, 1, claim.Generation, claim.WakeID, claim.Generation, true,
		[]Ack{{Stream: path, Offset: tail}}, now.Add(2*time.Second), 30000)
	if err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	if st != "NOSUB" && st != "FENCED" {
		t.Fatalf("stale shard ack after delete/recreate = %q; want NOSUB or FENCED", st)
	}
	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("get recreated sub: ok=%v err=%v", ok, err)
	}
	if len(sub.Links) != 1 || sub.Links[0].AckedOffset != initial {
		t.Fatalf("stale ack moved recreated cursor: links=%+v want offset %q", sub.Links, initial)
	}
}

// TestShardLegacyMissingIncarnationFencesStaleAck seeds the pre-fix final state:
// delete/recreate already happened before incarnation stamps existed, so both the
// recreated parent and the orphan g>0 shard lack incarnation fields. A stale shard
// ack must still fence and leave the recreated cursor untouched.
func TestShardLegacyMissingIncarnationFencesStaleAck(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	const id = "agent-handler-legacy-orphan"
	const path = "/p"
	initial := "0000000000000000_0000000000000000"
	tail := "0000000000000001_0000000000000000"
	links := []StreamLink{{Path: path, LinkType: LinkExplicit, AckedOffset: initial}}
	if _, err := s.CreateOrConfirm(id, pullWakeShardCfg(), links, now); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := client.HDel(ctx, subKey(id), "incarnation").Err(); err != nil {
		t.Fatalf("strip parent incarnation: %v", err)
	}
	if err := client.HSet(ctx, subShardKey(id, 1),
		"generation", "1",
		"wake_id", "wake-legacy",
		"phase", "live",
		"holder", "1",
		"holder_worker", "worker-old",
		"lease_until_ns", nsArg(now.Add(time.Minute)),
	).Err(); err != nil {
		t.Fatalf("seed legacy orphan shard: %v", err)
	}

	st, err := s.AckShardUnscoped(id, 1, 1, "wake-legacy", 1, true,
		[]Ack{{Stream: path, Offset: tail}}, now.Add(time.Second), 30000)
	if err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	if st != "FENCED" {
		t.Fatalf("legacy stale shard ack = %q; want FENCED", st)
	}
	sub, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("get recreated sub: ok=%v err=%v", ok, err)
	}
	if len(sub.Links) != 1 || sub.Links[0].AckedOffset != initial {
		t.Fatalf("legacy stale ack moved recreated cursor: links=%+v want offset %q", sub.Links, initial)
	}
}
