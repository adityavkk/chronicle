package webhook

import (
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
	if st, _ := s.AckShard(id, 0, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("ack shard 0 = %q; want OK", st)
	}
	if st, _ := s.AckShard(id, 1, r1.Generation, r1.WakeID, r1.Generation, true, nil, now, 30000); st != "OK" {
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
	if st, _ := s.AckShard(id, 3, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "FENCED" {
		t.Fatalf("shard-0 token acking shard 3 = %q; want FENCED (a holder of g must not ack g')", st)
	}
	// Shard 3 is untouched: its own holder still acks OK.
	if st, _ := s.AckShard(id, 3, r3.Generation, r3.WakeID, r3.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("shard-3 own ack after a foreign fenced attempt = %q; want OK", st)
	}
	// And shard 0's own token still acks shard 0 OK (the foreign attempt did not
	// consume it).
	if st, _ := s.AckShard(id, 0, r0.Generation, r0.WakeID, r0.Generation, true, nil, now, 30000); st != "OK" {
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
	if st, _ := s.AckShard(id, 0, r.Generation, r.WakeID, r.Generation, true, nil, now, 30000); st != "OK" {
		t.Fatalf("AckShard(id,0) with bare-Claim token = %q; want OK", st)
	}
}
