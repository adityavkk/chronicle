package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// differential_producer_test.go generalizes the fixed 10-row producer table
// (TestDifferentialProducerTable, kept below as a covered subset / seed corpus)
// into a rapid generator over (ProducerState, epoch, seq) concentrated near the
// old Lua-number precision boundaries and the int64 ends. For each generated
// case the property
// seeds the producer state directly, drives Store.Append against live Redis
// (which runs the Lua mirror validate_producer in common.lua), and asserts the
// FULL reply tuple (ProducerResult, CurrentEpoch, ExpectedSeq, ReceivedSeq,
// LastSeq), the error class, the persist/no-persist decision (the prod HASH
// equals the oracle's newState, or is byte-unchanged when the oracle returns
// none), and that the tail advanced EXACTLY on Accepted ∧ err==nil — against
// the pure-Go store.ValidateProducer oracle and, under -tags leanoracle, the
// proven Lean model (a third-way check via checkLeanProducer). [INV-PROD-08]
//
// It also carries a focused make_reply ↔ decodeScriptReply int64 round-trip
// sub-property (TestDifferentialReplyInt64RoundTrip) pinning that the script
// reply encoding survives the Lua double round-trip with no truncation across
// the full int64 range. [INV-REPLY-01]

// common.lua compares producer integers as decimal strings and forwards reply
// details as strings, so the Go core, live Lua, and Lean oracle agree across the
// full int64 domain. The former failure boundaries are still sampled so the
// property proves they stay fixed: 10^14 used to render via Lua tostring as
// scientific notation, and 2^53+1 used to collide with 2^53 under tonumber.
const (
	luaDoubleCompareBound = int64(1) << 53 // 2^53 = 9_007_199_254_740_992 (former tonumber comparison limit)
	luaReplyTostringBound = int64(1e14)    // 10^14 (former %.14g tostring rendering limit)
)

// boundaryInt64 draws an int64 concentrated near every precision boundary that
// used to matter: the 10^14 reply-rendering bound, the 2^53 comparison bound,
// and the 2^63 int64 ceiling. It mixes a full-range uniform draw with explicit
// edge
// clusters at each bound ± k plus the small anchors {0, 1, 2} so the
// accept / dup / gap / epoch-bump branches all fire on, just below, and just
// above each boundary rather than relying on uniform luck across the int64 space.
func boundaryInt64() *rapid.Generator[int64] {
	const k = 4
	edges := []int64{0, 1, 2}
	for d := int64(-k); d <= k; d++ {
		edges = append(edges, luaReplyTostringBound+d) // 10^14 ± k (former tostring rendering bound)
		edges = append(edges, luaDoubleCompareBound+d) // 2^53 ± k (the comparison bound)
	}
	// The int64 ceiling is 2^63 - 1 (math.MaxInt64); cluster just below it. d=0
	// is MaxInt64 itself; larger d would overflow, so only the (MaxInt64-k ..
	// MaxInt64) window is meaningful.
	for d := int64(0); d <= k; d++ {
		edges = append(edges, math.MaxInt64-d) // 2^63 - (1..k+1)
	}
	edges = append(edges, math.MinInt64) // the negative extreme
	return rapid.OneOf(
		rapid.Int64(),
		rapid.SampledFrom(edges),
	)
}

// boundaryLastSeq draws a seeded-state LastSeq, biasing toward the boundary
// values AND toward small offsets so the in-order (seq == lastSeq+1), duplicate
// (seq <= lastSeq), and gap (seq > lastSeq+1) branches all fire near 2^53/2^63.
func boundaryLastSeq() *rapid.Generator[int64] {
	return boundaryInt64()
}

// saturatingAdd returns base+d clamped to the int64 range, so a relative draw
// near MaxInt64/MinInt64 can't overflow into a wrapped value that would change
// the tested branch.
func saturatingAdd(base, d int64) int64 {
	switch {
	case d > 0 && base > math.MaxInt64-d:
		return math.MaxInt64
	case d < 0 && base < math.MinInt64-d:
		return math.MinInt64
	default:
		return base + d
	}
}

// producerSeqNear draws a seq positioned RELATIVE to a seeded lastSeq so the
// accept/dup/gap ladder is exercised on purpose (lastSeq ± {0,1,2}), mixed with
// the absolute boundary draws. This is what makes the same-epoch branches fire
// at the boundary instead of almost always landing in the gap branch.
func producerSeqNear(lastSeq int64) *rapid.Generator[int64] {
	rel := make([]int64, 0, 5)
	for d := int64(-2); d <= 2; d++ {
		rel = append(rel, saturatingAdd(lastSeq, d))
	}
	return rapid.OneOf(
		rapid.SampledFrom(rel),
		boundaryInt64(),
	)
}

// producerCase is one generated differential case: the seeded prior state (nil
// = first contact) and the incoming (epoch, seq) request.
type producerCase struct {
	state      *store.ProducerState
	epoch, seq int64
}

// producerCaseGen draws a producerCase. With ~1/4 probability the state is nil
// (first contact); otherwise it seeds a ProducerState whose Epoch/LastSeq are
// drawn near the boundaries, and the incoming epoch is drawn near the seeded
// epoch (so the stale/equal/bump epoch branches all fire) while seq is drawn
// relative to lastSeq (so accept/dup/gap all fire).
func producerCaseGen(now int64) *rapid.Generator[producerCase] {
	return rapid.Custom(func(t *rapid.T) producerCase {
		if rapid.IntRange(0, 3).Draw(t, "firstContact") == 0 {
			return producerCase{
				state: nil,
				epoch: boundaryInt64().Draw(t, "epoch"),
				seq:   boundaryInt64().Draw(t, "seq"),
			}
		}
		sEpoch := boundaryInt64().Draw(t, "stateEpoch")
		sLastSeq := boundaryLastSeq().Draw(t, "stateLastSeq")
		// Incoming epoch near the seeded epoch: stale (<), equal (==), bump (>).
		epoch := rapid.OneOf(
			rapid.SampledFrom(epochsNear(sEpoch)),
			boundaryInt64(),
		).Draw(t, "epoch")
		seq := producerSeqNear(sLastSeq).Draw(t, "seq")
		return producerCase{
			state: &store.ProducerState{Epoch: sEpoch, LastSeq: sLastSeq, LastUpdated: now - 100},
			epoch: epoch,
			seq:   seq,
		}
	})
}

// epochsNear returns the seeded epoch and its immediate neighbors (saturating at
// the int64 ends) so the stale/equal/bump epoch branches are all reachable.
func epochsNear(e int64) []int64 {
	out := make([]int64, 0, 3)
	for d := int64(-1); d <= 1; d++ {
		out = append(out, saturatingAdd(e, d))
	}
	return out
}

// TestDifferentialProducerProperty is the generalized producer differential
// (INV-PROD-08 / INV-DIFF-01). It replaces the fixed 10-row table with a rapid
// generator concentrated near the former 10^14 tostring boundary, the former
// 2^53 Lua-double comparison boundary, and the int64 ends. For every generated
// case, the live Lua mirror and the pure-Go oracle (and, under -tags
// leanoracle, the proven Lean model) must agree on the full reply tuple, the
// error class, the persist decision, and the tail-advances-exactly-on-accept
// property. No unsafe-domain skip remains: the Lua mirror now compares producer
// ints as decimal strings.
//
// Skipped under -short and when Redis is unreachable (newTestStore handles both).
func TestDifferentialProducerProperty(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Unix()
	ctx := context.Background()

	rapid.Check(t, func(t *rapid.T) {
		c := producerCaseGen(now).Draw(t, "case")
		assertProducerDifferential(t, s, ctx, now, c)
	})
}

// boundaryBand classifies an int64 into the band it occupies relative to the
// precision boundaries, so the generator's concentration near 10^14, 2^53, and
// 2^63 is machine-observable (this version of rapid has no coverage-label API).
// The 2^63 band covers both int64 extremes (MaxInt64 and MinInt64).
func boundaryBand(v int64) string {
	switch {
	case absWithin(v, math.MaxInt64, 8) || absWithin(v, math.MinInt64, 8):
		return "near2^63"
	case absWithin(v, luaDoubleCompareBound, 8):
		return "near2^53"
	case absWithin(v, luaReplyTostringBound, 8):
		return "near10^14"
	case v >= 0 && v <= 2:
		return "small"
	default:
		return "interior"
	}
}

// absWithin reports whether v is within d of center (handling the int64 ends
// without overflow). d is assumed small and non-negative.
func absWithin(v, center, d int64) bool {
	lo := int64(math.MinInt64)
	if center > math.MinInt64+d {
		lo = center - d
	}
	hi := int64(math.MaxInt64)
	if center < math.MaxInt64-d {
		hi = center + d
	}
	return v >= lo && v <= hi
}

// TestDifferentialProducerGeneratorCoverage is the coverage assertion required
// by INV-PROD-08: it confirms the (state, epoch, seq) generator demonstrably
// concentrates draws near the former 10^14/2^53 Lua boundaries and the 2^63
// int64 boundary, rather than relying on uniform luck across the int64 space.
// It is a pure generator probe (no Redis), so it runs on every build including
// -short. It also asserts the generator actually produces both first-contact
// (nil state) and seeded-state cases.
func TestDifferentialProducerGeneratorCoverage(t *testing.T) {
	var near14, near53, near63, seeded, firstContact int
	tally := func(v int64) {
		switch boundaryBand(v) {
		case "near10^14":
			near14++
		case "near2^53":
			near53++
		case "near2^63":
			near63++
		}
	}
	rapid.Check(t, func(t *rapid.T) {
		c := producerCaseGen(0).Draw(t, "case")
		if c.state == nil {
			firstContact++
		} else {
			seeded++
		}
		tally(c.epoch)
		tally(c.seq)
		if c.state != nil {
			tally(c.state.Epoch)
			tally(c.state.LastSeq)
		}
	})
	if near14 == 0 {
		t.Error("generator never drew near the 10^14 reply-rendering boundary")
	}
	if near53 == 0 {
		t.Error("generator never drew near the 2^53 comparison boundary")
	}
	if near63 == 0 {
		t.Error("generator never drew near the 2^63 boundary")
	}
	if firstContact == 0 {
		t.Error("generator never drew a first-contact (nil state) case")
	}
	if seeded == 0 {
		t.Error("generator never drew a seeded-state case")
	}
	t.Logf("coverage: near10^14=%d near2^53=%d near2^63=%d firstContact=%d seeded=%d",
		near14, near53, near63, firstContact, seeded)
}

// assertProducerDifferential seeds one producer case directly, drives Append
// against live Redis, and asserts the full reply tuple, error class, persist
// decision, and tail-advances-exactly-on-accept against the Go oracle (and the
// Lean oracle under -tags leanoracle). Shared by the property and the kept
// 10-row corpus table.
func assertProducerDifferential(t require, s *Store, ctx context.Context, now int64, c producerCase) {
	// Oracle: the pure-Go state machine.
	wantResult, wantNewState, wantErr := store.ValidateProducer(c.state, c.epoch, c.seq, now)

	// Third oracle (P1.2, issue #31): the proven Lean model, asserted against
	// the Go core. A no-op unless built with -tags leanoracle. [INV-PROD-08]
	if tt, ok := t.(*testing.T); ok {
		checkLeanProducer(tt, c.state, c.epoch, c.seq, now)
	}

	// Subject: a fresh stream with the producer state seeded directly.
	path := testPath("diffprop")
	if _, _, err := s.Create(path, store.CreateOptions{}); err != nil {
		t.Fatalf("Create(%s): %v", path, err)
	}
	if c.state != nil {
		if err := s.client.HSet(ctx, prodKey(path), "p", encodeProducerState(c.state)).Err(); err != nil {
			t.Fatalf("seed prod state: %v", err)
		}
	}
	epoch, seq := c.epoch, c.seq
	gotResult, gotErr := s.Append(path, []byte("payload"), store.AppendOptions{
		ProducerId: "p", ProducerEpoch: &epoch, ProducerSeq: &seq,
	})

	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("case %+v: err = %v, oracle %v", c, gotErr, wantErr)
	}
	if gotResult.ProducerResult != wantResult.ProducerResult {
		t.Fatalf("case %+v: ProducerResult = %v, oracle %v", c, gotResult.ProducerResult, wantResult.ProducerResult)
	}
	if gotResult.CurrentEpoch != wantResult.CurrentEpoch {
		t.Fatalf("case %+v: CurrentEpoch = %d, oracle %d", c, gotResult.CurrentEpoch, wantResult.CurrentEpoch)
	}
	if gotResult.ExpectedSeq != wantResult.ExpectedSeq {
		t.Fatalf("case %+v: ExpectedSeq = %d, oracle %d", c, gotResult.ExpectedSeq, wantResult.ExpectedSeq)
	}
	if gotResult.ReceivedSeq != wantResult.ReceivedSeq {
		t.Fatalf("case %+v: ReceivedSeq = %d, oracle %d", c, gotResult.ReceivedSeq, wantResult.ReceivedSeq)
	}
	if gotResult.LastSeq != wantResult.LastSeq {
		t.Fatalf("case %+v: LastSeq = %d, oracle %d", c, gotResult.LastSeq, wantResult.LastSeq)
	}

	// Persist/no-persist decision: the prod HASH must equal the oracle's
	// newState, or stay byte-unchanged when the oracle returns none.
	raw, err := s.client.HGet(ctx, prodKey(path), "p").Result()
	var persisted *store.ProducerState
	if err == nil {
		if persisted, err = decodeProducerState(raw); err != nil {
			t.Fatalf("decode persisted state: %v", err)
		}
	}
	switch {
	case wantNewState != nil:
		if persisted == nil || persisted.Epoch != wantNewState.Epoch || persisted.LastSeq != wantNewState.LastSeq {
			t.Fatalf("case %+v: persisted state = %+v, oracle newState %+v", c, persisted, wantNewState)
		}
	case c.state != nil:
		if persisted == nil || persisted.Epoch != c.state.Epoch || persisted.LastSeq != c.state.LastSeq {
			t.Fatalf("case %+v: persisted state = %+v, want untouched %+v", c, persisted, c.state)
		}
	default:
		if persisted != nil {
			t.Fatalf("case %+v: persisted state = %+v, want none", c, persisted)
		}
	}

	// Writes must happen EXACTLY on accepted results.
	tail, err := s.GetCurrentOffset(path)
	if err != nil {
		t.Fatalf("GetCurrentOffset: %v", err)
	}
	wrote := tail.ByteOffset == uint64(len("payload"))
	if wantResult.ProducerResult == store.ProducerResultAccepted && wantErr == nil {
		if !wrote {
			t.Fatalf("case %+v: accepted append did not write", c)
		}
	} else if wrote {
		t.Fatalf("case %+v: non-accepted append wrote data", c)
	}
}

// require is the subset of *testing.T / *rapid.T used by
// assertProducerDifferential, letting the same body drive both the rapid
// property and the kept corpus table. The Lean third-oracle hook only fires
// when the concrete type is *testing.T (the corpus table); the rapid property
// pins Go-vs-Lua, which already pins Lean transitively in the table run.
type require interface {
	Fatalf(format string, args ...any)
}

// TestDifferentialReplyInt64RoundTrip is the focused INV-REPLY-01 sub-property:
// make_reply (common.lua) emits every numeric reply field as a STRING precisely
// so int64 fidelity survives the Lua double round-trip, and decodeScriptReply
// (scripts.go) parses them back with strconv.ParseInt(_, 10, 64). This drives
// the REAL make_reply in Redis with int64 values drawn across the full int64
// range (concentrated near 2^53 and 2^63) and asserts sent == decoded for every
// numeric field.
//
// It is the encoding counterpart to validate_producer's decimal-string compare:
// make_reply never turns producer ints into Lua numbers, so the WIRE round-trip
// is exact across the ENTIRE int64 range (this is why every reply numeric is a
// string, not a Lua number). Skipped under -short and when Redis is unreachable.
func TestDifferentialReplyInt64RoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// A tiny driver chunk appended to the common.lua prelude so it can call the
	// local make_reply. It forwards five int64 strings (ARGV[1..5]) as the
	// producerResult/currentEpoch/expectedSeq/receivedSeq/lastSeq fields and
	// returns the full 9-element reply, exactly as the mutation scripts do.
	// Built via redis.NewScript (Script.Run), never a bare EVAL, per the
	// forbidigo rule in .golangci.yml.
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		t.Fatalf("read common.lua: %v", err)
	}
	const driver = `
return make_reply('OK', '', ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], '0', '0')
`
	replyProbe := redis.NewScript(string(prelude) + "\n" + driver)

	rapid.Check(t, func(t *rapid.T) {
		fields := [5]int64{
			boundaryInt64().Draw(t, "producerResult"),
			boundaryInt64().Draw(t, "currentEpoch"),
			boundaryInt64().Draw(t, "expectedSeq"),
			boundaryInt64().Draw(t, "receivedSeq"),
			boundaryInt64().Draw(t, "lastSeq"),
		}
		args := []any{
			fmt.Sprintf("%d", fields[0]),
			fmt.Sprintf("%d", fields[1]),
			fmt.Sprintf("%d", fields[2]),
			fmt.Sprintf("%d", fields[3]),
			fmt.Sprintf("%d", fields[4]),
		}
		raw, err := replyProbe.Run(ctx, s.client, nil, args...).Result()
		if err != nil {
			t.Fatalf("make_reply probe run: %v", err)
		}
		decoded, err := decodeScriptReply(raw)
		if err != nil {
			t.Fatalf("decodeScriptReply(%v): %v", raw, err)
		}
		got := [5]int64{
			decoded.ProducerRes, decoded.CurrentEpoch,
			decoded.ExpectedSeq, decoded.ReceivedSeq, decoded.LastSeq,
		}
		for i := range fields {
			if got[i] != fields[i] {
				t.Fatalf("INV-REPLY-01: field %d sent=%d decoded=%d (full reply %v)", i, fields[i], got[i], raw)
			}
		}
	})
}

// TestDifferentialProducerLargeIntegerBoundaries pins the former producer Lua
// number hazards as fixed: SEQ_GAP details at 10^14 stay decimal strings, and
// the full differential property covers the 2^53 comparison boundary. Skipped
// under -short and when Redis is unreachable.
func TestDifferentialProducerLargeIntegerBoundaries(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Unix()

	// firstContactGap drives a first-contact append at the given seq (state nil,
	// so the oracle returns ErrProducerSeqGap with ExpectedSeq 0, ReceivedSeq
	// seq) and returns the oracle ReceivedSeq plus the live-Redis result/error.
	firstContactGap := func(seq int64) (wantReceived int64, got store.AppendResult, redisErr error) {
		wantRes, _, wantErr := store.ValidateProducer(nil, 0, seq, now)
		if !errors.Is(wantErr, store.ErrProducerSeqGap) {
			t.Fatalf("seq %d: oracle did not return a gap: %v", seq, wantErr)
		}
		path := testPath("tostringlb")
		if _, _, err := s.Create(path, store.CreateOptions{}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		e, q := int64(0), seq
		got, gotErr := s.Append(path, []byte("payload"), store.AppendOptions{
			ProducerId: "p", ProducerEpoch: &e, ProducerSeq: &q,
		})
		return wantRes.ReceivedSeq, got, gotErr
	}

	t.Run("10^14-1 round-trips cleanly on both backends", func(t *testing.T) {
		seq := luaReplyTostringBound - 1
		wantReceived, got, gotErr := firstContactGap(seq)
		if !errors.Is(gotErr, store.ErrProducerSeqGap) {
			t.Fatalf("seq %d (10^14-1) should round-trip to a clean gap, got: %v", seq, gotErr)
		}
		if wantReceived != seq || got.ReceivedSeq != seq {
			t.Fatalf("ReceivedSeq oracle=%d redis=%d, want %d", wantReceived, got.ReceivedSeq, seq)
		}
	})

	t.Run("10^14 no longer renders as scientific notation", func(t *testing.T) {
		seq := luaReplyTostringBound
		wantReceived, got, gotErr := firstContactGap(seq)
		if !errors.Is(gotErr, store.ErrProducerSeqGap) {
			t.Fatalf("seq %d should round-trip to a clean gap, got: %v", seq, gotErr)
		}
		if wantReceived != seq || got.ReceivedSeq != seq {
			t.Fatalf("ReceivedSeq oracle=%d redis=%d, want %d", wantReceived, got.ReceivedSeq, seq)
		}
	})
}
