package redis

import (
	"context"
	"errors"
	"math"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// write_fence_differential_test.go is the live-Lua leg of the write-fence rung
// (#183, INV-FENCE-07): the Go oracles store.EvaluateWriteFence and
// store.FenceAuthority are held to the common.lua mirrors evaluate_write_fence
// and fence_auth over generated inputs, and the rung is then exercised end to
// end through append.lua / close.lua with the meta hash and marker seeded
// directly. Skipped under -short and when Redis is unreachable.

// preludeScript builds a test-only script from the common.lua prelude plus a
// driver body so a local helper can be called directly. Built via
// redis.NewScript (Script.Run), never a bare EVAL, per the forbidigo rule.
func preludeScript(t *testing.T, driver string) *redis.Script {
	t.Helper()
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		t.Fatalf("read common.lua: %v", err)
	}
	return redis.NewScript(string(prelude) + "\n" + driver)
}

// nulFreeBytes draws arbitrary bytes without NUL, the only byte the authority
// encoding reserves (it separates the subscription id from the incarnation).
func nulFreeBytes() *rapid.Generator[[]byte] {
	return rapid.SliceOf(rapid.Byte().Filter(func(b byte) bool { return b != 0 }))
}

// TestFenceAuthorityDifferential pins that fence_auth recovers exactly
// store.FenceAuthority from the marker key appendFenceKey builds, over
// adversarial stream paths (a literal ":append-fence:" inside the path, hash-tag
// braces, percent signs, unicode, NULs) and arbitrary NUL-free subscription ids
// and incarnations [INV-FENCE-07]. The key suffix and the seal field of one
// authority are therefore the same string on both sides.
func TestFenceAuthorityDifferential(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	probe := preludeScript(t, "return fence_auth(ARGV[1]) or ''")

	adversarial := []string{
		"/v1/s", "/a:append-fence:AAAA:0", "/a:append-fence:AAAA:0/b", "/a}:append-fence:AAAA:0",
		"/{b}/c", "/100%/x", "/ünïcode/路径", "/:append-fence:", "/x\x00y", "",
	}
	rapid.Check(t, func(t *rapid.T) {
		path := rapid.OneOf(rapid.SampledFrom(adversarial), rapid.String()).Draw(t, "path")
		fence := auth.AppendFence{
			SubscriptionID:          string(nulFreeBytes().Draw(t, "sub")),
			SubscriptionIncarnation: string(nulFreeBytes().Draw(t, "incarnation")),
			Shard:                   rapid.IntRange(0, 5).Draw(t, "shard"),
		}
		got, err := probe.Run(ctx, s.client, nil, appendFenceKey(path, fence)).Text()
		if err != nil {
			t.Fatalf("fence_auth probe: %v", err)
		}
		if want := store.FenceAuthority(fence); got != want {
			t.Fatalf("INV-FENCE-07 DIVERGENCE: fence_auth(%q) = %q, Go FenceAuthority = %q", appendFenceKey(path, fence), got, want)
		}
	})
}

// exactNs draws a nanosecond timestamp that is a multiple of 1024, so it is
// exactly representable as a Lua double anywhere in the int64 range: the lease
// comparison is the one place the mirror goes through tonumber (as the lease
// check always has), and the differential must not fail on the ~256 ns double
// rounding that real timestamps carry.
func exactNs() *rapid.Generator[int64] {
	const step = int64(1024)
	return rapid.Map(rapid.Int64Range(math.MinInt64/step+1, math.MaxInt64/step-1), func(k int64) int64 {
		return k * step
	})
}

// mostly draws true with probability 3/4, for the inputs a rule needs before
// it can fire at all — the fenced class, the fenced stream, producer headers
// on the open class — so no rule waits on a run of coin flips.
func mostly(t *rapid.T, label string) bool {
	return rapid.IntRange(0, 3).Draw(t, label) != 0
}

// other draws a value from choices that differs from v.
func other(t *rapid.T, label string, choices []string, v string) string {
	return rapid.SampledFrom(choices).Filter(func(c string) bool { return c != v }).Draw(t, label)
}

// writeFenceDiffInputGen draws a WriteFenceInput stratified over the rung's
// outcomes, so rules 3 and 4 and the fenced-class accept path — reachable only
// through the fence's own live, unsealed marker with producer headers at
// epoch == generation — fire at the default check budget rather than by
// coincidence (TestWriteFenceDiffGeneratorCoverage pins it). The fenced class
// starts from that accept shape and disturbs at most one input: the seal (at
// the generation, next to it, or at a boundary), one marker field (absent,
// revoked, generation, wake, holder, a lapsed lease, incarnation), or the
// producer headers (absent, or the epoch off the generation); one arm draws
// marker, seal and producer independently so unrelated combinations stay
// covered. Every generation and epoch is drawn at and around the 10^14 / 2^53
// / 2^63 boundaries (boundaryInt64) and the lease exactly at, just below and
// just above now. The open class draws its bound generation zero, positive or
// at a boundary.
func writeFenceDiffInputGen() *rapid.Generator[store.WriteFenceInput] {
	return rapid.Custom(func(t *rapid.T) store.WriteFenceInput {
		gen := boundaryInt64().Draw(t, "generation")
		near := func(label string) int64 {
			return rapid.OneOf(rapid.SampledFrom(epochsNear(gen)), boundaryInt64()).Draw(t, label)
		}
		now := exactNs().Draw(t, "now")
		ids := []string{"w_a", "w_b"}
		holders := []string{"worker-a", "worker-b", "wake:w_a"}
		incs := []string{"inc-1", "inc-2", ""}
		in := store.WriteFenceInput{
			StreamIncarnation: rapid.SampledFrom(incs).Draw(t, "incarnation"),
			NowNs:             now,
		}
		if !mostly(t, "fencedClass") {
			in.StreamFenced = mostly(t, "streamFenced")
			in.HasProducer = mostly(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
			in.BoundGeneration = rapid.OneOf(
				rapid.Just(int64(0)), rapid.Int64Range(1, math.MaxInt64), boundaryInt64(),
			).Draw(t, "boundGeneration")
			return in
		}

		fence := auth.AppendFence{
			SubscriptionID:          "sub",
			SubscriptionIncarnation: "sub-inc",
			Generation:              gen,
			WakeID:                  rapid.SampledFrom(ids).Draw(t, "wake"),
			Holder:                  rapid.SampledFrom(holders).Draw(t, "holder"),
		}
		in.Fence = &fence
		in.StreamFenced = mostly(t, "streamFenced")
		in.HasProducer, in.ProducerEpoch = true, gen
		in.BoundGeneration = near("boundGeneration") // ignored on the fenced class; drawn to prove it
		in.Marker = store.WriteFenceMarker{
			Present: true, State: store.WriteFenceMarkerLive, Generation: gen, WakeID: fence.WakeID,
			Holder: fence.Holder, LeaseUntilNs: now + 1024, StreamIncarnation: in.StreamIncarnation,
		}
		switch rapid.SampledFrom([]string{"none", "seal", "marker", "producer", "unrelated"}).Draw(t, "disturb") {
		case "seal":
			in.Seal = store.WriteFenceSeal{Present: true, WakeID: "w_s", Generation: rapid.OneOf(
				rapid.Just(gen), rapid.SampledFrom(epochsNear(gen)), boundaryInt64(),
			).Draw(t, "sealGeneration")}
		case "marker":
			m := &in.Marker
			switch rapid.SampledFrom([]string{"absent", "state", "generation", "wake", "holder", "lease", "incarnation"}).Draw(t, "markerField") {
			case "absent":
				*m = store.WriteFenceMarker{}
			case "state":
				m.State = store.WriteFenceMarkerRevoked
			case "generation":
				m.Generation = near("markerGeneration")
			case "wake":
				m.WakeID = other(t, "markerWake", ids, m.WakeID)
			case "holder":
				m.Holder = other(t, "markerHolder", holders, m.Holder)
			case "lease":
				m.LeaseUntilNs = rapid.OneOf(rapid.SampledFrom([]int64{now - 1024, now}), exactNs()).Draw(t, "lease")
			case "incarnation":
				m.StreamIncarnation = other(t, "markerIncarnation", incs, m.StreamIncarnation)
			}
		case "producer":
			in.HasProducer = rapid.Bool().Draw(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
		case "unrelated":
			in.HasProducer = rapid.Bool().Draw(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
			in.Marker = store.WriteFenceMarker{}
			if rapid.Bool().Draw(t, "markerPresent") {
				in.Marker = store.WriteFenceMarker{
					Present:           true,
					State:             rapid.SampledFrom([]string{store.WriteFenceMarkerLive, store.WriteFenceMarkerRevoked}).Draw(t, "state"),
					Generation:        near("markerGeneration"),
					WakeID:            rapid.SampledFrom(ids).Draw(t, "markerWake"),
					Holder:            rapid.SampledFrom(holders).Draw(t, "markerHolder"),
					LeaseUntilNs:      rapid.OneOf(rapid.SampledFrom([]int64{now - 1024, now, now + 1024}), exactNs()).Draw(t, "lease"),
					StreamIncarnation: rapid.SampledFrom(incs).Draw(t, "markerIncarnation"),
				}
			}
			if rapid.Bool().Draw(t, "sealPresent") {
				in.Seal = store.WriteFenceSeal{Present: true, Generation: near("sealGeneration"), WakeID: "w_s"}
			}
		}
		return in
	})
}

// writeFenceOutcomeLabel names the bucket a generated input landed in: the
// refusal reason, or which accept path — the open class; the fenced class on a
// fenced stream, the only route through rules 3 and 4; or the fenced class on
// a stream that never opted in.
func writeFenceOutcomeLabel(in store.WriteFenceInput, out store.WriteFenceOutcome) string {
	switch {
	case out.Reason != store.FenceNone:
		return string(out.Reason)
	case in.Fence == nil:
		return "accept-open"
	case in.StreamFenced:
		return "accept-fenced"
	default:
		return "accept-unfenced-stream"
	}
}

// TestWriteFenceDiffGeneratorCoverage confirms writeFenceDiffInputGen reaches
// every outcome the differential must gate — each refusal and each accept
// path, including the fenced class on a fenced stream, the only route through
// rules 3 and 4 (the live int_cmp on the producer epoch) — rather than by
// uniform luck. It samples a FIXED number of deterministic examples (via
// Generator.Example) so the assertion is independent of the rapid check
// budget, which shrinks under -short. Pure probe (no Redis), runs under -short.
func TestWriteFenceDiffGeneratorCoverage(t *testing.T) {
	const samples = 500
	gen := writeFenceDiffInputGen()
	seen := map[string]int{}
	for i := 0; i < samples; i++ {
		in := gen.Example(i)
		seen[writeFenceOutcomeLabel(in, store.EvaluateWriteFence(in))]++
	}
	for _, want := range []string{
		string(store.FenceSealed), string(store.FenceMarker), string(store.FenceProducerRequired),
		string(store.FenceEpoch), string(store.FenceBound), "accept-open", "accept-fenced", "accept-unfenced-stream",
	} {
		if seen[want] == 0 {
			t.Errorf("generator never reached %s", want)
		}
	}
	t.Logf("coverage over %d examples: %v", samples, seen)
}

// writeFenceDriver calls evaluate_write_fence with every input passed as ARGV
// (the marker row rebuilt from ARGV[8..13] when ARGV[7] is '1') and returns
// the {reason, generation, holder} triple, as fence_reply would carry it.
const writeFenceDriver = `
local fence_row = nil
if ARGV[7] == '1' then
  fence_row = { ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13] }
end
local reason, gen, holder = evaluate_write_fence(
  ARGV[1] == '1', ARGV[2], ARGV[3] == '1', ARGV[4], ARGV[5], ARGV[6],
  fence_row, ARGV[14] == '1', ARGV[15], tonumber(ARGV[16]),
  ARGV[17] == '1', ARGV[18], ARGV[19])
return { reason, gen, holder }
`

func writeFenceDriverArgs(in store.WriteFenceInput) []any {
	i64 := func(v int64) string { return strconv.FormatInt(v, 10) }
	fGen, fWake, fHolder := "0", "", ""
	if in.Fence != nil {
		fGen, fWake, fHolder = i64(in.Fence.Generation), in.Fence.WakeID, in.Fence.Holder
	}
	m := in.Marker
	return []any{
		boolArg(in.StreamFenced), in.StreamIncarnation, boolArg(in.Fence != nil), fGen, fWake, fHolder,
		boolArg(m.Present), m.State, i64(m.Generation), m.WakeID, m.Holder, i64(m.LeaseUntilNs), m.StreamIncarnation,
		boolArg(in.Seal.Present), i64(in.Seal.Generation), i64(in.NowNs),
		boolArg(in.HasProducer), i64(in.ProducerEpoch), i64(in.BoundGeneration),
	}
}

// TestWriteFenceDifferential pins store.EvaluateWriteFence ≡ evaluate_write_fence
// (INV-FENCE-07): for every generated input the live Lua mirror must return the
// same (reason, generation, holder) triple as the Go oracle, and an independent
// re-check derives the rule that must have fired from the input alone, so the
// two cannot agree by sharing a bug.
func TestWriteFenceDifferential(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	probe := preludeScript(t, writeFenceDriver)

	rapid.Check(t, func(t *rapid.T) {
		in := writeFenceDiffInputGen().Draw(t, "in")
		want := store.EvaluateWriteFence(in)

		raw, err := probe.Run(ctx, s.client, nil, writeFenceDriverArgs(in)...).Result()
		if err != nil {
			t.Fatalf("evaluate_write_fence probe: %v\n  in=%+v", err, in)
		}
		reply, err := toStrings(raw)
		if err != nil || len(reply) != 3 {
			t.Fatalf("malformed reply %v (%v)", raw, err)
		}
		got := store.WriteFenceOutcome{Reason: store.FenceReason(reply[0]), Holder: reply[2]}
		if got.Generation, err = strconv.ParseInt(reply[1], 10, 64); err != nil {
			t.Fatalf("INV-REPLY-01: generation %q is not a decimal int64: %v", reply[1], err)
		}
		if got != want {
			t.Fatalf("INV-FENCE-07 DIVERGENCE: live=%+v Go=%+v\n  in=%+v", got, want, in)
		}
		assertWriteFenceSemantics(t, in, got)
	})
}

// assertWriteFenceSemantics re-derives the rule that must have fired from the
// input alone (rule order: sealed, marker, producer_required, epoch; bound on
// the open class), independently of EvaluateWriteFence's code path.
func assertWriteFenceSemantics(t *rapid.T, in store.WriteFenceInput, out store.WriteFenceOutcome) {
	var want store.FenceReason
	if in.Fence == nil {
		if in.StreamFenced && in.HasProducer && in.BoundGeneration > 0 {
			want = store.FenceBound
		}
	} else {
		f, m := in.Fence, in.Marker
		markerOK := m.Present && m.State == store.WriteFenceMarkerLive && m.Generation == f.Generation &&
			m.WakeID == f.WakeID && m.Holder == f.Holder && m.LeaseUntilNs > in.NowNs &&
			m.StreamIncarnation == in.StreamIncarnation
		switch {
		case in.StreamFenced && in.Seal.Present && f.Generation <= in.Seal.Generation:
			want = store.FenceSealed
		case !markerOK:
			want = store.FenceMarker
		case in.StreamFenced && !in.HasProducer:
			want = store.FenceProducerRequired
		case in.StreamFenced && in.ProducerEpoch != f.Generation:
			want = store.FenceEpoch
		}
	}
	if out.Reason != want {
		t.Fatalf("semantic re-check: reason %q, derived %q\n  in=%+v", out.Reason, want, in)
	}
}

// fenceRungCase is one seeded end-to-end case for TestAppendLuaFenceRungEndToEnd.
type fenceRungCase struct {
	name      string
	fenced    bool                    // create the stream write-fenced
	fence     *auth.AppendFence       // nil = open class
	marker    *store.WriteFenceMarker // seeded raw for fence's authority; nil = none
	seal      string                  // raw wfseal:<auth> value for fence's authority; "" = none
	bound     map[string]string       // raw wfbind:<producer> seeds
	prod      map[string]string       // raw prod HASH seeds
	op        string                  // "append" | "close-producer" | "close-fenced"
	producer  bool
	epoch     int64
	wantErr   error
	wantFence store.WriteFenceOutcome
}

// TestAppendLuaFenceRungEndToEnd drives the fence rung through the real
// append.lua / close.lua with the meta hash (wf, wfseal:, wfbind:), the prod
// hash and the marker seeded directly, and asserts the error class, the
// disclosure triple, that a rejection leaves the tail and accessedAtNs
// untouched (the rung precedes the sliding-TTL touch), and that an accepted
// fenced-class write records wfLastOff and binds its producer id.
func TestAppendLuaFenceRungEndToEnd(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	f8 := claimFence(8, "w_8", "worker-a")
	live := func(mutate func(m *store.WriteFenceMarker)) *store.WriteFenceMarker {
		m := store.WriteFenceMarker{
			Present: true, State: store.WriteFenceMarkerLive, Generation: 8, WakeID: "w_8",
			Holder: "worker-a", LeaseUntilNs: f8.LeaseUntilNs,
		}
		if mutate != nil {
			mutate(&m)
		}
		return &m
	}
	accepted := store.WriteFenceOutcome{Generation: 8, Holder: "worker-a"}

	cases := []fenceRungCase{
		{
			name:   "fenced stream, live marker, epoch equals generation: accepted and bound",
			fenced: true, fence: &f8, marker: live(nil), op: "append", producer: true, epoch: 8,
			wantFence: accepted,
		},
		{
			name:   "fenced stream, fenced class without producer headers: producer_required",
			fenced: true, fence: &f8, marker: live(nil), op: "append",
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceProducerRequired, Generation: 8, Holder: "worker-a"},
		},
		{
			name:   "fenced stream, epoch above the generation: epoch",
			fenced: true, fence: &f8, marker: live(nil), op: "append", producer: true, epoch: 9,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceEpoch, Generation: 8, Holder: "worker-a"},
		},
		{
			name:   "fenced stream, sealed generation with its live marker: sealed",
			fenced: true, fence: &f8, marker: live(nil), seal: "8:w_8:0000000000000000_0000000000000009",
			op: "append", producer: true, epoch: 8,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceSealed, Generation: 8, Holder: "worker-a"},
		},
		{
			name:   "fenced stream, sealed generation behind a successor marker: sealed discloses the successor",
			fenced: true, fence: &f8, seal: "8:w_8:0000000000000000_0000000000000009",
			marker: live(func(m *store.WriteFenceMarker) { m.Generation, m.WakeID, m.Holder = 9, "w_9", "worker-b" }),
			op:     "append", producer: true, epoch: 8,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceSealed, Generation: 9, Holder: "worker-b"},
		},
		{
			name:   "fenced stream, absent marker: marker with nothing disclosed",
			fenced: true, fence: &f8, op: "append", producer: true, epoch: 8,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceMarker},
		},
		{
			name:   "fenced stream, revoked marker: marker without a holder",
			fenced: true, fence: &f8, op: "append", producer: true, epoch: 8,
			marker:  live(func(m *store.WriteFenceMarker) { m.State, m.LeaseUntilNs = store.WriteFenceMarkerRevoked, 0 }),
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceMarker, Generation: 8},
		},
		{
			name:   "fenced stream, lapsed lease: marker without a holder",
			fenced: true, fence: &f8, op: "append", producer: true, epoch: 8,
			marker:  live(func(m *store.WriteFenceMarker) { m.LeaseUntilNs = time.Now().Add(-time.Second).UnixNano() }),
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceMarker, Generation: 8},
		},
		{
			name:   "fenced stream, open class naming a bound producer: bound",
			fenced: true, bound: map[string]string{"p": "8"}, op: "append", producer: true, epoch: 9,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceBound, Generation: 8},
		},
		{
			name:   "fenced stream, open class replaying a bound producer's accepted tuple: bound, not duplicate",
			fenced: true, bound: map[string]string{"p": "8"}, prod: map[string]string{"p": "8:0:1"},
			op: "append", producer: true, epoch: 8,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceBound, Generation: 8},
		},
		{
			name:   "fenced stream, open class with an unbound producer establishes its epoch",
			fenced: true, bound: map[string]string{"q": "8"}, op: "append", producer: true, epoch: 3,
		},
		{
			name:   "fenced stream, open class without producer headers: accepted",
			fenced: true, bound: map[string]string{"p": "8"}, op: "append",
		},
		{
			name:  "unfenced stream, fenced class without producer headers keeps today's marker check",
			fence: &f8, marker: live(nil), seal: "9:w_9:0000000000000000_0000000000000009", op: "append",
			wantFence: accepted,
		},
		{
			name:  "unfenced stream, open class ignores bindings",
			bound: map[string]string{"p": "8"}, op: "append", producer: true, epoch: 1,
		},
		{
			name:   "close with producer, fenced stream, epoch below the generation: epoch",
			fenced: true, fence: &f8, marker: live(nil), op: "close-producer", producer: true, epoch: 7,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceEpoch, Generation: 8, Holder: "worker-a"},
		},
		{
			name:   "close with producer, fenced stream, sealed: sealed",
			fenced: true, fence: &f8, marker: live(nil), seal: "8:w_8:0000000000000000_0000000000000000",
			op: "close-producer", producer: true, epoch: 8,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceSealed, Generation: 8, Holder: "worker-a"},
		},
		{
			name:   "close with producer, fenced stream, accepted: closed and bound",
			fenced: true, fence: &f8, marker: live(nil), op: "close-producer", producer: true, epoch: 8,
			wantFence: accepted,
		},
		{
			name:   "close with producer, fenced stream, open class naming a bound producer: bound",
			fenced: true, bound: map[string]string{"p": "8"}, op: "close-producer", producer: true, epoch: 9,
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceBound, Generation: 8},
		},
		{
			name:   "close-only, fenced stream, fenced class without producer headers: producer_required",
			fenced: true, fence: &f8, marker: live(nil), op: "close-fenced",
			wantErr: store.ErrAppendFenced, wantFence: store.WriteFenceOutcome{Reason: store.FenceProducerRequired, Generation: 8, Holder: "worker-a"},
		},
		{
			name:  "close-only, unfenced stream, live marker: closed",
			fence: &f8, marker: live(nil), op: "close-fenced",
			wantFence: accepted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := testPath("fence-rung")
			meta := mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: tc.fenced})
			mustAppend(t, s, path, []byte(`{"seed":true}`), store.AppendOptions{ContentType: "application/json"})
			seedFenceRung(t, ctx, path, meta.Incarnation, tc)

			before := metaSnapshot(t, ctx, path)
			got, gotErr := runFenceRungOp(t, s, path, tc)
			if !errors.Is(gotErr, tc.wantErr) {
				t.Fatalf("err = %v, want %v", gotErr, tc.wantErr)
			}
			after := metaSnapshot(t, ctx, path)
			if tc.wantErr != nil {
				if got != tc.wantFence {
					t.Fatalf("disclosure = %+v, want %+v", got, tc.wantFence)
				}
				if after[fTail] != before[fTail] || after[fAccessedAt] != before[fAccessedAt] {
					t.Fatalf("rejection mutated the stream: tail %q->%q accessedAtNs %q->%q",
						before[fTail], after[fTail], before[fAccessedAt], after[fAccessedAt])
				}
				if _, ok := after[fLastFencedOff]; ok {
					t.Fatal("rejection wrote wfLastOff")
				}
				return
			}
			if got != (store.WriteFenceOutcome{}) {
				t.Fatalf("accepted write carried a disclosure: %+v", got)
			}
			bindsExpected := tc.fenced && tc.fence != nil && tc.producer
			if _, ok := after[fBindPrefix+"p"]; ok != bindsExpected && tc.bound["p"] == "" {
				t.Fatalf("wfbind:p present=%t, want %t", ok, bindsExpected)
			}
			if bindsExpected {
				if after[fBindPrefix+"p"] != "8" || after[fLastFencedOff] != after[fTail] {
					t.Fatalf("accepted fenced write: wfbind:p=%q wfLastOff=%q tail=%q, want 8 / tail",
						after[fBindPrefix+"p"], after[fLastFencedOff], after[fTail])
				}
			} else if _, ok := after[fLastFencedOff]; ok {
				t.Fatal("open-class or unfenced write recorded wfLastOff")
			}
		})
	}
}

// seedFenceRung writes the case's meta fields, prod state and marker directly.
func seedFenceRung(t *testing.T, ctx context.Context, path, incarnation string, tc fenceRungCase) {
	t.Helper()
	if tc.fence != nil {
		if tc.seal != "" {
			if err := testClient.HSet(ctx, metaKey(path), fSealPrefix+store.FenceAuthority(*tc.fence), tc.seal).Err(); err != nil {
				t.Fatalf("seed seal: %v", err)
			}
		}
		if m := tc.marker; m != nil {
			if err := testClient.HSet(ctx, appendFenceKey(path, *tc.fence),
				"state", m.State, "generation", m.Generation, "wake_id", m.WakeID, "holder", m.Holder,
				"lease_until_ns", m.LeaseUntilNs, "stream_incarnation", incarnation).Err(); err != nil {
				t.Fatalf("seed marker: %v", err)
			}
		}
	}
	for pid, gen := range tc.bound {
		if err := testClient.HSet(ctx, metaKey(path), fBindPrefix+pid, gen).Err(); err != nil {
			t.Fatalf("seed binding: %v", err)
		}
	}
	for pid, state := range tc.prod {
		if err := testClient.HSet(ctx, prodKey(path), pid, state).Err(); err != nil {
			t.Fatalf("seed prod: %v", err)
		}
	}
}

func metaSnapshot(t *testing.T, ctx context.Context, path string) map[string]string {
	t.Helper()
	fields, err := testClient.HGetAll(ctx, metaKey(path)).Result()
	if err != nil {
		t.Fatalf("HGETALL meta: %v", err)
	}
	return fields
}

// runFenceRungOp runs the case's mutation and returns its disclosure and error.
func runFenceRungOp(t *testing.T, s *Store, path string, tc fenceRungCase) (store.WriteFenceOutcome, error) {
	t.Helper()
	epoch, seq := tc.epoch, int64(0)
	switch tc.op {
	case "append":
		opts := store.AppendOptions{ContentType: "application/json", Fence: tc.fence}
		if tc.producer {
			opts.ProducerId, opts.ProducerEpoch, opts.ProducerSeq = "p", &epoch, &seq
		}
		res, err := s.Append(path, []byte(`{"value":1}`), opts)
		return store.WriteFenceOutcome{Reason: res.FenceReason, Generation: res.FenceGeneration, Holder: res.FenceHolder}, err
	case "close-producer":
		res, err := s.CloseStreamWithProducer(path, store.CloseProducerOptions{
			ProducerId: "p", ProducerEpoch: epoch, ProducerSeq: seq, Fence: tc.fence,
		})
		if res == nil {
			t.Fatalf("CloseStreamWithProducer returned a nil result with %v", err)
		}
		return store.WriteFenceOutcome{Reason: res.FenceReason, Generation: res.FenceGeneration, Holder: res.FenceHolder}, err
	case "close-fenced":
		res, err := s.CloseStreamFenced(path, *tc.fence)
		if res == nil {
			t.Fatalf("CloseStreamFenced returned a nil result with %v", err)
		}
		return store.WriteFenceOutcome{Reason: res.FenceReason, Generation: res.FenceGeneration, Holder: res.FenceHolder}, err
	default:
		t.Fatalf("unknown op %q", tc.op)
		return store.WriteFenceOutcome{}, nil
	}
}

// TestProducerHashEncodingUnchangedOnFencedStream pins that the producer HASH
// keeps its 3-segment "epoch:lastSeq:lastUpdated" encoding on a write-fenced
// stream after fenced and open-class writes: the binding lives in the meta
// hash (wfbind:<producer>), never in the prod value that decodeProducerState
// splits on every Get and every MATCHED create reply (A.1).
func TestProducerHashEncodingUnchangedOnFencedStream(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	path := testPath("wf-prod-encoding")
	mustCreate(t, s, path, store.CreateOptions{ContentType: "application/json", WriteFence: true})
	f1 := claimFence(1, "w_1", "worker-a")
	mustGrant(t, s, path, f1)
	mustFencedAppend(t, s, path, f1, 0)
	mustFencedAppend(t, s, path, f1, 1)
	zero := int64(0)
	mustAppend(t, s, path, []byte(`{"inbox":"hi"}`), store.AppendOptions{
		ContentType: "application/json", ProducerId: "wake-reg-7", ProducerEpoch: &zero, ProducerSeq: &zero,
	})

	values, err := testClient.HGetAll(ctx, prodKey(path)).Result()
	if err != nil {
		t.Fatalf("HGETALL prod: %v", err)
	}
	encoding := regexp.MustCompile(`^-?[0-9]+:-?[0-9]+:[0-9]+$`)
	for pid, v := range values {
		if !encoding.MatchString(v) {
			t.Fatalf("prod[%s] = %q is not the 3-segment encoding", pid, v)
		}
		if _, err := decodeProducerState(v); err != nil {
			t.Fatalf("prod[%s] = %q: %v", pid, v, err)
		}
	}
	meta, err := s.Get(path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if p := meta.Producers["p"]; p == nil || p.Epoch != 1 || p.LastSeq != 1 {
		t.Fatalf("producer p = %+v, want epoch 1 lastSeq 1", p)
	}
	if q := meta.Producers["wake-reg-7"]; q == nil || q.Epoch != 0 {
		t.Fatalf("producer wake-reg-7 = %+v, want epoch 0", q)
	}
	if bind, err := testClient.HGet(ctx, metaKey(path), fBindPrefix+"p").Result(); err != nil || bind != "1" {
		t.Fatalf("wfbind:p = %q, %v; want 1", bind, err)
	}
	if _, err := testClient.HGet(ctx, metaKey(path), fBindPrefix+"wake-reg-7").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("open-class producer was bound: %v", err)
	}
}
