package store

import (
	"encoding/base64"
	"math"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// write_fence_test.go pins the pure write-fence rung (#183, INV-FENCE-07): the
// Go oracle EvaluateWriteFence that the Lua evaluate_write_fence mirrors
// atomically inside append.lua / close.lua. The live differential lives in
// store/redis/write_fence_differential_test.go.

// liveMarker is a marker that exactly matches f at now, with lease headroom.
func liveMarker(f auth.AppendFence, incarnation string, nowNs int64) WriteFenceMarker {
	return WriteFenceMarker{
		Present:           true,
		State:             WriteFenceMarkerLive,
		Generation:        f.Generation,
		WakeID:            f.WakeID,
		Holder:            f.Holder,
		LeaseUntilNs:      nowNs + 1,
		StreamIncarnation: incarnation,
	}
}

// TestEvaluateWriteFenceTable pins every FenceReason, the first-hit rule order
// (seal precedes marker precedes producer precedes epoch; bound is open-class
// only), the boundaries (generation == seal, seal+1, MaxInt64, lease == now,
// incarnation mismatch, absent and revoked markers, unbound vs bound), that the
// open class on an unfenced stream never fences, and the disclosure fields of
// each branch.
func TestEvaluateWriteFenceTable(t *testing.T) {
	const (
		now = int64(1_000_000)
		inc = "stream-incarnation"
	)
	fence := auth.AppendFence{
		SubscriptionID:          "sub",
		SubscriptionIncarnation: "inc",
		Generation:              8,
		WakeID:                  "w_8",
		Holder:                  "worker-a",
	}
	live := liveMarker(fence, inc, now)
	with := func(mutate func(m *WriteFenceMarker)) WriteFenceMarker {
		m := live
		mutate(&m)
		return m
	}
	seal := func(gen int64) WriteFenceSeal {
		return WriteFenceSeal{Present: true, Generation: gen, WakeID: "w_old", Offset: Offset{ByteOffset: 9}}
	}
	fencedInput := func(m WriteFenceMarker) WriteFenceInput {
		return WriteFenceInput{
			StreamFenced:      true,
			StreamIncarnation: inc,
			Fence:             &fence,
			Marker:            m,
			NowNs:             now,
			HasProducer:       true,
			ProducerEpoch:     fence.Generation,
		}
	}
	maxFence := fence
	maxFence.Generation = math.MaxInt64

	tests := []struct {
		name string
		in   WriteFenceInput
		want WriteFenceOutcome
	}{
		{
			name: "open class on an unfenced stream never fences",
			in:   WriteFenceInput{HasProducer: true, BoundGeneration: 5},
			want: WriteFenceOutcome{Generation: 5},
		},
		{
			name: "open class on a fenced stream with an unbound producer is accepted",
			in:   WriteFenceInput{StreamFenced: true, HasProducer: true},
			want: WriteFenceOutcome{},
		},
		{
			name: "open class on a fenced stream without producer headers is accepted even when bound",
			in:   WriteFenceInput{StreamFenced: true, BoundGeneration: 8},
			want: WriteFenceOutcome{Generation: 8},
		},
		{
			name: "open class naming a bound producer is refused with the bound generation",
			in:   WriteFenceInput{StreamFenced: true, HasProducer: true, BoundGeneration: 8},
			want: WriteFenceOutcome{Reason: FenceBound, Generation: 8},
		},
		{
			name: "open class ignores marker and seal",
			in: WriteFenceInput{
				StreamFenced: true, HasProducer: true,
				Marker: with(func(m *WriteFenceMarker) { m.State = WriteFenceMarkerRevoked }),
				Seal:   seal(99),
			},
			want: WriteFenceOutcome{},
		},
		{
			name: "fenced class with a live matching marker on a fenced stream is accepted",
			in:   fencedInput(live),
			want: WriteFenceOutcome{Generation: 8, Holder: "worker-a"},
		},
		{
			name: "fenced class ignores the bound generation",
			in: func() WriteFenceInput {
				in := fencedInput(live)
				in.BoundGeneration = 5
				return in
			}(),
			want: WriteFenceOutcome{Generation: 8, Holder: "worker-a"},
		},
		{
			name: "fenced class on an unfenced stream keeps today's marker check only",
			in: WriteFenceInput{
				StreamIncarnation: inc, Fence: &fence, Marker: live, NowNs: now,
				Seal: seal(8), ProducerEpoch: 3,
			},
			want: WriteFenceOutcome{Generation: 8, Holder: "worker-a"},
		},
		{
			name: "generation equal to the seal is sealed; absent marker discloses the seal generation",
			in: func() WriteFenceInput {
				in := fencedInput(WriteFenceMarker{})
				in.Seal = seal(8)
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceSealed, Generation: 8},
		},
		{
			name: "generation below the seal is sealed",
			in: func() WriteFenceInput {
				in := fencedInput(WriteFenceMarker{})
				in.Seal = seal(9)
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceSealed, Generation: 9},
		},
		{
			name: "seal precedes marker and discloses the successor's live marker",
			in: func() WriteFenceInput {
				in := fencedInput(with(func(m *WriteFenceMarker) {
					m.Generation, m.WakeID, m.Holder = 9, "w_9", "worker-b"
				}))
				in.Seal = seal(8)
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceSealed, Generation: 9, Holder: "worker-b"},
		},
		{
			name: "seal precedes marker even when the marker is revoked",
			in: func() WriteFenceInput {
				in := fencedInput(with(func(m *WriteFenceMarker) {
					m.State, m.LeaseUntilNs = WriteFenceMarkerRevoked, 0
				}))
				in.Seal = seal(8)
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceSealed, Generation: 8},
		},
		{
			name: "generation one above the seal passes the seal",
			in: func() WriteFenceInput {
				in := fencedInput(live)
				in.Seal = seal(7)
				return in
			}(),
			want: WriteFenceOutcome{Generation: 8, Holder: "worker-a"},
		},
		{
			name: "MaxInt64 generation against a MaxInt64 seal is sealed",
			in: WriteFenceInput{
				StreamFenced: true, StreamIncarnation: inc, Fence: &maxFence,
				Marker: liveMarker(maxFence, inc, now), Seal: seal(math.MaxInt64),
				NowNs: now, HasProducer: true, ProducerEpoch: math.MaxInt64,
			},
			want: WriteFenceOutcome{Reason: FenceSealed, Generation: math.MaxInt64, Holder: "worker-a"},
		},
		{
			name: "MaxInt64 generation against a MaxInt64-1 seal is accepted",
			in: WriteFenceInput{
				StreamFenced: true, StreamIncarnation: inc, Fence: &maxFence,
				Marker: liveMarker(maxFence, inc, now), Seal: seal(math.MaxInt64 - 1),
				NowNs: now, HasProducer: true, ProducerEpoch: math.MaxInt64,
			},
			want: WriteFenceOutcome{Generation: math.MaxInt64, Holder: "worker-a"},
		},
		{
			name: "absent marker is refused with nothing to disclose",
			in:   fencedInput(WriteFenceMarker{}),
			want: WriteFenceOutcome{Reason: FenceMarker},
		},
		{
			name: "revoked marker is refused and discloses no holder",
			in: fencedInput(with(func(m *WriteFenceMarker) {
				m.State, m.LeaseUntilNs = WriteFenceMarkerRevoked, 0
			})),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 8},
		},
		{
			name: "marker at a newer generation is refused and discloses it with its holder",
			in: fencedInput(with(func(m *WriteFenceMarker) {
				m.Generation, m.WakeID, m.Holder = 9, "w_9", "worker-b"
			})),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 9, Holder: "worker-b"},
		},
		{
			name: "wake id mismatch is refused",
			in:   fencedInput(with(func(m *WriteFenceMarker) { m.WakeID = "w_other" })),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 8, Holder: "worker-a"},
		},
		{
			name: "holder mismatch is refused",
			in:   fencedInput(with(func(m *WriteFenceMarker) { m.Holder = "worker-b" })),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 8, Holder: "worker-b"},
		},
		{
			name: "lease equal to now has lapsed and discloses no holder",
			in:   fencedInput(with(func(m *WriteFenceMarker) { m.LeaseUntilNs = now })),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 8},
		},
		{
			name: "stream incarnation mismatch is refused",
			in:   fencedInput(with(func(m *WriteFenceMarker) { m.StreamIncarnation = "recreated" })),
			want: WriteFenceOutcome{Reason: FenceMarker, Generation: 8, Holder: "worker-a"},
		},
		{
			name: "fenced class without producer headers on a fenced stream is refused",
			in: func() WriteFenceInput {
				in := fencedInput(live)
				in.HasProducer, in.ProducerEpoch = false, 0
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceProducerRequired, Generation: 8, Holder: "worker-a"},
		},
		{
			name: "marker precedes the producer requirement",
			in: func() WriteFenceInput {
				in := fencedInput(WriteFenceMarker{})
				in.HasProducer = false
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceMarker},
		},
		{
			name: "producer epoch above the generation is refused",
			in: func() WriteFenceInput {
				in := fencedInput(live)
				in.ProducerEpoch = 9
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceEpoch, Generation: 8, Holder: "worker-a"},
		},
		{
			name: "producer epoch below the generation is refused",
			in: func() WriteFenceInput {
				in := fencedInput(live)
				in.ProducerEpoch = 7
				return in
			}(),
			want: WriteFenceOutcome{Reason: FenceEpoch, Generation: 8, Holder: "worker-a"},
		},
		{
			name: "fenced class on an unfenced stream needs neither producer headers nor the epoch",
			in: WriteFenceInput{
				StreamIncarnation: inc, Fence: &fence, Marker: live, NowNs: now,
			},
			want: WriteFenceOutcome{Generation: 8, Holder: "worker-a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EvaluateWriteFence(tt.in); got != tt.want {
				t.Fatalf("EvaluateWriteFence = %+v, want %+v\n  in=%+v", got, tt.want, tt.in)
			}
		})
	}
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

// writeFenceInputGen draws a WriteFenceInput stratified over the rung's
// outcomes, so rules 3 and 4 and the fenced-class accept path — reachable only
// through the fence's own live, unsealed marker with producer headers at
// epoch == generation — fire at the default check budget rather than by
// coincidence (TestWriteFenceGeneratorCoverage pins it). The fenced class
// starts from that accept shape and disturbs at most one input: the seal (at
// the generation, next to it, or anywhere), one marker field (absent, revoked,
// generation, wake, holder, a lapsed lease, incarnation), or the producer
// headers (absent, or the epoch off the generation); one arm draws marker,
// seal and producer independently so unrelated combinations stay covered.
// Generations are small or at the int64 sentinels, neighbours are ±1 with
// saturation, and the lease sits within two of now. The open class draws its
// bound generation zero, positive or anywhere.
func writeFenceInputGen() *rapid.Generator[WriteFenceInput] {
	return rapid.Custom(func(t *rapid.T) WriteFenceInput {
		gen := rapid.OneOf(
			rapid.Int64Range(1, 16),
			rapid.SampledFrom([]int64{0, -1, math.MaxInt64, math.MinInt64, 1 << 53, 1e14}),
		).Draw(t, "generation")
		near := func(label string) int64 {
			return rapid.OneOf(rapid.SampledFrom(saturatingNeighbors(gen)), rapid.Int64()).Draw(t, label)
		}
		now := rapid.Int64Range(0, 1<<40).Draw(t, "now")
		ids := []string{"w_a", "w_b"}
		holders := []string{"worker-a", "worker-b"}
		incs := []string{"inc-1", "inc-2", ""}
		in := WriteFenceInput{
			StreamIncarnation: rapid.SampledFrom(incs).Draw(t, "incarnation"),
			NowNs:             now,
		}
		if !mostly(t, "fencedClass") {
			in.StreamFenced = mostly(t, "streamFenced")
			in.HasProducer = mostly(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
			in.BoundGeneration = rapid.OneOf(
				rapid.Just(int64(0)), rapid.Int64Range(1, math.MaxInt64), rapid.Int64(),
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
		in.Marker = WriteFenceMarker{
			Present: true, State: WriteFenceMarkerLive, Generation: gen, WakeID: fence.WakeID,
			Holder: fence.Holder, LeaseUntilNs: now + 1, StreamIncarnation: in.StreamIncarnation,
		}
		switch rapid.SampledFrom([]string{"none", "seal", "marker", "producer", "unrelated"}).Draw(t, "disturb") {
		case "seal":
			in.Seal = WriteFenceSeal{Present: true, WakeID: "w_s", Generation: rapid.OneOf(
				rapid.Just(gen), rapid.SampledFrom(saturatingNeighbors(gen)), rapid.Int64(),
			).Draw(t, "sealGeneration")}
		case "marker":
			m := &in.Marker
			switch rapid.SampledFrom([]string{"absent", "state", "generation", "wake", "holder", "lease", "incarnation"}).Draw(t, "markerField") {
			case "absent":
				*m = WriteFenceMarker{}
			case "state":
				m.State = WriteFenceMarkerRevoked
			case "generation":
				m.Generation = near("markerGeneration")
			case "wake":
				m.WakeID = other(t, "markerWake", ids, m.WakeID)
			case "holder":
				m.Holder = other(t, "markerHolder", holders, m.Holder)
			case "lease":
				m.LeaseUntilNs = rapid.Int64Range(now-2, now).Draw(t, "lease")
			case "incarnation":
				m.StreamIncarnation = other(t, "markerIncarnation", incs, m.StreamIncarnation)
			}
		case "producer":
			in.HasProducer = rapid.Bool().Draw(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
		case "unrelated":
			in.HasProducer = rapid.Bool().Draw(t, "hasProducer")
			in.ProducerEpoch = near("producerEpoch")
			in.Marker = WriteFenceMarker{}
			if rapid.Bool().Draw(t, "markerPresent") {
				in.Marker = WriteFenceMarker{
					Present:           true,
					State:             rapid.SampledFrom([]string{WriteFenceMarkerLive, WriteFenceMarkerRevoked}).Draw(t, "state"),
					Generation:        near("markerGeneration"),
					WakeID:            rapid.SampledFrom(ids).Draw(t, "markerWake"),
					Holder:            rapid.SampledFrom(holders).Draw(t, "markerHolder"),
					LeaseUntilNs:      rapid.Int64Range(now-2, now+2).Draw(t, "lease"),
					StreamIncarnation: rapid.SampledFrom(incs).Draw(t, "markerIncarnation"),
				}
			}
			if rapid.Bool().Draw(t, "sealPresent") {
				in.Seal = WriteFenceSeal{Present: true, Generation: near("sealGeneration"), WakeID: "w_s"}
			}
		}
		return in
	})
}

// saturatingNeighbors returns {v-1, v, v+1} clamped to the int64 range.
func saturatingNeighbors(v int64) []int64 {
	out := []int64{v}
	if v > math.MinInt64 {
		out = append(out, v-1)
	}
	if v < math.MaxInt64 {
		out = append(out, v+1)
	}
	return out
}

// TestEvaluateWriteFenceProperty pins the structural invariants of the fence
// rung over generated inputs: totality and determinism; an open-class write on
// an unfenced stream is never fenced; an accepted fenced-class write on a fenced
// stream carried producer headers with Producer-Epoch equal to the generation
// (INV-FENCE-05's per-write premise); an accepted fenced-class write always
// matched a live, unexpired marker of the same stream incarnation; the open
// class can only ever be refused as bound; and a disclosed holder is exactly a
// live, unexpired marker's holder.
func TestEvaluateWriteFenceProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		in := writeFenceInputGen().Draw(t, "in")
		out := EvaluateWriteFence(in)
		if again := EvaluateWriteFence(in); again != out {
			t.Fatalf("nondeterministic: %+v then %+v", out, again)
		}
		switch out.Reason {
		case FenceNone, FenceSealed, FenceMarker, FenceProducerRequired, FenceEpoch, FenceBound:
		default:
			t.Fatalf("reason %q outside the vocabulary", out.Reason)
		}
		if in.Fence == nil {
			if in.StreamFenced && in.HasProducer && in.BoundGeneration > 0 {
				if out.Reason != FenceBound {
					t.Fatalf("bound producer on the open class must be refused: %+v -> %+v", in, out)
				}
			} else if out.Reason != FenceNone {
				t.Fatalf("open class refused as %q without a bound producer on a fenced stream: %+v", out.Reason, in)
			}
			if out.Holder != "" {
				t.Fatalf("open class disclosed a holder: %+v", out)
			}
			return
		}
		m := in.Marker
		live := m.Present && m.State == WriteFenceMarkerLive && m.LeaseUntilNs > in.NowNs
		if (out.Holder != "") != live || (live && out.Holder != m.Holder) {
			t.Fatalf("holder disclosure %q does not track the live marker: %+v", out.Holder, in)
		}
		if out.Reason == FenceBound {
			t.Fatalf("fenced class refused as bound: %+v", in)
		}
		if out.Reason != FenceNone {
			return
		}
		f := in.Fence
		if !live || m.Generation != f.Generation || m.WakeID != f.WakeID || m.Holder != f.Holder ||
			m.StreamIncarnation != in.StreamIncarnation {
			t.Fatalf("accepted fenced write without a matching live marker: %+v", in)
		}
		if in.StreamFenced {
			if in.Seal.Present && f.Generation <= in.Seal.Generation {
				t.Fatalf("accepted a sealed generation: %+v", in)
			}
			if !in.HasProducer || in.ProducerEpoch != f.Generation {
				t.Fatalf("accepted fenced write on a fenced stream without epoch == generation: %+v", in)
			}
		}
	})
}

// writeFenceOutcomeLabel names the bucket a generated input landed in: the
// refusal reason, or which accept path — the open class; the fenced class on a
// fenced stream, the only route through rules 3 and 4; or the fenced class on
// a stream that never opted in.
func writeFenceOutcomeLabel(in WriteFenceInput, out WriteFenceOutcome) string {
	switch {
	case out.Reason != FenceNone:
		return string(out.Reason)
	case in.Fence == nil:
		return "accept-open"
	case in.StreamFenced:
		return "accept-fenced"
	default:
		return "accept-unfenced-stream"
	}
}

// writeFenceOutcomeLabels is every bucket a write-fence input generator must
// reach: the five refusals and the three accept paths.
var writeFenceOutcomeLabels = []string{
	string(FenceSealed), string(FenceMarker), string(FenceProducerRequired), string(FenceEpoch),
	string(FenceBound), "accept-open", "accept-fenced", "accept-unfenced-stream",
}

// TestWriteFenceGeneratorCoverage confirms writeFenceInputGen reaches every
// outcome TestEvaluateWriteFenceProperty's clauses depend on — each refusal
// and each accept path, including the fenced class on a fenced stream, which
// is the only route through rules 3 and 4 — rather than by uniform luck. It
// samples a FIXED number of deterministic examples (via Generator.Example) so
// the assertion is independent of the rapid check budget, which shrinks under
// -short. Pure probe, runs under -short.
func TestWriteFenceGeneratorCoverage(t *testing.T) {
	const samples = 500
	gen := writeFenceInputGen()
	seen := map[string]int{}
	for i := 0; i < samples; i++ {
		in := gen.Example(i)
		seen[writeFenceOutcomeLabel(in, EvaluateWriteFence(in))]++
	}
	for _, want := range writeFenceOutcomeLabels {
		if seen[want] == 0 {
			t.Errorf("generator never reached %s", want)
		}
	}
	t.Logf("coverage over %d examples: %v", samples, seen)
}

// TestFenceAuthorityIdentity pins the authority encoding shared by the marker
// key suffix and the seal field: base64url of subscription id, NUL, incarnation,
// then ":" and the shard — and that it is injective over NUL-free ids, so two
// authorities can never share a marker or a seal (INV-FENCE-07).
func TestFenceAuthorityIdentity(t *testing.T) {
	f := auth.AppendFence{SubscriptionID: "sub-e1", SubscriptionIncarnation: "7", Shard: 0}
	want := base64.RawURLEncoding.EncodeToString([]byte("sub-e1\x007")) + ":0"
	if got := FenceAuthority(f); got != want {
		t.Fatalf("FenceAuthority = %q, want %q", got, want)
	}

	nulFree := rapid.SliceOf(rapid.Byte().Filter(func(b byte) bool { return b != 0 }))
	rapid.Check(t, func(t *rapid.T) {
		a := auth.AppendFence{
			SubscriptionID:          string(nulFree.Draw(t, "subA")),
			SubscriptionIncarnation: string(nulFree.Draw(t, "incA")),
			Shard:                   rapid.IntRange(0, 3).Draw(t, "shardA"),
		}
		b := auth.AppendFence{
			SubscriptionID:          string(nulFree.Draw(t, "subB")),
			SubscriptionIncarnation: string(nulFree.Draw(t, "incB")),
			Shard:                   rapid.IntRange(0, 3).Draw(t, "shardB"),
		}
		same := a.SubscriptionID == b.SubscriptionID &&
			a.SubscriptionIncarnation == b.SubscriptionIncarnation && a.Shard == b.Shard
		if (FenceAuthority(a) == FenceAuthority(b)) != same {
			t.Fatalf("authority collision: %+v vs %+v", a, b)
		}
		if got, wantSuffix := FenceAuthority(a), ":"+strconv.Itoa(a.Shard); got[len(got)-len(wantSuffix):] != wantSuffix {
			t.Fatalf("authority %q does not end with the shard suffix %q", got, wantSuffix)
		}
	})
}
