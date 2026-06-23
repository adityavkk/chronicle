package store

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

// config_match_property_test.go pins ConfigMatches idempotent-create soundness
// (INV-CFG-01). ConfigMatches(M, O) decides whether re-creating a stream from
// options O should be recognized as the SAME stream as the existing metadata M
// (an idempotent PUT) rather than a config conflict. It compares content type,
// TTL (nil-equivalence + value), ExpiresAt, Closed, ForkedFrom, and — for forks
// — two subtle branches the adversarial critic flagged:
//
//   - the ForkOffsetRequested-vs-ForkOffset fallback: a JSON fork created with a
//     sub-offset advances the internal ForkOffset past the user-supplied value,
//     so idempotency compares O.ForkOffset against the STORED REQUESTED offset
//     (M.ForkOffsetRequested), falling back to M.ForkOffset only when the
//     requested offset was never recorded (pre-PR metadata, nil).
//   - the nil-vs-0 ForkSubOffset equivalence: a nil O.ForkSubOffset and a 0 must
//     compare equal to the stored M.ForkSubOffset.
//
// The property generates an options O, builds the metadata M that a faithful
// create from O would persist (so ConfigMatches(M, O) MUST be true), then in the
// mismatch arm perturbs exactly one comparison input and asserts ConfigMatches
// flips to false. The live-Lua config_matches mirror is cross-checked in
// store/redis/config_match_differential_test.go over the same generators.

// off builds an Offset from a byte position (ReadSeq 0 — forks live in a single
// read-seq generation, so the divergence point is a byte offset).
func off(b uint64) Offset { return Offset{ReadSeq: 0, ByteOffset: b} }

// i64 / u64 return pointers to a literal, for the *int64 / *uint64 option fields.
func i64(v int64) *int64    { return &v }
func u64(v uint64) *uint64  { return &v }
func offp(b uint64) *Offset { o := off(b); return &o }

// configCase is one generated idempotency case: the options O, the metadata M
// that a create from O would persist, and whether ConfigMatches(M, O) should be
// true. The perturbed arm sets shouldMatch=false and records which field it
// disturbed for diagnostics.
type configCase struct {
	meta        StreamMetadata
	opts        CreateOptions
	shouldMatch bool
	perturbed   string
}

// baseOptsGen draws a CreateOptions covering the full comparison surface:
// content type (empty / bare / parameterized / mixed-case), TTL (nil or value),
// ExpiresAt (nil or value), Closed, and — with ~1/2 probability — a fork with a
// ForkOffset (nil or value), a ForkSubOffset (nil / 0 / >0), and a toggle for
// whether the persisted metadata recorded ForkOffsetRequested (the pre-PR
// fallback path) or not.
func baseOptsGen() *rapid.Generator[CreateOptions] {
	return rapid.Custom(func(t *rapid.T) CreateOptions {
		o := CreateOptions{
			ContentType: rapid.SampledFrom([]string{
				"", "application/json", "Application/JSON",
				"text/plain", "text/plain; charset=utf-8", "image/png",
			}).Draw(t, "ct"),
			Closed: rapid.Bool().Draw(t, "closed"),
		}
		if rapid.Bool().Draw(t, "hasTTL") {
			o.TTLSeconds = i64(rapid.Int64Range(0, 1<<40).Draw(t, "ttl"))
		}
		if rapid.Bool().Draw(t, "hasExp") {
			ns := rapid.Int64Range(0, 1<<50).Draw(t, "expNs")
			tm := time.Unix(0, ns).UTC()
			o.ExpiresAt = &tm
		}
		if rapid.Bool().Draw(t, "isFork") {
			o.ForkedFrom = rapid.SampledFrom([]string{"/src/a", "/src/b", "/parent"}).Draw(t, "forkedFrom")
			if rapid.Bool().Draw(t, "hasForkOff") {
				o.ForkOffset = offp(rapid.Uint64Range(0, 1<<40).Draw(t, "forkOff"))
			}
			switch rapid.IntRange(0, 2).Draw(t, "subOffKind") {
			case 0:
				// nil ForkSubOffset
			case 1:
				o.ForkSubOffset = u64(0) // explicit 0 — must equal nil
			case 2:
				o.ForkSubOffset = u64(rapid.Uint64Range(1, 1<<40).Draw(t, "subOff"))
			}
		}
		return o
	})
}

// metaFromOpts builds the StreamMetadata a faithful create from O would persist,
// so that ConfigMatches(metaFromOpts(O), O) is true by construction. It mirrors
// the create path's resolution of the fork fields:
//   - ContentType is stored normalized-but-equivalent: ConfigMatches compares it
//     via ContentTypeMatches, which is case/parameter-insensitive, so storing O's
//     content type verbatim already matches. (The empty -> octet-stream default
//     also matches, since ContentTypeMatches normalizes both sides.)
//   - For JSON forks with sub-offset > 0 the internal ForkOffset is advanced past
//     the user value; ForkOffsetRequested records the original. We model that by
//     setting ForkOffset to an ADVANCED value while ForkOffsetRequested holds
//     O.ForkOffset — the exact configuration the fallback must handle.
//   - ForkSubOffset is stored verbatim (0 when O's was nil or 0).
//
// recordRequested toggles whether the persisted metadata carries
// ForkOffsetRequested (post-PR) or leaves it nil (pre-PR fallback path).
func metaFromOpts(o CreateOptions, advanceForkOff bool, recordRequested bool) StreamMetadata {
	m := StreamMetadata{
		ContentType: o.ContentType,
		Closed:      o.Closed,
		ForkedFrom:  o.ForkedFrom,
	}
	if o.TTLSeconds != nil {
		m.TTLSeconds = i64(*o.TTLSeconds)
	}
	if o.ExpiresAt != nil {
		tm := *o.ExpiresAt
		m.ExpiresAt = &tm
	}
	if o.ForkedFrom != "" {
		// The user-supplied (requested) offset, defaulting to zero when omitted.
		var requested Offset
		if o.ForkOffset != nil {
			requested = *o.ForkOffset
		}
		// Internal ForkOffset: for a JSON sub-offset fork it is advanced PAST the
		// requested value. We model the advance only when asked AND a sub-offset
		// applies, so the fallback's "compare requested, not internal" branch is
		// genuinely exercised (internal != requested).
		advanced := advanceForkOff && o.ForkSubOffset != nil && *o.ForkSubOffset > 0
		internal := requested
		if advanced {
			internal = requested.Add(*o.ForkSubOffset * 7) // advance by an arbitrary nonzero amount
		}
		m.ForkOffset = internal
		// Record the requested offset to match what the real create path does: it
		// sets ForkOffsetRequested whenever opts.ForkOffset is supplied. The
		// pre-PR fallback (ForkOffsetRequested == nil) is ONLY legitimate when the
		// internal offset was NOT advanced (internal == requested) — a fork whose
		// offset was advanced always recorded the requested value, so leaving it
		// nil there would be an unreachable state, not an idempotent re-create.
		if o.ForkOffset != nil && (recordRequested || advanced) {
			req := requested
			m.ForkOffsetRequested = &req
		}
		// nil and 0 sub-offset both persist as 0.
		if o.ForkSubOffset != nil {
			m.ForkSubOffset = *o.ForkSubOffset
		}
	}
	return m
}

// configCaseGen draws a configCase. ~half are MATCHED (M built from O so
// ConfigMatches must hold, across all four (advanceForkOff × recordRequested)
// fork configurations); the other half PERTURB exactly one comparison input on a
// matched base so ConfigMatches must flip to false.
func configCaseGen() *rapid.Generator[configCase] {
	return rapid.Custom(func(t *rapid.T) configCase {
		o := baseOptsGen().Draw(t, "opts")
		advance := rapid.Bool().Draw(t, "advanceForkOff")
		recordReq := rapid.Bool().Draw(t, "recordRequested")
		m := metaFromOpts(o, advance, recordReq)

		if rapid.Bool().Draw(t, "matched") {
			return configCase{meta: m, opts: o, shouldMatch: true}
		}

		// Perturb exactly one comparison input. Only choose perturbations that
		// are guaranteed to change the decision for THIS case.
		field := perturbConfig(t, &m, &o)
		return configCase{meta: m, opts: o, shouldMatch: false, perturbed: field}
	})
}

// perturbConfig mutates exactly one comparison input on a matched (m, o) pair so
// that ConfigMatches MUST become false, and returns a label for the disturbed
// field. It picks only from perturbations valid for the current shape (e.g. it
// won't perturb a fork-only field on a non-fork case).
func perturbConfig(t *rapid.T, m *StreamMetadata, o *CreateOptions) string {
	choices := []string{"ct", "ttlValue", "ttlNil", "expValue", "expNil", "closed"}
	if o.ForkedFrom != "" {
		choices = append(choices, "forkSubOff")
		if o.ForkOffset != nil {
			choices = append(choices, "forkOffReq")
		}
		// Changing ForkedFrom on a fork is always a mismatch.
		choices = append(choices, "forkedFromChange")
	} else {
		// Turning a non-fork into a fork (or vice versa) flips ForkedFrom equality.
		choices = append(choices, "forkedFromAdd")
	}

	switch rapid.SampledFrom(choices).Draw(t, "perturb") {
	case "ct":
		// Change the BASE media type so ContentTypeMatches fails. Use a token
		// that cannot equal any generated base (different subtype).
		o.ContentType = "application/x-perturbed-" +
			rapid.StringMatching(`[a-z]{4}`).Draw(t, "ctsfx")
		return "ct"
	case "ttlValue":
		if o.TTLSeconds == nil {
			o.TTLSeconds = i64(rapid.Int64Range(1, 1<<40).Draw(t, "newttl"))
			return "ttlNilToValue"
		}
		o.TTLSeconds = i64(*o.TTLSeconds + 1)
		return "ttlValue"
	case "ttlNil":
		if o.TTLSeconds != nil {
			o.TTLSeconds = nil
			return "ttlValueToNil"
		}
		o.TTLSeconds = i64(1)
		return "ttlNilToValue"
	case "expValue":
		if o.ExpiresAt == nil {
			tm := time.Unix(0, 123456789).UTC()
			o.ExpiresAt = &tm
			return "expNilToValue"
		}
		tm := o.ExpiresAt.Add(time.Nanosecond)
		o.ExpiresAt = &tm
		return "expValue"
	case "expNil":
		if o.ExpiresAt != nil {
			o.ExpiresAt = nil
			return "expValueToNil"
		}
		tm := time.Unix(0, 999).UTC()
		o.ExpiresAt = &tm
		return "expNilToValue"
	case "closed":
		// Flip O's Closed relative to M so the comparison differs.
		o.Closed = !m.Closed
		return "closed"
	case "forkSubOff":
		// Make O's sub-offset differ from M's stored value by a guaranteed delta.
		o.ForkSubOffset = u64(m.ForkSubOffset + 1)
		return "forkSubOff"
	case "forkOffReq":
		// O carries a ForkOffset; bump it so it no longer equals the stored
		// requested offset (or its fallback). This targets the fallback branch.
		bumped := o.ForkOffset.Add(13)
		o.ForkOffset = &bumped
		return "forkOffReq"
	case "forkedFromChange":
		o.ForkedFrom += "-other"
		return "forkedFromChange"
	case "forkedFromAdd":
		o.ForkedFrom = "/newly/forked"
		o.ForkOffset = offp(0)
		return "forkedFromAdd"
	}
	return "none"
}

// TestConfigMatchesSoundness pins INV-CFG-01: ConfigMatches(M, O) is true for an
// identical re-create (M built faithfully from O) and false for a single-field
// perturbation, over content type / TTL (nil + value) / ExpiresAt / Closed /
// ForkedFrom / fork fields. Pure — runs on every build including -short.
func TestConfigMatchesSoundness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := configCaseGen().Draw(t, "case")
		got := c.meta.ConfigMatches(c.opts)
		if got != c.shouldMatch {
			t.Fatalf("INV-CFG-01: ConfigMatches = %v, want %v (perturbed=%q)\n  meta=%+v\n  opts=%+v",
				got, c.shouldMatch, c.perturbed, c.meta, c.opts)
		}
	})
}

// TestConfigMatchesForkOffsetFallback pins the ForkOffsetRequested-vs-ForkOffset
// fallback explicitly (INV-CFG-01, adversarial critic finding). It builds a JSON
// fork created WITH a sub-offset > 0 so the internal ForkOffset is advanced past
// the user-supplied value, and asserts BOTH legs of the fallback:
//   - post-PR metadata (ForkOffsetRequested recorded): an idempotent re-create
//     supplying the ORIGINAL requested offset MATCHES, even though it differs
//     from the advanced internal ForkOffset; supplying the advanced internal
//     offset MISMATCHES (the bug the fallback prevents).
//   - pre-PR metadata (ForkOffsetRequested == nil): the comparison falls back to
//     the internal ForkOffset, so a re-create supplying that internal offset
//     MATCHES.
func TestConfigMatchesForkOffsetFallback(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		requested := off(rapid.Uint64Range(0, 1<<40).Draw(t, "requested"))
		sub := rapid.Uint64Range(1, 1<<32).Draw(t, "sub")
		advanced := requested.Add(sub * 7) // internal ForkOffset, past the request

		base := func() (StreamMetadata, CreateOptions) {
			m := StreamMetadata{
				ContentType:   "application/json",
				ForkedFrom:    "/src",
				ForkOffset:    advanced,
				ForkSubOffset: sub,
			}
			o := CreateOptions{
				ContentType:   "application/json",
				ForkedFrom:    "/src",
				ForkOffset:    offp(requested.ByteOffset),
				ForkSubOffset: u64(sub),
			}
			return m, o
		}

		// Post-PR: requested recorded.
		m, o := base()
		req := requested
		m.ForkOffsetRequested = &req
		if !m.ConfigMatches(o) {
			t.Fatalf("INV-CFG-01 fallback: post-PR re-create with the requested offset should MATCH\n  m=%+v o=%+v", m, o)
		}
		// Supplying the ADVANCED internal offset (the pre-fix bug) must MISMATCH,
		// but only when it genuinely differs from the requested value (sub*7 > 0
		// guarantees this).
		o2 := o
		o2.ForkOffset = offp(advanced.ByteOffset)
		if advanced.Equal(requested) {
			t.Skip("degenerate: advance was zero")
		}
		if m.ConfigMatches(o2) {
			t.Fatalf("INV-CFG-01 fallback: re-create with the ADVANCED internal offset must MISMATCH "+
				"(this is the bug the requested-offset fallback prevents)\n  m=%+v o2=%+v", m, o2)
		}

		// Pre-PR: ForkOffsetRequested == nil -> fall back to internal ForkOffset.
		mPre, _ := base()
		mPre.ForkOffsetRequested = nil
		oPre := CreateOptions{
			ContentType:   "application/json",
			ForkedFrom:    "/src",
			ForkOffset:    offp(advanced.ByteOffset), // pre-PR forks only ever stored the resolved offset
			ForkSubOffset: u64(sub),
		}
		if !mPre.ConfigMatches(oPre) {
			t.Fatalf("INV-CFG-01 fallback: pre-PR (nil requested) re-create with the internal offset should MATCH\n  m=%+v o=%+v", mPre, oPre)
		}
	})
}

// TestConfigMatchesSubOffsetNilZeroEquivalence pins the nil-vs-0 ForkSubOffset
// equivalence (INV-CFG-01): for a fork with stored ForkSubOffset == 0, a
// re-create supplying nil ForkSubOffset and one supplying an explicit 0 must
// BOTH match, and any nonzero value must mismatch. Symmetrically, when the
// stored sub-offset is nonzero, only the exact value matches.
func TestConfigMatchesSubOffsetNilZeroEquivalence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		stored := rapid.Uint64Range(0, 1<<40).Draw(t, "stored")
		m := StreamMetadata{
			ContentType:   "application/octet-stream",
			ForkedFrom:    "/src",
			ForkOffset:    off(0),
			ForkSubOffset: stored,
		}
		base := CreateOptions{
			ContentType: "application/octet-stream",
			ForkedFrom:  "/src",
			ForkOffset:  offp(0),
		}

		if stored == 0 {
			// nil matches.
			oNil := base
			oNil.ForkSubOffset = nil
			if !m.ConfigMatches(oNil) {
				t.Fatalf("INV-CFG-01 nil-vs-0: stored 0 vs nil sub-offset should MATCH")
			}
			// explicit 0 matches.
			oZero := base
			oZero.ForkSubOffset = u64(0)
			if !m.ConfigMatches(oZero) {
				t.Fatalf("INV-CFG-01 nil-vs-0: stored 0 vs explicit 0 should MATCH")
			}
			// any nonzero mismatches.
			oNon := base
			oNon.ForkSubOffset = u64(rapid.Uint64Range(1, 1<<40).Draw(t, "non"))
			if m.ConfigMatches(oNon) {
				t.Fatalf("INV-CFG-01 nil-vs-0: stored 0 vs nonzero should MISMATCH")
			}
		} else {
			// nil (==0) mismatches a nonzero stored value.
			oNil := base
			oNil.ForkSubOffset = nil
			if m.ConfigMatches(oNil) {
				t.Fatalf("INV-CFG-01 nil-vs-0: stored %d vs nil(=0) should MISMATCH", stored)
			}
			// exact value matches.
			oExact := base
			oExact.ForkSubOffset = u64(stored)
			if !m.ConfigMatches(oExact) {
				t.Fatalf("INV-CFG-01 nil-vs-0: stored %d vs exact value should MATCH", stored)
			}
		}
	})
}

// TestConfigMatchGeneratorCoverage confirms configCaseGen reaches the structural
// cases the soundness property depends on: matched and perturbed arms, fork and
// non-fork cases, the advanced-ForkOffset (JSON sub-offset) configuration, the
// pre-PR (nil ForkOffsetRequested) fallback, and an explicit-0 sub-offset. It
// samples a FIXED number of deterministic examples (via Generator.Example) so the
// coverage assertion is independent of the rapid check budget, which shrinks
// under -short. Pure probe, runs under -short.
func TestConfigMatchGeneratorCoverage(t *testing.T) {
	const samples = 500
	gen := configCaseGen()
	var matched, perturbed, fork, nonFork, advanced, preFallback, zeroSub int
	for i := 0; i < samples; i++ {
		c := gen.Example(i)
		if c.shouldMatch {
			matched++
		} else {
			perturbed++
		}
		if c.opts.ForkedFrom != "" {
			fork++
			// Advanced internal offset present (JSON sub-offset fork).
			if c.shouldMatch && !c.meta.ForkOffset.Equal(reqOffset(c.opts)) {
				advanced++
			}
			if c.shouldMatch && c.meta.ForkOffsetRequested == nil {
				preFallback++
			}
			if c.opts.ForkSubOffset != nil && *c.opts.ForkSubOffset == 0 {
				zeroSub++
			}
		} else {
			nonFork++
		}
	}
	if matched == 0 || perturbed == 0 {
		t.Errorf("generator arms unbalanced: matched=%d perturbed=%d", matched, perturbed)
	}
	if fork == 0 || nonFork == 0 {
		t.Errorf("generator never drew both fork and non-fork: fork=%d nonFork=%d", fork, nonFork)
	}
	if advanced == 0 {
		t.Error("generator never drew an advanced-ForkOffset (JSON sub-offset) matched case")
	}
	if preFallback == 0 {
		t.Error("generator never drew a pre-PR (nil ForkOffsetRequested) fallback matched case")
	}
	if zeroSub == 0 {
		t.Error("generator never drew an explicit-0 sub-offset")
	}
	t.Logf("coverage: matched=%d perturbed=%d fork=%d nonFork=%d advanced=%d preFallback=%d zeroSub=%d",
		matched, perturbed, fork, nonFork, advanced, preFallback, zeroSub)
}

// reqOffset returns the user-requested fork offset from O (zero when omitted),
// used by the coverage probe to detect the advanced-internal-offset case.
func reqOffset(o CreateOptions) Offset {
	if o.ForkOffset != nil {
		return *o.ForkOffset
	}
	return off(0)
}
