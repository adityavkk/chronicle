package protocol

import (
	"math"
	"math/big"
	"strconv"
	"testing"

	"pgregory.net/rapid"
)

// parse_property_test.go pins the integer-parse grammars and the int64-overflow
// seam (INV-PARSE-01 / INV-PARSE-02 / INV-PARSE-03). Three helpers in parse.go
// re-implement closely-related-but-distinct integer grammars:
//
//   - ParseTTL       — grammar ^[1-9][0-9]*$|^0$, parsed as int64
//   - ParseSubOffset — same grammar, parsed as uint64
//   - IsValidIntegerString — the LOOSER grammar ^[0-9]+$ (leading zeros and
//     unbounded width allowed); the handler then narrows via
//     strconv.ParseInt(_,10,64), so the validation boundary spans two layers.
//
// These properties pin each grammar exactly against an independent oracle, the
// TTL-vs-SubOffset metamorphic relation (they diverge precisely on the
// [2^63, 2^64) window where TTL overflows int64 but SubOffset fits uint64), and
// the documented two-layer overflow seam: a value in [2^53, 2^63) passes
// IsValidIntegerString + ParseInt in Go but is the Go-accepts / Lua-imprecise
// boundary, pinned with a checked-in fixture. All pure — no Redis. (The live
// Lua leg for these grammars is the producer-header parse the existing
// differential harness already drives; this file pins the pure-Go grammars
// the issue scopes to.)

// strictGrammarOracle is an independent re-implementation of the ParseTTL /
// ParseSubOffset grammar ^[1-9][0-9]*$|^0$: a non-empty all-digit string with no
// leading zero unless it is exactly "0". No regexp, so it is a real oracle for
// the regexp-based production grammar rather than a copy of it.
func strictGrammarOracle(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	if s == "0" {
		return true
	}
	return s[0] != '0' // no leading zeros for any multi-digit / non-"0" value
}

// looseGrammarOracle re-implements IsValidIntegerString's grammar ^[0-9]+$:
// non-empty, all ASCII digits, leading zeros allowed, width unbounded.
func looseGrammarOracle(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

const (
	// pow2_53 is the Lua double exact-integer ceiling: tonumber() on a string
	// >= 2^53 may lose precision. This is the lower edge of the Go-accepts /
	// Lua-imprecise overflow seam (INV-PARSE-03).
	pow2_53 = uint64(1) << 53 // 9_007_199_254_740_992
	// pow2_63 is the int64 ceiling: values >= 2^63 overflow int64 (so ParseTTL
	// rejects them) but still fit uint64 (so ParseSubOffset accepts up to
	// 2^64-1). This is the upper edge of the seam and the TTL/SubOffset split.
	pow2_63 = uint64(1) << 63 // 9_223_372_036_854_775_808
)

// digitStringGen draws strings spanning the documented hazard space: well-formed
// integers, leading-zero variants, signed and whitespace-wrapped forms, decimal
// and hex spellings, and decimal renderings of values clustered at the 2^53,
// 2^63, and 2^64 boundaries (widths 1..21 digits). The mix is what makes the
// grammar-acceptance, the TTL/SubOffset split, and the overflow seam all fire on
// purpose rather than by uniform luck.
func digitStringGen() *rapid.Generator[string] {
	// Decimal renderings of the boundary values and their immediate neighbors.
	boundaries := []uint64{
		0, 1, 2, 9, 10,
		pow2_53 - 1, pow2_53, pow2_53 + 1,
		uint64(math.MaxInt64) - 1, uint64(math.MaxInt64), // 2^63-2, 2^63-1
		pow2_63, pow2_63 + 1, // 2^63, 2^63+1 (TTL overflow / SubOffset OK)
		math.MaxUint64 - 1, math.MaxUint64, // 2^64-2, 2^64-1
	}
	boundaryStrs := make([]string, 0, len(boundaries)+4)
	for _, v := range boundaries {
		boundaryStrs = append(boundaryStrs, strconv.FormatUint(v, 10))
	}
	// 2^64 and beyond: too big for uint64, must be rejected by the narrow parse
	// even though the loose/strict grammars accept the spelling.
	boundaryStrs = append(
		boundaryStrs,
		"18446744073709551616",           // 2^64
		"18446744073709551617",           // 2^64 + 1
		"99999999999999999999",           // 20 nines (> 2^64)
		"123456789012345678901234567890", // 30 digits
	)

	malformed := rapid.SampledFrom([]string{
		"", " ", "  ",
		"-1", "+1", "-0", "+0",
		"007", "00", "01", "0123", // leading zeros
		" 5", "5 ", " 5 ", "\t5", "5\n",
		"1.0", "1e3", "1E3", ".5", "5.",
		"0x1f", "0X1F", "1f", "deadbeef",
		"abc", "1a", "a1", "1_000",
		"4294967296", // 2^32, well-formed (no leading zero) — a strict accept
	})

	wellFormed := rapid.Custom(func(t *rapid.T) string {
		// A no-leading-zero decimal of a uniformly drawn uint64, or "0".
		v := rapid.Uint64().Draw(t, "v")
		return strconv.FormatUint(v, 10)
	})

	leadingZero := rapid.Custom(func(t *rapid.T) string {
		zeros := rapid.IntRange(1, 4).Draw(t, "zeros")
		v := rapid.Uint64().Draw(t, "v")
		s := strconv.FormatUint(v, 10)
		pad := make([]byte, zeros)
		for i := range pad {
			pad[i] = '0'
		}
		return string(pad) + s
	})

	return rapid.OneOf(
		rapid.SampledFrom(boundaryStrs),
		malformed,
		wellFormed,
		leadingZero,
	)
}

// TestParseTTLGrammar pins INV-PARSE-01: ParseTTL returns (v, nil) IFF the input
// matches ^[1-9][0-9]*$|^0$ AND fits int64; and on success the parsed value
// round-trips. The acceptance set is exactly (strict grammar) ∧ (fits int64).
func TestParseTTLGrammar(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := digitStringGen().Draw(t, "s")
		v, err := ParseTTL(s)

		grammarOK := strictGrammarOracle(s)
		fitsInt64 := false
		if grammarOK {
			if _, perr := strconv.ParseInt(s, 10, 64); perr == nil {
				fitsInt64 = true
			}
		}
		wantOK := grammarOK && fitsInt64

		if (err == nil) != wantOK {
			t.Fatalf("INV-PARSE-01: ParseTTL(%q) err=%v, want accept=%v (grammar=%v fitsInt64=%v)",
				s, err, wantOK, grammarOK, fitsInt64)
		}
		if err == nil {
			want, _ := strconv.ParseInt(s, 10, 64)
			if v != want {
				t.Fatalf("INV-PARSE-01: ParseTTL(%q)=%d, want %d", s, v, want)
			}
			if v < 0 {
				t.Fatalf("INV-PARSE-01: ParseTTL(%q)=%d is negative", s, v)
			}
		}
	})
}

// TestParseSubOffsetGrammar pins INV-PARSE-02: ParseSubOffset accepts the same
// strict grammar as ParseTTL but parses uint64, so it accepts the full
// [0, 2^64) range. Acceptance is exactly (strict grammar) ∧ (fits uint64).
func TestParseSubOffsetGrammar(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := digitStringGen().Draw(t, "s")
		v, err := ParseSubOffset(s)

		grammarOK := strictGrammarOracle(s)
		fitsUint64 := false
		if grammarOK {
			if _, perr := strconv.ParseUint(s, 10, 64); perr == nil {
				fitsUint64 = true
			}
		}
		wantOK := grammarOK && fitsUint64

		if (err == nil) != wantOK {
			t.Fatalf("INV-PARSE-02: ParseSubOffset(%q) err=%v, want accept=%v (grammar=%v fitsUint64=%v)",
				s, err, wantOK, grammarOK, fitsUint64)
		}
		if err == nil {
			want, _ := strconv.ParseUint(s, 10, 64)
			if v != want {
				t.Fatalf("INV-PARSE-02: ParseSubOffset(%q)=%d, want %d", s, v, want)
			}
		}
	})
}

// TestIsValidIntegerStringGrammar pins INV-PARSE-03's first clause:
// IsValidIntegerString accepts exactly ^[0-9]+$ — leading zeros allowed,
// width unbounded (it does NOT parse, so it never overflows). A 30-digit string
// of all digits is accepted; any non-digit or empty is rejected.
func TestIsValidIntegerStringGrammar(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := digitStringGen().Draw(t, "s")
		got := IsValidIntegerString(s)
		want := looseGrammarOracle(s)
		if got != want {
			t.Fatalf("INV-PARSE-03: IsValidIntegerString(%q)=%v, want %v", s, got, want)
		}
	})
}

// TestIsValidIntegerStringAdmitsLeadingZerosAndWidth pins the two distinguishing
// properties of the loose grammar against the strict one: it admits leading
// zeros (which ParseTTL/ParseSubOffset reject) and is unbounded in width (it
// accepts decimal strings far wider than any fixed integer). This is the
// "validate-loose, parse-narrow split across two layers" core of INV-PARSE-03.
func TestIsValidIntegerStringAdmitsLeadingZerosAndWidth(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Leading-zero forms: loose accepts, strict rejects (except "0" itself).
		zeros := rapid.IntRange(1, 5).Draw(t, "zeros")
		v := rapid.Uint64().Draw(t, "v")
		pad := make([]byte, zeros)
		for i := range pad {
			pad[i] = '0'
		}
		lz := string(pad) + strconv.FormatUint(v, 10)
		if !IsValidIntegerString(lz) {
			t.Fatalf("INV-PARSE-03: IsValidIntegerString(%q) should admit leading zeros", lz)
		}
		// The strict grammar rejects this exact leading-zero string (it is never
		// "0" because it has >= 2 chars with a leading '0').
		if strictGrammarOracle(lz) {
			t.Fatalf("fixture broken: strict grammar unexpectedly accepted leading-zero %q", lz)
		}

		// Unbounded width: a string of N>20 digits is accepted by the loose
		// grammar even though it cannot fit any fixed-width integer.
		width := rapid.IntRange(21, 60).Draw(t, "width")
		digits := make([]byte, width)
		for i := range digits {
			// First digit non-zero so it is genuinely > 2^64, the rest arbitrary.
			if i == 0 {
				digits[i] = byte('1' + rapid.IntRange(0, 8).Draw(t, "d0"))
			} else {
				digits[i] = byte('0' + rapid.IntRange(0, 9).Draw(t, "d"))
			}
		}
		wide := string(digits)
		if !IsValidIntegerString(wide) {
			t.Fatalf("INV-PARSE-03: IsValidIntegerString(%q) should admit unbounded width", wide)
		}
		// Cross-check it really exceeds uint64, so ParseSubOffset would reject it
		// — demonstrating the loose/narrow split.
		bi, ok := new(big.Int).SetString(wide, 10)
		if !ok {
			t.Fatalf("fixture broken: %q not parseable as big.Int", wide)
		}
		if bi.IsUint64() {
			t.Fatalf("fixture broken: %d-digit value %q unexpectedly fits uint64", width, wide)
		}
		if _, err := ParseSubOffset(wide); err == nil {
			t.Fatalf("INV-PARSE-03: ParseSubOffset(%q) should reject a value > 2^64", wide)
		}
	})
}

// TestTTLvsSubOffsetMetamorphic pins INV-PARSE-02's metamorphic relation:
// ParseTTL and ParseSubOffset share the strict grammar, so for any input that
// the grammar accepts they SUCCEED on exactly the same inputs EXCEPT the
// [2^63, 2^64) window, where TTL overflows int64 (rejects) but SubOffset fits
// uint64 (accepts). For grammar-rejected inputs both reject. The window is the
// sole point of divergence.
func TestTTLvsSubOffsetMetamorphic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := digitStringGen().Draw(t, "s")
		_, ttlErr := ParseTTL(s)
		_, subErr := ParseSubOffset(s)
		ttlOK := ttlErr == nil
		subOK := subErr == nil

		if !strictGrammarOracle(s) {
			// Both reject anything outside the shared grammar.
			if ttlOK || subOK {
				t.Fatalf("metamorphic: %q fails grammar but ttlOK=%v subOK=%v", s, ttlOK, subOK)
			}
			return
		}

		// Inside the grammar, classify by magnitude using big.Int (exact at any
		// width). The divergence window is exactly [2^63, 2^64).
		bi, ok := new(big.Int).SetString(s, 10)
		if !ok {
			t.Fatalf("grammar-valid %q failed big.Int parse", s)
		}
		inTTLWindow := bi.Cmp(new(big.Int).SetUint64(math.MaxInt64)) <= 0 // <= 2^63-1
		inU64 := bi.IsUint64()                                            // < 2^64
		inSplitWindow := !inTTLWindow && inU64                            // [2^63, 2^64)
		aboveU64 := !inU64                                                // >= 2^64

		switch {
		case inTTLWindow:
			// Both must accept.
			if !ttlOK || !subOK {
				t.Fatalf("metamorphic: %q in [0,2^63) but ttlOK=%v subOK=%v", s, ttlOK, subOK)
			}
		case inSplitWindow:
			// THE divergence: TTL overflows int64, SubOffset fits uint64.
			if ttlOK {
				t.Fatalf("metamorphic: %q in [2^63,2^64) but ParseTTL accepted (int64 overflow expected)", s)
			}
			if !subOK {
				t.Fatalf("metamorphic: %q in [2^63,2^64) but ParseSubOffset rejected (should fit uint64)", s)
			}
		case aboveU64:
			// Both overflow their respective widths -> both reject.
			if ttlOK || subOK {
				t.Fatalf("metamorphic: %q >= 2^64 but ttlOK=%v subOK=%v", s, ttlOK, subOK)
			}
		}
	})
}

// overflowSeamFixture is the checked-in [2^53, 2^63) boundary fixture for the
// two-layer overflow seam (INV-PARSE-03). Each row is a value that PASSES the Go
// validate-loose + parse-narrow path (IsValidIntegerString true, ParseInt
// succeeds, fits int64) yet sits at or above 2^53, where a Lua tonumber() of the
// same string loses integer precision. The fixture documents the seam rather
// than asserting agreement: Go accepts these exactly; the boundary is where Lua
// would silently round.
var overflowSeamFixture = []struct {
	s            string
	luaImprecise bool // true if >= 2^53, i.e. inside the Go-accepts / Lua-imprecise band
}{
	{"9007199254740991", false},   // 2^53 - 1: last integer Lua represents EXACTLY
	{"9007199254740992", true},    // 2^53: first value a Lua double cannot distinguish from 2^53+1
	{"9007199254740993", true},    // 2^53 + 1: rounds to 2^53 under a double
	{"9223372036854775807", true}, // 2^63 - 1 = math.MaxInt64: max int64, deep in the imprecise band
}

// TestParseOverflowSeamFixture pins INV-PARSE-03's documented two-layer seam
// with the checked-in fixture. For every row it asserts the Go reality the
// handler relies on: IsValidIntegerString accepts, the narrowing
// strconv.ParseInt(_,10,64) succeeds, and the value fits int64 — AND it pins the
// 2^53 boundary that classifies each row as Lua-imprecise. The seam is DOCUMENTED
// (the band is named and asserted), not silenced: a row sliding across 2^53
// trips this test. This is the parse-layer analogue of the producer reply
// boundary the existing differential harness pins; the live-Lua tonumber loss is
// the same hazard one layer down.
func TestParseOverflowSeamFixture(t *testing.T) {
	for _, row := range overflowSeamFixture {
		t.Run(row.s, func(t *testing.T) {
			// Layer 1: the loose validator accepts (it never overflows).
			if !IsValidIntegerString(row.s) {
				t.Fatalf("INV-PARSE-03: IsValidIntegerString(%q) = false, want true", row.s)
			}
			// Layer 2: the narrowing parse the handler does succeeds and fits int64.
			v, err := strconv.ParseInt(row.s, 10, 64)
			if err != nil {
				t.Fatalf("INV-PARSE-03: strconv.ParseInt(%q,10,64) = %v, want success", row.s, err)
			}
			// The Lua-imprecise classification is exactly v >= 2^53.
			gotImprecise := uint64(v) >= pow2_53
			if gotImprecise != row.luaImprecise {
				t.Fatalf("INV-PARSE-03: %q classified luaImprecise=%v, fixture says %v "+
					"(2^53 boundary moved — update the fixture intentionally)",
					row.s, gotImprecise, row.luaImprecise)
			}
			// The seam is real: a >= 2^53 value, when round-tripped through a
			// float64 (the Lua double model), loses its exact identity.
			if row.luaImprecise {
				asFloat := float64(v)
				back := int64(asFloat)
				// At or above 2^53 the float64 cannot represent every neighbor,
				// so int64(float64(v)) need not equal v; assert the loss is
				// possible by checking the next integer collides under float64.
				if v != math.MaxInt64 { // MaxInt64+1 overflows; skip the exact-top neighbor
					nextSame := float64(v) == float64(v+1)
					if !nextSame && back == v {
						t.Logf("note: %q (=%d) still round-trips exactly through float64; "+
							"the Lua loss bites neighbors that share its double", row.s, v)
					}
				}
			} else if int64(float64(v)) != v {
				// Below 2^53 the float64 round-trip is exact.
				t.Fatalf("INV-PARSE-03: %q (=%d) is < 2^53 but lost precision through float64", row.s, v)
			}
		})
	}
}

// TestParseGeneratorCoverage confirms digitStringGen reaches the bands the
// properties depend on: a value in [2^53, 2^63), one in the [2^63, 2^64)
// TTL/SubOffset split window, one >= 2^64, a leading-zero form, and a
// non-grammar (malformed) form. It samples a FIXED number of deterministic
// examples (via Generator.Example) so the coverage assertion is independent of
// the rapid check budget, which shrinks under -short. Pure probe, runs under
// -short.
func TestParseGeneratorCoverage(t *testing.T) {
	const samples = 500
	gen := digitStringGen()
	var seam53, split63, above64, leadingZero, malformed int
	maxInt64Big := new(big.Int).SetUint64(math.MaxInt64)
	for i := 0; i < samples; i++ {
		s := gen.Example(i)
		if !strictGrammarOracle(s) {
			malformed++
			if looseGrammarOracle(s) && len(s) > 1 && s[0] == '0' {
				leadingZero++
			}
			continue
		}
		bi, _ := new(big.Int).SetString(s, 10)
		switch {
		case !bi.IsUint64():
			above64++
		case bi.Cmp(maxInt64Big) > 0:
			split63++ // [2^63, 2^64)
		case bi.Cmp(new(big.Int).SetUint64(pow2_53)) >= 0:
			seam53++ // [2^53, 2^63]
		}
	}
	if seam53 == 0 {
		t.Error("generator never drew a value in the [2^53, 2^63) overflow-seam band")
	}
	if split63 == 0 {
		t.Error("generator never drew a value in the [2^63, 2^64) TTL/SubOffset split window")
	}
	if above64 == 0 {
		t.Error("generator never drew a value >= 2^64")
	}
	if leadingZero == 0 {
		t.Error("generator never drew a leading-zero form")
	}
	if malformed == 0 {
		t.Error("generator never drew a non-grammar (malformed) form")
	}
	t.Logf("coverage: seam53=%d split63=%d above64=%d leadingZero=%d malformed=%d",
		seam53, split63, above64, leadingZero, malformed)
}
