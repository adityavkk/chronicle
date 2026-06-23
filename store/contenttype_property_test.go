package store

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// contenttype_property_test.go pins the pure-Go content-type normalization
// surface (INV-CT-01 / INV-CT-02). Chronicle normalizes a content type in three
// independent places — store.ContentTypeMatches (extractMediaType + the
// ASCII-only equalFold), store/redis.normalizeCT, and the Lua norm_ct — and
// trusts they agree. This file pins the algebraic shape of ContentTypeMatches
// (reflexive, symmetric, base-media-type case-insensitive, parameter-stripping)
// AND the pure-Go side of the three-way agreement: the canonical string the
// ContentTypeMatches internals compute (extractMediaType + toLower) must equal
// what a clean re-implementation of the same rule produces, including the
// DELIBERATE ASCII-only fold (a non-ASCII uppercase byte must be left alone).
//
// The live-Lua leg (norm_ct, and store/redis.normalizeCT against it) is in
// store/redis/contenttype_differential_test.go. canonicalCT below is the spec
// the differential leg also targets, so the two files agree by construction.

// canonicalCT is the reference normalization the issue pins all three
// implementations to: empty -> "application/octet-stream", strip parameters at
// the first ';', then ASCII-only lowercase (A-Z), leaving every byte >= 0x80
// untouched (matching Lua's C-locale string.lower, NOT Unicode folding). It is
// written independently of the production helpers so a property comparing the
// production path against it is a real oracle check, not a tautology.
func canonicalCT(ct string) string {
	if ct == "" {
		ct = "application/octet-stream"
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	b := []byte(ct)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// ctTokenGen draws a content-type-like string designed to exercise every branch
// of the normalization: empty, the octet-stream default, bare and parameterized
// media types, mixed ASCII case, junk before the ';', and crucially non-ASCII
// upper bytes (e.g. 0xC0..0xDE, the Latin-1 uppercase range) that ASCII folding
// must leave UNFOLDED. It deliberately includes ';' so the parameter-stripping
// branch fires, and a leading/trailing/all-uppercase mix so equalFold's fold is
// observable on the base type.
func ctTokenGen() *rapid.Generator[string] {
	// Fixed, readable anchors covering the documented cases.
	anchors := []string{
		"",
		"application/json",
		"Application/JSON",
		"APPLICATION/JSON",
		"application/octet-stream",
		"text/plain; charset=utf-8",
		"text/plain;charset=UTF-8",
		"Text/Plain; Charset=UTF-8",
		";",                  // empty base, all parameters
		"; charset=utf-8",    // empty base after strip
		"application/json; ", // trailing param marker
		"image/png",
		"IMAGE/PNG",
		"\xc0\xc1/\xde",        // non-ASCII upper bytes only (must NOT fold)
		"Appli\xc9cation/json", // mixed ASCII upper + a non-ASCII byte
		"application/JSON\xff", // a 0xFF byte in the base
		"a", "A", "Z", "z",
	}
	// A generated mixer: a base of letters (both cases) + occasional non-ASCII
	// upper bytes, optionally followed by a ';' and arbitrary parameter bytes.
	generated := rapid.Custom(func(t *rapid.T) string {
		baseRunes := rapid.SliceOfN(rapid.OneOf(
			rapid.SampledFrom([]rune{
				'a', 'b', 'j', 's', 'o', 'n',
				'A', 'B', 'J', 'S', 'O', 'N',
				'/', '+', '-', '.',
			}),
			// Latin-1 uppercase letters: bytes >= 0x80, which ASCII folding
			// must never touch. Drawn as runes so they round-trip through the
			// string but stay multibyte (the fold operates byte-wise).
			rapid.SampledFrom([]rune{'À', 'É', 'Þ', 'ÿ'}),
		), 0, 8).Draw(t, "base")
		base := string(baseRunes)
		if rapid.Bool().Draw(t, "hasParam") {
			param := rapid.StringN(0, 6, 12).Draw(t, "param")
			return base + ";" + param
		}
		return base
	})
	return rapid.OneOf(rapid.SampledFrom(anchors), generated)
}

// TestContentTypeMatchesReflexive asserts INV-CT-01's reflexivity:
// ContentTypeMatches(x, x) is true for every content type, including empty
// (which normalizes to the octet-stream default), parameterized, mixed-case,
// and non-ASCII inputs. Pure — runs on every build including -short.
func TestContentTypeMatchesReflexive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		x := ctTokenGen().Draw(t, "x")
		if !ContentTypeMatches(x, x) {
			t.Fatalf("INV-CT-01 reflexivity violated: ContentTypeMatches(%q, %q) = false", x, x)
		}
	})
}

// TestContentTypeMatchesSymmetric asserts INV-CT-01's symmetry:
// ContentTypeMatches(a, b) == ContentTypeMatches(b, a) for every pair.
func TestContentTypeMatchesSymmetric(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := ctTokenGen().Draw(t, "a")
		b := ctTokenGen().Draw(t, "b")
		if ContentTypeMatches(a, b) != ContentTypeMatches(b, a) {
			t.Fatalf("INV-CT-01 symmetry violated: ContentTypeMatches(%q,%q)=%v but (%q,%q)=%v",
				a, b, ContentTypeMatches(a, b), b, a, ContentTypeMatches(b, a))
		}
	})
}

// TestContentTypeMatchesCaseInsensitiveBase asserts INV-CT-01's case
// insensitivity ON THE BASE MEDIA TYPE: ASCII-randomizing the case of the base
// (before any ';') must not change the match decision against the original.
// This pins that equalFold folds A-Z, and only the base type participates.
func TestContentTypeMatchesCaseInsensitiveBase(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		x := ctTokenGen().Draw(t, "x")
		// Build a case-randomized variant of x's BASE media type only, leaving
		// any parameters (and all non-ASCII bytes) byte-identical.
		flips := rapid.SliceOfN(rapid.Bool(), len(x), len(x)).Draw(t, "flips")
		b := []byte(x)
		semi := strings.IndexByte(x, ';')
		baseEnd := len(b)
		if semi >= 0 {
			baseEnd = semi
		}
		for i := 0; i < baseEnd; i++ {
			if !flips[i] {
				continue
			}
			c := b[i]
			switch {
			case c >= 'a' && c <= 'z':
				b[i] = c - ('a' - 'A')
			case c >= 'A' && c <= 'Z':
				b[i] = c + ('a' - 'A')
			}
		}
		variant := string(b)
		if !ContentTypeMatches(x, variant) {
			t.Fatalf("INV-CT-01 base case-insensitivity violated: ContentTypeMatches(%q, %q) = false",
				x, variant)
		}
		// And the parameter section must NOT participate: appending arbitrary
		// parameters to a NON-EMPTY bare base must still match the bare base.
		// (The empty-string case is excluded deliberately: "" defaults to
		// application/octet-stream, but "; charset=..." has an empty BASE that
		// is NOT the whole-empty default, so the two correctly do NOT match —
		// the empty default applies to the whole value, not the stripped base.)
		if semi < 0 && x != "" {
			withParam := x + "; charset=anything"
			if !ContentTypeMatches(x, withParam) {
				t.Fatalf("INV-CT-02 parameter-stripping violated: ContentTypeMatches(%q, %q) = false",
					x, withParam)
			}
		}
	})
}

// TestContentTypeNormalizerAgreesWithCanonical pins the pure-Go side of the
// three-way normalizer agreement (INV-CT-01 / INV-CT-02): the canonical string
// the ContentTypeMatches internals compute — extractMediaType followed by the
// ASCII fold (exposed here via toLower(extractMediaType(x))) — must equal the
// independent canonicalCT spec for every generated content type. It also pins
// the algebraic bridge: ContentTypeMatches(a, b) MUST equal
// (canonicalCT(a) == canonicalCT(b)), so the predicate and the canonical form
// are the same decision. The live Lua norm_ct and store/redis.normalizeCT are
// pinned to the SAME canonicalCT in the differential test.
func TestContentTypeNormalizerAgreesWithCanonical(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		x := ctTokenGen().Draw(t, "x")

		// extractMediaType + toLower is exactly what ContentTypeMatches folds
		// over (it normalizes empty first, then folds the base). Mirror that
		// empty-normalization here so we compare the same canonical form.
		in := x
		if in == "" {
			in = "application/octet-stream"
		}
		goCanon := toLower(extractMediaType(in))
		if goCanon != canonicalCT(x) {
			t.Fatalf("INV-CT-02: Go internals canonical %q != canonicalCT %q for input %q",
				goCanon, canonicalCT(x), x)
		}

		// The predicate is the equality of canonical forms.
		y := ctTokenGen().Draw(t, "y")
		if ContentTypeMatches(x, y) != (canonicalCT(x) == canonicalCT(y)) {
			t.Fatalf("INV-CT-01: ContentTypeMatches(%q,%q)=%v but canonicalCT equality=%v",
				x, y, ContentTypeMatches(x, y), canonicalCT(x) == canonicalCT(y))
		}
	})
}

// TestEqualFoldIsASCIIOnly pins INV-CT-02's most drift-prone clause: equalFold
// (via ContentTypeMatches) folds ASCII A-Z ONLY and must NOT perform Unicode
// case folding. The property constructs a base type containing a non-ASCII
// uppercase byte, then forms a variant where that exact byte is replaced by its
// Unicode-lowercase counterpart (which a Unicode fold like strings.EqualFold
// WOULD treat as equal). ContentTypeMatches must report them as DIFFERENT,
// pinning the deliberate divergence from strings.EqualFold called out in the
// source comment. A regression that "improves" equalFold to Unicode folding
// trips this immediately.
func TestEqualFoldIsASCIIOnly(t *testing.T) {
	// Pairs of (upper rune, lower rune) that strings.EqualFold treats as equal
	// but ASCII folding (byte-wise A-Z only) must treat as DIFFERENT, because
	// both bytes are >= 0x80.
	unicodePairs := []struct{ upper, lower rune }{
		{'À', 'à'}, // À / à
		{'É', 'é'}, // É / é
		{'Þ', 'þ'}, // Þ / þ
		{'Ā', 'ā'}, // Ā / ā
		{'А', 'а'}, // Cyrillic А / а
	}
	rapid.Check(t, func(t *rapid.T) {
		p := rapid.SampledFrom(unicodePairs).Draw(t, "pair")
		// Sanity: these are genuinely Unicode-equal but byte-different, so the
		// property is non-vacuous (a Unicode fold WOULD match them).
		su, sl := string(p.upper), string(p.lower)
		if !strings.EqualFold(su, sl) {
			t.Fatalf("test fixture broken: %q / %q are not Unicode-equal", su, sl)
		}
		if su == sl {
			t.Fatalf("test fixture broken: %q / %q are byte-identical", su, sl)
		}
		// Embed the rune in an otherwise-identical base type and assert the
		// ASCII fold leaves it UNFOLDED (so the two are NOT a content-type match).
		a := "text/" + su
		b := "text/" + sl
		if ContentTypeMatches(a, b) {
			t.Fatalf("INV-CT-02 violated: ContentTypeMatches(%q,%q)=true — a Unicode fold leaked in; "+
				"equalFold must fold ASCII A-Z only (C-locale string.lower)", a, b)
		}
		// And the canonical form must likewise leave the byte untouched.
		if canonicalCT(a) == canonicalCT(b) {
			t.Fatalf("INV-CT-02: canonicalCT folded a non-ASCII byte: %q vs %q", a, b)
		}
	})
}

// TestContentTypeGeneratorCoverage confirms the ctTokenGen generator actually
// reaches the branches the properties care about — empty, parameterized (';'),
// ASCII-uppercase base, and a non-ASCII (>= 0x80) byte — rather than relying on
// uniform luck. It samples a FIXED number of deterministic examples (via
// Generator.Example) so the coverage assertion is independent of the rapid check
// budget, which shrinks under -short. Pure probe (no Redis), runs under -short.
func TestContentTypeGeneratorCoverage(t *testing.T) {
	const samples = 500
	gen := ctTokenGen()
	var empty, withSemi, asciiUpper, nonASCII int
	for i := 0; i < samples; i++ {
		x := gen.Example(i)
		if x == "" {
			empty++
		}
		if strings.IndexByte(x, ';') >= 0 {
			withSemi++
		}
		base := x
		if i := strings.IndexByte(x, ';'); i >= 0 {
			base = x[:i]
		}
		for j := 0; j < len(base); j++ {
			c := base[j]
			if c >= 'A' && c <= 'Z' {
				asciiUpper++
				break
			}
		}
		for j := 0; j < len(x); j++ {
			if x[j] >= 0x80 {
				nonASCII++
				break
			}
		}
	}
	if empty == 0 {
		t.Error("generator never drew the empty content type")
	}
	if withSemi == 0 {
		t.Error("generator never drew a parameterized content type (with ';')")
	}
	if asciiUpper == 0 {
		t.Error("generator never drew an ASCII-uppercase base type")
	}
	if nonASCII == 0 {
		t.Error("generator never drew a non-ASCII (>= 0x80) byte")
	}
	t.Logf("coverage: empty=%d withSemi=%d asciiUpper=%d nonASCII=%d", empty, withSemi, asciiUpper, nonASCII)
}
