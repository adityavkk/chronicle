package redis

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// contenttype_differential_test.go is the live-Lua leg of the three-way
// content-type normalization agreement (INV-CT-01 / INV-CT-02). The pure-Go side
// — ContentTypeMatches internals reflexive/symmetric/case-insensitive, and the
// extractMediaType+toLower canonical form vs an independent spec — is pinned in
// store/contenttype_property_test.go. Here we drive generated content types
// through the REAL Lua norm_ct (common.lua) running inside Redis and assert it
// produces the same canonical string as the Go normalizeCT (store/redis) and the
// ContentTypeMatches internals (via store.ExtractMediaType + the ASCII fold),
// INCLUDING the deliberate ASCII-only fold: a non-ASCII uppercase byte must be
// left UNFOLDED by all three.
//
// Two drivers exercise norm_ct: a direct prelude probe (norm_ct(ARGV[1])) for
// the precise three-way string agreement, and the actual create path (so
// norm_ct is exercised in its real call site inside config_matches, where the
// normalized probe must equal the normalized stored content type for an
// idempotent re-create to match). Skipped under -short and when Redis is
// unreachable (newTestStore handles both).

// goNormCT reproduces the canonical normalization independently of the
// production helpers (empty -> octet-stream, strip at first ';', ASCII-only
// lowercase, non-ASCII bytes untouched), so the agreement check against the
// production normalizeCT and the live Lua norm_ct is a real oracle.
func goNormCT(ct string) string {
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

// ctDiffGen draws the same shape of content type the pure-Go property uses:
// empty, octet-stream default, bare and parameterized media types, mixed ASCII
// case, junk before ';', and non-ASCII upper bytes (>= 0x80) that must NOT fold.
func ctDiffGen() *rapid.Generator[string] {
	anchors := []string{
		"", "application/json", "Application/JSON", "APPLICATION/JSON",
		"application/octet-stream", "text/plain; charset=utf-8",
		"Text/Plain; Charset=UTF-8", ";", "; charset=utf-8",
		"application/json; ", "image/PNG",
		"\xc0\xc1/\xde", "Appli\xc9cation/json", "application/JSON\xff",
		"A", "z", "À/É", "text/Þ",
	}
	generated := rapid.Custom(func(t *rapid.T) string {
		baseRunes := rapid.SliceOfN(rapid.OneOf(
			rapid.SampledFrom([]rune{'a', 'j', 's', 'o', 'n', 'A', 'J', 'S', 'O', 'N', '/', '+', '-', '.'}),
			rapid.SampledFrom([]rune{'À', 'É', 'Þ', 'ÿ'}),
		), 0, 8).Draw(t, "base")
		base := string(baseRunes)
		if rapid.Bool().Draw(t, "hasParam") {
			return base + ";" + rapid.StringN(0, 6, 12).Draw(t, "param")
		}
		return base
	})
	return rapid.OneOf(rapid.SampledFrom(anchors), generated)
}

// TestDifferentialContentTypeNormalize is the three-way norm agreement
// (INV-CT-01 / INV-CT-02). For every generated content type it asserts the live
// Lua norm_ct, the Go normalizeCT, and the independent goNormCT spec all produce
// the same canonical string, and that store.ExtractMediaType + the ASCII fold
// agrees too. It then asserts the deliberate ASCII-only fold directly: a content
// type carrying a non-ASCII uppercase byte must be left byte-for-byte unfolded by
// the live Lua exactly as Go leaves it (no Unicode folding via C-locale
// string.lower).
func TestDifferentialContentTypeNormalize(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// A driver appended to the common.lua prelude so it can call the local
	// norm_ct. Built via redis.NewScript (never a bare EVAL), per the forbidigo
	// rule in .golangci.yml.
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		t.Fatalf("read common.lua: %v", err)
	}
	const driver = `return norm_ct(ARGV[1])`
	normProbe := redis.NewScript(string(prelude) + "\n" + driver)

	rapid.Check(t, func(t *rapid.T) {
		ct := ctDiffGen().Draw(t, "ct")

		raw, err := normProbe.Run(ctx, s.client, nil, ct).Result()
		if err != nil {
			t.Fatalf("norm_ct probe run for %q: %v", ct, err)
		}
		luaNorm, ok := raw.(string)
		if !ok {
			t.Fatalf("norm_ct(%q) returned non-string %T %v", ct, raw, raw)
		}

		goNorm := normalizeCT(ct)                           // production Go (store/redis)
		specNorm := goNormCT(ct)                            // independent spec
		internal := store.ExtractMediaType(ctOrDefault(ct)) // ContentTypeMatches's media-type extraction

		if goNorm != specNorm {
			t.Fatalf("INV-CT-02: Go normalizeCT(%q)=%q != spec %q", ct, goNorm, specNorm)
		}
		if luaNorm != goNorm {
			t.Fatalf("INV-CT-01/02 DIVERGENCE: live Lua norm_ct(%q)=%q but Go normalizeCT=%q",
				ct, luaNorm, goNorm)
		}
		// The ContentTypeMatches internals extract the same media type; lowering
		// it with the ASCII fold must reach the same canonical string.
		if foldASCII(internal) != goNorm {
			t.Fatalf("INV-CT-02: ContentTypeMatches internals %q (folded %q) != normalizeCT %q for %q",
				internal, foldASCII(internal), goNorm, ct)
		}

		// Non-ASCII fold check: every byte >= 0x80 in the canonical string must be
		// byte-identical to the source media type's corresponding byte — i.e. the
		// live Lua left it UNFOLDED. (Lowering only changes ASCII A-Z, so the
		// non-ASCII bytes of luaNorm must match the non-lowered media type bytes.)
		mt := store.ExtractMediaType(ctOrDefault(ct))
		assertNonASCIIUnfolded(t, ct, mt, luaNorm)
	})
}

// ctOrDefault applies the empty -> octet-stream default the way all three
// normalizers do, so the media-type extraction below operates on the same input.
func ctOrDefault(ct string) string {
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// foldASCII lowercases ASCII A-Z only (the equalFold/normalizeCT fold), leaving
// bytes >= 0x80 untouched.
func foldASCII(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// assertNonASCIIUnfolded checks that every non-ASCII byte of the (pre-fold) media
// type survives byte-identically into the Lua-normalized string — i.e. the live
// C-locale string.lower folded ASCII only and left >= 0x80 bytes alone. Since
// the fold is length-preserving and only rewrites A-Z, luaNorm must equal mt
// with A-Z lowered; in particular every >= 0x80 byte is unchanged.
func assertNonASCIIUnfolded(t *rapid.T, ct, mt, luaNorm string) {
	if len(luaNorm) != len(mt) {
		t.Fatalf("INV-CT-02: norm_ct(%q) changed length (%d -> %d) — fold should be length-preserving",
			ct, len(mt), len(luaNorm))
	}
	for i := 0; i < len(mt); i++ {
		c := mt[i]
		if c >= 0x80 {
			if luaNorm[i] != c {
				t.Fatalf("INV-CT-02 DIVERGENCE: live Lua folded a non-ASCII byte at %d: %q -> %#x (was %#x) for %q",
					i, ct, luaNorm[i], c, ct)
			}
		}
	}
}

// TestDifferentialContentTypeCreatePath exercises norm_ct in its REAL call site:
// config_matches inside create.lua. A stream is created with content type A;
// re-creating with content type B is idempotent (MATCHED) exactly when
// ContentTypeMatches(A, B) — which means norm_ct(A) == norm_ct(B) on the Lua
// side. This pins that the live normalization used for idempotency agrees with
// the Go ContentTypeMatches over generated A/B pairs (mixed case, parameters,
// and the octet-stream default). Skipped under -short / when Redis is unreachable.
func TestDifferentialContentTypeCreatePath(t *testing.T) {
	s := newTestStore(t)

	rapid.Check(t, func(t *rapid.T) {
		a := ctDiffGen().Draw(t, "a")
		b := ctDiffGen().Draw(t, "b")

		path := testPath("ctcreate")
		if _, _, err := s.Create(path, store.CreateOptions{ContentType: a}); err != nil {
			t.Fatalf("Create(ct=%q): %v", a, err)
		}
		// Re-create with B and the SAME (default) non-fork config: idempotent
		// iff the content types match.
		_, created, err := s.Create(path, store.CreateOptions{ContentType: b})

		want := store.ContentTypeMatches(a, b)
		if err == nil {
			if created {
				t.Fatalf("re-create ct=%q over ct=%q reported newly-created", b, a)
			}
			// MATCHED: the live config_matches treated A and B as the same CT.
			if !want {
				t.Fatalf("INV-CT-01 DIVERGENCE: live create MATCHED ct=%q over ct=%q but "+
					"Go ContentTypeMatches=false", b, a)
			}
		} else {
			if !errors.Is(err, store.ErrConfigMismatch) {
				t.Fatalf("re-create ct=%q over ct=%q: unexpected error %v", b, a, err)
			}
			if want {
				t.Fatalf("INV-CT-01 DIVERGENCE: live create MISMATCH ct=%q over ct=%q but "+
					"Go ContentTypeMatches=true", b, a)
			}
		}
	})
}
