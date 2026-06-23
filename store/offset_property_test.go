package store

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// offsetWidthSafeBound is the exclusive upper bound of the domain on which
// Offset.String() lexicographic order matches numeric order. Offset.String()
// formats each uint64 field with "%016d", a MINIMUM width: it zero-pads to 16
// digits but does not cap. 10^16 is the first value that renders to 17 digits,
// so at and above it the rendered field grows and bytewise string order
// diverges from numeric order. See LB-1 (issue #27) and the divergence fixture
// in offset_width_lb1_test.go.
const offsetWidthSafeBound = uint64(1e16) // 10_000_000_000_000_000

// signOf collapses an int to its sign in {-1, 0, +1}.
func signOf(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// strcmpSign returns the sign of the bytewise C-locale comparison of a vs b.
// Go's native string comparison is bytewise unsigned, which is exactly the
// ordering Redis ZRANGEBYLEX uses over ZSET members and Lua string compare
// uses, so this is the order the read path (read.lua) actually relies on.
func strcmpSign(a, b string) int {
	return signOf(strings.Compare(a, b))
}

// TestOffsetCompareMatchesStrcmpGuarded is the CI-green half of INV-OFF-02: on
// the documented SAFE domain (both fields < 10^16) the numeric Compare and the
// bytewise string comparison of String() agree in sign. This is the invariant
// the fixed-width offset encoding depends on for read-path correctness, and it
// runs on every default `go test` build (it is pure — no Redis).
//
// The matching divergence ABOVE 10^16 is surfaced unguarded in
// TestOffsetCompareMatchesStrcmpUnguarded (build-tag quarantined) and pinned as
// a regression fixture in TestOffsetWidthCounterexampleLB1. The format fix is a
// wire/persisted-format migration tracked in docs/FINDINGS (LB-1); do NOT
// change Offset.String() here.
func TestOffsetCompareMatchesStrcmpGuarded(t *testing.T) {
	safeField := func(t *rapid.T, label string) uint64 {
		return rapid.Uint64Range(0, offsetWidthSafeBound-1).Draw(t, label)
	}
	safeOffset := func(t *rapid.T, label string) Offset {
		return Offset{
			ReadSeq:    safeField(t, label+".readSeq"),
			ByteOffset: safeField(t, label+".byteOffset"),
		}
	}
	rapid.Check(t, func(t *rapid.T) {
		a := safeOffset(t, "a")
		b := safeOffset(t, "b")
		got := signOf(Compare(a, b))
		want := strcmpSign(a.String(), b.String())
		if got != want {
			t.Fatalf("INV-OFF-02 violated on safe domain: Compare(%v,%v) sign=%d but strcmp(%q,%q) sign=%d",
				a, b, got, a.String(), b.String(), want)
		}
	})
}
