//go:build offset_lb1_surface

// This file holds the UNGUARDED surfacing artifact for LB-1 (issue #27). It is
// quarantined behind the `offset_lb1_surface` build tag so a default
// `go test ./...` / `go test -race ./...` stays GREEN while the known bug
// stands. Run it deliberately to (re)produce the boundary counterexample:
//
//	go test -tags offset_lb1_surface -run TestOffsetCompareMatchesStrcmpUnguarded ./store/
//
// It WILL FAIL by design: rapid shrinks to a minimal pair with at least one
// field >= 10^16 where numeric Compare and bytewise String() order disagree.
// The shrunk pair is committed as the regression fixture
// TestOffsetWidthCounterexampleLB1 (offset_width_lb1_test.go). The fix is a
// wire/persisted-format migration tracked as the LB-1 finding; this surfacing
// test never changes Offset.String().
package store

import (
	"testing"

	"pgregory.net/rapid"
)

// TestOffsetCompareMatchesStrcmpUnguarded asserts sign(Compare(a,b)) ==
// sign(strcmp(a.String(), b.String())) over the FULL uint64 domain (no
// < 10^16 precondition). It fails at the %016d minimum-width boundary,
// documenting LB-1. Quarantined by build tag (see file header).
func TestOffsetCompareMatchesStrcmpUnguarded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := offsetGen().Draw(t, "a")
		b := offsetGen().Draw(t, "b")
		got := signOf(Compare(a, b))
		want := strcmpSign(a.String(), b.String())
		if got != want {
			t.Fatalf("LB-1 divergence: Compare(%v,%v) sign=%d but strcmp(%q,%q) sign=%d",
				a, b, got, a.String(), b.String(), want)
		}
	})
}
