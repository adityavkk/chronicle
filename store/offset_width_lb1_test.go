package store

import (
	"strings"
	"testing"
)

// TestOffsetWidthCounterexampleLB1 is the committed regression fixture for
// LB-1: Offset.String() uses fmt.Sprintf("%016d_%016d"), where %016d is a
// MINIMUM width, not a maximum. At and above 10^16 a field renders to 17+
// digits (max uint64 is 20 digits), the zero-padding stops keeping every
// rendered field the same length, and bytewise string order diverges from
// numeric order. Offset.Compare is correct (it compares the numeric fields),
// so the Go core and any string-order consumer DISAGREE in the >= 10^16 region.
//
// Where it bites: Redis stores stream frames as ZSET members keyed by the
// offset string (encodeFrame in store/redis/keys.go, which also hardcodes
// offsetStrLen = 33 on the same fixed-width assumption), and the read path
// range-scans them with ZRANGEBYLEX (store/redis/scripts/read.lua). Read
// correctness rests on lexicographic member order equalling numeric frame
// order, so past a 10^16-byte ByteOffset (~10 PB on one stream) the ZSET
// ordering inverts at the boundary and reads return frames out of order —
// silent data-plane corruption. The MemoryStore oracle never reproduces it
// because it reads by comparing the numeric ByteOffset field, not the string.
//
// These pairs are the minimal counterexamples rapid shrank to in the unguarded
// property TestOffsetCompareMatchesStrcmpUnguarded (issue #27,
// offset_property_unguarded_test.go). This fixture asserts the KNOWN divergence
// (numeric Compare says "<", bytewise String() order says ">") so it documents
// the bug in machine-checked form and keeps CI green; any future change to the
// rendered width MUST update this fixture intentionally.
//
// The fix (bump %016d to %020d, or enforce a runtime invariant field < 10^16)
// is a wire/persisted-format migration and is tracked as the LB-1 migration
// finding (see the epic) — NOT changed here. The sibling LB-2 (Stream-Seq, same
// digit-width hazard one layer up) shares this "string order must match
// intended order" failure class.
func TestOffsetWidthCounterexampleLB1(t *testing.T) {
	cases := []struct {
		name string
		a, b Offset
	}{
		{
			// Canonical minimal pair: the ByteOffset field crosses 10^16.
			// 9999999999999999 -> 16 digits, 10000000000000000 -> 17 digits.
			name: "byteoffset crosses 10^16 boundary",
			a:    Offset{ReadSeq: 0, ByteOffset: 9999999999999999},
			b:    Offset{ReadSeq: 0, ByteOffset: 10000000000000000},
		},
		{
			// The exact pair rapid shrank to (both fields pinned at the
			// boundary value in ReadSeq, ByteOffset crosses 10^16).
			name: "rapid-shrunk boundary pair",
			a:    Offset{ReadSeq: 9999999999999999, ByteOffset: 9999999999999999},
			b:    Offset{ReadSeq: 9999999999999999, ByteOffset: 10000000000000000},
		},
		{
			// ReadSeq field crosses 10^16 (the higher-order field): the
			// divergence is not confined to ByteOffset.
			name: "readseq crosses 10^16 boundary",
			a:    Offset{ReadSeq: 9999999999999999, ByteOffset: 0},
			b:    Offset{ReadSeq: 10000000000000000, ByteOffset: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Numeric order: a is strictly less than b.
			if got := Compare(tc.a, tc.b); got != -1 {
				t.Fatalf("expected numeric Compare(a,b) == -1 (a < b), got %d", got)
			}
			// String order DIVERGES: a sorts AFTER b bytewise, the documented
			// LB-1 inversion. (strings.Compare > 0 means a > b lexically.)
			sa, sb := tc.a.String(), tc.b.String()
			if got := strings.Compare(sa, sb); got <= 0 {
				t.Fatalf("expected LB-1 divergence: string order a > b (strings.Compare > 0), "+
					"got %d for a=%q b=%q — has Offset.String() been changed? this fixture must be updated intentionally",
					got, sa, sb)
			}
			// And the lengths differ, which is the root cause: %016d is a
			// minimum width, so the >= 10^16 rendering grew past 16 digits.
			if len(sa) == len(sb) {
				t.Fatalf("expected differing rendered lengths at the 10^16 boundary, both were %d: a=%q b=%q",
					len(sa), sa, sb)
			}
		})
	}
}
