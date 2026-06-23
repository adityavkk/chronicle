//go:build leanoracle

package redis

import (
	"errors"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/store/leanoracle"
)

// This file holds the REAL third-oracle hooks, built only with `-tags leanoracle`
// (which links the vendored Lean→C archive via cgo). It makes the producer/offset
// differential a TRIPLE oracle: Go core vs live Lua (asserted in the harness) vs
// the proven Lean model (asserted here against the Go core). Because the Go core
// is pinned to the live Lua subject in the harness, Lean-agrees-with-Go pins all
// three to one statement. (P1.2, issue #31.)
//
// Enable with: go test -tags leanoracle ./store/redis/...
// (needs cgo + the vendored store/leanoracle/libchronicle_oracle.a; no Lean
// toolchain is required to consume the vendored archive.)

// leanOracle is the process-wide proven-Lean oracle; the Lean runtime is
// initialized once on first use.
var leanOracle = leanoracle.New()

// checkLeanProducer asserts the proven Lean model reproduces the Go core's full
// ValidateProducer reply tuple AND the persist/no-persist decision for one
// (state, epoch, seq) case. Any pairwise disagreement fails the differential.
// [INV-PROD-08]
func checkLeanProducer(t *testing.T, state *store.ProducerState, epoch, seq, now int64) {
	t.Helper()

	wantRes, wantState, wantErr := store.ValidateProducer(state, epoch, seq, now)
	gotRes, gotState, gotErr := leanOracle.ValidateProducer(state, epoch, seq, now)

	if !errors.Is(gotErr, wantErr) {
		t.Errorf("lean oracle err = %v, Go core %v", gotErr, wantErr)
	}
	if gotRes.ProducerResult != wantRes.ProducerResult {
		t.Errorf("lean oracle ProducerResult = %v, Go core %v", gotRes.ProducerResult, wantRes.ProducerResult)
	}
	if gotRes.CurrentEpoch != wantRes.CurrentEpoch {
		t.Errorf("lean oracle CurrentEpoch = %d, Go core %d", gotRes.CurrentEpoch, wantRes.CurrentEpoch)
	}
	if gotRes.ExpectedSeq != wantRes.ExpectedSeq {
		t.Errorf("lean oracle ExpectedSeq = %d, Go core %d", gotRes.ExpectedSeq, wantRes.ExpectedSeq)
	}
	if gotRes.ReceivedSeq != wantRes.ReceivedSeq {
		t.Errorf("lean oracle ReceivedSeq = %d, Go core %d", gotRes.ReceivedSeq, wantRes.ReceivedSeq)
	}
	if gotRes.LastSeq != wantRes.LastSeq {
		t.Errorf("lean oracle LastSeq = %d, Go core %d", gotRes.LastSeq, wantRes.LastSeq)
	}

	// Persist/no-persist decision and the persisted fields.
	switch {
	case wantState == nil && gotState != nil:
		t.Errorf("lean oracle persists %+v, Go core persists nothing", gotState)
	case wantState != nil && gotState == nil:
		t.Errorf("lean oracle persists nothing, Go core persists %+v", wantState)
	case wantState != nil && gotState != nil:
		if gotState.Epoch != wantState.Epoch || gotState.LastSeq != wantState.LastSeq {
			t.Errorf("lean oracle persisted = %+v, Go core %+v", gotState, wantState)
		}
	}
}

// offsetWidthSafeBound is the exclusive upper bound of the LB-1 safe domain:
// 10^16 is the first ByteOffset/ReadSeq value whose "%016d" rendering grows past
// 16 digits, where Offset.String() byte-lex order diverges from numeric order
// (issue #27). It is NOT a boundary for Lean-vs-Go: both store.Compare and the
// Lean compare are NUMERIC, so they agree across the whole 2^64 domain. The
// boundary is recorded here only to LABEL the LB-1 region per INV-OFF-02 — the
// Compare-vs-String-lex divergence is asserted separately in
// store/offset_width_lb1_test.go and is not reintroduced here (no wire change).
const offsetWidthSafeBound = uint64(1e16)

// checkLeanOffsetCompare asserts the proven Lean model agrees with the Go core
// on sign(Compare(a,b)) for one offset pair. Inside the < 10^16 safe domain this
// is the pinned INV-OFF-01/INV-OFF-02 statement; at/above 10^16 the Lean-vs-Go
// numeric agreement still holds (and is asserted), with the pair LABELED as
// LB-1 so a future divergence there is read as expected rather than spurious.
func checkLeanOffsetCompare(t *testing.T, a, b store.Offset) {
	t.Helper()

	want := signOfInt(store.Compare(a, b))
	got := signOfInt(leanOracle.Compare(a, b))
	if got != want {
		lb1 := a.ReadSeq >= offsetWidthSafeBound || a.ByteOffset >= offsetWidthSafeBound ||
			b.ReadSeq >= offsetWidthSafeBound || b.ByteOffset >= offsetWidthSafeBound
		t.Errorf("lean oracle sign(Compare(%v,%v)) = %d, Go core %d (LB-1 region=%v)", a, b, got, want, lb1)
	}
}

// signOfInt collapses an int to its sign in {-1, 0, +1}.
func signOfInt(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}
