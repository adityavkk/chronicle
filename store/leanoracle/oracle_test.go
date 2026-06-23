//go:build leanoracle

package leanoracle

import (
	"errors"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// TestBridgeLinksAndCompares proves the cgo bridge links the vendored archive,
// initializes the Lean runtime, and round-trips Offset.Compare against the Go
// core on a handful of known cases (including the LB-1-relevant boundary
// values, all kept inside the < 10^16 safe domain). INV-OFF-01.
func TestBridgeLinksAndCompares(t *testing.T) {
	o := New()

	cases := []struct {
		name string
		a, b store.Offset
	}{
		{"equal zero", store.Offset{}, store.Offset{}},
		{"byteOffset less", store.Offset{ByteOffset: 5}, store.Offset{ByteOffset: 9}},
		{"byteOffset greater", store.Offset{ByteOffset: 9}, store.Offset{ByteOffset: 5}},
		{"readSeq dominates", store.Offset{ReadSeq: 2}, store.Offset{ReadSeq: 1, ByteOffset: 99}},
		{"readSeq equal byteOffset tie", store.Offset{ReadSeq: 7, ByteOffset: 7}, store.Offset{ReadSeq: 7, ByteOffset: 7}},
		{"just below 1e16 boundary", store.Offset{ByteOffset: 1e16 - 1}, store.Offset{ByteOffset: 1e16 - 2}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want := signOf(store.Compare(c.a, c.b))
			got := signOf(o.Compare(c.a, c.b))
			if got != want {
				t.Fatalf("Lean Compare(%v,%v) sign = %d, Go core = %d", c.a, c.b, got, want)
			}
		})
	}
}

// TestBridgeValidateProducer proves the bridge round-trips the full
// ValidateProducer reply tuple AND the persist/no-persist decision against the
// Go core, across all six outcomes of the state machine. INV-PROD-08.
func TestBridgeValidateProducer(t *testing.T) {
	o := New()
	const now = int64(1_700_000_000)

	st := func(epoch, lastSeq int64) *store.ProducerState {
		return &store.ProducerState{Epoch: epoch, LastSeq: lastSeq, LastUpdated: now - 100}
	}

	cases := []struct {
		name       string
		state      *store.ProducerState
		epoch, seq int64
	}{
		{"first contact seq 0 accepted", nil, 0, 0},
		{"first contact any epoch seq 0 accepted", nil, 7, 0},
		{"first contact nonzero seq is a gap", nil, 0, 3},
		{"stale epoch fenced", st(5, 9), 4, 0},
		{"epoch bump must start at seq 0", st(5, 9), 6, 1},
		{"epoch bump at seq 0 accepted", st(5, 9), 6, 0},
		{"duplicate seq", st(2, 4), 2, 4},
		{"old duplicate seq", st(2, 4), 2, 1},
		{"next seq accepted", st(2, 4), 2, 5},
		{"seq gap rejected", st(2, 4), 2, 7},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantRes, wantState, wantErr := store.ValidateProducer(c.state, c.epoch, c.seq, now)
			gotRes, gotState, gotErr := o.ValidateProducer(c.state, c.epoch, c.seq, now)

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("err = %v, Go core %v", gotErr, wantErr)
			}
			if gotRes.ProducerResult != wantRes.ProducerResult {
				t.Errorf("ProducerResult = %v, Go core %v", gotRes.ProducerResult, wantRes.ProducerResult)
			}
			if gotRes.CurrentEpoch != wantRes.CurrentEpoch {
				t.Errorf("CurrentEpoch = %d, Go core %d", gotRes.CurrentEpoch, wantRes.CurrentEpoch)
			}
			if gotRes.ExpectedSeq != wantRes.ExpectedSeq {
				t.Errorf("ExpectedSeq = %d, Go core %d", gotRes.ExpectedSeq, wantRes.ExpectedSeq)
			}
			if gotRes.ReceivedSeq != wantRes.ReceivedSeq {
				t.Errorf("ReceivedSeq = %d, Go core %d", gotRes.ReceivedSeq, wantRes.ReceivedSeq)
			}
			if gotRes.LastSeq != wantRes.LastSeq {
				t.Errorf("LastSeq = %d, Go core %d", gotRes.LastSeq, wantRes.LastSeq)
			}

			// Persist/no-persist decision and the persisted fields.
			switch {
			case wantState == nil && gotState != nil:
				t.Errorf("persist = true, Go core persists nothing (state %+v)", gotState)
			case wantState != nil && gotState == nil:
				t.Errorf("persist = false, Go core persists %+v", wantState)
			case wantState != nil && gotState != nil:
				if gotState.Epoch != wantState.Epoch || gotState.LastSeq != wantState.LastSeq {
					t.Errorf("persisted = %+v, Go core %+v", gotState, wantState)
				}
			}
		})
	}
}

// BenchmarkCompare measures the per-call cgo overhead of lean_offset_compare,
// the cheapest entry point, at the call volume the differential/fuzz loop runs
// at. This is the go/no-go spike number: ns/op is the per-call cost the harness
// pays to consult the third oracle. See docs/SPIKE-lean-cgo.md.
func BenchmarkCompare(b *testing.B) {
	o := New()
	a := store.Offset{ReadSeq: 3, ByteOffset: 100}
	c := store.Offset{ReadSeq: 3, ByteOffset: 200}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		sink += o.Compare(a, c)
	}
	_ = sink
}

// BenchmarkValidateProducer measures the per-call cost of the full producer
// reply tuple (nine cgo calls assembled into AppendResult + *ProducerState).
func BenchmarkValidateProducer(b *testing.B) {
	o := New()
	st := &store.ProducerState{Epoch: 2, LastSeq: 4, LastUpdated: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = o.ValidateProducer(st, 2, 5, 1)
	}
}

// signOf collapses an int to its sign in {-1, 0, +1} (local copy so the bridge
// self-test does not depend on the store-package test helpers).
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
