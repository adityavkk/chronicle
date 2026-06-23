//go:build leanoracle

// Package leanoracle is the THIRD differential oracle (P1.2, issue #31): the
// proven Lean producer/offset model, compiled to C via Lean's C backend and
// linked into Go through cgo. It exposes the same producer/offset surface the
// Go core (store.ValidateProducer, store.Compare) and the live Lua mirror
// implement, so the differential harness can pin one statement from three
// independent sides — Go core vs live Lua vs proven Lean.
//
// The static archive store/leanoracle/libchronicle_oracle.a is the VENDORED
// compiled C: it bundles the export shims, the two proven cores, and the slice
// of the Lean runtime they pull in, so building this package needs NO Lean
// toolchain — only a C toolchain and libc++ (which cgo already requires). The
// archive is rebuilt by store/leanoracle/scripts/build-lean-oracle.sh; the
// recorded toolchain pin and source commit live in PROVENANCE.txt, and the CI
// drift-guard rebuilds it and asserts byte-identity.
//
// This package is behind the `leanoracle` build tag so routine `go build` /
// `go test` stay cgo-free and need no archive present. Enable it with
// `go test -tags leanoracle ./...`.
package leanoracle

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR} -lchronicle_oracle -lc++
#include "chronicle_oracle.h"
*/
import "C"

import (
	"sync"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// initOnce guards the one-time Lean runtime bring-up. lean_initialize_runtime_module
// sets up the allocator and runtime, the module initializer runs the (trivial,
// allocation-free) top-level init for the Producer/Offset/Extern modules, and
// lean_io_mark_end_initialization flips the runtime out of init mode. After this
// the scalar entry points are plain C calls with no heap traffic.
var initOnce sync.Once

func ensureInit() {
	initOnce.Do(func() {
		C.lean_initialize_runtime_module()
		C.initialize_chronicleoracle_Chronicle_Extern(C.uint8_t(1))
		C.lean_io_mark_end_initialization()
	})
}

// LeanOracle is the proven-Lean-via-C implementation of the producer/offset
// surface. Construct it with New so the Lean runtime is initialized exactly once.
type LeanOracle struct{}

// New returns a LeanOracle with the Lean runtime initialized. Safe to call
// repeatedly and from multiple goroutines; the underlying init runs once.
func New() *LeanOracle {
	ensureInit()
	return &LeanOracle{}
}

// Compare returns the proven Lean lexicographic comparison of a and b as
// -1 / 0 / 1, mirroring store.Compare. The C entry returns int8_t; Go widens it
// as a signed value.
func (o *LeanOracle) Compare(a, b store.Offset) int {
	r := C.lean_offset_compare(
		C.uint64_t(a.ReadSeq), C.uint64_t(a.ByteOffset),
		C.uint64_t(b.ReadSeq), C.uint64_t(b.ByteOffset))
	return int(C.int8_t(r))
}

// ValidateProducer runs the proven Lean idempotent-producer state machine and
// translates its reply back into the Go core's surface:
// (store.AppendResult, *store.ProducerState, error). It mirrors
// store.ValidateProducer field-for-field, including the persist/no-persist
// decision (*ProducerState non-nil iff persist == 1).
func (o *LeanOracle) ValidateProducer(state *store.ProducerState, epoch, seq, nowUnix int64) (store.AppendResult, *store.ProducerState, error) {
	var present C.uint8_t
	var stEpoch, stLastSeq C.int64_t
	if state != nil {
		present = 1
		stEpoch = C.int64_t(state.Epoch)
		stLastSeq = C.int64_t(state.LastSeq)
	}
	e := C.int64_t(epoch)
	s := C.int64_t(seq)
	n := C.int64_t(nowUnix)

	resultClass := uint8(C.lean_validate_producer_result(present, stEpoch, stLastSeq, e, s, n))
	errClass := uint8(C.lean_validate_producer_error(present, stEpoch, stLastSeq, e, s, n))

	res := store.AppendResult{
		ProducerResult: producerResultOf(resultClass),
		CurrentEpoch:   int64(C.lean_validate_producer_current_epoch(present, stEpoch, stLastSeq, e, s, n)),
		ExpectedSeq:    int64(C.lean_validate_producer_expected_seq(present, stEpoch, stLastSeq, e, s, n)),
		ReceivedSeq:    int64(C.lean_validate_producer_received_seq(present, stEpoch, stLastSeq, e, s, n)),
		LastSeq:        int64(C.lean_validate_producer_last_seq(present, stEpoch, stLastSeq, e, s, n)),
	}

	var newState *store.ProducerState
	if uint8(C.lean_validate_producer_persist(present, stEpoch, stLastSeq, e, s, n)) == 1 {
		newState = &store.ProducerState{
			Epoch:       int64(C.lean_validate_producer_new_epoch(present, stEpoch, stLastSeq, e, s, n)),
			LastSeq:     int64(C.lean_validate_producer_new_last_seq(present, stEpoch, stLastSeq, e, s, n)),
			LastUpdated: nowUnix,
		}
	}

	return res, newState, errorOf(errClass)
}

// producerResultOf maps the Lean result class (0/1/2) to store.ProducerResult.
func producerResultOf(class uint8) store.ProducerResult {
	switch class {
	case 1:
		return store.ProducerResultAccepted
	case 2:
		return store.ProducerResultDuplicate
	default:
		return store.ProducerResultNone
	}
}

// errorOf maps the Lean error class (0/1/2/3) back to the Go sentinel errors so
// errors.Is comparisons in the harness line up with the Go core.
func errorOf(class uint8) error {
	switch class {
	case 1:
		return store.ErrProducerSeqGap
	case 2:
		return store.ErrStaleEpoch
	case 3:
		return store.ErrInvalidEpochSeq
	default:
		return nil
	}
}
