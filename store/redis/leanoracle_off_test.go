//go:build !leanoracle

package redis

import (
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// This file holds the NO-OP third-oracle hooks used when the build does NOT
// carry the `leanoracle` tag. Routine Go CI builds this variant: the vendored
// Lean→C archive (store/leanoracle/libchronicle_oracle.a) is not linked and no
// Lean/cgo toolchain is required, so the differential harness runs the original
// Go-core-vs-live-Lua pairing unchanged.
//
// The real implementations, which call the proven Lean model through cgo and
// assert it agrees with the Go core, live in leanoracle_on_test.go behind
// `//go:build leanoracle`. Enable the triple oracle with:
//
//	go test -tags leanoracle ./store/redis/...
//
// (P1.2, issue #31.)

// checkLeanProducer is a no-op without the leanoracle tag.
func checkLeanProducer(_ *testing.T, _ *store.ProducerState, _, _, _ int64) {}

// checkLeanOffsetCompare is a no-op without the leanoracle tag.
func checkLeanOffsetCompare(_ *testing.T, _, _ store.Offset) {}
