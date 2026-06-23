// Package leanoracle is the third differential oracle (P1.2, issue #31): the
// proven Lean producer/offset model, compiled to C via Lean's C backend and
// linked into Go through cgo, pinned against the Go core and the live Lua mirror.
//
// The functional bridge (the LeanOracle type, the cgo linkage, and the
// self-test) lives behind the `leanoracle` build tag in oracle.go and
// oracle_test.go, so routine `go build` / `go test` stay cgo-free and need no
// vendored archive. Build with `-tags leanoracle` to link the vendored
// store/leanoracle/libchronicle_oracle.a and run the third oracle. This file
// carries only the package clause (no build tag) so the directory is always a
// valid Go package; it declares no symbols.
//
// See README.md for the build recipe, PROVENANCE.txt for the toolchain pin, and
// docs/SPIKE-lean-cgo.md for the perf spike and the go/no-go.
package leanoracle
