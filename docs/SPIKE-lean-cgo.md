# Spike: proven Lean model -> C -> cgo as a third differential oracle

**Issue:** #31 (P1.2) - **Decision: GO.**

## The open question this spike retires

The differential-oracle strategy needs the *proven* Lean producer/offset model
to run as a third oracle next to the Go core and the live Lua mirror. The prior
art the strategy leans on compiled Lean -> C -> **Rust**, not Go. Nobody in the
cited work had run Lean -> C -> **cgo** at differential/fuzz call volumes, so the
recommendation rested on an unconfirmed path (FINDINGS.md, unverified
quantitative claims). This spike builds the real path end to end and measures it.

## What was built

1. `store/leanoracle/lean/Chronicle/Extern.lean` - `@[export]` C-entry shims over
   the proven cores. `Producer.lean` and `Offset.lean` next to it are
   byte-identical copies of the proven transcriptions (see PROVENANCE.txt), so the
   oracle runs the same definitions the proofs are about.
2. A standalone `lake` project that emits C for those three modules. It needs no
   mathlib (the cores are pure core-Lean), so the emitted C links against only the
   Lean runtime, not the heavy proof/mathlib object graph.
3. `store/leanoracle/scripts/build-lean-oracle.sh` - compiles the emitted C with
   the system clang at default visibility, partial-links it with the referenced
   Lean-runtime members into one self-contained relocatable object, and packs a
   deterministic static archive `libchronicle_oracle.a` (the vendored artifact).
4. `store/leanoracle/oracle.go` - the cgo bridge (`LeanOracle` with `Compare` and
   `ValidateProducer`), behind the `leanoracle` build tag, with one-time Lean
   runtime init.
5. The triple-oracle hook in `store/redis/differential_test.go`.

## ABI

Scalar-only, so no `lean_object` crosses the boundary and there is no
ref-counting on a call:

- `int8_t lean_offset_compare(uint64_t, uint64_t, uint64_t, uint64_t)` -> -1/0/1.
- `lean_validate_producer_*` - one accessor per reply field over the flattened
  request `(state_present, st_epoch, st_last_seq, epoch, seq, now)`; the bridge
  calls them and assembles `store.AppendResult` + `*store.ProducerState`. The full
  list is in `store/leanoracle/chronicle_oracle.h`.

Runtime bring-up is once per process: `lean_initialize_runtime_module()` ->
`initialize_chronicleoracle_Chronicle_Extern(1)` -> `lean_io_mark_end_initialization()`.

## Measured per-call overhead and allocation (the go/no-go numbers)

`go test -tags leanoracle -bench=. -benchtime=10000000x ./store/leanoracle/`
on Apple M4 Pro, Go 1.26, Lean 4.31.0, 10^7 iterations:

| entry point                       | ns/op |  B/op | allocs/op |
| --------------------------------- | ----: | ----: | --------: |
| `Compare` (1 cgo call)            | 68.06 |     0 |         0 |
| `ValidateProducer` (9 cgo calls)  |  1256 |    24 |         1 |

- **`Compare`: 68 ns/call, zero allocations.** At 10^7 calls that is ~0.68 s of
  added oracle time per CI run - negligible. This is the cheapest entry point and
  the one the offset differential hits most, so it is the relevant go/no-go
  number.
- **`ValidateProducer`: ~1.26 us/call, one 24-byte allocation** (the returned
  `*ProducerState` on accepts; the inputs cause no heap traffic). The cost is
  dominated by making nine cgo calls to assemble the reply tuple, not by the Lean
  work. At 10^7 producer checks that is ~12.5 s; the producer differential makes
  far fewer checks than the offset compare, so this is comfortably within budget.
  If a future generator drives producer checks into the tens of millions, the
  obvious win is one packed-struct cgo call instead of nine (recorded as a
  follow-up); it is not needed now.

Both entry points are 0-or-1 alloc and there is no per-call ref-counting, so
there is no GC pressure or memory growth at volume. The cgo call itself (~50-70
ns) is the floor, exactly as expected for cgo, not a Lean-runtime cost.

## Decision: GO

The Lean -> C -> cgo path works end to end, links cleanly, and is fast enough:
the proven model agrees with the Go core on every producer outcome and every
offset comparison (the bridge self-test and the triple-oracle differential both
pass against live Redis). The vendored archive lets routine Go CI link and run
the third oracle with no Lean toolchain present. The full wiring in this issue is
therefore committed rather than re-scoped to a fallback.

Fallbacks that are NOT needed (recorded for completeness): a subprocess oracle
over a length-prefixed stdin/stdout protocol, or codegen of the Lean model to Go.
Neither is warranted given the measured numbers.

## Honest caveats

- The vendored archive is **arch-specific** (Mach-O arm64 here). A CI runner on a
  different architecture must rebuild it for that arch via the build script; the
  drift guard then compares against the arch it runs on. Cross-arch vendoring (a
  fat archive or per-arch artifacts) is a follow-up if Linux CI is added.
- The archive is ~14 MB because the Lean runtime's allocator (mimalloc), libuv,
  and the IO/module-init path are pulled in transitively even for pure scalar
  functions; that is the realistic floor for a self-contained Lean-runtime-linked
  artifact and is acceptable for a one-time vendored static lib.
- The compile uses the **system clang**, not `leanc`: `leanc` bakes
  `-fvisibility=hidden`, which demotes the `@[export]` symbols to private and
  breaks cgo linking. Default visibility is required.
