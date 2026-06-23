# store/leanoracle — the proven-Lean third differential oracle (P1.2, #31)

This package is the THIRD differential oracle: Chronicle's proven Lean
producer/offset model, compiled to C via Lean's C backend and called from Go
through cgo. It pins the proven model against the Go core (`store.ValidateProducer`,
`store.Compare`) and, transitively through the differential harness, against the
live Lua mirror — so one statement is checked from three independent sides.

## Layout

| path | what |
| --- | --- |
| `oracle.go` | the cgo bridge: `LeanOracle` with `Compare` and `ValidateProducer`, behind `//go:build leanoracle`. |
| `oracle_test.go` | self-test (and benchmarks) that the bridge links, initializes, and round-trips known cases against the Go core. |
| `chronicle_oracle.h` | maintained C header declaring the entry points. |
| `libchronicle_oracle.a` | the **vendored** self-contained static archive (no Lean toolchain needed to consume). |
| `lean/` | the standalone `lake` compile target: `Chronicle/Extern.lean` (the `@[export]` shims) + byte-identical copies of the proven `Producer.lean` / `Offset.lean`. |
| `scripts/build-lean-oracle.sh` | regenerates `libchronicle_oracle.a` from `lean/`. |
| `PROVENANCE.txt` | exact toolchain pin, source commit, and hashes. |

## Consume it (routine Go CI — no Lean toolchain)

The package and the triple-oracle hook are behind the `leanoracle` build tag.
Without the tag, `go build` / `go test` are cgo-free and need no archive:

```sh
# bridge self-test
CC=cc MACOSX_DEPLOYMENT_TARGET=15.0 go test -tags leanoracle ./store/leanoracle/

# triple-oracle differential (needs live Redis: make redis-up)
CC=cc MACOSX_DEPLOYMENT_TARGET=15.0 go test -tags leanoracle ./store/redis/
```

`CGO_ENABLED=1` (the default with a C compiler present) is required for the
`leanoracle` build. The link needs libc++ (the bridge sets `-lc++` in cgo
LDFLAGS); no Lean toolchain is needed.

## Regenerate the vendored archive (needs the pinned Lean toolchain)

```sh
PATH="$HOME/.elan/bin:$PATH" store/leanoracle/scripts/build-lean-oracle.sh
PATH="$HOME/.elan/bin:$PATH" store/leanoracle/scripts/build-lean-oracle.sh --check   # drift guard
```

See `PROVENANCE.txt` for the exact toolchain and the deterministic-build notes,
and `docs/SPIKE-lean-cgo.md` for the perf spike and the go/no-go.

## How the build works (recipe)

1. `lake build` runs Lean's C backend over `Chronicle.{Producer,Offset,Extern}`,
   emitting one `.c` per module. The cores are pure core-Lean (no mathlib), so the
   C links against only the Lean runtime.
2. Each `.c` is compiled with the **system clang** at `-fvisibility=default`
   (NOT `leanc`, which bakes hidden visibility and would break cgo linking) and a
   pinned macOS deployment target.
3. `ld -r` partial-links the three objects with the *referenced members* of
   `libleanrt.a` / `libInit.a` / `libgmp.a` / `libuv.a` into one self-contained
   relocatable object; `-exported_symbol` keeps the public entry points and the
   runtime bring-up symbols global. Only libc/libc++/libSystem are left undefined,
   which the final Go cgo link resolves.
4. `ZERO_AR_DATE=1 ar` packs a deterministic (zero-timestamp) archive, so the CI
   drift guard can assert byte-identity.
