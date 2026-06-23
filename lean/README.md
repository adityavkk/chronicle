# Chronicle formal-verification: Lean 4 pure-core package

This is the **P0.5 skeleton** of Chronicle's formal-verification track (GitHub
issue #29, epic #25). It is a faithful **typed transcription** of Chronicle's
three correctness-critical pure cores into Lean 4. It contains **no theorems or
proofs** — its only job is to typecheck and compile under a pinned toolchain so
the proofs (issue #30 / P1.1) have a typed subject. The Lean→C compile and cgo
differential oracle (P1.2) come later still.

## What is transcribed

| Lean module | Go source | What it mirrors |
| --- | --- | --- |
| `Chronicle/Producer.lean` | `store/producer.go` (`ValidateProducer`) | the six-outcome idempotent-producer state machine, returning the full response-shaping tuple |
| `Chronicle/Offset.lean` | `store/offset.go` (`Compare`, `Offset.Add`) | the lexicographic offset order and the wrapping byte-offset add |
| `Chronicle/OffsetGreater.lean` | `webhook/state.go` (`offsetGreater`) | the cursor-monotonicity predicate with the `"-1"`/`""` sentinels |
| `Chronicle.lean` | — | umbrella import so `lake build` compiles all three |

Each Lean function carries a doc-comment naming its Go source file so the
model-vs-code correspondence is auditable.

## Pinned toolchain

`lean-toolchain` pins the exact Lean version:

```
leanprover/lean4:v4.31.0
```

(Lean 4.31.0, Lake 5.0.0). `elan` reads this file and selects the matching
toolchain automatically; no manual switching is needed.

**No mathlib.** This skeleton uses core Lean only (`Int`, `UInt64`, `String`,
structures, inductives). Mathlib is heavy and version-sensitive and is only
needed for the proofs (`LinearOrder`, `omega`, induction) in issue #30 — so it
is deliberately *not* a dependency here, and there is no `lake-manifest.json`
pinning a mathlib revision yet. The proofs issue adds `require mathlib` and the
committed `lake-manifest.json` at that point.

## Build

Install `elan` (the Lean toolchain manager) if you do not have it. On a machine
whose `~/.profile` is read-only (e.g. Nix Home Manager), pass `--no-modify-path`:

```sh
curl https://elan.lean-lang.org/elan-init.sh -sSf | sh -s -- -y --no-modify-path
export PATH="$HOME/.elan/bin:$PATH"   # add to your shell rc
```

Then, from this `lean/` directory:

```sh
lake build
```

`elan` will download Lean 4.31.0 on first build (pinned by `lean-toolchain`).
The build must succeed with no errors and no `sorry`/`partial`/totality
warnings. To make warnings (unused variables, `sorry`, partiality) fail the
build the way CI should:

```sh
lake build -- -DwarningAsError=true
```

## int64 / uint64 correspondence decisions

These are the deliberate Go→Lean integer-type choices. They are load-bearing:
the transcription must restate the Go integer semantics *exactly* or every later
proof and oracle run proves the wrong thing.

- **Producer `epoch`, `seq`, `nowUnix`, and `ProducerState.{Epoch,LastSeq,LastUpdated}`** —
  Go `int64`, modeled as Lean **`Int`** (unbounded integers), *not* `Int64`. The
  producer state machine only compares these and computes `LastSeq + 1`; it never
  relies on `int64` wrap-around over the realised domain, so `Int` is the faithful
  and proof-friendly model. `nowUnix` is an injected `Int` parameter (the Go clock
  is already injected) and only stamps `LastUpdated` on an accept.

- **`Offset.ReadSeq`, `Offset.ByteOffset`** — Go `uint64`, modeled as Lean
  **`UInt64`**, *not* `Nat`. Go `uint64` addition **wraps on overflow** and
  `Offset.Add` is plain `+`; `UInt64` arithmetic in Lean wraps mod 2^64 exactly as
  Go does. Modeling these as `Nat` would grow without bound instead of wrapping and
  would diverge at the top of the domain. `Offset.add` therefore wraps; `readSeq`
  is left untouched.

- **`offsetGreater` operands** — Go `string`, modeled as Lean **`String`**. Go's
  `>` on `string` is a **bytewise** (unsigned-byte) lexicographic comparison, so
  the model compares the UTF-8 bytes of each string (`byteCompareGt`) rather than
  relying on Lean's codepoint-level `String` ordering. Protocol offsets are ASCII
  (zero-padded digits and `_`, plus the `"-1"`/`""` sentinels), so the two orders
  coincide on real inputs, but the bytewise model is the precise mirror of Go.

## What comes next (not in this issue)

- **Proofs (P1.1, issue #30):** add `require mathlib` + `lake-manifest.json`, then
  the producer SM totality/determinism, the `Offset.compare` `LinearOrder` (with the
  `< 10^16` safe-domain theorem), and the `offsetGreater` strict-total-order /
  `MergeAcks` monotone-idempotent lemmas — importing these modules unchanged.
- **Third differential oracle (P1.2):** `@[extern]` annotations, Lean→C compile, and
  the cgo bridge so the proven source doubles as a differential oracle against the Go
  and Lua implementations.
