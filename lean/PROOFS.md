# Chronicle pure-core proofs: theorem → invariant → Go source map

This is the deliverable map for issue #30 (P1.1): every Lean theorem, the
`INV-…` id from [INVARIANTS.md](../docs/specs/formal-verification/INVARIANTS.md)
it discharges, and the Go function in the `main` tree it proves correct. The
differential-oracle issue (#31) and the TLA+ issues cross-reference the exact
theorem names below.

All proofs build under the pinned toolchain (`leanprover/lean4:v4.31.0`, mathlib
`v4.31.0` = rev `fabf563a7c95a166b8d7b6efca11c8b4dc9d911f`) with `lake build`.
`Chronicle/Axioms.lean` is the `#print axioms` gate: no theorem depends on
`sorryAx`, and none uses `native_decide` (so none depends on `Lean.ofReduceBool`).
The trusted base is the Lean kernel plus mathlib's three classical axioms
(`propext`, `Classical.choice`, `Quot.sound`).

## Producer state machine — `Chronicle/Producer/Proofs.lean`

Subject: [`store/producer.go` `ValidateProducer`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go)

| Theorem | INV | What it proves |
| --- | --- | --- |
| `validateProducer_total_det` | INV-PROD-01 | Determinism + result/error pairing: exactly `(err=none, {accepted,duplicate})` or `(err≠none, none)`; never `none`+`none`, never `err`+non-`none`. |
| `accept_iff_newState` | INV-PROD-02 | persisted `newState` is `some` ⟺ result is `accepted` (persist-exactly-on-accept). |
| `accept_err_none` | INV-PROD-02 | on accept the error is always `none`. |
| `accept_advances` | INV-PROD-03 | every accept sets `epoch=epoch`, `lastUpdated=now`, `lastSeq ∈ {0, seq}`. |
| `epoch_monotone` | INV-PROD-04 | `epoch < state.epoch` ⇒ `staleEpoch`, `currentEpoch=state.epoch`, no mutation. |
| `accept_epoch_nondecreasing` | INV-PROD-04 | persisted epoch is monotone non-decreasing across an accept. |
| `duplicate_no_mutation` | INV-PROD-05 | same epoch + `seq ≤ lastSeq` ⇒ `duplicate`, `lastSeq` echoed, no mutation. |
| `same_epoch_step` | INV-PROD-05 | an accepted same-epoch step has `lastSeq=seq` and `seq=old+1`. |
| `accepted_at_most_once` | INV-PROD-05 | replaying an already-accepted `(epoch,seq)` is a `duplicate`, no mutation. |
| `replay_lastSeq_monotone` | INV-PROD-05 | **op-list induction**: `lastSeq` never decreases across any replay ⇒ accepted at most once. |
| `gap_exact` | INV-PROD-06 | same epoch + `seq > lastSeq+1` ⇒ `seqGap`, `expected=lastSeq+1`, `received=seq`, no mutation. |
| `gap_first_contact` | INV-PROD-06 | first contact + `seq≠0` ⇒ `seqGap`, `expected=0`, `received=seq`. |
| `new_epoch_starts_at_zero` | INV-PROD-07 | `epoch > state.epoch ∧ seq≠0` ⇒ `invalidEpochSeq`, no mutation. |

## Offset order — `Chronicle/Offset/Proofs.lean`

Subject: [`store/offset.go` `Compare`/`Add`/`String`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)

| Theorem | INV | What it proves |
| --- | --- | --- |
| `instLinearOrder` | INV-OFF-01 | `Offset` carries a mathlib `LinearOrder` (via `Prod.Lex` over `(readSeq, byteOffset)`), giving refl/antisymm/trans/total/decidable. |
| `compare_eq_neg_one_iff` | INV-OFF-01 | `compare a b = -1 ↔ a < b` (ties the `{-1,0,1}` Go contract to the order). |
| `compare_eq_zero_iff` | INV-OFF-01 | `compare a b = 0 ↔ a = b`. |
| `compare_self` | INV-OFF-01 | reflexivity: `compare a a = 0`. |
| `compare_antisymm` | INV-OFF-01 | antisymmetry: `compare a b = -(compare b a)`. |
| `compare_mem` | INV-OFF-01 | range: `compare a b ∈ {-1,0,1}`. |
| `lex_eq_numeric_below_1e16` | INV-OFF-02 | **`%016d` byte-lex = numeric, with the explicit `< 10^16` hypothesis** (the LB-1 safe domain; bounded theorem only, no wire change, no counterexample search). |
| `padTo16_inj` | INV-OFF-02 | the 16-digit `%016d` rendering is injective on `[0, 10^16)`. |
| `add_strict_mono` | INV-OFF-06 | `bytes>0` + no `uint64` wrap ⇒ `compare o (add o bytes) = -1` (the no-wrap bound is the explicit overflow hypothesis). |
| `add_readSeq` | INV-OFF-06 | `add` leaves `readSeq` invariant. |
| `sentinel_unreachable` | INV-OFF-05 | `add` never touches `readSeq`, so an offset with non-max `readSeq` can't become `NowOffset`. |

Note: INV-OFF-03/04 (`ParseOffset` round-trip and malformed-input rejection) are
addendum items; the string-grammar parser transcription is left to a follow-up so
this issue's offset proofs stay focused on the order/arithmetic core (see Follow-ups).

## Cursor — `Chronicle/Cursor/Proofs.lean`

Subject: [`protocol/cursor.go` `GenerateResponseCursor`/`GenerateCursor`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go)

| Theorem | INV | What it proves |
| --- | --- | --- |
| `response_cursor_strict_progression` | INV-CUR-01 / INV-CURSOR-01 | client cursor `≥ current` ⇒ response **strictly greater** (numeric, not string-length-gated): the anti-CDN-cache-loop guarantee. |
| `response_cursor_none` | INV-CUR-01 | empty/invalid client cursor ⇒ current interval. |
| `response_cursor_behind` | INV-CUR-01 | client behind current ⇒ resets to current interval. |
| `response_cursor_ge_current` | INV-CUR-01 | response is always `≥ current` (never regresses below now). |
| `generate_cursor_time_monotone` | INV-CUR-02 | `now1 ≤ now2 ⇒ generateCursor now1 ≤ generateCursor now2`, via `Int` floor-division — holds across the **pre-epoch negative-interval edge**. |

Note: the source computes `jitterIntervals = (1 + (3600-1)/2)/20 = 90`, not the
"180" in the issue prose (which predates the `(max-min)/2` jitter). The Lean
transcription follows the source (90); the strict-progression proof only needs
`jitterIntervals > 0`, so the exact value is not load-bearing.

## Webhook reducers — `Chronicle/Webhook/Proofs.lean`

Subject: [`webhook/state.go` `offsetGreater`/`MergeAcks`](https://github.com/adityavkk/chronicle/blob/main/webhook/state.go)

| Theorem | INV | What it proves |
| --- | --- | --- |
| `offsetGreater_irrefl` | INV-CURSOR-01 | `offsetGreater a a = false` (an exact replay is a no-op). |
| `offsetGreater_asymm` | INV-CURSOR-01 | `offsetGreater a b ⇒ ¬ offsetGreater b a` (with the `"-1"`/`""` sentinels handled). |
| `byteListGt_irrefl`, `byteListGt_asymm` | INV-CURSOR-01 | the bytewise lexicographic ordering laws underneath. |
| `mergeLink_monotone` | INV-CURSOR-01 | one link advances forward-only: cursor stays or strictly increases. |
| `mergeAcks_monotone` | INV-CURSOR-01 | per-link forward-only over the whole link list (no cursor regresses). |
| `mergeLink_idempotent` | INV-CURSOR-01 | re-applying acks to one link is a no-op (already at the fixed point). |
| `mergeAcks_idempotent` | INV-CURSOR-01 | `mergeAcks (mergeAcks links acks) acks = mergeAcks links acks`. |

## Parametric single-holder fence — `Chronicle/Fence/SingleHolder.lean`

Subject: the algebraic core cited by [`webhook/state.go` `FenceDecision`](https://github.com/adityavkk/chronicle/blob/main/webhook/state.go), the Lua `fenced`, and [`jepsen/checker/model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go)

| Theorem | INV | What it proves |
| --- | --- | --- |
| `single_holder_instant` | INV-FENCE-01 | any two tokens accept-guarded against the same `current` are equal ⇒ at most one holder at an instant. |
| `deposed_stays_fenced` | INV-FENCE-01 | `current > t` ⇒ `t` is not accept-guarded (a deposed holder is fenced). |
| `deposed_stays_fenced_run` | INV-FENCE-01 | a deposed token stays fenced across any further run of grants (the token only rises). |
| `runGrants_monotone` | INV-FENCE-01 | the register's token is monotone non-decreasing across a grant sequence. |
| `instGenWakeFence` | INV-FENCE-01 | the theorem instantiated for the `(generation, wake_id)` subscription fence. |
| `instOwnerEpochFence` | INV-FENCE-01 | the theorem instantiated for the `(owner, epoch)` slot fence. |

## Anti-vacuity

Per the Lean-Squad adoption guard, every theorem is falsifiable on a deliberately
broken model (documented in each file's module doc-comment): e.g. dropping the
stale-epoch branch breaks `epoch_monotone`; a zero jitter step breaks
`response_cursor_strict_progression`; dropping the forward-only guard breaks
`mergeAcks_monotone`; non-strict token motion breaks `deposed_stays_fenced`; and
**removing the `< 10^16` hypothesis makes `lex_eq_numeric_below_1e16` false** (the
LB-1 hazard).

## Follow-ups (out of this issue's scope)

- INV-OFF-03/04: a faithful `ParseOffset` string-grammar transcription + round-trip
  and malformed-input-rejection proofs.
- The Lean→C differential oracle (#31).
- The unguarded `≥ 10^16` `Compare`-vs-`strcmp` counterexample (P0.3 sibling).
