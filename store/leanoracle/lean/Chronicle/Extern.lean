import Chronicle.Producer
import Chronicle.Offset

/-!
# C-export shims for the third differential oracle (P1.2, issue #31)

This module is the **compile target** of the Lean→C→cgo third differential
oracle. It exposes the *proven* pure cores — `Chronicle.Offset.compare` and
`Chronicle.Producer.validateProducer` — through a flat, scalar-only C ABI so
the Go differential harness can call the proven Lean model directly via cgo
and pin it against the Go core and the live Lua mirror.

`Producer.lean` and `Offset.lean` in this directory are **byte-identical copies**
of the proven transcriptions in the `formal-verification` worktree
(`lean/Chronicle/{Producer,Offset}.lean`, recorded commit in
`store/leanoracle/PROVENANCE.txt`); the `CI` drift-guard re-copies and rebuilds
to assert they have not diverged. They are pure core-Lean (no mathlib), so this
project builds without mathlib and the emitted C links against only the Lean
**runtime** (`libleanrt.a`, `libInit.a`, gmp, libuv), not the heavy mathlib
object graph. The proofs in the sibling worktree's `Chronicle/*/Proofs.lean` are
*about* these same definitions, so the oracle and the proofs are pinned to one
statement.

## ABI design (why scalar-only)

`@[export]` compiles each wrapper with Lean's ordinary calling convention.
Fixed-width scalars (`UInt8`, `UInt64`, `Int64`) are passed/returned **unboxed**
through the C ABI, but Lean's unbounded `Int` is a boxed `lean_object *` (GMP).
To keep the cgo boundary a plain C ABI with no `lean_object` marshalling and no
ref-counting on the hot path, every wrapper takes and returns only fixed-width
scalars:

* `Offset.{readSeq,byteOffset}` are Go `uint64` ⇒ modeled `UInt64` ⇒ C `uint64_t`.
* `Producer.{epoch,seq,nowUnix}` and `ProducerState.{epoch,lastSeq}` are Go
  `int64` ⇒ modeled `Int` ⇒ marshalled to/from C `int64_t` at the boundary via
  `Int.toInt64` / `Int64.toInt`.
* result/error classes and the persist flag are small enums ⇒ C `uint8_t`.

The producer reply tuple is several fields. Rather than pack a struct across the
ABI (which would force a boxed Lean structure or out-params), each field is a
separate `@[export]` accessor over the same inputs. `validateProducer` is a
pure, branch-only function (no allocation, no recursion), so recomputing it per
field is a handful of integer comparisons — far cheaper than boxing/unboxing a
struct, and it keeps the ABI a set of `(scalars) -> scalar` calls that cgo
marshals with zero heap traffic. The Go bridge calls the accessors once per case
and assembles the `store.AppendResult` / `*store.ProducerState`.
-/

namespace Chronicle.Extern

open Chronicle.Producer
open Chronicle.Offset

/-! ## Offset.compare -/

/-- C entry `int8_t lean_offset_compare(uint64_t, uint64_t, uint64_t, uint64_t)`.

Returns the lexicographic comparison of `(aReadSeq, aByteOffset)` vs
`(bReadSeq, bByteOffset)` as `-1 / 0 / 1`, exactly `store.Compare`. The Lean
`compare` returns an unbounded `Int` whose range is `{-1,0,1}`; it is narrowed
to `Int8` (C `int8_t`, widened by Go) at the boundary. -/
@[export lean_offset_compare]
def offsetCompare (aReadSeq aByteOffset bReadSeq bByteOffset : UInt64) : Int8 :=
  let a : Offset := { readSeq := aReadSeq, byteOffset := aByteOffset }
  let b : Offset := { readSeq := bReadSeq, byteOffset := bByteOffset }
  (Chronicle.Offset.compare a b).toInt8

/-! ## ValidateProducer

Each accessor takes the flattened request:

* `statePresent` — `0` = first contact (Go `state == nil`), `1` = existing state.
* `stEpoch`, `stLastSeq` — the existing `ProducerState` fields (ignored when
  `statePresent = 0`).
* `epoch`, `seq`, `now` — the request triple plus the injected clock.

and returns one field of the reply. The shared helper rebuilds the same
`Option ProducerState` the Go core passes (`nil` ⇒ `none`). -/

/-- Rebuild the `Option ProducerState` from the flattened C inputs. -/
@[inline] private def mkState (statePresent : UInt8) (stEpoch stLastSeq : Int64) :
    Option ProducerState :=
  if statePresent == 0 then
    none
  else
    some { epoch := stEpoch.toInt, lastSeq := stLastSeq.toInt, lastUpdated := 0 }

/-- Run the proven state machine on the flattened inputs. -/
@[inline] private def run (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) :
    AppendResult × Option ProducerState :=
  validateProducer (mkState statePresent stEpoch stLastSeq) epoch.toInt seq.toInt now.toInt

/-- C entry: `ProducerResult` class — `0` none, `1` accepted, `2` duplicate.
Mirrors `store.ProducerResultNone/Accepted/Duplicate`. -/
@[export lean_validate_producer_result]
def vpResult (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : UInt8 :=
  match (run statePresent stEpoch stLastSeq epoch seq now).fst.producerResult with
  | .none      => 0
  | .accepted  => 1
  | .duplicate => 2

/-- C entry: error class — `0` nil, `1` seqGap, `2` staleEpoch, `3` invalidEpochSeq.
Mirrors `nil / store.ErrProducerSeqGap / store.ErrStaleEpoch / store.ErrInvalidEpochSeq`. -/
@[export lean_validate_producer_error]
def vpError (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : UInt8 :=
  match (run statePresent stEpoch stLastSeq epoch seq now).fst.error with
  | .none            => 0
  | .seqGap          => 1
  | .staleEpoch      => 2
  | .invalidEpochSeq => 3

/-- C entry: `AppendResult.CurrentEpoch` (set on stale epoch). -/
@[export lean_validate_producer_current_epoch]
def vpCurrentEpoch (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  (run statePresent stEpoch stLastSeq epoch seq now).fst.currentEpoch.toInt64

/-- C entry: `AppendResult.ExpectedSeq` (set on a gap). -/
@[export lean_validate_producer_expected_seq]
def vpExpectedSeq (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  (run statePresent stEpoch stLastSeq epoch seq now).fst.expectedSeq.toInt64

/-- C entry: `AppendResult.ReceivedSeq` (set on a gap). -/
@[export lean_validate_producer_received_seq]
def vpReceivedSeq (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  (run statePresent stEpoch stLastSeq epoch seq now).fst.receivedSeq.toInt64

/-- C entry: `AppendResult.LastSeq` (set on success / duplicate). -/
@[export lean_validate_producer_last_seq]
def vpLastSeq (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  (run statePresent stEpoch stLastSeq epoch seq now).fst.lastSeq.toInt64

/-- C entry: persist flag — `1` ⟺ the Go core returns a non-nil `*ProducerState`
(i.e. the caller must persist), `0` otherwise. This is exactly the
`accept ⟺ newState ≠ none` decision proven by `accept_iff_newState`. -/
@[export lean_validate_producer_persist]
def vpPersist (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : UInt8 :=
  match (run statePresent stEpoch stLastSeq epoch seq now).snd with
  | none   => 0
  | some _ => 1

/-- C entry: persisted `ProducerState.Epoch` when `persist = 1` (else `0`). -/
@[export lean_validate_producer_new_epoch]
def vpNewEpoch (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  match (run statePresent stEpoch stLastSeq epoch seq now).snd with
  | none    => 0
  | some ns => ns.epoch.toInt64

/-- C entry: persisted `ProducerState.LastSeq` when `persist = 1` (else `0`). -/
@[export lean_validate_producer_new_last_seq]
def vpNewLastSeq (statePresent : UInt8) (stEpoch stLastSeq epoch seq now : Int64) : Int64 :=
  match (run statePresent stEpoch stLastSeq epoch seq now).snd with
  | none    => 0
  | some ns => ns.lastSeq.toInt64

end Chronicle.Extern
