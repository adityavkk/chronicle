import Chronicle.Producer.Proofs
import Chronicle.Offset.Proofs
import Chronicle.Cursor.Proofs
import Chronicle.Webhook.Proofs
import Chronicle.Fence.SingleHolder

/-!
# Axiom audit (the anti-`sorry` / TCB gate)

This module is the machine-checked "no theorem depends on `sorry`" gate the issue
acceptance criteria require. `#print axioms <thm>` prints exactly the axioms each
proof rests on. A clean run prints only Lean's standard trusted base —
`propext`, `Classical.choice`, `Quot.sound` (mathlib's baseline) — and crucially
**never `sorryAx`**. If any proof regressed to a `sorry`, `sorryAx` would appear
here and a CI grep for `sorryAx` fails the build.

We deliberately prefer `decide`/`omega`/`split_ifs` over `native_decide`, so
**no theorem here depends on `Lean.ofReduceBool`** (the axiom `native_decide`
admits, which trusts the compiler). The trusted base stays the Lean kernel plus
mathlib's three classical axioms — nothing more.

Run `lake env lean Chronicle/Axioms.lean` (or build it) and inspect the output;
CI greps it for `sorryAx` and `ofReduceBool` and fails on either.
-/

open Chronicle

-- Producer SM (INV-PROD-01..07)
#print axioms Producer.validateProducer_total_det
#print axioms Producer.accept_iff_newState
#print axioms Producer.accept_err_none
#print axioms Producer.accept_advances
#print axioms Producer.epoch_monotone
#print axioms Producer.accept_epoch_nondecreasing
#print axioms Producer.duplicate_no_mutation
#print axioms Producer.gap_exact
#print axioms Producer.gap_first_contact
#print axioms Producer.new_epoch_starts_at_zero
#print axioms Producer.same_epoch_step
#print axioms Producer.accepted_at_most_once
#print axioms Producer.replay_lastSeq_monotone

-- Offset order (INV-OFF-01,02,03,05,06)
#print axioms Offset.instLinearOrder
#print axioms Offset.compare_eq_neg_one_iff
#print axioms Offset.compare_eq_zero_iff
#print axioms Offset.compare_mem
#print axioms Offset.compare_self
#print axioms Offset.compare_antisymm
#print axioms Offset.add_strict_mono
#print axioms Offset.add_readSeq
#print axioms Offset.sentinel_unreachable
#print axioms Offset.lex_eq_numeric_below_1e16
#print axioms Offset.padTo16_inj

-- Cursor (INV-CUR-01, INV-CUR-02)
#print axioms Cursor.response_cursor_strict_progression
#print axioms Cursor.response_cursor_none
#print axioms Cursor.response_cursor_behind
#print axioms Cursor.response_cursor_ge_current
#print axioms Cursor.generate_cursor_time_monotone

-- Webhook reducers (INV-CURSOR-01)
#print axioms OffsetGreater.offsetGreater_irrefl
#print axioms OffsetGreater.offsetGreater_asymm
#print axioms Webhook.mergeAcks_monotone
#print axioms Webhook.mergeLink_monotone
#print axioms Webhook.mergeAcks_idempotent
#print axioms Webhook.mergeLink_idempotent

-- Parametric single-holder fence (INV-FENCE-01)
#print axioms Fence.single_holder_instant
#print axioms Fence.deposed_stays_fenced
#print axioms Fence.deposed_stays_fenced_run
#print axioms Fence.runGrants_monotone
#print axioms Fence.instGenWakeFence
#print axioms Fence.instOwnerEpochFence
