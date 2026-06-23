import Chronicle.Cursor
import Mathlib.Tactic

/-!
# Cursor progression proofs (INV-CUR-01, INV-CUR-02)

Function-level correctness of `Chronicle.Cursor.generateResponseCursor` /
`generateCursor`, the typed transcription of `protocol/cursor.go`. These replace
the weak length-gated *string* assertion in the Go tests with **numeric**
strictly-greater / monotone theorems, discharged by `omega` on the floor-division.

## Anti-vacuity

`response_cursor_strict_progression` is falsifiable: set `jitterIntervals := 0`
(a deliberately-broken model that fails to advance) and the strict `>` breaks —
that is exactly the anti-CDN-cache-loop property of PROTOCOL §10.1. The
`jitterIntervals > 0` fact is used essentially.
-/

namespace Chronicle.Cursor

open Chronicle.Cursor

/-- The jitter step is strictly positive — the fact that makes progression strict
(a zero step would re-emit the echoed cursor and reopen the CDN cache loop). -/
theorem jitterIntervals_pos : 0 < jitterIntervals := by decide

/-- **INV-CUR-01 / INV-CURSOR-01 — strict progression past the echoed cursor.**
When the client cursor parses to an interval at or ahead of the current interval
(`ci ≥ generateCursor now`), the response is **strictly greater** than the echoed
cursor (advanced by `jitterIntervals`). This is the numeric anti-cache-loop
guarantee. -/
theorem response_cursor_strict_progression
    (ci nowMs epochMs : Int) (h : ci ≥ generateCursor nowMs epochMs) :
    generateResponseCursor (some ci) nowMs epochMs > ci := by
  unfold generateResponseCursor
  simp only []
  have hnlt : ¬ (ci < generateCursor nowMs epochMs) := by omega
  rw [if_neg hnlt]
  have := jitterIntervals_pos
  omega

/-- **INV-CUR-01 — fallback to current interval for empty/invalid/behind cursors.**
A `none` (empty or unparseable) client cursor returns exactly the current
interval. -/
theorem response_cursor_none
    (nowMs epochMs : Int) :
    generateResponseCursor none nowMs epochMs = generateCursor nowMs epochMs := rfl

/-- **INV-CUR-01 — behind-current cursor resets to current interval.**
A client cursor strictly behind the current interval returns the current
interval (not an advance), so a stale echo cannot drag the response backward. -/
theorem response_cursor_behind
    (ci nowMs epochMs : Int) (h : ci < generateCursor nowMs epochMs) :
    generateResponseCursor (some ci) nowMs epochMs = generateCursor nowMs epochMs := by
  unfold generateResponseCursor
  simp only []
  rw [if_pos h]

/-- The response is always a valid integer ≥ the current interval (never regresses
below "now"): both branches are ≥ `generateCursor now`. -/
theorem response_cursor_ge_current
    (clientInterval : Option Int) (nowMs epochMs : Int) :
    generateResponseCursor clientInterval nowMs epochMs ≥ generateCursor nowMs epochMs := by
  unfold generateResponseCursor
  cases clientInterval with
  | none => simp
  | some ci =>
    simp only []
    by_cases h : ci < generateCursor nowMs epochMs
    · rw [if_pos h]
    · rw [if_neg h]
      have := jitterIntervals_pos
      omega

/-- **INV-CUR-02 — `generateCursor` is monotone non-decreasing in time.**
`now1 ≤ now2 ⇒ generateCursor now1 ≤ generateCursor now2`, via floor-division
monotonicity (`Int.ediv` is monotone in the numerator for a positive divisor).
This holds across the **pre-epoch negative-interval edge** too: `Int` floor
division is monotone for *all* numerators, negative ones included, which is the
edge the Go tests never cover. -/
theorem generate_cursor_time_monotone
    (now1 now2 epochMs : Int) (h : now1 ≤ now2) :
    generateCursor now1 epochMs ≤ generateCursor now2 epochMs := by
  unfold generateCursor
  apply Int.ediv_le_ediv (by decide : (0:Int) < intervalMs)
  omega

end Chronicle.Cursor
