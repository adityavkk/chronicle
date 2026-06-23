import Chronicle.Webhook
import Mathlib.Tactic

/-!
# Webhook reducer proofs (INV-CURSOR-01)

`offsetGreater` strict-order laws and `mergeAcks` monotone + idempotent, the typed
transcriptions of `webhook/state.go`. This is the machine-checked "acked offset
forward-only" guarantee: replays from the retry worker, the recovery sweep, and a
stale-but-unexpired token can never regress a cursor.

`UInt8`/`UInt64` carry only `LE`/`LT` (no `LinearOrder`) in this toolchain, so all
byte comparisons are routed through `.toNat` and closed by `omega`.

## Anti-vacuity

`mergeAcks_idempotent` is non-trivial: it depends on `offsetGreater` being
irreflexive (so the second pass cannot re-advance). `mergeAcks_monotone` uses the
forward-only guard essentially — change `mergeLink` to *always* set the offset
(dropping the guard) and a backward ack would regress the cursor, breaking it.
-/

namespace Chronicle.OffsetGreater

open Chronicle.OffsetGreater

/-- `byteListGt` is irreflexive: no byte list is strictly greater than itself. -/
theorem byteListGt_irrefl (xs : List UInt8) : byteListGt xs xs = false := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    unfold byteListGt
    have h1 : ¬ (x > x) := by rw [gt_iff_lt, UInt8.lt_iff_toNat_lt]; omega
    have h2 : ¬ (x < x) := by rw [UInt8.lt_iff_toNat_lt]; omega
    rw [if_neg h1, if_neg h2]
    exact ih

/-- `byteCompareGt` is irreflexive. -/
theorem byteCompareGt_irrefl (a : String) : byteCompareGt a a = false := by
  unfold byteCompareGt
  exact byteListGt_irrefl _

/-- **INV-CURSOR-01 — `offsetGreater` is irreflexive.** `offsetGreater a a = false`:
an offset is never strictly greater than itself, so an exact replay is a no-op. -/
theorem offsetGreater_irrefl (a : String) : offsetGreater a a = false := by
  unfold offsetGreater
  simp

/-- `byteListGt` is asymmetric: `a > b` ⇒ ¬`b > a`. -/
theorem byteListGt_asymm : ∀ (xs ys : List UInt8),
    byteListGt xs ys = true → byteListGt ys xs = false := by
  intro xs
  induction xs with
  | nil =>
    intro ys h
    cases ys with
    | nil => exact absurd h (by simp [byteListGt])
    | cons y ys => exact absurd h (by simp [byteListGt])
  | cons x xs ih =>
    intro ys h
    cases ys with
    | nil => rfl
    | cons y ys =>
      unfold byteListGt at h ⊢
      by_cases hxy : x > y
      · -- x > y ⇒ in `ys vs xs` we are at `y` vs `x` with y < x: first if false, second true
        have hyx_not : ¬ (y > x) := by
          rw [gt_iff_lt, UInt8.lt_iff_toNat_lt]
          rw [gt_iff_lt, UInt8.lt_iff_toNat_lt] at hxy; omega
        have hyx_lt : y < x := by
          rw [UInt8.lt_iff_toNat_lt]
          rw [gt_iff_lt, UInt8.lt_iff_toNat_lt] at hxy; omega
        rw [if_neg hyx_not, if_pos hyx_lt]
      · rw [if_neg hxy] at h
        by_cases hyx : x < y
        · rw [if_pos hyx] at h; exact absurd h (by simp)
        · rw [if_neg hyx] at h
          -- x = y
          have hxe : x.toNat = y.toNat := by
            rw [gt_iff_lt, UInt8.lt_iff_toNat_lt] at hxy
            rw [UInt8.lt_iff_toNat_lt] at hyx
            omega
          have h1 : ¬ (y > x) := by rw [gt_iff_lt, UInt8.lt_iff_toNat_lt]; omega
          have h2 : ¬ (y < x) := by rw [UInt8.lt_iff_toNat_lt]; omega
          rw [if_neg h1, if_neg h2]
          exact ih ys h

/-- **INV-CURSOR-01 — `offsetGreater` is asymmetric on real offsets.**
If `a > b` then ¬`b > a`. (Holds for all strings: the sentinel branches are
mutually exclusive with the bytewise branch.) -/
theorem offsetGreater_asymm (a b : String)
    (h : offsetGreater a b = true) : offsetGreater b a = false := by
  unfold offsetGreater at h ⊢
  by_cases hab : a == b
  · rw [if_pos hab] at h; exact absurd h (by simp)
  · rw [if_neg hab] at h
    have hab' : a ≠ b := by simpa using hab
    have hba : (b == a) = false := beq_false_of_ne (fun hc => hab' hc.symm)
    rw [if_neg (by rw [hba]; simp)]
    by_cases hb : b == "-1" || b == ""
    · -- b is a sentinel; from h, a is NOT a sentinel
      rw [if_pos hb] at h
      simp only [Bool.and_eq_true, bne_iff_ne, ne_eq] at h
      obtain ⟨ha1, ha2⟩ := h
      have hanot : ¬ (a == "-1" || a == "") := by simp [ha1, ha2]
      -- goal `offsetGreater b a`: second check is on `a` (not a sentinel ⇒ skip),
      -- third on `b` (a sentinel ⇒ false)
      rw [if_neg hanot, if_pos hb]
    · rw [if_neg hb] at h
      by_cases ha : a == "-1" || a == ""
      · rw [if_pos ha] at h; exact absurd h (by simp)
      · rw [if_neg ha] at h
        -- both real offsets: bytewise; goal `offsetGreater b a` skips both sentinel checks
        simp only [Bool.or_eq_true, beq_iff_eq] at ha hb
        push_neg at ha hb
        rw [if_neg (by simp [ha.1, ha.2]), if_neg (by simp [hb.1, hb.2])]
        unfold byteCompareGt at h ⊢
        exact byteListGt_asymm _ _ h

end Chronicle.OffsetGreater

namespace Chronicle.Webhook

open Chronicle.Webhook Chronicle.OffsetGreater

/-- After `mergeLink`, the path is preserved. -/
theorem mergeLink_path (acks : List Ack) (l : StreamLink) :
    (mergeLink acks l).path = l.path := by
  unfold mergeLink
  cases ackFor acks l.path with
  | none => rfl
  | some off => by_cases h : offsetGreater off l.ackedOffset <;> simp [h]

/-- **INV-CURSOR-01 — `mergeLink` advances forward-only.**
The post-cursor either equals the prior cursor (backward/equal ack: no-op) or is
strictly `offsetGreater` than it. -/
theorem mergeLink_monotone (acks : List Ack) (l : StreamLink) :
    (mergeLink acks l).ackedOffset = l.ackedOffset
    ∨ offsetGreater (mergeLink acks l).ackedOffset l.ackedOffset = true := by
  unfold mergeLink
  cases h : ackFor acks l.path with
  | none => left; rfl
  | some off =>
    by_cases hg : offsetGreater off l.ackedOffset
    · right; simp [hg]
    · left; simp [hg]

/-- **INV-CURSOR-01 — `mergeAcks` is monotone (forward-only) per link.**
Every link in the result either keeps its cursor or has it advanced to a strictly
greater offset; no cursor regresses. Stated over the `i`-th link of the (length-
preserving) result and the `i`-th input link. -/
theorem mergeAcks_monotone (links : List StreamLink) (acks : List Ack)
    (i : ℕ) (hi : i < links.length) :
    have hi' : i < (mergeAcks links acks).length := by simpa [mergeAcks] using hi
    (mergeAcks links acks)[i].ackedOffset = links[i].ackedOffset
    ∨ offsetGreater (mergeAcks links acks)[i].ackedOffset links[i].ackedOffset = true := by
  intro hi'
  have hget : (mergeAcks links acks)[i] = mergeLink acks links[i] := by
    unfold mergeAcks; rw [List.getElem_map]
  rw [hget]
  exact mergeLink_monotone acks links[i]

/-- After `mergeLink`, re-applying the *same* acks is a no-op: the cursor has
already moved to `off` (or stayed), and `offsetGreater off off = false`, so the
second pass cannot advance again. The key step behind idempotency. -/
theorem mergeLink_idempotent (acks : List Ack) (l : StreamLink) :
    mergeLink acks (mergeLink acks l) = mergeLink acks l := by
  conv_lhs => rw [mergeLink, mergeLink_path]
  cases h : ackFor acks l.path with
  | none => simp only [h]
  | some off =>
    -- the second pass compares `off` against the already-advanced cursor; that
    -- comparison is false (either off vs off, or off vs an unmoved smaller cursor)
    have key : offsetGreater off (mergeLink acks l).ackedOffset = false := by
      unfold mergeLink
      rw [h]
      by_cases hg : offsetGreater off l.ackedOffset
      · simp only [hg, if_true]; rw [offsetGreater_irrefl]
      · simp only [hg, if_false]
        simp only [Bool.not_eq_true] at hg; exact hg
    simp only [key, Bool.false_eq_true, if_false]

/-- **INV-CURSOR-01 — `mergeAcks` is idempotent.**
`mergeAcks (mergeAcks links acks) acks = mergeAcks links acks`: applying the same
acks twice equals applying them once, since each link's cursor is already at the
fixed point after the first pass. -/
theorem mergeAcks_idempotent (links : List StreamLink) (acks : List Ack) :
    mergeAcks (mergeAcks links acks) acks = mergeAcks links acks := by
  unfold mergeAcks
  rw [List.map_map]
  apply List.map_congr_left
  intro l _
  exact mergeLink_idempotent acks l

end Chronicle.Webhook
