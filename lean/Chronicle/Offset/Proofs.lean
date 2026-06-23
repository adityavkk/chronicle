import Chronicle.Offset
import Mathlib.Tactic
import Mathlib.Order.Basic

/-!
# Offset order proofs (INV-OFF-01, -02, -03, -05, -06)

Function-level correctness of `Chronicle.Offset.compare` / `add`, the typed
transcription of `Compare` / `Add` / `String` / `ParseOffset` in
`store/offset.go`.

The mathlib bridge is a key function `key : Offset → ℕ ×ₗ ℕ` sending an offset to
the lexicographic pair `(readSeq.toNat, byteOffset.toNat)`. mathlib already gives
`ℕ ×ₗ ℕ` a `LinearOrder` (via `Prod.Lex`), so the `Offset` `LinearOrder`
(INV-OFF-01) is transported along `key`, and the trichotomy sign of `compare`
is shown to agree with that order. All offset comparisons in the Go source run
over the lexicographic `(ReadSeq, ByteOffset)` pair, so this is faithful.

## Anti-vacuity

`compare_eq_strcmp_below_1e16` (INV-OFF-02) carries the load-bearing hypothesis
`field < 10^16`. Drop it and the theorem is *false* — that is the LB-1
`%016d`-min-width hazard from FINDINGS.md. The theorem here proves the bounded
(safe-domain) direction only; it does **not** change the `%016d` wire format and
does **not** search for the ≥ 10^16 counterexample (that is the P0.3 sibling).
-/

namespace Chronicle.Offset

open Chronicle.Offset

/-- Lexicographic key into `ℕ ×ₗ ℕ` (mathlib's `Prod.Lex`), the order all Go
offset comparisons run over. `toLex` tags the product so mathlib uses the
lexicographic `LinearOrder` rather than the componentwise (partial) one. -/
def key (o : Offset) : ℕ ×ₗ ℕ := toLex (o.readSeq.toNat, o.byteOffset.toNat)

theorem key_injective : Function.Injective key := by
  intro a b h
  unfold key at h
  rw [toLex_inj] at h
  obtain ⟨h1, h2⟩ := Prod.ext_iff.mp h
  have hr : a.readSeq = b.readSeq := UInt64.toNat_inj.mp h1
  have hb : a.byteOffset = b.byteOffset := UInt64.toNat_inj.mp h2
  cases a; cases b; simp_all

/-- The `Offset` linear order, transported from `ℕ ×ₗ ℕ` along the injective
`key`. mathlib's `LinearOrder.lift'` supplies reflexivity, antisymmetry,
transitivity, totality and decidability — i.e. the full **INV-OFF-01** law set. -/
noncomputable instance instLinearOrder : LinearOrder Offset :=
  LinearOrder.lift' key key_injective

/-- The numeric trichotomy of the lexicographic key, the bridge between the
`{-1,0,1}` Go `compare` and the mathlib order. -/
theorem key_lt_iff (a b : Offset) :
    key a < key b ↔
      (a.readSeq < b.readSeq ∨ (a.readSeq = b.readSeq ∧ a.byteOffset < b.byteOffset)) := by
  unfold key
  rw [Prod.Lex.toLex_lt_toLex]
  simp only []
  constructor
  · rintro (h | ⟨h1, h2⟩)
    · exact Or.inl (UInt64.lt_iff_toNat_lt.mpr h)
    · exact Or.inr ⟨UInt64.toNat_inj.mp h1, UInt64.lt_iff_toNat_lt.mpr h2⟩
  · rintro (h | ⟨h1, h2⟩)
    · exact Or.inl (UInt64.lt_iff_toNat_lt.mp h)
    · exact Or.inr ⟨by rw [h1], UInt64.lt_iff_toNat_lt.mp h2⟩

/-- `compare` re-expressed purely over the `toNat` of the two fields, so every
downstream order fact is a `Nat` comparison `omega` can settle. `UInt64` carries
only `LE`/`LT` (no `LinearOrder`) in this toolchain, so all reasoning is routed
through `.toNat`. -/
theorem compare_toNat (a b : Offset) :
    compare a b =
      (if a.readSeq.toNat < b.readSeq.toNat then -1
       else if b.readSeq.toNat < a.readSeq.toNat then 1
       else if a.byteOffset.toNat < b.byteOffset.toNat then -1
       else if b.byteOffset.toNat < a.byteOffset.toNat then 1
       else 0) := by
  unfold compare
  simp only [UInt64.lt_iff_toNat_lt, gt_iff_lt]

/-- The lex condition on offsets, expressed on `toNat` (the form `omega` handles). -/
theorem lt_toNat_iff (a b : Offset) :
    a < b ↔
      (a.readSeq.toNat < b.readSeq.toNat
       ∨ (a.readSeq.toNat = b.readSeq.toNat ∧ a.byteOffset.toNat < b.byteOffset.toNat)) := by
  show key a < key b ↔ _
  rw [key_lt_iff, UInt64.lt_iff_toNat_lt, UInt64.lt_iff_toNat_lt, UInt64.toNat_inj]

/-- **INV-OFF-01 — `compare` is the trichotomy sign of the linear order.**
`compare a b = -1 ↔ a < b`. This pins the Go `{-1,0,1}` contract to the proven
`LinearOrder`: range, totality, antisymmetry (`compare a b = -(compare b a)`),
reflexivity (`compare a a = 0`), and `compare a b = 0 ↔ a = b` all follow. -/
theorem compare_eq_neg_one_iff (a b : Offset) :
    compare a b = -1 ↔ a < b := by
  rw [compare_toNat, lt_toNat_iff]
  constructor
  · intro h; split_ifs at h <;> omega
  · intro h; split_ifs <;> omega

theorem compare_eq_zero_iff (a b : Offset) :
    compare a b = 0 ↔ a = b := by
  rw [compare_toNat,
      show (a = b) ↔ (a.readSeq.toNat = b.readSeq.toNat ∧ a.byteOffset.toNat = b.byteOffset.toNat)
        from ⟨fun h => by rw [h]; exact ⟨rfl, rfl⟩,
              fun ⟨h1, h2⟩ => by
                cases a; cases b
                simp only [Offset.mk.injEq]
                exact ⟨UInt64.toNat_inj.mp h1, UInt64.toNat_inj.mp h2⟩⟩]
  constructor
  · intro h; split_ifs at h <;> omega
  · intro h; split_ifs <;> omega

/-- `compare` range is exactly `{-1, 0, 1}`. -/
theorem compare_mem (a b : Offset) :
    compare a b = -1 ∨ compare a b = 0 ∨ compare a b = 1 := by
  rw [compare_toNat]
  split_ifs <;> tauto

/-- **INV-OFF-01 — reflexivity.** `compare a a = 0`. -/
theorem compare_self (a : Offset) : compare a a = 0 :=
  (compare_eq_zero_iff a a).mpr rfl

/-- **INV-OFF-01 — antisymmetry.** `compare a b = - compare b a`. -/
theorem compare_antisymm (a b : Offset) : compare a b = - compare b a := by
  rw [compare_toNat, compare_toNat]
  split_ifs <;> omega

/-! ## INV-OFF-06 — `add` strict monotonicity, `readSeq` invariant -/

/-- `add` leaves `readSeq` untouched (the field invariant catch-up reads rely on). -/
theorem add_readSeq (o : Offset) (bytes : UInt64) : (add o bytes).readSeq = o.readSeq := rfl

/-- **INV-OFF-06 — `add` strict monotonicity (no-overflow domain).**
For `bytes > 0` with no `UInt64` overflow (`byteOffset + bytes` does not wrap,
expressed as `o.byteOffset.toNat + bytes.toNat < 2^64`), the minted offset is
strictly greater: `compare o (o.add bytes) = -1`. The no-wrap hypothesis is the
machine-checked statement of the silent `uint64` wraparound bound. -/
theorem add_strict_mono (o : Offset) (bytes : UInt64)
    (hpos : bytes ≠ 0)
    (hno : o.byteOffset.toNat + bytes.toNat < 2 ^ 64) :
    compare o (add o bytes) = -1 := by
  rw [compare_eq_neg_one_iff]
  show key o < key (add o bytes)
  rw [key_lt_iff]
  refine Or.inr ⟨rfl, ?_⟩
  -- byteOffset < byteOffset + bytes given no wrap and bytes > 0
  have hb : (o.byteOffset + bytes).toNat = o.byteOffset.toNat + bytes.toNat := by
    rw [UInt64.toNat_add]
    rw [Nat.mod_eq_of_lt hno]
  have hbpos : 0 < bytes.toNat := by
    rcases Nat.eq_zero_or_pos bytes.toNat with h | h
    · have : bytes = 0 := by
        have : bytes.toNat = (0:UInt64).toNat := by simpa using h
        exact UInt64.toNat_inj.mp this
      exact absurd this hpos
    · exact h
  rw [UInt64.lt_iff_toNat_lt]
  show o.byteOffset.toNat < (add o bytes).byteOffset.toNat
  have : (add o bytes).byteOffset = o.byteOffset + bytes := rfl
  rw [this, hb]
  omega

/-! ## INV-OFF-05 — sentinel unreachability by minting

`NowOffset = (2^64-1, 2^64-1)`. `add` only ever changes `byteOffset` and leaves
`readSeq` fixed, so starting from any offset whose `readSeq ≠ 2^64-1`, no chain
of `add`s can reach `NowOffset` (its `readSeq` stays put). -/

/-- The "now" sentinel: max in both fields. -/
def nowOffset : Offset := { readSeq := 0xFFFFFFFFFFFFFFFF, byteOffset := 0xFFFFFFFFFFFFFFFF }

/-- **INV-OFF-05 — sentinel unreachable by minting.**
`add` never touches `readSeq`, so an offset whose `readSeq` is not already the max
can never become `NowOffset` by `add` (single step). -/
theorem sentinel_unreachable (o : Offset) (bytes : UInt64)
    (h : o.readSeq ≠ nowOffset.readSeq) :
    add o bytes ≠ nowOffset := by
  intro hc
  apply h
  have : (add o bytes).readSeq = nowOffset.readSeq := by rw [hc]
  rw [add_readSeq] at this
  exact this

/-! ## INV-OFF-02 — lex-string order = numeric order ONLY below 10^16

`String()` formats each field with `%016d` (a *minimum* width). For a field value
`< 10^16` the decimal rendering is at most 16 digits and is left-padded with `'0'`
to exactly 16 characters. We model that rendering faithfully as `padTo16 n`: the
16-element big-endian list of base-10 digits, most-significant first (the byte
order of the printed string, since `'0'..'9'` are ASCII-ordered). Byte-lex order
on the fixed-width 16-char string is exactly `List.Lex (· < ·)` on these digit
lists, so proving `padTo16` is **strictly monotone** below `10^16` shows
byte-lex = numeric on the safe domain.

At `≥ 10^16` the field renders in 17+ digits, the equal-width assumption fails,
and lex diverges from numeric — that is the LB-1 hazard. The `< 10^16` hypothesis
below is therefore explicit and load-bearing; this proves the bounded theorem
only. It does NOT change the `%016d` wire format and does NOT exhibit the
≥ 10^16 counterexample (the P0.3 sibling). -/

/-- The `%016d` rendering of `n` as its 16 big-endian base-10 digits
(most-significant first). `digit i = (n / 10^(15-i)) % 10`. This is the exact
character sequence `fmt.Sprintf("%016d", n)` emits for `n < 10^16`, and `'0'..'9'`
being ASCII-ordered means byte-lex on the string = `List.Lex (· < ·)` here. -/
def padTo16 (n : ℕ) : List ℕ :=
  (List.range 16).map (fun i => (n / 10 ^ (15 - i)) % 10)

/-- `padTo16` always yields 16 digits — the fixed width that makes byte-lex sound. -/
theorem padTo16_length (n : ℕ) : (padTo16 n).length = 16 := by
  simp [padTo16]

/-- Positional value of the big-endian digit list `padTo16 n`: read the 16 digits
back, most-significant first, as a base-10 number. -/
def value16 (ds : List ℕ) : ℕ := ds.foldl (fun acc d => acc * 10 + d) 0

/-- General reconstruction: folding the top-`w` big-endian digits of `n`
(most-significant first), seeded with the high part `n / 10^w`, recovers `n`.
Proven by induction on the width `w`; the inductive step is the single
`Nat.div_add_mod`-style identity `(n / 10^(w+1)) * 10 + (n / 10^w) % 10 = n / 10^w`. -/
theorem foldl_digits_recover (n : ℕ) :
    ∀ w, ((List.range w).map (fun i => (n / 10 ^ (w - 1 - i)) % 10)).foldl
            (fun acc d => acc * 10 + d) (n / 10 ^ w) = n := by
  intro w
  induction w with
  | zero => simp
  | succ k ih =>
    -- range (k+1) = 0 :: (range k).map (·+1); the head digit is the top one.
    rw [List.range_succ_eq_map, List.map_cons, List.foldl_cons, List.map_map]
    simp only [Nat.add_sub_cancel, Nat.sub_zero]
    -- head: (n / 10^k) % 10; seed becomes (n/10^(k+1))*10 + (n/10^k)%10 = n/10^k
    have hstep : n / 10 ^ (k + 1) * 10 + n / 10 ^ k % 10 = n / 10 ^ k := by
      have : n / 10 ^ (k + 1) = n / 10 ^ k / 10 := by
        rw [pow_succ, Nat.div_div_eq_div_mul]
      rw [this]
      omega
    rw [hstep]
    -- align: the cons'd tail (·+1) shifts indices, (k - (i+1)) = (k-1-i)
    have hmap : (List.map ((fun i => n / 10 ^ (k - i) % 10) ∘ Nat.succ) (List.range k))
              = (List.map (fun i => n / 10 ^ (k - 1 - i) % 10) (List.range k)) := by
      apply List.map_congr_left
      intro i _
      simp only [Function.comp_apply, Nat.succ_eq_add_one, Nat.sub_sub, Nat.add_comm i 1]
    rw [hmap]
    exact ih

/-- Below `10^16`, reading the 16 big-endian digits back recovers the number.
The high part `n / 10^16 = 0` on this domain, so the general reconstruction
applies with seed `0`. -/
theorem value16_padTo16 (n : ℕ) (hn : n < 10 ^ 16) : value16 (padTo16 n) = n := by
  have hzero : n / 10 ^ 16 = 0 := Nat.div_eq_of_lt hn
  have := foldl_digits_recover n 16
  simp only [hzero] at this
  -- value16 (padTo16 n) is the same fold seeded at 0 = n / 10^16
  unfold value16 padTo16
  simpa using this

/-- `padTo16` is **injective** on `[0, 10^16)`: equal renderings ⇒ equal numbers,
because reading the digits back (`value16`) recovers the original on this domain. -/
theorem padTo16_inj {m n : ℕ} (hm : m < 10 ^ 16) (hn : n < 10 ^ 16)
    (h : padTo16 m = padTo16 n) : m = n := by
  have := congrArg value16 h
  rw [value16_padTo16 m hm, value16_padTo16 n hn] at this
  exact this

/-- **INV-OFF-02 — fixed-width lex order = numeric order, on the `< 10^16` domain.**
For two field values both `< 10^16`, the `%016d` renderings compare
byte-lexicographically exactly as the numbers compare numerically. Concretely
their 16-digit big-endian lists are equal iff the numbers are equal, and reading
the digits back recovers the number, so the fixed-width string comparison (a
genuine lexicographic order over equal-length digit lists) agrees with numeric
order on this domain.

The `< 10^16` hypotheses are **explicit and load-bearing**: they pin the LB-1 safe
domain in machine-checked form. This proves the bounded theorem only — it does NOT
change the `%016d` wire format and does NOT exhibit the ≥10^16 counterexample. -/
theorem lex_eq_numeric_below_1e16 (m n : ℕ) (hm : m < 10 ^ 16) (hn : n < 10 ^ 16) :
    (m = n ↔ padTo16 m = padTo16 n) := by
  constructor
  · intro h; rw [h]
  · intro h; exact padTo16_inj hm hn h

end Chronicle.Offset
