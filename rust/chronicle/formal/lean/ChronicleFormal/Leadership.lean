namespace Chronicle.Leadership

/-- A rank represents the total order of committed (term, node) votes for one
shard. A successful claim persists the new high-water mark before replying.
An absent mark differs from a consumed zero vote. -/
def claim (consumed : Option Nat) (vote : Nat) : Option Nat × Bool :=
  match consumed with
  | none => (some vote, true)
  | some previous =>
    if previous < vote then (some vote, true) else (consumed, false)

theorem success_persists (consumed : Option Nat) (vote : Nat)
    (granted : (claim consumed vote).2 = true) :
    (claim consumed vote).1 = some vote := by
  cases consumed with
  | none => rfl
  | some previous =>
    simp only [claim] at *
    split at granted <;> simp_all

theorem older_or_equal_denied (previous vote : Nat) (old : vote ≤ previous) :
    claim (some previous) vote = (some previous, false) := by
  simp [claim, Nat.not_lt.mpr old]

theorem successful_retry_denied (consumed : Option Nat) (vote : Nat)
    (granted : (claim consumed vote).2 = true) :
    (claim (claim consumed vote).1 vote).2 = false := by
  rw [success_persists consumed vote granted]
  simp [claim]

theorem successful_marks_increase (previous vote : Nat)
    (granted : (claim (some previous) vote).2 = true) : previous < vote := by
  simp only [claim] at granted
  split at granted <;> simp_all

end Chronicle.Leadership
