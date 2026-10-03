namespace ChronicleFormal.StreamOrder

/- Byte values embed into Nat; proving the guard for all Nat lists also covers
   HTTP byte strings. No decimal parsing, case folding or empty-value elision. -/
def before : List Nat → List Nat → Prop
  | [], [] => False
  | [], _ :: _ => True
  | _ :: _, [] => False
  | a :: as, b :: bs => a < b ∨ (a = b ∧ before as bs)

def beforeDecidable : (a b : List Nat) → Decidable (before a b)
  | [], [] => inferInstanceAs (Decidable False)
  | [], _ :: _ => inferInstanceAs (Decidable True)
  | _ :: _, [] => inferInstanceAs (Decidable False)
  | a :: as, b :: bs => by
      letI := beforeDecidable as bs
      exact inferInstanceAs (Decidable (a < b ∨ (a = b ∧ before as bs)))

instance (a b : List Nat) : Decidable (before a b) := beforeDecidable a b

theorem before_trans (a b c : List Nat) (hab : before a b) (hbc : before b c) :
    before a c := by
  induction a generalizing b c with
  | nil => cases b <;> cases c <;> simp_all [before]
  | cons a as ih =>
      cases b with
      | nil => simp [before] at hab
      | cons b bs =>
          cases c with
          | nil => simp [before] at hbc
          | cons c cs =>
              simp only [before] at hab hbc ⊢
              rcases hab with hab | ⟨rfl, hab⟩
              · rcases hbc with hbc | ⟨rfl, _⟩
                · exact Or.inl (Nat.lt_trans hab hbc)
                · exact Or.inl hab
              · rcases hbc with hbc | ⟨rfl, hbc⟩
                · exact Or.inl hbc
                · exact Or.inr ⟨rfl, ih bs cs hab hbc⟩

def allowed (old next : Option (List Nat)) : Prop :=
  match old, next with
  | some old, some next => before old next
  | _, _ => True

instance (old next : Option (List Nat)) : Decidable (allowed old next) := by
  unfold allowed
  split <;> infer_instance

def applyToken (old next : Option (List Nat)) (duplicate : Bool) : Option (List Nat) :=
  if duplicate then old
  else if allowed old next then next.or old else old

theorem duplicate_preserves_token (old next : Option (List Nat)) :
    applyToken old next true = old := by simp [applyToken]

theorem absent_preserves_token (old : Option (List Nat)) :
    applyToken old none false = old := by cases old <;> simp [applyToken, allowed]

theorem rejected_preserves_token (old next : List Nat) (h : ¬ before old next) :
    applyToken (some old) (some next) false = some old := by
  simp [applyToken, allowed, h]

theorem changed_token_strictly_advances (old next : List Nat)
    (h : applyToken (some old) (some next) false ≠ some old) : before old next := by
  by_cases hb : before old next
  · exact hb
  · exact False.elim (h (rejected_preserves_token old next hb))

theorem empty_is_a_token : applyToken none (some []) false = some [] := by
  simp [applyToken, allowed]

theorem decimal_spelling_is_not_numeric : before [49, 48] [50] := by
  simp [before]

end ChronicleFormal.StreamOrder
