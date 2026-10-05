namespace Chronicle.Upgrade

/-- The new storage API retains the boundary, rather than deleting it. -/
def truncateAfter (logs : List Nat) (boundary : Option Nat) : List Nat :=
  match boundary with
  | none => []
  | some b => logs.filter (fun i => i ≤ b)

theorem committed_retained (logs : List Nat) (i committed boundary : Nat)
    (present : i ∈ logs) (committedEntry : i ≤ committed)
    (safeBoundary : committed ≤ boundary) :
    i ∈ truncateAfter logs (some boundary) := by
  simp only [truncateAfter, List.mem_filter, decide_eq_true_eq]
  exact ⟨present, Nat.le_trans committedEntry safeBoundary⟩

theorem zero_is_not_none : truncateAfter [0, 1] (some 0) = [0] ∧
    truncateAfter [0, 1] none = [] := by decide

end Chronicle.Upgrade
