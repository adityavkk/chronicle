import Std

/- Deterministic API arithmetic, not a proof of Raft or Rust refinement. -/
namespace ElectricReplication

def visible (applied committed : Nat) : Nat := min applied committed
def sessionReady (applied committed token : Nat) : Prop := token ≤ visible applied committed
def quorum (votes members : Nat) : Prop := members < 2 * votes

theorem publication_committed (a c : Nat) : visible a c ≤ c := Nat.min_le_right a c
theorem publication_applied (a c : Nat) : visible a c ≤ a := Nat.min_le_left a c

theorem session_no_rollback (a c t : Nat) (h : sessionReady a c t) : t ≤ c := by
  exact Nat.le_trans h (publication_committed a c)

theorem session_requires_apply (a c t : Nat) (h : sessionReady a c t) : t ≤ a := by
  exact Nat.le_trans h (publication_applied a c)

theorem session_monotone (a c a' c' t : Nat)
    (h : sessionReady a c t) (ha : a ≤ a') (hc : c ≤ c') : sessionReady a' c' t := by
  unfold sessionReady visible at *
  omega

/- Pigeonhole arithmetic: two majorities cannot fit in disjoint slots of N.
   Concrete voter set membership, uniqueness and joint config are Raft duties. -/
theorem majority_overlap_slots (a b n : Nat) (ha : quorum a n) (hb : quorum b n) : n < a + b := by
  unfold quorum at *
  omega

/- A durable marker cannot justify materializing more than its own prefix. -/
theorem restore_bound (snapshot committed : Nat) (h : snapshot ≤ committed) :
    visible snapshot committed = snapshot := Nat.min_eq_left h

#print axioms publication_committed
#print axioms session_no_rollback
#print axioms session_monotone
#print axioms majority_overlap_slots
#print axioms restore_bound
end ElectricReplication
