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

structure Fence where
  incarnation : Nat
  generation : Nat
  wake : Nat
  deriving DecidableEq

def authorized (current presented : Fence) (now deadline : Nat) : Prop :=
  current = presented ∧ now < deadline

theorem recreated_fences (c p : Fence) (now deadline : Nat)
    (h : c.incarnation ≠ p.incarnation) : ¬ authorized c p now deadline := by
  intro a
  exact h (congrArg Fence.incarnation a.1)

theorem generation_fences (c p : Fence) (now deadline : Nat)
    (h : c.generation ≠ p.generation) : ¬ authorized c p now deadline := by
  intro a
  exact h (congrArg Fence.generation a.1)

theorem expired_fences (c p : Fence) (now deadline : Nat)
    (h : deadline ≤ now) : ¬ authorized c p now deadline := by
  intro a
  exact Nat.not_lt_of_ge h a.2

/- A synchronous done can consume only its snapshot. Later appends remain work.
   The caller must additionally check the stream incarnation; this is arithmetic. -/
theorem snapshot_ack_keeps_suffix (acked snapshot tail : Nat)
    (ha : acked ≤ snapshot) (ht : snapshot < tail) : max acked snapshot < tail := by
  omega

inductive ForkDecision where
  | undecided | granted | aborted | released
  deriving DecidableEq

def decideFork (d : ForkDecision) (valid : Bool) : ForkDecision :=
  match d with
  | .undecided => if valid then .granted else .aborted
  | other => other

theorem grant_irrevocable (valid : Bool) :
    decideFork .granted valid = .granted := rfl

theorem abort_fences_delayed_grant (valid : Bool) :
    decideFork .aborted valid = .aborted := rfl

theorem release_fences_delayed_grant (valid : Bool) :
    decideFork .released valid = .released := rfl

/- Contiguous bounded import preserves exactly the requested prefix. Bytes and
   stream identity are separate obligations of the native range reader. -/
def importEnd (copied target limit : Nat) : Nat := min target (copied + limit)

theorem import_bounds (copied target limit : Nat) (h : copied ≤ target) :
    copied ≤ importEnd copied target limit ∧ importEnd copied target limit ≤ target := by
  unfold importEnd
  omega

theorem import_progress (copied target limit : Nat) (h : copied < target) (hl : 0 < limit) :
    copied < importEnd copied target limit := by
  unfold importEnd
  omega

theorem publication_exact_prefix (copied target : Nat)
    (bounded : copied ≤ target) (ready : target ≤ copied) : copied = target := by
  omega

theorem descendant_prevents_release (refs : Nat) (hasDescendant : 0 < refs) : refs ≠ 0 := by
  omega

#print axioms publication_committed
#print axioms session_no_rollback
#print axioms session_monotone
#print axioms majority_overlap_slots
#print axioms restore_bound
#print axioms recreated_fences
#print axioms generation_fences
#print axioms expired_fences
#print axioms snapshot_ack_keeps_suffix
#print axioms grant_irrevocable
#print axioms abort_fences_delayed_grant
#print axioms release_fences_delayed_grant
#print axioms import_bounds
#print axioms import_progress
#print axioms publication_exact_prefix
#print axioms descendant_prevents_release
end ElectricReplication
