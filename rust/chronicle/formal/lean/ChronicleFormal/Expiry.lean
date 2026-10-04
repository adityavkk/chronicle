namespace ChronicleFormal.Expiry

inductive Policy where
  | sliding (duration : Nat)
  | absolute (deadline : Nat)

structure State where
  incarnation : Nat
  access : Nat
  deleted : Bool

def deadline (p : Policy) (s : State) : Nat :=
  match p with
  | .sliding duration => s.access + duration
  | .absolute time => time

def touch (p : Policy) (s : State) (inc now : Nat) : State :=
  if inc = s.incarnation ∧ s.deleted = false ∧ now ≤ deadline p s then
    match p with
    | .sliding _ => { s with access := max s.access now }
    | .absolute _ => s
  else s

def expire (p : Policy) (s : State) (inc observedAccess now : Nat) : State :=
  if inc = s.incarnation ∧ observedAccess = s.access ∧ deadline p s < now then
    { s with deleted := true }
  else s

theorem touch_deadline_monotone (p : Policy) (s : State) (inc now : Nat) :
    deadline p s ≤ deadline p (touch p s inc now) := by
  unfold touch
  split
  · cases p <;> simp [deadline, Nat.le_max_left]
  · exact Nat.le_refl _

theorem absolute_never_slides (s : State) (time inc now : Nat) :
    deadline (.absolute time) (touch (.absolute time) s inc now) = time := by
  simp [deadline]

theorem stale_incarnation_preserves (p : Policy) (s : State) (inc access now : Nat)
    (h : inc ≠ s.incarnation) : expire p s inc access now = s := by
  simp [expire, h]

theorem renewed_access_preserves (p : Policy) (s : State) (inc access now : Nat)
    (h : access ≠ s.access) : expire p s inc access now = s := by
  simp [expire, h]

theorem expired_touch_does_not_revive (p : Policy) (s : State) (inc now : Nat)
    (h : deadline p s < now) : touch p s inc now = s := by
  simp [touch, Nat.not_le_of_lt h]

end ChronicleFormal.Expiry
