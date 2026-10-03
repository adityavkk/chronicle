namespace ChronicleFormal

structure Producer where
  epoch : Nat
  seq : Nat
deriving DecidableEq
inductive Life where | open | closed | deleted deriving DecidableEq

structure Entry where
  producer : Nat
  epoch : Nat
  seq : Nat
  incarnation : Nat
  payload : List UInt8
deriving DecidableEq

inductive Command where
  | write (producer epoch seq incarnation : Nat) (payload : List UInt8)
  | close (incarnation : Nat)
  | delete (incarnation : Nat)
  | expire (expectedIncarnation : Nat)
  | create (incarnation : Nat)
deriving DecidableEq

structure State where
  incarnation : Nat
  life : Life
  entries : List Entry
  producers : Nat → Producer
  expiryIncarnation : Nat

def initial : State := ⟨0, .open, [], fun _ => ⟨0,0⟩, 0⟩
def byteOffset (s : State) : Nat := (s.entries.map (·.payload.length)).sum

def apply (s : State) : Command → State
  | .write p e q i payload =>
      if i = s.incarnation ∧ s.life = .open ∧
          (((s.producers p).epoch = e ∧ q = (s.producers p).seq + 1) ∨
           (q = 0 ∧ (s.producers p).epoch < e))
      then { s with entries := s.entries ++ [⟨p,e,q,i,payload⟩]
                    producers := fun x => if x=p then ⟨e,q⟩ else s.producers x }
      else s
  | .close i => if i = s.incarnation ∧ s.life ≠ .deleted then {s with life := .closed} else s
  | .delete i => if i = s.incarnation then {s with life := .deleted} else s
  | .expire i => if i = s.incarnation ∧ i = s.expiryIncarnation
      then {s with life := .deleted} else s
  | .create i => if s.life = .deleted ∧ i = s.incarnation + 1
      then ⟨i, .open, [], fun _ => ⟨0,0⟩, i⟩ else s

def replay (base : State) (xs : List Command) : State := xs.foldl apply base

theorem replay_append (s : State) (xs ys : List Command) :
    replay s (xs ++ ys) = replay (replay s xs) ys := by
  induction xs generalizing s with
  | nil => rfl
  | cons x xs ih => simp [replay, List.foldl]

theorem accepted_write_byte_offset (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.life = .open ∧
      (((s.producers p).epoch = e ∧ q = (s.producers p).seq + 1) ∨
       (q = 0 ∧ (s.producers p).epoch < e))) :
    byteOffset (apply s (.write p e q s.incarnation payload)) =
      byteOffset s + payload.length := by
  simp [apply, h, byteOffset, List.sum_append]

theorem stale_incarnation_write_noop (s : State) (p e q i : Nat) (payload : List UInt8)
    (h : i ≠ s.incarnation) : apply s (.write p e q i payload) = s := by simp [apply, h]

theorem closed_write_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.life ≠ .open) : apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, h]

theorem exact_retry_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.producers p = ⟨e,q⟩) : apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, h]

theorem lower_sequence_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (he : (s.producers p).epoch = e) (hq : q ≤ (s.producers p).seq) :
    apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, he]
  omega

theorem epoch_regression_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (h : e < (s.producers p).epoch) :
    apply s (.write p e q s.incarnation payload) = s := by
  simp [apply]
  omega

theorem close_deleted_noop (s : State) (h : s.life = .deleted) :
    apply s (.close s.incarnation) = s := by simp [apply, h]

theorem delayed_expiry_after_create_noop (s : State) (old : Nat)
    (hi : old ≠ s.incarnation) : apply s (.expire old) = s := by simp [apply, hi]

theorem retry_after_other_producer_interleaving (s : State) (p other e q oe oq : Nat)
    (payload otherPayload : List UInt8) (hp : p ≠ other)
    (hd : s.producers p = ⟨e,q⟩) :
    apply (apply s (.write other oe oq s.incarnation otherPayload))
      (.write p e q s.incarnation payload) =
    apply s (.write other oe oq s.incarnation otherPayload) := by
  simp only [apply]
  split <;> simp [hp, hd]

theorem prefix_recovery (s : State) (committed suffix : List Command) :
    replay s (committed ++ suffix) = replay (replay s committed) suffix := replay_append s committed suffix

end ChronicleFormal
