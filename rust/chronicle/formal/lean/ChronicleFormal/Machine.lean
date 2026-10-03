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
  producers : Nat → Option Producer
  expiryIncarnation : Nat

def initial : State := ⟨1, .open, [], fun _ => none, 0⟩
def byteOffset (s : State) : Nat := (s.entries.map (·.payload.length)).sum

def accepts (previous : Option Producer) (epoch seq : Nat) : Prop :=
  match previous with
  | none => seq = 0
  | some p => (p.epoch = epoch ∧ seq = p.seq + 1) ∨ (seq = 0 ∧ p.epoch < epoch)

instance (previous : Option Producer) (epoch seq : Nat) :
    Decidable (accepts previous epoch seq) := by
  unfold accepts
  split <;> infer_instance

def apply (s : State) : Command → State
  | .write p e q i payload =>
      if i = s.incarnation ∧ s.life = .open ∧ accepts (s.producers p) e q
      then { s with entries := s.entries ++ [⟨p,e,q,i,payload⟩]
                    producers := fun x => if x=p then some ⟨e,q⟩ else s.producers x }
      else s
  | .close i => if i = s.incarnation ∧ s.life ≠ .deleted then {s with life := .closed} else s
  | .delete i => if i = s.incarnation then {s with life := .deleted} else s
  | .expire i => if i = s.incarnation ∧ i = s.expiryIncarnation
      then {s with life := .deleted} else s
  | .create i => if s.life = .deleted ∧ i = s.incarnation + 1
      then ⟨i, .open, [], fun _ => none, i⟩ else s

def replay (base : State) (xs : List Command) : State := xs.foldl apply base

theorem replay_append (s : State) (xs ys : List Command) :
    replay s (xs ++ ys) = replay (replay s xs) ys := by
  induction xs generalizing s with
  | nil => rfl
  | cons x xs ih => simp [replay, List.foldl]

theorem accepted_write_byte_offset (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.life = .open ∧ accepts (s.producers p) e q) :
    byteOffset (apply s (.write p e q s.incarnation payload)) =
      byteOffset s + payload.length := by
  simp [apply, h, byteOffset, List.sum_append]

theorem fresh_producer_accepts_zero (s : State) (p e : Nat) (payload : List UInt8)
    (hl : s.life = .open) (hp : s.producers p = none) :
    (apply s (.write p e 0 s.incarnation payload)).entries =
      s.entries ++ [⟨p,e,0,s.incarnation,payload⟩] := by
  simp [apply, accepts, hl, hp]

theorem fresh_producer_rejects_gap (s : State) (p e q : Nat) (payload : List UInt8)
    (hp : s.producers p = none) (hq : q ≠ 0) :
    apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, accepts, hp, hq]

theorem initial_epoch_zero_write (p : Nat) (payload : List UInt8) :
    byteOffset (apply initial (.write p 0 0 1 payload)) = payload.length := by
  simp [initial, apply, accepts, byteOffset]

theorem recreate_forgets_producer (s : State) (p : Nat) (hl : s.life = .deleted) :
    (apply s (.create (s.incarnation + 1))).producers p = none := by
  simp [apply, hl]

theorem write_offset_monotone (s : State) (p e q i : Nat) (payload : List UInt8) :
    byteOffset s ≤ byteOffset (apply s (.write p e q i payload)) := by
  simp only [apply]
  split <;> simp [byteOffset, List.sum_append]

theorem stale_incarnation_write_noop (s : State) (p e q i : Nat) (payload : List UInt8)
    (h : i ≠ s.incarnation) : apply s (.write p e q i payload) = s := by simp [apply, h]

theorem closed_write_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.life ≠ .open) : apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, h]

theorem exact_retry_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (h : s.producers p = some ⟨e,q⟩) : apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, accepts, h]

theorem lower_sequence_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (previous : Producer) (hp : s.producers p = some previous)
    (he : previous.epoch = e) (hq : q ≤ previous.seq) :
    apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, accepts, hp, he]
  omega

theorem epoch_regression_noop (s : State) (p e q : Nat) (payload : List UInt8)
    (previous : Producer) (hp : s.producers p = some previous) (h : e < previous.epoch) :
    apply s (.write p e q s.incarnation payload) = s := by
  simp [apply, accepts, hp]
  omega

theorem close_deleted_noop (s : State) (h : s.life = .deleted) :
    apply s (.close s.incarnation) = s := by simp [apply, h]

theorem delayed_expiry_after_create_noop (s : State) (old : Nat)
    (hi : old ≠ s.incarnation) : apply s (.expire old) = s := by simp [apply, hi]

theorem retry_after_other_producer_interleaving (s : State) (p other e q oe oq : Nat)
    (payload otherPayload : List UInt8) (hp : p ≠ other)
    (hd : s.producers p = some ⟨e,q⟩) :
    apply (apply s (.write other oe oq s.incarnation otherPayload))
      (.write p e q s.incarnation payload) =
    apply s (.write other oe oq s.incarnation otherPayload) := by
  simp only [apply]
  split <;> simp [accepts, hp, hd]

theorem prefix_recovery (s : State) (committed suffix : List Command) :
    replay s (committed ++ suffix) = replay (replay s committed) suffix := replay_append s committed suffix

/- The implementation's success-only result table, modeled independently of effects. -/
structure Retention where
  incarnation : Nat
  epoch : Nat
  seq : Nat
  frontier : Nat
  results : List (Nat × Nat)
deriving DecidableEq

def retained (q : Nat) : List (Nat × Nat) → Option Nat
  | [] => none
  | (q', end') :: rest => if q = q' then some end' else retained q rest

def retainSuccess (r : Retention) (q bytes : Nat) : Retention :=
  ⟨r.incarnation, r.epoch, q, r.frontier + bytes,
    (q, r.frontier + bytes) :: r.results⟩

inductive RetainedReply where
  | staleIncarnation | epochFenced | newEpoch | sequenceGap | duplicate (end' : Nat)
deriving DecidableEq

def retryReply (r : Retention) (inc epoch q : Nat) : RetainedReply :=
  if inc ≠ r.incarnation then .staleIncarnation
  else if epoch < r.epoch then .epochFenced
  else if r.epoch < epoch then if q = 0 then .newEpoch else .sequenceGap
  else match retained q r.results with
    | some end' => .duplicate end'
    | none => .sequenceGap

theorem newest_success_retains_original_frontier (r : Retention) (q bytes : Nat) :
    retained q (retainSuccess r q bytes).results = some (r.frontier + bytes) := by
  simp [retainSuccess, retained]

theorem later_success_preserves_old_frontier (r : Retention) (oldQ oldEnd q bytes : Nat)
    (hne : oldQ ≠ q) (h : retained oldQ r.results = some oldEnd) :
    retained oldQ (retainSuccess r q bytes).results = some oldEnd := by
  simp [retainSuccess, retained, hne, h]

theorem stale_incarnation_fences_before_cache (r : Retention) (inc epoch q : Nat)
    (h : inc ≠ r.incarnation) :
    retryReply r inc epoch q = .staleIncarnation := by simp [retryReply, h]

theorem old_epoch_fences_before_cache (r : Retention) (epoch q : Nat)
    (h : epoch < r.epoch) :
    retryReply r r.incarnation epoch q = .epochFenced := by simp [retryReply, h]

theorem new_epoch_never_uses_old_cache (r : Retention) (epoch : Nat)
    (h : r.epoch < epoch) :
    retryReply r r.incarnation epoch 0 = .newEpoch := by
  have hn : ¬ epoch < r.epoch := by omega
  simp [retryReply, h, hn]

end ChronicleFormal
