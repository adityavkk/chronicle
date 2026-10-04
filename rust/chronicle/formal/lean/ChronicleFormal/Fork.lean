namespace ChronicleFormal.Fork

inductive Decision where
  | preparing | committed | aborted
deriving DecidableEq

-- Replayed or delayed decisions cannot change a settled transaction.
def decide (old : Decision) (commit : Bool) : Decision :=
  match old with
  | .preparing => if commit then .committed else .aborted
  | settled => settled

theorem committed_is_irrevocable (later : Bool) :
    decide .committed later = .committed := by rfl

theorem aborted_is_irrevocable (later : Bool) :
    decide .aborted later = .aborted := by rfl

theorem decision_replay_idempotent (old : Decision) (first later : Bool) :
    decide (decide old first) later = decide old first := by
  cases old <;> cases first <;> rfl

structure Stream where
  bytes : List UInt8
  producers : Nat → Option Nat
  closed : Bool

def create (source : Stream) (boundary : Nat) (initial : List UInt8) : Stream :=
  ⟨source.bytes.take boundary ++ initial, fun _ => none, false⟩

theorem copied_prefix (source : Stream) (boundary : Nat) :
    ∃ suffix, (create source boundary []).bytes ++ suffix = source.bytes := by
  exact ⟨source.bytes.drop boundary, by simp [create]⟩

theorem boundary_offset (source : Stream) (boundary : Nat) (initial : List UInt8)
    (within : boundary ≤ source.bytes.length) :
    (create source boundary initial).bytes.length = boundary + initial.length := by
  simp [create, List.length_take, Nat.min_eq_left within]

theorem fork_writer_state_fresh (source : Stream) (boundary producer : Nat)
    (initial : List UInt8) :
    (create source boundary initial).producers producer = none ∧
    (create source boundary initial).closed = false := by
  exact ⟨rfl, rfl⟩

end ChronicleFormal.Fork
