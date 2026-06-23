import Chronicle.Producer
import Mathlib.Tactic

/-!
# Producer state-machine proofs (INV-PROD-01 .. INV-PROD-07)

Function-level correctness of `Chronicle.Producer.validateProducer`, the typed
transcription of `ValidateProducer` in `store/producer.go`. Every theorem here is
universally quantified over the whole `(state, epoch, seq, now)` domain — these
replace the 9-row example table the Go side ships with the "is it correct for
*every* input?" proof obligation.

## Anti-vacuity

Each theorem is falsifiable on a deliberately-broken model: e.g. dropping the
`epoch < st.epoch ⇒ StaleEpoch` branch breaks `epoch_monotone`; making the
`seq ≤ lastSeq` branch persist state breaks `accept_iff_newState` and
`duplicate_no_mutation`. The op-list replay lemma `replay_lastSeq_monotone` is the
non-trivial one: it is proven by induction over a list of `(epoch, seq)`
operations, not by `decide`, so it cannot be vacuous.
-/

namespace Chronicle.Producer

open Chronicle.Producer

/-- **INV-PROD-01 — totality + determinism / well-formedness.**
For every input the result tuple lands in exactly one of two disjoint shapes:
either `(err = none, result ∈ {accepted, duplicate})`, or
`(err ∈ {seqGap, staleEpoch, invalidEpochSeq}, result = none)`.
Never `result = none` with `err = none`; never a non-`none` error with a
non-`none` result. Totality is free in Lean (the function is structurally total);
this theorem pins the *determinism + result/error pairing* the Go contract names. -/
theorem validateProducer_total_det
    (state : Option ProducerState) (epoch seq nowUnix : Int) :
    let r := (validateProducer state epoch seq nowUnix).1
    (r.error = .none ∧ (r.producerResult = .accepted ∨ r.producerResult = .duplicate))
    ∨ (r.error ≠ .none ∧ r.producerResult = .none) := by
  simp only [validateProducer]
  rcases state with _ | st <;>
    simp only [apply_ite Prod.fst, apply_ite Prod.snd] <;> split_ifs <;> simp

/-- **INV-PROD-02 — `newState` non-nil ⟺ Accepted.**
The "persist exactly on accept" contract: the returned `Option ProducerState`
is `some` (caller must persist) precisely when the result is `Accepted`. On
Duplicate, every error path, and first-contact rejection it is `none`. -/
theorem accept_iff_newState
    (state : Option ProducerState) (epoch seq nowUnix : Int) :
    (validateProducer state epoch seq nowUnix).2.isSome
      ↔ (validateProducer state epoch seq nowUnix).1.producerResult = .accepted := by
  simp only [validateProducer]
  rcases state with _ | st <;>
    simp only [apply_ite Prod.fst, apply_ite Prod.snd] <;> split_ifs <;> simp

/-- Companion to INV-PROD-02: on an accept the error is always `none`. -/
theorem accept_err_none
    (state : Option ProducerState) (epoch seq nowUnix : Int)
    (h : (validateProducer state epoch seq nowUnix).1.producerResult = .accepted) :
    (validateProducer state epoch seq nowUnix).1.error = .none := by
  revert h
  simp only [validateProducer]
  rcases state with _ | st <;>
    simp only [apply_ite Prod.fst, apply_ite Prod.snd] <;> split_ifs <;> simp

/-- **INV-PROD-03 — Accept advances LastSeq / Epoch / LastUpdated by the rules.**
On every accept the persisted state has `epoch = epoch` (the request epoch) and
`lastUpdated = nowUnix`, and `lastSeq` is the documented value:
fresh ⇒ 0, epoch-bump ⇒ 0, same-epoch-next ⇒ `seq`. -/
theorem accept_advances
    (state : Option ProducerState) (epoch seq nowUnix : Int)
    (st' : ProducerState)
    (h : (validateProducer state epoch seq nowUnix).2 = some st') :
    st'.epoch = epoch ∧ st'.lastUpdated = nowUnix
      ∧ (st'.lastSeq = 0 ∨ st'.lastSeq = seq) := by
  revert h
  simp only [validateProducer]
  rcases state with _ | st <;>
    simp only [apply_ite Prod.fst, apply_ite Prod.snd] <;>
    split_ifs <;> intro h <;> simp_all <;> subst h <;> simp_all

/-- **INV-PROD-04 — Epoch monotonicity / zombie fencing.**
`epoch < state.epoch` ⇒ `ErrStaleEpoch`, `currentEpoch = state.epoch`,
result `none`, and **no state mutation** (`newState = none`). -/
theorem epoch_monotone
    (st : ProducerState) (epoch seq nowUnix : Int)
    (h : epoch < st.epoch) :
    (validateProducer (some st) epoch seq nowUnix).1.producerResult = .none
    ∧ (validateProducer (some st) epoch seq nowUnix).1.error = .staleEpoch
    ∧ (validateProducer (some st) epoch seq nowUnix).1.currentEpoch = st.epoch
    ∧ (validateProducer (some st) epoch seq nowUnix).2 = none := by
  simp only [validateProducer]
  rw [if_pos h]
  exact ⟨rfl, rfl, rfl, rfl⟩

/-- Corollary of INV-PROD-04: any accepted persist has `epoch ≥ state.epoch`,
i.e. the persisted epoch is monotone non-decreasing across an accept. -/
theorem accept_epoch_nondecreasing
    (st : ProducerState) (epoch seq nowUnix : Int) (st' : ProducerState)
    (h : (validateProducer (some st) epoch seq nowUnix).2 = some st') :
    st.epoch ≤ st'.epoch := by
  revert h
  simp only [validateProducer]
  simp only [apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> intro h <;> simp_all <;> subst h <;> simp_all <;> omega

/-- **INV-PROD-05 (point form) — per-epoch idempotency, no mutation on duplicate.**
Same epoch + `seq ≤ state.lastSeq` ⇒ `Duplicate`, `err = none`,
`lastSeq` echoed unchanged, and `newState = none` (no mutation). -/
theorem duplicate_no_mutation
    (st : ProducerState) (epoch seq nowUnix : Int)
    (he : epoch = st.epoch) (h : seq ≤ st.lastSeq) :
    (validateProducer (some st) epoch seq nowUnix).1.producerResult = .duplicate
    ∧ (validateProducer (some st) epoch seq nowUnix).1.error = .none
    ∧ (validateProducer (some st) epoch seq nowUnix).1.lastSeq = st.lastSeq
    ∧ (validateProducer (some st) epoch seq nowUnix).2 = none := by
  subst he
  simp only [validateProducer, apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> simp_all <;> omega

/-- **INV-PROD-06 — exact gap detection, side-effect-free.**
Same epoch + `seq > state.lastSeq + 1` ⇒ `ErrProducerSeqGap` with
`expectedSeq = state.lastSeq + 1`, `receivedSeq = seq`, no accept, no mutation. -/
theorem gap_exact
    (st : ProducerState) (epoch seq nowUnix : Int)
    (he : epoch = st.epoch) (h : seq > st.lastSeq + 1) :
    (validateProducer (some st) epoch seq nowUnix).1.producerResult = .none
    ∧ (validateProducer (some st) epoch seq nowUnix).1.error = .seqGap
    ∧ (validateProducer (some st) epoch seq nowUnix).1.expectedSeq = st.lastSeq + 1
    ∧ (validateProducer (some st) epoch seq nowUnix).1.receivedSeq = seq
    ∧ (validateProducer (some st) epoch seq nowUnix).2 = none := by
  subst he
  simp only [validateProducer, apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> simp_all <;> omega

/-- **INV-PROD-06 (first-contact form).**
First contact + `seq ≠ 0` ⇒ gap with `expectedSeq = 0`, `receivedSeq = seq`,
no accept, no mutation. -/
theorem gap_first_contact
    (epoch seq nowUnix : Int) (h : seq ≠ 0) :
    (validateProducer none epoch seq nowUnix).1.producerResult = .none
    ∧ (validateProducer none epoch seq nowUnix).1.error = .seqGap
    ∧ (validateProducer none epoch seq nowUnix).1.expectedSeq = 0
    ∧ (validateProducer none epoch seq nowUnix).1.receivedSeq = seq
    ∧ (validateProducer none epoch seq nowUnix).2 = none := by
  simp only [validateProducer, apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> simp_all

/-- **INV-PROD-07 — new epoch must start at seq 0.**
`epoch > state.epoch ∧ seq ≠ 0` ⇒ `ErrInvalidEpochSeq`, result `none`,
no mutation. -/
theorem new_epoch_starts_at_zero
    (st : ProducerState) (epoch seq nowUnix : Int)
    (h1 : epoch > st.epoch) (h2 : seq ≠ 0) :
    (validateProducer (some st) epoch seq nowUnix).1.producerResult = .none
    ∧ (validateProducer (some st) epoch seq nowUnix).1.error = .invalidEpochSeq
    ∧ (validateProducer (some st) epoch seq nowUnix).2 = none := by
  simp only [validateProducer, apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> simp_all <;> omega

/-! ## INV-PROD-05 — at-most-once accept via op-list replay (induction)

The heart of idempotency: replaying any list of `(epoch, seq)` operations within
a *fixed epoch* is monotone in the persisted `lastSeq`, and an op is Accepted
only when `seq = lastSeq + 1`. Together: once `seq` is accepted, `lastSeq` jumps
to `seq`, and every later replay of that same `seq` is a Duplicate — a given
`(epoch, seq)` is Accepted at most once. The replay monotonicity is proven by
induction over a list of operations. -/

/-- One same-epoch step on an existing state: a persisted (accepted) post-state
has `lastSeq = seq` and `seq = old + 1`. This is the inductive step behind
"accepted at most once". -/
theorem same_epoch_step
    (st : ProducerState) (seq nowUnix : Int) (st' : ProducerState)
    (h : (validateProducer (some st) st.epoch seq nowUnix).2 = some st') :
    st'.lastSeq = seq ∧ seq = st.lastSeq + 1 := by
  revert h
  simp only [validateProducer]
  simp only [apply_ite Prod.fst, apply_ite Prod.snd]
  split_ifs <;> intro h <;> simp_all <;> subst h <;> simp_all <;> omega

/-- A replay of an already-accepted `(epoch, seq)` on the advanced state is a
Duplicate with no mutation: the second acceptance is impossible. This is
INV-PROD-05's "at most once" in point form. -/
theorem accepted_at_most_once
    (st : ProducerState) (seq nowUnix nowUnix2 : Int)
    (st' : ProducerState)
    (haccept : (validateProducer (some st) st.epoch seq nowUnix).2 = some st') :
    (validateProducer (some st') st'.epoch seq nowUnix2).1.producerResult = .duplicate
    ∧ (validateProducer (some st') st'.epoch seq nowUnix2).2 = none := by
  obtain ⟨hls, hseq⟩ := same_epoch_step st seq nowUnix st' haccept
  have hle : seq ≤ st'.lastSeq := by omega
  have := duplicate_no_mutation st' st'.epoch seq nowUnix2 rfl hle
  exact ⟨this.1, this.2.2.2⟩

/-- Replay over a list of same-epoch seqs, threading the optional persisted state.
On a `none` newState (duplicate / gap / reject) the carried state is unchanged. -/
def replay : Option ProducerState → List Int → Int → Option ProducerState
  | s, [], _ => s
  | s, seq :: rest, now =>
    match (validateProducer s (epochOf s) seq now).2 with
    | some s' => replay (some s') rest now
    | none => replay s rest now
where
  /-- the epoch to validate against: the carried state's epoch, or 0 on first
      contact (first contact ignores epoch except to stamp it). -/
  epochOf : Option ProducerState → Int
  | some st => st.epoch
  | none => 0

/-- **INV-PROD-05 (replay form) — `lastSeq` never decreases across a replay.**
Folding any list of same-epoch operations is monotone in the persisted `lastSeq`:
no replay (duplicate, gap, or accept) can move the cursor backward. Since `lastSeq`
only rises and an accept requires `seq = lastSeq + 1`, no `(epoch, seq)` is
accepted twice. Proven by induction over the operation list. -/
theorem replay_lastSeq_monotone
    (ops : List Int) (st : ProducerState) (now : Int) :
    ∀ st', replay (some st) ops now = some st' → st.lastSeq ≤ st'.lastSeq := by
  induction ops generalizing st with
  | nil => intro st' h; simp only [replay, Option.some.injEq] at h; subst h; exact le_refl _
  | cons seq rest ih =>
    intro st' h
    simp only [replay, replay.epochOf] at h
    cases hstep : (validateProducer (some st) st.epoch seq now).2 with
    | none =>
      rw [hstep] at h
      exact ih st st' h
    | some s' =>
      rw [hstep] at h
      obtain ⟨hls, hseq⟩ := same_epoch_step st seq now s' hstep
      have : s'.lastSeq ≤ st'.lastSeq := ih s' st' h
      omega

end Chronicle.Producer
