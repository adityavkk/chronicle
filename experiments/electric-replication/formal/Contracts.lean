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

/- Sub-offset resolution uses only the remaining range, never wrapping addition.
   JSON wire input is assumed to be validated, comma-terminated values. This
   lexical model proves chunk independence and separator exclusions, not a
   refinement of serde_json or of the native range reader. -/
def boundedAdvance (anchor count tail : Nat) : Option Nat :=
  if anchor ≤ tail ∧ count ≤ tail - anchor then some (anchor + count) else none

theorem advance_exact_and_bounded (a n t p : Nat) (h : boundedAdvance a n t = some p) :
    p = a + n ∧ a ≤ p ∧ p ≤ t := by
  unfold boundedAdvance at h
  split at h <;> simp_all <;> omega

structure JsonScan where
  quoted : Bool := false
  escaped : Bool := false
  depth : Nat := 0
  messages : Nat := 0

def scanByte (s : JsonScan) (c : Char) : JsonScan :=
  if s.quoted then
    if s.escaped then {s with escaped := false}
    else if c = '\\' then {s with escaped := true}
    else if c = '"' then {s with quoted := false}
    else s
  else match c with
    | '"' => {s with quoted := true}
    | '{' | '[' => {s with depth := s.depth + 1}
    | '}' | ']' => {s with depth := s.depth - 1}
    | ',' => if s.depth = 0 then {s with messages := s.messages + 1} else s
    | _ => s

theorem quoted_comma_is_not_boundary (s : JsonScan) (h : s.quoted = true) :
    (scanByte s ',').messages = s.messages := by
  cases he : s.escaped <;> simp [scanByte, h, he]

theorem nested_comma_is_not_boundary (s : JsonScan)
    (h : s.quoted = false) (hd : 0 < s.depth) :
    (scanByte s ',').messages = s.messages := by
  simp [scanByte, h, Nat.ne_of_gt hd]

theorem scan_chunk_independent (s : JsonScan) (a b : List Char) :
    (a ++ b).foldl scanByte s = b.foldl scanByte (a.foldl scanByte s) :=
  List.foldl_append

/- An installed snapshot may be newer than the journal's apply marker.
   Every previously published local prefix must be covered by one authority.
   Recovery stays private until its target, independent of replay chunk size. -/
def restoreTarget (snapshot marker : Nat) : Nat := max snapshot marker

theorem restore_no_published_rollback (s m published : Nat)
    (covered : published ≤ s ∨ published ≤ m) : published ≤ restoreTarget s m := by
  unfold restoreTarget
  omega

theorem replay_chunks_do_not_regress_marker (marker chunkEnd : Nat)
    (covered : chunkEnd ≤ marker) : max marker chunkEnd = marker := by
  omega

/- A checkpoint retains the segment containing its final physical frame and
   every indexed entry. New frames never refer to earlier physical segments.
   These are floor arithmetic contracts, not proofs of filesystem persistence. -/
theorem retained_location_survives (floor location : Nat) (h : floor ≤ location) :
    ¬ location < floor := by omega

theorem suffix_survives_checkpoint (floor cut later : Nat)
    (h : floor ≤ cut) (ordered : cut ≤ later) : ¬ later < floor := by omega

theorem before_cut_replay_not_needed (cut frame : Nat) (covered : frame ≤ cut) :
    ¬ cut < frame := by omega

/- Read cohorts sample the STARTED generation, not the completed generation.
   Linearizable consensus confirmation and atomic start ordering are assumptions. -/
theorem in_flight_round_not_reusable (started completed observed : Nat)
    (atInvocation : observed = started) (notNewer : completed ≤ started) :
    ¬ observed < completed := by omega

/- Immutable ordered command batches preserve the sequential state transition.
   The Rust serializer, queue, dispatcher and native effects are separate
   refinement obligations; no command permutation is allowed by this lemma. -/
theorem batch_fold_equivalent {S C : Type} (step : S → C → S)
    (initial : S) (batches : List (List C)) :
    batches.foldl (fun state batch => batch.foldl step state) initial =
      batches.flatten.foldl step initial := by
  induction batches generalizing initial with
  | nil => rfl
  | cons batch rest ih =>
    simp only [List.foldl_cons, List.flatten_cons, List.foldl_append]
    exact ih (batch.foldl step initial)

theorem singleton_metadata_identity {C : Type} (command a b : C)
    (ha : a ∈ [command]) (hb : b ∈ [command]) : a = b := by
  simp_all

theorem moving_to_consensus_keeps_charge (queued inflight moved : Nat)
    (h : moved ≤ queued) : queued - moved + (inflight + moved) = queued + inflight := by
  omega

/- A receipt is an attempt identity, not a committed-prefix session position.
   The Rust wire codec, durable notification, consensus proof and bounded
   snapshot metadata are separate refinement obligations. -/
structure ReceiptLog where
  term : Nat
  leader : Nat
  index : Nat
  deriving DecidableEq

structure ReceiptKey where
  log : ReceiptLog
  ordinal : Nat
  deriving DecidableEq

def receiptResult (wanted stored : ReceiptKey) (semanticSuccess : Bool) : Option Bool :=
  if wanted = stored then some semanticSuccess else none

theorem receipt_result_binds_identity_and_semantics (wanted stored : ReceiptKey)
    (semanticSuccess outcome : Bool)
    (h : receiptResult wanted stored semanticSuccess = some outcome) :
    wanted = stored ∧ semanticSuccess = outcome := by
  unfold receiptResult at h
  split at h <;> simp_all

def invalidationProof (wanted : ReceiptLog) (retained : Option ReceiptLog)
    (applied : Nat) : Prop :=
  match retained with
  | none => False
  | some current => wanted.index = current.index ∧ current.index ≤ applied ∧ wanted ≠ current

theorem missing_result_is_not_invalidation (wanted : ReceiptLog) (applied : Nat) :
    ¬ invalidationProof wanted none applied := by
  simp [invalidationProof]

theorem uncommitted_replacement_is_not_invalidation (wanted current : ReceiptLog)
    (applied : Nat) (h : applied < current.index) :
    ¬ invalidationProof wanted (some current) applied := by
  unfold invalidationProof
  omega

def asyncAdmission (orphaned bytes available : Nat) : Prop :=
  orphaned = 0 ∧ bytes ≤ available

theorem recovered_suffix_fences_new_admission (orphaned bytes available : Nat)
    (h : 0 < orphaned) : ¬ asyncAdmission orphaned bytes available := by
  unfold asyncAdmission
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
#print axioms advance_exact_and_bounded
#print axioms quoted_comma_is_not_boundary
#print axioms nested_comma_is_not_boundary
#print axioms scan_chunk_independent
#print axioms restore_no_published_rollback
#print axioms replay_chunks_do_not_regress_marker
#print axioms retained_location_survives
#print axioms suffix_survives_checkpoint
#print axioms before_cut_replay_not_needed
#print axioms in_flight_round_not_reusable
#print axioms batch_fold_equivalent
#print axioms singleton_metadata_identity
#print axioms moving_to_consensus_keeps_charge
#print axioms receipt_result_binds_identity_and_semantics
#print axioms missing_result_is_not_invalidation
#print axioms uncommitted_replacement_is_not_invalidation
#print axioms recovered_suffix_fences_new_admission
end ElectricReplication
