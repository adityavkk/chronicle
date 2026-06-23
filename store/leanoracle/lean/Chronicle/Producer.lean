/-!
# Idempotent-producer state machine (typed transcription)

Faithful typed transcription of `ValidateProducer` from
`store/producer.go` (Chronicle repo, this worktree). This is the P0.5
skeleton: a **total** Lean function mirroring the Go branch-for-branch.
No theorems are stated here; the producer state-machine totality and
determinism proofs (INV-PROD-01) land in the P1.1 pure-core-proofs issue.

## int64 / uint64 correspondence

Go `epoch`, `seq`, `nowUnix` and the `ProducerState` fields are all Go
`int64`. They are modeled here as Lean `Int` (the unbounded mathematical
integers), NOT `Int64`. The producer state machine only compares these
values and adds `+ 1` to `LastSeq`; it never relies on `int64`
wrap-around, so `Int` is the faithful and proof-friendly model of the Go
semantics over the realised domain. (Offsets, which DO wrap, use `UInt64`
in `Chronicle/Offset.lean`.)

`nowUnix` only stamps `LastUpdated` on an accept; the Go source already
injects the clock, so it is an ordinary `Int` parameter here.
-/

namespace Chronicle.Producer

/-- Producer validation outcome.

Mirror of `ProducerResult` in `store/store.go`:
`ProducerResultNone = 0`, `ProducerResultAccepted = 1`,
`ProducerResultDuplicate = 2`. -/
inductive ProducerResult where
  | none      -- ProducerResultNone: no append; an error class accompanies it
  | accepted  -- ProducerResultAccepted: new data accepted
  | duplicate -- ProducerResultDuplicate: idempotent duplicate (204)
  deriving Repr, DecidableEq, Inhabited

/-- Producer-validation error class.

Mirror of the three producer sentinels in `store/store.go`:
`ErrStaleEpoch`, `ErrInvalidEpochSeq`, `ErrProducerSeqGap`. `none`
models the Go `nil` error (accept / duplicate). Modeled as a sum type so
the result is exhaustive and the P1 totality/determinism proof has a
named codomain. -/
inductive ProducerError where
  | none            -- nil error (Accepted or Duplicate)
  | seqGap          -- ErrProducerSeqGap
  | staleEpoch      -- ErrStaleEpoch
  | invalidEpochSeq -- ErrInvalidEpochSeq
  deriving Repr, DecidableEq, Inhabited

/-- The producer's current epoch/sequence state.

Mirror of `ProducerState` in `store/store.go`. All fields are Go `int64`,
modeled as `Int`. -/
structure ProducerState where
  epoch       : Int
  lastSeq     : Int
  lastUpdated : Int
  deriving Repr, DecidableEq, Inhabited

/-- The response-shaping result of one validation.

Mirror of the producer-relevant fields of `AppendResult` in
`store/store.go`. Only the fields `ValidateProducer` populates are
modeled: `producerResult`, the error class, and `currentEpoch`,
`expectedSeq`, `receivedSeq`, `lastSeq`. The Go `AppendResult` also
carries `Offset` and `StreamClosed`, which `ValidateProducer` never sets,
so they are omitted from this mirror. Fields the Go code leaves at their
zero value are modeled as `0` to match Go struct zero-initialisation, so
the P1.2 oracle can compare the whole tuple. -/
structure AppendResult where
  producerResult : ProducerResult
  error          : ProducerError
  currentEpoch   : Int := 0
  expectedSeq    : Int := 0
  receivedSeq    : Int := 0
  lastSeq        : Int := 0
  deriving Repr, DecidableEq, Inhabited

/-- `validateProducer state epoch seq nowUnix` applies the idempotent-producer
state machine to one request. `state` is `none` on first contact. Returns the
response-shaping `AppendResult` plus the new `ProducerState` to persist
(`some` only when the caller must persist, matching the non-nil `*ProducerState`
of the Go source).

Branch-for-branch transcription of `ValidateProducer` in
`store/producer.go`:

1. first contact (`state = none`): seq must be 0 ⇒ Accepted (lastSeq 0),
   else SeqGap with `expectedSeq = 0`, `receivedSeq = seq`;
2. stale epoch (`epoch < state.epoch`) ⇒ StaleEpoch with
   `currentEpoch = state.epoch`;
3. epoch bump (`epoch > state.epoch`): seq must be 0 ⇒ Accepted
   (lastSeq 0), else InvalidEpochSeq;
4. duplicate (`seq ≤ state.lastSeq`) ⇒ Duplicate with
   `lastSeq = state.lastSeq`;
5. next (`seq = state.lastSeq + 1`) ⇒ Accepted with `lastSeq = seq`;
6. gap (`seq > state.lastSeq + 1`) ⇒ SeqGap with
   `expectedSeq = state.lastSeq + 1`, `receivedSeq = seq`. -/
def validateProducer
    (state : Option ProducerState) (epoch seq nowUnix : Int) :
    AppendResult × Option ProducerState :=
  match state with
  | none =>
    -- No existing state - accept as new producer.
    if seq != 0 then
      -- First message from producer must be seq=0.
      ( { producerResult := .none
        , error := .seqGap
        , expectedSeq := 0
        , receivedSeq := seq }
      , none )
    else
      ( { producerResult := .accepted
        , error := .none
        , lastSeq := 0 }
      , some { epoch := epoch, lastSeq := 0, lastUpdated := nowUnix } )
  | some st =>
    -- Epoch validation (client-declared, server-validated).
    if epoch < st.epoch then
      -- Stale epoch - zombie fencing.
      ( { producerResult := .none
        , error := .staleEpoch
        , currentEpoch := st.epoch }
      , none )
    else if epoch > st.epoch then
      -- New epoch - must start at seq=0.
      if seq != 0 then
        ( { producerResult := .none
          , error := .invalidEpochSeq }
        , none )
      else
        ( { producerResult := .accepted
          , error := .none
          , lastSeq := 0 }
        , some { epoch := epoch, lastSeq := 0, lastUpdated := nowUnix } )
    else
      -- Same epoch - sequence validation.
      if seq <= st.lastSeq then
        -- Duplicate - idempotent success.
        ( { producerResult := .duplicate
          , error := .none
          , lastSeq := st.lastSeq }
        , none )
      else if seq == st.lastSeq + 1 then
        ( { producerResult := .accepted
          , error := .none
          , lastSeq := seq }
        , some { epoch := epoch, lastSeq := seq, lastUpdated := nowUnix } )
      else
        -- seq > lastSeq + 1 - gap detected.
        ( { producerResult := .none
          , error := .seqGap
          , expectedSeq := st.lastSeq + 1
          , receivedSeq := seq }
        , none )

end Chronicle.Producer
