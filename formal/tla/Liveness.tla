------------------------------ MODULE Liveness ------------------------------
(***************************************************************************)
(* Liveness / fairness encoding for Chronicle's wake recovery (issue #38,   *)
(* INV-WAKE-01, INV-RECOVER-01, INV-RECOVER-02, INV-DUE-01, INV-JEP-L1-01).  *)
(*                                                                         *)
(* This module is a DELIBERATELY SMALL pull-wake model (one subscription,   *)
(* one or two workers) carrying just enough state to state and check the     *)
(* temporal properties under weak fairness of the sweep and due-drain loops. *)
(* It is NOT a second copy of the full safety state machine -- the safety    *)
(* invariants are discharged by SubscriptionFence (#37) and Composed (#38).  *)
(* Here we check:                                                           *)
(*                                                                         *)
(*  (1) PendingWorkLeadsToWake -- []( idle /\ HasPendingWork ) ~> WakeIssued *)
(*      under WF on the sweep/due loops (INV-WAKE-01, the headline liveness).*)
(*  (2) StrandedLeadsToReemit  -- a pull-wake stranded in the arm-before-emit *)
(*      window (wake_sent=0, crash window 1) OR the post-emit T4 window       *)
(*      (wake_sent#0, no lease, never claimed) is EVENTUALLY re-emitted under *)
(*      WF of the sweep, with the 3*sweepInterval threshold parameterized as  *)
(*      ReemitTicks (INV-RECOVER-01 / INV-RECOVER-02; the LB-5 magic number). *)
(*  (3) NoDoubleLiveHolder == []SingleHolder under a SlowConsumer that was    *)
(*      emitted-to but has not yet claimed when a re-emit fires: the re-emit  *)
(*      degrades to AT-LEAST-ONCE, never two simultaneously ack-acceptable    *)
(*      holders (INV-JEP-L1-01).  Safety here is THRESHOLD-INDEPENDENT: the   *)
(*      (gen,wake) fence is the backstop regardless of ReemitTicks.           *)
(*                                                                         *)
(* TIMING ABSTRACTION (the LB-5 model).  The wall-clock test                 *)
(*   now - wake_event_sent_ns > 3*sweepInterval                              *)
(* is modeled discretely: while a pull-wake is stranded waking AND its wake   *)
(* was durably emitted (wake_sent=1), each SWEEP tick increments a per-sub    *)
(* staleTicks counter.  The T4 re-emit branch becomes ENABLED only once       *)
(* staleTicks > ReemitTicks (ReemitTicks = 3 sweep ticks = the 3*sweep        *)
(* floor).  This makes "3x" a tuning knob for WHEN re-emit happens; the       *)
(* safety backstop (the (gen,wake) fence) is independent of its value, which  *)
(* the sensitivity check (a lower ReemitTicks still preserving SingleHolder)  *)
(* demonstrates.                                                             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Workers,        \* a small worker set, e.g. {w1, w2}
    MaxGen,         \* generation ceiling
    ReemitTicks,    \* the discrete 3*sweepInterval floor: T4 re-emit enabled once staleTicks > ReemitTicks
    MaxStale        \* ceiling on staleTicks (state-space bound; MaxStale > ReemitTicks)

NoWorker == "none"
ASSUME NoWorker \notin Workers
Phases == {"idle", "waking", "live"}
NoToken == [held |-> FALSE, gen |-> 0, wake |-> 0]

VARIABLES
    phase,        \* the single sub's phase
    gen,          \* current generation (HINCRBY +1 on arm / rotate)
    wake,         \* current wake_id (0 = empty)
    wake_sent,    \* 0 = wake event not durably emitted (T1), 1 = emitted (T4 key)
    leaseUntil,   \* modeled lease deadline (0 = pull-wake has no lease)
    cursor,       \* delivery cursor (0 = before tail, advances to tail on done-ack)
    tail,         \* the modeled stream tail; HasPendingWork == cursor < tail
    holder,       \* current lease holder (NoWorker = none)
    token,        \* [Workers -> token]  what each worker carries
    emitted,      \* BOOLEAN: a wake event is sitting in the stream for a worker to claim
    staleTicks,   \* discrete age of the emitted-but-unclaimed wake (the 3*sweep floor counter)
    sweptTick     \* BOOLEAN toggle: TRUE in the instant a sweep tick just elapsed (helper)

vars == <<phase, gen, wake, wake_sent, leaseUntil, cursor, tail, holder, token, emitted, staleTicks, sweptTick>>

TokenT == [held: BOOLEAN, gen: 0..MaxGen, wake: 0..(MaxGen + 1)]

TypeOK ==
    /\ phase \in Phases
    /\ gen \in 0..MaxGen
    /\ wake \in 0..(MaxGen + 1)
    /\ wake_sent \in 0..1
    /\ leaseUntil \in 0..1
    /\ cursor \in 0..1
    /\ tail \in 0..1
    /\ holder \in Workers \cup {NoWorker}
    /\ token \in [Workers -> TokenT]
    /\ emitted \in BOOLEAN
    /\ staleTicks \in 0..MaxStale
    /\ sweptTick \in BOOLEAN

(***************************************************************************)
(* Init.  A pull-wake sub with pending work (tail = 1 > cursor = 0), idle,  *)
(* generation 0, no wake yet, nothing emitted.                              *)
(***************************************************************************)
Init ==
    /\ phase = "idle"
    /\ gen = 0
    /\ wake = 0
    /\ wake_sent = 0
    /\ leaseUntil = 0
    /\ cursor = 0
    /\ tail = 1                 \* pending work exists from the start
    /\ holder = NoWorker
    /\ token = [w \in Workers |-> NoToken]
    /\ emitted = FALSE
    /\ staleTicks = 0
    /\ sweptTick = FALSE

HasPendingWork == cursor < tail
Fenced(reqGen, reqWake, tokGen) ==
    \/ tokGen # gen \/ reqGen # gen \/ reqWake = 0 \/ reqWake # wake
AckAcceptable(w) == token[w].held /\ ~Fenced(token[w].gen, token[w].wake, token[w].gen)

----------------------------------------------------------------------------
(***************************************************************************)
(* THE DUE-DRAIN LOOP (the dueWorker; DecideDue).                           *)
(* DueFire: idle + pending work -> arm a wake (mint gen+1, fresh wake,       *)
(*          phase=waking, pull-wake sets wake_sent=0, no lease).             *)
(* DueClear: idle + caught up -> nothing to do (no transition needed).       *)
(* DueSkip: non-idle -> leave it (a wake is in flight).                      *)
(***************************************************************************)
DueFire ==
    /\ phase = "idle"
    /\ HasPendingWork
    /\ gen < MaxGen
    /\ gen' = gen + 1
    /\ wake' = gen + 1
    /\ phase' = "waking"
    /\ wake_sent' = 0
    /\ leaseUntil' = 0          \* pull-wake: no lease at arm
    /\ holder' = NoWorker
    /\ emitted' = FALSE         \* the wake event is not yet appended (window T1)
    /\ staleTicks' = 0
    /\ UNCHANGED <<cursor, tail, token, sweptTick>>

(***************************************************************************)
(* THE SWEEP LOOP (manager.go sweepOnce), two re-emit branches.             *)
(*                                                                         *)
(* SweepEmitT1 -- crash window 1 (arm-before-emit): pull-wake waking,        *)
(*   wake_sent=0.  The Go follow-up never appended the event; sweepOnce      *)
(*   re-emits immediately (no staleness gate).  Sets wake_sent=1, emitted.   *)
(*                                                                         *)
(* SweepEmitT4 -- crash window 4 / T4 (post-emit, never-claimed): pull-wake  *)
(*   waking, wake_sent=1, no lease, but the emitted event was never claimed  *)
(*   (it aged out: staleTicks > ReemitTicks).  Re-emit the SAME (gen,wake).  *)
(*   Gated on the 3*sweepInterval floor (staleTicks > ReemitTicks).          *)
(*                                                                         *)
(* SweepTick -- advances staleTicks for an emitted-but-unclaimed stranded    *)
(*   wake, the discrete model of wall-clock elapsing between sweeps.         *)
(***************************************************************************)
SweepEmitT1 ==
    /\ phase = "waking"
    /\ wake_sent = 0
    /\ leaseUntil = 0           \* pull-wake (no lease)
    /\ wake_sent' = 1
    /\ emitted' = TRUE
    /\ staleTicks' = 0          \* freshly (re-)emitted: age resets
    /\ UNCHANGED <<phase, gen, wake, leaseUntil, cursor, tail, holder, token, sweptTick>>

SweepEmitT4 ==
    /\ phase = "waking"
    /\ wake_sent = 1
    /\ leaseUntil = 0           \* pull-wake arms no lease, so LeaseExpired never fires
    /\ ~(\E w \in Workers: holder = w)   \* never claimed (no live holder)
    /\ staleTicks > ReemitTicks          \* the 3*sweepInterval floor reached
    /\ emitted' = TRUE                   \* re-emit the SAME (gen, wake)
    /\ staleTicks' = 0                   \* re-emit resets the age (idempotent per floor window)
    /\ UNCHANGED <<phase, gen, wake, wake_sent, leaseUntil, cursor, tail, holder, token, sweptTick>>

SweepTick ==
    /\ phase = "waking"
    /\ wake_sent = 1
    /\ leaseUntil = 0
    /\ ~(\E w \in Workers: holder = w)
    /\ staleTicks < MaxStale
    /\ staleTicks' = staleTicks + 1
    /\ UNCHANGED <<phase, gen, wake, wake_sent, leaseUntil, cursor, tail, holder, token, emitted, sweptTick>>

(* LeaseLapse -- the modeled lease deadline passing on a live holder (the     *)
(* claim-before-ack / slow-consumer / GC-pause window, crash window 4).  The  *)
(* lease lapses (leaseUntil -> 0) but the holder KEEPS its in-memory token --  *)
(* it is a SLOW consumer, still carrying its (gen,wake), not yet acked.  This  *)
(* is the SlowConsumer state: a worker that was emitted-to and claimed but is  *)
(* now slow/paused.  After the lapse, the sub falls back to a waking-style     *)
(* recoverable state (re-owed) and a re-emit / takeover can fire -- and that   *)
(* second delivery MUST NOT create two ack-acceptable holders.  expire_lease   *)
(* clears wake_id WITHOUT rotating gen (INV-FENCE-04): the slow holder's token  *)
(* is then fenced by the now-EMPTY wake_id, so it is no longer ack-acceptable. *)
LeaseLapse ==
    /\ phase = "live"
    /\ holder # NoWorker
    /\ leaseUntil # 0
    \* expire_lease.lua: idle, clear wake_id, clear lease; gen UNCHANGED, holder gone.
    \* The slow consumer KEEPS its in-memory token (UNCHANGED token), but that token
    \* is now FENCED by the empty wake_id (INV-FENCE-04: gen unchanged, wake cleared),
    \* so it is no longer ack-acceptable.  Idling re-owes the due mark, so the
    \* dueWorker re-arms (DueFire) a FRESH (gen+1, wake) and a second worker can claim
    \* it -- a TAKEOVER that leaves the slow holder carrying a stale, fenced token
    \* while the new holder is the sole ack-acceptable one (the genuine double-token,
    \* single-holder race the (gen,wake) fence must absorb).
    /\ phase' = "idle"          \* expire returns to idle (re-armable)
    /\ wake' = 0                \* wake_id CLEARED (the FENCED escape hatch) -> slow holder fenced
    /\ leaseUntil' = 0
    /\ holder' = NoWorker       \* the lease holder slot is cleared
    /\ wake_sent' = 0           \* a fresh arm will set this per dispatch (pull-wake: 0)
    /\ emitted' = FALSE         \* the original event was consumed by the (now-slow) claimant
    /\ staleTicks' = 0
    /\ UNCHANGED <<gen, cursor, tail, token, sweptTick>>

----------------------------------------------------------------------------
(***************************************************************************)
(* THE WORKER (consumer) actions: claim an emitted wake, then ack it.       *)
(* A SlowConsumer is a worker that has been emitted-to but has not yet       *)
(* claimed -- exactly the window a re-emit can land in.                      *)
(***************************************************************************)

(* Claim: a worker claims an in-flight wake (claim.lua + ClaimRotatesFence).  *)
(*                                                                         *)
(* BUSY guard (claim.lua:30-34): an UNEXPIRED live lease held by ANOTHER      *)
(* worker blocks the claim -- so a SECOND worker that races on a re-emitted   *)
(* wake while the first is still live (lease not expired) is REFUSED, which   *)
(* is exactly why a re-emit to a slow-but-still-live consumer cannot create   *)
(* two live holders.  This is modeled by leaving the claim DISABLED (a no-op) *)
(* when a live unexpired holder exists.                                       *)
(*                                                                         *)
(* When grantable, ClaimRotatesFence decides:                                *)
(*   * coalesce (phase=waking, wake set): reuse (gen,wake) -- the normal      *)
(*     first claim of an emitted wake.                                        *)
(*   * rotate (phase=live with an EXPIRED lease, i.e. taking over a deposed   *)
(*     holder): HINCRBY +1, fresh wake -> FENCES the deposed holder's token   *)
(*     (so the prior holder is no longer ack-acceptable: still single-holder).*)
(* `emitted` is required so a claim only fires when an event is actually in   *)
(* the stream (the SlowConsumer window a re-emit lands in).                   *)
Claim(w) ==
    /\ emitted
    /\ wake # 0
    \* BUSY: an unexpired live lease held by another worker blocks the claim.
    /\ ~(phase = "live" /\ holder # NoWorker /\ holder # w /\ leaseUntil # 0)
    /\ LET rotate == (phase # "waking")           \* coalesce only on waking-with-wake; else rotate
           g2 == IF rotate THEN gen + 1 ELSE gen
           wk2 == IF rotate THEN gen + 1 ELSE wake
       IN /\ (rotate => gen < MaxGen)             \* rotate needs gen headroom
          /\ gen' = g2
          /\ wake' = wk2
          /\ token' = [token EXCEPT ![w] = [held |-> TRUE, gen |-> g2, wake |-> wk2]]
    /\ phase' = "live"
    /\ holder' = w
    /\ leaseUntil' = 1          \* claim arms the lease
    /\ emitted' = FALSE         \* the wake event is consumed by this claim
    /\ staleTicks' = 0
    /\ UNCHANGED <<wake_sent, cursor, tail, sweptTick>>

(* Ack(done): a live holder acks.  done advances the cursor to the tail and   *)
(* idles the sub (delivery complete); the (gen,wake) fence is the sole gate.  *)
AckDone(w) ==
    /\ AckAcceptable(w)
    /\ phase = "live"
    /\ holder = w
    /\ cursor' = tail           \* delivered: cursor reaches tail (INV-JEP-L1-01)
    /\ phase' = "idle"
    /\ holder' = NoWorker
    /\ wake' = 0
    /\ leaseUntil' = 0
    /\ token' = [token EXCEPT ![w] = NoToken]
    /\ emitted' = FALSE
    /\ staleTicks' = 0
    /\ UNCHANGED <<gen, wake_sent, tail, sweptTick>>

----------------------------------------------------------------------------
(***************************************************************************)
(* WakeIssued: a wake is currently in flight for delivery -- either an event *)
(* is emitted in the stream (emitted), or a worker is already live on it     *)
(* (holder set).  This is the target of the leads-to.                        *)
(***************************************************************************)
WakeIssued == emitted \/ (phase \in {"waking", "live"} /\ wake # 0)

(* StrandedT1 / StrandedT4: the two crash-window stranded states. *)
StrandedT1 == phase = "waking" /\ wake_sent = 0 /\ leaseUntil = 0
StrandedT4 == phase = "waking" /\ wake_sent = 1 /\ leaseUntil = 0
              /\ ~(\E w \in Workers: holder = w)

----------------------------------------------------------------------------
(* BaseNext is the wake-recovery loop WITHOUT the crash-after-claim fault.    *)
(* The leads-to (liveness) properties are checked over BaseNext under WF: a   *)
(* claimed wake is always delivered (no adversarial re-lapse), so the only    *)
(* thing that can stall delivery is the sweep/due loop failing to fire --     *)
(* which is exactly what the fairness assumption rules out.                   *)
BaseNext ==
    \/ DueFire
    \/ SweepEmitT1
    \/ SweepEmitT4
    \/ SweepTick
    \/ \E w \in Workers: Claim(w)
    \/ \E w \in Workers: AckDone(w)

Next == BaseNext

(* SafetyNext adds LeaseLapse -- the crash-after-claim / slow-consumer fault   *)
(* (crash window 4).  Used ONLY for the NoDoubleLiveHolder safety check (an    *)
(* INVARIANT, no fairness needed): a re-emit / takeover after a slow holder    *)
(* lapsed must never yield two simultaneously ack-acceptable holders.          *)
SafetyNext == BaseNext \/ LeaseLapse

(***************************************************************************)
(* FAIRNESS.  Weak fairness of the sweep and due-drain loops is what makes   *)
(* the liveness properties hold: a continuously-enabled sweep/due step must  *)
(* eventually fire.  We also need WF on SweepTick (so the staleTicks counter *)
(* actually advances toward the T4 threshold) and on the consumer's Claim/   *)
(* Ack (so a claimed wake is eventually delivered, closing the leads-to).    *)
(*                                                                         *)
(* The DUE/SWEEP loops are the server's background workers (WF models "the   *)
(* loop keeps running").  Claim/AckDone get WF too: a live consumer that can  *)
(* make progress eventually does.  Removing the sweep/due WF (the FairNone   *)
(* spec, checked by the no-fairness cfg) breaks the leads-to -> TLC CEX,     *)
(* confirming the property is NON-TRIVIAL.                                   *)
(***************************************************************************)
Fairness ==
    /\ WF_vars(DueFire)
    /\ WF_vars(SweepEmitT1)
    /\ WF_vars(SweepEmitT4)
    /\ WF_vars(SweepTick)
    /\ \A w \in Workers: WF_vars(Claim(w))
    /\ \A w \in Workers: WF_vars(AckDone(w))

Spec == Init /\ [][Next]_vars /\ Fairness

(* The no-fairness spec: same safety, but NO weak fairness on sweep/due.     *)
(* The leads-to properties must FAIL here (a CEX where the loop never fires).*)
SpecNoFair == Init /\ [][Next]_vars

(* The safety spec including the crash-after-claim fault (LeaseLapse), for    *)
(* the NoDoubleLiveHolder invariant under the SlowConsumer + re-emit scenario.*)
SafetySpec == Init /\ [][SafetyNext]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* TEMPORAL PROPERTIES.                                                      *)
(***************************************************************************)

(* INV-WAKE-01: pending work eventually produces a wake. *)
PendingWorkLeadsToWake == [](( phase = "idle" /\ HasPendingWork ) ~> WakeIssued)

(* INV-RECOVER-01 / INV-RECOVER-02: a stranded pull-wake (T1 window, or the  *)
(* T4 post-emit window) is eventually re-emitted -- i.e. an emitted event is  *)
(* eventually sitting in the stream for a consumer.  Stated as: being         *)
(* stranded leads to `emitted` (a re-emit has put the event back).           *)
StrandedT1LeadsToReemit == [](StrandedT1 ~> emitted)
StrandedT4LeadsToReemit == [](StrandedT4 ~> emitted)

(* The headline liveness: pending work is eventually DELIVERED (cursor       *)
(* reaches tail).  This ties the leads-to to the three-lemma at-least-once   *)
(* chain (INV-JEP-L1-02): a wake that is issued is eventually claimed and     *)
(* acked, advancing the cursor to the tail.                                  *)
PendingWorkEventuallyDelivered == [](HasPendingWork ~> (cursor = tail))

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY under the SlowConsumer + re-emit scenario.                        *)
(*                                                                         *)
(* NoDoubleLiveHolder: even though SweepEmitT4 can re-emit a wake while a    *)
(* SlowConsumer has been emitted-to (the duplicate-delivery window), no two  *)
(* workers are EVER simultaneously ack-acceptable.  The (gen,wake) fence is  *)
(* the backstop: a re-emit reuses the SAME (gen,wake), so a second claimant  *)
(* coalesces onto the identical fence rather than minting a rival one, and    *)
(* only one worker can hold the lease (holder) at a time.  This is THRESHOLD- *)
(* INDEPENDENT -- it holds for ANY ReemitTicks, which the sensitivity cfg     *)
(* (ReemitTicks = 0) demonstrates.                                           *)
(***************************************************************************)
SingleHolder ==
    \A w1, w2 \in Workers:
        (w1 # w2 /\ AckAcceptable(w1) /\ AckAcceptable(w2)) => FALSE

NoDoubleLiveHolder == SingleHolder

StateConstraint ==
    /\ gen <= MaxGen
    /\ staleTicks <= MaxStale

=============================================================================
