------------------------------ MODULE Composed ------------------------------
(***************************************************************************)
(* The COMPOSED Chronicle control plane: the per-subscription (gen,wake_id) *)
(* fence (SubscriptionFence, #37) composed with the per-slot owner-epoch    *)
(* register (Ownership, #38), with owner_fenced(slot, me, epoch) wired as a *)
(* guard onto EVERY schedule-mutating subscription action, exactly as the   *)
(* inlined Lua does in arm_wake / ack / expire_lease / schedule_retry /     *)
(* release.                                                                 *)
(*                                                                         *)
(* This module is the LAYERING PROOF (issue #38, INV-OWNER-02): the         *)
(* owner-epoch fence is an OPTIMIZATION that suppresses a deposed owner's    *)
(* wasted work, NEVER a correctness dependency.  Single-holder is upheld by  *)
(* the (gen,wake) fence ALONE.  We discharge it by model-checking this same  *)
(* spec under TWO values of FENCE_MODE that differ ONLY in owner_fenced:     *)
(*                                                                         *)
(*   FENCE_MODE = "Real"      -- owner_fenced has its real CAS semantics.    *)
(*   FENCE_MODE = "AlwaysPass" -- owner_fenced is forced FALSE (the outer    *)
(*                               fence is DELETED).                          *)
(*                                                                         *)
(* []SingleHolder of the inner (gen,wake) fence must hold in BOTH runs.  The *)
(* AlwaysPass run is the load-bearing one: it deletes the outer fence and    *)
(* demands the inner fence still hold, which is exactly the claim "owner-     *)
(* epoch is optimization-only" (FINDINGS.md, INVARIANTS.md Top property #10).*)
(*                                                                         *)
(* This module reproduces the SubscriptionFence state machine inline (it is  *)
(* a faithful copy of #37's actions, augmented with the owner-scope guard)   *)
(* rather than INSTANCE-ing it, so the composed `vars` tuple and the         *)
(* symmetry sets stay flat and TLC-friendly.  Where an action's guard or     *)
(* effect is unchanged from #37 the comment says "(== SubscriptionFence)".   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* worker / replica identities (a worker is also a potential slot owner)
    Subs,           \* subscription identities
    Slots,          \* ownership-slot identities (a sub's schedule lives under one slot)
    MaxGen,         \* generation ceiling
    MaxEpoch,       \* owner_epoch ceiling
    MaxClock,       \* discrete-clock ceiling
    MaxCrashes,     \* crash budget
    FENCE_MODE      \* "Real" (owner_fenced enforced) | "AlwaysPass" (owner_fenced forced FALSE)

ASSUME FENCE_MODE \in {"Real", "AlwaysPass"}

NoWorker == "none"
NoOwner  == "none"
ASSUME NoWorker \notin Workers

Phases == {"idle", "waking", "live"}
NoToken == [held |-> FALSE, gen |-> 0, wake |-> 0]
DispatchTypes == {"webhook", "pullwake"}

(* Each sub is statically homed to one slot (SubSlot): its schedule/due/lease *)
(* writes inline the owner-epoch fence against THAT slot.  With one slot this *)
(* is the degenerate #14 case; with two it exercises independent registers.   *)
VARIABLES
    sub,        \* [Subs -> SubT]                 (== SubscriptionFence)
    token,      \* [Workers -> [Subs -> TokenT]]  (== SubscriptionFence)
    clock,      \* global discrete clock          (== SubscriptionFence)
    crashes,    \* crashes consumed               (== SubscriptionFence)
    pending,    \* [Subs -> {"none","emit","stamp"}] (== SubscriptionFence)
    dueMark,    \* [Subs -> BOOLEAN]              (== SubscriptionFence)
    leaseMem,   \* [Subs -> BOOLEAN]              (== SubscriptionFence)
    slot,       \* [Slots -> [owner, epoch, lease_expiry]]  (Ownership register)
    subSlot     \* [Subs -> Slots]  static homing of a sub's schedule to a slot

vars == <<sub, token, clock, crashes, pending, dueMark, leaseMem, slot, subSlot>>

TokenT == [held: BOOLEAN, gen: 0..MaxGen, wake: 0..(MaxGen + 1)]
SubT == [ phase: Phases, gen: 0..MaxGen, wake: 0..(MaxGen + 1),
          lease_until: 0..MaxClock, wake_sent: 0..1, cursor: 0..MaxClock,
          holder: Workers \cup {NoWorker}, dispatch: DispatchTypes ]
SlotT == [ owner: Workers \cup {NoOwner}, epoch: 0..MaxEpoch, lease_expiry: 0..MaxClock ]

TypeOK ==
    /\ sub \in [Subs -> SubT]
    /\ token \in [Workers -> [Subs -> TokenT]]
    /\ clock \in 0..MaxClock
    /\ crashes \in 0..MaxCrashes
    /\ pending \in [Subs -> {"none", "emit", "stamp"}]
    /\ dueMark \in [Subs -> BOOLEAN]
    /\ leaseMem \in [Subs -> BOOLEAN]
    /\ slot \in [Slots -> SlotT]
    /\ subSlot \in [Subs -> Slots]

Init ==
    /\ \E d \in [Subs -> DispatchTypes]:
         sub = [ s \in Subs |->
                   [ phase |-> "idle", gen |-> 0, wake |-> 0,
                     lease_until |-> 0, wake_sent |-> 0, cursor |-> 0,
                     holder |-> NoWorker, dispatch |-> d[s] ] ]
    /\ token = [w \in Workers |-> [s \in Subs |-> NoToken]]
    /\ clock = 0
    /\ crashes = 0
    /\ pending = [s \in Subs |-> "none"]
    /\ dueMark = [s \in Subs |-> FALSE]
    /\ leaseMem = [s \in Subs |-> FALSE]
    /\ slot = [h \in Slots |-> [owner |-> NoOwner, epoch |-> 0, lease_expiry |-> 0]]
    /\ \E m \in [Subs -> Slots]: subSlot = m

(***************************************************************************)
(* The two fences.                                                         *)
(***************************************************************************)

(* Inner (gen,wake) fence -- byte-for-byte common.lua `fenced` (== #37).     *)
Fenced(s, reqGen, reqWake, tokGen) ==
    \/ tokGen # sub[s].gen
    \/ reqGen # sub[s].gen
    \/ reqWake = 0
    \/ reqWake # sub[s].wake

(* Outer owner-epoch fence -- common.lua `owner_fenced(slot, me, epoch)`.    *)
(* A scope is <<me, epoch>>: epoch = 0 models the empty-string epoch ''      *)
(* (the load-balanced external/hot path), which short-circuits to FALSE      *)
(* without reading the slot -- owner_fenced's first line.  A non-zero epoch  *)
(* is an owner-scoped background caller: FENCED unless it is the slot's      *)
(* current owner at the matching epoch.                                     *)
(*                                                                         *)
(* FENCE_MODE = "AlwaysPass" forces owner_fenced FALSE unconditionally --    *)
(* the outer fence is DELETED (every owner-scoped write passes), so the only *)
(* thing standing between two workers and a double-grant is the inner fence. *)
OwnerFenced(h, me, epoch) ==
    IF FENCE_MODE = "AlwaysPass" THEN FALSE
    ELSE IF epoch = 0 THEN FALSE                           \* epoch '' => skip (external path)
    ELSE \/ slot[h].owner # me                             \* not the current owner
         \/ slot[h].epoch # epoch                          \* stale epoch

(* A scoped guard for a mutating action on sub s by worker w presenting an   *)
(* owner-epoch scope <<me, epoch>>.  We let TLC choose the scope: epoch=0    *)
(* (external/hot path, no owner check) OR the worker presents itself as an   *)
(* owner at SOME epoch in 1..MaxEpoch (an owner-scoped background caller,    *)
(* possibly a DEPOSED one carrying a stale epoch -- the late-write scenario).*)
PassesOwner(s, me, epoch) == ~OwnerFenced(subSlot[s], me, epoch)

OffsetGreater(a, b) == a > b
ClaimRotatesFence(phase, wake) == phase # "waking" \/ wake = 0
LeaseExpired(leaseUntil) == leaseUntil # 0 /\ clock >= leaseUntil
SlotLeaseExpired(h) == slot[h].lease_expiry <= clock
MinPlus(c, d) == IF c + d > MaxClock THEN MaxClock ELSE c + d

----------------------------------------------------------------------------
(***************************************************************************)
(* OWNERSHIP-REGISTER ACTIONS (== Ownership): claim_shard / depose / tick.  *)
(***************************************************************************)
ClaimShard(me, h) ==
    /\ ~(slot[h].owner # NoOwner /\ slot[h].owner # me /\ ~SlotLeaseExpired(h))
    /\ LET isRenew == slot[h].owner = me
       IN /\ \/ isRenew \/ slot[h].epoch < MaxEpoch
          /\ slot' = [slot EXCEPT
                        ![h].owner = me,
                        ![h].epoch = IF isRenew THEN slot[h].epoch ELSE slot[h].epoch + 1,
                        ![h].lease_expiry = MinPlus(clock, 1)]
    /\ UNCHANGED <<sub, token, clock, crashes, pending, dueMark, leaseMem, subSlot>>

Depose(h) ==
    /\ slot[h].owner # NoOwner
    /\ slot' = [slot EXCEPT ![h].lease_expiry = 0]
    /\ UNCHANGED <<sub, token, clock, crashes, pending, dueMark, leaseMem, subSlot>>

----------------------------------------------------------------------------
(***************************************************************************)
(* MUTATING SUBSCRIPTION ACTIONS, each fronted by the owner-epoch guard.    *)
(*                                                                         *)
(* The owner scope <<me, epoch>> is a parameter chosen by Next: epoch = 0    *)
(* is the external/hot path (no owner check); epoch in 1..MaxEpoch is an     *)
(* owner-scoped background caller.  PassesOwner gates the GRANTING branch;   *)
(* a FENCED owner check is a no-op (grants nothing, mutates nothing) -- the  *)
(* `return {'FENCED'}` at the top of each inlined-Lua script.               *)
(***************************************************************************)

(* --- Arm: arm_wake.lua (owner-scoped: the dueWorker arms a wake) -------- *)
Arm(s, me, epoch) ==
    /\ PassesOwner(s, me, epoch)                 \* owner_fenced(KEYS[4], ARGV[6], ARGV[7])
    /\ sub[s].phase = "idle"
    /\ sub[s].gen < MaxGen
    /\ LET g == sub[s].gen + 1
           w == sub[s].gen + 1
           isWebhook == sub[s].dispatch = "webhook"
       IN sub' = [sub EXCEPT
                    ![s].gen = g, ![s].wake = w, ![s].phase = "waking",
                    ![s].holder = NoWorker,
                    ![s].lease_until = IF isWebhook THEN MinPlus(clock, 1) ELSE 0,
                    ![s].wake_sent = IF isWebhook THEN sub[s].wake_sent ELSE 0]
          /\ leaseMem' = [leaseMem EXCEPT ![s] = isWebhook]
    /\ pending' = [pending EXCEPT ![s] = "emit"]
    /\ dueMark' = [dueMark EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<token, clock, crashes, slot, subSlot>>

(* --- WriteWakeEvent halves (== SubscriptionFence) ----------------------- *)
WakeAppend(s) ==
    /\ pending[s] = "emit"
    /\ sub[s].dispatch = "pullwake"
    /\ pending' = [pending EXCEPT ![s] = "stamp"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem, slot, subSlot>>

WakeStamp(s) ==
    /\ pending[s] = "stamp"
    /\ sub[s].dispatch = "pullwake"
    /\ pending' = [pending EXCEPT ![s] = "none"]
    /\ IF sub[s].phase = "waking" /\ sub[s].wake # 0
         THEN sub' = [sub EXCEPT ![s].wake_sent = 1]
         ELSE sub' = sub
    /\ UNCHANGED <<token, clock, crashes, dueMark, leaseMem, slot, subSlot>>

WebhookEmit(s) ==
    /\ pending[s] = "emit"
    /\ sub[s].dispatch = "webhook"
    /\ pending' = [pending EXCEPT ![s] = "none"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem, slot, subSlot>>

(* --- Claim: claim.lua.  The external/hot CLAIM path is load-balanced and   *)
(* passes epoch '' (no owner scope), so claim does NOT inline owner_fenced   *)
(* (it is NOT in the inlined-Lua set: arm_wake/ack/expire_lease/             *)
(* schedule_retry/release).  We model it WITHOUT an owner guard, matching    *)
(* the code.  (== SubscriptionFence otherwise.)                             *)
Claim(w, s) ==
    /\ sub[s].gen < MaxGen \/ ~ClaimRotatesFence(sub[s].phase, sub[s].wake)
    /\ ~(sub[s].phase = "live" /\ sub[s].holder # NoWorker /\ ~LeaseExpired(sub[s].lease_until))
    /\ LET rotate == ClaimRotatesFence(sub[s].phase, sub[s].wake)
           g == IF rotate THEN sub[s].gen + 1 ELSE sub[s].gen
           wk == IF rotate THEN sub[s].gen + 1 ELSE sub[s].wake
       IN /\ sub' = [sub EXCEPT
                       ![s].phase = "live", ![s].holder = w,
                       ![s].gen = g, ![s].wake = wk,
                       ![s].lease_until = MinPlus(clock, 1)]
          /\ token' = [token EXCEPT ![w][s] = [held |-> TRUE, gen |-> g, wake |-> wk]]
          /\ leaseMem' = [leaseMem EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<clock, crashes, pending, dueMark, slot, subSlot>>

(* --- Ack: ack.lua (owner-epoch guard inlined at top, THEN the (gen,wake)   *)
(* fence).  The owner guard is checked first; if it FENCES the ack is a       *)
(* no-op.  Then the inner fence is the SOLE safety gate.                      *)
Ack(w, s, done, reqOff, me, epoch) ==
    /\ PassesOwner(s, me, epoch)                 \* owner_fenced(KEYS[6], replica, epoch) at ack.lua top
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ LET newCur == IF OffsetGreater(reqOff, sub[s].cursor) THEN reqOff ELSE sub[s].cursor
       IN IF done
            THEN /\ sub' = [sub EXCEPT
                              ![s].cursor = newCur, ![s].phase = "idle",
                              ![s].holder = NoWorker, ![s].wake = 0, ![s].lease_until = 0]
                 /\ token' = [token EXCEPT ![w][s] = NoToken]
                 /\ dueMark' = [dueMark EXCEPT ![s] = FALSE]
                 /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]
            ELSE /\ sub' = [sub EXCEPT
                              ![s].cursor = newCur, ![s].phase = "live",
                              ![s].lease_until = MinPlus(clock, 1)]
                 /\ leaseMem' = [leaseMem EXCEPT ![s] = TRUE]
                 /\ UNCHANGED <<token, dueMark>>
    /\ UNCHANGED <<clock, crashes, pending, slot, subSlot>>

(* --- Release: release.lua (owner-epoch guard inlined, then (gen,wake)). --- *)
Release(w, s, me, epoch) ==
    /\ PassesOwner(s, me, epoch)
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ sub' = [sub EXCEPT
                 ![s].phase = "idle", ![s].holder = NoWorker,
                 ![s].wake = 0, ![s].lease_until = 0]
    /\ token' = [token EXCEPT ![w][s] = NoToken]
    /\ dueMark' = [dueMark EXCEPT ![s] = FALSE]
    /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<clock, crashes, pending, slot, subSlot>>

(* --- ExpireLease: expire_lease.lua (owner-scoped: the lease worker is the   *)
(* primary owner-scoped caller; owner-epoch guard inlined at top).  The       *)
(* faithful expire idles + clears wake_id WITHOUT rotating gen (the FENCED     *)
(* escape hatch, INV-FENCE-04).  This module always uses the faithful clear   *)
(* (the unfaithful variant is the #37 negative test, not re-run here).        *)
ExpireLease(s, me, epoch) ==
    /\ PassesOwner(s, me, epoch)                 \* owner_fenced(KEYS[4], ARGV[3], ARGV[4]) at expire_lease.lua top
    /\ sub[s].phase \in {"live", "waking"}
    /\ LeaseExpired(sub[s].lease_until)
    /\ leaseMem[s]
    /\ sub' = [sub EXCEPT
                 ![s].phase = "idle", ![s].holder = NoWorker,
                 ![s].wake = 0, ![s].lease_until = 0]
    /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]
    /\ dueMark' = [dueMark EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<token, clock, crashes, pending, slot, subSlot>>

(* --- SweepReemit: manager.go sweepOnce re-emit (== SubscriptionFence) ----- *)
SweepReemit(s) ==
    /\ sub[s].dispatch = "pullwake"
    /\ sub[s].phase = "waking"
    /\ pending[s] = "none"
    /\ pending' = [pending EXCEPT ![s] = "emit"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem, slot, subSlot>>

(* --- DueDrain: DecideDue (== SubscriptionFence) --------------------------- *)
DueDrain(s) ==
    /\ dueMark[s]
    /\ IF sub[s].phase # "idle"
         THEN dueMark' = dueMark
         ELSE dueMark' = [dueMark EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<sub, token, clock, crashes, pending, leaseMem, slot, subSlot>>

(* --- Tick (== SubscriptionFence, also advances slot lease clock) ---------- *)
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<sub, token, crashes, pending, dueMark, leaseMem, slot, subSlot>>

(* --- Crash (== SubscriptionFence; slot register survives, like the sub      *)
(* hash -- ownership records are durable in Redis, only the lease lapses via  *)
(* membership which Depose models).  Crash drops volatile Go follow-ups and   *)
(* worker w's tokens.                                                         *)
Crash(w) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ pending' = [s \in Subs |-> "none"]
    /\ token' = [token EXCEPT ![w] = [s \in Subs |-> NoToken]]
    /\ UNCHANGED <<sub, clock, dueMark, leaseMem, slot, subSlot>>

----------------------------------------------------------------------------
(***************************************************************************)
(* DEPOSED-OWNER-LATE-WRITE SCENARIO.                                       *)
(*                                                                         *)
(* The scenario the issue requires: an owner-scoped caller whose slot was   *)
(* TRANSFERRED attempts a schedule mutation carrying its now-STALE epoch.    *)
(* In FENCE_MODE = Real this write is FENCED by owner_fenced and is a no-op. *)
(* In FENCE_MODE = AlwaysPass the owner fence is DELETED, so the write       *)
(* actually executes -- and SingleHolder must still absorb it via the inner  *)
(* (gen,wake) fence and the empty wake_id left by expire_lease.             *)
(*                                                                         *)
(* The mutating actions above already accept ANY <<me, epoch>> scope chosen  *)
(* by Next, including a stale (deposed) one, so the scenario is reachable    *)
(* directly.  This predicate is the COVERAGE WITNESS that a GENUINE deposed   *)
(* late write fires: the sub's slot is currently owned by some worker `cur`   *)
(* at a real epoch (>= 1), AND a DIFFERENT worker `dep` presenting that       *)
(* slot's epoch (a former owner whose ownership was transferred away) has a   *)
(* mutating op enabled on the sub.  Requiring a real current owner (not the   *)
(* unowned init slot) and a distinct deposed caller proves a transfer-then-   *)
(* late-write actually occurred, not a trivial init-state hit.  (Negate it    *)
(* as an INVARIANT to extract the witnessing trace.)                          *)
DeposedLateWriteEnabled ==
    \E s \in Subs, dep \in Workers:
        LET h == subSlot[s] IN
        /\ slot[h].owner \in Workers          \* the slot is genuinely owned now ...
        /\ slot[h].owner # dep                \* ... by someone OTHER than the deposed caller
        /\ slot[h].epoch >= 1                  \* at a real (minted) epoch
        \* `dep` presents the slot's current epoch but is NOT its owner -> owner_fenced
        \* would FENCE it under Real; under AlwaysPass it slips through and mutates.
        /\ \/ (sub[s].phase = "idle" /\ sub[s].gen < MaxGen)           \* an Arm would mutate
           \/ (\E ww \in Workers: token[ww][s].held
                 /\ ~Fenced(s, token[ww][s].gen, token[ww][s].wake, token[ww][s].gen))  \* an Ack/Release would mutate
NotDeposedLateWrite == ~DeposedLateWriteEnabled

----------------------------------------------------------------------------
(***************************************************************************)
(* The owner-scope domain Next quantifies over.  epoch 0 = external/hot      *)
(* path (no owner check); 1..MaxEpoch = an owner-scoped background caller    *)
(* (any epoch, so deposed/stale scopes are included).                       *)
(***************************************************************************)
Epochs == 0..MaxEpoch
Offsets == 0..MaxClock

Next ==
    \/ \E s \in Subs, me \in Workers, e \in Epochs: Arm(s, me, e)
    \/ \E s \in Subs: WakeAppend(s)
    \/ \E s \in Subs: WakeStamp(s)
    \/ \E s \in Subs: WebhookEmit(s)
    \/ \E w \in Workers, s \in Subs: Claim(w, s)
    \/ \E w \in Workers, s \in Subs, off \in Offsets, me \in Workers, e \in Epochs: Ack(w, s, TRUE, off, me, e)
    \/ \E w \in Workers, s \in Subs, off \in Offsets, me \in Workers, e \in Epochs: Ack(w, s, FALSE, off, me, e)
    \/ \E w \in Workers, s \in Subs, me \in Workers, e \in Epochs: Release(w, s, me, e)
    \/ \E s \in Subs, me \in Workers, e \in Epochs: ExpireLease(s, me, e)
    \/ \E s \in Subs: SweepReemit(s)
    \/ \E s \in Subs: DueDrain(s)
    \/ Tick
    \/ \E w \in Workers: Crash(w)
    \* Ownership-register transitions:
    \/ \E me \in Workers, h \in Slots: ClaimShard(me, h)
    \/ \E h \in Slots: Depose(h)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.  SingleHolder is byte-identical to SubscriptionFence  *)
(* (#37): the layering proof asserts THIS inner-fence property in BOTH the   *)
(* Real and AlwaysPass runs.                                                 *)
(***************************************************************************)
AckAcceptable(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)

(* INV-FENCE-01 (SingleHolder): the inner-fence single-holder property.      *)
SingleHolder ==
    \A s \in Subs:
        \A w1, w2 \in Workers:
            (w1 # w2 /\ AckAcceptable(w1, s) /\ AckAcceptable(w2, s)) => FALSE

AtMostOneInflightWake ==
    \A s \in Subs:
        \A w1, w2 \in Workers:
            ( AckAcceptable(w1, s) /\ AckAcceptable(w2, s) )
              => ( token[w1][s].gen = token[w2][s].gen
                   /\ token[w1][s].wake = token[w2][s].wake )

(* INV-OWNER-01 (carried into the composed run): single owner per slot.      *)
SingleOwner ==
    \A h \in Slots:
        \A r1, r2 \in Workers:
            (r1 # r2 /\ slot[h].owner = r1 /\ slot[h].owner = r2) => FALSE

CursorBounded == \A s \in Subs: sub[s].cursor \in 0..MaxClock

(* The composed safety conjunction.  TLC checks this as an INVARIANT under    *)
(* BOTH FENCE_MODE values; SingleHolder holding under AlwaysPass discharges   *)
(* INV-OWNER-02 (layering: owner-epoch is optimization-only).                 *)
Inv ==
    /\ TypeOK
    /\ SingleHolder
    /\ AtMostOneInflightWake
    /\ SingleOwner
    /\ CursorBounded

(* Action properties (carried from #37 / Ownership).                         *)
GenMonotone == \A s \in Subs: sub'[s].gen >= sub[s].gen
EpochMonotone == \A h \in Slots: slot'[h].epoch >= slot[h].epoch
GenMonotoneProp == [][GenMonotone]_vars
EpochMonotoneProp == [][EpochMonotone]_vars

StateConstraint ==
    /\ \A s \in Subs: sub[s].gen <= MaxGen
    /\ \A h \in Slots: slot[h].epoch <= MaxEpoch
    /\ clock <= MaxClock
    /\ crashes <= MaxCrashes

=============================================================================
