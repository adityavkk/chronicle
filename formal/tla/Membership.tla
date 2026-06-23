---------------------------- MODULE Membership ----------------------------
(***************************************************************************)
(* Chronicle cluster MEMBERSHIP + HRW (rendezvous-hash) slot ownership +    *)
(* slot-reconcile CONVERGENCE, modeled at the implementation grain          *)
(* (issue #40, the L2/L4/L5 surface and INV-HRW-01 / INV-JEP-L4-01).        *)
(*                                                                         *)
(* This is the DISTRIBUTED layer that Ownership.tla (#38) does NOT cover:   *)
(* Ownership.tla is one slot's owner-epoch CAS register in isolation; here  *)
(* we model the WHOLE convergence loop -- the members ZSET (heartbeat +     *)
(* lease eviction), the per-replica HRW computation, and the slotReconcile  *)
(* loop that CASes each HRW-targeted slot -- and prove that under weak       *)
(* fairness, AFTER CHURN STOPS, every slot ends with EXACTLY ONE unexpired  *)
(* owner (no zero-owner coverage gap, no >1 split-brain), and STAYS there.  *)
(*                                                                         *)
(* Mirrors byte-for-byte:                                                   *)
(*   - Heartbeat (redis_store.go:793): ZADD members me -> now+memberTTL,    *)
(*     then ZREMRANGEBYSCORE members -inf "(now"  (evict expired).          *)
(*   - LiveMembers (redis_store.go:806): ZRANGEBYSCORE "(now" +inf -- a     *)
(*     member is live iff its lease score is STRICTLY GREATER than now.     *)
(*   - slotReconcileOnce (manager.go:1049): read live members, compute      *)
(*     TargetedSlots(me, members) = { h : HRWOwner(members,h) = me }, and    *)
(*     ClaimSlot each targeted slot. held = HRW-targeted INTERSECT granted   *)
(*     (CAS authority: ownership.go OwnedSlots).                            *)
(*   - HRWOwner (ownership.go:278): argmax over live members of a per-      *)
(*     (replica,slot) score, tie-broken by greatest replica id. The score   *)
(*     is a deterministic, language-stable hash every replica agrees on; we  *)
(*     model it as a CONSTANT total preference Score[r][h] (TLC enumerates   *)
(*     all assignments via a symmetric instantiation, or pins one).         *)
(*                                                                         *)
(* L3 lease-tail-drop refinement (INV-LR-01 / INV-JEP-L3-01) is modeled in  *)
(* MembershipLeaseTail.tla, which EXTENDS the per-slot lease ZSET with a     *)
(* DropLeaseTail action (ZREM the schedule entry, durable owner hash intact) *)
(* and a Reconcile that RE-DERIVES the entry from the durable hash.          *)
(*                                                                         *)
(* TIME is modeled as RELATIVE remaining-TTL counters (see the TIME MODEL    *)
(* block below) that gate ONLY lease liveness (member lease expiry, slot      *)
(* lease expiry), exactly as the ">(now" score-minus-now bounds do in the     *)
(* store. The HRW math and the CAS epoch algebra are TIME-FREE.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Replicas,      \* set of replica identities, e.g. {r1, r2, r3}
    Slots,         \* set of ownership-slot identities, e.g. {h1, h2}
    Score,         \* [Replicas -> [Slots -> Nat]]: the HRW score (deterministic hash). All
                   \* replicas agree on it. Distinct scores per (r,h) -> a clean argmax.
    MemberTTL,     \* member lease TTL (ticks): a heartbeat (re)sets the member's remaining ttl.
    SlotTTL,       \* slot lease TTL (ticks): a claim_shard (re)sets the slot's remaining ttl.
    MaxEpoch,      \* owner_epoch ceiling (state-space bound)
    MaxChurn       \* max # of Join/Crash churn events before churn MUST stop. Bounds the
                   \* state space AND the number of pre-StopChurn transfers, so MaxEpoch
                   \* can be sized with headroom and the epoch ceiling NEVER blocks the
                   \* post-churn convergence transfers (the state-constraint-during-
                   \* liveness hazard TLC warns about). REQUIRE MaxEpoch > MaxChurn + |Slots|.

(***************************************************************************)
(* TIME MODEL -- RELATIVE REMAINING-TTL, not an absolute clock.             *)
(*                                                                         *)
(* A lease is modeled by a "ticks remaining until expiry" counter in        *)
(* 0..TTL: a heartbeat / claim_shard RESETS it to the full TTL; a Tick      *)
(* DECREMENTS every live counter toward 0; reaching 0 means EXPIRED (the    *)
(* ZSET score has fallen at/below now -> ZREMRANGEBYSCORE evicts it, or      *)
(* claim_shard sees an expired foreign owner). This is the standard         *)
(* lease-expiry idiom and is faithful to the store: the absolute UnixNano    *)
(* score and the absolute `now` only ever matter via their DIFFERENCE       *)
(* (score - now = remaining lease), so tracking the difference directly      *)
(* removes the artificial absolute-clock ceiling that would otherwise make   *)
(* "a renewed lease stays live forever" inexpressible in a bounded model.    *)
(* A renewed lease (WF heartbeat) is reset to TTL every time it would reach  *)
(* 0, so it never lapses -- exactly the survivor's behavior.                 *)
(***************************************************************************)

NoOwner == "none"
ASSUME NoOwner \notin Replicas
\* Distinct HRW scores rule out ties, so HRWOwner is a clean single argmax. (Real
\* ties are tie-broken by greatest replica id in ownership.go; modeling distinct
\* scores is WLOG for the convergence property -- the tie-break only picks A winner.)
ASSUME \A r1, r2 \in Replicas, h \in Slots:
            (r1 # r2) => Score[r1][h] # Score[r2][h]

VARIABLES
    alive,         \* [Replicas -> BOOLEAN]: TRUE iff the process is running (its
                   \* heartbeat loop is alive). Join -> TRUE, Crash -> FALSE. This is
                   \* SEPARATE from the lease counter: a crashed replica can still hold
                   \* a not-yet-lapsed member lease (the ZSET score persists until the
                   \* TTL passes), and an alive replica keeps renewing its lease.
    memberTTL,     \* [Replicas -> 0..MemberTTL]: ticks of member lease remaining.
                   \* 0 == lease lapsed (evicted from the members ZSET). An ALIVE
                   \* replica is renewed before it reaches 0 (see TimeMayPass).
    slotOwner,     \* [Slots -> Replicas \cup {NoOwner}]: claim_shard owner_id
    slotEpoch,     \* [Slots -> Nat]: claim_shard owner_epoch (HINCRBY on transfer)
    slotTTL,       \* [Slots -> 0..SlotTTL]: ticks of slot lease remaining. 0 == lapsed.
    churnStopped,  \* BOOLEAN: once TRUE, no more Join/Crash. The convergence window
                   \* opens here; the liveness property is "eventually after this".
    churnLeft      \* Nat: remaining churn budget (Join/Crash events). 0 disables churn.

vars == <<alive, memberTTL, slotOwner, slotEpoch, slotTTL, churnStopped, churnLeft>>

TypeOK ==
    /\ alive \in [Replicas -> BOOLEAN]
    /\ memberTTL \in [Replicas -> 0..MemberTTL]
    /\ slotOwner \in [Slots -> Replicas \cup {NoOwner}]
    /\ slotEpoch \in [Slots -> 0..MaxEpoch]
    /\ slotTTL \in [Slots -> 0..SlotTTL]
    /\ churnStopped \in BOOLEAN
    /\ churnLeft \in 0..MaxChurn

----------------------------------------------------------------------------
(***************************************************************************)
(* Membership predicates -- mirror the ">(now" exclusive score bounds.      *)
(***************************************************************************)
\* A replica is a LIVE member iff it has lease ticks remaining -- the relative
\* form of LiveMembers' "(now" exclusive lower bound (score - now > 0), so a
\* score that has fallen to/below now (ttl 0) is "not live", exactly what
\* ZREMRANGEBYSCORE evicts (redis_store.go:808).
IsLiveMember(r) == memberTTL[r] > 0
LiveMembers == { r \in Replicas : IsLiveMember(r) }

\* A slot lease is unexpired iff it has ticks remaining -- the relative form of
\* claim_shard's "exp > now" test (Ownership.tla SlotLeaseExpired negated).
SlotLeaseLive(h) == slotTTL[h] > 0

----------------------------------------------------------------------------
(***************************************************************************)
(* HRW -- HRWOwner(members, h) = argmax_{r in members} Score[r][h]          *)
(* (ownership.go:278). With distinct scores (ASSUMEd), the argmax is a       *)
(* single replica.  ok=FALSE (no target) iff there are no live members.     *)
(***************************************************************************)
HRWTargetExists(h) == LiveMembers # {}
HRWOwner(h) ==
    CHOOSE r \in LiveMembers :
        \A r2 \in LiveMembers : Score[r][h] >= Score[r2][h]

\* The slots HRW assigns to `me` (TargetedSlots, ownership.go:291).
TargetedBy(me) == { h \in Slots : HRWTargetExists(h) /\ HRWOwner(h) = me }

----------------------------------------------------------------------------
(***************************************************************************)
(* Init.  No members, every slot unowned (owner NoOwner, epoch 0 -- HINCRBY  *)
(* mints 1 on the first claim), no slot lease, churn still ongoing.          *)
(***************************************************************************)
Init ==
    /\ alive = [r \in Replicas |-> FALSE]
    /\ memberTTL = [r \in Replicas |-> 0]
    /\ slotOwner = [h \in Slots |-> NoOwner]
    /\ slotEpoch = [h \in Slots |-> 0]
    /\ slotTTL = [h \in Slots |-> 0]
    /\ churnStopped = FALSE
    /\ churnLeft = MaxChurn

----------------------------------------------------------------------------
(***************************************************************************)
(* MEMBERSHIP actions.                                                      *)
(***************************************************************************)

\* Heartbeat(r): redis_store.go Heartbeat. ZADD members r -> clock+MemberTTL,
\* then evict expired (handled by the LiveMembers/SlotLeaseLive predicates,
\* which read against the live clock -- an expired score is simply "not live",
\* exactly what ZREMRANGEBYSCORE removes; we do not need a separate evict step
\* because no action ever READS an expired entry as live). This is the renew
\* that keeps a replica in the live set; weak-fairness on it is what makes a
\* survivor stay live (convergence). A replica heartbeats only while alive,
\* i.e. while it is ALREADY a member OR is (re)joining; a crashed replica's
\* heartbeat loop is stopped (modeled by Crash setting churn + this guard).
Heartbeat(r) ==
    /\ alive[r]                            \* the heartbeat loop runs iff the process is up
    /\ memberTTL' = [memberTTL EXCEPT ![r] = MemberTTL]   \* renew to full TTL
    /\ UNCHANGED <<alive, slotOwner, slotEpoch, slotTTL, churnStopped, churnLeft>>

\* Join(r): a fresh replica boots and its first heartbeat lands (membership
\* churn). Disabled once churn has stopped (the convergence window).
Join(r) ==
    /\ ~churnStopped
    /\ churnLeft > 0
    /\ ~alive[r]                            \* not currently a running process
    /\ alive' = [alive EXCEPT ![r] = TRUE]
    /\ memberTTL' = [memberTTL EXCEPT ![r] = MemberTTL]
    /\ churnLeft' = churnLeft - 1
    /\ UNCHANGED <<slotOwner, slotEpoch, slotTTL, churnStopped>>

\* Crash(r): a replica's process dies. Its heartbeat loop STOPS (alive -> FALSE),
\* so it no longer renews -- but its member-ZSET entry PERSISTS at its last score
\* until the TTL passes (memberTTL is NOT zeroed here; Tick lapses it, exactly as
\* ZREMRANGEBYSCORE evicts it only once its score falls at/below now). Its OWNED
\* slot leases likewise are NOT cleared here -- they lapse by their own slot TTL,
\* and then a survivor's claim_shard takes them over (a transfer / epoch bump).
\* Disabled once churn has stopped.
Crash(r) ==
    /\ ~churnStopped
    /\ churnLeft > 0
    /\ alive[r]
    /\ alive' = [alive EXCEPT ![r] = FALSE]
    /\ churnLeft' = churnLeft - 1
    /\ UNCHANGED <<memberTTL, slotOwner, slotEpoch, slotTTL, churnStopped>>

\* StopChurn: close the membership-churn window. After this, no Join/Crash; the
\* surviving members keep heartbeating (WF) and the reconcile loop converges.
\* At least one replica must survive (else there is no one to own the slots --
\* the "all dead" case is not a convergence claim, it is total outage).
StopChurn ==
    /\ ~churnStopped
    /\ \E r \in Replicas : alive[r]         \* at least one survivor (else total outage)
    /\ churnStopped' = TRUE
    /\ UNCHANGED <<alive, memberTTL, slotOwner, slotEpoch, slotTTL, churnLeft>>

----------------------------------------------------------------------------
(***************************************************************************)
(* SLOT RECONCILE -- the claim_shard CAS driven by the HRW target.          *)
(*                                                                         *)
(* ClaimSlot(me, h): manager.go slotReconcileOnce CASes each HRW-targeted   *)
(* slot. The Lua claim_shard semantics (Ownership.tla ClaimShard):          *)
(*   - BUSY iff a live foreign owner holds an unexpired lease -> no-op.      *)
(*   - owner = me -> RENEW (epoch kept), refresh the slot lease.            *)
(*   - else (unowned or expired-foreign) -> CLAIM (HINCRBY +1, take owner). *)
(* The reconcile loop only ATTEMPTS slots `me` HRW-targets, so the action   *)
(* is guarded by `h \in TargetedBy(me)` AND `me` being live.                *)
(***************************************************************************)
ReconcileClaim(me, h) ==
    /\ IsLiveMember(me)
    /\ h \in TargetedBy(me)
    \* BUSY guard: a live foreign owner blocks the claim (claim_shard.lua:30).
    /\ ~(slotOwner[h] # NoOwner /\ slotOwner[h] # me /\ SlotLeaseLive(h))
    /\ LET isRenew == slotOwner[h] = me
       IN /\ \/ isRenew                       \* renew: any epoch ok
             \/ slotEpoch[h] < MaxEpoch        \* transfer/first-claim: epoch headroom
          /\ slotOwner' = [slotOwner EXCEPT ![h] = me]
          /\ slotEpoch' = [slotEpoch EXCEPT ![h] =
                              IF isRenew THEN slotEpoch[h] ELSE slotEpoch[h] + 1]
          /\ slotTTL'   = [slotTTL   EXCEPT ![h] = SlotTTL]   \* (re)set the slot lease
    /\ UNCHANGED <<alive, memberTTL, churnStopped, churnLeft>>

----------------------------------------------------------------------------
(***************************************************************************)
(* Tick -- real time passes: DECREMENT every lease counter toward 0 (a       *)
(* member/slot whose remaining ttl reaches 0 has lapsed).                     *)
(*                                                                         *)
(* THE HEARTBEAT-HEADROOM GATE (INV-MEMBER-01 encoding). Tick is DISABLED if  *)
(* it would drive an ALIVE replica's member lease to 0. This is the model-    *)
(* level statement of CheckOwnershipConfig's heartbeatInterval <             *)
(* memberLeaseTTL/2: an alive replica's heartbeat ALWAYS fires before its     *)
(* lease can lapse, so time cannot starve a survivor's membership. (Strong    *)
(* fairness on Heartbeat then forces that heartbeat to actually fire while    *)
(* Tick is blocked, advancing the run.) A DEAD (crashed) replica has no such  *)
(* gate -- its lease is allowed to lapse, which is exactly how a crashed       *)
(* owner's slot becomes claimable by a survivor. Tick is enabled only while   *)
(* SOME counter is still positive (else it would be a pure no-op).            *)
(***************************************************************************)
Decr(x) == IF x > 0 THEN x - 1 ELSE 0
Tick ==
    \* member gate: never lapse an ALIVE replica's MEMBER lease -- it heartbeats
    \* faster than the member TTL (heartbeatInterval < memberLeaseTTL/2).
    /\ \A r \in Replicas : alive[r] => memberTTL[r] > 1
    \* slot gate: never lapse an ALIVE owner's SLOT lease when that owner is still
    \* the HRW target -- the slotReconcileLoop re-claims (renews) an owned slot at
    \* least as often as the heartbeat (slotReconcileInterval <= heartbeatInterval,
    \* INV-MEMBER-01), so an owned-and-still-targeted slot lease never lapses. A slot
    \* whose owner is DEAD or NO LONGER targeted is free to lapse -- that is exactly
    \* how a crashed/deposed owner's slot becomes claimable by the survivor.
    /\ \A h \in Slots :
            ( slotOwner[h] # NoOwner /\ alive[slotOwner[h]]
              /\ HRWTargetExists(h) /\ HRWOwner(h) = slotOwner[h] )
            => slotTTL[h] > 1
    /\ \/ \E r \in Replicas : memberTTL[r] > 0
       \/ \E h \in Slots : slotTTL[h] > 0
    /\ memberTTL' = [r \in Replicas |-> Decr(memberTTL[r])]
    /\ slotTTL'   = [h \in Slots |-> Decr(slotTTL[h])]
    /\ UNCHANGED <<alive, slotOwner, slotEpoch, churnStopped, churnLeft>>

----------------------------------------------------------------------------
Next ==
    \/ \E r \in Replicas : Heartbeat(r)
    \/ \E r \in Replicas : Join(r)
    \/ \E r \in Replicas : Crash(r)
    \/ StopChurn
    \/ \E me \in Replicas, h \in Slots : ReconcileClaim(me, h)
    \/ Tick

----------------------------------------------------------------------------
(***************************************************************************)
(* FAIRNESS.  Convergence is a LIVENESS claim, so it needs weak fairness:    *)
(*   - WF on each survivor's Heartbeat: a continuously-alive replica keeps    *)
(*     renewing its lease, so it does not falsely lapse out of LiveMembers.   *)
(*   - WF on each (me,h) ReconcileClaim: a continuously-enabled claim (me is  *)
(*     live AND HRW-targets h AND the slot is grantable) eventually fires --  *)
(*     this is the slotReconcileLoop ticking.                                 *)
(* We do NOT put fairness on Tick (the clock may stall), nor on Join/Crash    *)
(* (churn is adversarial, not fair). After StopChurn, the only enabled        *)
(* membership actions are the survivors' Heartbeats (fair) and the reconcile  *)
(* claims (fair), so the system is driven to the single-owner fixed point.    *)
(***************************************************************************)
(***************************************************************************)
(* FAIRNESS, and why Heartbeat needs STRONG fairness.                        *)
(*                                                                         *)
(*  - WF on Tick: real time passes, so a crashed owner's STALE slot lease     *)
(*    eventually lapses (its ttl decrements to 0) and becomes claimable -- the *)
(*    non-Zeno requirement that makes a zero-owner gap close.                  *)
(*                                                                         *)
(*  - SF (STRONG fairness) on each survivor's Heartbeat. This is the          *)
(*    model-level encoding of INV-MEMBER-01 / CheckOwnershipConfig's          *)
(*    `heartbeatInterval < memberLeaseTTL/2`: a heartbeat fires strictly       *)
(*    faster than the lease TTL, so a continuously-alive replica's lease is    *)
(*    ALWAYS renewed before it can lapse. Weak fairness is NOT enough here:    *)
(*    Tick could otherwise decrement a live member MemberTTL times in a row    *)
(*    (each step still leaving it enabled) and drive it to 0, after which      *)
(*    Heartbeat is disabled and WF no longer obligates it -- a SPURIOUS        *)
(*    starvation that the real timing config rules out. SF requires only that  *)
(*    Heartbeat be enabled INFINITELY OFTEN (it is, every state while the      *)
(*    member is still live) to force it to fire, so the survivor stays live.   *)
(*                                                                         *)
(*  - WF on each (me,h) ReconcileClaim: a continuously-enabled claim (me live  *)
(*    AND HRW-targets h AND the slot grantable) eventually fires (the          *)
(*    slotReconcileLoop ticking).                                              *)
(*                                                                         *)
(* No fairness on Join/Crash (churn is adversarial). After StopChurn the only  *)
(* enabled membership actions are the survivors' (fair) Heartbeats and the     *)
(* (fair) reconcile claims, driving the single-owner fixed point.              *)
(***************************************************************************)
Fairness ==
    /\ \A r \in Replicas : SF_vars(Heartbeat(r))
    /\ \A me \in Replicas, h \in Slots : WF_vars(ReconcileClaim(me, h))
    /\ WF_vars(Tick)
    \* WF on StopChurn: churn EVENTUALLY stops (while a survivor exists), so the
    \* convergence leads-to is checked on a real post-churn suffix, not satisfied
    \* only vacuously by perpetual churn. (Once churnLeft hits 0, Join/Crash are
    \* disabled, so StopChurn is continuously enabled and WF forces it.)
    /\ WF_vars(StopChurn)

Spec == Init /\ [][Next]_vars /\ Fairness

\* No-fairness spec: used by the negative test -- WITHOUT fairness the
\* convergence leads-to MUST FAIL (a run where the reconcile loop never fires).
SpecNoFair == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS (hold in every reachable state, no fairness needed).    *)
(***************************************************************************)

\* A slot register holds at most one owner -- trivially true of a single
\* owner field, stated as the observable: never two distinct current owners.
\* (The real content is the epoch algebra below.)
AtMostOneOwner ==
    \A h \in Slots :
        \A r1, r2 \in Replicas :
            (r1 # r2 /\ slotOwner[h] = r1 /\ slotOwner[h] = r2) => FALSE

\* No EFFECTIVE split-brain: at most one replica BOTH owns slot h AND holds an
\* unexpired lease on it. (The owner field is single, so this is about the lease
\* being live for at most one owner at a time -- which the single owner field
\* guarantees. The genuine split-brain risk is two replicas each BELIEVING they
\* own h; that is ruled out because claim_shard is a CAS on one register and a
\* transfer bumps the epoch, fencing the prior owner -- INV-OWNER-01.)
NoLiveSplitBrain ==
    \A h \in Slots :
        Cardinality({ r \in Replicas :
                        slotOwner[h] = r /\ SlotLeaseLive(h) }) <= 1

\* Epoch is monotone across every step (HINCRBY only ever bumps; renew keeps;
\* nothing lowers). The two-state property.
EpochMonotone ==
    \A h \in Slots : slotEpoch'[h] >= slotEpoch[h]

\* A transfer (owner changes from one real replica to a DIFFERENT real replica)
\* strictly bumps the epoch -- the CAS-is-authority property (INV-HRW-01: a slot
\* is OWNED only via a granted claim_shard, and a transfer mints a higher epoch
\* that fences the prior owner). A reused epoch on transfer would be a silent LWW.
TransferBumpsEpoch ==
    \A h \in Slots :
        (   slotOwner[h] # NoOwner
         /\ slotOwner'[h] # NoOwner
         /\ slotOwner'[h] # slotOwner[h] )
        => slotEpoch'[h] > slotEpoch[h]

Inv ==
    /\ TypeOK
    /\ AtMostOneOwner
    /\ NoLiveSplitBrain

EpochMonotoneProp == [][EpochMonotone]_vars
TransferBumpsEpochProp == [][TransferBumpsEpoch]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* CONVERGENCE -- the L4 liveness property (INV-JEP-L4-01).                  *)
(*                                                                         *)
(* The convergence target: every slot is owned by its HRW target with an     *)
(* unexpired lease, AND that target is the single live owner.  "Converged"   *)
(* is the predicate; the property is that AFTER churn stops, the system      *)
(* eventually reaches AND STAYS converged (no oscillation).                  *)
(***************************************************************************)

\* A slot is settled iff its current owner is exactly the HRW target among the
\* live members AND the slot lease is live (so the workers actually run for it)
\* AND the owner is an ALIVE process (no dead owner still holding a live-scored
\* slot lease -- that transient is exactly the gap convergence must close).
SlotSettled(h) ==
    /\ HRWTargetExists(h)
    /\ slotOwner[h] = HRWOwner(h)
    /\ SlotLeaseLive(h)
    /\ alive[slotOwner[h]]

Converged == \A h \in Slots : SlotSettled(h)

\* INV-JEP-L4-01: after churn stops, the system eventually converges and stays
\* converged -- exactly one unexpired owner per slot, the HRW target, no
\* oscillation. <>[]  (eventually-always) is the no-oscillation form.
EventualConvergence == churnStopped ~> []Converged

\* A weaker, also-checked form: convergence is eventually REACHED (good for
\* localizing a failure -- if this passes but EventualConvergence fails, the
\* system reaches the fixed point but does not stay there = oscillation).
EventuallyConverges == churnStopped ~> Converged

----------------------------------------------------------------------------
(***************************************************************************)
(* REACHABILITY WITNESSES (run the negation as an INVARIANT to PROVE the    *)
(* state is reached -- the run is non-vacuous).                             *)
(***************************************************************************)
\* A genuine TRANSFER happened (some slot reached epoch >= 2: claimed once,
\* then taken over by a survivor after the first owner crashed/lapsed).
TransferReachable == \E h \in Slots : slotEpoch[h] >= 2
NotTransferReachable == ~TransferReachable

\* A zero-owner coverage gap is TRANSIENTLY reachable: a slot that WAS claimed
\* (has a real owner record, epoch >= 1) but whose owner has CRASHED and whose
\* slot lease has LAPSED -- an orphaned slot with no live owner. This is the
\* genuine L2/L4 coverage gap (NOT the never-claimed init state), and the
\* convergence property is non-trivial precisely because this gap really occurs
\* and is then closed by a survivor's claim_shard.
ZeroOwnerGapReachable ==
    \E h \in Slots :
        /\ slotOwner[h] # NoOwner
        /\ slotEpoch[h] >= 1
        /\ ~SlotLeaseLive(h)
        /\ ~alive[slotOwner[h]]
NotZeroOwnerGapReachable == ~ZeroOwnerGapReachable

----------------------------------------------------------------------------
StateConstraint ==
    \* The only unbounded variable is slotEpoch (HINCRBY on every transfer); the
    \* relative-TTL counters are bounded by their constructors (0..MemberTTL /
    \* 0..SlotTTL), so no clock ceiling is needed.
    /\ \A h \in Slots : slotEpoch[h] <= MaxEpoch

=============================================================================
