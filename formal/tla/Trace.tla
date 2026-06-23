-------------------------------- MODULE Trace --------------------------------
(***************************************************************************)
(* Trace validation of the running Chronicle subscription engine against   *)
(* SubscriptionFence (#37) — the "smart casual verification" bridge,        *)
(* issue #39, research/01 Finding 1 (S ∩ T ≠ ∅).                            *)
(*                                                                         *)
(* This is the MODEL side of the late-binding bridge to the Go/Lua code.    *)
(* A driver runs the SHIPPED Lua scripts against a live Redis through the   *)
(* webhook.TracingStore seam (build tag `subtrace`) and records a JSONL     *)
(* trace of every fence linearization point {sub, op, preState, args,       *)
(* luaStatus, postState}.  `tracegen` converts ONE subscription's trace      *)
(* into a generated TraceData module (TraceWorkers / TraceMaxGen /          *)
(* TraceLog), and this wrapper constrains TLC to follow that log: at step i *)
(* exactly the spec action matching TraceLog[i] may fire (CCF's `IsEvent`    *)
(* per-action predicates).  TLC reconstructs the untraced variables          *)
(* (the chosen offset, the discrete clock, which worker carries a token)     *)
(* non-deterministically; validation SUCCEEDS iff some spec behavior          *)
(* reproduces the whole trace (TraceAccepted: ti reaches Len(TraceLog)+1).   *)
(*                                                                         *)
(* Run in CONSTRAINED / DFS mode (research/01 §"Run BFS for small checks,   *)
(* DFS for long traces"; CCF switched BFS->DFS for orders-of-magnitude       *)
(* faster validation).  The .cfg sets the trace module via the generated    *)
(* TraceData and checks INVARIANT TraceInv (TypeOK + the safety conjuncts    *)
(* of SubscriptionFence still hold on every traced state) and the           *)
(* postcondition TraceAccepted.                                             *)
(*                                                                         *)
(* GRAIN OF ATOMICITY (research/01 Pitfall): one trace line = one single-   *)
(* slot Lua commit = one spec action, EXCEPT the non-atomic pull-wake        *)
(* arm->emit split, which surfaces as two lines (arm ARMED, then stamp OK)   *)
(* and is composed here by mapping `arm` to Arm and `stamp` to              *)
(* WakeAppend ; WakeStamp (the two model half-steps run back to back when    *)
(* the trace shows the stamp committing).                                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, TLC, TraceData

(***************************************************************************)
(* SubscriptionFence is instantiated with the trace's worker/sub/gen scope. *)
(* The trace is per-subscription (the spec models subs as independent), so  *)
(* TheSub is the single sub s1 every TraceLog entry refers to.  Workers are *)
(* the canonicalized w1..wN the trace observed.  Faithful fault toggles      *)
(* (ExpireClearsFence = ClaimReScores = TRUE) — trace validation checks the  *)
(* SHIPPED behavior, not the injected faults.                                *)
(***************************************************************************)
TheSub == "s1"

VARIABLES
    sub, token, clock, crashes, pending, dueMark, leaseMem,
    ti      \* the 1-based trace index (the line counter CCF's IsEvent advances)

\* MaxClock/MaxCrashes are generous bounds — the trace never crashes (the driver
\* exercises live races, not origin restarts), and the discrete clock only needs
\* to range far enough to make a lease expire for the takeover/expire scenarios.
MaxGen     == TraceMaxGen + 1
MaxClock   == Len(TraceLog) + 2
MaxCrashes == 0

F == INSTANCE SubscriptionFence WITH
        Workers <- TraceWorkers,
        Subs <- {TheSub},
        MaxGen <- MaxGen,
        MaxClock <- MaxClock,
        MaxCrashes <- MaxCrashes,
        ExpireClearsFence <- TRUE,
        ClaimReScores <- TRUE

specVars == <<sub, token, clock, crashes, pending, dueMark, leaseMem>>
allVars  == <<sub, token, clock, crashes, pending, dueMark, leaseMem, ti>>

----------------------------------------------------------------------------
(***************************************************************************)
(* The current trace line, and its phase/dispatch decoded for the model.    *)
(***************************************************************************)
AtEnd == ti > Len(TraceLog)
Cur   == TraceLog[ti]

\* Map a recorded phase string to the model's phase domain.
ModelPhase(p) == p   \* "idle"/"waking"/"live" already match Phases

----------------------------------------------------------------------------
(***************************************************************************)
(* IsEvent predicates.  Each binds a spec action to the current trace line, *)
(* requiring the spec's resulting sub-state to match the recorded postState *)
(* (generation, wake, phase) — so TLC only accepts a behavior that          *)
(* reproduces the observed commit — and advances the trace index.            *)
(*                                                                         *)
(* Granting actions (ARMED / CLAIMED / OK / EXPIRED / stamp-OK) drive the    *)
(* matching SubscriptionFence action and then assert the post-state.         *)
(* Non-granting outcomes (BUSY / FENCED / STALE) are OBSERVED NO-OPS: the    *)
(* spec leaves durable state UNCHANGED (INV-FENCE-03 stale-inert), so the    *)
(* trace step only advances ti and asserts nothing in `sub` changed.  This   *)
(* is the validation of stale-inertness against the real code.               *)
(***************************************************************************)

\* The model sub-state matches the recorded post-state of the current line.
PostMatches ==
    /\ sub'[TheSub].gen   = Cur.postGen
    /\ sub'[TheSub].wake  = Cur.postWake
    /\ sub'[TheSub].phase = ModelPhase(Cur.postPhase)

\* A no-op observation: durable sub-state is unchanged and equals the recorded
\* post-state (which, for a no-op, equals the pre-state).
NoOpObserved ==
    /\ UNCHANGED specVars     \* durable + volatile state unchanged (stale-inert)
    /\ sub[TheSub].gen   = Cur.postGen
    /\ sub[TheSub].wake  = Cur.postWake

\* --- arm (arm_wake.lua) ---------------------------------------------------
IsArm ==
    /\ Cur.op = "arm"
    /\ IF Cur.status = "ARMED"
         THEN /\ F!Arm(TheSub)
              /\ PostMatches
         ELSE NoOpObserved      \* BUSY / NOSUB / FENCED arm = no-op

\* --- claim (claim.lua) ----------------------------------------------------
\* A granted claim is performed by the recorded worker; coalesce vs rotate is
\* decided by the spec's ClaimRotatesFence, and PostMatches pins the result.
IsClaim ==
    /\ Cur.op = "claim"
    /\ IF Cur.status = "CLAIMED"
         THEN /\ Cur.worker \in TraceWorkers
              /\ F!Claim(Cur.worker, TheSub)
              /\ PostMatches
              /\ token'[Cur.worker][TheSub].gen  = Cur.postGen
              /\ token'[Cur.worker][TheSub].wake = Cur.postWake
         ELSE NoOpObserved      \* BUSY / NOSUB claim = no-op

\* --- ack (ack.lua) --------------------------------------------------------
\* An OK ack is performed by the worker carrying the matching token; the offset
\* is reconstructed by TLC (any offset that reproduces the recorded phase). A
\* FENCED ack is the deposed/stale-token no-op: NO worker holds an ack-acceptable
\* token at the recorded request gen, so the spec's Ack is disabled and we record
\* the observed no-op — the direct trace-validation of INV-FENCE-01/03.
IsAck ==
    /\ Cur.op = "ack"
    /\ IF Cur.status = "OK"
         THEN /\ Cur.worker \in TraceWorkers
              /\ \E off \in 0..MaxClock: F!Ack(Cur.worker, TheSub, Cur.done, off)
              /\ PostMatches
         ELSE NoOpObserved      \* FENCED / NOSUB ack = no-op (stale-inert)

\* --- release (release.lua) ------------------------------------------------
IsRelease ==
    /\ Cur.op = "release"
    /\ IF Cur.status = "OK"
         THEN /\ Cur.worker \in TraceWorkers
              /\ F!Release(Cur.worker, TheSub)
              /\ PostMatches
         ELSE NoOpObserved

\* --- expire (expire_lease.lua, a server step) -----------------------------
\* The model's ExpireLease guard needs the lease deadline reached; the clock is
\* untraced, so TLC must have advanced it (via F!Tick) far enough. We require the
\* spec action to fire and the recorded idle post-state to match. EXPIRED only.
IsExpire ==
    /\ Cur.op = "expire"
    /\ IF Cur.status = "EXPIRED"
         THEN /\ F!ExpireLease(TheSub)
              /\ PostMatches
         ELSE NoOpObserved      \* ACTIVE / NOSUB = no-op

\* --- stamp (record_wake_sent.lua) -----------------------------------------
\* The stamp half of the non-atomic arm->emit split. In the model this is the
\* WakeAppend ; WakeStamp pair; the trace shows them as one observable commit
\* (wake_event_sent_ns stamped). We compose them: the pending marker must be
\* mid-follow-up (the spec's Arm set pending="emit"), and the stamp sets
\* wake_sent. A STALE stamp is a no-op. We accept the stamp as a wake_sent flip
\* that leaves (gen,wake,phase) unchanged.
IsStamp ==
    /\ Cur.op = "stamp"
    /\ IF Cur.status = "OK"
         THEN \* stamp only flips wake_sent; gen/wake/phase are unchanged, so the
              \* recorded post-state must equal the (matching) pre-state fence.
              /\ sub[TheSub].gen   = Cur.postGen
              /\ sub[TheSub].wake  = Cur.postWake
              /\ sub[TheSub].phase = ModelPhase(Cur.postPhase)
              /\ pending[TheSub] \in {"emit", "stamp"}   \* mid arm->emit follow-up
              /\ sub' = [sub EXCEPT ![TheSub].wake_sent = 1]
              /\ pending' = [pending EXCEPT ![TheSub] = "none"]
              /\ UNCHANGED <<token, clock, crashes, dueMark, leaseMem>>
         ELSE NoOpObserved

----------------------------------------------------------------------------
(***************************************************************************)
(* The constrained next-state.  Either the discrete clock ticks (an          *)
(* untraced internal step TLC may take to make a lease expire) WITHOUT        *)
(* advancing the trace index, or the current trace line is consumed by its    *)
(* matching IsEvent and ti advances.  At end-of-trace only Tick remains.       *)
(***************************************************************************)
TickStep ==
    /\ F!Tick
    /\ UNCHANGED ti

TraceStep ==
    /\ ~AtEnd
    /\ \/ IsArm
       \/ IsClaim
       \/ IsAck
       \/ IsRelease
       \/ IsExpire
       \/ IsStamp
    /\ ti' = ti + 1

TraceNext ==
    \/ TraceStep
    \/ TickStep

TraceInit ==
    /\ F!Init
    /\ ti = 1

TraceSpec == TraceInit /\ [][TraceNext]_allVars

----------------------------------------------------------------------------
(***************************************************************************)
(* Validation verdict + safety re-check.                                     *)
(***************************************************************************)

\* TraceAccepted (CCF's postcondition): the longest behavior consumed the whole
\* trace. We check its NEGATION as an INVARIANT — TLC reports a "violation" whose
\* error trace is the ACCEPTING run (ti past the end), i.e. the trace IS a legal
\* spec behavior. This is the standard witness-by-invariant-violation idiom: a
\* reported violation of NotAccepted == SUCCESS (the trace validated). If TLC
\* reports "No error has been found", NO behavior reached the end, so the trace
\* does NOT validate — a real code/spec divergence (a HIGH finding): inspect the
\* deepest ti TLC reached to localize the first non-matching line.
TraceAccepted == AtEnd
NotAccepted   == ~AtEnd

\* While following the trace, the SubscriptionFence safety invariants must still
\* hold on every reached state — trace validation re-states the spec's safety
\* over the REAL execution (single-holder, stale-inert, gen-monotone, cursor).
TraceInv ==
    /\ F!TypeOK
    /\ F!SingleHolder
    /\ F!AtMostOneInflightWake
    /\ F!StaleInert

\* A bound so TLC's DFS is finite even though the clock is free.
TraceConstraint ==
    /\ clock <= MaxClock
    /\ ti <= Len(TraceLog) + 1

=============================================================================
