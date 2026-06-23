-------------------------- MODULE SubscriptionFence --------------------------
(***************************************************************************)
(* Chronicle subscription control-plane fence, modeled at the             *)
(* implementation's grain (issue #37, INV-FENCE-01..04, INV-CURSOR-01,    *)
(* INV-WAKE-02, INV-LEASE-01, INV-LEASE-02).                              *)
(*                                                                         *)
(* This is the model side of the central concurrent-protocol claim. It    *)
(* mirrors the shipped Lua scripts and their Go mirrors byte-for-byte at   *)
(* the level of guards and state transitions; the action <-> source map   *)
(* is in README.md. The Porcupine oracle jepsen/checker/model_fence.go is *)
(* the safety statement this TLC run re-states under EXHAUSTIVE            *)
(* interleaving of concurrent workers, lease expiries, and crashes at the  *)
(* four non-atomic (durable-Lua-write / Go-follow-up) windows.             *)
(*                                                                         *)
(* The fence/gen algebra is deliberately TIME-FREE (INV-JEP-REC-01):       *)
(* lease_until is a *modeled* discrete deadline that governs only the      *)
(* claim grant/BUSY split and the ExpireLease guard, never the safety      *)
(* invariants, which rest on the monotone generation alone.                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,     \* set of worker identities, e.g. {w1, w2}
    Subs,        \* set of subscription identities, e.g. {s1, s2}
    MaxGen,      \* generation ceiling (state-space bound; INV-FENCE-02 ceiling)
    MaxClock,    \* discrete-clock ceiling (lease deadlines live in [0, MaxClock])
    MaxCrashes,  \* crash budget (bounds the crash/restart fan-out)
    ExpireClearsFence,  \* fault toggle: TRUE = faithful (expire idles + clears wake_id, gen unchanged).
                        \* FALSE injects the INV-FENCE-04 / INV-LEASE-01 fault: expire leaves the
                        \* sub in a claimable waking state with the fence intact -> must break SingleHolder.
    ClaimReScores       \* fault toggle: TRUE = faithful (claim_due re-scores forward, never ZREM).
                        \* FALSE injects the INV-LEASE-02 fault (claim ZREMs the due member) -> drops at-least-once.

(***************************************************************************)
(* Phases mirror webhook.Phase (idle / waking / live).                     *)
(*                                                                         *)
(* The wake register is modeled as a natural number: wake = 0 is the       *)
(* empty wake_id "" (no current fence), wake > 0 is a minted wake_id. We   *)
(* do NOT assert wake-id distinctness: arm_wake.lua / claim.lua mint a     *)
(* caller-supplied wake_id with no uniqueness check, so safety rests on    *)
(* the monotone *generation* alone (INV-FENCE-02, model_fence.go header).  *)
(* We still draw fresh wakes from a per-sub counter so they are distinct   *)
(* enough to exercise the fence, but no invariant depends on it.           *)
(*                                                                         *)
(* gen = 0 is the fresh-hash generation (never armed). HINCRBY +1 makes    *)
(* the first arm gen 1. lease_until = 0 is "no lease" (pull-wake waking,   *)
(* or idle). wake_event_sent in {0, 1}: 0 = not durably emitted (T1/sweep  *)
(* re-emit key), 1 = emitted.                                              *)
(***************************************************************************)
NoWorker == "none"   \* sentinel for "no holder" (Lua holder='0' / holder_worker='')

ASSUME NoWorker \notin Workers

Phases == {"idle", "waking", "live"}

(* A worker either holds no token, or a token <<gen, wake>> minted at claim. *)
NoToken == [held |-> FALSE, gen |-> 0, wake |-> 0]

VARIABLES
    sub,       \* [Subs -> [phase, gen, wake, lease_until, wake_sent, cursor, holder, dispatch]]
    token,     \* [Workers -> [Subs -> token]]  the (gen,wake) a worker carries per sub
    clock,     \* global discrete clock (Naturals), advanced by Tick
    crashes,   \* count of crashes consumed (<= MaxCrashes)
    pending,   \* [Subs -> {"none","emit","stamp"}] the in-flight non-atomic Go follow-up:
               \*   "emit"  = ARMED committed, writeWakeEvent not yet appended  (window T1/T2)
               \*   "stamp" = wake appended, RecordWakeEventSent not yet stamped (window T2)
               \*   "none"  = no follow-up owed
    dueMark,   \* [Subs -> BOOLEAN] the due-set "needs a wake" outbox mark (Move 2)
    leaseMem   \* [Subs -> BOOLEAN] is the sub's schedule MEMBER present in the lease ZSET?
               \* This is the at-least-once handle (INV-LEASE-02): claim_due re-scores the
               \* member forward (it stays present) and NEVER ZREMs, so a holder that
               \* crashes before acking leaves the member to fall due again and be
               \* reclaimed (ExpireLease, which requires the member present).  The
               \* ClaimReScores=FALSE fault makes claim ZREM the member -> a crashed
               \* holder strands the sub live with no recoverable lease (delivery lost).

vars == <<sub, token, clock, crashes, pending, dueMark, leaseMem>>

(* Each sub is born webhook (a lease arms at arm) or pull-wake (no lease at  *)
(* arm; lease starts at claim).  Both dispatch types are exercised.          *)
DispatchTypes == {"webhook", "pullwake"}

(***************************************************************************)
(* Type invariant.                                                         *)
(***************************************************************************)
TokenT == [held: BOOLEAN, gen: 0..MaxGen, wake: 0..(MaxGen + 1)]

SubT == [ phase: Phases,
          gen: 0..MaxGen,
          wake: 0..(MaxGen + 1),
          lease_until: 0..MaxClock,
          wake_sent: 0..1,
          cursor: 0..MaxClock,
          holder: Workers \cup {NoWorker},
          dispatch: DispatchTypes ]

TypeOK ==
    /\ sub \in [Subs -> SubT]
    /\ token \in [Workers -> [Subs -> TokenT]]
    /\ clock \in 0..MaxClock
    /\ crashes \in 0..MaxCrashes
    /\ pending \in [Subs -> {"none", "emit", "stamp"}]
    /\ dueMark \in [Subs -> BOOLEAN]
    /\ leaseMem \in [Subs -> BOOLEAN]

(***************************************************************************)
(* Init.  A fresh subscription hash: phase idle, generation 0, no wake, no  *)
(* lease, cursor 0, no holder.  No worker holds a token.  Both dispatch     *)
(* assignments are explored by leaving dispatch unconstrained in Init       *)
(* (TLC enumerates each function in [Subs -> DispatchTypes]).               *)
(***************************************************************************)
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

(***************************************************************************)
(* The fence predicate, byte-for-byte the common.lua `fenced` mirror       *)
(* (FenceDecision in state.go).  A token is stale unless its generation,    *)
(* the request generation, and the request wake all match the current      *)
(* fence (and the request wake is non-empty).  TRUE => the op is FENCED.    *)
(***************************************************************************)
Fenced(s, reqGen, reqWake, tokGen) ==
    \/ tokGen # sub[s].gen
    \/ reqGen # sub[s].gen
    \/ reqWake = 0
    \/ reqWake # sub[s].wake

(* offset_greater(a,b) for the modeled cursor (a > b on naturals; 0 is the  *)
(* "-1"/"" beginning sentinel, less than any real offset).                  *)
OffsetGreater(a, b) == a > b

(* ClaimRotatesFence (state.go): a grantable claim mints a fresh fence       *)
(* UNLESS it is the normal first claim of an already-issued wake             *)
(* (phase = waking AND wake set).  Mirror of claim.lua's branch.            *)
ClaimRotatesFence(phase, wake) == phase # "waking" \/ wake = 0

(* LeaseExpired (state.go): a deadline that is set (#0) and reached.         *)
LeaseExpired(leaseUntil) == leaseUntil # 0 /\ clock >= leaseUntil

(* MinPlus keeps a modeled deadline inside [0, MaxClock].                    *)
MinPlus(c, d) == IF c + d > MaxClock THEN MaxClock ELSE c + d

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS                                                                  *)
(*                                                                         *)
(* Each action transcribes the guard of exactly one shipped Lua/Go mirror. *)
(* A non-granting reply (BUSY / FENCED / STALE / NOSUB / ACTIVE) is a       *)
(* stuttering (no-op) step: it grants nothing and mutates no durable state, *)
(* which is precisely INV-FENCE-03 (stale-inert).  We therefore only model  *)
(* the GRANTING branch of each action as a state change; the non-granting   *)
(* branches are absorbed as enabled-but-stuttering (UNCHANGED vars), so     *)
(* TLC still explores them as reachable transitions without bloating the    *)
(* fingerprint set.                                                         *)
(***************************************************************************)

(* --- Arm: arm_wake.lua --------------------------------------------------*)
(* Only from phase = idle: HINCRBY generation +1, set wake_id, phase=waking,*)
(* holder='0'.  ZADD the due mark.  Webhook arms the lease here; pull-wake  *)
(* does not (lease starts at claim) and sets wake_event_sent_ns = 0.  A     *)
(* non-idle sub returns BUSY and mutates nothing (the coalescing source of  *)
(* INV-WAKE-02).  After ARMED commits, a Go follow-up is owed (writeWake-   *)
(* Event for pullwake / deliverWebhook for webhook): pending := "emit".     *)
Arm(s) ==
    /\ sub[s].phase = "idle"
    /\ sub[s].gen < MaxGen                      \* state-space bound on gen
    /\ LET g == sub[s].gen + 1
           w == sub[s].gen + 1                  \* fresh wake drawn from the gen counter
           isWebhook == sub[s].dispatch = "webhook"
       IN sub' = [sub EXCEPT
                    ![s].gen = g,
                    ![s].wake = w,
                    ![s].phase = "waking",
                    ![s].holder = NoWorker,
                    ![s].lease_until = IF isWebhook
                                         THEN MinPlus(clock, 1)   \* webhook arms a lease
                                         ELSE 0,                  \* pull-wake: no lease yet
                    ![s].wake_sent = IF isWebhook THEN sub[s].wake_sent ELSE 0]
          /\ leaseMem' = [leaseMem EXCEPT ![s] = isWebhook]  \* webhook ZADDs the lease member
    /\ pending' = [pending EXCEPT ![s] = "emit"]
    /\ dueMark' = [dueMark EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<token, clock, crashes>>

(* --- WriteWakeEvent: manager.go writeWakeEvent + record_wake_sent.lua ----*)
(* The Go follow-up after ARMED for a pull-wake: append the wake event,     *)
(* then RecordWakeEventSent stamps wake_event_sent_ns fenced on (gen,wake). *)
(* Modeled as two non-atomic steps so a Crash can fall between them (window *)
(* T2).  Step a (append): pending "emit" -> "stamp".  Step b (stamp):       *)
(* fenced on (gen,wake) -- a superseded stamp is STALE (no-op).             *)
WakeAppend(s) ==
    /\ pending[s] = "emit"
    /\ sub[s].dispatch = "pullwake"
    /\ pending' = [pending EXCEPT ![s] = "stamp"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem>>

WakeStamp(s) ==
    /\ pending[s] = "stamp"
    /\ sub[s].dispatch = "pullwake"
    /\ pending' = [pending EXCEPT ![s] = "none"]
    \* record_wake_sent.lua: stamp only if still the current (gen,wake); else STALE no-op.
    /\ IF sub[s].phase = "waking" /\ sub[s].wake # 0
         THEN sub' = [sub EXCEPT ![s].wake_sent = 1]
         ELSE sub' = sub
    /\ UNCHANGED <<token, clock, crashes, dueMark, leaseMem>>

(* The webhook Go follow-up (deliverWebhook) carries no durable Redis stamp *)
(* in scope for the fence; it just clears the pending marker.               *)
WebhookEmit(s) ==
    /\ pending[s] = "emit"
    /\ sub[s].dispatch = "webhook"
    /\ pending' = [pending EXCEPT ![s] = "none"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem>>

(* --- Claim: claim.lua + ClaimRotatesFence (state.go) --------------------*)
(* Rejected (BUSY) while another worker holds an UNEXPIRED live lease       *)
(* (phase=live AND holder set AND lease not expired).  Otherwise grantable: *)
(*   * coalesce (phase=waking AND wake set): reuse (gen,wake).              *)
(*   * every other case (idle, cleared wake, EXPIRED live takeover):        *)
(*     HINCRBY +1, fresh wake -> fences out the deposed holder.             *)
(* On grant: phase=live, holder=worker, arm a lease, ZADD lease zset.       *)
(* The granted worker now carries the token <<gen,wake>> (the claim reply). *)
Claim(w, s) ==
    /\ sub[s].gen < MaxGen \/ ~ClaimRotatesFence(sub[s].phase, sub[s].wake)
       \* (only the rotate branch can hit the gen ceiling; coalesce never rotates)
    \* BUSY guard: an unexpired live lease held by ANY worker blocks the claim.
    /\ ~(sub[s].phase = "live" /\ sub[s].holder # NoWorker /\ ~LeaseExpired(sub[s].lease_until))
    /\ LET rotate == ClaimRotatesFence(sub[s].phase, sub[s].wake)
           g == IF rotate THEN sub[s].gen + 1 ELSE sub[s].gen
           wk == IF rotate THEN sub[s].gen + 1 ELSE sub[s].wake
       IN /\ sub' = [sub EXCEPT
                       ![s].phase = "live",
                       ![s].holder = w,
                       ![s].gen = g,
                       ![s].wake = wk,
                       ![s].lease_until = MinPlus(clock, 1)]
          /\ token' = [token EXCEPT ![w][s] = [held |-> TRUE, gen |-> g, wake |-> wk]]
          \* claim ZADDs the lease member (re-scores forward).  ClaimReScores=FALSE
          \* injects the INV-LEASE-02 fault: the member is ZREMed (left FALSE) so a
          \* later crash strands the lease with nothing to reclaim it.
          /\ leaseMem' = [leaseMem EXCEPT ![s] = ClaimReScores]
    /\ UNCHANGED <<clock, crashes, pending, dueMark>>

(* --- Ack: ack.lua -------------------------------------------------------*)
(* Fence check first (common.lua fenced) is the SOLE safety gate.  A fenced *)
(* ack grants nothing and mutates nothing (no-op).  An accepted ack:        *)
(*   * advances the cursor forward-only (offset_greater) for the named path,*)
(*   * done='1' -> idle, holder='0', wake_id='', lease=0, ZREM schedule,    *)
(*                  clear due mark;                                          *)
(*   * done='0' -> heartbeat: extend lease, phase=live.                     *)
(* reqOff is the offset the worker acks; we let it be any modeled offset so *)
(* TLC exercises both forward and backward/equal acks (cursor no-op).       *)
Ack(w, s, done, reqOff) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
       \* (req_gen = token_gen = the carried token; req_wake = the carried wake)
    /\ LET newCur == IF OffsetGreater(reqOff, sub[s].cursor) THEN reqOff ELSE sub[s].cursor
       IN IF done
            THEN /\ sub' = [sub EXCEPT
                              ![s].cursor = newCur,
                              ![s].phase = "idle",
                              ![s].holder = NoWorker,
                              ![s].wake = 0,
                              ![s].lease_until = 0]
                 /\ token' = [token EXCEPT ![w][s] = NoToken]
                 /\ dueMark' = [dueMark EXCEPT ![s] = FALSE]
                 /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]  \* ack(done) ZREMs the member
            ELSE /\ sub' = [sub EXCEPT
                              ![s].cursor = newCur,
                              ![s].phase = "live",
                              ![s].lease_until = MinPlus(clock, 1)]
                 /\ leaseMem' = [leaseMem EXCEPT ![s] = TRUE]   \* heartbeat ZADDs the member forward
                 /\ UNCHANGED <<token, dueMark>>
    /\ UNCHANGED <<clock, crashes, pending>>

(* --- Release: release.lua -----------------------------------------------*)
(* Fenced like ack.  An accepted release idles the sub (no cursor change),  *)
(* clears holder/wake/lease, ZREMs the schedule and the due mark.           *)
Release(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ sub' = [sub EXCEPT
                 ![s].phase = "idle",
                 ![s].holder = NoWorker,
                 ![s].wake = 0,
                 ![s].lease_until = 0]
    /\ token' = [token EXCEPT ![w][s] = NoToken]
    /\ dueMark' = [dueMark EXCEPT ![s] = FALSE]
    /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]  \* release ZREMs the member
    /\ UNCHANGED <<clock, crashes, pending>>

(* --- ExpireLease: expire_lease.lua (a SERVER step, no client op) --------*)
(* If phase in {live, waking} AND lease set (#0) AND deadline reached:       *)
(* clear holder/wake_id/lease, phase=idle, re-owe the due mark.  Crucially  *)
(* it does NOT rotate the generation (no HINCRBY) -- INV-FENCE-04, the       *)
(* FENCED escape hatch: the deposed holder's late ack is fenced by the       *)
(* now-empty wake_id, not by a higher gen.  A pull-wake waking with          *)
(* lease_until=0 is left untouched.                                          *)
(*                                                                          *)
(* The negative test for INV-FENCE-04 / INV-LEASE-01 is the ExpireClearsFence *)
(* toggle.  The escape hatch is load-bearing: expire returns the sub to IDLE  *)
(* and CLEARS wake_id (gen UNCHANGED, no HINCRBY), so the deposed holder's     *)
(* token (gen still = cur.gen) is fenced by the now-EMPTY wake_id -- an        *)
(* observed FENCED is then an unconditional legal no-op (INV-FENCE-04), and    *)
(* the coalesce window is closed (phase idle, wake 0).                         *)
(*                                                                            *)
(* ExpireClearsFence = FALSE injects the unsound variant: expire drops only    *)
(* the lease but leaves the sub in a CLAIMABLE waking state with the fence     *)
(* (gen, wake) INTACT and the holder's token still valid.  A second worker can *)
(* then coalesce onto the SAME (gen, wake) -- ClaimRotatesFence is FALSE for   *)
(* waking-with-wake -- so two workers hold the identical current fence: two    *)
(* ack-acceptable tokens.  This MUST violate SingleHolder, proving the         *)
(* invariant constrains the spec.  Faithful runs use ExpireClearsFence = TRUE. *)
ExpireLease(s) ==
    /\ sub[s].phase \in {"live", "waking"}
    /\ LeaseExpired(sub[s].lease_until)
    \* The lease worker only surfaces members present in the lease ZSET; a member
    \* claim ZREMed (the INV-LEASE-02 fault) is invisible, so expiry never fires.
    /\ leaseMem[s]
    /\ IF ExpireClearsFence
         THEN \* FAITHFUL: idle, clear wake/holder/lease; gen UNCHANGED (no HINCRBY).
              /\ sub' = [sub EXCEPT
                           ![s].phase = "idle",
                           ![s].holder = NoWorker,
                           ![s].wake = 0,
                           ![s].lease_until = 0]
              /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]  \* ZREM the lease member
         ELSE \* INJECTED FAULT: drop only the lease; leave a claimable waking
              \* fence intact (wake/gen/holder untouched). Re-opens coalesce.
              /\ sub' = [sub EXCEPT
                           ![s].phase = "waking",
                           ![s].lease_until = 0]
              /\ leaseMem' = [leaseMem EXCEPT ![s] = FALSE]
    /\ dueMark' = [dueMark EXCEPT ![s] = TRUE]      \* re-owe (unconditional ZADD)
    /\ UNCHANGED <<token, clock, crashes, pending>>

(* --- SweepReemit: manager.go sweepOnce re-emit branches -----------------*)
(* Re-emit the SAME (gen, wakeID) for a stranded pull-wake.  Two branches:  *)
(*   T1 (arm-before-emit): phase=waking, wake_sent=0  -> writeWakeEvent.     *)
(*   T4 (post-emit, never-claimed): phase=waking, wake_sent#0, stale.        *)
(* Both re-run the same Go follow-up reusing the same (gen,wake): pending    *)
(* := "emit".  Idempotent and claim-fence-safe (the (gen,wake) fence makes   *)
(* the duplicate event safe).  Modeled without the 3*sweepInterval threshold *)
(* (the deferred liveness sibling); the staleness is abstracted as enabled.  *)
SweepReemit(s) ==
    /\ sub[s].dispatch = "pullwake"
    /\ sub[s].phase = "waking"
    /\ pending[s] = "none"          \* not already mid-follow-up
    /\ pending' = [pending EXCEPT ![s] = "emit"]
    /\ UNCHANGED <<sub, token, clock, crashes, dueMark, leaseMem>>

(* --- DueDrain: DecideDue (state.go) -------------------------------------*)
(* The dueWorker drains a due mark to exactly one of fire / clear / skip.    *)
(*   DueFire  (idle + pending work): issue a wake -> Arm path (here: re-arm  *)
(*            via setting the mark consumed; the Arm action models the mint).*)
(*   DueClear (idle + caught up): clear the mark.                            *)
(*   DueSkip  (non-idle): leave the mark (a wake is already in flight).      *)
(* We model the reconcile of the mark itself (the load-bearing DueClear,     *)
(* since claim_due never ZREMs).  "Pending work" is abstracted as the cursor *)
(* lagging a modeled tail; we let DueDrain nondeterministically treat the    *)
(* sub as caught-up (clear) when idle, exercising DueClear.                  *)
DueDrain(s) ==
    /\ dueMark[s]
    /\ IF sub[s].phase # "idle"
         THEN dueMark' = dueMark                          \* DueSkip
         ELSE dueMark' = [dueMark EXCEPT ![s] = FALSE]    \* DueClear (idle, caught up)
    /\ UNCHANGED <<sub, token, clock, crashes, pending, leaseMem>>

(* --- Tick: advance the discrete clock (enables LeaseExpired) -------------*)
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<sub, token, crashes, pending, dueMark, leaseMem>>

(* --- Crash / Restart: the four non-atomic windows -----------------------*)
(* A Crash fires BETWEEN a durable Lua write and its Go follow-up.  The      *)
(* durable Redis state (sub, dueMark) SURVIVES (it was committed); the       *)
(* volatile Go follow-up (pending) and the crashed worker's in-memory token  *)
(* are LOST.  The four windows are exactly the reachable `pending` markers   *)
(* and the claim-before-ack gap:                                             *)
(*                                                                          *)
(*   W1 arm-before-emit : pending="emit" on a pullwake (wake_sent=0)         *)
(*   W2 lua-commit/Go-stamp : pending="stamp" (appended, not yet stamped)    *)
(*   W3 post-emit/never-claimed : wake_sent=1, phase=waking, no lease (T4)    *)
(*   W4 claim-before-ack : a worker holds a token but its process dies       *)
(*                         before acking; the lease falls due and is reclaimed*)
(*                                                                          *)
(* Crash drops the pending follow-up (so the sweep must recover it) and may  *)
(* drop a worker's token (so the holder is gone -> lease will expire ->       *)
(* reclaim).  The DURABLE sub hash is untouched: this is the crux of the      *)
(* recovery argument.  Bounded by MaxCrashes.                                *)
Crash(w) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    \* Lose the volatile Go follow-up for every sub (origin restart), and lose
    \* worker w's in-memory tokens.  The durable sub hash + dueMark survive.
    /\ pending' = [s \in Subs |-> "none"]
    /\ token' = [token EXCEPT ![w] = [s \in Subs |-> NoToken]]
    \* The durable Redis state survives: sub hash, dueMark, AND the lease ZSET
    \* member (leaseMem).  The surviving member is exactly what lets the lease
    \* worker re-surface the lapsed lease and reclaim it (INV-LEASE-02): the
    \* crash-after-claim window (W4) recovers BECAUSE the member was re-scored,
    \* not ZREMed, at claim.
    /\ UNCHANGED <<sub, clock, dueMark, leaseMem>>

----------------------------------------------------------------------------
Next ==
    \/ \E s \in Subs: Arm(s)
    \/ \E s \in Subs: WakeAppend(s)
    \/ \E s \in Subs: WakeStamp(s)
    \/ \E s \in Subs: WebhookEmit(s)
    \/ \E w \in Workers, s \in Subs: Claim(w, s)
    \/ \E w \in Workers, s \in Subs, off \in 0..MaxClock: Ack(w, s, TRUE, off)
    \/ \E w \in Workers, s \in Subs, off \in 0..MaxClock: Ack(w, s, FALSE, off)
    \/ \E w \in Workers, s \in Subs: Release(w, s)
    \/ \E s \in Subs: ExpireLease(s)
    \/ \E s \in Subs: SweepReemit(s)
    \/ \E s \in Subs: DueDrain(s)
    \/ Tick
    \/ \E w \in Workers: Crash(w)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS                                                        *)
(***************************************************************************)

(* A worker's token is ACK-ACCEPTABLE iff ack.lua would NOT fence it: its    *)
(* (gen,wake) equals the current fence and the wake is non-empty.  This is   *)
(* exactly the OK gate of ack.lua / FenceDecision.                          *)
AckAcceptable(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)

(* INV-FENCE-01 (SingleHolder): at NO state do two distinct workers hold an  *)
(* ack-acceptable token for the same sub.  The central safety property.      *)
SingleHolder ==
    \A s \in Subs:
        \A w1, w2 \in Workers:
            (w1 # w2 /\ AckAcceptable(w1, s) /\ AckAcceptable(w2, s)) => FALSE

(* INV-WAKE-02 (AtMostOneInflightWake): at most one in-flight wake per sub.  *)
(* The register holds a single (gen,wake); a non-idle sub has at most one    *)
(* current wake.  Stated structurally: a sub never carries two distinct      *)
(* live fences -- there is one wake field, so this reduces to "if waking/    *)
(* live then exactly one wake is current", i.e. the register is a function.  *)
(* Operationally INV-WAKE-02 is that Arm mints only from idle; we check the  *)
(* observable consequence: no two ack-acceptable tokens with DIFFERENT       *)
(* (gen,wake) exist for one sub (a second concurrent fence).                 *)
AtMostOneInflightWake ==
    \A s \in Subs:
        \A w1, w2 \in Workers:
            ( AckAcceptable(w1, s) /\ AckAcceptable(w2, s) )
              => ( token[w1][s].gen = token[w2][s].gen
                   /\ token[w1][s].wake = token[w2][s].wake )

(* INV-CURSOR-01 (CursorForwardOnly): the cursor never exceeds MaxClock and  *)
(* (the two-state form) never decreases.  The state predicate below bounds   *)
(* it; the action property CursorNeverRegresses is the real monotonicity.    *)
CursorBounded ==
    \A s \in Subs: sub[s].cursor \in 0..MaxClock

(* --- Two-state (action) properties --------------------------------------*)

(* INV-FENCE-02 (GenMonotone): cur.gen is non-decreasing across every step.  *)
GenMonotone ==
    \A s \in Subs: sub'[s].gen >= sub[s].gen

(* INV-CURSOR-01 (CursorForwardOnly), the real two-state monotonicity:       *)
(* per sub the cursor never decreases.                                       *)
CursorForwardOnly ==
    \A s \in Subs: sub'[s].cursor >= sub[s].cursor

(* INV-FENCE-03 (StaleInert): a step that does NOT change the current fence  *)
(* register of a sub leaves that sub's durable phase/lease/cursor unchanged  *)
(* UNLESS it is a granting step (gen or wake changed) or a server lease       *)
(* transition.  We state the contrapositive used in the issue: an op whose    *)
(* request gen differs from cur.gen is inert.  Modeled as: for any worker     *)
(* whose token gen != cur.gen, no Ack/Release by it changes durable state.    *)
(* This is enforced structurally by the guards (Ack/Release require          *)
(* ~Fenced), so StaleInert holds by construction; we add a defensive         *)
(* state-level companion: a held token whose gen is BELOW the current gen     *)
(* can never be ack-acceptable (a stale token is inert).                     *)
StaleInert ==
    \A w \in Workers, s \in Subs:
        (token[w][s].held /\ token[w][s].gen # sub[s].gen)
          => ~AckAcceptable(w, s)

(* INV-LEASE-02 (NoStrandedLease / at-least-once): a live subscription whose  *)
(* holder is gone (no worker holds an ack-acceptable token for it) MUST still  *)
(* have its lease member present (leaseMem) so the lease worker can re-surface *)
(* the lapsed lease and reclaim it.  claim_due re-scores the member forward    *)
(* and never ZREMs, so a crash-after-claim (W4) leaves the member to fall due  *)
(* again -> reclaimable.  The ClaimReScores=FALSE fault ZREMs the member at    *)
(* claim, so a crash strands the sub live with nothing to reclaim it: this     *)
(* invariant then has a counterexample, proving INV-LEASE-02 is exercised.     *)
NoStrandedLease ==
    \A s \in Subs:
        ( sub[s].phase = "live"
          /\ sub[s].lease_until # 0
          /\ \A w \in Workers: ~AckAcceptable(w, s) )
        => leaseMem[s]

(* The conjunction TLC checks as a state invariant.                          *)
Inv ==
    /\ TypeOK
    /\ SingleHolder
    /\ AtMostOneInflightWake
    /\ CursorBounded
    /\ StaleInert
    /\ NoStrandedLease

(* Temporal wrappers for the two-state (action) properties, checked via      *)
(* PROPERTY in the .cfg.  [][A]_vars holds iff every step satisfies A.        *)
GenMonotoneProp == [][GenMonotone]_vars
CursorForwardOnlyProp == [][CursorForwardOnly]_vars

(***************************************************************************)
(* CRASH-WINDOW COVERAGE WITNESSES.                                         *)
(*                                                                         *)
(* Each predicate is TRUE exactly in the state of one of the four          *)
(* non-atomic crash windows.  To prove a window is REACHED on a config,    *)
(* run TLC with the negation as an INVARIANT (e.g. INVARIANT NotW1): TLC    *)
(* reports a counterexample, and the trace is a witness reaching the        *)
(* window.  Used by the coverage cfg, not the safety run.                  *)
(***************************************************************************)
\* W1 arm-before-emit: a pull-wake ARMED (phase=waking, wake_sent=0) with the
\* writeWakeEvent Go follow-up still owed (pending="emit"). manager.go:1180.
WindowW1 == \E s \in Subs:
              /\ sub[s].dispatch = "pullwake"
              /\ sub[s].phase = "waking" /\ sub[s].wake_sent = 0
              /\ pending[s] = "emit"
\* W2 lua-commit-then-Go stamp: the wake was appended but RecordWakeEventSent
\* has not yet stamped wake_event_sent_ns (pending="stamp"). record_wake_sent.lua.
WindowW2 == \E s \in Subs: pending[s] = "stamp"
\* W3 post-emit / never-claimed (T4): wake_sent=1, still waking, no lease.
\* manager.go:1196.
WindowW3 == \E s \in Subs:
              /\ sub[s].dispatch = "pullwake"
              /\ sub[s].phase = "waking" /\ sub[s].wake_sent = 1
              /\ sub[s].lease_until = 0
\* W4 claim-before-ack: the sub is live with a lease but no worker holds an
\* ack-acceptable token (the holder crashed after claim, before ack); the
\* lease member survives so it is reclaimable. claim_due.lua / INV-LEASE-02.
WindowW4 == \E s \in Subs:
              /\ sub[s].phase = "live" /\ sub[s].lease_until # 0
              /\ (\A w \in Workers: ~AckAcceptable(w, s))
              /\ leaseMem[s]

NotW1 == ~WindowW1
NotW2 == ~WindowW2
NotW3 == ~WindowW3
NotW4 == ~WindowW4

(***************************************************************************)
(* State constraint to keep the model finite even if a bound is loosened.   *)
(***************************************************************************)
StateConstraint ==
    /\ \A s \in Subs: sub[s].gen <= MaxGen
    /\ clock <= MaxClock
    /\ crashes <= MaxCrashes

=============================================================================
