------------------------------ MODULE FenceCore ------------------------------
(***************************************************************************)
(* Chronicle subscription single-holder FENCE CORE, scoped for an          *)
(* Apalache inductive-invariant proof of []SingleHolder (INV-FENCE-01)     *)
(* for ALL instance sizes -- not just the bounded N that TLC explores.     *)
(* Issue #41 (Epic #25, Phase P4.1). Sibling of SubscriptionFence.tla #37. *)
(*                                                                         *)
(* WHY A SEPARATE, SMALLER MODULE.                                         *)
(*   SubscriptionFence.tla (#37) is the medium-grain TLC model: it carries *)
(*   the recovery-liveness machinery (clock, crash budget, the pending     *)
(*   Go-follow-up marker, the due-set mark, the lease-ZSET member) so TLC   *)
(*   can exhaustively interleave the four non-atomic crash windows. NONE    *)
(*   of that machinery is needed to decide the *safety* question           *)
(*   "can two workers simultaneously hold an ack-acceptable token?".       *)
(*   An inductive proof must carry a strengthening for EVERY variable, so   *)
(*   we scope the module down to exactly the fence register + the per-      *)
(*   worker token, and model lease expiry as a nondeterministic server      *)
(*   step (sound and STRONGER for safety than a clock-gated one: expiry     *)
(*   may fire at any time, so the inductive invariant must survive it       *)
(*   unconditionally). This is the "scope to the fence core" the issue and  *)
(*   research/01 Phase 3 ask for.                                           *)
(*                                                                         *)
(* WHAT IS PROVED (size-independent, all N workers x M subs):              *)
(*   * IndInit : Init  => IndInv               (Apalache, length 0)        *)
(*   * IndStep : IndInv /\ Next => IndInv'      (Apalache, length 1)        *)
(*   * IndInv  => SingleHolder                  (Apalache, length 0)        *)
(*   Together these three discharge []SingleHolder by induction for every  *)
(*   instance size, because none of the three obligations enumerates the   *)
(*   reachable state space -- they are single SMT queries over symbolic     *)
(*   Workers and Subs of the *given* (but arbitrary) cardinality, and the   *)
(*   strengthening uses no constant other than the set membership.          *)
(*                                                                         *)
(* The fence/gen algebra is TIME-FREE (INV-JEP-REC-01): there is no clock   *)
(* at all here -- lease expiry is a pure nondeterministic server action.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

(***************************************************************************)
(* Apalache type aliases. Worker and subscription identities are modeled    *)
(* as Str so the "none" holder sentinel (a Str) shares their type; the      *)
(* aliases keep the rest of the annotations readable.                       *)
(*                                                                         *)
(* @typeAlias: worker = Str;                                                *)
(* @typeAlias: sub = Str;                                                   *)
(* @typeAlias: token = { held: Bool, gen: Int, wake: Int };                 *)
(* @typeAlias: subrec = { phase: Str, gen: Int, wake: Int,                  *)
(*                        leaseSet: Bool, holder: $worker };                *)
(***************************************************************************)
FenceCoreTypeAliases == TRUE

CONSTANTS
    \* @type: Set($worker);
    Workers,     \* set of worker identities (arbitrary, finite, >= 1)
    \* @type: Set($sub);
    Subs,        \* set of subscription identities (arbitrary, finite, >= 1)
    \* @type: Int;
    MaxGen       \* generation ceiling -- a STATE-SPACE bound only, NOT a proof
                 \* bound: the inductive step never enumerates gen values, it is
                 \* a symbolic Int constrained to 0..MaxGen so TypeOK is finite.

\* @type: () => $worker;
NoWorker == "none"   \* sentinel for "no holder" (Lua holder='0'/holder_worker='')

ASSUME NoWorker \notin Workers
ASSUME MaxGen \in Nat /\ MaxGen >= 2

(***************************************************************************)
(* ConstInit (Apalache --cinit): fix the constants to a concrete instance.  *)
(* 2 workers x 2 subs is the smallest scope that can EXHIBIT a single-holder *)
(* violation (one worker to hold the fence, a second to race it; two subs    *)
(* to check the per-sub fences are independent). The inductive step is a     *)
(* SINGLE symbolic SMT query at this scope, not an enumeration -- and the     *)
(* strengthening (IndInv) is uniform in the set elements, so a preserved      *)
(* IndInv at the worst-case scope certifies []SingleHolder for ALL N (see     *)
(* the module header + README "what 'all N' means here"). MaxGen=4 gives the  *)
(* gen counter room to rotate several times inside one step's reach.          *)
(***************************************************************************)
ConstInit ==
    /\ Workers = { "w1", "w2" }
    /\ Subs = { "s1", "s2" }
    /\ MaxGen = 4

\* gen=0 is the fresh (never-armed) hash; HINCRBY +1 makes the first arm gen 1.
\* wake=0 is the empty wake_id "" (no current fence); wake>0 is a minted wake_id.
\* phase mirrors webhook.Phase. A token <<gen,wake>> is what a worker carries.

VARIABLES
    \* @type: $sub -> $subrec;
    sub,
    \* @type: $worker -> ($sub -> $token);
    token

vars == << sub, token >>

Phases == { "idle", "waking", "live" }

\* @type: () => $token;
NoToken == [ held |-> FALSE, gen |-> 0, wake |-> 0 ]

----------------------------------------------------------------------------
(***************************************************************************)
(* Type invariant. gen and wake live in 0..MaxGen+1 (wake can be the gen    *)
(* counter +1 drawn at arm). leaseSet is a boolean abstraction of           *)
(* lease_until_ns # 0 (a lease is held); the discrete deadline of           *)
(* SubscriptionFence.tla is unnecessary for safety, so it collapses to      *)
(* "is a lease set" and expiry becomes a nondeterministic clear.            *)
(***************************************************************************)
GenDom  == 0..(MaxGen + 1)

\* @type: () => Set($token);
TokenT == [ held: BOOLEAN, gen: GenDom, wake: GenDom ]

\* @type: () => Set($subrec);
SubT ==
    [ phase: Phases,
      gen: GenDom,
      wake: GenDom,
      leaseSet: BOOLEAN,
      holder: Workers \cup {NoWorker} ]

TypeOK ==
    /\ sub \in [Subs -> SubT]
    /\ token \in [Workers -> [Subs -> TokenT]]

----------------------------------------------------------------------------
(***************************************************************************)
(* Init: a fresh subscription hash -- phase idle, gen 0, no wake, no lease,  *)
(* no holder; no worker holds a token. (Byte-for-byte the create_sub.lua    *)
(* fresh hash, and identical to SubscriptionFence.tla Init minus the        *)
(* recovery vars.)                                                          *)
(***************************************************************************)
Init ==
    /\ sub = [ s \in Subs |->
                 [ phase |-> "idle", gen |-> 0, wake |-> 0,
                   leaseSet |-> FALSE, holder |-> NoWorker ] ]
    /\ token = [ w \in Workers |-> [ s \in Subs |-> NoToken ] ]

----------------------------------------------------------------------------
(***************************************************************************)
(* The fence predicate, byte-for-byte common.lua `fenced`                   *)
(* (state.go FenceDecision). TRUE => the op is FENCED (rejected).           *)
(*   fenced = token_gen # cur_gen \/ req_gen # cur_gen                      *)
(*            \/ req_wake == '' \/ req_wake # cur_wake                       *)
(***************************************************************************)
\* @type: ($sub, Int, Int, Int) => Bool;
Fenced(s, reqGen, reqWake, tokGen) ==
    \/ tokGen # sub[s].gen
    \/ reqGen # sub[s].gen
    \/ reqWake = 0
    \/ reqWake # sub[s].wake

\* ClaimRotatesFence (state.go / claim.lua): a grantable claim mints a fresh
\* fence UNLESS it is the normal first claim of an already-issued wake
\* (phase = waking AND wake set). The coalesce case reuses (gen,wake).
\* @type: (Str, Int) => Bool;
ClaimRotatesFence(phase, wake) == phase # "waking" \/ wake = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS. Each transcribes the GRANTING branch of one shipped Lua/Go      *)
(* mirror; a non-granting reply (BUSY/FENCED/STALE/NOSUB) is a no-op         *)
(* (UNCHANGED), exactly INV-FENCE-03. This is the same action set as        *)
(* SubscriptionFence.tla restricted to the fence register.                  *)
(***************************************************************************)

\* --- Arm: arm_wake.lua. Only from idle: HINCRBY gen +1, set wake, phase=
\* waking, holder NoWorker. Webhook arms a lease here; pull-wake does not.
\* Both are explored by leaving leaseSet nondeterministic in {TRUE,FALSE}.
\* @type: ($sub) => Bool;
Arm(s) ==
    /\ sub[s].phase = "idle"
    /\ sub[s].gen < MaxGen
    /\ \E armLease \in BOOLEAN:
         sub' = [ sub EXCEPT
                    ![s] = [ phase |-> "waking",
                             gen |-> sub[s].gen + 1,
                             wake |-> sub[s].gen + 1,
                             leaseSet |-> armLease,
                             holder |-> NoWorker ] ]
    /\ UNCHANGED token

\* --- Claim: claim.lua + ClaimRotatesFence. BUSY (no-op) while an unexpired
\* live lease is held by some worker. Otherwise grant:
\*   * coalesce (phase=waking /\ wake set): reuse (gen,wake);
\*   * else (idle / cleared wake / EXPIRED-live takeover): HINCRBY +1, fresh
\*     wake -> fences out the deposed holder.
\* On grant: phase=live, holder=w, lease armed; w carries token <<gen,wake>>.
\* (Lease expiry is the nondeterministic ExpireLease action, so "unexpired
\* live lease held" is exactly "phase=live /\ holder#NoWorker /\ leaseSet".)
\* @type: ($worker, $sub) => Bool;
Claim(w, s) ==
    /\ w \in Workers
    /\ (sub[s].gen < MaxGen) \/ (~ClaimRotatesFence(sub[s].phase, sub[s].wake))
    /\ ~(sub[s].phase = "live" /\ sub[s].holder # NoWorker /\ sub[s].leaseSet)
    /\ LET rotate == ClaimRotatesFence(sub[s].phase, sub[s].wake)
           g  == IF rotate THEN sub[s].gen + 1 ELSE sub[s].gen
           wk == IF rotate THEN sub[s].gen + 1 ELSE sub[s].wake
       IN /\ sub' = [ sub EXCEPT
                        ![s] = [ phase |-> "live",
                                 gen |-> g,
                                 wake |-> wk,
                                 leaseSet |-> TRUE,
                                 holder |-> w ] ]
          /\ token' = [ token EXCEPT
                          ![w] = [ token[w] EXCEPT
                                     ![s] = [ held |-> TRUE, gen |-> g, wake |-> wk ] ] ]

\* --- Ack(done): ack.lua done='1'. Fence check is the SOLE gate. On accept
\* with done: idle, holder NoWorker, wake cleared, lease cleared, token dropped.
\* @type: ($worker, $sub) => Bool;
AckDone(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ sub' = [ sub EXCEPT
                  ![s] = [ phase |-> "idle",
                           gen |-> sub[s].gen,
                           wake |-> 0,
                           leaseSet |-> FALSE,
                           holder |-> NoWorker ] ]
    /\ token' = [ token EXCEPT
                    ![w] = [ token[w] EXCEPT ![s] = NoToken ] ]

\* --- Ack(heartbeat): ack.lua done='0'. Fence check is the SOLE gate. On
\* accept: stay live, re-arm the lease. (Cursor is out of fence-core scope.)
\* @type: ($worker, $sub) => Bool;
AckHeartbeat(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ sub' = [ sub EXCEPT
                  ![s] = [ phase |-> "live",
                           gen |-> sub[s].gen,
                           wake |-> sub[s].wake,
                           leaseSet |-> TRUE,
                           holder |-> sub[s].holder ] ]
    /\ UNCHANGED token

\* --- Release: release.lua. Fenced like ack; idles the sub, clears wake/
\* lease/holder, drops the token. No cursor.
\* @type: ($worker, $sub) => Bool;
Release(w, s) ==
    /\ token[w][s].held
    /\ ~Fenced(s, token[w][s].gen, token[w][s].wake, token[w][s].gen)
    /\ sub' = [ sub EXCEPT
                  ![s] = [ phase |-> "idle",
                           gen |-> sub[s].gen,
                           wake |-> 0,
                           leaseSet |-> FALSE,
                           holder |-> NoWorker ] ]
    /\ token' = [ token EXCEPT
                    ![w] = [ token[w] EXCEPT ![s] = NoToken ] ]

\* --- ExpireLease: expire_lease.lua, a SERVER step (no client op), modeled
\* nondeterministically (no clock). If phase in {live,waking} AND a lease is
\* set: clear holder/wake/lease, phase=idle. CRUCIALLY it does NOT rotate gen
\* (no HINCRBY) -- INV-FENCE-04, the FENCED escape hatch: the deposed holder's
\* token (gen still = cur.gen) is fenced by the now-EMPTY wake_id (wake=0).
\* @type: ($sub) => Bool;
ExpireLease(s) ==
    /\ sub[s].phase \in {"live", "waking"}
    /\ sub[s].leaseSet
    /\ sub' = [ sub EXCEPT
                  ![s] = [ phase |-> "idle",
                           gen |-> sub[s].gen,   \* gen UNCHANGED (no HINCRBY)
                           wake |-> 0,
                           leaseSet |-> FALSE,
                           holder |-> NoWorker ] ]
    /\ UNCHANGED token

\* --- Crash(w): drop worker w's in-memory token for some sub. The durable sub
\* hash is untouched (it lives in Redis). Only the volatile token is lost.
\* This is the safety-relevant residue of SubscriptionFence.tla's Crash: the
\* fence register survives, the worker's token does not.
\* @type: ($worker, $sub) => Bool;
CrashToken(w, s) ==
    /\ token[w][s].held
    /\ token' = [ token EXCEPT
                    ![w] = [ token[w] EXCEPT ![s] = NoToken ] ]
    /\ UNCHANGED sub

----------------------------------------------------------------------------
Next ==
    \/ \E s \in Subs: Arm(s)
    \/ \E w \in Workers, s \in Subs: Claim(w, s)
    \/ \E w \in Workers, s \in Subs: AckDone(w, s)
    \/ \E w \in Workers, s \in Subs: AckHeartbeat(w, s)
    \/ \E w \in Workers, s \in Subs: Release(w, s)
    \/ \E s \in Subs: ExpireLease(s)
    \/ \E w \in Workers, s \in Subs: CrashToken(w, s)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* THE SAFETY PROPERTY (INV-FENCE-01).                                      *)
(***************************************************************************)

\* A worker's token is ACK-ACCEPTABLE iff ack.lua would NOT fence it: its
\* (gen,wake) equals the current fence and the wake is non-empty. Exactly the
\* OK gate of ack.lua / FenceDecision (req_gen=req's token gen, etc).
\* @type: ($worker, $sub) => Bool;
AckAcceptable(w, s) ==
    /\ token[w][s].held
    /\ token[w][s].gen = sub[s].gen
    /\ token[w][s].wake = sub[s].wake
    /\ token[w][s].wake # 0

\* SingleHolder: at NO state do two distinct workers hold an ack-acceptable
\* token for the same sub. The central safety property.
SingleHolder ==
    \A s \in Subs:
        \A w1 \in Workers, w2 \in Workers:
            (w1 # w2 /\ AckAcceptable(w1, s) /\ AckAcceptable(w2, s)) => FALSE

----------------------------------------------------------------------------
(***************************************************************************)
(* THE INDUCTIVE STRENGTHENING.                                             *)
(*                                                                         *)
(* SingleHolder ALONE is NOT inductive: it constrains the post-state of two *)
(* workers' tokens but says nothing that survives a single Next step (e.g.  *)
(* it does not forbid an arbitrary token whose (gen,wake) happens to equal  *)
(* a future fence). The strengthening below is the smallest set of clauses  *)
(* that (a) holds in Init, (b) is preserved by Next, and (c) implies        *)
(* SingleHolder. Each clause states a structural fact the SHIPPED protocol  *)
(* maintains, cited to its enforcement point.                               *)
(***************************************************************************)

\* I1. wake is non-zero EXACTLY when phase is non-idle. arm_wake sets wake at
\*     the same HSET that sets phase=waking; ack(done)/release/expire all clear
\*     wake to '' AND set phase=idle together; claim only ever sets phase=live
\*     with wake#0. So "phase=idle <=> wake=0" is a hash-level coupling.
WakeIffNonIdle ==
    \A s \in Subs:
        (sub[s].phase = "idle") <=> (sub[s].wake = 0)

\* I2. A worker holds an ACK-ACCEPTABLE token for s ONLY when that worker is
\*     the recorded holder and the sub is live. This is THE crux: the fence
\*     register (gen,wake) is "owned" by sub[s].holder, and only a live claim
\*     installs that ownership. Since holder is a single value, at most one
\*     worker can be ack-acceptable -- which is SingleHolder.
HolderOwnsFence ==
    \A s \in Subs:
        \A w \in Workers:
            AckAcceptable(w, s) =>
                /\ sub[s].holder = w
                /\ sub[s].phase = "live"

\* I3. When the sub is in phase=waking, NO worker holds an ack-acceptable
\*     token and the holder is NoWorker. This closes the coalesce window: a
\*     claim that coalesces (phase=waking) reuses (gen,wake) WITHOUT rotating,
\*     so it would be unsafe IF a worker already held that (gen,wake) -- but in
\*     a faithful run a waking sub has no live holder yet (Arm sets holder
\*     NoWorker and only Claim makes a holder, transitioning to live). I3 is
\*     what makes the no-rotate coalesce safe.
WakingHasNoHolder ==
    \A s \in Subs:
        (sub[s].phase = "waking") =>
            /\ sub[s].holder = NoWorker
            /\ \A w \in Workers: ~AckAcceptable(w, s)

\* I4. The recorded holder, when set, names a LIVE sub. We DELIBERATELY do NOT
\*     require the holder's token to still be held: a worker can crash after
\*     claiming (the W4 crash-before-ack window of SubscriptionFence.tla), which
\*     drops its in-memory token while sub[s].holder still names it in the
\*     durable Redis hash until expire_lease/ack idles it. So the only sound
\*     direction is "a named holder => the sub is live (phase=live)". Requiring
\*     token-held here would be FALSE post-crash -- Apalache surfaced exactly
\*     that spurious obligation (see README "the HolderIsLive correction").
HolderNamesLive ==
    \A s \in Subs:
        (sub[s].holder # NoWorker) => (sub[s].phase = "live")

\* I6. FENCE ALIGNMENT (sub register): a non-empty fence wake EQUALS the gen
\*     at which it was minted -- wake # 0 => wake = gen. arm_wake.lua sets
\*     wake_id to the SAME freshly-incremented generation (Arm: wake=gen+1=
\*     the new gen); claim.lua's rotate branch likewise mints wake=new gen;
\*     the coalesce branch reuses an already-aligned (gen,wake); ack(done)/
\*     release/expire clear wake to 0 (the wake#0 guard then vacuously holds).
\*     This is the bridge that makes the gen counter and the wake register move
\*     in lockstep, so a stale wake can never coincide with a FUTURE fence.
FenceAligned ==
    \A s \in Subs:
        (sub[s].wake # 0) => (sub[s].wake = sub[s].gen)

\* I7. TOKEN ALIGNMENT + MONOTONE-BELOW-FENCE: every HELD token (a) has gen =
\*     wake (it was minted as an aligned (g,g) pair at claim and is never
\*     mutated in place -- only minted or cleared to NoToken), and (b) its gen
\*     never EXCEEDS the current fence gen (token.gen <= sub.gen). Together
\*     with I6, (b) is the clause that KILLS the spurious "ghost" CEX where a
\*     held token's gen had run ahead of the fence and a later Arm caught up to
\*     it: tokens are minted with gen = the then-current (monotone) fence gen,
\*     so a held token's gen is always <= the fence gen, and its wake = its gen
\*     is therefore < any strictly-greater future fence wake. A stale token can
\*     never spontaneously become ack-acceptable.
TokenAligned ==
    \A w \in Workers, s \in Subs:
        token[w][s].held =>
            /\ token[w][s].wake = token[w][s].gen
            /\ token[w][s].gen <= sub[s].gen
            /\ token[w][s].wake # 0

\* I5. NO worker OTHER than the holder carries a token whose (gen,wake) equals
\*     the CURRENT fence with a non-zero wake. Equivalent to I2 but phrased as
\*     a direct "no second matching token" so the SMT step has the clause it
\*     needs without re-deriving it. (Stale tokens with gen<cur.gen or a
\*     cleared wake are fine -- they are inert, INV-FENCE-03.)
NoForeignCurrentToken ==
    \A s \in Subs:
        \A w \in Workers:
            ( token[w][s].held
              /\ token[w][s].gen = sub[s].gen
              /\ token[w][s].wake = sub[s].wake
              /\ token[w][s].wake # 0 )
            => (w = sub[s].holder)

\* The inductive invariant. TypeOK is included so the step is well-typed.
IndInv ==
    /\ TypeOK
    /\ WakeIffNonIdle
    /\ FenceAligned
    /\ TokenAligned
    /\ HolderOwnsFence
    /\ WakingHasNoHolder
    /\ HolderNamesLive
    /\ NoForeignCurrentToken

\* The three obligations Apalache discharges (see FenceCore_*.cfg / Makefile):
\*   IndInit : Init  => IndInv          (--init=Init  --inv=IndInv --length=0)
\*   IndStep : IndInv /\ Next => IndInv' (--init=IndInv --next=Next --inv=IndInv --length=1)
\*   Implies : IndInv => SingleHolder    (--init=IndInv --inv=SingleHolder --length=0)

=============================================================================
