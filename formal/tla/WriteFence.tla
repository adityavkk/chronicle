------------------------------- MODULE WriteFence -------------------------------
(***************************************************************************)
(* Chronicle write-fencing extension: the stream-slot APPEND CAPABILITY,   *)
(* modeled at the implementation's grain (issue #183, INV-FENCE-05/06/07). *)
(*                                                                         *)
(* SubscriptionFence.tla (#37) proves the CONTROL-plane claim is single-    *)
(* holder. This is its DATA-plane sibling: on one write-fenced stream, can  *)
(* the accepted fenced appends of two writer epochs ever INTERLEAVE? The    *)
(* control plane is abstracted to exactly what the stream slot observes     *)
(* (a minimal (gen, wake, holder, phase) copy of the subscription fence);   *)
(* the stream slot carries the claim MARKER, the per-authority SEAL, the    *)
(* producer state, and the log of accepted fenced appends. Each action      *)
(* transcribes the guard of one shipped script or Go orchestration step     *)
(* (README.md action <-> source map); a non-granting reply (FENCED / STALE  *)
(* / DUP / a producer rejection) is a stuttering no-op, exactly as in the   *)
(* sibling module.                                                          *)
(*                                                                         *)
(* The fence algebra is TIME-FREE (INV-JEP-REC-01): `lease_until_ns > now`  *)
(* collapses to booleans flipped by nondeterministic lapses -- one per      *)
(* (authority, worker) for the subscription lease a claim carries, one on   *)
(* the marker for its own deadline -- so the invariants rest on the         *)
(* generation, the seal, and the marker alone, never on a clock. Two facts  *)
(* of the real clock are kept, both as guards: a marker's deadline never    *)
(* exceeds its holder's subscription deadline (the heartbeat extends the    *)
(* sub lease in ack.lua BEFORE the re-grant extends the marker, so the      *)
(* marker lapses first or together), and on one authority a newer claim's   *)
(* deadline is later than every deadline an older claim ever had (the same  *)
(* lease TTL, set later), so an older claim never outlives a newer one.     *)
(*                                                                         *)
(* Producer ids: each claimant writes under its OWN producer id. A shared   *)
(* id would add the base producer state machine's per-id epoch order on    *)
(* top of the fence and could mask a fence hole; per-claimant ids are the   *)
(* weaker, cover-all assumption (the fence must order the generations by    *)
(* itself). The producer state machine still runs per id, unchanged.        *)
(*                                                                         *)
(* Crash is the ORIGIN's restart (K.1/K.2/K.5 are all origin crashes): it   *)
(* loses every in-flight Go orchestration and keeps the workers' bearer     *)
(* tokens, so the K.1 redelivery is representable. A worker's own crash is  *)
(* dominated by its stuttering and is not a separate action.                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, TLC

CONSTANTS
    Workers,        \* claimant identities, e.g. {w1, w2}
    Auths,          \* write-fence authorities (subscription incarnations linked to
                    \* the stream), e.g. {a1}; the 2x2 lane uses two on one stream
    MaxGen,         \* generation ceiling per authority (state-space bound)
    MaxSeq,         \* producer sequence ceiling per epoch (state-space bound)
    MaxCrashes,     \* crash budget (bounds the crash fan-out)
    CheckInSlot,    \* fault toggle: TRUE = faithful (the fence check and the write are ONE
                    \* script, append.lua step 4). FALSE injects the pre-#169 TOCTOU: the
                    \* check and the write become two steps with Lapse/Claim interleavable.
    BindProducers,  \* fault toggle: TRUE = faithful (rule 5: an open-class write naming a
                    \* bound producer id is refused). FALSE injects the "omit the credential /
                    \* autoClaim" hole: an open write can advance a bound producer's epoch.
    SealBeforeIdle, \* fault toggle: TRUE = faithful (done seals in the stream slot BEFORE
                    \* ack.lua idles the claim). FALSE injects idle-first / seal-second.
    SealOnRevoke,   \* fault toggle: TRUE = faithful (done records the per-authority seal).
                    \* FALSE injects "done only tombstones" (today's revoke_append_fence.lua):
                    \* a delayed grant can revive a reaped generation.
    SealKeyedByAuth \* fault toggle: TRUE = faithful (the seal is the meta field wfseal:<auth>,
                    \* one per authority). FALSE injects the rejected generation-only seal
                    \* (design Q5): one stream-global seal that a second authority inherits.

(***************************************************************************)
(* Sentinels and domains. gen = 0 is the fresh (never-claimed) authority;   *)
(* HINCRBY +1 makes the first claim gen 1. wake = 0 is the empty wake_id    *)
(* "" (no current fence); a fresh wake is drawn from the gen counter as in  *)
(* SubscriptionFence (no distinctness is asserted or relied on). A seal of  *)
(* 0 is "no wfseal:<auth> field". A producer's epoch is the claim           *)
(* generation it wrote under (rule 4 binds them), seq the last accepted     *)
(* sequence in that epoch.                                                  *)
(***************************************************************************)
NoWorker == "none"   \* Lua holder='0' / holder_worker='' / an absent marker's holder

ASSUME NoWorker \notin Workers
ASSUME MaxGen \in Nat /\ MaxGen >= 1
ASSUME MaxSeq \in Nat

Phases == {"idle", "live"}
MarkerStates == {"live", "revoked"}
\* The Go-orchestrated done sequence, per authority:
\*   "sealing"   = seal_append_fence.lua committed (sealed), ack.lua done owed
\*   "already"   = the done was redelivered; the re-seal was idempotent (already)
\*   "lost"      = the origin crashed with the seal committed and ack.lua owed (K.1)
\*   "recovered" = the redelivery after that crash found the seal (already)
\*   "idled"     = (SealBeforeIdle=FALSE only) ack.lua ran, the seal is still owed
PendingStates == {"none", "sealing", "already", "lost", "recovered", "idled"}
SealedOwed    == {"sealing", "already", "lost", "recovered"}  \* seal durable, ack.lua owed
IdleReady     == SealedOwed \ {"lost"}                       \* a done sequence that can idle

GenDom == 0..MaxGen
SeqDom == 0..MaxSeq

NoToken    == [held |-> FALSE, gen |-> 0, wake |-> 0]
NoMarker   == [present |-> FALSE, state |-> "revoked", gen |-> 0, wake |-> 0,
               holder |-> NoWorker, live |-> FALSE]
NoProd     == [present |-> FALSE, epoch |-> 0, lastSeq |-> 0, bound |-> FALSE]
NoInflight == [active |-> FALSE, epoch |-> 0, seq |-> 0]

VARIABLES
    ctl,       \* [Auths -> [phase, gen, wake, holder]] the subscription fence register as the
               \* stream slot sees it: Claim rotates it, Idle/ExpireLease clear it (control slot)
    marker,    \* [Auths -> [present, state, gen, wake, holder, live]] the stream-slot claim
               \* marker ds:{path}:append-fence:<auth> (state/generation/wake_id/holder) and,
               \* time-free, its own `lease_until_ns > now` (live)
    sealGen,   \* [Auths -> GenDom] the authority's wfseal:<auth> generation (0 = none)
    leaseLive, \* [Auths -> [Workers -> BOOLEAN]] time-free `lease_until_ns > now` for the
               \* subscription deadline carried by the worker's claim (its token's claim,
               \* the delayed grant's and the delayed seal's snapshot)
    token,     \* [Auths -> [Workers -> [held, gen, wake]]] the write token a worker carries;
               \* a bearer credential whose bytes outlive the claim (never dropped)
    prod,      \* [Workers -> [present, epoch, lastSeq, bound]] ds:{path}:prod per producer id
               \* (one per claimant, see the header) + the wfbind:<producer_id> meta field
    log,       \* Seq of [auth, gen, holder, seq]: the accepted FENCED-class appends, in
               \* stream order (the frames whose fence block admitted them)
    pending,   \* [Auths -> PendingStates] the in-flight Go-orchestrated done sequence
    inflight,  \* [Auths -> [Workers -> [active, epoch, seq]]] (CheckInSlot=FALSE only) an
               \* append whose fence check passed out of slot and whose write is still owed
    crashes    \* count of crashes consumed (<= MaxCrashes)

vars == <<ctl, marker, sealGen, leaseLive, token, prod, log, pending, inflight, crashes>>

(***************************************************************************)
(* Type invariant.                                                         *)
(***************************************************************************)
CtlT      == [phase: Phases, gen: GenDom, wake: GenDom, holder: Workers \cup {NoWorker}]
MarkerT   == [present: BOOLEAN, state: MarkerStates, gen: GenDom, wake: GenDom,
              holder: Workers \cup {NoWorker}, live: BOOLEAN]
TokenT    == [held: BOOLEAN, gen: GenDom, wake: GenDom]
ProdT     == [present: BOOLEAN, epoch: GenDom, lastSeq: SeqDom, bound: BOOLEAN]
EntryT    == [auth: Auths, gen: GenDom, holder: Workers, seq: SeqDom]
InflightT == [active: BOOLEAN, epoch: GenDom, seq: SeqDom]

TypeOK ==
    /\ ctl \in [Auths -> CtlT]
    /\ marker \in [Auths -> MarkerT]
    /\ sealGen \in [Auths -> GenDom]
    /\ leaseLive \in [Auths -> [Workers -> BOOLEAN]]
    /\ token \in [Auths -> [Workers -> TokenT]]
    /\ prod \in [Workers -> ProdT]
    /\ log \in Seq(EntryT)
    /\ pending \in [Auths -> PendingStates]
    /\ inflight \in [Auths -> [Workers -> InflightT]]
    /\ crashes \in 0..MaxCrashes

(***************************************************************************)
(* Init. A fresh authority: idle at gen 0, no marker, no seal, no lease, no  *)
(* token. Fresh producer ids, an empty stream, no follow-up owed.            *)
(***************************************************************************)
Init ==
    /\ ctl = [a \in Auths |-> [phase |-> "idle", gen |-> 0, wake |-> 0, holder |-> NoWorker]]
    /\ marker = [a \in Auths |-> NoMarker]
    /\ sealGen = [a \in Auths |-> 0]
    /\ leaseLive = [a \in Auths |-> [w \in Workers |-> FALSE]]
    /\ token = [a \in Auths |-> [w \in Workers |-> NoToken]]
    /\ prod = [w \in Workers |-> NoProd]
    /\ log = << >>
    /\ pending = [a \in Auths |-> "none"]
    /\ inflight = [a \in Auths |-> [w \in Workers |-> NoInflight]]
    /\ crashes = 0

----------------------------------------------------------------------------
(***************************************************************************)
(* PREDICATES, each the mirror of one shipped guard.                        *)
(***************************************************************************)

(* The write-fence predicate for the FENCED class, store/write_fence.go     *)
(* EvaluateWriteFence rules 1-2 (common.lua evaluate_write_fence), time-    *)
(* free. Rule 3 (producer headers mandatory) is a static 400 and rule 4     *)
(* (Producer-Epoch = generation) is the epoch parameter of the append.      *)
(* Rule 1 -- sealed: the request generation is at or below the authority's  *)
(* seal. Checked BEFORE the marker so a holder whose done landed learns      *)
(* `sealed`, not `marker`.                                                  *)
Sealed(a, w) == token[a][w].gen <= sealGen[a]

(* Rule 2 -- marker: append.lua step 4 as shipped (#169): the marker must   *)
(* be present, live, name exactly this (generation, wake_id, holder), and   *)
(* its own lease must be unexpired (time-free).                              *)
MarkerOK(a, w) ==
    /\ marker[a].present
    /\ marker[a].state = "live"
    /\ marker[a].gen = token[a][w].gen
    /\ marker[a].wake = token[a][w].wake
    /\ marker[a].holder = w
    /\ marker[a].live

FenceAdmits(a, w) == token[a][w].held /\ ~Sealed(a, w) /\ MarkerOK(a, w)

(* The base producer state machine, store.ValidateProducer / common.lua     *)
(* validate_producer, unchanged by the extension (it is wrapped, not         *)
(* modified). TRUE => the write is accepted and advances (epoch, lastSeq).   *)
(* A duplicate (same epoch, seq <= lastSeq) writes nothing; a stale epoch,   *)
(* a non-zero first seq, or a gap is rejected. All three are no-ops here.    *)
ProdAccepts(p, epoch, seq) ==
    IF ~p.present THEN seq = 0
    ELSE IF epoch < p.epoch THEN FALSE
    ELSE IF epoch > p.epoch THEN seq = 0
    ELSE seq = p.lastSeq + 1

Advanced(epoch, seq, bound) ==
    [present |-> TRUE, epoch |-> epoch, lastSeq |-> seq, bound |-> bound]

(* grant_append_fence.lua (C.4) guard, time-free (the lease test is the      *)
(* caller's leaseLive). Order: seal -> marker generation -> same-generation  *)
(* exactness. TRUE => the grant installs the marker.                         *)
Grantable(a, g, wk, w) ==
    /\ g > sealGen[a]                              \* durable revocation: never re-grant a sealed gen
    /\ marker[a].present =>
         /\ g >= marker[a].gen                     \* a lower generation is FENCED
         /\ g = marker[a].gen =>                   \* same gen: only the exact live claim renews
              /\ marker[a].state = "live"
              /\ marker[a].wake = wk
              /\ marker[a].holder = w

(* Supersession (C.4): a higher-generation grant over a present marker at a  *)
(* lower, not-yet-sealed generation fixes the predecessor's seal at the      *)
(* marker's generation (its definite last fenced offset, R3.3).              *)
GrantSeal(a, g) ==
    IF marker[a].present /\ marker[a].gen < g /\ marker[a].gen > sealGen[a]
      THEN marker[a].gen
      ELSE sealGen[a]

Max(x, y) == IF x > y THEN x ELSE y

(* Writing authority a's seal to s. Faithfully the field wfseal:<auth> is     *)
(* keyed by the marker's authority suffix; the SealKeyedByAuth=FALSE fault   *)
(* is the generation-only seal every authority on the stream inherits.       *)
SealAt(a, s) ==
    IF SealKeyedByAuth THEN [sealGen EXCEPT ![a] = s]
                       ELSE [b \in Auths |-> Max(sealGen[b], s)]

Installed(g, wk, w) ==
    [present |-> TRUE, state |-> "live", gen |-> g, wake |-> wk, holder |-> w, live |-> TRUE]

Tombstone(g, wk, w) ==
    [present |-> TRUE, state |-> "revoked", gen |-> g, wake |-> wk, holder |-> w, live |-> FALSE]

(* seal_append_fence.lua staleness (a delayed seal cannot fence a newer      *)
(* generation; a same-generation seal must name the exact claim). Taken by   *)
(* DelayedSeal only -- a current holder's seal is never stale -- and W10     *)
(* witnesses the STALE landing.                                              *)
SealStale(a, g, wk, w) ==
    /\ marker[a].present
    /\ \/ marker[a].gen > g
       \/ marker[a].gen = g /\ (marker[a].wake # wk \/ marker[a].holder # w)

(* The stream-slot effect of seal_append_fence.lua for claim (g, wk, w) on   *)
(* authority a: no mutation when STALE; otherwise the marker becomes the     *)
(* tombstone and the seal advances to g, monotone (idempotent `already`).    *)
(* The SealOnRevoke=FALSE fault is today's revoke_append_fence.lua: the      *)
(* tombstone without the seal.                                               *)
SealSlot(a, g, wk, w) ==
    LET stale == SealStale(a, g, wk, w)
    IN /\ marker' = IF stale THEN marker ELSE [marker EXCEPT ![a] = Tombstone(g, wk, w)]
       /\ sealGen' = IF stale \/ ~SealOnRevoke THEN sealGen
                     ELSE SealAt(a, Max(sealGen[a], g))

(* The worker's token names the authority's CURRENT claim (ack.lua fenced:   *)
(* token gen = cur gen, wake non-empty and equal). Used by done/heartbeat.   *)
TokenCurrent(a, w) ==
    /\ token[a][w].held
    /\ ctl[a].gen = token[a][w].gen
    /\ ctl[a].wake # 0
    /\ ctl[a].wake = token[a][w].wake

(* The clock's ordering fact (header): no claim on a older than generation   *)
(* g is still inside its subscription lease. A live lease always belongs to  *)
(* the worker's latest claim on a, whose generation its token carries.       *)
NoOlderLive(a, g) == \A w \in Workers: leaseLive[a][w] => token[a][w].gen >= g

----------------------------------------------------------------------------
(***************************************************************************)
(* ACTIONS. Each transcribes the GRANTING / ACCEPTING branch of exactly one  *)
(* shipped script or Go orchestration step; every other reply is a          *)
(* stutter.                                                                  *)
(***************************************************************************)

(* --- Claim: claim.lua rotate + grant_append_fence.lua (handleClaim) -----*)
(* BUSY while a live holder's lease is unexpired; otherwise the claim       *)
(* rotates (HINCRBY +1, fresh wake, holder = w, phase live) and the marker  *)
(* is granted at the new generation BEFORE any token leaves (K.2). A grant  *)
(* the seal refuses (new gen <= seal) releases the claim: a stutter here.   *)
(* Supersession seals the predecessor's marker generation. The worker now   *)
(* carries the write token <<gen, wake>> under a fresh, unexpired lease.     *)
Claim(a, w) ==
    /\ ctl[a].gen < MaxGen
    /\ \/ ctl[a].phase = "idle"
       \/ ctl[a].phase = "live" /\ ~leaseLive[a][ctl[a].holder]   \* expired-live takeover
    /\ LET g == ctl[a].gen + 1
       IN /\ Grantable(a, g, g, w)
          /\ ctl' = [ctl EXCEPT ![a] = [phase |-> "live", gen |-> g, wake |-> g, holder |-> w]]
          /\ sealGen' = SealAt(a, GrantSeal(a, g))
          /\ marker' = [marker EXCEPT ![a] = Installed(g, g, w)]
          /\ token' = [token EXCEPT ![a][w] = [held |-> TRUE, gen |-> g, wake |-> g]]
          /\ leaseLive' = [leaseLive EXCEPT ![a][w] = TRUE]
          /\ pending' = [pending EXCEPT ![a] = "none"]
    /\ UNCHANGED <<prod, log, inflight, crashes>>

(* --- Renew: heartbeat re-grant (mintWriteTokenOnAck -> grantWriteFences) -*)
(* A non-done ack by the current holder inside its lease re-runs the grant  *)
(* at the same (gen, wake, holder): it extends a live marker's lease,       *)
(* re-arms one that lapsed, reinstalls a reaped one (K.12), and is FENCED   *)
(* (ack 500) once the generation is sealed or a successor's marker is       *)
(* installed.                                                                *)
Renew(a, w) ==
    /\ TokenCurrent(a, w)
    /\ ctl[a].phase = "live" /\ ctl[a].holder = w
    /\ leaseLive[a][w]
    /\ LET g == token[a][w].gen
           wk == token[a][w].wake
       IN /\ Grantable(a, g, wk, w)
          /\ sealGen' = SealAt(a, GrantSeal(a, g))
          /\ marker' = [marker EXCEPT ![a] = Installed(g, wk, w)]
    /\ UNCHANGED <<ctl, leaseLive, token, prod, log, pending, inflight, crashes>>

(* --- DelayedGrant: a grant EVAL from an old snapshot landing late --------*)
(* The mintWriteTokenOnAck snapshot race (manager.go:1213-1218): the grant  *)
(* was decided against a stale view of the subscription and arrives after  *)
(* the claim ended. It carries the snapshot's (gen, wake, holder, lease):   *)
(* enabled while the deadline is unexpired, it re-runs the same guard. The  *)
(* per-authority seal is what refuses it for every past generation of the   *)
(* authority, independent of the tombstone's retention (K.4).               *)
DelayedGrant(a, w) ==
    /\ token[a][w].held
    /\ leaseLive[a][w]
    /\ LET g == token[a][w].gen
           wk == token[a][w].wake
       IN /\ Grantable(a, g, wk, w)
          /\ sealGen' = SealAt(a, GrantSeal(a, g))
          /\ marker' = [marker EXCEPT ![a] = Installed(g, wk, w)]
    /\ UNCHANGED <<ctl, leaseLive, token, prod, log, pending, inflight, crashes>>

(* --- Lapse: the wall clock passes a claim's subscription deadline --------*)
(* No script: `lease_until_ns <= now` becomes true for the deadline carried  *)
(* by w's claim on a -- and, since the marker's own deadline never exceeds   *)
(* it (header), for w's marker in the same instant. From here w's delayed   *)
(* grant is FENCED (grant_append_fence.lua line 1) and the marker refuses   *)
(* w's writes (rule 2). Older claims on a lapse first (header).              *)
Lapse(a, w) ==
    /\ leaseLive[a][w]
    /\ NoOlderLive(a, token[a][w].gen)
    /\ leaseLive' = [leaseLive EXCEPT ![a][w] = FALSE]
    /\ marker' = IF marker[a].holder = w THEN [marker EXCEPT ![a].live = FALSE] ELSE marker
    /\ UNCHANGED <<ctl, sealGen, token, prod, log, pending, inflight, crashes>>

(* --- MarkerLapse: the wall clock passes the marker's own deadline --------*)
(* The marker's lease_until_ns is fixed at the last SUCCESSFUL grant while   *)
(* every heartbeat extends the subscription lease first (ack.lua), so a run  *)
(* of failed re-grants lets the marker lapse -- and be reaped -- under a     *)
(* live claim (K.12). Fail-closed: the holder's writes are `marker` until   *)
(* its next heartbeat re-grants.                                             *)
MarkerLapse(a) ==
    /\ marker[a].present /\ marker[a].state = "live" /\ marker[a].live
    /\ NoOlderLive(a, marker[a].gen)
    /\ marker' = [marker EXCEPT ![a].live = FALSE]
    /\ UNCHANGED <<ctl, sealGen, leaseLive, token, prod, log, pending, inflight, crashes>>

(* --- ExpireLease: expire_lease.lua (a SERVER step, no client op) ---------*)
(* A live claim whose lease lapsed is idled: holder/wake cleared, gen       *)
(* UNCHANGED (no HINCRBY, INV-FENCE-04). The marker is untouched: its own   *)
(* lease_until_ns fences the deposed holder; the successor's grant records  *)
(* the supersession seal. Any owed done follow-up is moot.                  *)
ExpireLease(a) ==
    /\ ctl[a].phase = "live"
    /\ ~leaseLive[a][ctl[a].holder]
    /\ ctl' = [ctl EXCEPT ![a] = [phase |-> "idle", gen |-> ctl[a].gen, wake |-> 0, holder |-> NoWorker]]
    /\ pending' = [pending EXCEPT ![a] = "none"]
    /\ UNCHANGED <<marker, sealGen, leaseLive, token, prod, log, inflight, crashes>>

(* --- Reap: the PEXPIRE reaper deletes the marker key ---------------------*)
(* A revoked tombstone lives for retentionMs; a live marker lives for        *)
(* (lease_until_ns - now) + retentionMs, so it is reaped only once its own   *)
(* deadline lapsed. Independent of the subscription's state.                 *)
Reap(a) ==
    /\ marker[a].present
    /\ \/ marker[a].state = "revoked"
       \/ marker[a].state = "live" /\ ~marker[a].live
    /\ marker' = [marker EXCEPT ![a] = NoMarker]
    /\ UNCHANGED <<ctl, sealGen, leaseLive, token, prod, log, pending, inflight, crashes>>

(* --- AppendFenced: append.lua on a fenced stream, fenced class -----------*)
(* One script: the fence block (rules 1-2), the epoch binding (rule 4:      *)
(* Producer-Epoch = the token's generation), then validate_producer, then   *)
(* the write. An accepted write appends to the log, advances the producer,  *)
(* and binds the producer id (wfbind). Faithful only when CheckInSlot.      *)
AppendFenced(a, w, epoch, seq) ==
    /\ CheckInSlot
    /\ FenceAdmits(a, w)
    /\ epoch = token[a][w].gen
    /\ ProdAccepts(prod[w], epoch, seq)
    /\ log' = Append(log, [auth |-> a, gen |-> epoch, holder |-> w, seq |-> seq])
    /\ prod' = [prod EXCEPT ![w] = Advanced(epoch, seq, TRUE)]
    /\ UNCHANGED <<ctl, marker, sealGen, leaseLive, token, pending, inflight, crashes>>

(* --- AppendCheck / AppendCommit: the pre-#169 TOCTOU (CheckInSlot=FALSE) -*)
(* AppendFenced cut in two, nothing else changed. AppendCheck is its fence  *)
(* block (rules 1-2 + 4) decided OUT of the stream slot; AppendCommit is    *)
(* the rest of append.lua -- validate_producer, then the write -- landing   *)
(* later without re-running the fence. Only the fence was ever out of slot: *)
(* validate_producer always ran inside append.lua, so the commit re-runs    *)
(* the producer state machine and a write it rejects is dropped (the 4xx    *)
(* reply). Between the two steps the lease can lapse and a successor can be *)
(* granted and write -- the #169 shape.                                      *)
AppendCheck(a, w, epoch, seq) ==
    /\ ~CheckInSlot
    /\ ~inflight[a][w].active
    /\ FenceAdmits(a, w)
    /\ epoch = token[a][w].gen
    /\ inflight' = [inflight EXCEPT ![a][w] = [active |-> TRUE, epoch |-> epoch, seq |-> seq]]
    /\ UNCHANGED <<ctl, marker, sealGen, leaseLive, token, prod, log, pending, crashes>>

AppendCommit(a, w) ==
    /\ inflight[a][w].active
    /\ LET f == inflight[a][w]
           ok == ProdAccepts(prod[w], f.epoch, f.seq)
       IN /\ log' = IF ok THEN Append(log, [auth |-> a, gen |-> f.epoch, holder |-> w, seq |-> f.seq])
                          ELSE log
          /\ prod' = IF ok THEN [prod EXCEPT ![w] = Advanced(f.epoch, f.seq, TRUE)] ELSE prod
    /\ inflight' = [inflight EXCEPT ![a][w] = NoInflight]
    /\ UNCHANGED <<ctl, marker, sealGen, leaseLive, token, pending, crashes>>

(* --- AppendOpen: append.lua, open class, naming a fenced producer id -----*)
(* No token: the fence block runs only rule 5 -- refused iff the producer   *)
(* id is bound (wfbind set by an accepted fenced write). Otherwise exactly   *)
(* the base producer state machine (PROTOCOL.md 5.2 byte-for-byte). Not in  *)
(* the fenced log; the binding flag is untouched.                            *)
AppendOpen(p, epoch, seq) ==
    /\ ~(BindProducers /\ prod[p].bound)
    /\ ProdAccepts(prod[p], epoch, seq)
    /\ prod' = [prod EXCEPT ![p] = Advanced(epoch, seq, prod[p].bound)]
    /\ UNCHANGED <<ctl, marker, sealGen, leaseLive, token, log, pending, inflight, crashes>>

(* --- SealMarker: done, step 1 -- seal_append_fence.lua -------------------*)
(* sealWriteFencesIfCurrent: the holder's done at the current (gen, wake)   *)
(* -- accepted after the lease deadline, like ack.lua done -- runs the seal *)
(* in the stream slot (SealSlot). The seal is durable BEFORE ack.lua runs   *)
(* (SealBeforeIdle); a redelivery after the origin lost the follow-up (K.1) *)
(* finds the seal and recovers. The lazy fault runs it after ack.lua        *)
(* instead, on the claim ack.lua already idled.                              *)
SealMarker(a, w) ==
    /\ token[a][w].held
    /\ ctl[a].gen = token[a][w].gen
    /\ IF SealBeforeIdle
         THEN /\ TokenCurrent(a, w)
              /\ ctl[a].phase = "live" /\ ctl[a].holder = w
         ELSE pending[a] = "idled"
    /\ LET g == token[a][w].gen
           already == sealGen[a] >= g
       IN /\ SealSlot(a, g, token[a][w].wake, w)
          /\ pending' = [pending EXCEPT ![a] =
                           IF ~SealBeforeIdle THEN "none"
                           ELSE IF pending[a] = "lost" THEN "recovered"
                           ELSE IF already THEN "already"
                           ELSE "sealing"]
    /\ UNCHANGED <<ctl, leaseLive, token, prod, log, inflight, crashes>>

(* --- DelayedSeal: a seal EVAL from an old snapshot landing late ----------*)
(* sealWriteFencesIfCurrent decides against a Get snapshot and the seal     *)
(* EVAL lands in the stream slot afterwards; done is accepted after the     *)
(* deadline, so between the two the claim can be idled (expire_lease.lua)   *)
(* and a successor granted. The seal carries the snapshot's (gen, wake,     *)
(* holder): STALE against a newer marker it mutates nothing; otherwise it   *)
(* tombstones and seals a generation that already ended -- over-sealing,    *)
(* never unsafe (K.6). The follow-up ack.lua is FENCED, so no done sequence *)
(* is pending. Only the non-current case is new: current, it IS SealMarker. *)
DelayedSeal(a, w) ==
    /\ token[a][w].held
    /\ ~TokenCurrent(a, w)
    /\ SealSlot(a, token[a][w].gen, token[a][w].wake, w)
    /\ UNCHANGED <<ctl, leaseLive, token, prod, log, pending, inflight, crashes>>

(* --- Idle: done, step 2 -- ack.lua done='1' ------------------------------*)
(* Fence-only (accepted after the deadline): idles the claim, clears wake   *)
(* and holder, gen UNCHANGED. Faithfully it runs only once this claim's     *)
(* seal committed (pending sealing/already/recovered). The token is NOT     *)
(* dropped: its bytes outlive the claim and the fence must refuse them      *)
(* (K.1, K.7).                                                               *)
Idle(a, w) ==
    /\ TokenCurrent(a, w)
    /\ SealBeforeIdle => pending[a] \in IdleReady
    /\ ctl' = [ctl EXCEPT ![a] = [phase |-> "idle", gen |-> ctl[a].gen, wake |-> 0, holder |-> NoWorker]]
    /\ pending' = [pending EXCEPT ![a] = IF SealBeforeIdle THEN "none" ELSE "idled"]
    /\ UNCHANGED <<marker, sealGen, leaseLive, token, prod, log, inflight, crashes>>

(* --- Crash: the origin restarts --------------------------------------------*)
(* Loses every owed Go follow-up (a done that sealed but has not yet acked  *)
(* must be redelivered: "lost") and every out-of-slot in-flight append.     *)
(* The workers' bearer tokens survive in their own memory. The durable      *)
(* Redis state -- ctl, marker, seal, producer, log -- survives: that is the  *)
(* crux of the fail-closed argument (K.1).                                   *)
Crash ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ pending' = [a \in Auths |-> IF pending[a] \in SealedOwed THEN "lost" ELSE "none"]
    /\ inflight' = [a \in Auths |-> [w \in Workers |-> NoInflight]]
    /\ UNCHANGED <<ctl, marker, sealGen, leaseLive, token, prod, log>>

----------------------------------------------------------------------------
Next ==
    \/ \E a \in Auths, w \in Workers: Claim(a, w)
    \/ \E a \in Auths, w \in Workers: Renew(a, w)
    \/ \E a \in Auths, w \in Workers: DelayedGrant(a, w)
    \/ \E a \in Auths, w \in Workers: Lapse(a, w)
    \/ \E a \in Auths: MarkerLapse(a)
    \/ \E a \in Auths: ExpireLease(a)
    \/ \E a \in Auths: Reap(a)
    \/ \E a \in Auths, w \in Workers, e \in GenDom, s \in SeqDom: AppendFenced(a, w, e, s)
    \/ \E a \in Auths, w \in Workers, e \in GenDom, s \in SeqDom: AppendCheck(a, w, e, s)
    \/ \E a \in Auths, w \in Workers: AppendCommit(a, w)
    \/ \E p \in Workers, e \in GenDom, s \in SeqDom: AppendOpen(p, e, s)
    \/ \E a \in Auths, w \in Workers: SealMarker(a, w)
    \/ \E a \in Auths, w \in Workers: DelayedSeal(a, w)
    \/ \E a \in Auths, w \in Workers: Idle(a, w)
    \/ Crash

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS                                                        *)
(***************************************************************************)

(* INV-FENCE-05 (EpochsDoNotInterleave): per authority, the accepted fenced  *)
(* appends are totally ordered by writer epoch (= claim generation) in       *)
(* stream order, and an epoch-equal run has one holder. The append           *)
(* capability of a deposed generation never lands after its successor's.    *)
EpochsDoNotInterleave ==
    \A i, j \in DOMAIN log:
        (i < j /\ log[i].auth = log[j].auth) =>
            /\ log[i].gen <= log[j].gen
            /\ (log[i].gen = log[j].gen => log[i].holder = log[j].holder)

(* The last fenced entry written under producer id p, if any.               *)
LastFenced(p) ==
    {k \in DOMAIN log: log[k].holder = p /\ \A m \in DOMAIN log: m > k => log[m].holder # p}

(* BoundProducerNeverOpen (rule 5): once a producer id is bound by an        *)
(* accepted fenced write, only fenced writes advance it -- its (epoch,       *)
(* lastSeq) is exactly the last fenced entry's. An open-class write that     *)
(* advanced a bound id (the "omit the credential / autoClaim" hole) leaves   *)
(* the producer ahead of the log.                                            *)
BoundProducerNeverOpen ==
    \A p \in Workers:
        prod[p].bound =>
            \E k \in LastFenced(p): prod[p].epoch = log[k].gen /\ prod[p].lastSeq = log[k].seq

(* A marker that would admit its holder's writes: present, live, unexpired.  *)
MarkerAuthorizes(a) ==
    /\ marker[a].present
    /\ marker[a].state = "live"
    /\ marker[a].live

(* INV-FENCE-06, the ordering clause (SealPrecedesIdle): an authority whose  *)
(* claim ended never leaves a marker that still authorizes writes. The seal  *)
(* (revoke + wfseal) is durable BEFORE ack.lua idles, and a lease lapse       *)
(* disarms the marker before expire_lease.lua idles -- so there is never a   *)
(* claimable subscription with a live, unexpired marker (the lazy-seal crash *)
(* window, K.1).                                                              *)
SealPrecedesIdle ==
    \A a \in Auths: ctl[a].phase = "idle" => ~MarkerAuthorizes(a)

(* SealIsolation: an authority's seal is written only at its own claim      *)
(* generations (wfseal:<auth> is keyed by the marker's authority suffix),   *)
(* so it never runs ahead of that authority's fence register. Its negative  *)
(* control is the generation-only seal (SealKeyedByAuth=FALSE, design Q5):  *)
(* one authority's done seals the other, whose next claim is refused.       *)
SealIsolation ==
    \A a \in Auths: sealGen[a] <= ctl[a].gen

(* The conjunction TLC checks as a state invariant.                          *)
Inv ==
    /\ TypeOK
    /\ EpochsDoNotInterleave
    /\ BoundProducerNeverOpen
    /\ SealPrecedesIdle
    /\ SealIsolation

(* --- Two-state (action) properties --------------------------------------*)

(* INV-FENCE-02 (GenMonotone): each authority's generation never decreases.  *)
GenMonotone ==
    \A a \in Auths: ctl'[a].gen >= ctl[a].gen

(* INV-FENCE-06 (SealedIsFinal): the seal is monotone, and every append      *)
(* accepted in a step lies strictly above the seal of its authority as it    *)
(* stood when the write was admitted. After a seal at g no append with       *)
(* generation <= g of that authority is ever accepted.                       *)
SealedIsFinal ==
    /\ \A a \in Auths: sealGen'[a] >= sealGen[a]
    /\ Len(log') > Len(log) =>
         LET e == log'[Len(log')] IN e.gen > sealGen[e.auth]

GenMonotoneProp == [][GenMonotone]_vars
SealedIsFinalProp == [][SealedIsFinal]_vars

(***************************************************************************)
(* CRASH-WINDOW / NON-VACUITY WITNESSES.                                    *)
(*                                                                         *)
(* Each witness is a STATE or a STEP the safety argument must genuinely     *)
(* reach. Run TLC with the negation -- INVARIANT NotWx for a state witness,  *)
(* PROPERTY NotWx (an action property) for a step witness -- and the        *)
(* counterexample is the witness. Used by the coverage configs, not the     *)
(* safety run.                                                              *)
(***************************************************************************)
\* W5 (state) seal-before-ack, recovered: one authority has accepted appends at two
\* generations -- a predecessor's generation ended (done or lease lapse) and
\* its successor genuinely wrote, so EpochsDoNotInterleave is not vacuous.
WindowW5 == \E i, j \in DOMAIN log: log[i].auth = log[j].auth /\ log[i].gen # log[j].gen
\* W6 (state) the deposed holder's late write: a worker still carries a token the
\* fence refuses (marker superseded / revoked / reaped / sealed, or its lease
\* lapsed) -- a FENCED reply is genuinely reachable for a held token. (K.2,
\* a granted marker whose token never left, is closed trivially and not
\* modeled: Claim grants and mints atomically.)
WindowW6 == \E a \in Auths, w \in Workers: token[a][w].held /\ ~FenceAdmits(a, w)
\* W7 (step) seal redelivery, the K.1 window entered AND recovered: the origin
\* crashed with the seal committed and ack.lua owed ("sealing", then Crash:
\* "lost"), the holder redelivered its done, the re-seal found wfseal >= g
\* (`already`: "recovered"), and ack.lua then idled the claim. The recovery is
\* the Idle step out of "recovered", so the witness is that step.
RecoveredIdle == \E a \in Auths, w \in Workers: pending[a] = "recovered" /\ Idle(a, w)
\* W8 (state) delayed grant refused by the seal: a worker carries a token under an
\* unexpired deadline whose generation the authority has sealed -- the grant
\* EVAL from its old snapshot passes the lease test and is refused by the seal
\* (K.4), not by the tombstone.
WindowW8 == \E a \in Auths, w \in Workers:
              token[a][w].held /\ leaseLive[a][w] /\ token[a][w].gen <= sealGen[a]
\* W9 (step) marker reaped under a live claim (K.12), repaired: the holder's
\* subscription lease is live and its generation unsealed, its marker is gone
\* (its own deadline lapsed and the key was reaped; the holder's writes are
\* `marker`), and the next heartbeat re-grant reinstalls it. The witness is
\* the Renew step over an absent marker: in the faithful lane Renew's only
\* non-stuttering effect is this repair (re-arm / reinstall), back to an
\* earlier state, so no state predicate can show it -- the step can.
ReapedReinstall == \E a \in Auths, w \in Workers: ~marker[a].present /\ Renew(a, w)
\* W10 (state) the delayed seal lands STALE: a worker still carries a token whose
\* claim ended and whose authority's marker names a newer claim, so the seal
\* EVAL from its snapshot (DelayedSeal) takes seal_append_fence.lua's STALE
\* branch (SealStale) and mutates nothing.
WindowW10 == \E a \in Auths, w \in Workers:
               /\ token[a][w].held /\ ~TokenCurrent(a, w)
               /\ SealStale(a, token[a][w].gen, token[a][w].wake, w)

NotW5  == ~WindowW5
NotW6  == ~WindowW6
NotW7  == [][~RecoveredIdle]_vars
NotW8  == ~WindowW8
NotW9  == [][~ReapedReinstall]_vars
NotW10 == ~WindowW10

(***************************************************************************)
(* State constraint to keep the model finite even if a bound is loosened.   *)
(***************************************************************************)
StateConstraint ==
    /\ \A a \in Auths: ctl[a].gen <= MaxGen
    /\ crashes <= MaxCrashes

=============================================================================
