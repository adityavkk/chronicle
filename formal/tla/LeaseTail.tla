----------------------------- MODULE LeaseTail -----------------------------
(***************************************************************************)
(* L3 lease-tail-drop refinement (issue #40, INV-LR-01 / INV-JEP-L3-01).    *)
(*                                                                         *)
(* The slot/subscription lease lives in TWO places in Chronicle:            *)
(*   1. the DURABLE record -- a hash (the sub hash {phase, lease_until_ns,   *)
(*      ...} for a subscription, or the {ownership} slot hash {owner_id,     *)
(*      owner_epoch, lease_expiry_ns} for a slot). This is the SOURCE OF      *)
(*      TRUTH.                                                               *)
(*   2. the per-slot lease SCHEDULE ZSET (leaseZKey) -- a CACHE the lease     *)
(*      worker scans to know WHEN to expire-check. It is DERIVED from the     *)
(*      durable record, re-derivable, and a crash / DR-failover / a stray     *)
(*      ZREM can drop it while the durable record survives.                  *)
(*                                                                         *)
(* The hazard (the "lease tail drop", scenario_leasetail.go): the ZSET entry *)
(* is ZREMmed but the durable record is intact. The lease worker, which      *)
(* scans the ZSET, is then BLIND to this lease -- so without recovery the     *)
(* lease never gets its expire-check, the sub never returns to idle, and a    *)
(* stranded wake is never re-fired (delivery stalls).                        *)
(*                                                                         *)
(* The recovery (manager.go reconcileLeases + restore_lease.lua, INV-LR-01):  *)
(* reconcileLeases reads the DURABLE list (List(), NOT the ZSET) and, for     *)
(* every record that is live/waking with a positive lease_until_ns but is     *)
(* MISSING from the lease ZSET, RestoreLease re-ZADDs the entry derived from   *)
(* the durable hash. RestoreLease is PHASE-CONDITIONED on the hash still       *)
(* being live/waking, so a concurrently-idled record is left untouched; a     *)
(* re-ZADD of a present entry rewrites the same score (idempotent).           *)
(*                                                                         *)
(* THE PROPERTY WE CHECK: the lease state is RECOVERABLE from the durable      *)
(* record alone. After ANY interleaving of drops and reconciles, under weak    *)
(* fairness of the reconcile loop, a record that is durably live-with-lease     *)
(* eventually has its ZSET entry restored (so the lease worker can see it       *)
(* again) -- AND the reconcile never INVENTS a ZSET entry for a record that      *)
(* is not durably live (no spurious lease), and never regresses a durably-       *)
(* idled record. (INV-LR-01 idempotence + phase-conditioning.)                   *)
(*                                                                         *)
(* This is the lease analogue of INV-RECOVER-04 (the fan-out index is a cache  *)
(* repairable from the canonical links): here the lease ZSET is a cache         *)
(* repairable from the canonical durable record.                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Records      \* set of lease-bearing records (subscriptions or ownership slots)

\* The durable phase of a record. live/waking carry an active lease the worker
\* must track; idle carries none. (Mirrors the sub-hash phase / a slot's
\* owned-vs-released state.)
Phases == {"idle", "live", "waking"}
HasLease(p) == p \in {"live", "waking"}

VARIABLES
    durablePhase,  \* [Records -> Phases]: the SOURCE OF TRUTH (the durable hash).
    leaseZSet      \* SUBSET Records: the per-slot lease SCHEDULE ZSET (the cache).
                   \* A record is "in the schedule" iff it is a member here.

vars == <<durablePhase, leaseZSet>>

TypeOK ==
    /\ durablePhase \in [Records -> Phases]
    /\ leaseZSet \in SUBSET Records

----------------------------------------------------------------------------
(***************************************************************************)
(* Init: every record idle, the schedule empty (consistent: no lease, no     *)
(* schedule entry).                                                          *)
(***************************************************************************)
Init ==
    /\ durablePhase = [r \in Records |-> "idle"]
    /\ leaseZSet = {}

----------------------------------------------------------------------------
(***************************************************************************)
(* DURABLE transitions (the client/worker writes the source of truth AND      *)
(* keeps the schedule in step -- the normal, non-crashed path).               *)
(***************************************************************************)

\* ArmLease(r): a record goes live/waking and takes a lease. The durable hash
\* is written AND the schedule ZSET is ZADDed in step (the happy path, both
\* sides consistent). Models arm_wake/claim arming the lease + leaseZKey ZADD.
ArmLease(r, p) ==
    /\ p \in {"live", "waking"}
    /\ durablePhase[r] = "idle"
    /\ durablePhase' = [durablePhase EXCEPT ![r] = p]
    /\ leaseZSet' = leaseZSet \cup {r}

\* IdleLease(r): a record returns to idle (ack done / expire_lease) -- the
\* durable hash is set idle AND the schedule entry ZREMmed, in step.
IdleLease(r) ==
    /\ HasLease(durablePhase[r])
    /\ durablePhase' = [durablePhase EXCEPT ![r] = "idle"]
    /\ leaseZSet' = leaseZSet \ {r}

----------------------------------------------------------------------------
(***************************************************************************)
(* THE FAULT -- DropLeaseTail(r): the ZSET schedule entry is dropped (ZREM /  *)
(* crash between the hash write and the ZADD / DR-failover loses the un-AOF'd  *)
(* ZSET tail) while the DURABLE record is INTACT. This is the stranded-lease   *)
(* window the lease worker is blind to.                                       *)
(***************************************************************************)
DropLeaseTail(r) ==
    /\ r \in leaseZSet
    /\ HasLease(durablePhase[r])        \* durable record still live/waking
    /\ leaseZSet' = leaseZSet \ {r}     \* the schedule entry vanishes
    /\ UNCHANGED durablePhase            \* the source of truth survives

----------------------------------------------------------------------------
(***************************************************************************)
(* THE RECOVERY -- ReconcileLease(r) (reconcileLeases + restore_lease.lua,    *)
(* INV-LR-01). For a record that is DURABLY live/waking but MISSING from the   *)
(* schedule ZSET, re-derive (re-ZADD) the schedule entry FROM THE DURABLE      *)
(* HASH. PHASE-CONDITIONED: only restores when the durable hash is still       *)
(* live/waking (RestoreLease's guard), so a concurrently-idled record is NOT   *)
(* given a spurious lease. Idempotent: a re-ZADD of a present entry is a no-op  *)
(* on the set (so we guard on `r \notin leaseZSet`, the meaningful case).      *)
(***************************************************************************)
ReconcileLease(r) ==
    /\ HasLease(durablePhase[r])         \* durable record live/waking (the guard)
    /\ r \notin leaseZSet                \* schedule entry is missing (dropped)
    /\ leaseZSet' = leaseZSet \cup {r}   \* re-derive it from the durable hash
    /\ UNCHANGED durablePhase

----------------------------------------------------------------------------
Next ==
    \/ \E r \in Records, p \in {"live", "waking"} : ArmLease(r, p)
    \/ \E r \in Records : IdleLease(r)
    \/ \E r \in Records : DropLeaseTail(r)
    \/ \E r \in Records : ReconcileLease(r)

(***************************************************************************)
(* FAIRNESS: weak fairness of the reconcile loop -- a continuously-stranded    *)
(* lease (durably live/waking, missing from the schedule) is EVENTUALLY        *)
(* restored. No fairness on the fault (DropLeaseTail is adversarial). We also   *)
(* assume the durable record EVENTUALLY STOPS being re-dropped long enough to   *)
(* recover: modeled by WF on ReconcileLease (a continuously-enabled restore     *)
(* must fire). To keep the leads-to non-vacuous we do NOT make DropLeaseTail    *)
(* fair, so an adversary cannot drop forever faster than reconcile restores     *)
(* under WF -- but a record that is dropped and then left alone is restored.    *)
(***************************************************************************)
Fairness == \A r \in Records : WF_vars(ReconcileLease(r))

Spec == Init /\ [][Next]_vars /\ Fairness
SpecNoFair == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* SAFETY INVARIANTS.                                                        *)
(***************************************************************************)

\* INV-LR-01 (no spurious lease / never invent membership): the schedule ZSET
\* never contains a record that is NOT durably live/waking. Reconcile only ever
\* restores from a live/waking durable hash, and IdleLease ZREMs in step, so a
\* schedule entry always reflects a real durable lease. (The lease analogue of
\* INV-RECOVER-04's "reconcile never invents membership absent from links".)
NoSpuriousLease ==
    \A r \in Records : r \in leaseZSet => HasLease(durablePhase[r])

Inv ==
    /\ TypeOK
    /\ NoSpuriousLease

----------------------------------------------------------------------------
(***************************************************************************)
(* THE RECOVERABILITY LIVENESS -- the headline L3 property.                  *)
(*                                                                         *)
(* StrandedLease(r): the record is durably live/waking (it OWES a lease) but   *)
(* is MISSING from the schedule ZSET -- the lease worker is blind to it.        *)
(* Recovered(r): the schedule entry is present again (the worker can see it).   *)
(*                                                                         *)
(* INV-LR-01 / INV-JEP-L3-01: a stranded lease is EVENTUALLY recovered from     *)
(* the durable record alone -- being stranded LEADS TO either being restored     *)
(* OR the durable record having (legitimately) idled (no lease owed anymore).    *)
(* The disjunct handles a record that idles WHILE stranded: that is a valid      *)
(* resolution (no lease to recover), not a stall.                                *)
(***************************************************************************)
StrandedLease(r) == HasLease(durablePhase[r]) /\ r \notin leaseZSet
Recovered(r) == r \in leaseZSet
Idled(r) == durablePhase[r] = "idle"

LeaseRecoverable ==
    \A r \in Records : StrandedLease(r) ~> (Recovered(r) \/ Idled(r))

----------------------------------------------------------------------------
(* Non-vacuity witness: the stranded state is actually reachable (run the      *)
(* negation as an INVARIANT -> MUST be violated).                              *)
SomeStranded == \E r \in Records : StrandedLease(r)
NoStranded == ~SomeStranded

=============================================================================
