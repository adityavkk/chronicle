----------------------------- MODULE Ownership -----------------------------
(***************************************************************************)
(* Chronicle owner-epoch slot-ownership CAS, modeled at the implementation *)
(* grain (issue #38, INV-OWNER-01).  This is the standalone owner-epoch     *)
(* register: per slot a {owner_id, owner_epoch, lease_expiry} record, and   *)
(* the claim_shard.lua / check_owner.lua semantics.  It is the {ownership}- *)
(* layer analogue of the per-subscription (gen,wake_id) fence in            *)
(* SubscriptionFence.tla -- a DIFFERENT, ORTHOGONAL axis: it shards which    *)
(* REPLICA runs the background loops for a slot, never the per-(subId,g)     *)
(* claim granularity (webhook/ownership.go header).                         *)
(*                                                                         *)
(* Mirrors byte-for-byte:                                                   *)
(*   - claim_shard.lua: BUSY iff a live foreign owner (owner # me /\         *)
(*     owner present /\ lease unexpired); else grant.  owner = me -> RENEW   *)
(*     (epoch KEPT).  owner # me -> TRANSFER (HINCRBY owner_epoch +1,        *)
(*     strictly up; mints 1 on the first claim of an unowned slot).          *)
(*   - check_owner.lua: UNOWNED if no owner; FENCED if owner # me OR epoch   *)
(*     mismatch; else OWNER.  Reads owner_id/owner_epoch only, never the     *)
(*     lease clock -- so the OWNER verdict is TIME-FREE and exact            *)
(*     (model_shard.go: "no escape hatch is needed").                        *)
(*                                                                         *)
(* The Go mirror is webhook/ownership.go (SlotClaim / OwnerCheck /          *)
(* OwnerScope); the Porcupine oracle is jepsen/checker/model_shard.go.      *)
(*                                                                         *)
(* Time is deliberately a discrete modeled clock that gates ONLY the        *)
(* claim grant/BUSY split (lease_expiry), exactly as in model_shard.go: the *)
(* epoch algebra itself (transfer => strictly greater; renew => identical)  *)
(* is time-free.                                                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Replicas,     \* set of replica identities, e.g. {r1, r2}
    Slots,        \* set of ownership-slot identities, e.g. {h1}
    MaxEpoch,     \* owner_epoch ceiling (state-space bound)
    MaxClock      \* discrete-clock ceiling (slot lease deadlines live in [0, MaxClock])

NoOwner == "none"   \* sentinel for owner_id = false (Lua HGET miss / unowned slot)

ASSUME NoOwner \notin Replicas

VARIABLES
    slot,    \* [Slots -> [owner, epoch, lease_expiry]]
    oclock   \* global discrete clock (Naturals), advanced by OTick

ovars == <<slot, oclock>>

SlotT == [ owner: Replicas \cup {NoOwner},
           epoch: 0..MaxEpoch,
           lease_expiry: 0..MaxClock ]

OTypeOK ==
    /\ slot \in [Slots -> SlotT]
    /\ oclock \in 0..MaxClock

(***************************************************************************)
(* Init.  Every slot unowned: owner = NoOwner, epoch 0 (HINCRBY mints 1 on  *)
(* the first transfer, so 0 is the never-claimed sentinel; parseOwnerEpoch  *)
(* parses a missing epoch to 0, below any minted epoch), no lease.          *)
(***************************************************************************)
OInit ==
    /\ slot = [h \in Slots |->
                 [owner |-> NoOwner, epoch |-> 0, lease_expiry |-> 0]]
    /\ oclock = 0

(* SlotLeaseExpired mirrors claim_shard.lua's `exp > now` test (negated):    *)
(* a foreign owner blocks only while its lease is strictly in the future.    *)
SlotLeaseExpired(h) == slot[h].lease_expiry <= oclock

MinPlus(c, d) == IF c + d > MaxClock THEN MaxClock ELSE c + d

(***************************************************************************)
(* ClaimShard(me, h) -- claim_shard.lua.                                    *)
(*                                                                         *)
(*   owner # false /\ owner # me /\ exp > now  ->  BUSY (no grant, no-op).   *)
(*   owner = me                                ->  RENEWED (epoch KEPT).     *)
(*   else (unowned or expired-foreign)         ->  CLAIMED (HINCRBY +1).     *)
(*                                                                         *)
(* The BUSY branch is a stuttering no-op (grants nothing, mutates nothing). *)
(* Only the granting branches change state.  A transfer needs epoch room.   *)
(***************************************************************************)
ClaimShard(me, h) ==
    \* BUSY guard: a live foreign owner blocks the claim (mirror claim_shard.lua:30).
    /\ ~(slot[h].owner # NoOwner /\ slot[h].owner # me /\ ~SlotLeaseExpired(h))
    /\ LET isRenew == slot[h].owner = me
       IN /\ \/ isRenew                               \* renew: any epoch is fine
             \/ slot[h].epoch < MaxEpoch              \* transfer: needs epoch headroom
          /\ slot' = [slot EXCEPT
                        ![h].owner = me,
                        ![h].epoch = IF isRenew THEN slot[h].epoch ELSE slot[h].epoch + 1,
                        ![h].lease_expiry = MinPlus(oclock, 1)]
    /\ UNCHANGED oclock

(* Depose(h) -- a slot owner relinquishes / its process dies.  Models the    *)
(* membership drop that lets ANOTHER replica transfer the slot.  The owner_id*)
(* / owner_epoch stay INTACT in the hash (model_shard.go: ownership has no    *)
(* silent mutation; the record persists until the next claim_shard TRANSFER), *)
(* only the lease lapses -- so a deposed-then-resumed owner still carries its  *)
(* old (owner_id, epoch) and is fenced ONLY by a later transfer bumping epoch. *)
Depose(h) ==
    /\ slot[h].owner # NoOwner
    /\ slot' = [slot EXCEPT ![h].lease_expiry = 0]   \* lapse the lease; owner/epoch intact
    /\ UNCHANGED oclock

(* OTick advances the discrete clock, enabling SlotLeaseExpired.             *)
OTick ==
    /\ oclock < MaxClock
    /\ oclock' = oclock + 1
    /\ UNCHANGED slot

ONext ==
    \/ \E me \in Replicas, h \in Slots: ClaimShard(me, h)
    \/ \E h \in Slots: Depose(h)
    \/ OTick

OSpec == OInit /\ [][ONext]_ovars

----------------------------------------------------------------------------
(***************************************************************************)
(* check_owner.lua verdict, as a pure operator over the register.           *)
(***************************************************************************)
OwnerVerdict(h, me, expectedEpoch) ==
    IF slot[h].owner = NoOwner THEN "UNOWNED"
    ELSE IF slot[h].owner # me \/ slot[h].epoch # expectedEpoch THEN "FENCED"
    ELSE "OWNER"

----------------------------------------------------------------------------
(***************************************************************************)
(* INV-OWNER-01 standalone safety properties.                               *)
(***************************************************************************)

(* A slot has at most one owner at a time: trivially true of the register   *)
(* (one owner field), but we state it as the observable consequence -- two   *)
(* distinct replicas are never BOTH the current owner.  (A single-field      *)
(* register cannot hold two owners; the real content is the epoch algebra    *)
(* below.)                                                                  *)
SingleOwner ==
    \A h \in Slots:
        \A r1, r2 \in Replicas:
            (r1 # r2 /\ slot[h].owner = r1 /\ slot[h].owner = r2) => FALSE

(* INV-OWNER-01 (epoch monotone): owner_epoch never decreases (HINCRBY only  *)
(* ever bumps; renew keeps it; nothing lowers it).  The two-state property.  *)
EpochMonotone ==
    \A h \in Slots: slot'[h].epoch >= slot[h].epoch

(* INV-OWNER-01 (transfer bumps strictly): if ownership changes hands across *)
(* a step (owner goes from r1 to a DIFFERENT r2, both real), the epoch       *)
(* strictly increased.  A renew (owner unchanged) keeps the epoch.  This is  *)
(* the load-bearing CAS property model_shard.go checks: a transfer that      *)
(* reused the epoch would be a silently-dropping LWW.                        *)
TransferBumpsEpoch ==
    \A h \in Slots:
        (   slot[h].owner # NoOwner
         /\ slot'[h].owner # NoOwner
         /\ slot'[h].owner # slot[h].owner )
        => slot'[h].epoch > slot[h].epoch

(* RENEW keeps the epoch: if the owner is unchanged and real across a step,  *)
(* the epoch is unchanged (bump-on-transfer-only).                          *)
RenewKeepsEpoch ==
    \A h \in Slots:
        (   slot[h].owner # NoOwner
         /\ slot'[h].owner = slot[h].owner
         /\ slot'[h].lease_expiry >= slot[h].lease_expiry )  \* a ClaimShard renew, not a Depose
        => slot'[h].epoch = slot[h].epoch

OInv ==
    /\ OTypeOK
    /\ SingleOwner

(* Action-property wrappers checked via PROPERTY in the .cfg.                *)
EpochMonotoneProp == [][EpochMonotone]_ovars
TransferBumpsEpochProp == [][TransferBumpsEpoch]_ovars
RenewKeepsEpochProp == [][RenewKeepsEpoch]_ovars

(* Reachability witnesses (run the negation as an INVARIANT to prove the     *)
(* state is reached): a live foreign owner is BUSY, and a transfer occurred. *)
\* BUSY-reachable: a slot owned by r with an unexpired lease, and a DIFFERENT
\* replica exists that would be refused.  Witnesses the BUSY branch is live.
BusyReachable ==
    \E h \in Slots, r \in Replicas:
        /\ slot[h].owner = r
        /\ ~SlotLeaseExpired(h)
        /\ \E r2 \in Replicas: r2 # r
NotBusyReachable == ~BusyReachable
\* Transfer-reachable: some slot reached epoch >= 2, i.e. ownership changed
\* hands at least once after the first claim (a genuine TRANSFER, not just a
\* first claim).
TransferReachable == \E h \in Slots: slot[h].epoch >= 2
NotTransferReachable == ~TransferReachable

OStateConstraint ==
    /\ \A h \in Slots: slot[h].epoch <= MaxEpoch
    /\ oclock <= MaxClock

=============================================================================
