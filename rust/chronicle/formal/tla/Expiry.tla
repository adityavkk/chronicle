------------------------------ MODULE Expiry ------------------------------
EXTENDS Naturals
CONSTANT BadExpiryFence
VARIABLES sliding, incarnation, access, deleted, pending, safeDeletion
vars == <<sliding, incarnation, access, deleted, pending, safeDeletion>>
Deadline == IF sliding THEN access + 2 ELSE 2
Init == /\ sliding \in BOOLEAN /\ incarnation = 1 /\ access = 0
        /\ deleted = FALSE /\ pending = [inc |-> 0, access |-> 0, now |-> 0]
        /\ safeDeletion = TRUE
(* Time is supplied in committed commands, never sampled during apply.
   Non-monotonic samples include bounded clock rollback. *)
Touch(now) == /\ ~deleted /\ now <= Deadline
              /\ access' = IF sliding /\ now > access THEN now ELSE access
              /\ UNCHANGED <<sliding, incarnation, deleted, pending, safeDeletion>>
Prepare(now) == /\ ~deleted /\ now > Deadline
                /\ pending' = [inc |-> incarnation, access |-> access, now |-> now]
                /\ UNCHANGED <<sliding, incarnation, access, deleted, safeDeletion>>
ValidExpiry == pending.inc = incarnation /\ pending.access = access
               /\ pending.now > Deadline
Expire == /\ ~deleted /\ pending.inc # 0 /\ (BadExpiryFence \/ ValidExpiry)
          /\ deleted' = TRUE /\ safeDeletion' = ValidExpiry
          /\ UNCHANGED <<sliding, incarnation, access, pending>>
Delete == /\ ~deleted /\ deleted' = TRUE
          /\ UNCHANGED <<sliding, incarnation, access, pending, safeDeletion>>
Recreate == /\ deleted /\ incarnation = 1 /\ incarnation' = 2
            /\ access' = 0 /\ deleted' = FALSE
            /\ UNCHANGED <<sliding, pending, safeDeletion>>
(* Recovery is replay through the durable applied boundary, including access
   time and immutable policy. Serialization is checked against Rust separately. *)
Recover == UNCHANGED vars
Next == (\E now \in 0..5: Touch(now) \/ Prepare(now)) \/ Expire \/ Delete \/ Recreate \/ Recover
Spec == Init /\ [][Next]_vars
ExpiryFenced == safeDeletion
AbsoluteUnchanged == ~sliding => access = 0
===========================================================================
