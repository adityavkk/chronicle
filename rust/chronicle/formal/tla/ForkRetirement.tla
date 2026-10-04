--------------------------- MODULE ForkRetirement ---------------------------
EXTENDS Naturals, TLC
CONSTANT BadForgetPin
VARIABLES incarnation, decision, target, retained, deleted, delayed, erased
vars == <<incarnation,decision,target,retained,deleted,delayed,erased>>
(* One old transaction; each transition is a durable Raft-group action.
   A delayed Prepare can arrive even after abort cleanup and source recreation.
   Source identity is durable: strict newer incarnation is retirement evidence.
   Absence/timeouts are never evidence. *)
Init == /\ incarnation=1 /\ decision="preparing" /\ target="absent"
        /\ retained=FALSE /\ deleted=FALSE /\ delayed=TRUE /\ erased=FALSE
Prepare == /\ delayed /\ target="absent"
           /\ target'="prepared" /\ delayed'=FALSE
           /\ UNCHANGED <<incarnation,decision,retained,deleted,erased>>
Commit == /\ incarnation=1 /\ decision="preparing" /\ target="prepared"
          /\ decision'="commit" /\ retained'=TRUE
          /\ UNCHANGED <<incarnation,target,deleted,delayed,erased>>
Abort == /\ incarnation=1 /\ decision="preparing" /\ decision'="abort"
         /\ UNCHANGED <<incarnation,target,retained,deleted,delayed,erased>>
Finish == /\ target="prepared" /\ decision \in {"commit","abort"}
          /\ target'=IF decision="commit" THEN "live" ELSE "absent"
          /\ UNCHANGED <<incarnation,decision,retained,deleted,delayed,erased>>
(* A duplicate Prepare already in flight survives a successful abort Finish. *)
Duplicate == /\ ~delayed /\ decision="abort" /\ target="absent"
             /\ delayed'=TRUE
             /\ UNCHANGED <<incarnation,decision,target,retained,deleted,erased>>
Delete == /\ decision#"preparing" /\ ~deleted /\ deleted'=TRUE
          /\ UNCHANGED <<incarnation,decision,target,retained,delayed,erased>>
Recreate == /\ incarnation=1 /\ deleted /\ (~retained \/ BadForgetPin)
            /\ incarnation'=2 /\ decision'="forgotten"
            /\ UNCHANGED <<target,retained,deleted,delayed,erased>>
RecoverRetired == /\ incarnation=2 /\ target="prepared"
                  /\ target'="absent" /\ erased'=TRUE
                  /\ UNCHANGED <<incarnation,decision,retained,deleted,delayed>>
Next == Prepare \/ Commit \/ Abort \/ Finish \/ Duplicate \/ Delete \/ Recreate \/ RecoverRetired
Spec == Init /\ [][Next]_vars
CommittedPinned == retained => incarnation=1
NoCommittedErasure == erased => ~retained
=============================================================================
