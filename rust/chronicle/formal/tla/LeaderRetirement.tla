-------------------------- MODULE LeaderRetirement --------------------------
CONSTANT WaitForLearner
VARIABLES demoted, removal, applied, leader
vars == <<demoted, removal, applied, leader>>

(* Start after replacement voters have committed, with the old leader retained
   as a node. Raft supplies safe quorum commitment; it is not reimplemented here.
   demoted records local application of that uniform membership boundary. *)
Init == /\ demoted = FALSE /\ removal = FALSE
        /\ applied = FALSE /\ leader = TRUE
DeliverDemotion == /\ ~demoted /\ demoted' = TRUE
                   /\ UNCHANGED <<removal, applied, leader>>
RemoveSelf == /\ demoted /\ ~removal
              /\ (~WaitForLearner \/ ~leader)
              /\ removal' = TRUE
              /\ UNCHANGED <<demoted, applied, leader>>
ApplyRemoval == /\ removal /\ ~applied /\ applied' = TRUE
                /\ UNCHANGED <<demoted, removal, leader>>
Tick == /\ applied /\ leader /\ leader' = FALSE
        /\ UNCHANGED <<demoted, removal, applied>>
Next == DeliverDemotion \/ RemoveSelf \/ ApplyRemoval \/ Tick
Spec == Init /\ [][Next]_vars
        /\ WF_vars(DeliverDemotion) /\ WF_vars(RemoveSelf)
        /\ WF_vars(ApplyRemoval) /\ WF_vars(Tick)
RemovedAfterDemotion == removal => demoted
StoppedAfterApply == ~leader => applied
EventuallyStopped == <>~leader
=============================================================================
