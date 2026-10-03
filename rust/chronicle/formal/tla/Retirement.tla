----------------------------- MODULE Retirement -----------------------------
EXTENDS Naturals
CONSTANT BadCompletion
VARIABLES repaired, reachable, retained, nonvoter, observed, retired
vars == <<repaired, reachable, retained, nonvoter, observed, retired>>

(* Raft supplies committed uniform replacement and durable membership delivery.
   This models reconciliation, not the consensus algorithm. An unreachable old
   replica is not required for repair when the applicable quorums are available. *)
Init == /\ repaired = FALSE /\ reachable = FALSE /\ retained = FALSE
        /\ nonvoter = FALSE /\ observed = FALSE /\ retired = FALSE
Repair == /\ ~repaired /\ repaired' = TRUE /\ retained' = TRUE
          /\ UNCHANGED <<reachable, nonvoter, observed, retired>>
Connect == /\ reachable' = ~reachable
           /\ observed' = FALSE
           /\ UNCHANGED <<repaired, retained, nonvoter, retired>>
Deliver == /\ repaired /\ retained /\ reachable /\ nonvoter' = TRUE
           /\ UNCHANGED <<repaired, reachable, retained, observed, retired>>
Observe == /\ reachable /\ observed' = nonvoter
           /\ UNCHANGED <<repaired, reachable, retained, nonvoter, retired>>
Prune == /\ repaired /\ retained /\ (observed \/ ~reachable)
         /\ retained' = FALSE
         /\ retired' = IF BadCompletion THEN TRUE ELSE observed
         /\ UNCHANGED <<repaired, reachable, nonvoter, observed>>
Retry == /\ repaired /\ ~retained /\ reachable /\ ~nonvoter
         /\ retained' = TRUE
         /\ UNCHANGED <<repaired, reachable, nonvoter, observed, retired>>
(* Process restart does not erase delivered durable demotion. A reconciler
   forgets observations and can derive pending cleanup from registered identities. *)
Restart == /\ observed' = FALSE
           /\ UNCHANGED <<repaired, reachable, retained, nonvoter, retired>>
Next == Repair \/ Connect \/ Deliver \/ Observe \/ Prune \/ Retry \/ Restart
Spec == Init /\ [][Next]_vars
FairRepairSpec == Spec /\ WF_vars(Repair)
EventuallyRepaired == <>repaired
RetirementVerified == retired => nonvoter
RepairIndependent == ~repaired => ENABLED Repair
=============================================================================
