----------------------------- MODULE Retirement -----------------------------
EXTENDS Naturals
CONSTANTS BadCompletion, BadFreshness
VARIABLES repaired, reachable, retained, version, observed, retired
vars == <<repaired, reachable, retained, version, observed, retired>>

(* Raft supplies committed uniform replacement and durable membership delivery.
   This models reconciliation, not the consensus algorithm. An unreachable old
   replica is not required for repair when the applicable quorums are available. *)
Init == /\ repaired = FALSE /\ reachable = FALSE /\ retained = FALSE
        /\ version = 0 /\ observed = 3 /\ retired = FALSE
(* 0 is an earlier learner configuration; promotion 1 can still be in flight.
   Only committed demotion boundary 2 certifies this cleanup episode.
   Observation 3 means no observation. *)
Repair == /\ ~repaired /\ repaired' = TRUE /\ retained' = TRUE
          /\ UNCHANGED <<reachable, version, observed, retired>>
Connect == /\ reachable' = ~reachable
           /\ observed' = 3
           /\ UNCHANGED <<repaired, retained, version, retired>>
LatePromotion == /\ reachable /\ version = 0 /\ version' = 1
                 /\ UNCHANGED <<repaired, reachable, retained, observed, retired>>
Deliver == /\ repaired /\ retained /\ reachable /\ version' = 2
           /\ UNCHANGED <<repaired, reachable, retained, observed, retired>>
Observe == /\ reachable /\ observed' = version
           /\ UNCHANGED <<repaired, reachable, retained, version, retired>>
Prune == /\ repaired /\ retained
         /\ (observed = 2 \/ ~reachable \/ (BadFreshness /\ observed = 0))
         /\ retained' = FALSE
         /\ retired' = (BadCompletion \/ observed = 2 \/ (BadFreshness /\ observed = 0))
         /\ UNCHANGED <<repaired, reachable, version, observed>>
Retry == /\ repaired /\ ~retained /\ reachable /\ version < 2
         /\ retained' = TRUE
         /\ UNCHANGED <<repaired, reachable, version, observed, retired>>
(* Process restart does not erase delivered durable demotion. A reconciler
   forgets observations and can derive pending cleanup from registered identities. *)
Restart == /\ observed' = 3
           /\ UNCHANGED <<repaired, reachable, retained, version, retired>>
Next == Repair \/ Connect \/ LatePromotion \/ Deliver \/ Observe \/ Prune \/ Retry \/ Restart
Spec == Init /\ [][Next]_vars
FairRepairSpec == Spec /\ WF_vars(Repair)
EventuallyRepaired == <>repaired
RetirementVerified == retired => version = 2
RepairIndependent == ~repaired => ENABLED Repair
=============================================================================
