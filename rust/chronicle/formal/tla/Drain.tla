------------------------------- MODULE Drain -------------------------------
CONSTANT BadLatePlacement
VARIABLES cached, draining, pending, assigned, retired
vars == <<cached, draining, pending, assigned, retired>>
Init == /\ cached = FALSE /\ draining = FALSE /\ pending = FALSE
        /\ assigned = FALSE /\ retired = FALSE
Observe == /\ ~draining /\ cached' = TRUE
           /\ UNCHANGED <<draining, pending, assigned, retired>>
Drain == /\ draining' = TRUE
         /\ UNCHANGED <<cached, pending, assigned, retired>>
(* A cached health observation never overrides eligibility at control-log apply. *)
Place == /\ cached /\ (~draining \/ BadLatePlacement) /\ pending' = TRUE
         /\ UNCHANGED <<cached, draining, assigned, retired>>
Complete == /\ pending /\ assigned' = TRUE /\ pending' = FALSE
            /\ UNCHANGED <<cached, draining, retired>>
Retire == /\ draining /\ ~pending /\ ~assigned /\ retired' = TRUE
          /\ UNCHANGED <<cached, draining, pending, assigned>>
Next == Observe \/ Drain \/ Place \/ Complete \/ Retire
Spec == Init /\ [][Next]_vars
DrainFenced == retired => (~pending /\ ~assigned)
=============================================================================
