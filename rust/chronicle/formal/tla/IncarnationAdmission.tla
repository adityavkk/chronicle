----------------------- MODULE IncarnationAdmission -----------------------
EXTENDS Naturals
CONSTANT BadRefreshAtApply
VARIABLES incarnation, deleted, prepared, finished, safe
vars == <<incarnation, deleted, prepared, finished, safe>>
Init == /\ incarnation = 1 /\ deleted = FALSE /\ prepared = 0
        /\ finished = FALSE /\ safe = TRUE
(* An implicit HTTP operation resolves the current incarnation before proposing.
   An explicit request retains its caller-supplied incarnation. Raft supplies
   ordered committed application; this model does not implement consensus. *)
Admit(i) == /\ prepared = 0 /\ ~deleted /\ i \in {1, incarnation}
            /\ prepared' = i
            /\ UNCHANGED <<incarnation, deleted, finished, safe>>
Delete == /\ ~deleted /\ deleted' = TRUE
          /\ UNCHANGED <<incarnation, prepared, finished, safe>>
Recreate == /\ deleted /\ incarnation = 1
            /\ incarnation' = 2 /\ deleted' = FALSE
            /\ UNCHANGED <<prepared, finished, safe>>
Apply == /\ prepared # 0 /\ ~finished /\ finished' = TRUE
         /\ safe' = IF ~deleted /\ (BadRefreshAtApply \/ prepared = incarnation)
                    THEN prepared = incarnation ELSE safe
         /\ UNCHANGED <<incarnation, deleted, prepared>>
Next == (\E i \in 1..2: Admit(i)) \/ Delete \/ Recreate \/ Apply \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars
AdmittedIncarnationBound == safe
=============================================================================
