--------------------------- MODULE MC_Liveness -----------------------------
(* TLC harness for the Liveness / fairness module (#38).  No SYMMETRY: the    *)
(* temporal leads-to properties and the per-worker WF fairness conditions     *)
(* name concrete workers, so we run the (small) state space un-quotiented.    *)
EXTENDS Liveness
=============================================================================
