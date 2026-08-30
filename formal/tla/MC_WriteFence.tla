---------------------------- MODULE MC_WriteFence ----------------------------
(* TLC harness for the WriteFence lanes (issue #183): the 1 authority x 2     *)
(* worker PR gate, the 2 authority x 2 worker nightly run, the fault-         *)
(* injection configs and the W5-W10 coverage witnesses (W7 and W9 are step    *)
(* witnesses checked as action properties). Workers and Auths are declared as *)
(* symmetry sets so TLC quotients the state space by permutations of the      *)
(* interchangeable claimant / authority identities; on a singleton Auths TLC  *)
(* warns ("contains less than two elements") and quotients by Workers alone,  *)
(* exactly as MC_1x1 does for the subscription fence.                         *)
EXTENDS WriteFence

Sym == Permutations(Workers) \cup Permutations(Auths)
=============================================================================
