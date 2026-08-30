---------------------------- MODULE MC_WriteFence ----------------------------
(* TLC harness for the WriteFence lanes (issue #183): the 1 authority x 2     *)
(* worker PR gate, the 2 authority x 2 worker nightly seal-isolation run, the *)
(* four fault-injection configs and the W5-W8 coverage witnesses. Workers and *)
(* Auths are declared as symmetry sets so TLC quotients the state space by    *)
(* permutations of the interchangeable claimant / authority identities.       *)
EXTENDS WriteFence

Sym == Permutations(Workers) \cup Permutations(Auths)
=============================================================================
