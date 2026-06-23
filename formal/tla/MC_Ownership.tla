--------------------------- MODULE MC_Ownership ----------------------------
(* TLC harness for the standalone Ownership owner-epoch CAS module (#38).     *)
(* Replicas and Slots are declared as symmetry sets so TLC quotients the      *)
(* state space by permutations of the interchangeable identities.            *)
EXTENDS Ownership

OSym == Permutations(Replicas) \cup Permutations(Slots)
=============================================================================
