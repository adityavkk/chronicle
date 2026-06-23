------------------------------- MODULE MC_2x2 -------------------------------
(* TLC harness for the 2 subscription x 2 worker exhaustive interleaving.     *)
(* This config exercises rotate-on-expired-takeover and the deposed-holder    *)
(* late-ack race.  Workers and Subs are declared as symmetry sets so TLC      *)
(* quotients the state space by permutations of the interchangeable           *)
(* worker/sub identities.                                                     *)
EXTENDS SubscriptionFence

Sym == Permutations(Workers) \cup Permutations(Subs)
=============================================================================
