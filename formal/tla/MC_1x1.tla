------------------------------- MODULE MC_1x1 -------------------------------
(* TLC harness for the 1 subscription x 1 worker fast lane (smoke / CI).      *)
(* Workers and Subs are declared as symmetry sets so TLC quotients the state  *)
(* space by permutations of the (interchangeable) worker/sub identities.      *)
EXTENDS SubscriptionFence

Sym == Permutations(Workers) \cup Permutations(Subs)
=============================================================================
