--------------------------- MODULE MC_Composed -----------------------------
(* TLC harness for the Composed (SubscriptionFence x Ownership) spec (#38).   *)
(*                                                                          *)
(* NOTE: we do NOT declare a SYMMETRY set here.  In the composed model a      *)
(* worker identity is ALSO a slot-owner identity (slot[h].owner ranges over   *)
(* Workers), and the owner-epoch guard PassesOwner compares a worker against  *)
(* slot[h].owner.  Permuting Workers while the owner field names a concrete   *)
(* worker is still sound, but to keep the layering proof's witness traces     *)
(* legible (and to avoid any symmetry-vs-named-constant subtlety in the       *)
(* owner-scope quantifier) we run the full state space without quotienting.   *)
(* The bounds are chosen so the un-quotiented run still terminates fast.      *)
EXTENDS Composed
=============================================================================
