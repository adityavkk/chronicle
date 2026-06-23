------------------------------- MODULE MC_Anim -------------------------------
(* Spectacle/headless-render harness for the SubscriptionFence animation     *)
(* (#41 part B). 2 workers x 1 subscription, NO symmetry (named identities so *)
(* the SVG frames are legible and a shared trace is reproducible). Drives the *)
(* AnimView/AnimAlias of SubscriptionFence_anim. Used by `make spectacle-     *)
(* frames` to emit one SVG per state of a curated crash-window trace.         *)
EXTENDS SubscriptionFence_anim
=============================================================================
