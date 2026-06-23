--------------------------- MODULE MC_Membership ---------------------------
(* TLC harness for the Membership convergence spec (issue #40).             *)
(* The HRW Score is pinned to a concrete distinct-per-(replica,slot)        *)
(* assignment so TLC has a single deterministic argmax per slot; the        *)
(* convergence property is independent of WHICH replica wins each slot, so  *)
(* one representative score table is sufficient (and far smaller than        *)
(* enumerating all score functions).                                        *)
EXTENDS Membership

\* Concrete distinct score table over the model's replicas/slots. Pinned in
\* the harness (not the .cfg) because a function-valued CONSTANT is easiest
\* to give as a TLA+ definition. r1 wins h1, r2 wins h2 at the top, but every
\* (r,h) score is distinct so HRWOwner is a clean argmax under any live subset.
MCReplicas == {"r1", "r2", "r3"}
MCSlots == {"h1", "h2"}

\* Distinct scores: encode as 10*slotIdx + replicaIdx variations so no two
\* replicas tie on a slot. Given as an explicit function.
MCScore ==
    [ r \in MCReplicas |->
        [ h \in MCSlots |->
            CASE r = "r1" /\ h = "h1" -> 9
              [] r = "r2" /\ h = "h1" -> 5
              [] r = "r3" /\ h = "h1" -> 1
              [] r = "r1" /\ h = "h2" -> 2
              [] r = "r2" /\ h = "h2" -> 8
              [] r = "r3" /\ h = "h2" -> 6
              [] OTHER -> 0 ] ]

=============================================================================
