-------------------------- MODULE ResourceBalance --------------------------
EXTENDS Naturals, FiniteSets, Sequences
CONSTANTS BadCooldown, BadGeneration
VARIABLES generation, lastMove, now, pending, proposals, accepted
vars == <<generation, lastMove, now, pending, proposals, accepted>>

(* Only placement admission is modeled. Consensus serializes apply; existing
   PlacementIntent/MembershipAdmission models cover catch-up and joint consensus.
   Two controllers can hold independently stale resource/placement observations.
   Resources are preferences, never authority to bypass generation or cooldown. *)
Init == /\ generation = 1 /\ lastMove = 0 /\ now = 0
        /\ pending = FALSE /\ proposals = {} /\ accepted = <<>>
Observe == /\ proposals' = proposals \cup {generation}
           /\ UNCHANGED <<generation, lastMove, now, pending, accepted>>
Tick == /\ now < 6 /\ now' = now + 1
        /\ UNCHANGED <<generation, lastMove, pending, proposals, accepted>>
Move(g) == /\ g \in proposals /\ ~pending /\ generation < 4
           /\ (BadGeneration \/ g = generation)
           /\ (BadCooldown \/ now >= lastMove + 2)
           /\ accepted' = Append(accepted,
                [at |-> now, previous |-> lastMove, expected |-> g,
                 actual |-> generation])
           /\ generation' = generation + 1 /\ lastMove' = now
           /\ pending' = TRUE /\ UNCHANGED <<now, proposals>>
Finish == /\ pending /\ pending' = FALSE
          /\ UNCHANGED <<generation, lastMove, now, proposals, accepted>>
Next == Observe \/ Tick \/ Finish \/ (\E g \in proposals : Move(g))
Spec == Init /\ [][Next]_vars
Cooldown == \A i \in DOMAIN accepted : accepted[i].at >= accepted[i].previous + 2
Current == \A i \in DOMAIN accepted : accepted[i].expected = accepted[i].actual
=============================================================================
