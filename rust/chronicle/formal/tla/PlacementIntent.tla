--------------------------- MODULE PlacementIntent ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS BadRepairLock, BadConcurrent, BadGeneration, BadHistory
Shards == {0, 1}
Nodes == {1, 2, 3, 4}
VoterSets == {v \in SUBSET Nodes : Cardinality(v) = 3}
MaxGeneration == 3
VARIABLES placements, assigned, completed
vars == <<placements, assigned, completed>>

(* Control-group apply only. Raft supplies durable serialized commands; the
   membership admission model separately covers delayed/cancelled data-group
   work. Initial placements have known history; legacy unknown history is
   conservatively expanded from the permanent registry in the implementation. *)
Init == /\ placements = [s \in Shards |->
            [generation |-> 1, voters |-> {1, 2, 3}, complete |-> TRUE,
             history |-> {1, 2, 3}]]
        /\ assigned = [s \in Shards |-> {1, 2, 3}]
        /\ completed = [s \in Shards |-> 1]

Place(s, expected, voters, repair) ==
    /\ placements[s].generation = expected
    /\ expected < MaxGeneration
    /\ \A other \in Shards :
          placements[other].complete \/
          (repair /\ ~BadRepairLock /\ (other = s \/ BadConcurrent))
    /\ placements' = [placements EXCEPT ![s] =
         [generation |-> expected + 1, voters |-> voters, complete |-> FALSE,
          history |-> IF BadHistory THEN voters ELSE @.history \cup voters]]
    /\ assigned' = [assigned EXCEPT ![s] = @ \cup voters]
    /\ UNCHANGED completed

(* This represents an applied target-membership response, not a read barrier.
   It may arrive late with the generation captured by a superseded controller. *)
Complete(s, generation) ==
    /\ generation <= placements[s].generation
    /\ (generation = placements[s].generation \/ BadGeneration)
    /\ placements' = [placements EXCEPT ![s].complete = TRUE]
    /\ completed' = [completed EXCEPT ![s] = generation]
    /\ UNCHANGED assigned

Next == (\E s \in Shards, expected \in 1..MaxGeneration,
             voters \in VoterSets, repair \in BOOLEAN : Place(s, expected, voters, repair))
        \/ (\E s \in Shards, generation \in 1..MaxGeneration : Complete(s, generation))
Spec == Init /\ [][Next]_vars
OneMovement == Cardinality({s \in Shards : ~placements[s].complete}) <= 1
HistoryRetained == \A s \in Shards : assigned[s] \subseteq placements[s].history
CompletionCurrent == \A s \in Shards : placements[s].complete =>
                         completed[s] = placements[s].generation
RepairEnabled == \A s \in Shards :
    (~placements[s].complete /\ placements[s].generation < MaxGeneration) =>
      ENABLED Place(s, placements[s].generation, {1, 2, 3}, TRUE)

(* Bounded intent generation eventually stops. Actual quorum/IO availability
   and controller fairness are assumptions, not inferred from health probes. *)
FairSpec == Spec /\ \A s \in Shards, g \in 1..MaxGeneration : WF_vars(Complete(s, g))
EventuallyComplete == <>[] (\A s \in Shards : placements[s].complete)
=============================================================================
