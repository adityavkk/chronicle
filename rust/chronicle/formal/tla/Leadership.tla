---------------------------- MODULE Leadership ----------------------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS DuplicateClaim, LoseLedger, IgnoreBudget, IgnoreCooldown
VARIABLES id, phase, vote, generation, plannedVote, plannedGeneration,
          highWater, lastClaim, now, replies, permits, queued, grants, sent, effects
vars == <<id, phase, vote, generation, plannedVote, plannedGeneration,
          highWater, lastClaim, now, replies, permits, queued, grants, sent, effects>>

(* One shard, two attempts and two totally ordered committed votes. Raft
   serializes durable control apply. Replies and caller permits are volatile;
   grants/sent/effects are external history, not recoverable application state.
   Generation abstracts replica intent/membership admission, not a second lease.
   Model source/target are distinct eligible voters at admission. *)
Init == /\ id = 0 /\ phase = "closed" /\ vote = 0 /\ generation = 0
        /\ plannedVote = 0 /\ plannedGeneration = 0
        /\ highWater = -1 /\ lastClaim = -2 /\ now = 0
        /\ replies = {} /\ permits = {} /\ queued = <<>>
        /\ grants = <<>> /\ sent = <<>> /\ effects = <<>>
Plan == /\ phase = "closed" /\ id < 2
        /\ (IgnoreBudget \/ vote > highWater)
        /\ id' = id + 1 /\ phase' = "planned"
        /\ plannedVote' = vote /\ plannedGeneration' = generation
        /\ UNCHANGED <<vote, generation, highWater, lastClaim, now,
                       replies, permits, queued, grants, sent, effects>>
Claim == /\ (phase = "planned" \/ (DuplicateClaim /\ phase = "claimed"))
         /\ plannedGeneration = generation
         /\ (IgnoreBudget \/ DuplicateClaim \/ plannedVote > highWater)
         /\ (IgnoreCooldown \/ now >= lastClaim + 2)
         /\ Len(grants) < 3
         /\ phase' = "claimed" /\ highWater' = plannedVote /\ lastClaim' = now
         /\ replies' = replies \cup {id}
         /\ grants' = Append(grants, [attempt |-> id, source |-> plannedVote,
                                       at |-> now, previous |-> lastClaim])
         /\ UNCHANGED <<id, vote, generation, plannedVote, plannedGeneration,
                        now, permits, queued, sent, effects>>
Deliver(i) == /\ i \in replies /\ replies' = replies \ {i}
              /\ permits' = permits \cup {i}
              /\ UNCHANGED <<id, phase, vote, generation, plannedVote,
                             plannedGeneration, highWater, lastClaim, now,
                             queued, grants, sent, effects>>
Submit == /\ id \in permits /\ phase = "claimed"
          /\ plannedGeneration = generation /\ plannedVote = vote
          /\ permits' = permits \ {id}
          /\ queued' = Append(queued, [attempt |-> id, source |-> plannedVote,
                                       generation |-> plannedGeneration])
          /\ sent' = Append(sent, id)
          /\ UNCHANGED <<id, phase, vote, generation, plannedVote,
                         plannedGeneration, highWater, lastClaim, now,
                         replies, grants, effects>>
Handle == /\ Len(queued) > 0
          /\ effects' = Append(effects, [expected |-> Head(queued).source,
                 actual |-> vote, expectedGeneration |-> Head(queued).generation,
                 actualGeneration |-> generation])
          /\ queued' = Tail(queued)
          /\ UNCHANGED <<id, phase, vote, generation, plannedVote,
                         plannedGeneration, highWater, lastClaim, now,
                         replies, permits, grants, sent>>
Close == /\ phase # "closed" /\ phase' = "closed"
         /\ UNCHANGED <<id, vote, generation, plannedVote, plannedGeneration,
                        highWater, lastClaim, now, replies, permits,
                        queued, grants, sent, effects>>
Crash == /\ replies' = {} /\ permits' = {}
         /\ highWater' = IF LoseLedger THEN -1 ELSE highWater
         /\ phase' = IF LoseLedger /\ phase = "claimed" THEN "planned" ELSE phase
         /\ UNCHANGED <<id, vote, generation, plannedVote, plannedGeneration,
                        lastClaim, now, queued, grants, sent, effects>>
AdvanceVote == /\ vote = 0 /\ vote' = 1
               /\ UNCHANGED <<id, phase, generation, plannedVote, plannedGeneration,
                              highWater, lastClaim, now, replies, permits,
                              queued, grants, sent, effects>>
Repair == /\ generation = 0 /\ generation' = 1
          /\ UNCHANGED <<id, phase, vote, plannedVote, plannedGeneration,
                         highWater, lastClaim, now, replies, permits,
                         queued, grants, sent, effects>>
Tick == /\ now < 4 /\ now' = now + 1
        /\ UNCHANGED <<id, phase, vote, generation, plannedVote, plannedGeneration,
                       highWater, lastClaim, replies, permits, queued, grants,
                       sent, effects>>
Next == Plan \/ Claim \/ Submit \/ Handle \/ Close \/ Crash \/ AdvanceVote
        \/ Repair \/ Tick \/ (\E i \in 1..2 : Deliver(i))
Spec == Init /\ [][Next]_vars
OneGrant == \A i, j \in DOMAIN grants :
            grants[i].attempt = grants[j].attempt => i = j
VoteBudget == \A i, j \in DOMAIN grants :
              i < j => grants[i].source < grants[j].source
Cooldown == \A i \in DOMAIN grants : grants[i].at >= grants[i].previous + 2
OneSubmission == \A i, j \in DOMAIN sent : sent[i] = sent[j] => i = j
(* Deliberately FALSE for the faithful API: enqueue does not atomically bind
   the eventual upstream trigger to its observed vote or membership. *)
NoStaleExecution == \A i \in DOMAIN effects :
                   effects[i].expected = effects[i].actual /\
                   effects[i].expectedGeneration = effects[i].actualGeneration
=============================================================================
