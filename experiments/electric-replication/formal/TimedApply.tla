--------------------------- MODULE TimedApply ---------------------------
EXTENDS Naturals, Sequences, TLC
CONSTANT LocalClock
VARIABLES commands, applied, clock, lastAccess, present, replies
vars == <<commands, applied, clock, lastAccess, present, replies>>
Nodes == {1, 2}
Max(a,b) == IF a > b THEN a ELSE b
Init == /\ commands = <<>>
        /\ applied = [n \in Nodes |-> 0]
        /\ clock = applied /\ lastAccess = applied
        /\ present = [n \in Nodes |-> TRUE]
        /\ replies = [n \in Nodes |-> <<>>]
\* The leader's wall-clock sample is DATA in the committed command. Applying
\* max(previous, sample) prevents time reversal after clock/leader changes.
Propose == /\ Len(commands) < 3
           /\ \E t \in 0..3, touch \in BOOLEAN:
                 commands' = Append(commands, [time |-> t, touch |-> touch])
           /\ UNCHANGED <<applied, clock, lastAccess, present, replies>>
Apply(n) ==
 LET c == commands[applied[n]+1]
     t == Max(clock[n], c.time + (IF LocalClock /\ n = 2 THEN 1 ELSE 0))
     exists == present[n] /\ t <= lastAccess[n] + 1
 IN /\ applied[n] < Len(commands)
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ clock' = [clock EXCEPT ![n] = t]
    /\ lastAccess' = [lastAccess EXCEPT ![n] = IF exists /\ c.touch THEN t ELSE @]
    /\ present' = [present EXCEPT ![n] = exists]
    /\ replies' = [replies EXCEPT ![n] = Append(@, exists)]
    /\ UNCHANGED commands
Next == Propose \/ \E n \in Nodes: Apply(n)
Spec == Init /\ [][Next]_vars
SamePrefix == applied[1] = applied[2] =>
  /\ clock[1] = clock[2] /\ lastAccess[1] = lastAccess[2]
  /\ present[1] = present[2] /\ replies[1] = replies[2]
=======================================================================
