----------------------------- MODULE Links -----------------------------
EXTENDS Naturals, TLC
CONSTANTS StaleObservation, OldIncarnationAck, DiscoverAtTail
VARIABLES inc, alive, tail, serial, packets, seen, acked, authorized, claim, regressed
vars == <<inc, alive, tail, serial, packets, seen, acked, authorized, claim, regressed>>
Max(a,b) == IF a > b THEN a ELSE b
Min(a,b) == IF a < b THEN a ELSE b
Current == [inc |-> IF alive THEN inc ELSE 0, tail |-> tail, index |-> serial]
Empty == [inc |-> 0, tail |-> 0, index |-> 0]
Init == /\ inc = 1 /\ alive = TRUE /\ tail = 1 /\ serial = 1
        /\ packets = {} /\ seen = Empty /\ acked = 0 /\ authorized = 0
        /\ claim = Empty /\ regressed = FALSE
Append == /\ alive /\ tail < 2 /\ serial < 5
          /\ tail' = tail + 1 /\ serial' = serial + 1
          /\ UNCHANGED <<inc, alive, packets, seen, acked, authorized, claim, regressed>>
Delete == /\ alive /\ serial < 5 /\ alive' = FALSE
          /\ tail' = 0 /\ serial' = serial + 1
          /\ UNCHANGED <<inc, packets, seen, acked, authorized, claim, regressed>>
Recreate == /\ ~alive /\ inc = 1 /\ serial < 5
            /\ alive' = TRUE /\ inc' = 2 /\ tail' = 1 /\ serial' = serial + 1
            /\ UNCHANGED <<packets, seen, acked, authorized, claim, regressed>>
Capture == /\ packets' = packets \cup {Current}
           /\ UNCHANGED <<inc, alive, tail, serial, seen, acked, authorized, claim, regressed>>
Observe(p) == /\ p \in packets /\ (p.index >= seen.index \/ StaleObservation)
              /\ seen' = p /\ regressed' = (regressed \/ p.index < seen.index)
              /\ acked' = IF p.inc = seen.inc THEN acked
                           ELSE IF DiscoverAtTail THEN p.tail ELSE 0
              /\ authorized' = IF p.inc = seen.inc THEN authorized ELSE 0
              /\ UNCHANGED <<inc, alive, tail, serial, packets, claim>>
Claim == /\ seen.inc # 0 /\ claim' = seen
         /\ UNCHANGED <<inc, alive, tail, serial, packets, seen, acked, authorized, regressed>>
Ack == /\ claim.inc # 0 /\ seen.inc # 0
       /\ (claim.inc = seen.inc \/ OldIncarnationAck)
       /\ acked' = Max(acked, Min(claim.tail, seen.tail))
       /\ authorized' = IF claim.inc = seen.inc THEN Max(authorized, claim.tail) ELSE authorized
       /\ UNCHANGED <<inc, alive, tail, serial, packets, seen, claim, regressed>>
Crash == UNCHANGED vars
Next == Append \/ Delete \/ Recreate \/ Capture \/ Claim \/ Ack \/ Crash
        \/ \E p \in packets: Observe(p)
Spec == Init /\ [][Next]_vars
Safe == /\ ~regressed /\ acked <= authorized /\ authorized <= seen.tail
\* Notifications may be lost forever: fair catalog repair suffices in a stable period.
ObserveLatest == Observe(Current)
LiveNext == Capture \/ ObserveLatest \/ Claim \/ Ack \/ Crash
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Capture)
            /\ WF_vars(ObserveLatest) /\ WF_vars(Claim) /\ WF_vars(Ack)
Progress == <> (acked = 1)
========================================================================
