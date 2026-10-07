------------------------ MODULE Subscriptions ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS StaleWorker, LatestTailAck, ForgetIntent
VARIABLES inc, alive, tail, acked, gen, snapshot, held, deadline, now,
          intent, packets, authorized, staleAccepted
vars == <<inc, alive, tail, acked, gen, snapshot, held, deadline, now,
          intent, packets, authorized, staleAccepted>>
Max(a,b) == IF a > b THEN a ELSE b
Init == /\ inc = 1 /\ alive = TRUE /\ tail = 0 /\ acked = 0
        /\ gen = 0 /\ snapshot = 0 /\ held = FALSE /\ deadline = 0
        /\ now = 0 /\ intent = "none" /\ packets = {}
        /\ authorized = 0 /\ staleAccepted = FALSE
Append == /\ tail < 2 /\ tail' = tail + 1
          /\ UNCHANGED <<inc, alive, acked, gen, snapshot, held, deadline,
                         now, intent, packets, authorized, staleAccepted>>
Wake == /\ alive /\ ~held /\ tail > acked /\ gen < 2 /\ now < 2
        /\ gen' = gen + 1 /\ snapshot' = tail /\ held' = TRUE
        /\ deadline' = now + 1 /\ intent' = "pending"
        /\ UNCHANGED <<inc, alive, tail, acked, now, packets, authorized, staleAccepted>>
Deliver == /\ held /\ intent = "pending"
           /\ packets' = packets \cup {[inc |-> inc, gen |-> gen, offset |-> snapshot]}
           /\ intent' = "delivered"
           /\ UNCHANGED <<inc, alive, tail, acked, gen, snapshot, held,
                          deadline, now, authorized, staleAccepted>>
Ack(p) ==
 LET valid == alive /\ held /\ now < deadline /\ p.inc = inc /\ p.gen = gen
 IN /\ p \in packets /\ (valid \/ StaleWorker)
    /\ acked' = Max(acked, IF LatestTailAck THEN tail ELSE p.offset)
    /\ authorized' = Max(authorized, p.offset)
    /\ staleAccepted' = (staleAccepted \/ ~valid)
    /\ held' = FALSE /\ intent' = "none"
    /\ UNCHANGED <<inc, alive, tail, gen, snapshot, deadline, now, packets>>
Tick == /\ now < 2 /\ now' = now + 1
        /\ UNCHANGED <<inc, alive, tail, acked, gen, snapshot, held, deadline,
                       intent, packets, authorized, staleAccepted>>
Expire == /\ held /\ now >= deadline /\ held' = FALSE /\ intent' = "none"
          /\ UNCHANGED <<inc, alive, tail, acked, gen, snapshot, deadline,
                         now, packets, authorized, staleAccepted>>
Delete == /\ alive /\ alive' = FALSE /\ held' = FALSE /\ intent' = "none"
          /\ UNCHANGED <<inc, tail, acked, gen, snapshot, deadline, now,
                         packets, authorized, staleAccepted>>
Recreate == /\ ~alive /\ inc = 1 /\ inc' = 2 /\ alive' = TRUE
            /\ gen' = 0 /\ acked' = 0 /\ authorized' = 0
            /\ UNCHANGED <<tail, snapshot, held, deadline, now, intent,
                           packets, staleAccepted>>
Crash == /\ intent' = IF ForgetIntent THEN "none" ELSE intent
         /\ UNCHANGED <<inc, alive, tail, acked, gen, snapshot, held, deadline,
                        now, packets, authorized, staleAccepted>>
Next == Append \/ Wake \/ Deliver \/ Tick \/ Expire \/ Delete \/ Recreate
        \/ Crash \/ \E p \in packets: Ack(p)
Spec == Init /\ [][Next]_vars
Safe == /\ ~staleAccepted /\ acked <= authorized /\ authorized <= tail
        /\ (held => intent # "none")
\* Stable period: no lease expiry/crashes, finite writes, cooperative worker.
AnyAck == \E p \in packets: Ack(p)
LiveNext == Append \/ Wake \/ Deliver \/ AnyAck
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Append) /\ WF_vars(Wake)
            /\ WF_vars(Deliver) /\ WF_vars(AnyAck)
Progress == <> (acked = 2)
=====================================================================
