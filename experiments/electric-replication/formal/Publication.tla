------------------------- MODULE Publication -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Nodes, MaxEntry, EarlyFlush, EarlyPublish, LocalAck
VARIABLES staged, stable, applied, snapshots, committed, acknowledged, up
vars == <<staged, stable, applied, snapshots, committed, acknowledged, up>>
Quorum(S) == Cardinality(S) * 2 > Cardinality(Nodes)
Min(a,b) == IF a < b THEN a ELSE b

Init == /\ staged = [n \in Nodes |-> 0]
        /\ stable = staged /\ applied = staged /\ snapshots = staged
        /\ committed = 0 /\ acknowledged = 0 /\ up = Nodes

\* Raft's matching-log prefix and leader-completeness are ASSUMED here.
\* Numbers denote a common history; this model checks its durability bridge.
Stage(n) == /\ n \in up /\ staged[n] < MaxEntry
            /\ staged' = [staged EXCEPT ![n] = @ + 1]
            /\ UNCHANGED <<stable, applied, snapshots, committed, acknowledged, up>>
Flush(n) == /\ n \in up /\ stable[n] < staged[n]
            /\ stable' = [stable EXCEPT ![n] = staged[n]]
            /\ UNCHANGED <<staged, applied, snapshots, committed, acknowledged, up>>
Commit == /\ committed < MaxEntry
          /\ Quorum({n \in Nodes: (IF EarlyFlush THEN staged[n] ELSE stable[n]) > committed})
          /\ committed' = committed + 1
          /\ UNCHANGED <<staged, stable, applied, snapshots, acknowledged, up>>
Publish(n) ==
    LET bound == IF EarlyPublish THEN staged[n] ELSE Min(stable[n], committed)
    IN /\ n \in up /\ applied[n] < bound
       /\ applied' = [applied EXCEPT ![n] = bound]
       /\ UNCHANGED <<staged, stable, snapshots, committed, acknowledged, up>>
Ack(n) == /\ n \in up
          /\ acknowledged < (IF LocalAck THEN stable[n] ELSE applied[n])
          /\ acknowledged' = IF LocalAck THEN stable[n] ELSE applied[n]
          /\ UNCHANGED <<staged, stable, applied, snapshots, committed, up>>
Snapshot(n) == /\ n \in up /\ snapshots[n] < applied[n]
               /\ snapshots' = [snapshots EXCEPT ![n] = applied[n]]
               /\ UNCHANGED <<staged, stable, applied, committed, acknowledged, up>>
Crash(n) == /\ n \in up /\ up' = up \ {n}
            /\ staged' = [staged EXCEPT ![n] = stable[n]]
            /\ applied' = [applied EXCEPT ![n] = snapshots[n]]
            /\ UNCHANGED <<stable, snapshots, committed, acknowledged>>
Recover(n) == /\ n \notin up /\ up' = up \cup {n}
              /\ applied' = [applied EXCEPT ![n] = Min(stable[n], committed)]
              /\ UNCHANGED <<staged, stable, snapshots, committed, acknowledged>>
Next == Commit \/ \E n \in Nodes:
        Stage(n) \/ Flush(n) \/ Publish(n) \/ Ack(n) \/ Snapshot(n) \/ Crash(n) \/ Recover(n)
SafetySpec == Init /\ [][Next]_vars
Safe == /\ acknowledged <= committed
        /\ \A n \in Nodes: snapshots[n] <= applied[n] /\ applied[n] <= committed
        /\ Quorum({n \in Nodes: stable[n] >= committed})
\* Liveness only in the explicitly stable network/disk period. No crashes,
\* sufficient resources, every replica can receive this matching finite prefix.
LiveNext == Commit \/ \E n \in Nodes: Stage(n) \/ Flush(n) \/ Publish(n) \/ Ack(n)
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Commit)
            /\ \A n \in Nodes: WF_vars(Stage(n)) /\ WF_vars(Flush(n))
                               /\ WF_vars(Publish(n)) /\ WF_vars(Ack(n))
Progress == <> (acknowledged = MaxEntry)
=====================================================================
