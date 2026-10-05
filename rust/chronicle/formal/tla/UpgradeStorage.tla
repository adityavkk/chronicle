-------------------------- MODULE UpgradeStorage --------------------------
EXTENDS Integers, FiniteSets
CONSTANTS EarlyFlush, EarlyReply, InclusiveTruncate, SplitSnapshot
VARIABLES last, pending, disk, notified, committed, applied, replies,
          snapshotMeta, snapshotData
vars == <<last, pending, disk, notified, committed, applied, replies,
          snapshotMeta, snapshotData>>
Prefix(i) == 0..i
Init == /\ last = -1 /\ pending = -1
        /\ disk = {} /\ notified = {} /\ committed = {}
        /\ applied = {} /\ replies = {}
        /\ snapshotMeta = -1 /\ snapshotData = -1
Append == /\ pending = -1 /\ last < 2
          /\ last' = last + 1 /\ pending' = last + 1
          /\ UNCHANGED <<disk, notified, committed, applied, replies,
                         snapshotMeta, snapshotData>>
Persist == /\ pending >= 0 /\ disk' = disk \cup {pending}
           /\ UNCHANGED <<last, pending, notified, committed, applied, replies,
                          snapshotMeta, snapshotData>>
Notify == /\ pending >= 0 /\ (EarlyFlush \/ pending \in disk)
          /\ notified' = notified \cup {pending} /\ pending' = -1
          /\ UNCHANGED <<last, disk, committed, applied, replies,
                         snapshotMeta, snapshotData>>
Commit(i) == /\ i \in notified /\ Prefix(i) \subseteq disk
             /\ committed' = committed \cup Prefix(i)
             /\ UNCHANGED <<last, pending, disk, notified, applied, replies,
                            snapshotMeta, snapshotData>>
Apply(i) == /\ i \in committed /\ applied' = applied \cup Prefix(i)
            /\ UNCHANGED <<last, pending, disk, notified, committed, replies,
                           snapshotMeta, snapshotData>>
Reply(i) == /\ i \in committed /\ (EarlyReply \/ i \in applied)
            /\ replies' = replies \cup {i}
            /\ UNCHANGED <<last, pending, disk, notified, committed, applied,
                           snapshotMeta, snapshotData>>
Truncate(b) == /\ b \in -1..last /\ pending = -1
               /\ committed \subseteq Prefix(b)
               /\ disk' = {i \in disk : IF InclusiveTruncate THEN i < b ELSE i <= b}
               /\ notified' = notified \cap Prefix(b) /\ last' = b
               /\ UNCHANGED <<pending, committed, applied, replies,
                              snapshotMeta, snapshotData>>
Install(i) == /\ i \in applied /\ i >= snapshotMeta
              /\ snapshotMeta' = i
              /\ snapshotData' = IF SplitSnapshot THEN snapshotData ELSE i
              /\ UNCHANGED <<last, pending, disk, notified, committed, applied, replies>>
Crash == /\ pending' = -1
         /\ last' = CHOOSE i \in disk \cup {-1} : \A j \in disk : j <= i
         /\ UNCHANGED <<disk, notified, committed, applied, replies,
                        snapshotMeta, snapshotData>>
Next == Append \/ Persist \/ Notify \/ Crash
        \/ (\E i \in 0..2 : Commit(i) \/ Apply(i) \/ Reply(i) \/ Install(i))
        \/ (\E b \in -1..2 : Truncate(b))
Spec == Init /\ [][Next]_vars
FlushDurable == notified \subseteq disk
ReplyDurable == replies \subseteq applied
CommittedRetained == committed \subseteq disk
SnapshotAtomic == snapshotMeta = snapshotData
=============================================================================
