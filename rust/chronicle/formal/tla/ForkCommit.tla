----------------------------- MODULE ForkCommit -----------------------------
EXTENDS Naturals, TLC
CONSTANTS BadUnlock, BadPublish, BadRecovery
VARIABLES decision, source, tail, captured, target, prepared, retained,
          sourceUp, targetUp, observed
vars == <<decision,source,tail,captured,target,prepared,retained,
          sourceUp,targetUp,observed>>
(* Each transition is an already durable group apply, not a model of Raft.
   Unavailability includes process crash, loss of quorum, and network partition.
   No participant is permitted to infer abort from these conditions. *)
Init == /\ decision="idle" /\ source="live" /\ tail=0 /\ captured=0
        /\ target="absent" /\ prepared=FALSE /\ retained=FALSE
        /\ sourceUp=TRUE /\ targetUp=TRUE /\ observed="none"
Locked == decision="preparing" /\ ~BadUnlock
Append == /\ sourceUp /\ source="live" /\ ~Locked /\ tail<2
          /\ tail'=tail+1
          /\ UNCHANGED <<decision,source,captured,target,prepared,retained,
                          sourceUp,targetUp,observed>>
Begin == /\ sourceUp /\ source="live" /\ decision="idle"
         /\ decision'="preparing" /\ captured'=tail
         /\ UNCHANGED <<source,tail,target,prepared,retained,sourceUp,targetUp,observed>>
Prepare == /\ targetUp /\ decision="preparing" /\ target="absent"
           /\ target'=(IF BadPublish THEN "live" ELSE "prepared") /\ prepared'=TRUE
           /\ UNCHANGED <<decision,source,tail,captured,retained,sourceUp,targetUp,observed>>
Commit == /\ sourceUp /\ decision="preparing" /\ prepared /\ source="live"
          /\ decision'="commit" /\ retained'=TRUE
          /\ UNCHANGED <<source,tail,captured,target,prepared,sourceUp,targetUp,observed>>
Abort == /\ sourceUp /\ decision="preparing" /\ decision'="abort"
         /\ UNCHANGED <<source,tail,captured,target,prepared,retained,sourceUp,targetUp,observed>>
Finalize == /\ targetUp /\ sourceUp /\ target="prepared"
            /\ decision \in {"commit","abort"}
            /\ target'=IF decision="commit" THEN "live" ELSE "absent"
            /\ UNCHANGED <<decision,source,tail,captured,prepared,retained,sourceUp,targetUp,observed>>
DeleteSource == /\ sourceUp /\ source="live" /\ ~Locked
                /\ source'=IF retained THEN "soft" ELSE "gone"
                /\ UNCHANGED <<decision,tail,captured,target,prepared,retained,sourceUp,targetUp,observed>>
DeleteTarget == /\ targetUp /\ target="live" /\ target'="deleted"
                /\ UNCHANGED <<decision,source,tail,captured,prepared,retained,sourceUp,targetUp,observed>>
Release == /\ sourceUp /\ target="deleted" /\ retained
           /\ retained'=FALSE /\ source'=IF source="soft" THEN "gone" ELSE source
           /\ UNCHANGED <<decision,tail,captured,target,prepared,sourceUp,targetUp,observed>>
ReadTarget == /\ targetUp
              /\ observed'=CASE target="prepared" -> "unavailable"
                                [] target="live" -> "live"
                                [] OTHER -> "absent"
              /\ UNCHANGED <<decision,source,tail,captured,target,prepared,retained,sourceUp,targetUp>>
SourceFailure == /\ sourceUp'=~sourceUp
                 /\ UNCHANGED <<decision,source,tail,captured,target,prepared,retained,targetUp,observed>>
TargetFailure == /\ targetUp'=~targetUp
                 /\ target'=IF BadRecovery /\ target="prepared" THEN "absent" ELSE target
                 /\ UNCHANGED <<decision,source,tail,captured,prepared,retained,sourceUp,observed>>
Next == Append \/ Begin \/ Prepare \/ Commit \/ Abort \/ Finalize \/
        DeleteSource \/ DeleteTarget \/ Release \/ ReadTarget \/ SourceFailure \/ TargetFailure
Spec == Init /\ [][Next]_vars
PreparedTail == decision="preparing" => tail=captured
PublishedCommitted == target="live" => decision="commit" /\ retained
CommitProtected == decision="commit" /\ target#"deleted" => retained /\ source#"gone"
DecisionRecoverable == decision="commit" => target#"absent"
(* Separate liveness assumes eventual quorum/network availability and fairness. *)
LiveSpec == Spec /\ <>[](sourceUp /\ targetUp)
            /\ WF_vars(Prepare) /\ WF_vars(Commit) /\ WF_vars(Abort)
            /\ WF_vars(Finalize) /\ WF_vars(Release)
Settles == (decision="preparing") ~> (decision \in {"commit","abort"})
Publishes == (decision="commit" /\ target="prepared") ~> (target \in {"live","deleted"})
=============================================================================
