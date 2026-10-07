----------------------------- MODULE Forks -----------------------------
EXTENDS Naturals, TLC
CONSTANTS PartialPublish, EarlyRelease, FalseAbsence, ForgetGrant
VARIABLES decision, destination, copied, pinned, sourceAlive, sourcePresent,
          descendants, everGranted, observedAbsent, published
vars == <<decision, destination, copied, pinned, sourceAlive, sourcePresent,
          descendants, everGranted, observedAbsent, published>>
Init == /\ decision = "undecided" /\ destination = "reserved" /\ copied = 0
        /\ pinned = FALSE /\ sourceAlive = TRUE /\ sourcePresent = TRUE
        /\ descendants = 0 /\ everGranted = FALSE /\ observedAbsent = FALSE
        /\ published = FALSE
Grant == /\ decision = "undecided" /\ sourceAlive
         /\ decision' = "granted" /\ pinned' = TRUE /\ everGranted' = TRUE
         /\ UNCHANGED <<destination, copied, sourceAlive, sourcePresent,
                        descendants, observedAbsent, published>>
Abort == /\ decision = "undecided" /\ decision' = "aborted"
         /\ UNCHANGED <<destination, copied, pinned, sourceAlive, sourcePresent,
                        descendants, everGranted, observedAbsent, published>>
ResolveAbort == /\ decision = "aborted" /\ destination = "reserved"
                /\ destination' = "absent"
                /\ UNCHANGED <<decision, copied, pinned, sourceAlive, sourcePresent,
                               descendants, everGranted, observedAbsent, published>>
Copy == /\ decision = "granted" /\ destination = "reserved"
        /\ sourcePresent /\ copied < 2 /\ copied' = copied + 1
        /\ UNCHANGED <<decision, destination, pinned, sourceAlive, sourcePresent,
                       descendants, everGranted, observedAbsent, published>>
Publish == /\ decision = "granted" /\ destination = "reserved"
           /\ (copied = 2 \/ PartialPublish)
           /\ destination' = "visible" /\ published' = TRUE
           /\ UNCHANGED <<decision, copied, pinned, sourceAlive, sourcePresent,
                          descendants, everGranted, observedAbsent>>
ForkChild == /\ destination = "visible" /\ descendants = 0
             /\ descendants' = 1
             /\ UNCHANGED <<decision, destination, copied, pinned, sourceAlive,
                            sourcePresent, everGranted, observedAbsent, published>>
DeleteChild == /\ destination = "visible"
               /\ destination' = IF descendants = 0 THEN "retired" ELSE "soft"
               /\ UNCHANGED <<decision, copied, pinned, sourceAlive, sourcePresent,
                              descendants, everGranted, observedAbsent, published>>
DeleteDescendant == /\ descendants = 1 /\ descendants' = 0
                    /\ destination' = IF destination = "soft" THEN "retired" ELSE destination
                    /\ UNCHANGED <<decision, copied, pinned, sourceAlive, sourcePresent,
                                   everGranted, observedAbsent, published>>
Release == /\ decision = "granted"
           /\ (destination = "retired" \/ EarlyRelease)
           /\ decision' = "released" /\ pinned' = FALSE
           /\ UNCHANGED <<destination, copied, sourceAlive, sourcePresent,
                          descendants, everGranted, observedAbsent, published>>
DeleteSource == /\ sourceAlive /\ sourceAlive' = FALSE
                /\ sourcePresent' = pinned
                /\ UNCHANGED <<decision, destination, copied, pinned, descendants,
                               everGranted, observedAbsent, published>>
CollectSource == /\ ~sourceAlive /\ ~pinned /\ sourcePresent' = FALSE
                 /\ UNCHANGED <<decision, destination, copied, pinned, sourceAlive,
                                descendants, everGranted, observedAbsent, published>>
Read == /\ destination = "reserved" /\ everGranted
        /\ observedAbsent' = FalseAbsence
        /\ UNCHANGED <<decision, destination, copied, pinned, sourceAlive,
                       sourcePresent, descendants, everGranted, published>>
Crash == /\ pinned' = IF ForgetGrant THEN FALSE ELSE pinned
         /\ UNCHANGED <<decision, destination, copied, sourceAlive, sourcePresent,
                        descendants, everGranted, observedAbsent, published>>
Next == Grant \/ Abort \/ ResolveAbort \/ Copy \/ Publish \/ ForkChild
        \/ DeleteChild \/ DeleteDescendant \/ Release \/ DeleteSource
        \/ CollectSource \/ Read \/ Crash
Spec == Init /\ [][Next]_vars
Safe == /\ ~observedAbsent
        /\ (published => copied = 2)
        /\ (decision = "granted" => pinned /\ sourcePresent)
        /\ (everGranted => decision \in {"granted", "released"})
        /\ (decision = "released" => destination = "retired" /\ descendants = 0)
LiveNext == Grant \/ Copy \/ Publish \/ DeleteChild \/ Release \/ Crash
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Grant) /\ WF_vars(Copy)
            /\ WF_vars(Publish) /\ WF_vars(DeleteChild) /\ WF_vars(Release)
Progress == <> (published /\ decision = "released")
=======================================================================
