---------------------------- MODULE Receipts ----------------------------
EXTENDS Naturals, TLC
CONSTANTS EarlyReceipt, PublishAccepted, IndexOnly, InvalidateAbsent,
          AcceptMeansSuccess, ReceiptSession
VARIABLES offered, originalOK, logID, durableID, originalFlushed, committedID,
          appliedID, cacheID, retained, accepted, visible, answer, session
vars == <<offered,originalOK,logID,durableID,originalFlushed,committedID,
          appliedID,cacheID,retained,accepted,visible,answer,session>>

\* One numeric log position, two distinct full term/leader identities.
\* Mature consensus supplies durable-quorum commitment and no replacement of
\* committed entries. ApplyRecovery supplies the pre-publication marker/replay.
Valid(id) == id = 2 \/ (id = 1 /\ originalOK)
Init == /\ offered = FALSE /\ originalOK \in BOOLEAN
        /\ logID = 0 /\ durableID = 0 /\ originalFlushed = FALSE
        /\ committedID = 0 /\ appliedID = 0 /\ cacheID = 0
        /\ retained = TRUE /\ accepted = FALSE /\ visible = {}
        /\ answer = "none" /\ session = FALSE
Offer == /\ ~offered /\ offered' = TRUE /\ logID' = 1
         /\ UNCHANGED <<originalOK,durableID,originalFlushed,committedID,
                        appliedID,cacheID,retained,accepted,visible,answer,session>>
Flush == /\ logID # 0 /\ durableID # logID /\ durableID' = logID
         /\ originalFlushed' = (originalFlushed \/ logID = 1)
         /\ UNCHANGED <<offered,originalOK,logID,committedID,appliedID,
                        cacheID,retained,accepted,visible,answer,session>>
Accept == /\ ~accepted /\ logID = 1 /\ (EarlyReceipt \/ durableID = 1)
          /\ accepted' = TRUE
          /\ visible' = IF PublishAccepted THEN visible \cup {1} ELSE visible
          /\ session' = IF ReceiptSession THEN TRUE ELSE session
          /\ UNCHANGED <<offered,originalOK,logID,durableID,originalFlushed,
                         committedID,appliedID,cacheID,retained,answer>>
Replace == /\ logID = 1 /\ committedID = 0
           /\ logID' = 2 /\ durableID' = 0
           /\ UNCHANGED <<offered,originalOK,originalFlushed,committedID,
                          appliedID,cacheID,retained,accepted,visible,answer,session>>
Commit == /\ committedID = 0 /\ durableID # 0
          /\ committedID' = durableID
          /\ UNCHANGED <<offered,originalOK,logID,durableID,originalFlushed,
                         appliedID,cacheID,retained,accepted,visible,answer,session>>
Apply == /\ appliedID = 0 /\ committedID # 0
         /\ appliedID' = committedID /\ cacheID' = committedID
         /\ visible' = IF Valid(committedID) THEN {committedID} ELSE {}
         /\ UNCHANGED <<offered,originalOK,logID,durableID,originalFlushed,
                        committedID,retained,accepted,answer,session>>
Expire == /\ cacheID # 0 /\ cacheID' = 0 /\ retained' = FALSE
          /\ UNCHANGED <<offered,originalOK,logID,durableID,originalFlushed,
                         committedID,appliedID,accepted,visible,answer,session>>
Observation == IF cacheID = 1 \/ (IndexOnly /\ cacheID # 0)
               THEN IF AcceptMeansSuccess \/ Valid(cacheID) THEN "committed" ELSE "rejected"
               ELSE IF retained /\ appliedID = 2 /\ logID = 2 THEN "invalidated"
               ELSE IF retained /\ logID = 1 /\ appliedID = 0 THEN "pending"
               ELSE IF InvalidateAbsent THEN "invalidated" ELSE "unknown"
Query == /\ accepted /\ answer' = Observation
         /\ session' = (session \/ Observation = "committed")
         /\ UNCHANGED <<offered,originalOK,logID,durableID,originalFlushed,
                        committedID,appliedID,cacheID,retained,accepted,visible>>
Next == Offer \/ Flush \/ Accept \/ Replace \/ Commit \/ Apply \/ Expire \/ Query
SafetySpec == Init /\ [][Next]_vars
Safe == /\ (accepted => originalFlushed)
        /\ (\A id \in visible: id = committedID /\ id = appliedID /\ Valid(id))
        /\ (answer = "committed" => committedID = 1 /\ appliedID = 1 /\ originalOK)
        /\ (answer = "rejected" => committedID = 1 /\ appliedID = 1 /\ ~originalOK)
        /\ (answer = "invalidated" => committedID = 2 /\ appliedID = 2)
        /\ (session => committedID = 1 /\ appliedID = 1 /\ originalOK)
\* Stable original leader, eventual durable quorum, retained result until query.
LiveNext == Offer \/ Flush \/ Accept \/ Commit \/ Apply \/ Query
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Offer) /\ WF_vars(Flush)
            /\ WF_vars(Accept) /\ WF_vars(Commit) /\ WF_vars(Apply) /\ WF_vars(Query)
Progress == <> (accepted /\ answer \in {"committed","rejected"})
=======================================================================
