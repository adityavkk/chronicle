----------------------------- MODULE ForkStaging -----------------------------
EXTENDS Naturals, Sequences
CONSTANTS BadGap, BadReady
VARIABLES staged, ready, decided, published, available
vars == <<staged,ready,decided,published,available>>
source == <<11,22>>
Init == /\ staged = <<>> /\ ready=FALSE /\ decided=FALSE
        /\ published=FALSE /\ available=TRUE
Chunk(i) == /\ available /\ ~ready /\ i \in 0..1
            /\ (i<=Len(staged) \/ BadGap)
            /\ staged'=IF i<Len(staged) THEN staged ELSE Append(staged,source[i+1])
            /\ UNCHANGED <<ready,decided,published,available>>
Seal == /\ available /\ (Len(staged)=Len(source) \/ BadReady)
        /\ ready'=TRUE /\ UNCHANGED <<staged,decided,published,available>>
Decide == /\ ready /\ decided'=TRUE
          /\ UNCHANGED <<staged,ready,published,available>>
Publish == /\ available /\ decided /\ published'=TRUE
           /\ UNCHANGED <<staged,ready,decided,available>>
CrashRecover == /\ available'=~available
                /\ UNCHANGED <<staged,ready,decided,published>>
Next == (\E i \in 0..1: Chunk(i)) \/ Seal \/ Decide \/ Publish \/ CrashRecover
Spec == Init /\ [][Next]_vars
StagedPrefix == staged=SubSeq(source,1,Len(staged))
ReadyComplete == ready => staged=source
PublishedComplete == published => staged=source /\ decided
=============================================================================
