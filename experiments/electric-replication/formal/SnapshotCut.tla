-------------------------- MODULE SnapshotCut --------------------------
EXTENDS Naturals, Sequences, TLC
CONSTANTS UnpinnedPath, UnboundedCopy, LateMetadata, OverlapInstall,
          EarlyReference, EarlyCleanup
VARIABLES applied, owner, files, cut, candidate, phase, archives, reference, high
vars == <<applied, owner, files, cut, candidate, phase, archives, reference, high>>

\* One group, one name and two incarnations. Prefixes of open inodes are
\* immutable; appends extend them and unlink/recreate changes only the name.
\* Raft supplies committed transitions and honest fsync is assumed. Metadata
\* (including leases, receipts, clock and membership) is represented by applied.
MaxEntry == 3
Image(i, o, bytes, metadata) ==
    [index |-> i, incarnation |-> o, data |-> bytes, meta |-> metadata]
Empty == Image(0, 1, <<>>, 0)
Current == Image(applied, owner, files[owner], applied)
Init == /\ applied = 0 /\ owner = 1 /\ files = [i \in 1..2 |-> <<>>]
        /\ cut = Empty /\ candidate = Empty /\ phase = "Idle"
        /\ archives = {Empty} /\ reference = Empty /\ high = 0

ApplyAppend == /\ applied < MaxEntry /\ applied' = applied + 1
          /\ files' = [files EXCEPT ![owner] = Append(@, applied')]
          /\ UNCHANGED <<owner, cut, candidate, phase, archives, reference, high>>
Recreate == /\ owner = 1 /\ applied < MaxEntry
            /\ owner' = 2 /\ applied' = applied + 1
            /\ UNCHANGED <<files, cut, candidate, phase, archives, reference, high>>
Capture == /\ phase = "Idle" /\ applied > reference.index
           /\ cut' = Current /\ phase' = "Captured"
           /\ UNCHANGED <<applied, owner, files, candidate, archives, reference, high>>
Copy == /\ phase = "Captured"
        \* Model successful copies. A short file is rejected, not published;
        \* the path mutation must also fail with a long-enough replacement.
        /\ Len(files[IF UnpinnedPath THEN owner ELSE cut.incarnation]) >= Len(cut.data)
        /\ LET inode == IF UnpinnedPath THEN owner ELSE cut.incarnation
               bytes == IF UnboundedCopy THEN files[inode]
                        ELSE SubSeq(files[inode], 1, Len(cut.data))
           IN candidate' = Image(cut.index, cut.incarnation, bytes,
                                 IF LateMetadata THEN applied ELSE cut.meta)
        /\ phase' = "Copied"
        /\ UNCHANGED <<applied, owner, files, cut, archives, reference, high>>
FileSync == /\ phase = "Copied" /\ phase' = "FileSynced"
            /\ UNCHANGED <<applied, owner, files, cut, candidate, archives, reference, high>>
DirectorySync == /\ phase = "FileSynced" /\ phase' = "Durable"
                 /\ archives' = archives \cup {candidate}
                 /\ UNCHANGED <<applied, owner, files, cut, candidate, reference, high>>
Publish == /\ (phase = "Durable" \/ (EarlyReference /\ phase = "FileSynced"))
           /\ reference' = candidate /\ phase' = "Referenced"
           /\ high' = IF candidate.index > high THEN candidate.index ELSE high
           /\ UNCHANGED <<applied, owner, files, cut, candidate, archives>>
Cleanup == /\ (phase = "Referenced" \/ (EarlyCleanup /\ phase = "Durable"))
           /\ archives' = {candidate} /\ phase' = "Idle"
           /\ UNCHANGED <<applied, owner, files, cut, candidate, reference, high>>

\* A newer incoming snapshot is already validated and durable. Without the
\* build/install lifecycle lock, its publication/cleanup can overtake a builder.
Install == /\ (phase = "Idle" \/ OverlapInstall) /\ applied < MaxEntry
           /\ applied' = applied + 1
           /\ files' = [files EXCEPT ![owner] = Append(@, applied')]
           /\ reference' = Image(applied', owner, files'[owner], applied')
           /\ archives' = {reference'} /\ high' = applied'
           /\ UNCHANGED <<owner, cut, candidate, phase>>
Crash == /\ phase # "Idle" /\ phase' = "Idle"
         /\ cut' = reference /\ candidate' = reference
         /\ UNCHANGED <<applied, owner, files, archives, reference, high>>
Next == ApplyAppend \/ Recreate \/ Capture \/ Copy \/ FileSync \/ DirectorySync
        \/ Publish \/ Cleanup \/ Install \/ Crash
SafetySpec == Init /\ [][Next]_vars
Safe == /\ reference \in archives /\ reference.index = high
        /\ (phase \in {"Copied", "FileSynced", "Durable", "Referenced"} => candidate = cut)
LiveNext == ApplyAppend \/ Capture \/ Copy \/ FileSync \/ DirectorySync \/ Publish \/ Cleanup
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(ApplyAppend) /\ WF_vars(Capture)
            /\ WF_vars(Copy) /\ WF_vars(FileSync) /\ WF_vars(DirectorySync)
            /\ WF_vars(Publish) /\ WF_vars(Cleanup)
Progress == <> (reference.index = MaxEntry /\ phase = "Idle")
=======================================================================
