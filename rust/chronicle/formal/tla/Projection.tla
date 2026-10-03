------------------------------ MODULE Projection ------------------------------
EXTENDS Naturals, Sequences
CONSTANTS BadRestart, BadIncarnation
VARIABLES authority, incarnation, cache, cacheIncarnation, published,
          live, copying, target, targetIncarnation, staged, lastRead
vars == <<authority,incarnation,cache,cacheIncarnation,published,
          live,copying,target,targetIncarnation,staged,lastRead>>

(* authority is the already committed/applied SQLite view. Consensus is supplied
   by OpenRaft, not modeled as a second file or majority protocol here. *)
Init == /\ authority = <<>> /\ incarnation=1
        /\ cache = <<>> /\ cacheIncarnation=1 /\ published=0
        /\ live=TRUE /\ copying=FALSE /\ target = <<>>
        /\ targetIncarnation=1 /\ staged = <<>>
        /\ lastRead=[actual |-> <<>>, expected |-> <<>>, fileInc |-> 1, inc |-> 1]

Commit(b) == /\ live /\ ~copying /\ Len(authority)<2
             /\ authority'=Append(authority,b)
             /\ UNCHANGED <<incarnation,cache,cacheIncarnation,published,
                             live,copying,target,targetIncarnation,staged,lastRead>>

Recreate == /\ live /\ ~copying /\ incarnation<2
            /\ incarnation'=incarnation+1 /\ authority' = <<>>
            /\ published'=IF BadIncarnation THEN published ELSE 0
            /\ UNCHANGED <<cache,cacheIncarnation,live,copying,
                            target,targetIncarnation,staged,lastRead>>

(* One blocking actor serializes applied views and projection I/O. A temporary
   file is not a readable generation until all its bytes have been written. *)
BeginCopy == /\ live /\ ~copying /\ copying'=TRUE
             /\ target'=authority /\ targetIncarnation'=incarnation /\ staged' = <<>>
             /\ UNCHANGED <<authority,incarnation,cache,cacheIncarnation,published,live,lastRead>>
CopyByte == /\ live /\ copying /\ Len(staged)<Len(target)
            /\ staged'=Append(staged,target[Len(staged)+1])
            /\ UNCHANGED <<authority,incarnation,cache,cacheIncarnation,published,
                            live,copying,target,targetIncarnation,lastRead>>
Publish == /\ live /\ copying /\ Len(staged)=Len(target)
           /\ cache'=staged /\ cacheIncarnation'=targetIncarnation
           /\ published'=Len(staged) /\ copying'=FALSE
           /\ UNCHANGED <<authority,incarnation,live,target,targetIncarnation,staged,lastRead>>

Read == /\ live /\ ~copying
        /\ (BadIncarnation \/ cacheIncarnation=incarnation)
        /\ published=Len(authority) /\ published<=Len(cache)
        /\ lastRead'=[actual |-> SubSeq(cache,1,published), expected |-> authority,
                       fileInc |-> cacheIncarnation, inc |-> incarnation]
        /\ UNCHANGED <<authority,incarnation,cache,cacheIncarnation,published,
                        live,copying,target,targetIncarnation,staged>>

(* Durable authority survives; a projection may be missing/partial/corrupt.
   Restart must discard its publication metadata, never trust local file size. *)
Crash == /\ live /\ live'=FALSE /\ copying'=FALSE /\ cache' = <<9>>
         /\ published'=0
         /\ UNCHANGED <<authority,incarnation,cacheIncarnation,
                         target,targetIncarnation,staged,lastRead>>
Restart == /\ ~live /\ live'=TRUE
           /\ published'=IF BadRestart THEN Len(cache) ELSE 0
           /\ UNCHANGED <<authority,incarnation,cache,cacheIncarnation,
                           copying,target,targetIncarnation,staged,lastRead>>

Next == (\E b \in {1,2}: Commit(b)) \/ Recreate \/ BeginCopy \/ CopyByte \/
        Publish \/ Read \/ Crash \/ Restart
Spec == Init /\ [][Next]_vars
DataFromAuthority == lastRead.actual=lastRead.expected
GenerationCurrent == lastRead.fileInc=lastRead.inc
=============================================================================
