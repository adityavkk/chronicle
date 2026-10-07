# Cross-partition forks: contract before implementation

This extends the native parent-reference graph, not stream placement. Full paths
still hash to fixed groups. Local forks retain the native shared prefix. A remote
fork needs one native-wire mirror per source incarnation and destination group:
replay there must not depend on a live remote server. Several children share and
monotonically extend that mirror; payload travels through the destination's
existing consensus WAL, never a second payload journal. Cold offload remains off.

## Ordered decisions, publication and retirement

1. Read the source configuration/incarnation through its owner. Prevalidate all
   headers and the initial body using the native create rules. Reserve the
   destination name in its log. The transaction is identified by destination
   group and reservation log index within the pinned cluster identity.
2. The source orders `Undecided → Granted` or `Undecided → Aborted` in its log.
   Grant checks the exact source incarnation, resolves the requested fork point
   and inherited configuration, and increments its native reference count once.
   It is irrevocable and is the fork's linearization point. Invalid requests
   produce a durable abort, never a temporary pin with visible delete effects.
3. The destination records the grant, imports exact wire bytes in bounded,
   contiguous, idempotent chunks, and creates the native child under its apply
   lock only after the entire granted prefix is local. Initial-body bytes are
   encoded once with the native encoder. Child TTL starts at the grant time,
   not at the end of an arbitrarily long transfer. Producer state is not inherited.
4. Until publication or durable abort, destination reads, writes, fork-source
   queries and catalog observations cannot report absence. Return 503; retries
   of the same create resume the reservation, different mutations conflict. No
   speculative hot-file tail or live-reader wakeup is public.
5. Only actual child hard collection authorizes source release. Soft deletion
   with grandchildren is not collection. A durable release intent retries after
   crashes until the source idempotently decrements its reference. Source and
   destination terminal transaction fences survive snapshots and log purge.
   No timeout-based pin reclamation is safe when the destination is unavailable.

The source may legitimately return 410 following its deletion after Grant, even
if destination materialization has not finished. That is a committed-but-pending
fork, not an aborted operation. Timeouts and connection loss do not cancel it.
Before a grant, cancellation must first commit Abort at the source and resolve
the race. A rejected create must not transiently turn a source DELETE into 410.

Mirrors are internal native streams, not protocol resources. Their metadata has
no inherited TTL, they flatten the immutable source prefix (including ancestry),
and they are hidden from catalogs and public lookups. Parent pointers in a native
child still resolve by the original source path on recovery. Source retention
prevents a second live incarnation at that path until all descendants release.

Network calls never hold the state-machine lock. Workers can duplicate or reorder
control calls; source decisions, destination chunk offsets and incarnation fences
are authoritative. Recovery scans durable pending/retirement records. Bounded
admission is required; no distributed transaction relies on a worker's memory.

## Formal boundary and remaining implementation gates

`Forks.tla` abstracts one transaction and one descendant, quorum-durable decisions,
snapshot recovery, a two-chunk transfer, source deletion and release. It assumes
Raft log matching, correct native range bytes and honest storage. Mutations expose
partial publication, early release, false absence and lost durable grant. Stable
period liveness assumes available quorums, fair workers and finite transfer.
Lean proves decision irreversibility and exact range/publication arithmetic;
neither model proves Rust refinement, network authentication or physical disks.

Cross-group fault histories must independently check byte identity and deletion
retention through retries, source/destination leader loss, snapshots, pending-name
races and descendant deletion. Single-partition conformance is insufficient.
