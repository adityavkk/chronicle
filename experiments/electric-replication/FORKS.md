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

## Sub-offset resolution

A JSON sub-offset counts complete flattened values after a server-minted anchor,
not arbitrary commas. Scan validated native wire bytes in at most 64 KiB windows;
carry string, escape and nesting state across windows and ancestor file ranges.
Only a comma outside a string at nesting depth zero ends a message. Stop at the
requested boundary, including its wire delimiter. Never load the entire suffix
or a complete large JSON value. Missing bytes fail the read; insufficient messages
return 400. A binary sub-offset must fit `tail - anchor` before adding to the
anchor; large unsigned inputs must not wrap. A zero sub-offset does not scan.

Lean proves the arithmetic bound, quoted/nested comma exclusions, and chunk
composition of the lexical fold before implementing this change. Valid JSON and
anchor alignment remain input assumptions; generated tests check the actual
native WAL, range reader, snapshots and parser against independently constructed
JSON values. These lemmas are not a proof of the Rust implementation.

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

## Terminal-fence compaction contract (not implemented yet)

The current source `decisions` and destination `destinations` maps retain terminal
transactions indefinitely. Simply deleting those entries would let a delayed
Grant reacquire a source reference, possibly on a recreated stream. A timeout is
not a retirement certificate. One scalar high-water mark also stalls behind a
single long-lived fork; replaying an older sparse frontier must not reopen IDs
that a newer frontier already retired.

Use source-owned, monotonic **retired ID intervals**, scoped to the destination
group and existing cluster identity. A destination captures its applied log cut
and the complement of its active transaction IDs below that cut. A bounded range
certificate may cover only that complement: pending import, live descendants,
and release awaiting source confirmation remain holes. IDs below the captured
cut cannot be allocated again. Gaps from unrelated log commands need no individual
tombstone. A source orders each certificate with Grant/Release, unions it with
its existing retired ranges, and removes covered terminal decisions atomically.
Grant must consult the retired ranges before it can create a new decision.
Duplicated, delayed or reordered certificates can only add fences, never erase
one; overlapping/adjacent intervals coalesce.

Destination terminal-result retention is separate from source ownership. It must
be bounded without deleting a pending or live transaction. Foreground workers
holding a retired transaction ID must handle expiration without panicking or
reporting an abort for an unknown result. Ordinary idempotent PUT retries still
resolve against the native stream/configuration, not an indefinitely retained
transaction reply. A recovery sweep can reconstruct certificates from the durable
applied cut and active transactions; no network call holds the apply lock. Failed
delivery does not authorize source collection. Admission must bound unswept
terminal records during peer failure rather than growing them without limit.

`FenceCompaction.tla` specifies this retirement protocol before Rust changes. It
includes unrelated log positions, active holes, delayed old Grants, reordered
range certificates, terminal collection and recovery. Its negative switches
cover a certificate that includes active work, replacing rather than unioning
fences, and losing fences on restart. It assumes unique monotonic IDs and atomic
quorum-durable metadata apply; it does not prove interval encoding, authentication,
Rust recovery or bounded sweeper cost. TLC passes 12,526 distinct safety states
at three ID positions, and stable-period liveness over 349 states at two positions.
All three deliberately unsafe mutations violate the invariant. Exact model/config
hashes and counterexamples are retained under `evidence/formal/fences*` and the
three mutation names. Native implementation and generated/fault tests remain
required before claiming this gate. Old snapshot readers must not silently
discard the new fences; storage/RPC identity compatibility must be qualified with
the implementation.
