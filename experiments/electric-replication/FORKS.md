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

## Terminal-fence compaction contract

The earlier source `decisions` and destination `destinations` maps retained terminal
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
three mutation names. Native implementation and generated/fault tests complement
this abstract model; they do not establish a refinement proof or independent-disk
durability. Old snapshot readers must not silently discard the new fences.

The implementation candidate uses the existing partition worker, not a separate
coordinator. For a terminal destination record it captures the maximal interval
containing that ID, below the current applied cut, excluding every nonterminal
record for that source group. It commits the interval at the source, then commits
destination collection. Lost acknowledgements repeat this sequence. The source
rejects delayed Grants with 410 and treats repeated Releases as successful;
neither operation recreates a decision. Source intervals only grow by union.

Each group admits at most 4,096 source decisions and 4,096 destination records
(including live forks); a new reservation/grant at that bound returns 429.
Existing decisions, release and compaction remain available. The pending-import
bound remains 32. These are explicit experimental resource limits, not a claim
of qualified high-cardinality operation. A disconnected owner cannot accumulate
unbounded terminal metadata. Retired intervals coalesce across unrelated log
positions; holes correspond to retained work, not the number of historical PUTs.

Collection retains only the latest 256 terminal replies by reservation index,
separately from ownership. A foreground request whose result was evicted returns
503 (unknown), never a fabricated abort or success. It can retry the ordinary
PUT/read lifecycle. The cache is not a permanent idempotency ledger. Live objects
still resolve idempotent PUTs through the native configuration check. Stale import
workers tolerate removed destination records and never publish from their clone.
Dead mirror-index entries are collected only after their native object is gone.

This changes data identity to 9, snapshot envelope to `ERSP0006`, and consensus
RPC namespace to `/_raft9/`. There is no in-place/rolling migration from identity
8: old data and old snapshots must fail closed before replay. This is a new
isolated candidate; the published identity-8 checkpoint remains qualified only
for its recorded tests. The trusted-control-plane assumption still applies to
retirement certificates; production transport authentication remains a gate.

## Executed identity-9 qualification

`conformance-031` discovers and passes all 332 unchanged upstream tests, with
subscriptions enabled and zero failures/skips/todo. All 144 Rust tests pass
(two unchanged forensic helpers ignored). Generated native-WAL tests cover
reordered/duplicate interval unions, destination-group separation, zero/MAX
boundaries, exact retained-reference counts, replay and snapshot restore. The
4,096-record boundary test fills both owners, checks rejection and duplicate
resolution at the limit, loses a source acknowledgement, then checks reclamation,
a pending-ID hole and exact latest-256 reply retention after snapshots/restarts.

`fork-fault-019` independently checks 1,718 operations, 323 fork resources, four
delayed retired Grants and eight reclamation observations; one unknown mutation
remains in the history. It cycles 320 short-lived forks around an older live
child, verifies native descendant retention, installs fence-containing snapshots
into a fresh learner, crashes/restarts the cluster, and recreates the source.
Settled source/destination decisions and mirror indices are empty; retired ranges
coalesce to one interval per peer group and replies remain bounded at 256. Four
negative history mutations are rejected. This is an ordered single-host fixture,
not arbitrary concurrent lifecycle linearizability, an independent storage-domain
test, or high-cardinality throughput qualification.

`/_admin/{group}/fork-stats` exposes schema-1 counts and the local applied index,
without paths, payloads or credentials. Counts are local committed observations,
not a globally consistent group census. Growth in terminal records toward the
4,096 bound calls for restoring peer/quorum progress, not expiring ownership.
These diagnostics are a foundation, not completed production telemetry/alerts.

## Existing-fork retry correction

`fork-retry-001` preserves a real three-process failure beyond the pinned suite:
an existing, readable child returns 409 to a matching PUT after its parent is
deleted. Both the remote preflight and native prepare path consult the source
before checking the existing target. Protocol sections 4.2 and 5.1 require 200.
Passing 332/332 is not evidence that this case works.

The destination must resolve an existing PUT against its committed child
incarnation and current closure state, without depending on source availability.
Parse the same native configuration rules; initial data is ignored. Keep the
original source configuration in the durable grant for inheritance: using the
child's TTL override or the mirror's intentionally absent expiry would compare
the wrong configuration. Local forks retain these defaults on their native
parent. Reconfirmation must never create a missing child or acquire a reference.
A missing child follows the ordinary grant protocol; deletion/recreation cannot
be bypassed with an old result or cached configuration. No network call belongs
inside apply. The added metadata requires a new experimental data identity.

Before implementation, the extended native-WAL state-machine property fails
with 409 versus 200 (`properties/fork-retry-red.txt`). It generates inherited and
overridden TTL, deletion versus expiry, offsets/payloads, and snapshot/recovery;
it also specifies equivalent headers, conflicting defaults, closure transitions,
unchanged bytes and no resurrection after child deletion. The remote generated
property additionally checks original source defaults through grant/import,
snapshot/replay and destination recreation. Both properties now pass.

The candidate now commits `Reconfirm` at the destination before probing a source.
Existing children use the native configuration matcher and response builder;
they ignore initial data and never call native create. Pending transactions
retain their existing reservation semantics. A failed source probe reconfirms
again, since a concurrent creator may have published while that probe ran.
Only absent targets enter the source-grant protocol. Grant metadata keeps the
original source configuration, matched to the current child incarnation, through
replay and snapshots. This adds a metadata round trip to new cross-group fork
creation, not to POST append. The bounded 4,096-record map is searched for the
live child's descriptor; high-cardinality throughput is not qualified.

The retry candidate is identity **10**, snapshot `ERSP0007`, RPC `/_raft10/`;
identity 9's stored grants do not contain these defaults and are rejected. The
earlier identity-9 qualification above is historical evidence, not qualification
of this changed format. There is still no rolling upgrade or data migration.

`conformance-032` discovers/executes/passes 332 unchanged tests with zero
failures/skips/todo. All 144 replicated and 113 standalone Rust tests pass (the
same two upstream forensic helpers remain ignored), and the full TLA/Lean runner
passes 55 TLC mutations plus its Lean negative control. `fork-fault-020` checks
1,728 operations, 323 forks and ten reconfirmations, retaining two unknown
mutations. Six negative history mutations are rejected. Its source-only outage
uses a deliberate one-voter source membership while the destination keeps three
voters: matching and conflicting TTL requests keep working at the destination,
including after snapshot/restart. Deleting the child during that outage does not
allow an old PUT to resurrect it; restored source ownership subsequently releases
and compacts. The four stale-Grant and eight bounded-reclamation observations also
pass. This remains an ordered single-host fixture, not a general linearizability
proof or an independent-disk durability qualification.
The TLA/Lean models do not specify HTTP normalization or source-default selection;
those contracts are exercised by the generated native-WAL tests and HTTP histories.
