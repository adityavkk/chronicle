# Local acceptance without speculative reads

Settled user contract, specified before implementation. The strong default remains
quorum-fsync. This document does not claim the async implementation or its gates
have passed yet.

## HTTP contract

`POST /stream` with `Stream-Durability: local-fsync` requests durable **acceptance**
of an append, not successful execution. Ordinary lifecycle operations, cross-group
fork coordination and subscription cursor/lease mutations remain quorum-fsync;
they reject this mode rather than silently changing their guarantees. The default
and explicit `quorum-fsync` append keep their existing synchronous semantics.

An operator may explicitly configure `append_durability: "local-fsync"` as the
default for headerless POST appends. The shipped default remains `quorum-fsync`;
an explicit request header overrides the append default. Lifecycle/subscription
operations stay quorum-fsync in either configuration. The effective mode is
recorded in the replicated command, so replay never consults current config.
This is useful for clients that cannot send extension headers, including the
pinned unchanged ds-bench. Its 2xx counter measures **acceptance**, not commitment,
in this opt-in configuration; committed progress/drain needs separate evidence.

After the actual native WAL group-fsync covers its consensus entry, the leader
returns HTTP **202**, `Stream-Durability: local-fsync`, an opaque `Stream-Receipt`,
and a relative `Location` for awaiting that receipt. The JSON response says
`accepted`. It has **no Stream-Session or Stream-Next-Offset**: sequence checks,
JSON validation, stream existence/closure and other semantic outcomes are not
established by acceptance. Even a rejected append can have a durable receipt.
An already committed append may still return 202; its receipt then resolves
immediately. Basic HTTP parsing, size and admission errors can precede acceptance.

`GET /_receipts/<receipt>?wait_ms=30000` waits for progress without busy polling.
The wait is bounded to 30 seconds; zero (the default) returns the current result.
A small client helper supplies deadline/cancellation handling so application code
can await a receipt without writing its own polling loop. The returned states are:

| State | Meaning |
| --- | --- |
| `pending` | This replica retains the exact entry, but has not applied its committed outcome. No promise it will commit. |
| `committed` | Exact receipt identity applied successfully after quorum commitment. Includes original status/headers/body and a usable Stream-Session. |
| `rejected` | Exact receipt identity committed, but deterministic semantic execution rejected the append. Includes the original error result; acceptance was not success. |
| `invalidated` | A **committed** replacement at this log position proves this receipt's entry cannot become committed. |
| `unknown` | This replica lacks sufficient retained evidence, including lagging/unavailable origins or expired result history. This is not proof of rejection or loss. |

The receipt binds cluster, partition, full Raft LogId (term, leader node and index),
and command ordinal within its immutable batch. An index alone is insufficient:
a future leader can overwrite an accepted uncommitted entry at the same index.
Receipts are not authentication credentials; this experiment still requires a
trusted transport. A malformed or foreign-cluster receipt is a request error.
It cannot be supplied as Stream-Session.

Completed result lookup returns HTTP 200 even when the enclosed append result is
a protocol error. A timed-out pending wait returns 202. Unknown/expired lookup
returns 404 with an explicit `unknown` JSON state; invalidation returns 410 with
`invalidated`. Lookup includes local applied/log progress and admission occupancy
so clients can distinguish acceptance rate from replication/application progress.
Follower observations may lag. Only a committed result issues a session token;
unknown, pending or invalidated receipts never do.

## Native storage and recovery

The consensus entry remains the only authoritative payload WAL record. A batch
has a transient local-durability notification channel, excluded from serialization.
The existing Journal append callback signals it **after** `Shard::wait_durable`,
using the assigned full LogId. No receipt is issued merely because OpenRaft's
unbounded API channel accepted a request or because logical metrics advanced.
There is no receipt payload side log or second durability transaction.

Actual Raft replication continues after 202. The worker may submit further
batches after local durability; it retains each request's admission credits until
consensus resolves it. Waiting for quorum to free the *batch dispatch* window
would unnecessarily make local acceptance throughput depend on quorum latency.
Count and encoded-byte admission limits bound queued plus unresolved commands;
HTTP cancellation, timeout and successful 202 do not free those credits. A full
backlog returns 429. Followers that cannot keep up eventually stop admission;
an ever-growing backlog is not a successful throughput measurement.

Restart does not mint a fresh backlog allowance on top of orphaned accepted
entries. New WAL proposals remain fenced until the recovered uncommitted suffix is
applied or definitively replaced. A recovered entry's full identity matters: a
shorter replacement log need not grow back to the old numeric high-water index
to clear this fence. The fence is checked only until resolved, not by rescanning
the log on every steady-state append.

The same fence applies at **each new leadership term**, not only at startup. A
follower may inherit an uncommitted suffix without the previous leader's in-memory
credits. The new owner first applies or replaces the inherited suffix, including
the election entry, before proposing new entries. The single FIFO dispatcher owns
this preparation; readiness is cached per term. Locally bounded HTTP requests can
wait in its queue during recovery, but cannot get durable acceptance before it.
This may delay a new leader's first local acceptance until quorum recovery
completes; a previously prepared, isolated leader can still accept up to its bound.
The bound covers the **prepared owner's locally queued and unresolved work**, not
the physical WAL or a follower's received suffix. During recovery inherited debt
can coexist with locally charged work; it must resolve before new proposals. It
is reported as recovering, not disguised as a fresh available WAL allowance.

Preparation at HTTP ingress alone is insufficient: a write can already be queued
inside OpenRaft when the leadership term changes. Each dispatched proposal carries
the prepared full leader ID (term and node). RaftCore checks that precondition
atomically before assigning an index; mismatch rejects without a WAL entry. This
uses a narrow backport of upstream's expected-leader admission check to pinned
0.9.25, not a custom election or commit rule. `AdmissionEpoch.tla` separates local
reservation, application queue, Raft API queue and log assignment. It rejects both
reusing preparation across terms and omitting the assignment-time check. Like the
other models, it assumes consensus's log ownership rules rather than proving them.

Apply first fsyncs its covering Commit marker, then invokes the native handlers.
For local-acceptance commands it also retains their deterministic replies in
partition-owned metadata. Recent result batches are bounded (the latest 1,024
batches containing receipts, at most 65,536 results), included in snapshots and
rebuilt by private committed replay. Retention is by count, not a promised time
interval; at high rates it can be short. Clients should await promptly and save
terminal results they need later. Expiration does not undo the write or producer
deduplication, and missing cached results never establish invalidation.

No ordinary, prefix, session, long-poll or SSE read can see accepted-but-uncommitted
payload. The existing pre-publication marker and native wakeup boundary do not
change. One member still commits after its own durability; multiple members need
a durable majority. A local receipt survives an honest-disk process restart but
can lose its append after failover. It does not promise independent-disk survival.

Retry an unknown or lost response with the same producer ID/epoch/sequence and
body. Receipts identify **attempts**, not producer operations: two accepted retries
can have distinct receipts while native producer deduplication creates one effect.
A committed retry can return the protocol's duplicate response. Deduplication is
scoped to the stream incarnation; deletion/recreation does not give cross-lifetime
exactly-once effects. Receipt outcomes remain historical even if the stream is
subsequently deleted or recreated.

## Verification obligations

TLA+ separates acceptance from commitment, full-identity receipt resolution,
semantic rejection, result expiration, committed-only publication, and recovered
backlog admission. Negative variants acknowledge before fsync, expose accepted
bytes, match only an index, invalidate from absence, treat acceptance as semantic
success, or free capacity on 202/restart. Stable-leader liveness assumes eventual
durable quorum/application, a waiting client, and retention until observation.
It does not promise progress during a permanent partition.

Lean proves the deterministic identity/classification and credit arithmetic
contracts, not a refinement of Rust, Raft or fsync. Generated real-WAL tests must
cover mixed strong/local batches, distinct per-command results, partial apply,
snapshots/replay/eviction, and stale receipts after replacement. Real-process
histories must retain local 202s separately from committed results, test minority
admission/backpressure, delayed followers, leader/whole-cluster crashes, retries,
read invisibility, membership movement and conclusive versus unknown loss.
The unchanged 332-test default-strong conformance suite remains a separate gate.
Matched benchmarks must count local accepts and committed progress separately,
report latency/memory/traffic/fsync costs and sustained lag, and drain or account
for every accepted receipt. Local-only runs do not qualify independent disks/AZs.
