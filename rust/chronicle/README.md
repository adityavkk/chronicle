# Chronicle Raft — implementation checkpoint, not a production release

One Rust binary hosts four fixed data shards and a replicated placement group,
using OpenRaft 0.9.25 and SQLite WAL/FULL. Three replicas acknowledge only after
durable-majority consensus. Stream offsets count wire bytes, not log entries.
Strict reads use a leadership barrier; `?consistency=stale` deliberately allows
an older committed prefix. One stream remains leader ordered.

The implementation runs on a real local k3d cluster, not an in-memory simulation.
The tested host is one orb with nested Docker: it does not represent independent
disks, AZs, or power-loss durability. Keep all APIs on a trusted private network;
the listener includes unauthenticated admin and Raft routes. Do not expose it
through a public portal. The read-only Grafana UI can use a portal.

## Build and run

```sh
make check       # pinned toolchain/lockfile; fmt, clippy, tests, rustdoc, checker tests
make formal      # pinned TLC + Lean; positive and negative checks
make local       # FIRST genesis only: build, k3d, PVCs, explicit initialization, bootstrap
```

Prerequisites: Rust 1.90, Docker with privileged networking, k3d 5.8.3,
kubectl, Python 3, Java, Lean 4.31.0. See [deployment](ops/README.md) for restart,
learner admission, draining, and teardown. `make local` is intentionally not a
recovery procedure; it never overwrites existing identity/storage.

HTTP example: PUT `/v1/stream/tenant/path` creates an octet stream; POST appends;
GET reads; DELETE tombstones. Producer headers are `producer-id`, `producer-epoch`,
and `producer-seq`. Recreate requires explicit next `stream-incarnation`; omitted
incarnation defaults to 1 rather than rebinding an old request to a new stream.

## What is verified at this checkpoint

* Formal design preceded implementation in a separate commit. Bounded TLC checks
  and Lean proofs have explicit assumptions and [model-to-code gaps](formal/README.md).
  They are not an end-to-end proof or mechanized Rust refinement.
* Real three-replica bootstrap, fourth-node learner admission, three shard moves,
  explicit Raft drain, process restart, and fault histories have retained evidence.
* A forced pod replacement exposed an overlapping-owner failure: 310 acknowledgements
  disappeared from a strict read. The failed history and database archive remain.
  [Diagnosis and actual-PVC regressions](evidence/OWNERSHIP.md) distinguish the lock,
  restart/admission, and recipient-routing fixes from broader durability claims.
* An isolated test package injects actual SQLite VFS `xWrite` and `xSync` errors,
  checking error propagation, no speculative cache publication and prior-state
  retention on reopen. Its small test-only C/FFI boundary is not in the server binary.
* The existing Go Porcupine checker accepts an offline Rust history flag. The
  original failure is Illegal; larger ambiguous histories can time out as Unknown.
  Python's prefix/retention smoke checker is not a substitute for linearizability.
  The post-review real-cluster drain history has 2,262 successful append responses,
  52 strict reads, and Porcupine `Ok`; 364 transport-unknown and 647 rejected
  append attempts mean this was **not uninterrupted availability**. Three shard
  memberships moved to exclude node 4, with all five placements complete.
  `review-drain.jsonl` preserves the first failed attempt (admin forwarding timed
  out); `review-drain2.jsonl` records the fixed run. A 21-operation real HTTP
  lifecycle/retry history also received `Ok`.
* [Verified replica retirement](evidence/RETIREMENT.md) now distinguishes voter
  replacement from delivery of demotion to the old process. Fresh node-5 admission,
  drain/restart, and partition/repair/heal histories received Porcupine `Ok` with
  600, 600 and 720 records respectively. The partition run retained 23 unknown
  append attempts and 3 unknown reads. Untouched groups stayed empty. These checks
  do not cover a failed destination during initial learner catch-up.
* [Membership admission fencing](evidence/MEMBERSHIP-ADMISSION.md) closes delayed
  controller work across reelection and cancelled-membership completion races.
  The narrowly patched, checksum-pinned OpenRaft source retains its licenses.
* [Pending-target repair](evidence/PENDING-PLACEMENT.md) now supersedes a failed
  catch-up destination without losing possible-voter history. Gated k3d histories
  on the repaired and an unaffected shard each retained 720 records and received
  Porcupine `Ok`. Two further 720-record histories reached the snapshot receiver
  before its SQLite install transaction; one SIGKILLed and restarted that
  container on its existing PVC. Both received Porcupine `Ok`.
* [Terminal storage failure](evidence/FAIL-STOP.md) now stops the shared node
  instead of advertising a partly broken process as healthy. A blocked-body
  k3d run verified self-exit 1, same-PVC restart, 480 retained records and
  Porcupine `Ok`. Permanent disk failure/spare replacement remains unqualified.
* OpenRaft's pinned storage contract suite passes, alongside SQLite VFS faults,
  snapshot/reopen producer and membership checks, and a slow-body admission test.
  New placement completion awaits a membership operation and verifies its applied
  state; matching applied voters or a read barrier alone are insufficient.
* VictoriaMetrics, VictoriaLogs and VictoriaTraces receive real request telemetry
  through the OTel Collector; Grafana has populated request/index/export panels.
  Bounded lossy exporters, W3C forwarding, separated loss/failure counters, stage
  histograms and completion phase timings have targeted tests. Retained Victoria
  queries show a real forwarded request's parent/child spans and two correlated logs.
* Feature-gated storage pause points exercise log-flush, apply and snapshot-install
  boundaries. Subprocess crash/release checks pass on the actual local PVC; these
  are narrower than a live distributed membership fault or power-loss test.
  Two live Raft histories additionally pause after log persistence or committed
  apply, issue concurrent identical retries, terminate the leader pod, and recover.
  Both received Porcupine `Ok`; the log-only retry appended, while the committed
  apply retry returned its retained duplicate result. The histories preserve the
  unknown responses and the bounded pause observation rather than hiding them.
* Committed SQLite bytes are lazily projected into rebuildable Electric-format
  files. Metadata capture is fenced across snapshot/lifecycle replacement; opened
  ranges keep their original inode and frontier. Cache retention is capped at 64
  files and 16 MiB per group; evicted inodes held by active readers remain until
  those reads finish. Admission covers responses and cancelled blocking reads.
  Delivery uses 256 KiB file reads, with unexpected EOF treated as an error.
  This is portable range delivery, **not zero-copy sendfile**. Ordinary filesystem
  reliability is assumed; hostile mutation/silent corruption of a published inode
  is not detected by a cryptographic cache checksum. Restart never trusts caches.
* [Native-election experiments](evidence/CAMPAIGNS.md) exercised preferred-voter
  partitions and membership round trips under paced traffic. The mechanism stays
  **default off**; it is neither directed transfer nor resource-informed balancing.
  Failed harness attempts and the missed stability threshold remain in the evidence.
* [Strict long polling](evidence/LONGPOLL.md) has bounded admission and a fresh
  barrier/view after its five-second deadline. Real k3d gates cover concurrent
  append, recreation, close and loss of quorum at that boundary. These HTTP
  observations are not general linearizability evidence.
* [Strict SSE](evidence/SSE.md) streams bounded text/JSON/base64 chunks and publishes
  a control offset only after its captured range. Each new observation requires a barrier;
  recreation or post-header failure aborts without advancing the cursor. The
  60-second application lifetime assumes HTTP consumer progress, not forced
  socket teardown when the transport stops polling. Forwarded streams retain
  admission on both nodes. This is not full protocol conformance.
* A [release-mode workload comparison](evidence/PERFORMANCE.md) measured 156 ack/s
  with 172 ms p99 on one hot stream, versus 262 ack/s with 74 ms p99 across four
  shards, using the same CP/strict semantics and eight producers. Each case
  retained 2,048 records with no failed/unknown append and Porcupine `Ok` for
  every stream history. This short shared-host run is not a capacity estimate;
  coarse resource samples cannot compare per-case peaks.

## Explicitly unfinished

This is a bounded replicated vertical slice, **not the full requested deliverable**.
Protocol conformance beyond the implemented request subset,
zero-copy sendfile, resource-informed placement and leadership
balancing, deeper replication/storage trace linkage, broader I/O-fault schedules, stronger
admission/snapshot crash schedules, repeated steady-state measurements and external
equal-semantics performance baselines remain.
The [unmodified conformance baseline](evidence/CONFORMANCE.md) is failing, not
waived. Until implemented, PUT fork/absolute-expiry headers return 501 before
body extraction or storage access, rather than acknowledging an operation with
those semantics silently removed. POST `Stream-Seq` is a per-incarnation,
byte-lexicographic token (`"10" < "2"`), committed with payload/producer state.
Absent leaves it unchanged; present-empty is a token. Producer retries ignore
changed tokens after fencing; token conflicts do not consume producer tuples.
Mixed-version operation and downgrade after token-bearing writes are unsupported.
Completion events track response-body completion, errors and cancellation;
`bytes_out` counts data frames yielded to the HTTP server, not proven client receipt.
The [real delivery fault](evidence/DELIVERY.md) distinguishes a truncated 200 response
from a completed response and a valid forwarded empty 204. Extractor/admission
rejections before the stream handler remain outside this completion observer.
Payload state is buffered and bounded (8 MiB per stream, 16 MiB per shard), with
backpressure rather than cold offload. Do not infer many-stream production capacity
from these local tests or treat illustrative Fermi targets as measured SLOs.

`vendor/electric/` preserves the exact upstream Apache-2.0 source and provenance.
`src/wire.rs` adapts its offset and JSON framing rules. Its independent WAL and local
tier manifests are not authorities for the replicated service.
