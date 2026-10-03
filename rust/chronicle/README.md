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
* OpenRaft's pinned storage contract suite passes, alongside SQLite VFS faults,
  snapshot/reopen producer and membership checks, and a slow-body admission test.
  Placement completion now reads applied membership, not effective Raft metrics.
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

## Explicitly unfinished

This is a bounded replicated vertical slice, **not the full requested deliverable**.
Protocol conformance beyond the implemented request subset, SSE/long polling,
zero-copy sendfile, resource-informed placement and leadership
balancing, deeper replication/storage trace linkage, broader I/O-fault schedules, stronger
admission/snapshot crash schedules, and equal-semantics performance baselines remain.
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
