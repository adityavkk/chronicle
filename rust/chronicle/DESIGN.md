# Durable Streams, replicated

Decision before implementation: use OpenRaft 0.9.25 (MIT OR Apache-2.0), stable
revision 8815cdba2826f74e848acef361ad03f93bb1c3f8. It owns elections, durable-log
replication, learner catch-up, joint consensus and read barriers. raft-rs 0.7.0
plus raft-engine 0.4.2 is credible but requires more correctness-sensitive Ready,
HardState, read-index and membership orchestration. Do not copy OpenRaft's
RocksDB example's premature log-flush callback.

SQLite WAL with synchronous=FULL owns log/vote/commit and applied state. Blocking
transactions run on blocking workers, serialized per database. Completion means
transaction commit, not enqueue. No independent Electric WAL or speculative tail.
Each fixed shard is one Raft group; group zero holds placement intents. Stream
identity hashes tenant plus canonical path, independent of node count. All groups
share one binary, not one consensus group per stream.

Strict reads use ensure_linearizable before taking the applied view. Optional
stale reads expose only a committed prefix, not freshness. Timeouts mean unknown
outcome. Every append (including a retry) crosses consensus; deterministic apply
suppresses duplicate effects. This deliberately avoids a pending duplicate fast
path. Concurrent duplicates may have distinct log indexes, never distinct effects.

The app state machine maps formal Write/Create/Close/Delete to Append/Create/
Delete commands; append-and-close is atomic. Incarnation fences lifecycle retries.
Byte offsets are Electric wire-byte lengths, not Raft indexes. TTL expiration is
a replicated conditional deletion with expected incarnation and deadline. System
time is supplied in commands, never consulted by deterministic apply.

Snapshots contain payloads, per-stream incarnation, config, producer epoch/sequence
and original response frontier, lifecycle, expiry, applied index and membership.
Initially snapshot size is bounded; oversized shards backpressure rather than
silently exhausting memory. Cold offload, AP inbox, arbitrary splitting and
individual-stream movement are deferred. No local tier manifest is distributed
authority. Many streams scale across groups; one hot stream remains leader ordered.

Placement reconciliation records intent in group zero, adds learners with blocking
catch-up, changes membership through OpenRaft, then records completion. A stale
controller cannot bypass the data group's term or committed membership. Movement
is whole-shard, one at a time, with a cooldown. Target rotation uses node health
and supplied failure domains, not measured disk/CPU/memory capacity. State-machine
byte limits backpressure writes; they are not resource-informed placement.
Resource-informed placement and leadership balancing remain unfinished. A native
election preference is available only as the default-off experimental policy
documented in `formal/README.md`; it is not directed transfer. Failure domains are
inputs, not inferred from Kubernetes node names in production.

Durability target: acknowledged writes survive loss of one of three independent
replica disks/nodes, assuming honest fsync/storage and no Byzantine faults. Production
replicas should span three AZs in one region. Local k3d on one host does not test AZ,
power-loss or independent disk failures. Loss of majority stops strict operations.

Formal evidence is in formal/. Raft is assumed, not re-proved. TLC is bounded;
Lean proves selected pure properties. Rust refinement is not mechanized. Tests,
fault histories, storage hooks and independent review must bridge that gap; none
constitutes an end-to-end proof of hardware durability.
