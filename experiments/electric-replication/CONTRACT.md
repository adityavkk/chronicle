# Electric engine replication experiment

This is a source-integrated extension, not Chronicle's Go/Redis default and not
the separate SQLite experiment on `rust/distributed-streams`. It is experimental
and not production-qualified. See the evidence ledger for executed gates; this
contract describes the intended implementation, not proof of completion.

## Provenance and reuse

The baseline is Electric's Apache-2.0 Rust server at
https://github.com/electric-sql/electric/commit/88793e76595d69be300731b9b25c58538923a53b
(2026-07-22, npm release 0.1.5; the Cargo manifest still says 0.1.0).
`engine/src`, `Cargo.toml`, and `Cargo.lock` were imported verbatim before edits;
`UPSTREAM-TREE.txt` records original Git blob identities. `engine/LICENSE` is
the upstream license. The initial import commit separates reuse from changes.

The June 26 article's source-era release is
https://github.com/electric-sql/electric/commit/c3b73d040eb72e32e97bb05644befce5abf780bc
(npm 0.1.2). It is NOT byte-identical to this baseline. July fixes address lost
multi-segment replay, torn tails, acknowledged deletes, fail-stop fsync, sidecar
recovery, and checkpoint amplification. The existing Chronicle ds-bench campaign
already uses 0.1.5. Compare matched unmodified 0.1.5 and adapted 0.1.5 builds;
never import headline blog throughput into a new result.

The engine has no library/storage plugin API. We preserve its real WAL codec,
positioned writes, segment allocation/roll, dedicated per-shard group-commit
threads, wire-file handlers, file-range bodies, Linux sendfile, and epoll SSE
reactor. We extend the source with a Raft journal record kind, non-destructive
resume, indexed WAL reads, and a request-dispatch hook. This is code reuse, not
merely compatible file formats. Original local-WAL mode remains separately
buildable. The replicated apply path uses native buffered wire-file writes
without a second WAL. There is no SQLite or RocksDB payload engine.

OpenRaft **0.9.25**, MIT OR Apache-2.0, source
https://github.com/databendlabs/openraft/commit/8815cdba2826f74e848acef361ad03f93bb1c3f8
owns elections, log matching, leader confirmation, learners and joint consensus.
Its `RaftLogStorage::append` callback must follow native `wait_durable`, not
enqueue. `save_vote` must also fsync before returning. Storage writes are ordered.
The alternatives are raft-rs plus manual Ready/HardState/read-index orchestration
or an OpenRaft prerelease. The async admission path now uses a narrow backport of
upstream's expected-leader write condition; [vendor provenance](vendor/README.md)
records the original crate, licenses and three changed files. Elections, commit
and storage rules remain pinned 0.9.25. We do not implement private consensus or
assume the previous experiment's proofs apply.

The apply worker persists a covering `Commit` marker in that same journal and
waits for fsync **before the first native handler in a committed batch**. This
barrier is not removed; it moves out of the consensus core's serialized command
loop so that core may queue work while apply waits. Both optional OpenRaft
`save_committed`/`read_committed` methods use their default no-op/None pairing.
The marker is private state-machine recovery metadata: startup rebuilds through
it before `Raft::new`, whose `applied_state()` supplies the recovered boundary.
Returning a private marker from `read_committed` while ignoring `save_committed`
would violate their paired API contract. A snapshot newer than the marker is
also authoritative; marker-covered replay chunks must not append older markers.
`ApplyRecovery.tla` precedes this change and checks private replay, partial apply,
newer snapshot installation, and crash boundaries, with three negative mutations.
This is a scheduling change, not a weaker durability mode or a parity claim.

## Ordering, durability and visibility

Fixed stream partitions are independent Raft groups. Partition identity hashes
the exact protocol path with fixed FNV-1a and a persisted partition count, not
node count. Each partition has one ordered log. Placement changes replicas of a
whole group through learner catch-up and joint consensus; they do not rehash
streams. Different groups can have different leaders and replica sets. One hot
stream is still ordered by one group and cannot scale without bound.

The authoritative journal is Electric's sharded WAL containing Raft entries,
votes, committed positions, suffix truncations, purge markers and snapshot
references. Entries contain the protocol request, not a second database state.
The in-memory index holds file locations/log IDs, not historical payloads.
Native stream files are the read materialization. Only committed commands reach
native handlers, so neither writer tail, durable tail, producer state, closure,
SSE notification nor a newly created stream can expose a speculative entry.

After HTTP parsing and full body collection, durable HTTP commands may omit only
`content-length`, `transfer-encoding`, `expect`, `connection`, `accept` and
`user-agent`. No mutation handler consumes these envelope fields. Preserve the
body, method, path, all other headers and their original relative/duplicate order.
In particular **Host is semantic**: create/fork Location headers use it, and
subscription creation captures it into its callback URL. Lean proves that this
stable filter preserves first-match lookup for every retained name, with a
negative mutation that wrongly drops Host. The complete native consumed-header
set and the Rust implementation remain review/property/conformance obligations;
the proof does not discover that set or prove HTTP parsing. Existing WAL entries
with additional envelope fields remain readable; this is not a schema change.

By default, writes use **quorum-fsync**. A synchronous 2xx response means the command was
replicated to a quorum whose WAL durability callbacks completed, committed, and
applied on the responding leader. A deterministic rejection may also be logged.
An I/O error applying a committed command is terminal, not a replicated 500 whose
effects differ across replicas. One-member mode is explicitly local-fsync, not
replicated durability. The opt-in **local-fsync202** append mode acknowledges only
local durable acceptance, not semantic success or quorum durability. Its distinct
receipt, await API, bounded admission, possible invalidation and committed-only
reads are specified in [ASYNC.md](ASYNC.md). Lifecycle and control mutations stay
quorum-fsync; no speculative read mode is offered.

Timeout, connection loss, 503, or lost leadership after submission is **unknown
outcome**, not proof of abort. No ingress automatically replays a mutation.
Clients use the protocol producer epoch/sequence for retry deduplication; retries
go through consensus even when an earlier request appears in local producer
state. Concurrent duplicate proposals can occupy multiple Raft entries but must
have one stream effect. Unidentified POST retries can duplicate data. Producer
state is captured in snapshots and reconstructed from committed replay.

## Usable consistency API

`Stream-Consistency` on GET/HEAD selects:

* `linearizable` (default): leader/quorum confirmation, wait for its confirmed
  commit position to be applied, then read. No stale fallback. During a minority
  partition it fails or times out.
* `prefix`: read only this replica's applied committed state. Freshness is
  unbounded; without a session token reads on different replicas may go backward.
* `session`: requires `Stream-Session` from a previous response. Wait until this
  replica has applied at least that group position, then read, or return 503.
  A token encodes cluster identity, partition, and committed log index. Reject a
  different cluster/partition or malformed token; do not treat it as an offset.

`Stream-Durability: quorum-fsync` remains the default. POST appends also accept
`local-fsync`; an operator can explicitly select that append default in config.
Unknown values are rejected, never silently downgraded. Synchronous successful
writes return `Stream-Session`; local acceptance instead returns `Stream-Receipt`.
Tokens fence the history, not a stream incarnation: a subsequent committed
delete can legitimately return 404. Clients maintain a token per partition.
There is no general cross-group transaction or globally consistent snapshot.
Cross-group forks use the specific retained-prefix ownership protocol in
`FORKS.md`; subscription creation uses per-group observation cuts in
`SUBSCRIPTIONS.md`.

For live reads the selected barrier governs the initial observation. Later SSE
and long-poll delivery is committed-prefix streaming, NOT a new quorum round for
each frame; a disconnected replica may stop delivering. Clients resume with
offsets and preserve their session token. Responses bypass shared HTTP caches
when requesting consistency: upstream cache headers must not defeat barriers.

Concurrent reads may share a confirmation only if it **started after each read
arrived**. Each invocation samples an atomic started-generation before joining a
serialized confirmation queue. It can reuse only a completed generation strictly
greater than its sampled generation. A read arriving during a round must wait for
another round; sampling the completed generation would let it reuse a stale
confirmation. Failed rounds stay failed and canceled rounds never advance the
completed generation. There is no time lease or stale fallback. `ReadCohorts.tla`
precedes implementation and checks arrival/write/completion/cancellation ordering,
with mutations for the wrong counter and a non-strict comparison. This assumes
OpenRaft's underlying confirmation is linearizable and waits for local apply.

## Recovery, checkpoint, snapshot and ownership

Assume non-Byzantine consensus participants, honest successful fsync, durable
directory fsync/atomic rename, no rollback of acknowledged disk state, and unique
node identities. Acquire a lifetime exclusive data-directory lock and persist
cluster/node/partition identity. A copied identity on another disk is NOT fenced
by flock: never run it concurrently; replacement uses a fresh node ID/learner.
Membership addresses are routing hints, never authority to acknowledge writes.

Resume the physical WAL after its last complete CRC-valid frame; never use the
single-node engine's `reset_after_recovery` on a consensus WAL. For retained entries
and the post-checkpoint suffix, **any nonzero torn or CRC-invalid frame is fatal,
including the active segment's tail.** Without
additional durable boundaries it cannot be distinguished from corrupted
acknowledged data. Only all-zero final preallocation padding is discarded; a
nonzero suffix behind a zero header is fatal. Checksums detect accidental
corruption, not malicious edits. WAL I/O errors are fail-stop, with no in-place
fsync retry. Recovery rebuilds the index and reads vote/commit/snapshot/truncation
records in physical order. Replacing a corrupt node requires a fresh identity and
learner restore from a healthy quorum, not silently trimming its authoritative log.

On boot, native hot files are rebuilt from the last durable snapshot plus the
committed journal suffix before serving. Never infer commitment from file size.
Snapshot files stream payloads in bounded windows, include complete native
metadata (including producer state), applied position and membership, and are
checksummed. Persist the file and its directory before journaling its reference.
Install into a new materialization generation before atomically changing the
in-memory view. Raft purge may follow only a durable snapshot. Physical segment
reclamation additionally requires a journal checkpoint covering vote, membership,
commit and retained locations; a logical purge alone does not authorize unlink.

The reclamation contract (`JournalReclaim.tla`, specified before implementation)
captures the full location-only index and final physical frame under the journal
lock, waits for that frame's native durability barrier, then writes a checksummed
checkpoint, fsyncs it, atomically renames it and fsyncs the directory. Only then
may segments older than both the replay cut's segment and every retained entry's
segment be unlinked. Appends after the cut remain in the original WAL, not a
second payload log. Readers pin retained locations against unlink while reading.
Recovery validates retained frames, then resumes strictly after the recorded
physical cut; it never substitutes a stale checkpoint for corruption. A crash
before checkpoint publication retains the old replay path. A crash after it can
leave extra old segments but cannot lose needed ones. Directory fsync failures
are terminal; successful rename alone does not authorize reclamation.

Snapshot cleanup serializes with snapshot construction/installation. A durable
new journal reference is required before unlinking old files. Snapshot readers
open their file under the journal lock before cleanup; open file descriptors
survive unlink on Linux. In-progress/unreferenced snapshot files are cleaned only
after a successful replacement or recovery, not while another builder is active.
Disposable old hot generations can be removed on restart, before serving; live
readers may still own an old generation, so it is not deleted speculatively.

Cold-tier support is retained in the upstream source but **disabled in replicated
mode** until object identities include cluster/group/incarnation/range/checksum,
manifest publication crosses consensus, and reference-safe GC is implemented.
Otherwise a follower or old leader could delete another replica's objects.
TTL uses the committed clock in `TIMED-STATE.md`, never local-clock expiration.
Cross-group forks and subscriptions retain durable control state in the same
journal and snapshot as stream data. Experimental node identity 6 and snapshot
envelope format 5 include bounded receipt outcomes (see `ASYNC.md`) and reject
earlier data. No data migration or mixed-version operation is supplied.
Physical WAL reclamation and obsolete-snapshot cleanup now have native-file
property and real-process syscall-fault qualifications. Cold-tier ownership,
terminal-fence compaction and production auth/TLS remain gates. No Internet
exposure is qualified here.

## Formal and empirical boundaries

TLA+ models the storage-to-consensus integration assuming Raft log matching and
leader completeness; it does not re-prove Raft. Bounded safety, fairness-qualified
liveness, and deliberately broken durability/publication mutations are required.
Lean proves deterministic prefix/session/quorum arithmetic contracts without
`sorry`; it does not prove Rust refinement, syscall correctness or disk behavior.
Property tests must exercise real WAL restart/truncation boundaries. Multi-process
histories retain unknown outcomes and use an independent checker. SIGKILL on one
orb is not independent-disk, power-loss or AZ qualification.

Benchmarks pin ds-bench at `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf` and use
the existing corrected seeding/window rules. Local matched-source measurements
are qualification only. Separate unmodified local-fsync, one-node adaptation,
and three-node quorum-fsync costs; preserve errors, seeds, exact byte probes,
raw windows, process/disk/network samples and source/config/binary hashes.
Paid evaluation and publication require separate authorization.
