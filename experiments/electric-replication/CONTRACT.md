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

The qualified comparison baseline uses OpenRaft **0.9.25**, MIT OR Apache-2.0, source
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

The current isolated candidate uses stock **0.10.0-alpha.36**, not the vendored
backport, from
https://github.com/databendlabs/openraft/commit/0acd6b8d547ad4468f66708b05bc03baaf04c7c8.
Both `openraft` and `openraft-legacy` are exact registry pins under MIT OR
Apache-2.0. Their crate SHA-256 hashes are respectively
`858ce506e827306c40820757d94bcf086b5ad54f7c8b9c47578362e4c048e842`
and `ed7cd03a9781c02e11be6fe50f9a4b57ab5daa909f0eeb3c8963e365637b62b7`;
both packaged `.cargo_vcs_info.json` files identify that upstream commit.
`Cargo.lock` pins the complete dependency graph. No registry source was modified.
The candidate's qualification results must be kept distinct from 0.9 results;
being newer or compiling does not establish either safety or performance.

0.10 submits native append IO without awaiting its durable callback on the core
loop. The callback still follows actual native fsync. Its exclusive
`truncate_after` maps to the journal's inclusive first-removed event, including
the empty-prefix boundary. The state-machine stream reads/applies at most 64 log
entries per cohort; it does not materialize an entire recovery range in RAM.
Replication reads are likewise capped at 64 entries. `ReadPolicy::ReadIndex`
remains explicit; the new admission lease is **not** a switch to lease reads.
The upstream legacy network adapter preserves 64 KiB file-backed snapshot chunks.

Candidate data identity is **10** and consensus RPC paths start `/_raft10/`.
Identity-9 and earlier directories are rejected before native recovery; old
`/_raft/`, `/_raft8/` and `/_raft9/` RPCs are rejected before decoding or assigning
a log entry. Earlier receivers do not recognize the new consensus path. Identity
10 and snapshot envelope `ERSP0007` preserve retirement fences and the original
source configuration needed for existing-fork retries in [FORKS.md](FORKS.md).
There is **no mixed-version rolling upgrade, automatic data migration or older
snapshot import**. The 0.9 checkout/binary and
its directories remain the rollback baseline; candidate qualification uses fresh
directories. The separately approved lease-admission difference is specified in
[ASYNC.md](ASYNC.md). Supported production migration remains an open gate.

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
Capture metadata from the live state at the exclusive applied cut, not from
lagging hot sidecars. Encoding it directly into the archive avoids separately
rewriting/fsyncing those disposable sidecars; the archive's durability barriers
remain mandatory. Snapshot-only restore must preserve dedup and timed state
without relying on a pre-snapshot journal to repair stale metadata.
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
second payload log. Indexed readers hold the index mutex from location lookup
through the physical read. Capturing the cut joins those earlier readers; later
readers can only select retained locations or frames at/after the cut. Thus unlink
does not need to hold the index mutex through filesystem I/O. Releasing a reader's
guard between lookup and I/O is unsafe (`UnpinnedReader` negative mutation).
Synchronous checkpoint/reclamation I/O must hand off the Tokio worker, without
dropping maintenance serialization or any durability barrier. The finite model
checks reader/reclamation safety and stable-period progress, not OS scheduling,
disk latency or arbitrary cancellation. A delayed real-fsync process history
must separately qualify scheduling and exact recovery.
Recovery validates retained frames, then resumes strictly after the recorded
physical cut; it never substitutes a stale checkpoint for corruption. A crash
before checkpoint publication retains the old replay path. A crash after it can
leave extra old segments but cannot lose needed ones. Directory fsync failures
are terminal; successful rename alone does not authorize reclamation.

OpenRaft's pinned [core awaits storage purge](https://github.com/databendlabs/openraft/blob/0acd6b8d547ad4468f66708b05bc03baaf04c7c8/openraft/src/core/raft_core.rs#L2436-L2457).
Worker handoff alone therefore cannot keep that group responsive. The
[storage contract](https://github.com/databendlabs/openraft/blob/0acd6b8d547ad4468f66708b05bc03baaf04c7c8/openraft/src/storage/v2/raft_log_storage.rs#L143-L148)
requires serialized, hole-free logical deletion, not immediate physical GC.
Purge must first wait for its covering durable snapshot reference and persist
the native Purge record; only then may it signal maintenance and return. One
maintenance task per group consumes a single coalescing notification before
checkpoint I/O. Work arriving during I/O remains pending, rather than creating
an unbounded task queue or being lost when the earlier operation finishes
(`LostWake` liveness mutation). Startup also signals reclamation, recovering a
notification lost in a crash. This is not a new authoritative WAL or a changed
checkpoint format. Storage errors still fail-stop, including background errors.
The finite model overapproximates requests by permitting work after every durable
frame; it assumes eventual I/O completion, not a bound on disk growth under a
stalled device. No additional read/ack or minority-availability guarantee follows.

Snapshot cleanup serializes with snapshot construction/installation. A durable
new journal reference is required before unlinking old files. Snapshot readers
open their file under the journal lock before cleanup; open file descriptors
survive unlink on Linux. In-progress/unreferenced snapshot files are cleaned only
after a successful replacement or recovery, not while another builder is active.
Disposable old hot generations can be removed on restart, before serving; live
readers may still own an old generation, so it is not deleted speculatively.

The pinned-cut snapshot candidate (`SnapshotCut.tla`, specified before its
implementation) separates the view cut from the snapshot lifecycle lock. Under
the exclusive view, capture the applied ID, membership, committed clock, complete
control metadata and each live stream's metadata, an independently opened inode
handle and exact file length. Copy only that prefix after releasing the view:
later appends must not extend it, and deletion/recreation must not replace its
inode. Replicated hot files are append-only; failed append rollback cannot
truncate below an earlier applied cut. Native cold compaction remains disabled.
Opening files by path or reading metadata after releasing the cut is forbidden.

One lifecycle lock serializes build, install, durable reference publication and
cleanup, in that order, independently of apply. Installation still replaces the
view exclusively. This prevents an old builder publishing over a newer installed
snapshot or cleanup deleting another builder's file. Capture may pause apply
for metadata cloning and file opens; payload copy/hash/fsync must not. Memory is
one extra metadata cut plus bounded copy buffers, not a payload-sized buffer;
file descriptors scale with live streams and descriptor exhaustion is a storage
failure, not permission to produce an incomplete snapshot. Metadata growth itself
still needs the lifecycle qualification below.

The server uses Tokio's multi-thread runtime, including with one worker; blocking
capture/copy sections hand that worker off for Raft/network progress. They remain
non-cancellable disk operations, not a disk-time guarantee. `ApplyRecovery.tla`
and `JournalReclaim.tla` retain their durability/order assumptions; `SnapshotCut`
checks prefix/metadata identity, serialized install and durable cleanup. Real-file
generated tests must cover appends, unlink/recreate and snapshot-only restore;
delayed-fsync process histories must additionally check same-group apply progress,
membership/term stability, exact committed prefixes and restart. These models do
not prove Rust refinement, descriptor behavior or executor scheduling.

Cold-tier support is retained in the upstream source but **disabled in replicated
mode** until object identities include cluster/group/incarnation/range/checksum,
manifest publication crosses consensus, and reference-safe GC is implemented.
Otherwise a follower or old leader could delete another replica's objects.

Review of the pinned native engine identifies additional integration boundaries,
not permission to enable its standalone offloader unchanged:

* `tier::offload_one` uploads, size-verifies and durably updates a **local**
  manifest before unlinking its chunk. That is not a replicated ownership or
  publication decision. Native object keys include the stream file/incarnation
  and offset, but not our cluster/group identity or content checksum.
* `handlers::stream_resolved_body` retains remote keys for lazy range GETs.
  Keeping an `Arc<StreamState>` does not pin those objects against
  `gc_remote_segments`. Native local file descriptors survive unlink; an object
  key does not provide that guarantee. Old snapshot metadata also needs its
  remote references protected until restore/transfer no longer needs them.
* Native GC waits at most 1,000 × 2 ms for an uploader, then ignores remote
  delete errors. A slow or indeterminate PUT can finish after that deletion;
  process death is not proof that an external request never completes. Durable
  retry/reconciliation and explicit retired-owner fencing are required.
* The imported filesystem object fixture used lossy slash-to-underscore keys,
  one shared temporary name and no directory fsync after rename. The local
  adaptation now encodes opaque keys injectively, uses create-new temporary
  files, and fsyncs the file and complete directory chain before PUT succeeds.
  DELETE reclaims empty key directories and fsyncs surviving ancestors, including
  missing-object retries. A failed PUT/DELETE is still an unknown outcome. The
  fresh fixture layout intentionally does not read old flat fixture directories.
  Generated reopen/range/reclamation tests, concurrent uploads and real
  ENOSPC/file-EIO/directory-EIO tests cover these boundaries, not independent
  object-server power loss or provider durability.

The native wire/range and bounded streaming paths remain the intended reuse
boundary. Object publication, read/snapshot pins, delayed uploads, ownership
transfer and reclamation need a formal contract and actual file/object fault
tests before any replicated cold-tier flag is accepted. No cold durability or GC
qualification is claimed by the hot-only conformance and benchmark results.

### Shared cold ownership contract

`ColdOwnership.tla` now has two complementary checks: the original focused
participant/recovery model and the **new Begin-ID/certified-inventory model**.
The earlier one-object pass does not verify the inventory algorithm. Neither
model proves the runtime. The native implementation below is opt-in and remains
qualification-only (`cold` requires `fault_testing: true`); the evidence ledger
distinguishes its executed local checks from production acceptance.

Use one immutable object namespace per cluster/group. Every upload first obtains
an accepted **committed singleton Begin**, whose unique log position is its birth
ID. Include that ID, stream incarnation, range and checksum in the key. A batch
index alone cannot distinguish two accepted Begins; Begin is not a batched data
append. A failed or unknown Begin response never authorizes guessing an ID and
uploading. A matching pending Begin and a verified durable PUT are prerequisites
for committed manifest publication. Retrying a PUT uses exactly the same bytes
and identity; a checksum mismatch is an error, not an overwrite strategy.

Publish consumes the pending descriptor and creates a live reference; Abort
consumes it without publication. Retiring the final stream/ancestor/grant
reference removes the live descriptor. **Absence is not permission to publish**:
an old Publish must fail if its matching pending Begin is gone, even after all
terminal reply records have been collected. Reusing an old object for a new
stream requires a new Begin/key, not resurrection of the retired identity. Thus
irreversibility does not require one permanent tombstone per historical object.

The native manifest/range resolver remains the reuse boundary between remote
ranges and the fd-backed hot tail. Publication, references and retirement cross
consensus; payloads do not enter a second authoritative WAL. Offloader uploads,
ordinary range reads and DELETE retries run outside state-machine locks.
**Native JSON-fork apply/replay can resolve remote bytes under the apply lock.**
Provider failure there fail-stops the replica; it cannot become a deterministic
rejection with different effects on different replicas. An object outage can
therefore block recovery or availability, not just cold reads. This preserves
native fork semantics but is a significant availability cost. A PUT/DELETE timeout or I/O
error has an unknown outcome: it neither drops the pending Begin nor proves the
object absent. Even after an Abort or successful DELETE, an already issued PUT
may complete. Recovery and repeated inventory-based collection handle that
physical resurrection without allowing logical reattachment.

Track **storage participants independently of Raft voting membership**. Removing
a voter does not discard its old snapshots, committed-but-not-applied journal
or cold readers. Every participant holds a monotonic recovery floor. Advancing
it requires a complete durable snapshot at/after the floor, **completed durable
logical purge** through that floor, an admission fence against older reads or
transfers, and drainage of earlier readers, builders and snapshot transfers.
Applied progress alone is insufficient: replaying a JSON-fork command can need
an object's bytes even when subsequent committed deletion removed all live
references. Snapshot publication alone is insufficient while retained log
replay can still require that object. Issuing OpenRaft `trigger().purge_log`
does not prove the storage purge has completed.

The committed group GC floor cannot exceed any registered participant's floor.
An unreachable removed-but-undrained participant therefore blocks **advancement**
beyond its last certificate; already certified old garbage may still be removed.
There is no timeout or unsafe force-GC. Voting membership or applied progress is
not a substitute for a storage-participant certificate. Administrative abandonment
of a lost disk requires an independently qualified fencing/replacement protocol;
it is not modeled as ordinary membership removal here.

Register a fresh learner **before snapshot transfer** through an ordered group
decision, pinning the floor observed by that registration. Waiting until install
finishes is unsafe: the sender may crash and lose its in-memory transfer pin
while the receiver retains an old snapshot. Reject snapshots below that learner's
registered floor. A continuously registered identity may restart using its
durable recovery certificate without an online quorum lookup; requiring quorum
before every member can replay would deadlock a full-cluster restart. A drained,
uncertified or replacement identity must bootstrap from an authorized current
snapshot before native replay or serving. A copied disk is not a new identity.

#### Certified inventories replace per-deletion tombstones

At an exact applied cut `F`, capture **all live and pending keys**, including the
currently active inventory object. Freeze that set; later retirement cannot
change the captured bytes. Allocate the new inventory's own Begin **after** `F`,
upload it durably, then commit activation only when every registered participant
has certified recovery at/after `F`. The ordering avoids an inventory needing to
contain itself. Registration and activation are serialized committed decisions,
not a racing membership query followed by external deletion.

An inventory descriptor binds its exact cut, immutable object identity, length
and checksum. A physical key is deletable only when its birth is at/below that
descriptor's certified cut **and** it is absent from that exact inventory.
Combining a newer cutoff with an older protected set is unsafe. An unreadable,
partial or checksum-invalid inventory grants no deletion authority; it is not
an empty set. Newly born objects above the cut are ineligible even if absent.

Replacement retires the previous inventory from current metadata, but does
**not** immediately delete its bytes. The replacement captured that old root,
and an older durable snapshot can still require it. A subsequent certified cut
that excludes the old root can collect it. Inventory objects, including aborted
or abandoned uploads, follow the same Begin, pending, retirement and GC rules
as payload objects; there is no separate immortal chain of inventory manifests.

Each scan pins a complete hash-verified inventory into an immutable local
handle and fixes its finite birth-prefix bound for that round. The implementation
must use bounded streaming buffers and, when necessary, a pinned local file;
"complete" does not mean loading the entire inventory into RAM. Deletion of the
remote inventory cannot invalidate an already verified local handle. Newer
inventory activation must not continually reset the round's cursor. Finish the
round, then wrap over the cumulative eligible prefix, not only births since the
previous inventory or one apparently clean listing. Repeated cumulative scans
catch old PUTs that arrive behind a cursor or after a previous DELETE.

Recovery restores the active descriptor, participant registrations and durable
certificates from the snapshot plus committed WAL before resuming scans. An
incomplete scan can be discarded and restarted; retirement authority cannot.
The full-cluster restart check permits local certified recovery without a quorum
bootstrap dependency. It does not permit an uncertified stale disk to replay
below its authorized floor, and does not solve production identity revocation.

#### What the model checks, assumes and bounds

The inventory invariants are:

| Predicate | Obligation |
| --- | --- |
| `InvDataSafe` | Live objects, pinned readers, durable snapshot/replay roots and in-flight snapshot transfers retain physical bytes. |
| `InvNoResurrection` | No terminal object becomes live again. |
| `InvDeleteSafe` | No issued deletion targets a live or pending key. |
| `InvAccounting` | Every accepted Begin is pending, live or terminal, including unknown upload outcomes. |
| `InvCertificates` | A participant's certified floor never exceeds its durable snapshot or completed purge. |
| `InvShape` | Ownership sets are disjoint; at most one pending and one live inventory descriptor, with a consistent active root. |
| `InvMonotone` | Once eligible, an old key never becomes protected again. |
| `InvReclaimed`, `InvRestarted` | During the specified stable period, covered retired keys become permanently absent, and a whole cluster can restart and finish the history. |

The original component explores one object, two established participants and one
learner. The inventory component explores six keys: two payloads, an aborted
inventory, and three replacements. Its asymmetric history places live/pending
keys at exact cuts and retires them between capture and activation. The primary
inventory configurations start from empty with one storage participant; the
two-participant removal/restart and learner-transfer configurations start from a
reachable second-activation cut, with older snapshots still needing the prior
payload/inventory. These are **bounded suffix checks**, not all multi-node
interleavings from genesis. The scan configuration abstracts endless newer
inventories by their unchanged protection of the already retired target prefix
(`p`, `q`, `a`, `i1`). It is a scanner-starvation check, not a representation of
every later inventory generation or a proof that the final `i2`/`i3` roots are
eventually collected; that needs a later cut excluding them.

Safety includes successful and unknown PUT/DELETE results, delayed uploads,
reader pins, transfer-source crash, removal without drainage and loss of volatile
scan/acknowledgement state at whole-cluster crash. Actual external I/O is not
executed by TLC. Snapshots, completed purge and certificates are distinct atomic
durable events; fsync, rename, WAL framing, consensus correctness and successful
object-store durability are assumptions, not derived from the state count.
The committed history and unique accepted Begin order are supplied to the model,
not a second implementation of Raft. Historical `dead`, captures, object contents
and delayed operations are ghost/server state, not mandated retained metadata.
The stipulated replay dependency at cut 7 abstracts a committed JSON fork;
this model does not execute its Rust handler or prove composition with the
cross-group grant protocol and arbitrary reference multiplicities.

Stable-period liveness assumes eventual participant recovery, no new indefinitely
pinned readers/transfers, eventual successful retried I/O, finite old PUTs, and
weak fairness of enabled commit, checkpoint, completed-purge, restart and scanner
actions. Its stable configurations exclude recurring unknown I/O/failures; safety
does not. Listings must eventually enumerate every surviving eligible key, with
finite rounds and eventual visibility after the final late PUT. The model uses
a sorted finite birth prefix, not a provider's actual pagination API. A provider
with omissions, unreliable continuation, permanent retention locks, or endless
new late PUTs does not satisfy this liveness contract. The runtime must establish
these assumptions or weaken its reclamation claim explicitly.

The intended authoritative cold-GC metadata bound is proportional to **current live
references + admitted pending Begins + registered storage participants**, plus
one active and at most one pending inventory descriptor; it is not proportional
to all historical deletions. Inventory bytes scale with live/pending references
at their represented cut. This follows from the implemented representation and
the absence of terminal reattachment authority; TLC checks only its finite
shape, **not an unbounded inductive proof or a Rust memory bound**. The runtime
bounds pending Begins at 64, registered participants at 64, payload objects at
8 MiB and each inventory at 256 MiB. Admission fails rather than dropping state
at these bounds. Spools are unlinked immediately and held by descriptors;
completed scans release their verified local files. Permanent participant
drainage/revocation is not implemented: removed nodes must rejoin and certify.
Physical orphan bytes and old inventories have no unconditional
count/time bound under stalled participants, arbitrarily delayed PUTs or storage
failure. An arbitrary retired key is eventually collectible only after a later
certified cut excludes it and fair cumulative scanning reaches it after its
last upload. Continuous-growth reclamation rate versus upload rate is unproved.

An SSE response conservatively holds its original pin until disconnect; there
is no advancing cursor watermark yet. An indefinitely connected reader can
therefore intentionally prevent newer certified cuts and physical reclamation.
Large JSON values without a boundary inside 8 MiB stay hot rather than being
split unsafely or silently rejected by the stream protocol. Hot-file reclamation
does not imply all older native WAL segments have already been physically
collected; their independent durable checkpoint/reclamation rules still apply.

Inventory uploads use bounded multipart buffers. A crash can leave incomplete
provider uploads that ordinary object listing cannot enumerate. The bucket
must have an incomplete-multipart abort policy; Chronicle does not currently
install or verify it. Completed late uploads still enter cumulative GC. Do not
configure object expiration or out-of-band deletion for live cold keys. The
local campaign exercises late real multipart completion across uploader crash,
not the provider's eventual incomplete-upload lifecycle enforcement.

The fail-fast runner is `bash experiments/electric-replication/scripts/formal.sh`;
configs are `formal/cold{,-live,-inventory,-inventory-live,-scan-live,-participants,
-transfer,-restart}.cfg`. `evidence/formal/` retains exact TLC outputs,
counterexamples and source hashes; the evidence ledger records completed results.
New negative controls distinguish live/pending cuts, immutable captures, missing
inventory reads, old-root preservation, unknown uploads, completed purge, reader
and learner pins, restart certificates, quorum-bootstrap deadlock, and starvation
or missed late PUTs in delta-only/one-pass/resetting scans. A timeout or parser
failure is not a detected mutation.

There is no TLA-to-Rust refinement proof, unbounded proof, cold-specific Lean
proof or independent provider/disk qualification. The runtime's correspondence
and local tests below narrow that gap; they do not remove those limitations.

#### Native implementation and correspondence

Cold clusters use identity **11**, RPC `/_raft11/` and snapshot envelope
`ERSP0008`. Hot-only identity 10/`ERSP0007` is unchanged. Cold snapshots retain
ownership descriptors, pending Begins, participant floors and the high node ID
alongside native manifests and hot tails. Existing formats are not migrated;
mixed hot/cold membership and editing the identity marker are unsupported.
All nodes in a group must address the same durable object namespace. The
serialized identity binds public provider configuration, never credentials.

| Model action/obligation | Runtime boundary | Executed-test responsibility |
| --- | --- | --- |
| `Begin`, verified PUT, `PublishP`; `InvAccounting` | `cold::State::apply`, `cold_io::cold_seal`/`cold_finish`; singleton consensus commands and mandatory native commit marker | Held upload across majority/minority split; retry the original short range after append; never publish an absent Begin. |
| `Capture`, `Activate`; `InvDeleteSafe` | `State::capture` merges live+pending+old root into sorted fixed-width records; new Begin follows capture; activation checks every participant | Native-WAL generated exact-cut, previous-root, immutable capture and retry tests; omit-pending implementation mutation. |
| Snapshot, completed purge, `CertifyNode` | `Machine::certify_cold`, `Journal::recovery_covers`, `Runtime::save` | Snapshot alone fails certification; `recovery_covers` joins the native LSN because the index updates at staging; native fsync failure/restart checks. |
| Reader/transfer pins | View-locked HTTP admission, `protect_response`, `SnapshotFile`, snapshot builder/current-snapshot APIs | Real lazy HTTP reader and actual OpenRaft snapshot transfer span retirement; ignoring pins fails the native-WAL property. |
| `AdmitLearner`, removal, `RestartNode` | Committed `Register` precedes bootstrap/add-learner; locally fsynced identity-bound certificate; snapshot-floor validation | Receiver must actually install a snapshot and read cold bytes during JSON-fork replay; removed node restart/rejoin and full-cluster restart. |
| `StartScan`, `Scan`, `WrapScan`; `InvMonotone` | Complete hash-verified local inventory; ordered `list_page`, fixed round/cut, cumulative restart of scanning | Corrupt inventory grants no deletion; unknown DELETE and PUT arriving after earlier scans; repeated generations and restart. |
| `InvNoResurrection` | `Publish` requires the still-pending descriptor; `Retire` checks native stream/ancestor ownership | After abort/retirement and subsequent inventory replacement, stale resume fails even if bytes physically reappear. |

Certification checks known pins before requesting snapshot/purge work, then
rechecks under the exclusive view lock at the actual durable certificate seam.
The preflight only avoids work that cannot yet earn a certificate; it is not a
substitute for the final check. Without it, a newer snapshot/purge could make
OpenRaft deliver a newer snapshot instead of replaying the originally held
suffix; `cold-runtime-008` retains that failed coverage assertion. The passing
`009`, `011` and final `012` observe actual remote GETs during the learner's
JSON-fork replay. `010` retains a live-feed fixture error, not a passing run.

`VerifiedBlob` verifies a complete payload object's SHA-256 before exposing a
range. This costs a full object fetch (at most 8 MiB) for an uncached subrange;
it is not a cold-path sendfile or performance-parity claim. Inventories stream
to files and use binary-search membership, rather than a history-sized hash set.
The native local tail still uses fd-backed reads/sendfile. Native compaction
copies the remaining tail in 64 KiB windows. Standalone sealing/offloading/remote
deletion is disabled on consensus-managed tiers, so it cannot race the owner.

Cold inline SSE bounds binary/text reads at 64 KiB, carrying an incomplete UTF-8
suffix to the next frame. JSON windows grow only until a complete top-level value
fits, never beyond 8 MiB; the replicated command admission cap is 1 MiB. The
control offset advances by emitted wire bytes, not the observed full tail, and
up-to-date/closed flags wait for that tail. The previous whole-prefix allocation
failed the generated native-WAL/reopen regression; the final property checks
binary, Unicode and nested/escaped JSON at both sides of the window boundary.
The real MinIO campaign separately reconstructs an actual three-window cold SSE
feed followed by committed append/closure. This is a bounded per-read allocation,
not bounded aggregate connection count, fast remote subranges, advancing pin
watermarks or a cold fanout performance claim.

`/_admin/{group}/cold` exposes bounded, versioned status, named counters and
qualification controls. The counters include publication, blocked certificates,
scan/delete outcomes, inventory failures and HTTP cold bodies; offloader
verification GETs are not counted as HTTP cold reads. This is diagnostic
observability, not completed production metrics/tracing, authenticated admin
access, TLS or tenant isolation. The whole experimental listener remains
loopback-only. S3 credentials come from `ELECTRIC_COLD_ACCESS_KEY_ID` and
`ELECTRIC_COLD_SECRET_ACCESS_KEY`, not node JSON or consensus state.

#### Work-driven offload and local admission

The scheduler retains one uploader per group. Each pass gives one pending retry,
one retirement and a due inventory a turn, then attempts at most 16 new payloads,
wrapping the stream cursor immediately. A productive pass yields and continues
without a 100 ms tax per object. Apply sends a coalesced notification; an idle/retry timer
also covers leadership changes and missed notifications. Failures retain their
durable Begin and impose a retry delay, not a busy retry loop. There is no
unbounded candidate queue or new concurrent PUT pipeline. Selection scans the
current catalog; it does not introduce a distributed scheduling index.
Existing durable pending descriptors remain capped at 64 per group, payloads
at 8 MiB, and the scheduler holds only its current payload plus cursor. A pass
bounds attempts, not wall time: provider timeout/retry and quorum delays remain
real costs. A permanently unavailable provider cannot satisfy a liveness claim.

Cold resource policy is node-local, outside the identity-11 serialized cold
configuration. It must not reinterpret committed commands or change receipts,
leases, fsync, publication, pins, certificates or GC authority. All groups share
one admission budget. Originating data writes count unreclaimed local stream
tails, staged fork data, retained unapplied journal bytes and outstanding byte
credits. A successful local 202 or canceled HTTP request does not return credit.
Resolved credits survive until a view-locked observation accounts for their
effects; restart and stale/failed observations close admission. The next sample
must include unapplied WAL debt, not only readable tails.
Sampling takes all group view read locks in group order before the node budget
mutex. Independent admission latency between groups on that node is not
promised; this is not a lock-free reservation or cluster-wide coordinator.

The default policy is 512 MiB of node-wide originating backlog and 2 GiB of
filesystem headroom, configurable as `cold_admission.max_backlog_bytes` and
`cold_admission.min_free_bytes`. HTTP 429 means no admission (backlog, stale
observation, or the existing proposal bound); 507 means disk headroom; 413 means
a request exceeds the configured budget. The versioned `/_admin/{group}/pressure`
observation reports node-wide sampled debt, active/spent credits and rejection
reasons. Polling multiple groups does not make those counters additive.

The charged command classes are native stream `PUT`/`POST` and fork
`Reserve`/`Chunk`, including data-bearing replication batches. Empty stream
creation and close POSTs are charged too. Deletion, cold maintenance,
subscription control-plane commands and the other non-data command classes are
exempt; snapshot reception has separate disk accounting. This is not a tenant
quota or general bound on control-plane metadata. That state, maintenance,
telemetry and unrelated filesystem writers still need operational headroom.

At 75% backlog pressure the worker seals partial tails. It also remembers the
largest rejected request's required headroom: high water alone deadlocks when
small open tails occupy 1.25 MiB of a 2 MiB budget and a 1 MiB request retries.
This is one bounded scalar, not a stored request or a promise to reserve space
for that client. A covering observation clears satisfied demand; restart loses
the hint, so a client must retry. Published short prefixes must also actually
compact, rather than waiting for a full segment of reclaimable bytes.
Recovery and snapshot installation reclaim already-published short prefixes
left by the former threshold before admitting/serving the restored view. This
uses the same native intent/fsync/rename compaction, not a new payload format.

Follower replication needs a different rule: applying the originator's backlog
cap to AppendEntries can stop it before a later cold-publication entry that
would release that backlog. Followers retain disk-headroom admission, not that
leader backlog cap. This is **not** an atomic cluster-wide byte reservation or a
hard cap on every follower's hot bytes. A pressured follower may stop receiving;
a healthy quorum can continue, and restoring space or replacing that replica
may be necessary. RPC timeout must not release credits for still-running I/O.
Snapshot installation must budget its whole received image, not just the final
64 KiB chunk. Recovery samples before admitting new client growth.
In particular, different group leaders can each originate their node's budget
before the other node receives it. Neither the configured 512 MiB nor the
originator-only `AdmissionSafe` model is a 512 MiB bound on total physical data
at every replica. Transport admission additionally protects each receiving
node's measured disk floor; maintenance and filesystem consumers remain the
explicit exceptions below. There is no distributed reservation protocol.

Disk admission is conservative accounting plus measured filesystem free space,
not perfect physical reservation. Space for WAL segment allocation, snapshot
replacement, compaction, pinned/unlinked files and maintenance must remain.
Other processes and volumes sharing a filesystem can consume that headroom;
no software admission policy can guarantee otherwise. Recovery/GC work must
not depend on acquiring the same backlog credits that it releases. Object
outages should stop new data admission, not weaken a committed obligation.

`ColdWork.tla` checks round-robin service of sparse streams despite a perpetually
hot stream, bounded pass/in-flight work, unknown-upload retention, concurrent
byte-credit accounting, observation/release races and restart admission. Its
admission bound assumes charged costs cover growth and observations cover all
durable debt; it is not a proof of filesystem allocation costs. Stable-period
progress assumes eventually successful I/O, finite outstanding requests and
fair opportunities to reclaim and retry. The first liveness run correctly
failed with merely weak fairness for reclamation: repeated samples could
indefinitely preempt it. The repaired specification explicitly requires fair
reclamation opportunities; that failed history is retained. Scheduling fairness
is checked over a fixed finite stream set, not unbounded catalog arrivals or a
per-tenant service guarantee. Four configurations
and seven negative controls pass in `cold-work-formal-004`, including asymmetric
request costs (2/3/2 with a limit of 4) and the high-water-only starvation
mutation. The three-process MinIO history `cold-work-faults-001` reproduces that
stall; `002` retains the incomplete fix which uploads all forty short tails but
does not reclaim them because the native compaction threshold is still 64 KiB.
Neither failed history is a successful qualification. The existing
`ColdOwnership` obligations still apply separately; no composed refinement or
implementation/performance qualification is implied by this model result.

| Model action / obligation | Runtime correspondence and independent check |
|---|---|
| `Select`, `Yield`, `Resume`, `SparseProgress` | `cold_worker` selects from committed catalog state, wraps its cursor and bounds passes; `cold-work-faults-007` overlaps a continuously busy stream with previously queued sparse work and a real held PUT. |
| `Unknown`, `pending = owed` | Failed PUT/publication retains the original Begin identity; `cold_finish` retries its immutable range. Existing `ColdOwnership` and actual MinIO late-I/O histories remain required. |
| `Admit`, `Write`, `Finish`, `AdmissionSafe` | `Budget` charges an originating request; `batch::Completion` owns its ticket beyond HTTP cancellation/202; native-WAL/proptest and the concurrent outage probe check independent accepted bytes. Removing carried spent credits in an isolated real Rust implementation fails with 65 bytes promised under a 64-byte limit (`cold-work-implementation-mutation-001`). This model abstracts originator-only growth, not follower bypass. |
| `Capture`, `Sample`, `Restart` | All view locks precede the budget mutex; observations include retained unapplied journal frames, and spent credits clear only at that cut. Full-process restart must preserve existing pressure. |
| `Reclaim`, `AdmissionProgress` | Pressure seals short tails, rejected demand activates reclamation below high water, and committed publication uses native compaction. Generated native recovery cases include checkpoints from the former compaction threshold. Progress assumes successful I/O, usable headroom, fair sampling/retries and eventual participant recovery. |

The scheduler model abstracts a pass to two new attempts over three streams;
the runtime uses sixteen plus the separate pending-retry turn. The admission
model uses three asymmetric requests, not every allocator/transport interleaving.
These links identify the checked boundaries; they are not a machine-checked
refinement between the models and the Rust program.

Local implementation/qualification is recorded in
`evidence/cold-work-qualification-001/checked.json`, not inferred from this model.
The full existing ownership/fault obligations were rerun, as were both unchanged
332-test compatibility suites. Measured single-stream backlog stabilizes over
the tested 90-second intervals; high-cardinality backlog still grows and RSS
has no established plateau. Thus the work-driven/admission contract is not an
unconditional sustainable-rate, physical-space or production-readiness claim.
`cold-work-generations-002` separately verifies 48-generation deletion/pin cycles,
including observed completed DELETE rather than listing absence alone. Eventual
reclamation still depends on recovered participants, finite late writes and
successful fair storage work, as specified by `ColdOwnership`.

The local object campaign uses a separate, unmodified MinIO process built from
`minio/minio` tag `RELEASE.2025-04-22T22-12-26Z`, commit
`0d7408fc9969caf07de6a8c3a84f9fbb10a6739e` (AGPL-3.0). Its source and binary
are not vendored or linked into this Apache-2.0 engine. Provenance records source,
server binary, driver, MinIO binary and configs. Loopback proxies forward actual
S3 requests; faults do not substitute filesystem fixtures for the provider.
Random per-run credentials and raw stores stay in disposable `.tmp` directories;
the launcher deletes the credential file on cleanup. Evidence keeps synthetic
client bytes, object hashes, failed histories and unknown outcomes, not secrets.
These tests use one host/disk and do not establish MinIO/cloud durability,
power-loss behavior, independent-host recovery or an unbounded reclamation rate.

TTL uses the committed clock in `TIMED-STATE.md`, never local-clock expiration.
Cross-group forks and subscriptions retain durable control state in the same
journal and snapshot as stream data. Experimental node identity 10 retains the
command-count ceiling and node/cluster/partition binding introduced in identity 7,
the 0.10 consensus representation fence in identity 8, retirement intervals in
identity 9, and adds the original source configuration to each fork grant.
It rejects earlier data, including
the version-6 experiment that allowed 128-command batches:
restoring the 64-command ceiling must not silently invalidate its receipt
ordinals. Snapshot envelope format is 7, retaining bounded receipt outcomes (see
`ASYNC.md`) and bounded terminal fork results. No data migration or mixed-version operation is supplied; editing
`IDENTITY` by hand is not a migration. Use a fresh experimental cluster or retain
the matching earlier binary to inspect its existing data.
Physical WAL reclamation and obsolete-snapshot cleanup now have native-file
property and real-process syscall-fault qualifications. Terminal fork compaction
also has TLA+, generated native-WAL recovery and real-process qualifications;
its fixed admission limits and remaining scale boundary are in `FORKS.md`.
Cold high-cardinality/long-duration qualification, participant revocation and
production auth/TLS remain gates. No Internet exposure is qualified here.

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
