# ADR-0006: Prototype immutable sealed segments outside the Redis frame ZSET

**Status:** Accepted for a feature-gated prototype. Rejected as the default until the listed gates pass.

**Date:** 2026-07-30

## Context

Chronicle stores each message as one member in a Redis lexicographic sorted set. A full replay asks Redis to find, copy, and return every matching member. The application then decodes every frame before writing the HTTP response. More Chronicle replicas do not remove that work from the Redis primary.

The sealed configuration comparison supplied with issue 6 reported 57.6 MiB/s for one Chronicle and one Redis at 512 catch-up readers on 16 vCPUs. The strongest sharded Chronicle result was 109.9 MiB/s on a different 4 vCPU topology. Rust WAL, Node memory, and Ursula disk results in that study were about 2,600 to 2,800 MiB/s. Those results used different storage, durability, process, and hardware shapes. They show the size of the replay gap. They do not predict the result of this prototype.

Kafka uses one representation for stored and transmitted record batches. Its design relies on sequential files, the operating system page cache, and `sendfile` to avoid extra copies. [Kafka design documentation](https://kafka.apache.org/20/design/design/) describes that mechanism.

Ursula uses immutable, content-addressed parts. It writes the parts before a manifest and then changes a `CURRENT` pointer with a compare-and-swap condition. The manifest contains a generation and a durable record boundary. Its cache verifies ranges by checksum, and its garbage collector retains recent manifest generations before it removes parts. The reviewed source was commit [`2d11bad`](https://github.com/tonbo-io/ursula/tree/2d11bad101bc576ce36f596fde601d5f2ec7aa9a). The relevant files are [`manifest.rs`](https://github.com/tonbo-io/ursula/blob/2d11bad101bc576ce36f596fde601d5f2ec7aa9a/crates/ursula-index/src/manifest.rs), [`index.rs`](https://github.com/tonbo-io/ursula/blob/2d11bad101bc576ce36f596fde601d5f2ec7aa9a/crates/ursula-index/src/index.rs), and [`cache.rs`](https://github.com/tonbo-io/ursula/blob/2d11bad101bc576ce36f596fde601d5f2ec7aa9a/crates/ursula-index/src/cache.rs).

Chronicle cannot put a local file or an object upload inside the Redis append script. Treating either one as part of the append linearization point would add an unproved distributed commit protocol. The prototype keeps the complete Redis frame ZSET. An append is acknowledged only according to the existing Redis script and configured Redis durability tier.

## Decision

We will test three immutable segment candidates behind `CHRONICLE_SEGMENT_MODE`. The default is `off`.

The candidates are:

1. `redis-chunks`, which stores immutable data and sparse indexes in Redis strings.
2. `local-files`, which stores immutable files on the Chronicle host or a shared volume.
3. `object-cache`, which stores sealed objects in an object store and keeps a bounded local cache. The prototype uses a filesystem object-store emulator.

We recommend `object-cache` as the production direction if its measured result passes the performance and failure gates. It gives every replica access to the same sealed prefix and removes sealed payload capacity from Redis. `local-files` remains the page-cache performance reference. `redis-chunks` remains the lowest-risk migration reference, but it does not remove payload memory or read bandwidth from Redis.

This decision does not approve a production cutover. Chronicle continues to write every frame to the current Redis ZSET in every prototype mode. Candidate mode defaults are also fail closed: new manifests start in `shadow`, and a normal read does not trigger synchronous sealing.

## Redis responsibilities

Redis remains responsible for these decisions:

- The append script validates content type, stream sequence, producer epoch, producer sequence, closure, expiry, and the expected tail.
- The append script writes the frame ZSET, tail, producer state, closure state, and notification atomically in one stream hash slot.
- Stream metadata, fork metadata, reference counts, expiry, notifications, subscriptions, fencing, and the bounded hot tail remain in Redis.
- A segment reader asks Redis to validate stream liveness and return records after the sealed boundary before it serves immutable prefix bytes.
- A segment failure falls back to the complete Redis copy. A Redis failure does not fall back to unchecked local bytes because Chronicle could not enforce expiry, deletion, or the current incarnation.

The prototype adds manifest keys and immutable strings in `redis-chunks` mode. It does not change the existing metadata, producer, fork, notification, or frame keys.

## Immutable record format

Each segment has a data object and an index object.

The data object contains ordered records. Each record starts with this 20-byte header:

| Field | Width |
|---|---:|
| Read sequence | 8 bytes, unsigned, big endian |
| Byte offset | 8 bytes, unsigned, big endian |
| Payload length | 4 bytes, unsigned, big endian |

The payload follows the header without conversion. JSON messages keep the exact flattened bytes produced by the current store. Binary appends keep their existing message boundary. The logical offset is the end offset, so a read returns records whose end offset is greater than the requested offset.

The sparse index contains one 32-byte entry for every configured number of records. The default stride is 128.

| Field | Width |
|---|---:|
| End read sequence | 8 bytes |
| End byte offset | 8 bytes |
| Record ordinal | 8 bytes |
| Byte position in the data object | 8 bytes |

Manifest version 2 limits each segment to 1,024 records. It records a SHA-256
digest for each 65,536 byte data block. It also records the sparse index entries
and a digest of the complete index object. The manifest digest authenticates
those fields.

Serving reads use the sparse entry to select a data position. The local and
object emulator backends use `ReadAt`, and the Redis backend uses `GETRANGE`.
Each backend returns one complete authenticated block and verifies its digest
before it returns any byte. Promotion and repair still verify the complete data
and index checksum, every block digest, every sparse entry, every record count,
and the final offset.

## Manifest and publication

Each manifest contains:

- Format version, backend mode, path, content type, and stream incarnation.
- A strictly increasing manifest generation.
- Migration state.
- A sealed-through logical offset.
- An ordered list of contiguous segment references.
- Segment object keys, record counts, byte lengths, index stride, checksums, and offset ranges.
- The source path and fork boundary when the stream is a fork.
- Creation and publication timestamps.

Every new stream has a persisted random 128-bit incarnation. The first maintained atomic read of a legacy Redis stream assigns the missing field with `HSETNX`; deleting the metadata hash and recreating the path therefore assigns another value even when the clock and stream configuration are identical. A digest of creation metadata is used only as a transient compatibility identity before that backfill. `Delete` and a successful new `Create` also remove the current manifest pointer. Every serving read compares the manifest incarnation with the metadata returned by the same atomic primary operation that returned the hot tail. It never joins a prefix from one incarnation to a tail from another.

Publication follows this order:

1. Read one atomic, TTL-neutral primary snapshot after the last sealed offset.
2. Encode one or more immutable segments.
3. Make the data and index durable.
4. Write a content-addressed manifest object.
5. Change the current pointer only if its token still equals the token read at step 1.

The local backend writes a temporary file, calls `fsync`, renames the file, and calls `fsync` on the parent directory. It takes a per-stream filesystem lock around pointer comparison and replacement. The object emulator uses separate origin and cache directories and the same durable pointer order.

The Redis candidate uses `SET NX` for immutable strings. It invokes a Lua compare-and-swap script through `redis.Script.Run` to change the current token. All candidate keys use the existing escaped stream hash tag.

The object-store target must use object generation preconditions for both the manifest object and current pointer. Google Cloud Storage provides strong object read-after-write and listing consistency. Its documentation still warns that caches can return stale data and that ranged reads without an object generation can combine different versions. [Cloud Storage consistency](https://cloud.google.com/storage/docs/consistency) states both rules. Content-addressed keys, generation-bound reads, and checksum verification remain required.

## Sealing and recovery

Sealing is a recoverable copy from the primary store. It is not part of append acknowledgement.

If Chronicle stops before the pointer change, the prior manifest remains current. A later seal reads the missing range from Redis and publishes another generation. Objects from the interrupted attempt remain allocated. This prototype does not reclaim them.

If Chronicle stops after the pointer change, every referenced object was durable before it became visible. A restart loads `CURRENT`, verifies the manifest digest, verifies its identity and contiguous ranges, and resumes from the sealed-through offset.

Concurrent sealers can write the same content-addressed object. Only one can change a given expected current token. A loser reloads the new generation and retries.

The prototype materializes a fork's logical prefix into the fork's own segments. The manifest also records the source reference and fork boundary. This costs more storage than shared part references, but a source soft delete cannot break a sealed fork. A later compactor may share immutable source segments only after it proves that garbage collection retains every object referenced by a live fork and by every retained rollback generation.

## Reads and cache behavior

A serving read performs these operations:

1. Ask the primary `PageReader` to capture the stream incarnation, content type,
   closed state, and upper tail in one bounded read. This first page renews a
   sliding TTL once.
2. Load and validate the current manifest.
3. Reject the candidate path if the primary snapshot has a different
   incarnation or content type.
4. Create a process local reader lease that holds the exact primary snapshot, a
   manifest copy, and a manifest pin.
5. Read at most one immutable segment range for each page.
6. Validate the root incarnation before every continuation.
7. Read the hot tail through the primary `PageReader` with the snapshot captured
   at step 1.

Continuation pages do not renew a sliding TTL. Sealing, recovery, and migration
verification also use no-touch page reads. A no-change seal and every transition
read and checksum every referenced object. A corrupt shadow generation cannot
become serving or cutover.

`shadow` manifests are never served. `serving` and `cutover` manifests may be served.

The object cache uses the segment checksum and block number as its key. It
verifies each cached block before use. A stale or partial block is removed and
fetched from the origin. The cache writes a temporary file, calls `fsync`,
renames it, and removes least recently used files until it is under its byte
limit.

The first immutable page still performs one bounded primary page read to
capture the authoritative snapshot. This adds one bounded primary read and can
add one Redis script call. The implementation does not use `sendfile`.

## Migration and rollback

The states are:

| State | Writes | Reads | Rollback |
|---|---|---|---|
| `off` | Current Redis ZSET only | Current Redis ZSET only | Not applicable |
| `shadow` | Current Redis ZSET plus recoverable segment copies | Redis ZSET | Change the startup mode to `off` or delete the segment pointer |
| `serving` | Same as `shadow` | Verified segments plus Redis hot tail, with Redis fallback | Change the manifest to `shadow` or the startup mode to `off` |
| `cutover` | The prototype still keeps the Redis ZSET | Same as `serving` | The control API refuses rollback because this is the tested policy boundary |

Old nodes ignore segment keys. New nodes accept appends written by old nodes because sealing reads the authoritative ZSET. New nodes still write the authoritative ZSET, so old nodes read appends written by new nodes. Producer acknowledgements and offsets do not depend on manifest progress.

A future change that trims sealed ZSET members needs a separate ADR and another proof. It must make the segment generation durable before the trim, preserve inherited fork ranges, and define how an old node is prevented from serving a trimmed stream. This ADR does not permit that change.

## Garbage collection and compaction

Deletion is disabled in this prototype. `GC` is an audit operation. It classifies policy-protected items as kept and policy-eligible items as deferred, but `ManifestsDeleted` and `SegmentsDeleted` remain zero.

This is required because object creation and manifest publication are separate operations. A publisher can finish `Put` and pause before `Publish`. A collector that sees only current manifests cannot distinguish those objects from orphans. The previous implementation deleted two staged objects in that schedule and then allowed the publisher to make a broken manifest current. Deterministic file, object-emulator, and Redis tests now hold publication at that point and require the objects to remain.

Real reclamation needs a publication barrier:

- File and object modes need a durable, collector-visible staging lease or one shared lock that covers every object write through pointer publication.
- Redis needs a same-slot staging-generation protocol whose publish and collection checks are atomic.
- Every mode also needs shared snapshot, fork, rollback, and repair holds with crash recovery.

Until those protocols have deterministic crash and concurrency tests, Chronicle must not run an external lifecycle policy or repair command that deletes candidate manifests or objects. This ADR makes no reclamation-readiness claim.

`segments.Store.PinSnapshot` still protects the current manifest in one process, and ordinary segment reads hold that local pin for their duration. This is preparation for bounded paging, not permission to reclaim across replicas.

Compaction must write new immutable objects and publish a new manifest. It must not edit an existing segment. Replacement ranges must be contiguous and must cover the same logical offsets and bytes. The prototype does not compact segments.

## Candidate comparison

| Concern | Redis strings and sparse index | Local append-only files | Object-backed segments and bounded cache |
|---|---|---|---|
| Durability | It has the same persistence and failover limits as the Redis deployment. `SET NX` does not add a stronger guarantee than Redis AOF and replication. | Durability depends on file `fsync`, directory `fsync`, volume behavior, and whether the volume survives the host. | Durability depends on a successful object write and conditional pointer update. The local cache is disposable. |
| Crash window | An unreachable string can remain before manifest CAS. Reclamation is disabled. | A staged or renamed file can remain before manifest publication. The prior `CURRENT` stays valid. Reclamation is disabled. | An uploaded object can remain before manifest publication. The prior object generation stays valid. Reclamation is disabled. |
| Multi-replica access | Every replica can read the same strings. All reads still consume Redis memory and network. | A host-local file is not visible to another replica. A shared volume adds its own failure and throughput behavior. | Every replica can read the same origin. Each replica can keep a bounded local cache. |
| Migration | This is the smallest layout change and uses the current Redis cluster slot. | New nodes need the same local or shared path after restart. | New nodes need object credentials, bucket policy, and cache storage. |
| Rollback | Disable the read plane. The complete ZSET is unchanged. | Disable the read plane. The complete ZSET is unchanged. | Disable the read plane. The complete ZSET is unchanged. |
| Garbage collection | Inventory and history can classify candidates, but deletion needs a same-slot staging barrier. | Directory traversal can classify candidates, but deletion needs a durable staging lease or shared lock. | Inventory can classify candidates, but deletion needs generation-bound staging leases and shared reader holds. |
| Compaction | It rewrites large strings inside Redis and retains old strings until the new manifest is current. | It writes sequential files and can use the page cache. | It uploads replacement objects and changes the manifest after upload. |
| Forks | Shared Redis strings are possible, but reference tracking must be exact. The prototype materializes fork bytes. | Reflinks may help on some filesystems, but they are not portable. The prototype materializes fork bytes. | Content-addressed objects can be shared across fork manifests after GC understands references. The prototype materializes fork bytes. |
| Checksum and repair | Checksums detect corruption, but repair reads the authoritative frame ZSET in the prototype. | Checksums detect file and cache corruption. Repair reseals from Redis. | Checksums detect stale cache entries and origin corruption. Repair evicts cache or reseals from Redis. |
| Consistency | Redis commands are strongly ordered on the primary for one slot. Failover durability remains a separate issue. | Local rename gives one-host pointer atomicity. A shared filesystem must provide the required lock and rename behavior. | Content addressing avoids overwrite races. Pointer changes require object generation preconditions even on a strongly consistent service. |
| Encryption | Redis transport and storage encryption depend on the Redis deployment. | The operator must configure volume encryption and file permissions. | Cloud Storage encrypts server-side data before disk at no extra charge and supports other key custody modes. [Standard Cloud Storage encryption](https://cloud.google.com/storage/docs/encryption/default-keys) documents the default. |
| Cost | Payload bytes consume Redis RAM. Reads consume Redis CPU and network. | The host pays for attached storage and replica duplication. A shared filesystem adds provisioned throughput cost. | The service charges for stored bytes, operations, retrieval in some classes, and network transfer. [Cloud Storage pricing](https://cloud.google.com/storage/pricing) separates operation, retrieval, and network charges. |
| Cache behavior | Redis is the shared cache and the authority. There is no independent eviction budget for sealed data. | The operating system page cache uses host memory. Chronicle cannot enforce a per-stream cache budget. | Chronicle enforces a local byte bound. A miss needs two object reads in this format, one for data and one for the index. |
| Operations | It adds Redis keys and GC load but no new service. | It adds disk capacity, inode, mount, and host replacement procedures. | It adds credentials, object lifecycle, request-rate, cache-disk, and regional egress procedures. |
| Expected limit | Redis remains the payload bottleneck. | Host-local throughput can be high, but replica placement controls data access. | Cold reads pay object latency. Warm reads can use local files, but cache churn and request amplification can dominate small segments. |

## Performance gates

The benchmark matrix uses 8, 32, 128, and 512 closed-loop readers. Each result must report:

- Catch-up throughput and HTTP time to first byte.
- p50, p95, and p99 full-read latency.
- Errors and client-side drops.
- Chronicle and Redis CPU and RSS.
- Redis network input and output bytes and connection count.
- Segment bytes read and written, fallback count, checksum failures, origin reads, cache hits, cache misses, and cache evictions.
- Open file count where the operating system exposes `/proc/<pid>/fd`.
- Seal latency as the durability barrier for immutable visibility.

The report must keep hardware, topology, durability, dataset, and cache state beside each number. Results from the prior sealed study are comparison context only.

## Correctness gates

The feature cannot become the default unless all of these gates pass:

- The 332 protocol conformance tests pass in `off`, `redis-chunks`, `local-files`, and `object-cache`.
- `make test`, `make lint`, and `make spec-check` pass.
- The Redis differential suite and segment differential suite pass for binary bytes, JSON boundaries, offsets, closure, TTL, expiry, forks, producer fencing, idempotency, delete and recreate, and resumed reads.
- `store-linz` passes Porcupine in every mode under concurrent append, read, close, and injected Redis faults.
- Deterministic faults at create, write, sync, rename, upload, checksum, manifest, cache, migration, cutover, rollback, and garbage collection never expose incomplete or corrupt bytes.
- Restart, Redis restart, upload interruption, Redis partition, stale cache, checksum failure, concurrent append and seal, fork deletion, and manifest publication races pass.

Any lost acknowledged byte, duplicate accepted producer append, reordered frame, offset gap, stale incarnation read, premature visibility, or accepted stale fence rejects the candidate.

## PageReader integration

`segments.Store` implements `store.PageReader`,
`store.PageSnapshotReleaser`, and
`store.NotificationSubscriberProvider`. The handler selects the segment page
reader instead of `legacyPageReader`. Each reader receives a different opaque
lease token, even when two readers use the same manifest generation.

The wrapper releases the manifest pin after the final page, an error,
cancellation, or handler completion. A corrupt range marks that lease as
primary only. Later pages use the captured bounded primary snapshot and do not
retry the segment backend. The implementation and acceptance tests are in
`docs/integration/immutable-segments-pagereader.md`.

## Consequences

The prototype can measure a sealed read plane without changing append acknowledgement or the current default. Mixed old and new nodes can read each other's writes, and rollback does not lose data because Redis retains every frame.

The prototype doubles storage for sealed bytes and retains interrupted or
obsolete candidate objects because deletion is disabled. It adds manifest,
lease, and audit state. The serving path copies authenticated blocks into Go
memory. The object emulator proves local publication, range authentication, and
cache behavior only. It does not prove production object latency, conditional
writes, multipart interruption, quotas, credentials, regional failure behavior,
or cost. A real object store campaign remains a separate approved gate.
