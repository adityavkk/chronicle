# Draft: Projection Snapshots extension for Durable Streams

Status: **proposed**, 2026-09-26. This document is Chronicle-owned, not part of
the pristine vendored `PROTOCOL.md`, and does not change the conformance pin.
Chronicle implements the HTTP API below as an **opt-in extension**, disabled by
default. Enable with `--enable-snapshots` / `CHRONICLE_ENABLE_SNAPSHOTS=true` on
the MemoryStore or Redis backend. Experimental segment wrappers are not supported.
The [store-level experiment](../../experimental/checkpoint/) remains a separate
folding example, not the storage implementation used by these endpoints.
See [research and Electric integration](../research/13-checkpointing-and-agent-recovery.md).

## 1. Scope and invariant

This is an optional additive extension under protocol §11.1. An application
publishes a complete state image for a named, versioned projection of **one**
source stream. The server stores opaque image bytes; it does not execute a
reducer or infer a schema from JSON event keys.

If C is the next-read offset saved with a snapshot, the image MUST represent the
projection of exactly the source prefix consumed before resuming at C. Applying
the canonical suffix starting at C MUST produce the same projection as full
replay through the same ending boundary. This equivalence includes any ordering,
deduplication, continuation, or other metadata the projection requires.

C is an opaque server-returned `Stream-Next-Offset` from a completely applied
read batch, or its SSE control equivalent. It is **not** the offset of a guessed
last event. Clients MUST NOT increment, parse, or synthesize it. The server MAY
interpret its own offsets to validate ordering and complete-message boundaries.
V1 does not accept partial-event/partial-batch or fork sub-offset checkpoints.
Chronicle additionally restricts a fork's publication position to a complete frame
stored in that fork's own data. Inherited-prefix cuts and the bare fork point
return 409: Chronicle cannot validate a parent's boundary atomically in the
fork's Redis slot. Normal fork reads and full replay remain available.

A snapshot is an optimization, not a new source of truth. Publication MUST NOT:

- append to, renumber, rewrite or truncate the canonical stream;
- consume producer sequence numbers, renew claim leases, or acknowledge wakes;
- trigger a normal source-stream append notification;
- grant permission to reproduce an external side effect;
- implicitly apply a retention policy.

Snapshot GET/PUT/DELETE do not count as source-stream access and do not renew its
sliding TTL. Ordinary suffix reads retain their normal access semantics.

## 2. Discovery, identity and rolling upgrades

An implementing server advertises `Stream-Snapshot: v1` and an opaque
`Stream-Incarnation` on successful source HEAD and GET responses. Incarnation is
stable over appends/closure and changes after delete/recreate or any operation
that could replace an already visible prefix. A fork has its own incarnation.

Snapshot requests address a subresource of the exact source URL using the single
query parameter `snapshot=<projection>`. The projection identifier is a
case-sensitive, URL-encoded application identifier, 1–128 UTF-8 bytes, without
control characters. It MUST change when schema, reducer semantics, or image
serialization become incompatible. Multiple versions may coexist.

`snapshot` MUST NOT be combined with `offset`, `live`, `cursor`, `limit`, or
other stream-read parameters. Duplicate parameters are invalid. The source URL
path is unchanged, so source namespace authorization cannot be bypassed by a
separate snapshot path. Snapshot lookup MUST NOT create a source stream.

Clients MUST verify `Stream-Snapshot: v1` on every snapshot response and every
guarded source read before using its body. An old server may ignore unknown
query parameters/headers and return ordinary stream data or a create response.
Such a response MUST NOT be interpreted as a snapshot or successful publication.
Publishers MUST NOT attempt this API until the deployment routes all snapshot
requests to supporting replicas: an old PUT route could create an ordinary
source if the source disappeared. Capability discovery alone is not a rollout
barrier.

## 3. Read the latest image

```http
GET /v1/stream/assistant/alice/main?snapshot=entity-v1 HTTP/1.1
Authorization: Bearer <read-credential>
```

On success:

```http
HTTP/1.1 200 OK
Stream-Snapshot: v1
Stream-Incarnation: <opaque-source-incarnation>
Stream-Snapshot-Offset: <opaque-next-read-offset-C>
ETag: "<strong-image-etag>"
Content-Type: application/vnd.electric.entity-image+json
Content-Digest: sha-256=:<base64-sha256-of-body>:
Cache-Control: private, no-store

<complete image bytes>
```

Angle-bracket values above are illustrative placeholders, not literal wire data.
The strong ETag MUST bind source incarnation, projection identifier, offset,
content type and exact image bytes. `Content-Digest` uses the SHA-256 structured
field form from RFC 9530; verify it over the transferred content (v1 uses identity
content encoding). A digest detects corruption, not malicious publishers.

The response MUST NOT emit `Stream-Next-Offset`, `Stream-Up-To-Date` or
`Stream-Closed` as though the image were source events. The image can lag an open
or closed source. GET returns the complete image or fails; a client MUST NOT
install a partially received image. V1 has no range or streaming image format.

404 means no image or no accessible source. A source that is soft-deleted or
whose required history is unavailable follows the base 410 semantics. A server
MUST check source liveness and authorization even when an image remains cached.
A snapshot is never accessible merely because its old bytes still exist.

## 4. Publish an image

```http
PUT /v1/stream/assistant/alice/main?snapshot=entity-v1 HTTP/1.1
Authorization: Bearer <snapshot-publisher-credential>
Stream-Snapshot: v1
If-Stream-Incarnation: <incarnation-used-during-fold>
Stream-Snapshot-Offset: <exact-completely-consumed-C>
If-None-Match: *
Content-Type: application/vnd.electric.entity-image+json
Content-Digest: sha-256=:<base64-sha256-of-body>:

<complete immutable image bytes>
```

For replacement, use a single strong `If-Match` ETag from the latest image
instead of `If-None-Match: *`. Exactly one of these conditions is required.
The publisher MUST capture state and C together before asynchronous upload.
Neither a later HEAD nor an append acknowledgement proves that the materializer
has applied all events through that position.

The server MUST validate before publication, atomically with the latest-pointer
change where relevant:

1. The caller has explicit source-scoped snapshot-publication authority.
2. The source exists, is not expired/soft-deleted, and incarnation matches.
3. C is a valid complete-message boundary at or before its current tail, and not
   `now` or an arbitrary interior byte position. The initial position must use
   the source's concrete returned offset, not a synthesized sentinel.
4. C is not before the currently published image's offset for this projection.
5. The HTTP precondition matches. A missing precondition is 428; a failed one is
   412. Clients must reload after a lost reply before retrying with a new ETag.
6. The entire body is present, within the configured size bound, and its digest
   matches. Identity content encoding only in v1.
7. The resulting count and aggregate body bytes fit the source's snapshot quota.
   A replacement subtracts the prior body's size before adding its new size.
   Retrying an identical image at capacity remains a no-op. Rejected writes
   neither consume quota nor evict an existing image.

The server cannot verify the fold itself. An authorized publisher vouches for
semantic equivalence. Equal C plus identical metadata/body is a no-op after
successful precondition evaluation; equal C with different content is 409.
Changing a broken reducer requires a new projection version rather than silently
replacing a different state at the same cut.

201 reports first publication, 200 replacement/no-op. Both have empty bodies and
echo the snapshot marker, incarnation, offset and new ETag. `Content-Digest`
describes HTTP message content, so these empty responses MUST NOT echo the
uploaded body's digest as their own. Validation errors are 400;
unauthorized/forbidden are 401/403; absent source is 404; regressing,
future or non-boundary C is 409; oversized bodies are 413; unsupported content
encoding/type is 415; exhausted aggregate storage quota is 507. No mutation
occurs on any rejection. The server SHOULD advertise `Stream-Snapshot-Max-Bytes`,
`Stream-Snapshot-Max-Total-Bytes`, and `Stream-Snapshot-Max-Versions` on source
HEAD. These bound one body, the sum of all bodies, and the number of projection
identifiers respectively. Metadata is additional bounded storage. Clients
SHOULD retire obsolete versions rather than retry a 507 in a tight loop.

Bytes and metadata MUST become visible together. For object storage, write and
verify an immutable object first, then atomically publish its descriptor. Failed
uploads leave the old descriptor untouched. Readers that already acquired an
old descriptor must retain access long enough to finish; do not delete an old
object immediately on pointer replacement. Storage keys/URLs are server-owned;
publishers cannot supply arbitrary fetch URLs.

### Conditional retirement

```http
DELETE /v1/stream/assistant/alice/main?snapshot=entity-v1 HTTP/1.1
Authorization: Bearer <snapshot-publisher-credential>
Stream-Snapshot: v1
If-Stream-Incarnation: <source-incarnation>
If-Match: "<current-image-etag>"
```

Retirement uses snapshot-publication authority, **not source deletion authority**.
The server checks source liveness, incarnation and ETag atomically with removing
that projection and freeing its count/byte quota. Success is 204 with the marker
and matched incarnation. An absent image or stale ETag/incarnation is 412; an
absent source is 404 and a soft-deleted source is 410. A missing `If-Match` is
428. Weak ETags, wildcard/list conditions, or `If-None-Match` are invalid (400).
After a lost reply, reload: an absent image may mean retirement already succeeded.
No events, producer progress, wake state, other projection images or source
metadata are deleted. A reader that already loaded an image can finish its
guarded suffix read. Retirement does not prevent future authorized publication
of that version; it is not a permanent tombstone or a rollback recovery fence.

## 5. Restore and resume without a race

```http
GET /v1/stream/assistant/alice/main?offset=<C> HTTP/1.1
If-Stream-Incarnation: <snapshot-incarnation>
```

An implementing server MUST check `If-Stream-Incarnation` before producing source
bytes and reject mismatches with 412. The response MUST echo the matched
incarnation and `Stream-Snapshot: v1`. The guard applies to normal, long-poll and
SSE reads, every continuation/reconnect, and empty/closed-tail responses. It
cannot be only a HEAD preflight: delete/recreate can occur afterward. A live
connection MUST terminate rather than emit any bytes from a new incarnation.

Clients verify identity/version/digest, hydrate temporary state, and consume the
suffix at **exactly C**, with the guard. They do not fold the image as ordinary
events. On a miss, unsupported server, incompatible version or corrupt image,
they may discard the image and perform full replay. They MUST NOT convert source
read failures/410 into empty state. After 412, discard the old view and restart
against the new source identity; do not continue the old offset on the new log.

Snapshot restoration does not imply caught-up status. Normal suffix reading
establishes `Stream-Up-To-Date`/closure. A saved `Stream-Cursor` is optional and
only a cache optimization; it cannot replace C or source identity.

## 6. Security, lifecycle, durability and compatibility

- Reads require source read authority; publication requires a distinct grant.
  Append/claim/write tokens alone MUST NOT authorize snapshot replacement. A
  publisher can poison every reader of its projection, not merely append data.
- Clients MUST NOT serialize secrets, credentials, leases or process-local
  optimistic transactions as generic runtime state. Projection formats define
  what is serializable and which observers they preserve.
- Source expiry/deletion invalidates discovery immediately. Stored images must
  follow source data-erasure policy even when physically reclaimed later. Closing
  a source permits publishing its final projection. Forks never inherit a source
  image automatically in v1.
- Source-prefix durability is a prerequisite. After failover/restore that may
  have replaced acknowledged history, servers MUST invalidate affected snapshots
  or change the incarnation unless they can prove the source prefix survived.
  A separately surviving snapshot must not conceal source rollback. Atomic
  publication does not strengthen Redis's asynchronous replication guarantees.
- Authenticated snapshot responses and guarded source reads use
  `Cache-Control: private, no-store` in v1. Unguarded base reads retain their
  existing cache semantics. All new request/response headers must be supported
  by CORS and forwarded by authorized gateways. Do not reuse an unguarded cached
  response for a guarded request.
- An incompatible image is disposable only while the necessary original history
  remains. Deleting history requires a separate retention/reset protocol for old
  clients, subscribers, forks and projections; this extension grants no such
  permission. Existing State Protocol control markers retain their meaning.

## 7. Chronicle implementation and rollout

The optional `store.ProjectionSnapshotStore` capability leaves `store.Store`
unchanged. MemoryStore holds images under the same lock as source operations.
Redis stores descriptor and raw binary body fields per projection in a dedicated
`ds:{<escaped-source-path>}:snapshots` HASH. This is a KV lookup, not another
Durable Stream. No snapshot bytes enter the source metadata HASH or event ZSET.
Images are at most **1 MiB** each, at most **8 projection versions** and **4 MiB
total image body bytes** per source. Content types are limited to 1024 UTF-8
bytes. Image encoding is application-owned.

Fields `d:<projection>` contain small JSON descriptors (incarnation, offset,
content type, ETag), while `b:<projection>` contain exact binary bodies.
`__count` and `__bytes` are updated in the same Lua operation as publication or
retirement. Field prefixes keep arbitrary valid projection names disjoint from
bookkeeping. Replacing an image uses `HSTRLEN` for old-body accounting, not JSON
parsing or base64 decoding of its bytes. The earlier unpublished JSON/base64
storage format is not migrated: invalidate those experimental image hashes
with publishers quiesced before upgrading. The source log is unchanged.

`snapshot_get.lua` validates source visibility and reads the image atomically.
`snapshot_put.lua` validates incarnation, expiry, frame boundary, nonregression
and CAS before publication, including atomic quota enforcement.
`snapshot_delete.lua` conditionally retires one image and frees its quota.
The Go wrapper verifies the stored ETag before
serving bytes and rejects images beyond the captured source tail. Source
deletion, including soft deletion, erases the image HASH. Expiry is checked
lazily and images share the source's Redis GC backstop; snapshot activity never
extends that backstop. Forks never inherit a parent's images.

Publication is **always authorization-enforced**, including when base-protocol
`CHRONICLE_AUTH_MODE=insecure`. Only an authenticated service with an explicit
`snapshot-publish` action for the source namespace may publish. Existing explicit
`trusted_gateway` policies delegate all actions and retain that broad authority.
Read, append, caller, OIDC user, and wake credentials alone do not publish images.
Snapshot reads use the same read authorization policy as the source; use
`CHRONICLE_AUTH_MODE=enforce` in protected deployments.

For example, add a narrowly scoped publisher to the mounted service policy
specified by `CHRONICLE_SERVICE_POLICY_FILE` (credentials are configured through
the existing service identity mechanism, never in this document):

```json
{
  "services": [
    {
      "identity": "snapshot-worker",
      "actions": ["read", "snapshot-publish"],
      "namespaces": ["agents"]
    }
  ]
}
```

The handler exposes the extension headers through CORS. Gateways must forward
the query and conditional/integrity headers unchanged, and must authorize
snapshot publication before substituting a privileged backend credential.
Deploy support to **all replicas and gateways before enabling publishers**. New
Chronicle binaries reject snapshot requests with 501 when disabled rather than
falling through to stream creation; pre-extension binaries cannot promise this.
The feature flag is a rollout gate, not a replacement for authorization.

**Failover prerequisite:** same-slot Lua provides atomic concurrency, not stronger
Redis replication durability. If a recovery might have changed an acknowledged
source prefix, quiesce consumers and publishers before reopening traffic,
invalidate affected images, and change affected stream incarnations. Discard
in-flight materializers too: otherwise an old worker can republish a stale fold.
Chronicle does not automatically detect same-incarnation history rollback; a
managed deployment must guarantee prefix preservation or supply this recovery
procedure. A body checksum or tail comparison cannot detect every rollback.

### Recovery drill for a potentially changed source prefix

This is an **operator procedure**, not an online API or an automatic failover
detector. Get deployment approval before any shared-database writes.

1. Stop **all** Chronicle traffic, appenders, consumers and snapshot publishers
   against the affected Redis deployment; terminate live reads and discard
   in-memory materializers. Disabling new publication alone is insufficient.
2. Complete Redis recovery and establish which source prefixes may have changed.
   Include affected fork descendants, because their projections can depend on
   inherited prefixes. If that set cannot be proven, treat every source as affected.
3. Inventory the actual existing source metadata keys and companion image keys.
   For each affected source, use a fresh never-reused opaque incarnation and
   use `WATCH <source-meta-key>` followed by `EXISTS <source-meta-key>` on its
   owning Redis primary. Continue only if EXISTS returns 1; otherwise UNWATCH
   and skip that vanished source. Execute the following transaction on the
   **same connection**. WATCH must abort if the metadata changes or expires;
   this requires Redis 6.0.9+ (the drill uses Redis 7). Do not use this template
   on older Redis versions: HSET could recreate metadata after key expiry.
   Both keys share its hash slot. Substitute exact keys from the inventory,
   not unescaped user paths (the key encoder escapes `%`, `{`, and `}`).

   ```text
   MULTI
   HSET <source-meta-key> incarnation <fresh-incarnation>
   DEL <source-snapshots-key>
   EXEC
   ```

4. Check every transaction reply. A null EXEC result means WATCH aborted: repeat
   from WATCH/EXISTS, never retry just the HSET. Persist/replicate the repaired state according
   to the deployment's durability policy, and verify no affected old image
   remains. Do not use a later rollback to undo this repair: it restores the
   old incarnation. An interrupted repair can be rerun while still quiesced,
   using fresh incarnations throughout.
5. Restart fresh materializers. Confirm that old-incarnation guarded reads and
   publication attempts fail with 412, while a full replay of recovered history
   can publish a new image. Resume normal traffic only after these checks.

This repairs snapshot/source consistency; it cannot recover lost events or make
already executed external effects exactly once. Consumer acknowledgements,
producer state, and effect reconciliation need the deployment's separate
disaster-recovery procedure.

`TestProjectionSnapshotRollbackRecoveryProcedure` runs this transaction against
disposable Redis. It first demonstrates the undetectable equal-tail rollback,
then checks stale readers/publishers are fenced and fresh replay succeeds. It
does not simulate managed Redis failover, replication, or operator quiescence.

Executable API coverage: `go test -race -run TestSnapshot .` exercises initial
replay, conditional save, fresh-handler restore, paged suffix recovery,
replacement, authorization, discovery, cache posture and incarnation guards.
The Redis variant uses unique keys in disposable DB11 by default, configurable
with `CHRONICLE_SNAPSHOT_REDIS_URL`. Store differential/concurrency/lifecycle
tests run with `go test -race -run 'TestSnapshot|TestProjectionSnapshot' ./store/redis`.

## 8. Deferred work

Keep v1 full-image, single-source and non-destructive. Defer historical image
selection (`at-or-before`), fork inheritance, multi-stream consistent cuts,
incremental/delta images, object-download indirection and snapshot notifications.
An Electric reader that needs raw history before the latest image must perform a
separate event read, not assume the latest image is a wake checkpoint.

Electric SDK hydration/export, runtime wake-input recovery, and agents-server
proxy changes are provided as [pinned upstream patches](../../experimental/electric-snapshots/README.md),
not released dependencies. Enabling this Chronicle option alone does not
accelerate existing Electric activations. The patched runtime explicitly retains
full raw-input replay for compatibility; bounded raw-input recovery and observer
bootstrap remain deferred. See the API and rollout limits in
[ELECTRIC-AGENTS.md](../ELECTRIC-AGENTS.md#projection-snapshots-opt-in-upstream-patches).
