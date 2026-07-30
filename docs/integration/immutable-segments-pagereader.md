# Immutable segments with bounded catch-up

## Status

The integration branch implements `store.PageReader` on `segments.Store`.
Chronicle can now serve an immutable prefix without loading the complete segment
or index object. The same response then reads the hot tail from the Redis
primary under the snapshot captured on the first page.

The focused unit tests pass. This work is not ready for `main` until the full
unit, lint, protocol conformance, segment conformance, and Jepsen gates pass.
The 332 Durable Streams protocol tests and the Jepsen and Porcupine correctness
checks remain release requirements. A performance change must not weaken or
skip either correctness suite.

## Capabilities

`segments.Store` implements these optional store capabilities:

```go
var _ store.PageReader = (*segments.Store)(nil)
var _ store.PageSnapshotReleaser = (*segments.Store)(nil)
var _ store.NotificationSubscriberProvider = (*segments.Store)(nil)
```

The HTTP handler therefore selects the bounded page reader instead of
`legacyPageReader`. The segment wrapper also exposes the Redis primary's
notification subscriber through the provider interface. Segment files and
caches never act as notification authorities.

Server startup rejects a segment wrapper that cannot expose the Redis
notification capability. It also validates the segment mode, directory, target
size, index stride, cache size, and initial migration state before starting the
server.

## First page

The first page performs these operations:

1. The primary `PageReader` captures the stream incarnation, content type,
   closed state, and upper tail in one bounded read.
2. The wrapper loads the current immutable manifest and verifies its digest.
3. The wrapper requires the manifest and primary snapshot to name the same
   stream incarnation and content type.
4. The wrapper creates a random process local lease token. Two readers never
   share a token, even when they use the same manifest generation.
5. The lease stores a copy of the manifest, the exact primary snapshot, and a
   manifest pin.
6. The wrapper serves at most one immutable segment range for the page.

The primary capture reads some bytes that the immutable page does not return.
The page statistics count those payload bytes as discarded work. The
`ReturnedBytes` value counts only bytes returned to the caller.

## Continuation pages

A continuation carries the `ReadSnapshot` returned by the first page. The
opaque `StoreToken` resolves the process local lease. Chronicle rejects an
unknown token or a token whose path, tail, incarnation, content type, or closed
state differs from the captured snapshot.

Before every immutable continuation, the wrapper asks the primary to validate
the captured root snapshot at its original tail. A delete and recreate returns
`store.ErrReadSnapshotChanged`. A concurrent append does not change the
captured upper tail, so the response does not include bytes appended after its
first page.

When the continuation reaches the sealed boundary, the wrapper reads the hot
tail through the primary `PageReader` with the exact primary snapshot captured
on the first page. It does not capture a second tail at the boundary.

The wrapper releases the manifest pin and nested primary snapshot when a page
fails, when the caller cancels the read, or when the caller explicitly ends the
response. A final page does not invalidate the snapshot on return because SSE
must perform one last incarnation check before it attaches to the live hub.
Every maintained handler and the compatibility `Read` method release the
snapshot with a defer. Release is idempotent.

## Immutable range format

Manifest version 2 records these fields for each segment:

- The segment contains no more than 1,024 records.
- `BlockBytes` is 65,536 bytes.
- `BlockChecksums` contains one SHA-256 digest for each data block.
- `IndexChecksum` authenticates the complete fixed width index object.
- `IndexEntries` contains the sparse offset, ordinal, and data position entries
  that the manifest digest authenticates.
- `Checksum` authenticates the complete data and index pair for promotion and
  repair checks.

The serving decoder finds the last sparse entry at or before the requested
offset. It then fetches complete authenticated blocks through
`Backend.ReadDataRange`. The local backend uses `ReadAt`. The Redis backend uses
`GETRANGE`. Object cache mode stores and evicts individual authenticated blocks
and reads a missing block from the origin with `ReadAt`.

Each backend verifies the block checksum before returning any byte. The decoder
also verifies that the selected sparse entry names the actual record header at
its recorded position.

The decoder stops before it fetches a payload that would exceed the byte target
when the page already contains a record. It stops at the frame limit. A first
record larger than the byte target is returned alone, which matches the
`PageReader` contract. Chronicle serves at most one immutable segment per page,
so a page can be underfilled.

## Promotion and corruption

Serving reads authenticate only the blocks they fetch. Before Chronicle
promotes a shadow manifest to serving state, `verifyManifest` reads every
referenced data and index object and performs these checks:

- The complete object lengths match the manifest.
- The complete data and index checksum matches.
- Every data block checksum matches.
- The complete index checksum matches.
- The index object matches the manifest entries.
- Every sparse entry names the actual record at its ordinal and data position.
- Record offsets increase, the record count matches, and the final offset
  matches the segment reference.

If a serving range is missing or corrupt, the wrapper marks that reader lease
as primary only and returns the same bounded page from the captured primary
snapshot. Later pages in that response do not retry the immutable backend.
Chronicle never returns a byte from a block that failed authentication.

## Sealing and repair

Sealing and repair drain the primary through bounded `ReadPage` calls with
`NoTouch` set. The first page captures one snapshot. Every later page uses that
snapshot. Chronicle aborts publication if a page has a gap, makes no progress,
changes incarnation, or returns an error.

Each primary page becomes one immutable segment. The primary reader limits a
page to 1,024 frames, so a stream with more frames produces multiple segments.
The manifest becomes visible only after all segment objects are durable and the
backend publishes the new manifest through compare and swap.

The first page of a logical client read renews a sliding TTL. Continuation
pages, sealing, repair, manifest verification, and inherited source reads do
not renew it.

## Tests

The merged tests cover these cases:

- The handler selects the real `segments.Store` page reader.
- A resumed read fetches authenticated ranges and never calls the whole object
  read method.
- Every backend range is aligned and no larger than 65,536 bytes.
- Pages obey byte and frame bounds. An oversized first frame is returned alone.
- More than 1,024 records continue across two segments with no gap or duplicate.
- Concurrent appends stay outside the first page's snapshot.
- Delete and recreate during continuation fails with
  `store.ErrReadSnapshotChanged`.
- Independent readers receive different lease tokens.
- Final-page confirmation remains valid until explicit release. Errors,
  cancellation, compatibility reads, and handlers release snapshot pins.
- A corrupt range falls back to bounded primary pages and the lease stays
  primary only.
- A forged sparse position fails both full and ranged decoding.
- A legacy Redis source without an incarnation remains readable through a fork.
  A direct root read then persists an incarnation, and later fork reads still
  succeed.
- Sliding TTL renewal happens once per logical client snapshot.
- The default Jepsen run includes `paged-catchup`.
- CI runs the complete protocol suite in the default layout and in every
  immutable backend and migration state.

## Limits

Manifest pins are process local. Garbage collection remains audit only because
Chronicle does not yet have a shared cross replica hold protocol or a staging
barrier that distinguishes an orphan from a publisher paused between object
write and manifest publication.

The first immutable page also performs one bounded primary page read to capture
the authoritative snapshot. This adds bounded storage work and can add one
Redis script call. Removing that call would require a separate atomic metadata
snapshot operation with the same incarnation and tail guarantees.

Manifest version 2 is incompatible with version 1. The feature remains off by
default. Operators must create and verify new version 2 generations before
enabling serving mode.
