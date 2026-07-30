# Immutable segment migration and rollback

The immutable read plane is experimental. Keep `CHRONICLE_SEGMENT_MODE=off` unless you are running the issue 6 gates. If a candidate mode is set, new manifests default to `shadow` and read-triggered sealing defaults to `false`.

## Preconditions

Run these commands from the repository root:

```bash
make test
make lint
make spec-check
make conformance-segments
```

Run the data-plane linearizability scenario against each mode:

```bash
go build -o bin/jepsen-checker ./jepsen/checker

bin/jepsen-checker -scenario store-linz -redis-url redis://localhost:6379/15 \
  -segment-mode off
bin/jepsen-checker -scenario store-linz -redis-url redis://localhost:6379/15 \
  -segment-mode redis-chunks
bin/jepsen-checker -scenario store-linz -redis-url redis://localhost:6379/15 \
  -segment-mode local-files -segment-dir /tmp/chronicle-segments-local
bin/jepsen-checker -scenario store-linz -redis-url redis://localhost:6379/15 \
  -segment-mode object-cache -segment-dir /tmp/chronicle-segments-object
```

Never run two commands against the same Redis database at the same time. The scenario creates a unique stream, but a Redis fault injector can affect every client of that database.

## Stage 1: shadow

This stage is a local verification procedure, not a deployable migration. The prototype has no public backfill or transition control surface. Start one test replica with:

```bash
CHRONICLE_SEGMENT_MODE=object-cache \
CHRONICLE_SEGMENT_INITIAL_STATE=shadow \
CHRONICLE_SEGMENT_DIR=/var/lib/chronicle/segments \
CHRONICLE_SEGMENT_CACHE_BYTES=268435456 \
./bin/chronicle
```

In `shadow`, explicit `segments.Store.Seal` calls, initial-body creates, and close operations copy Redis records into immutable objects. Reads still use the Redis frame ZSET. A no-change seal reads and checksums every referenced object. There is no background backfill in this prototype.

Check these metrics:

```text
chronicle_segment_seals_total
chronicle_segment_backend_bytes_written_total
chronicle_segment_checksum_failures_total
chronicle_segment_primary_fallbacks_total
```

Stop if checksum failures increase. Stop if sealing prevents normal requests from meeting their latency target. An append remains accepted in Redis even when a seal fails, so recovery can retry it.

## Stage 2: mixed-version serving test

Do not perform this stage in production. `StateServing` requires an explicit, successful `segments.Store.Transition` after full object verification. The environment value applies only when a new manifest is created and is retained here for test fixtures.

```bash
CHRONICLE_SEGMENT_MODE=object-cache \
CHRONICLE_SEGMENT_INITIAL_STATE=serving \
CHRONICLE_SEGMENT_DIR=/var/lib/chronicle/segments \
./bin/chronicle
```

Verify all four paths:

1. Append through an old replica and read through a serving replica.
2. Append through a serving replica and read through an old replica.
3. Retry one producer tuple through both replicas and confirm one accepted append.
4. Delete and recreate one test stream, then confirm that no old payload appears.

Serving replicas validate Redis liveness and read the Redis hot tail before they return segment bytes. A segment or cache error increments the fallback counter and reads the complete Redis copy.

## Rollback before cutover

Set `CHRONICLE_SEGMENT_MODE=off` and restart the replica. This is the safest rollback because it ignores every prototype key and file.

To keep shadow copying active while disabling segment reads, set:

```bash
CHRONICLE_SEGMENT_INITIAL_STATE=shadow
```

The startup setting applies to new manifests. Existing serving manifests require the `segments.Store.Transition(path, StateShadow)` control operation. The prototype exposes that operation to tests and repair code. It does not expose a public HTTP route.

Do not delete segment objects or manifests during rollback. The prototype keeps all generations because reclamation is disabled. Redis still contains the complete stream.

## Cutover boundary

`StateCutover` is a one-way policy marker in this prototype. The transition code refuses a later change back to `shadow`.

Do not use `StateCutover` in production. The prototype does not trim Redis frames, so cutover has no capacity benefit. A later proposal that removes sealed ZSET members needs a new ADR, mixed-version exclusion, a tested restore path, and a separate approval.

## Recovery

After a Chronicle restart and on the next segment operation:

1. Chronicle reads `CURRENT`.
2. Chronicle verifies the manifest digest, stream incarnation, state, and segment ranges.
3. A serving read checksums every object it uses. A no-change seal or transition checksums every referenced object.
4. Chronicle reads any range after `sealed_through` from Redis with a TTL-neutral atomic snapshot.
5. The next seal publishes another generation with a compare-and-swap pointer update.

An object that is not reachable from a manifest may be an orphan, or it may belong to a publisher paused between object creation and manifest publication. Do not remove it.

If an object checksum fails, keep the Redis ZSET and run `segments.Store.Repair` while the candidate is not serving traffic. Repair reads the complete authoritative stream with a TTL-neutral snapshot, writes generation-qualified replacement objects, verifies them, and publishes a new manifest generation. It never edits or deletes the corrupt object in place.

## Garbage collection

Deletion is disabled. `segments.Store.GC` is an audit operation only. It reports:

- `ManifestsKept` and `SegmentsKept` for items protected by the supplied policy.
- `ManifestsDeferred` and `SegmentsDeferred` for items that the policy would otherwise reclaim.
- Zero deleted manifests and zero deleted segments.

Do not apply an object lifecycle rule, remove files, or delete Redis candidate keys. A real collector needs a durable staging barrier that covers `Put` through `Publish`, plus shared fork, snapshot, rollback, and repair holds. File, object-emulator, and Redis regressions prove that the old collector could delete publisher staging objects.

An in-process snapshot reader must still call `segments.Store.PinSnapshot` and keep the returned pin until its last page or cursor expires. Ordinary whole-body segment reads pin for their duration. These pins prepare the API for future bounded paging but do not permit deletion.

Review the machine-readable `GCResult` only as capacity evidence. This prototype makes no reclamation-readiness claim.

The downside is unbounded candidate storage growth. Keep the mode `off` outside an isolated test until a later ADR adds and proves the staging and retention protocols.
