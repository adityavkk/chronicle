# Snapshot-plus-tail experiment

This is executable **store-level sample code**, separate from Chronicle's opt-in
HTTP feature. The server implements the [draft protocol](../../docs/spec/SNAPSHOTS.md)
using `store.ProjectionSnapshotStore`, not this prototype's trusted Redis cache.
[Research and Electric integration details](../../docs/research/13-checkpointing-and-agent-recovery.md)
explain the design. The separate [Electric patch bundle](../electric-snapshots/README.md)
implements opt-in SDK/runtime integration against pinned upstream sources.

`Capture` uses Chronicle's `store.PageReader` (MemoryStore or Redis), restores a
compatible JSON image, and folds only the suffix through a fixed tail. It uses
the existing incarnation guard on every page, including an empty tail read.
The returned image is complete and immutable relative to the returned state.

`RedisCache` stores one image per source path/incarnation/projection version.
Publication uses single-key Lua bytewise CAS. Offset ordering lives in Go via
`store.Compare`; there is no duplicated Lua offset predicate. It cannot replace
a newer image with an older one; conflicting state at the same offset is an
error. Reads need one key lookup, not an index-stream replay.

## Use from a trusted worker

The following is an integration sketch, with `source` implementing both
`store.Store` and `store.PageReader`, an existing Redis client, and an
application-supplied `Projection[State]`:

```go
cache := checkpoint.RedisCache{Client: redisClient}
meta, err := source.Get(path)
if err != nil {
    return err
}
prior, err := cache.Load(ctx, path, meta.Incarnation, projection.Version)
if err != nil && !errors.Is(err, checkpoint.ErrInvalidImage) {
    return err // choose an explicit availability policy; don't hide every error
}
state, image, stats, err := checkpoint.Capture(ctx, source, path, projection, prior)
if err != nil {
    return err
}
// state is caught up to image.Offset, not necessarily the current head now.
// Capture independently rejects the prior image if the source was recreated.
if shouldCheckpoint(stats) {
    if err := cache.Save(ctx, image); err != nil {
        log.Printf("optional checkpoint publication failed: %v", err)
    }
}
// Use state; saving an image does not acknowledge any subscription.
```

`Projection.Apply` must be deterministic and side-effect-free. Its state must
round-trip through Go JSON without precision/type loss. Use typed numbers rather
than decoding int64 IDs into `map[string]any` float64 values. Change `Version`
with reducer/schema/serialization changes. Do not pass live mutable/optimistic
state; `Capture` owns the fold until serialization completes. Offset arithmetic
belongs to the store, never the projection.

## Run

With Go from `go.mod` and a local Redis (`.agents/setup` installs prerequisites):

```bash
go test -race -count=1 ./experimental/checkpoint
go test -race -short ./experimental/checkpoint -rapid.checks=1000
go test ./experimental/checkpoint -run '^$' -bench BenchmarkCapture -benchmem -count=3
```

The Redis test defaults to database 11, uses unique keys and never flushes a
database. Override **only** with `CHECKPOINT_REDIS_URL` pointing to disposable test
Redis. It exercises Chronicle's actual Redis source store and a fresh checkpoint
client, not just an in-memory mock. `-short` skips that integration leg.

The benchmark compares 10,010-event full folding with a 10,000-event image plus
10-event suffix, including restore/serialization but not Redis/network I/O.
It models a tiny fixed-size, order-sensitive aggregate, **not EntityStreamDB**.
The MemoryStore itself still walks its message slice; frames/op measures reducer
work, not all storage traversal. Do not extrapolate this to agent latency.

## Deliberate limits

- No HTTP routes, credentials, claim/ack coupling or production wiring.
- Trusted in-process callers only: a digest is not proof of a correct reducer,
  a valid source boundary, or authorization. `Save` expects `Capture` output.
- Full JSON images, at most 1 MiB including metadata; no large-object storage.
- Cache entries expire after 24 hours without a new publication. Same-image
  retries do not extend expiry. Corrupt entries fall back on read, but publication
  fails until the bad entry expires or an operator invalidates it.
- No destructive retention. Source deletion/recreation prevents reuse, but old
  bytes may remain until cache expiry. Not suitable for immediate-erasure policy.
- Assumes no rollback of an acknowledged source prefix. This cache is independent
  of source replication; discard it after uncertain failover/restore. Neither
  incarnation comparison nor a checksum detects every same-incarnation rollback.
- No image sharing across forks, incremental images, historical selection,
  serializer migrations, automatic cadence, multi-stream cut, or recovery of
  process stacks/leases/external effects.
- This Go experiment does **not** implement Electric's hydration API. The separate
  upstream patches own row ordering, sequence counters and event pointers, and
  conservatively retain full raw-wake replay. See their validation and limitations.
