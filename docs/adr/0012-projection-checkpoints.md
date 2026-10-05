# ADR 0012: Projection checkpoints separate from stream retention

- Status: Accepted for Chronicle's opt-in implementation; upstream spec proposed
- Date: 2026-09-26

## Context

Electric Agents rebuilds EntityStreamDB during cold activation. Model-context
summaries and State Protocol snapshot markers do not avoid downloading/reducing
the prefix. Chronicle's `ReadSnapshot` fixes a response's tail and incarnation;
it is not a persisted application-state snapshot. Immutable sealed segments
accelerate byte reads but do not reduce the number of events a client must fold.

State values alone cannot reproduce all agent behavior: sequence counters,
timeline ordering, fork pointers and raw unhandled wakes have independent
semantics. Arbitrary event streams cannot be reduced by latest-key compaction.

## Decision

1. Keep the canonical stream unchanged. Add optional versioned projection images
   with an exact consumed next-read offset and stream incarnation.
2. Let application workers produce images. Chronicle validates identity, boundary,
   publication concurrency and access, but never executes user reducers in Lua.
3. Publish the complete image/descriptor atomically; latest never moves backwards.
   A compatible image plus suffix must equal a full replay of that projection.
4. Keep materialization progress separate from subscription acknowledgements,
   claim fencing and external-effect completion. No snapshot save advances them.
5. Retain source history in v1; misses/version changes/corruption can fall back.
   Retention, historical image selection and fork inheritance are separate work.
6. Implement the transport/storage capability behind `EnableSnapshots`, with
   same-slot source validation, lifecycle cleanup and a distinct
   `snapshot-publish` permission. Keep the vendored protocol pristine; propose
   the extension in `docs/spec/SNAPSHOTS.md`. Electric's SDK restoration and
   pending-input recovery belong in upstream patches, not in Chronicle's storage
   layer and not implied by HTTP support.

## Consequences

Cold replay becomes proportional to image size plus suffix and pending raw input,
not necessarily constant time. Unique-key histories can still be large. Full
images trade write amplification for simpler recovery; cadence must be measured.

The HTTP implementation uses a dedicated same-slot image HASH, with separate
small descriptor and raw binary body fields per projection. Source delete/soft-delete
removes it; it shares source expiry and never refreshes access time. Images are
limited to 1 MiB, eight versions and 4 MiB of total bodies per source. Atomic
conditional retirement frees quota without affecting the source. These fixed
initial limits bound accidental publisher growth; this is not log retention.
Fork publication is
restricted to boundaries stored in the fork's own slot; inherited-prefix cuts
cannot be atomically validated there. Experimental segment wrappers are not yet
snapshot-capable. Full images incur serialization/write amplification, so cadence
belongs to the materializer, not every append.

`experimental/checkpoint` remains the original trusted fold/cache experiment,
with its own 24-hour expiry; HTTP does not use that cache. Neither implementation
can infer arbitrary acknowledged-source rollback after Redis failover. Deployment
must preserve source prefixes or quiesce materializers and invalidate images and
source incarnations before resuming traffic; no extra durability is implied.
The spec includes a recovery procedure and a disposable-Redis regression drill.

Adding a backend feature alone will not speed Electric. The companion
[upstream patches](../../experimental/electric-snapshots/README.md) add committed
StreamDB hydration/export and opt-in runtime recovery, but deliberately retain
full raw-event replay. Bounded raw-input recovery requires a separate proven
contract; it is not justified by restored collection rows. The
[research](../research/13-checkpointing-and-agent-recovery.md) names the exact
source hooks, hazards and wider rollout acceptance tests.
