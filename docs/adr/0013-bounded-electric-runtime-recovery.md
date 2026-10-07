# ADR 0013: Explicit bounded-input recovery for Electric Agents

- Status: Accepted for local opt-in integration; upstream proposed
- Date: 2026-10-03
- Extends: [ADR 0012](0012-projection-checkpoints.md); no Chronicle wire change

## Context

Chronicle's projection snapshot is an opaque image of a particular source-stream
prefix. Electric represents folded stream state as in-memory collections, not
as a Postgres backup. Restoring these collections avoids folding old events,
but the first Electric patch separately downloads the entire raw log for
`processWake` input selection, signals, setup and handler-visible `ctx.events`.
The measured full-raw-replay integration did not improve activation medians.

An arbitrary handler can depend on historical event order and multiplicity that
current collection rows do not preserve. Silently changing its inputs is not a
valid recovery optimization. Conversely, storing the entire raw prefix inside
the image merely relocates the unbounded read.

Materialization progress also differs from completed work: a consumed batch can
contain inputs A and B while the handler has completed only A. Neither the last
read position nor a mutable acknowledgment candidate proves B was processed.

## Decision

1. Add opt-in `rawInputRecovery: 'bounded-checkpoint-v1'` to Electric's runtime
   policy. Preserve legacy full-replay behavior. The application must use a fresh
   projection version and accept that `ctx.events`, setup events and
   self-observation contain the post-checkpoint delta, not historical callbacks.
   Applications requiring historical input must retain full replay. The initial
   delta starts after durable completed progress, not the notification's tail
   hint; otherwise a notification covering A and B can hide A while a later
   checkpoint incorrectly certifies both. Optional event offset headers must not
   change which unfinished inputs require handling.
2. Record completed progress as a reserved runtime checkpoint event in the
   canonical source stream. It is not a second snapshot stream or a new
   Chronicle control-plane record. The event carries the consumed source cut
   and deterministic materializer sequence covered by completed work. It does
   not persist leases, fencing credentials, JavaScript stacks or bearer tokens.
3. Write progress only after successful work and entity writes have drained.
   Pending queued/live wakes, unresolved inbox entries or unhandled signals
   prevent a new checkpoint. In particular, completion of A must not checkpoint
   a batch containing unfinished B. A failed handler must not produce progress
   or acknowledge the failed activation through this path. The final callback's
   acknowledgment must also stop before unresolved input: an output appended
   after B does not prove B was handled. Declining an image while acknowledging
   that later output is not safe recovery.
4. Capture the checkpoint and projection at a committed state boundary. If a
   fresh obligation arrives after the processed cut but before the captured
   image cut, decline that image rather than pretending the obligation is
   completed. A fresh append after the image cut remains in the suffix. These
   decisions must be independent of how HTTP responses happen to group events.
   Image bytes, row pointers and timeline metadata must also be independent of
   response grouping. A materializer sequence counts known-schema changes;
   a flattened stream position counts every JSON item. They are not interchangeable
   when control or unknown items occur. Preserve legacy partial-update semantics
   when exporting canonical committed rows, not optimistic visible state.
   Provisional `applyEvent` ordering must stay out of canonical rows and indexes;
   the authoritative echo establishes source-derived ordering, including index
   insertion order. Capture committed state on source application even when an
   unrelated optimistic transaction is still pending. Timeline tokens must sort
   monotonically across the supported integer range, not only within a fixed
   small-event-count fixture. Codec and application projection versions must
   change when the ordering encoding changes; old images must not mix with new
   suffix tokens.
   Public export requires explicit committed capture (`onCommittedBatch` in the
   SDK, `onCommittedSnapshot` in EntityStreamDB). Reject export during synchronous
   uncommitted batch hooks, when pointer indexes may already have advanced but
   canonical rows have not; allow it after the complete commit. These requirements
   preserve default legacy sync visibility while preventing mixed-cut images.
5. On a compatible hit, restore the image and completed progress, then read only
   the canonical suffix from the image cut under the matching incarnation guard.
   Use that state reader's committed batches for the raw-input delta as well;
   bounded recovery must not open a second historical reader. A durably completed
   redelivery need not rerun its handler. Opaque explicit wake payloads without a
   canonical source identity are unsupported in bounded mode; an offset alone
   cannot distinguish an old hint from new out-of-log work. Standard timer, cron
   and fork paths continue to use their canonical source events. A missing or
   incompatible image may replay retained history under the same bounded-input
   contract. An observed source-incarnation or live source failure prevents new
   certification and acknowledgment, but must not bypass release and disposal
   during cleanup.
6. Treat the checkpoint event as management-only. It must not recursively create
   handler work or another checkpoint on a management-only redelivery. Progress
   events use the normal fenced runtime writer; image publication continues to
   require the separate snapshot-publisher authority. Bounded mode requires
   Electric's authenticated write-token gate on raw entity-stream appends;
   ordinary input payloads cannot create progress events. Chronicle treats
   event bodies as opaque, so a direct Chronicle append credential is trusted
   backend authority, not safe browser authority for this mode. Deployments
   permitting untrusted direct raw appends need a different authenticated
   progress format before enabling bounded recovery.
7. Keep Chronicle's responsibilities unchanged. Images are deterministic source
   projections: equal source cut with different image bytes is still a conflict.
   Saving an image never acknowledges work. The runtime's ordinary callback
   remains responsible for acknowledging a successfully completed activation.

## Consequences

The fast path can remove historical transfer, JSON parsing and folding, rather
than only the last of these. It still restores all required live state and reads
new input. Append-only state, a large pending suffix, image size limits and
publication overhead can limit or eliminate the benefit.

The progress append adds source traffic and must be included in measurements.
Paired replay/checkpoint benchmarks use the **same bounded-input contract**;
they do not claim equivalence to legacy handlers that inspect history. A
successful image GET is insufficient evidence: the transparent source gate must
show every admitted source GET starts at or after the image cut, with the correct
incarnation, including failed/aborted requests. State, input, output,
acknowledgment and lease-release assertions remain necessary.

A checkpoint records durable completion; it does not create exactly-once
external effects. Applications must persist effects as source facts or make
external calls idempotent. Required handler writes, including tracked secondary
writes, must settle before certification. Later resource-disposal or final-callback
failure does not undo that durable completion. A teardown failure before the
callback attempts release without acknowledgment; a failed or lost callback
response can leave its outcome uncertain. Surface the failure and let a retry
use the valid checkpoint without rerunning completed handler work. The checkpoint
does not certify successful resource teardown.

Changing an image codec or `projectionVersion` invalidates images, not progress
markers already in the source. Discard disposable streams from the buggy
pre-release bounded builds. If those builds processed real inputs, audit and
rebuild progress under the application's replay/idempotency policy before reuse;
rebuilding an image cannot repair an incorrect durable completion claim.

Tests must cover a progress/image save followed by failed disposal or a failed
final callback, concurrent input during checkpoint creation, and a crash boundary
with multiple inputs in one batch. Broader HA and source-prefix durability
requirements from ADR 0012 remain unchanged.
