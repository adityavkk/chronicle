# Async subscription fan-out implementation plan

## Implementation

- [x] Add a pure bounded queue with explicit enqueue, coalesce, overflow,
  processing, retry, and completion transitions.
- [x] Make `OnStreamAppend` perform only the bounded queue handoff.
- [x] Add a manager-owned async worker with idempotent lifecycle and bounded
  batches.
- [x] Batch subscriber hydration and distinct linked-tail reads for each dirty
  stream.
- [x] Route overflow and processing errors through the existing eager recovery
  seam without weakening the periodic sweep.
- [x] Add bounded-cardinality metrics for the synchronous hook, queue, async
  processing, overflow, recovery requests, and recovery delay.
- [x] Document the backpressure and failure policy in `docs/PLAN.md`.

## Verification

- [x] Unit-test every queue transition, fairness, bounds, retry, and shutdown.
- [x] Prove blocked subscriber lookup and blocked tail evaluation do not delay an
  HTTP append response.
- [x] Prove failed and producer-duplicate appends enqueue nothing.
- [x] Prove eventual wake, burst coalescing, duplicate safety, overflow recovery,
  retry, reconnect recovery, restart recovery, and owner transfer.
- [x] Cover direct and glob links, multiple links, webhook and pull-wake, stream
  close and delete, concurrent append and shutdown, and queue saturation.
- [x] Extend fault coverage only at the real process-local notification boundary.
- [x] Benchmark subscriber counts 1, 4, 64, 256, and 1000 with multiple link
  counts before and after.
- [x] Run every repository, conformance, fault, race, leak, and Jepsen gate named
  in the issue acceptance criteria.
- [x] Review the final diff, record evidence, and commit locally with the required
  human author and committer identity. Do not push.
