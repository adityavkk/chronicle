# 11 — Subscription hardening, as implemented

**Purpose:** record what was actually built against the four crash-window slices
specified in [10-subscription-hardening-handoff.md](10-subscription-hardening-handoff.md),
with the real file names and commit SHAs, and document the one place the
implementation deliberately diverges from doc 10. The design rationale lives in
07/09/10; this is the as-built ledger. Each slice landed as its own commit on
`subscription-hardening`, compiling and green on `go test ./...` and the
conformance suite (`subscriptions: true`) before the next began.

All four windows are origin-restart races between a durable Redis write and a
non-atomic Go follow-up. None changed the `{__ds}` single-slot keyspace, the
at-least-once delivery guarantee, or the `generation`/`wake_id` fence.

---

## Slice 2 — fence rotation on expired-lease takeover

**Commit `457bd69`** (`fix(webhook): rotate the fence on expired-lease claim takeover`).

`webhook/scripts/claim.lua` now mints a fresh `generation` + `wake_id` on every
grantable claim **except** the normal first claim of an already-issued pull-wake
event (`phase == waking` with a `wake_id` set). The decisive case is taking over
a lease whose deadline passed before the lease worker expired it: the takeover
reaches `claim.lua` with `phase == live`, the unexpired-lease `BUSY` guard
already returned for a live holder, so this path knows the lease is expired and
rotates. The deposed worker's still-valid token carries the old generation, so
its late ack now returns `409 FENCED` instead of advancing the cursor under the
new holder's lease.

`webhook/state.go` gains `ClaimRotatesFence(phase, wakeID)` — the pure mirror,
`!(phase == waking && wakeID != "")` — kept beside the existing `ClaimDecision`
reducer so the Go and Lua descriptions of the conflict check change together.

Tests: `TestClaimExpiredLeaseRotatesFence` and `TestClaimUnexpiredLeaseStillBusy`
in `webhook/redis_store_test.go`.

**Framing refinement (review).** Acks are forward-only (`MergeAcks`, and the
authoritative advance in `ack.lua`), so the pre-fix bug could never move a cursor
*backward* — a stale ack could only re-apply or over-advance within the deposed
worker's own snapshot while a second worker held the same `(generation, wake_id)`.
That bounds the impact to split-brain / churn (two live holders of one fence)
rather than silent cursor corruption, but two holders of one fence still violates
the single-holder invariant the lease exists to provide. The rotation restores
it: after a takeover there is exactly one generation that can ack.

## Slice 1 — durable pull-wake recovery

**Commit `19c3af8`** (`fix(webhook): recover a pull-wake stranded between arm and event emit`).

For pull-wake, `issueWake` arms the wake durably (`arm_wake.lua` sets
`phase = waking`, bumps `generation`, mints `wake_id`, and — new here — stamps
`wake_event_sent_ns = 0`; no lease, the lease starts at claim) and then writes
the wake event in a separate, non-atomic step. A crash between the two left
`phase = waking` with no event and nothing to deliver it: the old `sweepOnce`
only re-woke `idle` subscriptions, and a later append coalesced against the
phantom `waking` (the phase gate is "pending AND `phase == idle`").

Fix, three moving parts:

- `arm_wake.lua` stamps `wake_event_sent_ns = 0` for pull-wake (the "not yet
  emitted" marker).
- `writeWakeEvent` (`webhook/manager.go`) records the durable emit via
  `webhook/scripts/record_wake_sent.lua` (store method `RecordWakeEventSent`),
  fenced on `(generation, wake_id)` so a stamp from a superseded wake is ignored.
- `sweepOnce` re-emits any pull-wake stuck in `phase == waking` with
  `wake_event_sent_ns == 0`.

Duplicate wake events are claim-fence-safe: a re-emit produces at most a
duplicate *claim attempt*, never a duplicate cursor advance, because the claim
and ack both run through the generation fence.

Tests: `TestRecordWakeEventSentFences` and `TestSweepReemitsStrandedPullWake` in
`webhook/manager_test.go`.

### Deviation from doc 10

Doc 10's slice-1 design specified a full delivery **outbox**: a new
`ds:{__ds}:sched:wake` ZSET, `wake_event_due_ns`/`wake_event_attempts` fields, a
`DueWakes` claim, and a dedicated `wakeWorker` that re-scores due wakes forward
and re-appends them — the same re-score-never-`ZREM` machinery as the lease and
retry workers.

**Built instead:** a flag plus the existing sweep. A single
`wake_event_sent_ns` field (0 = stranded, else the emit timestamp),
`record_wake_sent.lua` to stamp it under the fence, and a branch in `sweepOnce`
that re-emits stranded pull-wakes. No `sched:wake` ZSET, no `DueWakes`, no
`wakeWorker`.

**Why.** The two designs deliver the same guarantee — a pull-wake stranded
between arm and emit is eventually re-emitted, at-least-once, made safe by the
fence — but the sweep is *already* the recovery path for every other stuck-wake
case (idle-with-pending, expired lease). A dedicated outbox would add a third
schedule ZSET and a fourth worker loop to re-implement retry semantics the sweep
already has. The flag turns "re-emit a stranded pull-wake" into one more
predicate in the scan the sweep was already doing. Far less machinery, same
correctness. The outbox's only edge over the flag is *latency* — a ZSET due-score
fires sooner than the next 2 s sweep tick — and a stranded pull-wake is a
post-crash recovery event, not a hot path, so sweep latency is the right cost to
pay. If a future workload needs sub-sweep recovery latency for wakes, the outbox
in doc 10 §slice-1 is the upgrade path.

## Slice 3 — glob-link reconciliation

**Commit `5f70a1c`** (`fix(webhook): reconcile glob links missed by a crashed OnStreamCreated`).

`OnStreamCreated` is a best-effort post-commit hook and a new pattern
subscription's create-time backfill is best-effort; a crash in either leaves a
matching stream unlinked, and it does not self-heal — a later append to an
unlinked stream has no subscriber in the fan-out to wake, and the sweep only
re-evaluates *existing* links.

Fix: a separate slow reconcile loop. `store/redis/list.go` gains `ListStreamMeta`,
returning `{path, tail, created-at}` per live stream (one pipelined `HMGET` per
candidate, soft-deleted streams skipped, the reserved `{__ds}` slot excluded).
`reconcilePatternLinks` lists streams once and, for every pattern subscription,
links any matching stream it is missing:

- at the **beginning** offset when `stream.CreatedAt > sub.CreatedAt` — the
  stream was created during the outage (a missed `OnStreamCreated`), so its data
  should wake the subscriber and is replayed; or
- at the **tail** when the stream predates the subscription — a missed
  pre-existing-stream backfill, no history replayed.

`webhook/manager.go` gains `reconcilePatternLinks` / `reconcileLoop` /
`reconcileOnce` / `RunReconcile`.

Tests: `TestReconcileBackfillsPatternLinkForStreamCreatedDuringOutage` and
`TestReconcileBackfillsPreexistingStreamAtTail` in `webhook/manager_test.go`.

## Slice 4 — fan-out index repair

**Commit `909915f`** (`fix(webhook): repair the fan-out index from canonical links`).

The per-stream fan-out index (`ds:{__ds}:stream:<path>` SET) that drives
`OnStreamAppend` is maintained from Go *after* the Lua link write. A crash
between drops the index entry while the canonical link
(`ds:{__ds}:sub:<id>:links` HASH) survives — the subscription is still correct in
Redis but that stream's appends fall back to sweep latency instead of the
low-latency append trigger.

Fix: `RedisStore.ReconcileIndexes` rebuilds the `SADD`s from the canonical links.
It mirrors links and never invents membership — it cannot add a stream a
subscription is not actually linked to. It runs in the reconcile loop.

This is the lowest-severity slice: the index is a cache, not correctness state,
so a dropped entry self-heals via the sweep and the only symptom is latency. That
is why it shares the slow reconcile loop rather than warranting its own path.

Tests: `TestReconcileRepairsMissingFanoutIndex` and
`TestReconcileIndexDoesNotInventMembership` in `webhook/redis_store_test.go`.

## Review refinement — the reconcile loop is decoupled from the fast sweep

Doc 10 sketched slices 3 and 4 as work folded "inside the sweep" with a bounded
budget. As built, the two reconciliation passes (pattern-link recovery and
fan-out index repair) run on a **separate** loop —
`reconcileLoop` / `reconcileOnce`, default interval 30 s — not on the 2 s
`sweepOnce` tick.

The reason is cost. The fast sweep is `O(subscriptions)`: it reads each
subscription and recomputes pending work against cached tails. Pattern
reconciliation is `O(pattern subs × streams)` because it lists every live stream
and glob-matches; index repair walks every link. Running an `O(streams)` scan on
the 2 s recovery cadence would couple the latency-critical stuck-wake backstop to
a keyspace-sized scan. Decoupling keeps the sweep fast and the expensive
reconcile on a cadence matched to its job — recovering from rare crash windows,
not steady-state delivery. Correctness is unchanged; only the schedule differs.

## Net

- Four committed slices (`457bd69`, `19c3af8`, `5f70a1c`, `909915f`), each with
  tests, on `subscription-hardening`.
- One deliberate deviation from doc 10: slice 1 uses the lighter sweep-based
  re-emit (`wake_event_sent_ns` + `record_wake_sent.lua` + `sweepOnce`) rather
  than the doc-10 outbox (`sched:wake` ZSET + `DueWakes` + `wakeWorker`) — same
  at-least-once guarantee, far less machinery, sweep latency the acceptable cost.
- One scheduling refinement: the reconcile loop (slices 3 + 4) is decoupled from
  the fast sweep because it is `O(streams)`, not `O(subscriptions)`.
- One framing refinement: forward-only acks bound the pre-fix slice-2 bug to
  split-brain / churn rather than cursor corruption; the rotation restores the
  single-holder invariant.
- `webhook/` remains the only subscription implementation. The `{__ds}` keyspace,
  at-least-once delivery, and the `generation`/`wake_id` fence are unchanged.
  `go test ./...` and the 332-test conformance suite (`subscriptions: true`) are
  green.
