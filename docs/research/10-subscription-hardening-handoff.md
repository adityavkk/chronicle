# 10 -- Subscription hardening handoff

## Decision

Start from `main`, not from `codex-subscription`.

`main` already has the right control-plane shape: the `webhook/` package keeps
subscription config, links, generation, wake ids, lease schedules, retry
schedules, JWKS, and token keys in Redis under the `{__ds}` hash tag. Critical
state transitions run through Lua scripts. The background manager has lease,
retry, and recovery workers. This is the architecture described by
[09-subscription-implementation-plan.md](09-subscription-implementation-plan.md).

The `codex-subscription` branch is useful as a validation branch. It passed the
full durable-streams conformance suite and has a cleaner conformance runner and
restart smoke report, but its implementation is less safe: claim uses Redis
`WATCH`, while ack/release validate and then mutate state through multiple Go
calls. Do not port the `subscription/` package into `main`.

Sources compared:

- `main` at `32c8d1edde913a5b17572b369e01eceaaddfa312`.
- `origin/codex-subscription` at
  `a1cdac799dc40edaac6cb6163e2cb7a51ce60189`.
- The durable subscription implementation on `main` starts at
  `ec4ab93 test(jepsen): k8s fault-injection durability harness + results`.

## Existing main implementation

The source implementation lives in `webhook/`.

- `webhook/types.go` defines `Subscription`, `Config`, `StreamLink`, `Phase`,
  and the wire-facing domain nouns.
- `webhook/keys.go` defines the Redis control-plane keyspace. All correctness
  keys use `ds:{__ds}:...`.
- `webhook/redis_store.go` is the imperative Redis shell. It calls Lua for
  `CreateOrConfirm`, `ArmWake`, `Claim`, `Ack`, `Release`, `ExpireLease`,
  `DueLeases`, and `DueRetries`.
- `webhook/scripts/*.lua` contains the atomic state transitions.
- `webhook/manager.go` owns stream hooks, wake issuance, webhook delivery,
  lease/retry workers, and the recovery sweep.
- `webhook/routes.go` exposes the reserved `__ds` API surface.
- `handler.go` calls `OnStreamCreated` and `OnStreamAppend` after durable stream
  writes.
- `jepsen/` and `docs/jepsen/results.md` document the current k8s failure
  harness.

The branch is stronger than `codex-subscription` because `ack.lua` fences and
advances cursors in one Redis turn, `claim_due.lua` re-scores due ZSET entries
instead of removing them, and the manager has a sweep that can recover idle
subscriptions with pending work.

## What to port from `codex-subscription`

Port these ideas or files selectively. Keep the main `webhook/` package as the
only subscription implementation.

1. Conformance script ergonomics.
   - Source: `origin/codex-subscription:scripts/conformance.sh`.
   - Add `CHRONICLE_SKIP_REDIS_START`, `CHRONICLE_REDIS_CONTAINER`, and
     `CHRONICLE_REDIS_DB`.
   - Preserve the main script's behavior by default.
   - Use `npm --prefix test/conformance install --package-lock=false
     --no-audit --no-fund --silent` if the team wants conformance runs not to
     rewrite `package-lock.json`.

2. Verification report shape.
   - Source:
     `origin/codex-subscription:docs/durable-subscriptions/implementation-report.md`.
   - Port the idea, not the exact results. Main already has stronger Jepsen
     docs, but a concise "commands run, pass count, Redis DBs, k8s context"
     report makes later audits easier.

3. Restart test cases.
   - Source:
     `origin/codex-subscription:subscription/integration_test.go`.
   - Port the scenarios into `webhook/redis_store_test.go` or a new
     `webhook/manager_integration_test.go`:
     callback token survives manager restart, one of many concurrent workers
     claims, and a pending pull wake survives a manager restart.
   - Main has overlapping coverage. Add only tests that cover a distinct
     failure window.

4. Pre-claim pending-work behavior.
   - Source:
     `origin/codex-subscription:subscription/manager.go`.
   - Main's claim route calls `store.Claim` without first proving pending work.
     Add a test for "claim idle subscription with no pending streams".
   - If the pinned conformance suite and `PROTOCOL.md` require `NO_PENDING_WORK`,
     add the check in `webhook/routes.go` before minting the claim token. If the
     protocol permits empty claims, document the decision in this file's follow-up
     notes.

Do not port:

- `subscription/redis_repository.go` as an implementation. It uses a mix of
  `WATCH`, pipelines, and separate repository calls where main already has Lua.
- The old branch's `subscription/` package names. Main deliberately keeps the
  Caddy-aligned `webhook/` package name.
- Any Redis Streams wake-queue design. Main's HASH/ZSET/Lua control plane is the
  chosen architecture.

## Hardening slice 1: durable pull-wake event emission

### Current failure window

`webhook/manager.go` arms a pull-wake before it writes the wake event:

- `issueWake` calls `store.ArmWake` at `webhook/manager.go:236`.
- `arm_wake.lua` sets `phase = waking`, increments `generation`, and stores
  `wake_id` at `webhook/scripts/arm_wake.lua:13`.
- For pull-wake, `arm_lease = 0`, so no lease ZSET entry is created.
- `issueWake` then calls `writeWakeEvent` at `webhook/manager.go:255`.
- `writeWakeEvent` appends to `wake_stream` through `AppendWakeEvent` at
  `webhook/manager.go:259`.

If the process exits after `ArmWake` and before `AppendWakeEvent`, Redis
durably records `phase=waking`, but no worker ever sees a wake event. The
current sweep does not repair this state:

- `sweepOnce` only reissues wakes for `phase == idle` at
  `webhook/manager.go:481`.
- `expire_lease.lua` explicitly leaves lease-less pull-wake `waking` state alone
  at `webhook/scripts/expire_lease.lua:3`.

### Required design

Add a durable pull-wake delivery outbox in the `{__ds}` control plane.

Recommended Redis additions:

- `wakeZKey = ds:{__ds}:sched:wake` in `webhook/keys.go`.
- Subscription hash fields:
  - `wake_event_due_ns`
  - `wake_event_sent_ns`
  - `wake_event_attempts`

Change `arm_wake.lua`:

- Accept `wake_zset` as a key.
- When `arm_lease == 0`, set `wake_event_due_ns = now_ns` and `ZADD wake_zset
  now_ns <subscription_id>`.
- Keep webhook behavior on `leaseZKey`; webhook delivery already has durable
  retry and lease schedules.

Add store methods in `webhook/store.go` and `webhook/redis_store.go`:

- `DueWakes(now time.Time, limit int, visibility time.Duration) ([]string, error)`
  using the existing `claim_due.lua`.
- `RecordWakeEventDelivered(id string, generation int64, wakeID string, now time.Time) (string, error)`
  as a Lua script that removes `id` from `wakeZKey` only if the stored
  generation and wake id still match.
- `ScheduleWakeEventRetry(id string, generation int64, wakeID string, next time.Time)`.

Add a `wakeWorker` in `webhook/manager.go`:

1. Claim due IDs from `wakeZKey` by re-scoring them forward, the same way
   `leaseWorker` and `retryWorker` claim work.
2. Load the subscription.
3. If it is not `pull-wake`, not `phase=waking`, or the generation/wake no
   longer match the due item, mark the outbox item complete.
4. Append the wake event to `wake_stream`.
5. On append success, call `RecordWakeEventDelivered`.
6. On append failure, leave or re-score the ZSET entry so another worker retries.

The worker may append a duplicate wake event if the process dies after append
and before `RecordWakeEventDelivered`. That is acceptable if claim fencing works:
duplicate wake events produce duplicate claim attempts, not duplicate cursor
advances.

### Tests

Add tests before code where possible.

- `TestPullWakeOutboxRetriesAppendFailure`:
  inject an `AppendWakeEvent` failure, verify `phase=waking` plus `wakeZKey`
  remains, then run the wake worker/sweep and verify the wake event appears.
- `TestPullWakeOutboxCrashAfterAppendBeforeMarkDelivered`:
  simulate append success with no `RecordWakeEventDelivered`, run the worker
  again, and verify duplicate wake events do not allow two live holders.
- k8s scenario:
  add a fault-injection mode that exits after `ArmWake` and before wake event
  append. The checker should still observe the cursor reaching tail after fresh
  pods start.

## Hardening slice 2: rotate the fence on expired claim takeover

### Current failure window

`claim.lua` rejects a claim while another worker has an unexpired live lease:

- `webhook/scripts/claim.lua:19` returns `BUSY` when `phase == live`,
  `holder == 1`, and `lease_until > now`.

If the lease has expired but the lease worker has not run yet, a second worker
can claim the same stored `generation` and `wake_id`:

- `claim.lua` only increments `generation` when `phase == idle` or `wake == ''`.
- For `phase == live` with an expired lease, it reuses the old generation and
  wake id.

The token is signed over subscription id and generation, not over holder or a
stored token id:

- `webhook/crypto.go` mints token payloads with `Sub`, `Generation`, `Exp`, and
  `Jti`.
- `webhook/crypto.go` validates signature, subject, expiry, and returns
  generation.
- `common.lua` fences only `token_generation`, request `generation`, and
  request `wake_id`.

Result: worker A can receive token `(generation=N, wake_id=W)`, let its lease
expire, worker B can claim the same `(N, W)`, and worker A can still ack with
its unexpired token. That stale ack can advance the cursor during worker B's
lease.

### Required design

Rotate the fence when a claim takes over an expired live lease.

Preferred change in `claim.lua`:

- If `phase == live` and the stored lease is expired, increment `generation` and
  replace `wake_id` with the caller-provided new wake id before granting the
  claim.
- If `phase == waking` with no live holder, keep the existing generation and
  wake id. That is the normal first claim of an already-issued pull wake event.
- Keep `BUSY` for unexpired live leases.

Update the pure mirror in `webhook/state.go` so tests and comments match Lua.

Alternative design:

- Store the token `jti` or a `token_generation` field in Redis and require
  `ack.lua`/`release.lua` to match it.

The generation-rotation change is simpler and matches the protocol's generation
fence. Use token-id storage only if upstream requires reusing the same
generation across expired holder takeovers.

### Tests

- `TestClaimExpiredLeaseRotatesFence`:
  worker A claims, time advances past the lease, worker B claims, generation or
  wake id changes, worker A ack returns `FENCED`, worker B ack succeeds.
- `TestClaimUnexpiredLeaseStillBusy`:
  existing behavior remains `ALREADY_CLAIMED`.
- Add a k8s checker case where a worker claims and sleeps beyond
  `lease_ttl_ms` while another worker claims and acks.

## Hardening slice 3: recover missed glob links

### Current failure window

Glob links are not atomically created with stream creation.

- `handler.go` calls `h.onStreamCreated(path)` only after stream creation has
  committed at `handler.go:264`.
- `OnStreamCreated` scans subscriptions and calls `store.Link` for matching
  patterns at `webhook/manager.go:171`.
- A pattern subscription's initial backfill is best-effort in
  `webhook/manager.go:523`.
- `sweepOnce` only evaluates existing links at `webhook/manager.go:497`.

If the process exits after the stream commit and before `OnStreamCreated`, an
existing pattern subscription never receives a link. If the process exits after
subscription creation and before `backfill`, a new pattern subscription misses
older streams. The sweep cannot wake work that has no link.

### Required design

Make pattern link reconciliation part of the recovery sweep.

Extend the stream lister boundary:

- Current lister: `store/redis/list.go` exposes `ListStreamPaths`.
- Add a lister method that returns path, current tail, and stream `CreatedAt`.
  The metadata already contains `CreatedAt` in `store/store.go` and
  `store/redis/meta.go`.

Add `Manager.reconcilePatternLinks(now)`:

1. List all active subscriptions.
2. List all live streams with path, tail, and created time.
3. For every subscription with `Config.Pattern`, match paths through
   `GlobMatch`.
4. If the link is missing:
   - If `stream.CreatedAt` is after `sub.CreatedAt`, link at beginning offset.
     This recovers a missed `OnStreamCreated`, including an initial append that
     happened during the outage.
   - If `stream.CreatedAt` is before or equal to `sub.CreatedAt`, link at the
     current tail. This recovers a missed subscription-create backfill without
     replaying history.
5. After linking, call `maybeWake` or let the normal pending check in the same
   sweep issue the wake.

Performance note: the first implementation can be `O(subscriptions * streams)`
inside the sweep. If this becomes too expensive, add a Redis stream registry and
pattern index later. Correctness comes first; the sweep is the outage backstop,
not the low-latency path.

### Tests

- `TestSweepBackfillsPatternLinkForStreamCreatedDuringOutage`:
  create a pattern subscription, create a matching stream without calling
  `OnStreamCreated`, append data, run `RunSweep`, verify the link exists at the
  beginning offset and a wake is issued.
- `TestSweepBackfillsPatternSubscriptionCreateCrash`:
  create an existing stream, create a pattern subscription while suppressing
  `backfill`, run `RunSweep`, verify the link exists at the current tail and no
  historical data is delivered.
- Add a Jepsen scenario where origins are killed immediately after stream
  creation with initial data.

## Hardening slice 4: repair the fan-out index

### Current failure window

Canonical links live in `ds:{__ds}:sub:<id>:links`, but the append-time fan-out
index is maintained from Go after Lua state changes:

- `CreateOrConfirm` runs `create_sub.lua`, then `indexStream` for each explicit
  link at `webhook/redis_store.go:63`.
- `Link` runs `link_stream.lua`, then `indexStream` at
  `webhook/redis_store.go:134`.
- `streamSubsKey(path)` is documented as best-effort in `webhook/keys.go`.
- `OnStreamAppend` reads only `StreamSubscribers(path)` at
  `webhook/manager.go:194`.

If an origin dies after the canonical link write but before `indexStream`, the
subscription remains correct in Redis but loses the low-latency append trigger.
The sweep can still wake already-linked pending work, but it does not rebuild
the fan-out index. Future appends may rely on the sweep until the index is
repaired.

### Required design

Treat `links` as canonical and `streamSubsKey` as a cache.

Add `RedisStore.ReconcileIndexes(ctx)` or a manager-level reconciliation step:

1. Read all subscription ids from `subsKey`.
2. For each subscription, read `linksKey(id)`.
3. For each link path, ensure `SADD streamSubsKey(path) id`.
4. Optionally remove stale entries:
   - For each `streamSubsKey(path)` encountered, remove ids whose links no
     longer contain `path`.
   - Defer full stale cleanup if it is expensive; missing `SADD` repair is the
     correctness-critical part.

Run index repair:

- At manager start before or during the first `RunSweep`.
- Periodically inside the recovery sweep with a bounded budget.
- After `CreateOrConfirm`, `Link`, and pattern backfill as a fast path, keeping
  existing behavior.

### Tests

- `TestSweepRepairsMissingFanoutIndex`:
  create a subscription and link in Redis, delete the `streamSubsKey` member,
  append a message, run sweep or repair, verify future appends wake through the
  index.
- `TestIndexRepairDoesNotCreateLinks`:
  prove the repair only mirrors canonical links and does not invent stream
  membership.

## Hardening slice 5: docs and operational truth

Some docs still describe the pre-implementation state.

- `README.md` still says the optional subscription API is not implemented.
- `docs/research/07-subscription-wake-lease-durability.md` says the layer is not
  implemented and `/__ds/*` returns `501`.
- `docs/research/09-subscription-implementation-plan.md` says the branch is
  "in progress".

After slices 1-4 land, update these docs:

- Mark the `webhook/` Redis implementation as the current source.
- Keep the historical design notes in 07 and 09, but add a status note at the
  top pointing to this handoff and the implementation.
- Add an operations note for Redis durability posture:
  `appendonly yes`, AOF on persistent storage, and the deployment's chosen
  fsync/replication tradeoff.

## Test matrix for the final branch

Run these before merging the hardening work:

```bash
go test ./...
./scripts/conformance.sh
```

If using the codex conformance runner improvements:

```bash
CHRONICLE_SKIP_REDIS_START=1 \
CHRONICLE_REDIS_CONTAINER=<redis-container> \
CHRONICLE_REDIS_DB=15 \
./scripts/conformance.sh
```

Run the k8s failure harness:

```bash
jepsen/up.sh
jepsen/run.sh baseline origin-restart redis-restart
```

Add these scenarios to `jepsen/checker`:

- `pull-wake-arm-crash`: kill all Chronicle pods after `ArmWake` and before
  wake event append. Expected result: the outbox worker emits or re-emits the
  wake and the cursor reaches tail.
- `expired-lease-takeover`: worker A claims and stalls past `lease_ttl_ms`,
  worker B claims and acks, worker A's later ack is `409 FENCED`.
- `glob-create-crash`: create matching streams while killing origins before
  `OnStreamCreated`. Expected result: recovery backfills links and delivers
  pending work.
- `index-repair`: delete selected `ds:{__ds}:stream:<path>` entries during the
  workload. Expected result: sweeps repair the index and later appends wake
  without waiting for the next full scan.

For Redis restart tests, keep the main harness's persistent AOF/PVC model from
`docs/jepsen/results.md`. An ephemeral Redis pod deletion tests data loss from
ephemeral storage, not subscription durability.

## Implementation order

1. Add tests for the four confirmed failure windows.
2. Implement expired-lease claim fence rotation. It is the smallest code change
   and protects cursor correctness.
3. Implement the pull-wake outbox. It closes the strongest stuck-wake window.
4. Add pattern reconciliation to the sweep.
5. Add fan-out index repair.
6. Port conformance runner ergonomics from `codex-subscription`.
7. Update docs and Jepsen scenarios.

Each slice should compile, pass `go test ./...`, and keep the conformance suite
green before the next slice starts.

## Acceptance criteria

The hardening work is done when:

- `webhook/` remains the only subscription implementation.
- Full durable-streams conformance passes with `subscriptions: true`.
- A stale worker cannot ack after an expired lease takeover.
- A pull-wake cannot remain permanently stuck in lease-less `waking` state after
  an origin crash.
- Pattern subscriptions recover links for streams created during origin outages.
- Missing fan-out index entries are repaired from canonical links.
- The k8s harness documents baseline, origin restart, Redis restart, and the new
  pull-wake/glob/fencing failure scenarios.
- README and research docs no longer claim subscriptions are unimplemented.
