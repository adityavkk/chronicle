# Redis grouped-actions research — Lua scripts vs Redis Functions

**Question.** chronicle performs every stream mutation as an *atomic group* of
Redis operations (read → validate → branch → write → notify) using **server-side
Lua scripts** run through go-redis `Script.Run` (`EVAL`/`EVALSHA`). Is that best
practice? Now that the operator has **Redis 8 in a managed enterprise offering**,
should chronicle reach for **Redis Functions** (`FUNCTION LOAD`/`FCALL`) — which
the repo previously rejected on portability grounds — or something else? This
folder evaluates the current approach against Functions and every realistic
alternative, grounded in primary Redis docs, cloud-provider docs, and production
prior art, and issues a recommendation.

> Tracked in **issue #ISSUE_NUMBER** (branch `research/redis-grouped-actions`,
> spun out of the `horizontal-scale` work).

## Headline: the Lua approach is best practice — keep it, and fix one wart

chronicle's use of `EVAL`/`EVALSHA` Lua for atomic grouped operations is
**squarely best practice and exactly what the production ecosystem does**
(BullMQ, Sidekiq, go-redsync, Stripe, Celery — all `EVAL`/`EVALSHA`, effectively
none on Functions). The recommendation is **stay on Lua; do not migrate to Redis
Functions.** Confidence: high. Full reasoning in
[05-recommendation.md](05-recommendation.md).

The decision rests on one clean split:

- **On correctness, Functions and `EVAL` Lua are identical.** Same atomic
  single-threaded execution, same effects-based replication (the only mode since
  Redis 7.0). chronicle's idempotent-producer fencing and fork bookkeeping come
  from *atomicity + `{path}`/`{__ds}` single-slot hash tags*, which both share.
  **Migrating buys zero correctness.** `EVAL` is also **not deprecated**.
- **So the case for Functions is pure packaging ergonomics** — and each win is
  weak for chronicle or fixable without them, while three real costs (no go-redis
  `FCall` auto-reload → lost self-healing failover; lost reach to ElastiCache
  Serverless / MemoryDB Multi-Region; a pipeline hazard) cut against migrating.

## The one genuine wart, and the cheap fix

The status quo's real weakness is the **prelude-concatenation pattern**: every
one of ~21 scripts ships a full duplicate copy of its `common.lua` (~58% of cached
script source is redundant), and editing `common.lua` re-SHAs every script in its
group. This *is* the "scripts can't share code" limitation Redis says Functions
were built to solve — but it's recoverable **without** Functions via a
**build-time include** (the cleaner version of what BullMQ already does). Plus
three statements in the repo docs are simply wrong and should be corrected.

## What it means for chronicle (action plan)

Do the cheap, low-risk work; skip the migration:

1. **Correct three false repo-doc statements** — the `NOSCRIPT → SCRIPT LOAD`
   claim (it's actually `NOSCRIPT → full-source EVAL`), the `nowMs` unit (it's
   nanoseconds), and the stale "determinism" rationale for `nowNs`-as-`ARGV` (the
   real reason is single-clock consistency); and re-scope the Functions-rejection
   rationale to the precise platform facts.
2. **Replace runtime `common.lua` concatenation with a build-time include** — one
   source of truth, no behavior change. Captures the dedup half of what Functions
   offered.
3. **Optionally adopt `redis.NewScriptServerSHA`** (v9.19.0+) so scripts are
   `SCRIPT LOAD`-class and exempt from Redis 7.4's `EVAL` LRU-eviction cap.
4. **Guard against pipelining `Script.Run`** (go-redis #3228).

**Reconsider Functions only if** chronicle drops the broad-managed-Redis pitch for
a single enterprise target **and** go-redis ships an `FCall` auto-reload **and** a
concrete need (introspection/audit/server-side durability) emerges. See
[05](05-recommendation.md) §"Conditions".

## Documents

| # | Doc | What it covers |
|---|---|---|
| 01 | [current-approach.md](01-current-approach.md) | How chronicle does grouped Redis actions today (code-accurate); the prelude pattern; the prior decision |
| 02 | [redis-functions.md](02-redis-functions.md) | Redis Functions deep dive — what they change, what they don't, the go-redis client asymmetry |
| 03 | [managed-platform-support.md](03-managed-platform-support.md) | `EVAL` vs `FUNCTION` support matrix across AWS/Azure/GCP/Redis Enterprise/Valkey (primary sources) |
| 04 | [alternatives-and-prior-art.md](04-alternatives-and-prior-art.md) | MULTI/EXEC, WATCH, pipelines, modules; what BullMQ/Sidekiq/redsync/Stripe actually do |
| 05 | [recommendation.md](05-recommendation.md) | The decision: matrix, action plan, risks, conditions to reconsider |

## Method

Findings were produced by a multi-agent research workflow (7 parallel research
angles → adversarial verification of each angle's decision-critical claims → a
pro-Functions / pro-Lua advocacy panel → a weighed synthesis), cross-checked
against an independent primary-source pass (the `search` skill + direct fetches of
redis.io, AWS/Azure/GCP docs, and go-redis source). Every load-bearing claim is
cited inline in the per-topic docs.
