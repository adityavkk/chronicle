# ADR-0001: Use Lua `EVAL`/`EVALSHA` (not Redis Functions) for atomic grouped Redis operations

- **Status:** Accepted
- **Date:** 2026-06-15
- **Deciders:** @adityavkk
- **Tracking issue:** [#4](https://github.com/adityavkk/chronicle/issues/4)
- **Research:** [`docs/specs/research/redis-grouped-actions/`](../specs/research/redis-grouped-actions/README.md)

## Context

chronicle performs every stream mutation as an **atomic group** of Redis
operations — read → validate → branch → write → notify — because the Durable
Streams protocol attaches correctness guarantees to the group (idempotent-producer
epoch/seq fencing, `Stream-Seq` regression checks, lazy TTL expiry, closed-stream
semantics, optimistic-tail framing, lease compare-and-set with fence rotation).
None of this is expressible as a single command or inside a `MULTI/EXEC`
transaction (which cannot branch on an intermediate read). So chronicle runs each
group as **server-side Lua**, executed atomically on Redis's single-threaded
engine, via go-redis `Script.Run` (`EVAL`/`EVALSHA`). There are ~21 such scripts
across two subsystems (`store/redis/scripts/`, `webhook/scripts/`), each prefixed
at load time with a string-concatenated copy of a shared `common.lua` prelude.

An earlier decision (`docs/research/05-redis-design.md` §5, `docs/PLAN.md`)
rejected **Redis Functions** (`FUNCTION LOAD`/`FCALL`) on portability grounds.
That decision was **re-opened** because the operator now has **Redis 8 in a
managed enterprise offering**, where Functions are available — raising the
question of whether the Lua approach is still best practice and whether Functions
(or another approach) is now better.

The evaluation (multi-agent research + adversarial verification + an independent
primary-source pass; see the research folder) examined the realistic option space:
(a) stay on Lua as-is, (b) stay on Lua and fix the prelude pattern, (c) migrate to
Redis Functions, (d) non-Lua alternatives (`MULTI/EXEC`, `WATCH`, pipelining,
native modules).

### Decision drivers

- **Correctness of linearizable mutations** — must not regress.
- **Portability** across managed Redis (the README pitches "managed Redis
  generally").
- **Failover/restart resilience** with minimal operator burden.
- **Maintainability** of the shared-helper code.

## Decision

**Stay on Lua `EVAL`/`EVALSHA` via go-redis `Script.Run` (option b). Do not
migrate to Redis Functions.** Adopt the cheap, behavior-preserving improvements
that capture the one legitimate advantage Functions would have offered.

The decisive facts (each verified against primary sources in the research docs):

1. **Functions buy zero correctness.** On the two axes chronicle's correctness
   rests on — atomic single-threaded execution and effects-based replication —
   Redis documents Functions and `EVAL` Lua as **identical**. chronicle's fencing
   and fork bookkeeping derive from atomicity + the `{path}`/`{__ds}` single-slot
   hash-tag discipline, which both surfaces share. `EVAL` is **not deprecated**.
2. **Functions cost more than they give, *for chronicle*.** go-redis has **no
   `FCall` auto-reload** analogous to `Script.Run`, so Functions would replace
   chronicle's self-healing failover with an operator-owned bootstrap (startup
   `FUNCTION LOAD` + catch-load-retry shim + per-master cluster loading). They are
   also **blocked on AWS ElastiCache Serverless and MemoryDB Multi-Region** while
   `EVAL` runs everywhere — `EVAL` is a strict superset of deployment targets.
3. **Prior art agrees.** BullMQ, Sidekiq, go-redsync, Stripe, and Celery all use
   `EVAL`/`EVALSHA` for exactly this; effectively none migrated to Functions.

The one genuine weakness of the status quo — the `common.lua`
prelude-concatenation (~58% of cached script source is redundant copies; editing
the prelude re-SHAs every script in its group) — is fixable **without** Functions
via a build-time include (the cleaner form of what BullMQ already does).

Full reasoning, decision matrix, and conditions-to-reconsider:
[`05-recommendation.md`](../specs/research/redis-grouped-actions/05-recommendation.md).

### Considered and rejected

- **(c) Migrate to Redis Functions** — rejected: zero correctness gain, a net
  reliability regression under go-redis (no auto-reload), and a portability loss
  the "managed Redis generally" pitch cannot absorb. Kept as a *conditional
  future* (see Consequences → "When to revisit").
- **(d) `MULTI/EXEC` / `WATCH` / pipelining / native modules** — rejected upstream:
  transactions can't branch on an intermediate read; `WATCH` needs retry loops
  (Redis itself recommends Lua over `WATCH` here); pipelining isn't atomic; native
  modules aren't loadable on managed Redis.

## Consequences

### Positive

- **No migration, no rewrite, no correctness risk.** The codebase stays as-is on
  the dimension that matters.
- **Maximal portability retained** — runs on every surveyed managed Redis,
  including ElastiCache Serverless and MemoryDB Multi-Region.
- **Self-healing failover retained** — `Script.Run`'s `EVALSHA → full-source EVAL`
  fallback recovers from a flushed/failed-over cache in one extra round-trip per
  script per node, with zero operator runbook.

### Negative / trade-offs (accepted)

- Forgoes Functions' AOF-persistence/replication of the logic and
  `FUNCTION LIST/STATS/DUMP` introspection. Acceptable today; a gap only if
  script-level audit/backup or server-side durability becomes a hard requirement.
- The team must **never wrap these scripts in a pipeline/`MULTI`** — `Script.Run`'s
  `NOSCRIPT` fallback does not fire there (go-redis #3228).

### Follow-up actions

**Required (documentation only — no behavior change):** correct three inaccurate
statements in the repo docs. Tracked in [#4](https://github.com/adityavkk/chronicle/issues/4):

1. `NOSCRIPT → SCRIPT LOAD → retry` → the default `NewScript().Run()` path is
   `NOSCRIPT → full-source EVAL → retry`; go-redis never issues `SCRIPT LOAD` for
   `NewScript` (`05-redis-design.md:223,460`).
2. `nowMs` → the implemented scripts pass/compute in **nanoseconds** (`nowNs` /
   `UnixNano`) (`05-redis-design.md` §5 + §8 catalog).
3. Re-scope the Functions-rejection rationale from "more often ACL-blocked" to the
   precise platform facts, and clarify that `nowNs`-as-`ARGV` is for **single-clock
   consistency** between Go and Lua (under effects replication, `TIME` is legal —
   it is not a determinism requirement) (`05-redis-design.md:220-221,379`;
   `PLAN.md:176-178`).

   *(These corrections are applied in the same change that introduces this ADR.)*

**Optional (code hygiene — behavior-preserving, may be deferred):**

4. Replace runtime `common.lua` concatenation with a **build-time include**
   (`go:generate` / BullMQ-style `--- @include`) — one source of truth.
5. Adopt `redis.NewScriptServerSHA` (go-redis v9.19.0+; chronicle is on v9.20.0)
   so scripts are `SCRIPT LOAD`-class and exempt from Redis 7.4's `EVAL`
   LRU-eviction cap (~500).
6. Add a guard/comment that these scripts must not be wrapped in a
   pipeline/`MULTI` (go-redis #3228).

> **No correctness or behavior change to the code is required by this decision.**
> Items 4–6 are quality improvements, not fixes.

### When to revisit

Reconsider migrating to Functions only if **all** hold: (1) chronicle drops the
broad managed-Redis pitch for a single enterprise target and removes ElastiCache
Serverless / MemoryDB Multi-Region / pre-7.0 from its supported matrix; **and**
(2) go-redis ships an `FCall` auto-reload wrapper (or chronicle accepts owning the
bootstrap runbook); **and** (3) a concrete need emerges that Functions uniquely
serve (introspection/audit, server-side durability, multi-library versioning).
Details in [`05-recommendation.md`](../specs/research/redis-grouped-actions/05-recommendation.md).
