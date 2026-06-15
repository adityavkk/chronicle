# 05 — Recommendation

**Verdict: stay on Lua `EVAL`/`EVALSHA` via go-redis `Script.Run`. Do not migrate
to Redis Functions. Instead, do the three cheap fixes that capture the one
legitimate advantage Functions would have offered.** Confidence: **high.**

This is a *hybrid* answer in the precise sense that the migrate-to-Functions
advocate is genuinely right about one thing — the prelude-duplication is a real
wart — and wrong that Functions are the right way to fix it for chronicle.

> **Recorded as
> [ADR-0001](../../../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md).**
> No code change is *required* by this decision; the only required follow-up is the
> documentation corrections below. The build-time include, `NewScriptServerSHA`,
> and pipeline guard are optional, behavior-preserving hygiene.

## Why (the short version)

The decision separates cleanly into "what chronicle's correctness rests on" vs
"what a migration would actually change," and the verified research is unanimous
on the first:

- On **atomicity** and **effects-based replication** — the two axes chronicle's
  linearizable mutations depend on — Functions and `EVAL` Lua are documented as
  **identical**. chronicle's idempotent-producer epoch/seq fencing
  (`validate_producer`) and cross-slot fork bookkeeping derive entirely from
  *atomicity + the `{path}`/`{__ds}` single-slot hash-tag discipline*, which both
  surfaces share. **Migrating buys exactly zero correctness.** Both advocates
  concede this; the pro-Functions advocate states plainly there is *"NO
  correctness, atomicity, or replication-safety benefit."*
- `EVAL` is **not deprecated** (command page: "Available since 2.6.0", no
  deprecation marker). So there is no "must-migrate" pressure — it's a pure
  engineering trade-off.

With correctness off the table, the case *for* Functions reduces to four
packaging/ergonomics wins, each weak for chronicle or counterbalanced:

1. **Code-sharing** — real (the prelude *is* the workaround Redis names), but
   fixable **without** Functions via a build-time include (what BullMQ does).
2. **Durability/persistence** — real, but only matters because of the volatile
   cache, which `Script.Run` already self-heals (one extra round-trip after
   failover, zero runbook).
3. **Atomic library replacement / no SHA-churn** — real, but a one-time,
   self-healing post-deploy cost, not an outage.
4. **Introspection** (`FUNCTION LIST/STATS/DUMP`) — a minor nicety chronicle has
   never needed.

Against those sit **three concrete costs**, all decision-relevant:

- **A new liveness obligation.** go-redis has no `FCall` auto-reload analogous to
  `Script.Run`; a fresh/flushed/failed-over instance returns `Library not loaded`,
  so chronicle would hand-roll startup `FUNCTION LOAD REPLACE` + a
  catch-load-retry shim + per-master cluster loading — re-implementing what
  `Script.Run` gives for free, and trading away chronicle's self-healing property.
- **Portability.** Across every platform surveyed, exactly one asymmetry exists:
  **AWS ElastiCache Serverless (and MemoryDB Multi-Region) block the
  `function`/`fcall` family while permitting `EVAL`** — and *no* platform does the
  reverse. `EVAL` is a strict superset of deployment targets; the README pitches
  "managed Redis generally." (Your *specific* enterprise target supports
  Functions — see "Conditions" — but the general pitch does not.)
- **A pipeline hazard** to respect either way: `Script.Run`'s `NOSCRIPT` fallback
  doesn't fire inside a pipeline/`MULTI` (go-redis #3228).

And the **prior art** is the clincher: BullMQ, Sidekiq, go-redsync, Stripe, and
Celery all use `EVAL`/`EVALSHA` for exactly this; effectively none migrated to
Functions. chronicle's approach is mainstream best practice, not a smell.

## Decision matrix

| Option | Portability | Reliability | Maintainability | Performance | Effort | Verdict |
|---|---|---|---|---|---|---|
| **(a)** Stay on Lua, unchanged | Best — `EVAL` everywhere incl. Serverless | Best — `Script.Run` self-heals, zero runbook | Weakest realistic — `common.lua` edit re-SHAs all 7/14; ~58% redundant cached source; 3 false repo-doc statements | Steady-state single-RT `EVALSHA`; atomicity identical to Functions | Zero | Safe but leaves cheap-to-fix debt + wrong docs |
| **(b)** Stay on Lua, **fix the pattern** | Best — unchanged | Best — unchanged; `NewScriptServerSHA` adds eviction-exemption | **Strong** — one source of truth via build-time include; docs corrected | Identical | **Low** | ✅ **RECOMMENDED core** |
| **(c)** Migrate to Functions | Worse — supported on your enterprise target but loses Serverless/MemoryDB-MR; dual path re-adds duplication | Worse — no `FCall` auto-reload; startup-load + shim + per-master loading | Best on packaging — one prelude/library, atomic `LOAD REPLACE`, introspection | Identical; no perf gain | Medium — rewrite 21 call sites + new bootstrap/lifecycle | ❌ REJECTED — zero correctness gain, net reliability regression, portability loss |
| **(d)** Hybrid/conditional (= b now, revisit c only if triggers flip) | Best now | Best now | Strong now; full dedup deferred | Identical | Low now | ✅ **RECOMMENDED framing** |

## Concrete action plan (option b)

In priority order. None of these is a migration; all are low-risk.

1. **Correct the three false statements in the repo docs** (do this first, it's
   free and they actively mislead):
   - **`NOSCRIPT → SCRIPT LOAD → retry`** (`docs/research/05-redis-design.md:223,
     460`; `docs/PLAN.md`) → the default `NewScript().Run()` path is
     **`NOSCRIPT → full-source EVAL → retry`**; go-redis never issues
     `SCRIPT LOAD` for `NewScript`.
   - **`nowMs`** (`05-redis-design.md:379`) → the scripts pass and compute in
     **nanoseconds** (`nowNs` / `UnixNano`).
   - **The determinism justification** → under effects replication, `TIME` is
     *legal* inside scripts, so "no `TIME` dependency for determinism" is stale.
     The real, still-valid reason for `nowNs`-as-`ARGV` is **single-clock
     consistency** between Go's framing/expiry math and Lua's TTL math.
   - **Re-scope the Functions-rejection rationale** (`05-redis-design.md:379`)
     from "more often ACL-blocked" to the precise truth: *blocked on ElastiCache
     Serverless and MemoryDB Multi-Region, and absent on pre-7.0 engines;
     supported on every 7.0+ node-based/enterprise platform including the
     operator's Redis Enterprise — we stay on `EVAL` for maximal portability and
     self-healing failover, not because Functions are unavailable.*

2. **Replace runtime `common.lua` concatenation with a build-time include.** A
   `go:generate` step (or BullMQ-style `--- @include` directive) that inlines the
   prelude once at build time gives `common.lua` a **single source of truth**
   while each shipped script still carries it. This removes the maintainability
   hazard (mass SHA churn on a prelude edit becomes a deliberate, visible build
   artifact) without touching invocation semantics or portability. It captures
   the **dedup half** of what Functions offered.

3. **Optional, low-effort: switch `loadScript` to `redis.NewScriptServerSHA`**
   (available since v9.19.0; chronicle is on v9.20.0) so steady-state scripts are
   **`SCRIPT LOAD`-class and exempt** from the Redis 7.4 `EVAL` LRU-eviction
   bucket (cap ~500). Caveats: it adds a `SCRIPT LOAD` round-trip at first use,
   and `NOSCRIPT` recovery still falls back to `EVAL`. Treat this as
   **shared-tenant hygiene**, not a necessity — chronicle's fixed 21-script
   catalog is far under the 500 cap, so eviction only bites if a co-tenant abuses
   `EVAL` on the same instance.

4. **Add a guard/comment** that these scripts must never be wrapped in a
   pipeline/`MULTI` (go-redis #3228), so a future batching optimization doesn't
   reintroduce a `NOSCRIPT` failure.

## Risks of this recommendation

- Doing **nothing** (option a) leaves three demonstrably false statements in the
  repo docs and the SHA-churn footgun — self-healing but easy to misdiagnose as
  an incident.
- Staying on Lua forgoes Functions' AOF-persistence and `FUNCTION
  LIST/STATS/DUMP` introspection — acceptable today (`Script.Run` self-heals),
  but a gap if chronicle later needs script-level audit/backup.
- `NewScriptServerSHA` changes first-call behavior; adopt deliberately and test
  against the managed endpoint (some tiers no-op `SCRIPT FLUSH` and manage the
  cache themselves).
- Continuing on `Script.Run` means never wrapping these scripts in a
  pipeline/`MULTI` — make the guard explicit.

## Conditions that would flip the answer toward Functions (option c)

Reconsider migrating only if **all** hold:

1. chronicle commits to a **single managed-enterprise target** (Redis Cloud /
   Redis Enterprise / Azure Managed Redis) and **explicitly drops ElastiCache
   Serverless, MemoryDB Multi-Region, and pre-7.0 engines** from its supported
   matrix and README — removing `EVAL`'s only portability advantage; **and**
2. go-redis ships a first-class `Function` type with an **`FCall`
   auto-reload-on-missing-library** wrapper (eliminating the hand-rolled
   bootstrap/retry and the self-healing regression), **or** chronicle accepts
   owning a robust startup-load + catch-load-retry + per-master cluster runbook;
   **and**
3. a concrete need emerges that Functions **uniquely** serve — script-level
   introspection/audit/backup (`FUNCTION DUMP`/`LIST WITHCODE`), server-side
   durability the volatile cache can't meet, or a multi-library deployment-
   versioning story whose pain exceeds the build-time-include fix.

Separately, revisit the **eviction-exemption** sub-decision (`NewScriptServerSHA`)
sooner if chronicle is co-tenanted on a shared managed instance where another
workload pushes the `EVAL` cache toward the 500-script cap.

Absent those, **stay on Lua.**
