# Triage — Issue #7: adopt `redis.NewScriptServerSHA`

> **Decision: CLOSE — not worth it.** Confidence: high. No code change.
> Tracks [#7](https://github.com/adityavkk/chronicle/issues/7); follows
> [ADR-0001](../../../../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md).

## Proposal

Switch `loadScript` from `redis.NewScript` to `redis.NewScriptServerSHA` so
scripts are loaded via `SCRIPT LOAD` and thus **exempt** from Redis 7.4's `EVAL`
LRU-eviction (hard cap ~500), which `NewScript` (client-SHA, EVAL-loaded) is
subject to.

## Why it isn't worth it — three independent reasons

1. **Margin: chronicle has 21 scripts vs a ~500-script cap (25× headroom).** It
   cannot self-evict. The cap is per-instance script *identity*; even many
   chronicle tenants/namespaces against one instance share the same 21 SHAs, so
   the margin holds.

2. **Threat model doesn't apply.** Eviction only bites if a **co-tenant EVAL
   abuser** pushes >500 distinct scripts on a **shared** instance — which
   contradicts both chronicle's target (single-tenant managed *enterprise* Redis)
   and its documented **`noeviction`** deployment contract
   (`DEPLOYMENT.md`, `research/05-redis-design.md`).

3. **The mechanism is leaky — it doesn't even deliver the protection.** Verified
   against **go-redis v9.20.0** `script.go`: `NewScriptServerSHA`'s `SCRIPT LOAD`
   exemption is **non-durable**. On a `NOSCRIPT` after a cache flush/failover,
   `EvalSha` calls `ensureHash()`, which **fast-paths** (hash already cached) and
   does **not** re-`SCRIPT LOAD`; the retry `EVALSHA` fails `NOSCRIPT` again, and
   `Run()` falls back to **full-source `EVAL`**, re-caching the script as an
   **evictable `EVAL`-loaded** entry that is never re-`SCRIPT LOAD`-ed until
   process restart. So the exemption lapses after the **first** failover — exactly
   the window an operator would care about.

## Cost of adopting it

A `SCRIPT LOAD` round-trip per script at first use, plus a behavior change away
from the ecosystem-standard self-healing `Script.Run` (`EVALSHA` → `EVAL`
fallback) — in exchange for protection that is both **unneeded** (21 ≪ 500) and
**illusory** (lapses after the first flush). Net: high conceptual overhead, ~zero
value.

## The real concern lives in #8, not here

The *one* place server-side `SCRIPT LOAD` genuinely helps is **scripts run inside
a pipeline/`MULTI`**, where `Script.Run`'s `NOSCRIPT → EVAL` self-heal cannot fire
(go-redis [#3228]). chronicle runs **zero** scripts inside any pipeline today
(every `Pipeline`/`TxPipeline` site contains only `HGet`/`HGetAll`), so this does
not bite — and the guard against a *future* regression is tracked by
[#8](https://github.com/adityavkk/chronicle/issues/8), where it belongs.

## Prior art

Mature OSS (BullMQ/ioredis, Sidekiq/redis-rb, redis-py) all use the plain
`EVALSHA` → `NOSCRIPT` → reload pattern at scale; they do **not** pre-`SCRIPT
LOAD` for eviction protection. chronicle is already on that mainstream path.

## If the scenario ever becomes real

If a future deployment genuinely co-tenants chronicle on a shared instance with
`EVAL` abusers, the correct fix is **operator-side** — a dedicated / `noeviction`
instance and monitoring `INFO evicted_scripts` — not a leaky client constructor.
(Re-evaluate only if go-redis also fixes the `ensureHash` fast-path so
`NewScriptServerSHA` re-`SCRIPT LOAD`s on `NOSCRIPT`; even then reasons 1–2 stand.)

[#3228]: https://github.com/redis/go-redis/issues/3228
