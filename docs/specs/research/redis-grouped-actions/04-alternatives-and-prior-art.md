# 04 — Alternatives beyond Functions, and what the ecosystem actually does

Functions are the headline alternative, but they aren't the only one. This doc
clears the other options off the table and then grounds the whole question in
prior art — because "is this best practice?" is ultimately answered by what the
production ecosystem does for the *same* problem.

## The non-Lua alternatives are non-starters (and chronicle was right)

| Alternative | Verdict | Why |
|---|---|---|
| **`MULTI`/`EXEC` transactions** | ✗ cannot do it | Commands are queued and *all* replies return only at `EXEC` — you cannot read a value mid-transaction and branch on it. That is exactly what `append.lua`'s validation chain needs. Redis also does **not** roll back on a failed command. ([transactions]) |
| **`WATCH` / optimistic CAS** | ✗ wrong tool | Can read first, but only via an abort-and-retry loop, and Redis's own canonical guidance prefers Lua over `WATCH` for value-dependent multi-step logic: *"Lua scripts are more powerful … they can also read values … and are more efficient than 'WATCHed' transactions because they don't require optimistic locking."* ([no-rollbacks]) |
| **Pipelining** | ✗ not atomic | Only batches round-trips; *"pipelining can't help in this scenario since the client needs the reply of the read command before it can call the write command."* ([pipelining]) |
| **Native C/Rust modules** (RedisGears, redis-cell, …) | ✗ not portable | Not loadable on the major managed platforms (ElastiCache, MemoryDB direct you to Redis Cloud or self-hosting). Violates the managed-Redis requirement outright. |
| **Anything new in Redis 8** | ✗ no new primitive | Redis 8 added *no* server-side programmability primitive beyond `EVAL` scripts and Functions; its "One Redis" module merge is orthogonal to atomic grouped writes. (see [02](02-redis-functions.md)) |

So the realistic choice space is exactly two: **`EVAL` Lua** (status quo) vs
**Redis Functions** — both of which are atomic Lua on the single-threaded engine.
chronicle's documented rejection of `MULTI/EXEC` and `WATCH` is correct.

## Prior art: the whole ecosystem uses `EVAL`/`EVALSHA`, not Functions

This is the strongest sanity check on "best practice." Every widely-used
Redis-backed system that needs chronicle's atomic read-compute-write guarantees
uses `EVAL`/`EVALSHA`, and effectively **none** have migrated to Functions:

- **BullMQ** (dominant Node.js job queue): uses `EVAL`/`EVALSHA`, not Functions,
  for atomic multi-key state transitions — *and independently arrived at
  chronicle's exact engineering patterns*: a shared-include prelude **inlined at
  build time**, version-tagged script names, and hash tags for slot-safety.
  (BullMQ's build-time `--- @include` is the cleaner version of chronicle's
  runtime concatenation — see [05](05-recommendation.md).)
- **Sidekiq** (Ruby jobs): `EVAL`/`EVALSHA` (`LUA_ZPOPBYSCORE` etc.), not
  Functions. Its documented run-in with **Lua CJSON's 14-digit number limit**
  directly validates chronicle's choice to pass int64s as fixed-shape **string**
  arrays decoded in Go rather than relying on Lua number encoding.
- **go-redsync** (the canonical Go Redlock — directly relevant, same language and
  same `redis.NewScript` path chronicle uses): lock release/extend are `EVAL`
  compare-and-delete / compare-and-pexpire scripts. Not Functions.
- **Stripe**'s production rate limiters and the widely-copied four-rate-limiter
  gist: `EVAL` Lua, explicitly on managed Redis (ElastiCache). Not Functions.
- **Celery/Kombu**, **redis-rb**: `EVAL`/`EVALSHA`.

The honest framing (the research soft-pedals the absolutes): this is *absence of
a prominent migration*, not a formal census. But the trend is unmistakable — the
ecosystem treats Functions as a **deployment-convenience**, not a correctness or
best-practice upgrade. chronicle is in excellent, mainstream company.

## Two chronicle designs that prior art independently validates

1. **int64-as-fixed-shape-string-array** (`decodeScriptReply`) sidesteps the exact
   Lua CJSON bug that bit Sidekiq. Keep it.
2. **The prelude-include pattern itself** is industry-standard (BullMQ) — the only
   improvement is *when* you inline it (build time, not runtime). See
   [05](05-recommendation.md).

## One semantics caveat to respect regardless of the decision

`Script.Run`'s `NOSCRIPT` fallback **does not fire inside a pipeline / `MULTI`**
(go-redis [#3228], closed *not-planned*). chronicle's plain `Script.Run` usage is
in the safe path today; a future batching optimization that wraps these scripts in
a pipeline would silently reintroduce a `NOSCRIPT` failure. Add an explicit guard.

## On read-replica scaling (a tempting Functions "win" that isn't)

`no-writes` → `FCALL_RO`/`EVAL_RO` can run on read-only replicas. But chronicle
**cannot use it as written**, for an *architectural* reason, not a programmability
one: its only read-path script (`read.lua`) **writes** on every read (`HSET
accessedAtNs` + `refresh_backstop`'s `PEXPIRE`/`PERSIST` for sliding-TTL), and
`PUBLISH` is itself classified as a write inside scripts. Realizing replica read
scaling would require first splitting a genuinely side-effect-free read path — and
*then* it's achievable with `EVAL_RO` or a `#!lua flags=no-writes` shebang
**without** migrating to Functions. So this is not a reason to adopt Functions.

[transactions]: https://redis.io/docs/latest/develop/using-commands/transactions/
[no-rollbacks]: https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/
[pipelining]: https://redis.io/docs/latest/develop/using-commands/pipelining/
[#3228]: https://github.com/redis/go-redis/issues/3228
