# Triage — Issue #6: build-time `common.lua` include

> **Decision: CLOSE — not worth it.** Confidence: high. No code change.
> Tracks [#6](https://github.com/adityavkk/chronicle/issues/6); follows
> [ADR-0001](../../../../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md).

## Proposal

Replace the runtime `common.lua` concatenation in `loadScript()`
(`store/redis/scripts.go:26`, `webhook/scripts.go:25`) with a build-time
include / codegen step (e.g. `go:generate`, BullMQ-style `--- @include`) so the
prelude has a single source of truth.

## Why it isn't worth it

1. **It doesn't solve the stated problem.** Build-time inlining produces
   **byte-identical** assembled scripts — both `loadScript` impls already do
   `redis.NewScript(prelude + "\n" + body)`. So the duplicate prelude in the
   Redis script cache and the one-time re-SHA after a `common.lua` edit **both
   remain unchanged**. Codegen only moves *where* concatenation happens (compile
   time vs a 3-line runtime function run once at init).

2. **The runtime approach already gives the only real benefit.** `common.lua`
   lives in exactly one physical file per subsystem today, `go:embed` compiles it
   into the binary, and `loadScript` prepends it deterministically. That **is** a
   single source of truth — no generated artifacts needed.

3. **The cited downsides are within documented-acceptable bounds.**
   - The re-SHA on a prelude edit is the **self-healing `NOSCRIPT → full-source
     EVAL` reload ADR-0001 specifically chose to keep**; it fires once per node
     after deploy and is invisible to callers.
   - The duplication is ~**52 KB** (store prelude 4451 B × 7 + webhook prelude
     1575 B × 14 = 53 207 B; ~80 KB total assembled across 21 scripts) — trivially
     under Redis's own guidance that *"even large applications that use hundreds of
     cached scripts shouldn't be an issue"* and far under the ~500-script Redis 7.4
     `EVAL` eviction cap.

4. **Codegen would *add* net developer friction:** generated `.lua` files in the
   tree, a `go:generate` step, and a CI drift/sync check (a brand-new failure mode
   if someone edits a generated file or forgets to regenerate) — all to ship
   identical bytes to Redis.

## Prior art (the mainstream pattern is runtime prepend)

- **BullMQ `@include`** — the model the issue cites — is a **runtime**
  `ScriptLoader.interpolate()` that assembles includes in-memory at connection
  time, *not* committed generated files. It needs a parser only because it has a
  deep multi-level include graph; chronicle has a single flat prelude.
  ([api.docs.bullmq.io], [bullmq#897])
- **redis-rb-scripting** implements chronicle's pattern as a first-class feature:
  an `includes/` dir prepended to every script at runtime, with EVALSHA-else-EVAL
  fallback — described as *"a primitive form of extracting library code."*
  ([redis-rb-scripting])
- **Redis docs**: hundreds of cached scripts is fine; the only real concern is
  *runtime-generated* script variation, which chronicle never does (21 fixed
  scripts assembled once at init). ([eval-intro])
- **redis-lua (chanks)**: stores `.lua` in VCS and relies on EVALSHA→load→EVALSHA
  self-healing rather than precompiling — the same property ADR-0001 kept.

So chronicle is **already aligned** with prior art.

## Notes (from adversarial review)

- The two preludes (`store/redis/scripts/common.lua`,
  `webhook/scripts/common.lua`) are **intentionally disjoint** — zero shared
  helpers (store: `meta_map`/`is_expired`/`validate_producer`/…; webhook:
  `offset_greater`/`split_link`/`fenced`). There is no DRY win in unifying them
  either; closing forfeits nothing.
- The one technically-real adjacent gap — `redis.NewScript` only SHA1s its input
  and never parses Lua, so a malformed prelude surfaces at runtime, not at build —
  is **already covered** by existing integration tests that exercise every
  assembled script against Redis (`store/redis/integration_test.go`,
  `differential_test.go`, `webhook/redis_store_test.go`). No new tooling warranted.

## Outcome

Keep the runtime concatenation as-is. If desired, a single one-line code comment
above `loadScript` could note that editing `common.lua` re-SHAs the group on next
deploy and self-heals via the `NOSCRIPT → EVAL` fallback — but that needs no issue
and no build machinery.

[api.docs.bullmq.io]: https://api.docs.bullmq.io/classes/v1.ScriptLoader.html
[bullmq#897]: https://github.com/taskforcesh/bullmq/pull/897
[redis-rb-scripting]: https://github.com/codekitchen/redis-rb-scripting
[eval-intro]: https://redis.io/docs/latest/develop/programmability/eval-intro/
