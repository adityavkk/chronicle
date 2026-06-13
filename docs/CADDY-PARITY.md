# Caddy plugin parity map

Chronicle tracks the Durable Streams reference implementation
([`packages/caddy-plugin`](https://github.com/durable-streams/durable-streams/tree/main/packages/caddy-plugin)),
pinned at the commit recorded in [spec/README.md](spec/README.md). This table
is the porting contract: when upstream changes a file on the left, diff it and
port the behavioral change to the file on the right.

| Upstream (caddy-plugin) | Chronicle | Relationship |
| --- | --- | --- |
| `store/store.go` | `store/store.go` | **Verbatim** (frozen parity file) |
| `store/offset.go` | `store/offset.go` | **Verbatim** |
| `store/offset_test.go` | `store/offset_test.go` | **Verbatim** |
| `store/memory_store.go` | `store/memory_store.go` | Near-verbatim; `validateProducer` delegates to `store.ValidateProducer`, JSON helpers live in `store/json.go` |
| `store/memory_store.go` `validateProducer` | `store/producer.go` `ValidateProducer` | Lifted to a pure function (clock injected); **also mirrored in `store/redis/scripts/append.lua`** — change all three together |
| `store/memory_store.go` `processJSONAppend` | `store/json.go` `ProcessJSONAppend` | Exported port |
| `store/file_store.go`, `store/bbolt.go`, `store/segment.go`, `store/filepool.go` | `store/redis/` | **Re-implementation** of the same contract on Redis (ZSET-lex frames + Lua, see PLAN.md §4); not line-mapped |
| `handler.go` | `handler.go`, `handler_sse.go` | Faithful port: same method set/names, zap→slog, Caddy middleware signature → stdlib, webhook hooks replaced by a marked re-entry comment |
| `handler.go` header constants | `protocol/headers.go` | Verbatim values |
| `handler.go` `parseTTL`/`parseSubOffset`/`isValidIntegerString` | `protocol/parse.go` | Exported ports |
| `handler.go` cursor functions | `protocol/cursor.go` | Pure ports (clock as argument) |
| `module.go` (Caddyfile config) | `config.go` + `cmd/chronicle/main.go` | Same knobs and defaults (`long_poll_timeout` 30s, `sse_reconnect_interval` 60s); `data_dir`/`max_file_handles` → Redis options |
| `cmd/caddy/main.go` (dev mode) | `cmd/chronicle/main.go` | Equivalent standalone server |
| `webhook/` | `webhook/` | **Re-implementation on Redis** of the reserved `__ds` subscription APIs. The pinned Caddy checkout predates PROTOCOL §6–7 (it is consumer-centric, query-param, webhook-only, `epoch`); chronicle targets the protocol and the `0.3.5` conformance suite both implementations converge on. Keeps Caddy's engine structure (`Subscription`, `Manager`, `Routes`, the idle/waking/live state machine, the lifecycle method names); the wire and records use the protocol nouns. See docs/research/07 and 09. |
| `test/conformance.test.ts` | `test/conformance/conformance.test.ts` + `scripts/conformance.sh` | Same harness shape: 500 ms long-poll timeout, health-stream readiness probe |

## Update procedure

1. `git -C ../durable-streams pull`, note the new commit.
2. `git -C ../durable-streams diff <pinned>..HEAD -- PROTOCOL.md packages/caddy-plugin packages/server-conformance-tests`
3. Port changes per the table. For producer-semantics changes, update
   `store/producer.go`, `store/memory_store.go`, **and**
   `store/redis/scripts/append.lua` + `close.lua`; the differential table in
   `store/redis` integration tests will catch drift.
4. Bump `@durable-streams/server-conformance-tests` in
   `test/conformance/package.json`, run `make conformance`.
5. Update the pin in `docs/spec/README.md` (re-copy PROTOCOL.md +
   IMPLEMENTATION_TESTING.md) and this file if mappings moved.

## Known deliberate deviations

- `ServeHTTP(w, r)` (stdlib) instead of Caddy's middleware signature.
- `mount.go`: chronicle strips the configured stream root before handing
  paths to the store and translates `Stream-Forked-From`/`Location`
  accordingly (Caddy mounts at `/` in its harness, so upstream never needed
  this).
- Expired streams with active forks flip to soft-deleted rather than being
  hard-deleted on `Create` (protects fork readers; documented in the redis
  store tests).
- Logging is `log/slog`, not zap.
- `webhook/`: the subscription fencing counter is named `generation` (the
  PROTOCOL §7 wire noun), not the Caddy package's `epoch`. The whole control
  plane is persisted to Redis under one `{__ds}` hash tag rather than held in an
  in-memory map, which is the durability gap chronicle exists to close (the
  Caddy engine loses every cursor/lease/generation on restart). Wake fan-out is
  not folded into `append.lua` (it would cross hash-tag slots); the append-time
  hook is best-effort and the recovery sweep is the durability backstop.
