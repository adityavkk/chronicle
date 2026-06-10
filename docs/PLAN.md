# Chronicle implementation plan

Chronicle is a Go server implementing the [Durable Streams protocol](spec/PROTOCOL.md)
on Redis 8. It mirrors the official Caddy plugin implementation closely enough to
evolve with that codebase, while swapping the storage layer for Redis.

This plan records the architecture, the Redis data model, the tradeoffs behind each
decision, and the route to passing the full server conformance suite. Research inputs
live in [docs/research/](research/).

## 1. Goal and success criteria

1. **100% pass rate** on `@durable-streams/server-conformance-tests` (CLI mode,
   ~330 tests) against chronicle backed by a live local Redis 8. This includes the
   unconditional groups: core protocol, SSE, JSON mode, property-based tests,
   **idempotent producers**, stream closure, and forks.
2. The **subscriptions group** (reserved `__ds` APIs, webhooks, pull-wake) is the
   suite's only optional group — the CLI cannot even enable it, and the Caddy
   plugin's own conformance gate runs without it. Chronicle defers it; the design
   leaves room (route dispatch reserves the `__ds` prefix).
3. Code quality: strongly typed, functional-core/imperative-shell, exact Caddy
   naming parity where the concepts coincide (see §3), golangci-lint clean,
   gofumpt formatted, well documented for humans.

## 2. Architecture: pure core, imperative edge

```
cmd/chronicle/          main: flags/env → config → wire deps → serve, graceful shutdown
  │
  ├── chronicle (root package)
  │     handler.go      Handler: ServeHTTP dispatch + handleCreate/handleAppend/
  │                     handleRead/handleHead/handleDelete/handleSSE (Caddy names)
  │     config.go       Config: LongPollTimeout, SSEReconnectInterval, ... (Caddy knobs)
  │
  ├── protocol/         PURE. No I/O, no clocks (clock passed in), no Redis.
  │     headers.go      protocol header constants (verbatim from Caddy handler.go)
  │     parse.go        header/query parsing → typed requests (TTL grammar, producer
  │                     header triple, offset params, fork headers)
  │     producer.go     the producer-validation state machine as a pure function:
  │                     decide(state, epoch, seq) → (Accept|Duplicate|StaleEpoch|
  │                     SeqGap|InvalidEpochSeq, newState)
  │     cursor.go       interval cursor generation + monotonic advancement (clock arg)
  │     json.go         JSON-mode validation + one-level array flattening
  │     sse.go          SSE event framing (data/control events, base64, CRLF-injection
  │                     safe line splitting) → []byte, pure
  │
  └── store/            the storage contract, mirroring Caddy's store package verbatim
        store.go        Store interface, Offset, Message, StreamMetadata, CreateOptions,
                        AppendOptions, AppendResult, error variables — same names,
                        same signatures as caddy-plugin/store/store.go
        offset.go       Offset type: "%016d_%016d" ReadSeq_ByteOffset (copied semantics)
        redis/          the Redis 8 implementation
          store.go      RedisStore: satisfies store.Store
          scripts.go    embedded Lua scripts (append, create, close, delete, fork)
          keys.go       key schema ({path} hash-tagged)
          notify.go     pub/sub waiter machinery for WaitForMessages
```

**Pure core.** Everything decidable without I/O lives in `protocol/` and the
non-I/O parts of `store/` (offset math, metadata config-matching). These are plain
functions over immutable inputs — trivially unit-testable, no mocks. The producer
state machine in particular is spec section 5.2.1 transcribed into one pure
function, used both by the Lua script (mirrored logic) and by unit tests as the
oracle.

**Imperative edge.** `Handler` methods do HTTP parsing/writing and call the store;
`store/redis` does Redis I/O. Both stay thin: decide-then-do, with decisions
delegated to the pure core.

## 3. Caddy parity contract

Chronicle reuses, verbatim where Go allows:

- **Store layer**: the full `store.Store` interface and supporting types
  (`Offset`, `ParseOffset`, `Message`, `StreamMetadata`, `CreateOptions`,
  `AppendOptions`, `AppendResult`, `CloseResult`, `CloseProducerResult`,
  `ProducerState`, `ProducerResult`, and every `Err*` variable). A new backend
  upstream lands as a new file in their `store/`; chronicle tracks the same shape
  in ours.
- **Handler layer**: `Handler` with `handleCreate`, `handleAppend`, `handleRead`,
  `handleHead`, `handleDelete`, `handleSSE`, `formatResponse`, `writeError`,
  `newHTTPError`, plus the protocol header constants. The one deliberate
  difference: chronicle's `ServeHTTP(w, r)` is stdlib `http.Handler` (Caddy's
  takes a `next caddyhttp.Handler` middleware argument).
- **Config knobs**: `LongPollTimeout`, `SSEReconnectInterval` with Caddy's
  defaults; `DataDir`/`MaxFileHandles` are replaced by Redis connection options.

Upstream tracking: `docs/spec/README.md` pins the vendored spec commit
(`82f9963`). When upstream moves, diff `caddy-plugin/{handler.go,store/}` against
the parity table in `docs/CADDY-PARITY.md` and port behavioral changes.

## 4. Redis data model

### 4.1 What storage must support

From the spec + conformance inventory (research docs 01/04):

- Atomic append: validate (closed, content-type, `Stream-Seq` lexicographic
  regression, producer epoch/seq) **and** write **and** bump tail **and** notify,
  all-or-nothing, serialized per stream.
- Message-granular history: `Read(path, offset)` returns the messages with
  end-offset > requested offset, preserving boundaries (JSON mode renders them as
  a JSON array; fork sub-offsets slice the first following message).
- Live tailing: long-poll/SSE waiters woken promptly (conformance appends 500 ms
  after opening the poll and expects delivery well inside a 5 s test timeout).
- Sliding TTL (renewed by GET/POST, not HEAD) and absolute expiry, observable
  within ~1–2 s of nominal (lazy expiry on access is acceptable — the suite polls).
- Producer state per `(stream, producerId)`; survives as long as the stream.
- Forks: refcounts, soft-delete, cascade GC, offset-space sharing with source.
- Read-your-writes: a GET issued after an append's 2xx must see the data.

### 4.2 Candidate models considered

| Model | Append | Read by offset | Boundaries | Verdict |
| --- | --- | --- | --- | --- |
| (a) STRING + `APPEND`, read `GETRANGE` | O(1) | exact byte ranges | lost — needs a separate boundary index anyway | rejected: 512 MB/key cap forces chunking, JSON mode needs boundaries regardless |
| (b) Redis Streams `XADD`/`XRANGE` | O(1) | by stream ID, not byte offset — needs ID↔byte-offset index | kept | rejected: double bookkeeping, IDs leak server-assigned time semantics, trimming semantics we don't want |
| (c) LIST of frames + ZSET index byte-offset→list-index | O(1) ×2 | ZSET lookup + `LRANGE` | kept | workable but two structures to keep consistent per message |
| (d) **ZSET, member = `"%016d_%016d" + "|" + data`, score 0, read via `ZRANGEBYLEX`** | one `ZADD` | one `ZRANGEBYLEX (prefix +inf` | kept | **chosen** |

Model (d) exploits the protocol's own offset design: offsets are zero-padded,
fixed-width, lexicographically sortable strings, so prefixing each frame with its
end-offset makes byte-wise lex order equal stream order, and "messages with
end-offset > X" is exactly `ZRANGEBYLEX key (X| +`-style open interval. One
sorted set per stream, one command per read, boundaries intact, members unique by
construction (offset prefix). The 16+1+16+1-byte prefix overhead per message is
noise, and the conformance suite's 10 MB single-append fits comfortably under
both the 512 MB member cap and default `proto-max-bulk-len`.

Known limit (documented, acceptable): a whole stream lives in one ZSET on one
shard, so stream size is bounded by node memory — same class of limit as the
Caddy memory store, and Walmart-managed Redis deployments size for it. Catch-up
reads of huge streams return everything from the requested offset today; if that
becomes a problem, a `maxReadChunkSize` (spec-permitted partial reads, the suite
already follows `Stream-Next-Offset` pagination) plus `ZRANGEBYLEX ... LIMIT` is
a drop-in optimization.

### 4.3 Key schema

All keys for one stream share the `{...}` hash tag so they land in one cluster
slot, making multi-key Lua scripts cluster-safe:

| Key | Type | Contents |
| --- | --- | --- |
| `ds:{<path>}:meta` | HASH | contentType, currentOffset (readSeq, byteOffset), closed, lastSeq, ttlSeconds, expiresAt, createdAt, lastAccessedAt, forkedFrom, forkOffset, forkOffsetRequested, forkSubOffset, refCount, softDeleted, closedBy (producer tuple) |
| `ds:{<path>}:msg` | ZSET | frames `"<offset>|<data>"`, score 0 |
| `ds:{<path>}:prod` | HASH | producerId → `epoch:lastSeq:lastUpdated` |
| `ds:notify:{<path>}` | pub/sub channel | `"a"` on append, `"c"` on close, `"d"` on delete |

Path is the URL path, used raw inside the hash tag (binary-safe; Redis keys are
binary; `{`/`}` in user paths are escaped).

### 4.4 Atomicity: Lua scripts

Every mutation is one `EVALSHA` operating only on `{<path>}`-tagged keys —
validation, write, metadata update, and `PUBLISH` execute atomically and
serialized per stream (Redis is single-threaded per shard), which directly
satisfies the spec's "serialize validation + append per (stream, producerId)"
MUST and its atomic producer-state+append SHOULD:

- `create.lua` — existence/config-match check (idempotent PUT), fork validation
  (source meta read, refcount incr), initial data write, closed-state creation.
- `append.lua` — full validation chain in spec precedence order (closed →
  content-type → producer epoch/seq → Stream-Seq), JSON-mode frames passed in
  pre-flattened by Go (JSON parsing stays in Go; Lua receives the frame list),
  ZADD frames, meta update, PUBLISH.
- `close.lua` — close-only path incl. producer-tuple dedup (`closedBy`).
- `read_meta_touch.lua` — metadata fetch + sliding-TTL touch (GET path).
- `delete.lua` — refcount-aware delete/soft-delete + cascade GC walk.

Tradeoff vs `MULTI/EXEC`+`WATCH`: optimistic transactions would need
read-modify-write retry loops for producer validation and give weaker
serialization under contention; Lua gives single-round-trip linearizable
mutations. Cost: logic duplicated between Go (pure-core oracle, used in tests)
and Lua (execution) — mitigated by table-driven tests asserting the two agree,
and by keeping Lua scripts small and heavily commented. Redis Functions were
considered (nicer packaging) but `FUNCTION LOAD` is more often restricted on
managed Redis than `EVALSHA`/`SCRIPT LOAD`; chronicle uses classic scripts with
automatic NOSCRIPT reload.

### 4.5 Live tailing: pub/sub with a re-check, plus poll fallback

`WaitForMessages` (same signature as Caddy's):

1. Fast path: closed-at-tail check, then a `Read` — if frames exist, return.
2. SUBSCRIBE `ds:notify:{<path>}`, then **re-Read before waiting** (an append
   between step 1 and the subscribe must not be missed — pub/sub is
   fire-and-forget).
3. Wait for: notification → re-Read; context/timeout → return `timedOut`;
   close notification → return `streamClosed`.
4. Defensive poll every 1 s while waiting (covers dropped pub/sub messages on
   connection churn; conformance tolerates this latency only as a fallback, the
   pub/sub path is the primary wake).

Keyspace notifications were rejected: they require `notify-keyspace-events`
server config that managed Redis frequently locks down, and they fire per-command
rather than per-protocol-event. Plain `PUBLISH` from the mutation scripts is
explicit and config-free.

The SSE loop mirrors Caddy's: read → emit `data` + `control` events → wait
(WaitForMessages with short timeout) → loop, closing on reconnect interval or
`streamClosed` per spec §5.8.

### 4.6 TTL and expiry

Mirrors the Caddy stores' **lazy expiry** (`IsExpired` checked on every access)
as the source of truth — `lastAccessedAt` is bumped by GET/POST (not HEAD) inside
the Lua scripts, satisfying the sliding-window MUSTs exactly. On top, a Redis key
TTL (`PEXPIRE`, TTL + 60 s slack, refreshed on access) acts as garbage collection
so idle expired streams don't leak memory. Exception: streams with `refCount > 0`
or `softDeleted` never carry key TTLs (fork readers need the data); cascade GC
deletes them explicitly.

`noeviction` is the required maxmemory policy (documented in deployment docs):
any LRU/random eviction would silently truncate streams.

### 4.7 Consistency and durability honesty

Redis replication is asynchronous: an acked append can be lost on failover.
Chronicle documents this (deployment docs) and offers `--append-wait-replicas N`
(issue `WAIT` after mutations) as an opt-in durability/latency tradeoff, default
off — same posture as the upstream file store (which fsyncs but is single-node).
The conformance suite never restarts the server; crash durability is a
white-box/deployment concern per IMPLEMENTATION_TESTING.md.

### 4.8 Go Redis client

**rueidis vs go-redis/v9** — final call deferred to research doc 05's
recommendation; requirements either way: `EVALSHA` script helper with NOSCRIPT
fallback, robust pub/sub resubscription, cluster support, context-first API.
(§9 decision log records the outcome.)

## 5. Conformance strategy

- `make conformance`: `docker compose up -d --wait redis` → build & start
  chronicle on `:4437` with `--long-poll-timeout 500ms` (matching the Caddy test
  harness) → `npx @durable-streams/server-conformance-tests --run
  http://localhost:4437` → teardown. Suite paths are hardcoded under
  `/v1/stream/*`; chronicle serves the protocol there (configurable root).
- Readiness probe mirrors the Caddy harness: PUT+DELETE `/v1/stream/__health__`.
- Dev loop: `make conformance-watch` (the CLI's `--watch` mode) and vitest
  `-t <pattern>` filtering for single-group iteration.
- Test DB hygiene: conformance namespace accumulates streams (suite never cleans
  up); `make redis-flush` flushes the dedicated DB between runs.

## 6. Testing pyramid

1. **Pure-core unit tests** (no Redis): offset parsing/format/compare, TTL
   grammar, producer state machine (table-driven, mirrors spec §5.2.1 pseudocode
   exactly), cursor monotonicity, JSON flattening, SSE framing incl. CRLF
   injection vectors.
2. **Store integration tests** (live Redis, `-run Integration`, skipped with
   `-short`): the behavioral contract Caddy's `store/*_test.go` files encode —
   create/append/read/close/delete/fork/producer flows, expiry timing, waiter
   wakeups, plus Lua-vs-Go producer-oracle agreement.
3. **Conformance e2e** (the gate): full suite vs live Redis.
4. **CI** (GitHub Actions, enterprise): lint → unit → integration (redis service
   container) → conformance.

## 7. Developer experience

- `make build run test lint fmt conformance redis-up redis-down` — one-word verbs.
- `docker-compose.yml` provides Redis 8 with AOF persistence and a healthcheck.
- `README.md`: quickstart (3 commands to a running server + first stream),
  architecture tour, conformance instructions.
- `docs/`: this plan, research corpus, deployment notes (managed Redis
  requirements: noeviction, Lua/EVALSHA allowed, pub/sub allowed, cluster slots
  via hash tags).
- Default port 4437 (spec §13.1, IANA-selected for Durable Streams).

## 8. Milestones

1. **M1 — skeleton serves**: config, Handler dispatch with CORS/security headers,
   store interface, Redis store create/head/delete; health probe passes.
2. **M2 — core protocol**: append/read catch-up, offsets, ETag/304, JSON mode,
   TTL; ~half the suite green.
3. **M3 — live modes**: long-poll + SSE + cursors; closure semantics end-to-end.
4. **M4 — producers**: idempotency Lua + handler paths; producer groups green.
5. **M5 — forks**: fork create/read/lifecycle/soft-delete/cascade GC; fork groups
   green.
6. **M6 — 100%**: property-based stragglers, browser-security headers on error
   paths, polish; full suite green in CI; docs complete.

Implementation order follows conformance-group dependency, and each milestone
lands as a PR-sized commit series on `main` (single-author repo; worktrees via
`wt` for any parallel agent work).

## 9. Decision log

| # | Decision | Why (short) | Revisit if |
| --- | --- | --- | --- |
| 1 | Defer subscriptions/webhooks group | only optional group; CLI can't enable; Caddy gate skips it | product need or suite makes it mandatory |
| 2 | ZSET-lex frame model (d) | one structure, one command per op, boundaries preserved | multi-GB streams per key become real |
| 3 | Lua scripts over MULTI/WATCH | linearizable single-RTT mutations, per-stream serialization for free | managed Redis bans EVALSHA |
| 4 | Pub/sub + re-check + slow poll | config-free, prompt wakeups, race-safe | none expected |
| 5 | Lazy expiry + key-TTL GC backstop | exact sliding-window semantics + no leak | active reaper needed for fork GC at scale |
| 6 | Client lib: see doc 05 | — | — |
| 7 | Stdlib `net/http` (Go 1.22+ mux), no framework | zero deps on the hot path, Caddy parity is at handler level not router level | — |
