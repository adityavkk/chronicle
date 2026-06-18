# Supplementary findings (srch web/code journey)

Collected 2026-06-10 with the `srch` SDK (web + code domains). These complement the
deep-dive docs 01–07.

## Redis capabilities that shape the design

- **`HEXPIRE` / hash-field TTL (Redis ≥ 7.4, native in 8.x)** — per-field expiry on
  hashes. A natural fit for **producer idempotency records**: each producer's
  `(epoch, lastSeq)` entry can age out independently of the stream itself.
  See [HEXPIRE docs](https://redis.io/docs/latest/commands/hexpire/) and the
  [architecture & benchmarks post](https://redis.io/blog/hash-field-expiration-architecture-and-benchmarks/).
  Caveat: field TTLs are lost on `HSET` overwrite semantics in some paths — verify
  behavior in integration tests before relying on it.
- **512 MB per-string hard cap** and `proto-max-bulk-len` (default **512 MB** server
  side, but managed providers often lower it) bound both `APPEND` payloads and
  `GETRANGE` responses. Multi-GB streams therefore require **chunked storage**
  regardless of the data model chosen. See
  [redis/redis#7354](https://github.com/redis/redis/issues/7354) and the
  [APPEND docs](https://redis.io/docs/latest/commands/append/).

## Caddy plugin public surface (pkg.go.dev)

The published module `github.com/durable-streams/durable-streams/packages/caddy-plugin`
exposes `Handler` with config fields chronicle should mirror in its own config:

```go
type Handler struct {
    DataDir              string         `json:"data_dir,omitempty"`
    MaxFileHandles       int            `json:"max_file_handles,omitempty"`
    LongPollTimeout      caddy.Duration `json:"long_poll_timeout,omitempty"`
    SSEReconnectInterval caddy.Duration `json:"sse_reconnect_interval,omitempty"`
    // ...
}
```

Deployment guidance ([durablestreams.com/deployment](https://durablestreams.com/deployment))
mounts the handler at `route /v1/stream/*` with `long_poll_timeout 30s`,
`sse_reconnect_interval 120s` as the documented example defaults.

## Go Redis client

Two maintained candidates (both under the `redis` GitHub org as of mid-2026):

- `github.com/redis/go-redis/v9` — canonical, broadest adoption.
- `github.com/redis/rueidis` (v1.0.75+) — RESP3-first, auto-pipelining, faster in
  [benchmarks](https://github.com/rueian/rueidis-benchmark); latency tradeoffs under
  low concurrency discussed in [rueidis#609](https://github.com/redis/rueidis/discussions/609)
  and [rueidis#626](https://github.com/redis/rueidis/issues/626).

Decision recorded in [PLAN.md](../PLAN.md) (see tradeoffs in
[05-redis-design.md](05-redis-design.md)).

## Ecosystem signals

- Electric launched **hosted Durable Streams** (Jan 2026:
  [announcement](https://electric.ax/blog/2026/01/22/announcing-hosted-durable-streams)) —
  the protocol is the contract; independent backends are expected and welcomed.
- The 0.1.0 release emphasizes **strict message-order preservation across all read
  modes** (catch-up, long-poll, SSE) — a key conformance axis for a Redis backend
  where reads and notifications race ([0.1.0 post](https://electric-sql.com/blog/2025/12/23/durable-streams-0.1.0)).
