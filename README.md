# Chronicle

A [Durable Streams](https://github.com/durable-streams/durable-streams) protocol
server backed by **Redis 8**, written in Go.

**Docs:** [adityavkk.github.io/chronicle](https://adityavkk.github.io/chronicle/)

Durable Streams gives you URL-addressable, append-only byte streams over plain
HTTP: create a stream with `PUT`, append with `POST`, read with `GET` — including
catch-up reads from any offset, long-polling, and SSE live tailing — with
explicit closure (EOF), TTL expiry, stream forking, and Kafka-style idempotent
producers. Chronicle implements the full protocol, validated by the official
server conformance suite, and stores everything in Redis.

Chronicle deliberately mirrors the reference [Caddy plugin
implementation](https://github.com/durable-streams/durable-streams/tree/main/packages/caddy-plugin):
the storage contract (`store.Store`), type names, handler structure, and
behavioral details are kept in lockstep so chronicle can evolve alongside
upstream. Where the Caddy plugin persists to memory or files, chronicle
persists to Redis — making it a fit for teams who already operate managed Redis
and want durable streams without new infrastructure.

## Quickstart

Requirements: Go ≥ 1.26, Docker (for Redis).

```bash
make redis-up      # start Redis 8 (docker compose, AOF persistence)
make run           # build + start chronicle on :4437
```

Then, in another terminal:

```bash
# Create a stream
curl -i -X PUT http://localhost:4437/v1/stream/demo -H 'Content-Type: application/json'

# Append a couple of messages (one request = one batch)
curl -i -X POST http://localhost:4437/v1/stream/demo \
  -H 'Content-Type: application/json' -d '[{"hello":"world"},{"n":2}]'

# Read from the beginning
curl -i 'http://localhost:4437/v1/stream/demo?offset=-1'

# Live-tail from the current position (long-poll)
curl -i 'http://localhost:4437/v1/stream/demo?offset=now&live=long-poll'

# Close the stream (EOF for all readers)
curl -i -X POST http://localhost:4437/v1/stream/demo -H 'Stream-Closed: true'
```

Every response carries `Stream-Next-Offset` — pass it as `?offset=` to resume
exactly where you left off. See the [protocol spec](docs/spec/PROTOCOL.md) for
the full surface, or use any of the official client libraries (TypeScript,
Python, Go, Rust, …) against chronicle's base URL.

## Configuration

Flags take precedence over environment variables; both over defaults.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` | `CHRONICLE_LISTEN` | `:4437` | HTTP listen address (4437 is the protocol's IANA-selected port) |
| `--redis-url` | `CHRONICLE_REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `--store` | `CHRONICLE_STORE` | `redis` | Storage backend: `redis` or `memory` (dev/testing) |
| `--stream-root` | `CHRONICLE_STREAM_ROOT` | `/v1/stream/` | URL prefix streams live under |
| `--long-poll-timeout` | `CHRONICLE_LONG_POLL_TIMEOUT` | `30s` | How long `live=long-poll` waits before `204` |
| `--sse-reconnect-interval` | `CHRONICLE_SSE_RECONNECT_INTERVAL` | `60s` | SSE connection cycling (enables CDN collapsing) |
| `--subscriptions` | `CHRONICLE_SUBSCRIPTIONS` | `true` | Enable the reserved `__ds` subscription APIs (requires the redis backend) |
| `--public-url` | `CHRONICLE_PUBLIC_URL` | _(listen addr)_ | Externally reachable origin used in webhook `callback_url` / `jwks_url` |
| `--webhook-allow-private` | `CHRONICLE_WEBHOOK_ALLOW_PRIVATE` | `false` | Allow webhook delivery to private/loopback addresses (trusted networks / local dev) |

### Redis requirements

- Redis 6.0+ — chronicle uses `EVALSHA` Lua scripts, pub/sub, ZSET-lex, and
  key-level `PEXPIRE`/`PERSIST` (no hash-field TTLs / `HEXPIRE`). Managed Redis
  8.x is the recommended and standard target; the load-test rig validated on
  Memorystore Redis 7.2 ([loadtest/RESULTS-gke.md](loadtest/RESULTS-gke.md)).
- `maxmemory-policy noeviction` — any eviction policy can silently truncate
  streams. Chronicle warns at startup if it can read the config and it differs.
- Cluster mode: all keys for a stream share a `{path}` hash tag (single slot),
  so scripts stay cluster-legal. Fork lifecycle operations span two streams and
  are documented as non-atomic across slots.
- Durability honesty: Redis replication is asynchronous — an acknowledged
  append can be lost on failover. Within a healthy primary, appends are atomic
  and producer idempotency is exact.

## How it's stored

Per stream (path `p`):

| Key | Type | Holds |
| --- | --- | --- |
| `ds:{p}:meta` | hash | content type, tail offset, closed flag, TTL/expiry, fork lineage, refcount |
| `ds:{p}:msg` | sorted set | message frames `"<offset>\|<bytes>"`, ordered lexicographically by offset |
| `ds:{p}:prod` | hash | idempotent-producer state: `producerId → epoch:lastSeq` |
| `ds:notify:{p}` | pub/sub | wakes long-poll/SSE waiters on append/close/delete |

Every mutation is a single Lua script: validation (closure, content type,
`Stream-Seq` ordering, producer epoch/sequence), write, tail update, and
notification happen atomically and serialized per stream. The design and its
tradeoffs are documented in [docs/PLAN.md](docs/PLAN.md); the research that
informed it is under [docs/research/](docs/research/).

## Conformance

Chronicle is tested against the official
[`@durable-streams/server-conformance-tests`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests)
suite — hundreds of black-box protocol tests including idempotent producers, stream
closure, forks, JSON mode, SSE, and property-based fuzzing. The exact certified
result (currently **332/332 at `0.3.5`**) and the pinned spec commit are recorded
in [`SPEC_VERSION.md`](SPEC_VERSION.md):

```bash
make conformance                                   # full suite vs live Redis
make conformance-filter FILTER="Idempotent"        # one group while iterating
```

This includes the reserved subscriptions API (`__ds` webhooks + pull-wake): the
suite runs with `subscriptions: true` (`test/conformance/conformance.test.ts`)
and chronicle enables subscriptions by default. The subscription engine lives in
`subscriptions.go` + the `webhook/` package (signed webhook delivery, pull-wake
claim/ack/release, generation fencing, leases, JWKS). It is crash-hardened
against the four origin-restart windows — stranded pull-wakes, expired-lease
fence reuse, missed glob links, and a dropped fan-out index — as designed in
[docs/research/10-subscription-hardening-handoff.md](docs/research/10-subscription-hardening-handoff.md)
and recorded as built in
[docs/research/11-subscription-hardening-implemented.md](docs/research/11-subscription-hardening-implemented.md).

## Integrations

- **ElectricSQL Agents** — chronicle works as a drop-in Durable Streams backend
  for ElectricSQL's agents runtime (`@electric-ax/agents-*`). See
  [docs/ELECTRIC-AGENTS.md](docs/ELECTRIC-AGENTS.md) for a tested, copy-paste
  runbook (and the gotchas that aren't obvious).

## Development

New here? **[AGENTS.md](AGENTS.md)** is the implementer's map — codebase layout,
the cheat sheets (design docs, jepsen, the load-test rig), and the open scaling
work. The GKE load-test rig and its "don't repeat my mistakes" notes live under
[loadtest/](loadtest/) ([rig README](loadtest/README.md),
[implementer notes](loadtest/AGENTS.md), [results](loadtest/RESULTS-gke.md)).

```bash
make test         # unit + integration tests (-race); integration needs redis
make test-unit    # pure-core tests only (no redis, runs in <1s)
make lint         # golangci-lint
make fmt          # gofumpt + go mod tidy
```

Layout (see [docs/PLAN.md](docs/PLAN.md) for the architecture):

```
protocol/      pure protocol logic: headers, parsing, cursors, producer rules
store/         the storage contract (mirrors the Caddy plugin's store package)
store/redis/   the Redis backend: Lua scripts, frames, pub/sub waiters
handler.go     HTTP layer (mirrors the Caddy plugin's handler)
webhook/       the __ds subscription engine: webhook + pull-wake, fencing, sweep
metrics/       Prometheus /metrics + /healthz + /readyz (-metrics-listen)
cmd/chronicle/ the server binary
loadtest/      GKE + managed-Redis load-test rig (see loadtest/AGENTS.md)
loadgen/       the load generator (dsload + the sweep-scale driver)
```

The `store/` and handler layers intentionally track
`durable-streams/packages/caddy-plugin` (pinned at the commit recorded in
[docs/spec/README.md](docs/spec/README.md)); when upstream changes, diff those
files and port. Derived code is MIT-licensed from upstream — see NOTICE.
