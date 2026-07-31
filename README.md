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
| `--read-page-bytes` | `CHRONICLE_READ_PAGE_BYTES` | `1048576` | Returned payload target for each HTTP and SSE catch-up storage page. One valid frame may exceed it |
| `--sse-hub-replay-bytes` | `CHRONICLE_SSE_HUB_REPLAY_BYTES` | `1048576` | Replay memory retained for each active stream's shared SSE hub |
| `--sse-hub-batch-bytes` | `CHRONICLE_SSE_HUB_BATCH_BYTES` | `262144` | Target retained bytes in one shared live SSE data event |
| `--sse-notification-connections` | `CHRONICLE_SSE_NOTIFICATION_CONNECTIONS` | `1` | Maximum store-owned Redis Pub/Sub connections for SSE stream notifications |
| `--sse-client-write-timeout` | `CHRONICLE_SSE_CLIENT_WRITE_TIMEOUT` | `10s` | Maximum time allowed to flush one SSE data-and-control update to one client |
| `--metrics-listen` | `CHRONICLE_METRICS_LISTEN` | _(empty)_ | Separate listener for `/metrics`, `/healthz`, and `/readyz` |
| `--metrics-pprof` | `CHRONICLE_METRICS_PPROF` | `false` | Expose Go profiles under `/debug/pprof/` on the observability listener |
| `--subscriptions` | `CHRONICLE_SUBSCRIPTIONS` | `true` | Enable the reserved `__ds` subscription APIs (requires the redis backend) |
| `--public-url` | `CHRONICLE_PUBLIC_URL` | _(listen addr)_ | Externally reachable origin used in webhook `callback_url` / `jwks_url` |
| `--webhook-allow-private` | `CHRONICLE_WEBHOOK_ALLOW_PRIVATE` | `false` | Allow webhook delivery to private/loopback addresses (trusted networks / local dev) |
| _(env only)_ | `CHRONICLE_AUTH_MODE` | `insecure` | Stream authn/authz enforcement ([#126](https://github.com/adityavkk/chronicle/issues/126)): `insecure` evaluates decisions as telemetry only (base clients unaffected); `enforce` fails closed — appends require a claim-scoped write token (`electric-claim-token` or `Authorization: Bearer`) minted on pull-wake claim |
| _(env only)_ | `CHRONICLE_SERVICE_BEARER` | _(unset)_ | Trusted-backend service credential(s) ([#126](https://github.com/adityavkk/chronicle/issues/126) TB4): `token` or `name:token`, comma-separated for rotation overlap. Set the Electric agents-server's `DURABLE_STREAMS_BEARER` to a listed token; its requests are then served pre-authorized |
| _(env only)_ | `CHRONICLE_TRUSTED_SPIFFE_IDS` | _(unset)_ | In-mesh service allowlist: comma-separated `spiffe://` URIs matched against the sidecar-injected `X-Forwarded-Client-Cert` URI SAN (the last element across all XFCC header lines). The sidecar **must** strip client-supplied XFCC (Envoy `forward_client_cert_details: SANITIZE_SET`). **Fail-closed ([#130](https://github.com/adityavkk/chronicle/issues/126)):** setting this alone refuses startup — you must also set `CHRONICLE_XFCC_REQUIRED_HEADER` (a marker gate) **or** `CHRONICLE_XFCC_TRUST_WITHOUT_MARKER=true` (consciously accept the marker-less posture), so raw client XFCC is never trusted by default |
| _(env only)_ | `CHRONICLE_XFCC_REQUIRED_HEADER` | _(unset)_ | Optional `Name: value` sidecar-marker gate ([#126](https://github.com/adityavkk/chronicle/issues/126) hardening): when set, an XFCC mesh identity is honored only if the request also carries this header with this exact value — a header the sidecar injects and strips from inbound requests, so a forged XFCC alone cannot authenticate |
| _(env only)_ | `CHRONICLE_XFCC_TRUST_WITHOUT_MARKER` | `false` | Explicit opt-in to trust XFCC mesh identity with **no** marker gate ([#126](https://github.com/adityavkk/chronicle/issues/126) hardening, #130). Only for a deployment where the sidecar provably strips inbound XFCC. Prefer a marker instead; this exists so the fail-closed default can be deliberately overridden, never silently |
| _(env only)_ | `CHRONICLE_KEYS_FILE` | _(unset — keys live in Redis)_ | Load the Ed25519 signing key(s) + HMAC token key from a mounted secrets file ([#123](https://github.com/adityavkk/chronicle/issues/123)/[#126](https://github.com/adityavkk/chronicle/issues/126) custody): key material never touches Redis; a configured-but-unloadable file refuses startup. **Fail-closed perms ([#131](https://github.com/adityavkk/chronicle/issues/126)):** the mount must be `0400`/`0600` — any group- or world-readability (or any write bit) refuses to load |
| _(env only)_ | `CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ` | `false` | Explicit opt-in to load a **group-readable** keys file ([#131](https://github.com/adityavkk/chronicle/issues/126)). The documented Kubernetes `fsGroup` exception: a non-root container reading a root-owned secret through the group bit, where `0400` is unreadable and `0440` is the minimum. Still warns; never permits group-**write** or world access. Use a dedicated single-reader `fsGroup`, never a shared login group |
| _(env only)_ | `CHRONICLE_OIDC_ISSUER` | _(unset)_ | OIDC issuer for user principals ([#126](https://github.com/adityavkk/chronicle/issues/126) TB5): PingFed RS256/ES256 access tokens verified via discovery-fetched JWKS become namespace-scoped read/create/delete principals. Requires the other two OIDC vars |
| _(env only)_ | `CHRONICLE_OIDC_AUDIENCE` | _(unset)_ | Audience the OIDC token's `aud` must carry |
| _(env only)_ | `CHRONICLE_OIDC_NS_CLAIM` | _(unset)_ | Claim name holding the caller's namespace prefixes (string or array); the claim→scope mapping is IdP-side deploy config |
| _(env only)_ | `CHRONICLE_KEY_ROTATION_OVERLAP` | _(per-family defaults)_ | Rotation overlap window for both Ed25519 key families ([#123](https://github.com/adityavkk/chronicle/issues/123)): how long a retiring kid keeps verifying after its successor takes over; defaults derive from each family's max token lifetime |
| _(env only)_ | `CHRONICLE_WAKE_TOKEN_AUD` | _(unset)_ | `aud` stamped into minted `wake_token`s ([#123](https://github.com/adityavkk/chronicle/issues/123)) **and** required by the data-plane entity gate ([#126](https://github.com/adityavkk/chronicle/issues/126) TB6b) — one value keeps the mint and the gate in agreement; a woken entity's token then reads/appends within exactly its own entity subtree |

Chronicle captures one tail offset for each catch-up response and reads toward
it in bounded, frame-aligned storage pages. Each returned page targets 1 MiB
and at most 1,024 frames. Chronicle never splits a durable frame, so an
oversized first frame can exceed the byte target. Smaller pages lower
per-reader memory use but add Redis round trips.

Each Chronicle replica keeps one live SSE hub for each stream with connected
clients. The Redis store multiplexes all logical stream registrations over one
physical Pub/Sub connection by default. The same default uses Redis global
Pub/Sub for the supported cluster topology. Set
`--sse-notification-connections` above one only when notification actor load
requires parallel connection groups. Each extra group can open one more
physical Redis connection.

The hub reads each append once and shares one formatted data event with local
clients. Redis Pub/Sub is a wake hint. The hub rereads durable state every
second, so a lost notification does not lose data and an active live reader
renews a positive sliding TTL. A reconnect restores every desired channel and
wakes each hub for an immediate durable no-touch refresh. The first confirmed
notification generation starts from the register-first authoritative page
without a duplicate readiness read; exact attach still performs a final
bounded no-touch incarnation confirmation.

Each client holds one coalesced wake signal, not a payload queue. The replay
limit applies after a client reaches the live tail. A client that falls behind
the retained window, or cannot accept one update before the write timeout,
disconnects and resumes from the last `streamNextOffset` control value it
received. Batches are split by retained message plus formatted-event bytes, and
the batch target cannot exceed the replay limit. Chronicle never splits one
durable message, so one message may exceed both byte targets.

`/metrics` reports logical registrations in `chronicle_sse_subscriptions` and
physical connections in `chronicle_sse_notification_connections{topology}`.
It also reports active hubs and clients, bounded refresh pages and bytes,
catch-up and confirmation pages, exact raw, wire, index, and total ring bytes,
indexed lookup work and misses, lagged disconnects, write timeouts, lifecycle
events, and bounded recovery reasons through the `chronicle_sse_*` metrics.
No SSE metric uses a stream path label.

Runtime profiling is opt-in and requires `--metrics-listen`. Bind that listener
only on a protected network: pprof data can reveal process internals, and block
and mutex sampling adds overhead while profiling is enabled.

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

### Regional DR promotion hook

For active-passive Redis DR, the failover controller must notify each chronicle
process after the standby Redis has been promoted and the Redis endpoint has
flipped. Send `SIGUSR1` to the chronicle process. With subscriptions enabled,
chronicle handles `SIGUSR1` by calling `SubscriptionService.Promote()`, which
re-establishes slot ownership on the promoted primary and runs the eager
failover reconcile. Ordinary Redis reconnects also trigger an eager reconnect
reconcile through the go-redis `OnConnect` hook; `SIGUSR1` is the explicit
promotion decision hook.

## How it's stored

Per stream (path `p`):

| Key | Type | Holds |
| --- | --- | --- |
| `ds:{p}:meta` | hash | content type, tail offset, opaque incarnation ID, closed flag, TTL/expiry, fork lineage, refcount |
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
