# 06 — The Durable Streams ecosystem: positioning, operations, pitfalls, and how chronicle should track upstream

> Research briefing for **chronicle**, a Go implementation of the Durable Streams protocol backed by Redis 8, designed to mirror the official Caddy plugin closely enough to evolve with it.
>
> Sources: ElectricSQL announcement and 0.1.0 release posts, durablestreams.com deployment docs (and their VitePress sources in the monorepo), the Show HN thread (`item?id=46209189`) plus the adjacent s2-lite thread, the independent Rust server (`thesampaton.github.io/durable-streams-rust-server`), and the reference monorepo cloned at `/Users/auk000v/dev/durable-streams` (HEAD `82f9963ae0b489566352393be9b4796c788c99c2`, 2026-06-03).

---

## 1. Motivation and positioning

### 1.1 What problem it solves

Durable Streams is ElectricSQL's delivery protocol, extracted from 18 months of production use under their Postgres sync engine and standardized as a standalone protocol in December 2025. The framing from the announcement post (2025-12-09) and the repo README:

- **The gap:** durable, replayable logs exist everywhere in backend infrastructure (WALs, Kafka topics, event stores) but are not available as a first-class *client-facing* primitive. WebSocket/SSE connections are fragile — tabs suspend, networks flap, devices switch — and every team rebuilds bespoke resume protocols on top.
- **The trigger market:** AI products. "Token streaming is the UI for chat and copilots... When the stream fails, the product fails — even if the model did the right thing." The README's first line is now "Durable Streams are the data primitive for the agent loop."
- **The design:** each stream is a URL; an append-only log with **opaque, monotonic offsets** that clients persist locally and resume from ("give me everything after X"). Servers are **stateless with respect to readers** — no per-client session state. Create with `PUT`, append with `POST`, read with `GET`; catch-up reads then live tailing via long-poll or SSE. Content-type-agnostic byte streams, plus a JSON mode with array flattening.
- **Production claims:** "reliably delivering millions of state changes every day," sync latency "under 15ms end-to-end" through Postgres, and load tests with "millions of concurrent clients subscribed to a single stream without degradation" (achieved via CDN fan-out, not origin connections).

In the Show HN post, co-founder Kyle Mathews stated the explicit goal: **"a spec with many implementations, not a single codebase,"** inviting ports in other languages and pointing at the conformance suite as the compatibility contract. Chronicle is exactly the kind of implementation they are soliciting.

### 1.2 Protocol layering and roadmap

The ecosystem is deliberately layered, with Electric 2.0 being refactored on top of it:

| Layer | Status | What it is |
|---|---|---|
| **Durable Streams** | shipped (0.1.0, 2025-12-23) | Transport: durable, offset-resumable byte/JSON streams over HTTP |
| **State Protocol** (`@durable-streams/state`) | shipped with 0.1.0 | "Insert, update, and delete operations on typed entities" with optional transaction IDs/timestamps, layered as messages on a stream; TanStack DB integration for reactive queries via differential dataflow |
| **Database adapters** | roadmap | Postgres/MySQL/SQLite replication speaking the State Protocol |
| **Electric Cloud** | launched early 2026 | Managed hosting |

Higher-level products already built on the stream layer: Durable Proxy (AI token streams), Durable Sessions, StreamDB, StreamFS, y-durable-streams (Yjs), TanStack AI and Vercel AI SDK transports, AnyCable integration.

**Implication for chronicle:** the protocol layer is the stable contract; everything above it (state, sessions, StreamDB) rides on conformant servers for free. Chronicle never needs to know about the State Protocol — it just has to be byte-exact at the transport layer.

### 1.3 0.1.0 release facts

- First npm releases: `@durable-streams/client`, `server`, `state`, `cli`, `server-conformance-tests`, `client-conformance-tests`.
- At 0.1.0: **124 server conformance tests, 110 client conformance tests** (ported from Electric's production suite). The suite has since grown fast — see §6.
- "A production-ready binary built on Caddy" is the recommended production server; the Node server is explicitly dev/test only.
- Official Go and Python clients shipped alongside TypeScript.
- All packages are **pre-1.0**; `AGENTS.md` mandates `patch` changesets unless breaking.

---

## 2. Ecosystem map

| Component | Where | Relevance to chronicle |
|---|---|---|
| Protocol spec | `PROTOCOL.md` (repo root, "source of truth"; v1.0 DRAFT) | The contract; see §6 on churn |
| Caddy plugin (Go) | `packages/caddy-plugin`, version 0.1.0 | The codebase chronicle mirrors |
| Node reference server | `packages/server` | Dev/test; defines defaults chronicle should echo |
| Server conformance suite | `packages/server-conformance-tests` (npm 0.3.5 at HEAD) | Chronicle's acceptance gate |
| Client conformance suite | `packages/client-conformance-tests` | YAML test cases; where bug fixes land first per repo policy |
| Go client | `packages/client-go` | Candidate e2e harness for chronicle (§5) |
| Rust server (independent) | thesampaton.github.io/durable-streams-rust-server | Best prior art for a non-Caddy backend (§4) |
| Community impls | `ahimsalabs/durable-streams-go` (alt Go client+server), `Clickin/durable-streams-java` | Evidence third-party servers are welcomed |
| Adjacent ecosystem | S2 / s2-lite (s2.dev), Honker, AnyCable | Competing/complementary "streams as a primitive" systems; source of practitioner critique (§3.2) |

---

## 3. Operational guidance and community-raised pitfalls

### 3.1 Official deployment guidance (durablestreams.com/deployment, `docs/deployment.md`)

Defaults and directives chronicle should reproduce or consciously map:

| Caddy directive | Default | Meaning |
|---|---|---|
| `data_dir` | none (in-memory) | Persistent storage path (file segments + LMDB metadata) |
| `long_poll_timeout` | `30s` | How long the server parks long-poll requests |
| `sse_reconnect_interval` | `60s` | Server proactively closes SSE connections this often **for CDN cache collapsing** |
| `max_file_handles` | `100` | Open-file LRU cache (file store only) |

Other operational facts:

- Default port **4437**, stream endpoint **`/v1/stream/*`**; binary runs as `durable-streams-server dev` (zero config, in-memory) or `run --config Caddyfile`.
- Node server adds `compression` (gzip/deflate, default on) and **`cursorIntervalSeconds` (default 20)** — the cursor interval used for CDN request collapsing in live mode.
- **Auth is explicitly out of protocol scope.** The recommended pattern is `forward_auth` in front of the handler (or a static API-key matcher). Chronicle should likewise keep auth out of the core handler and document middleware/forward-auth patterns rather than inventing protocol-level auth.
- Service packaging: systemd unit (`Restart=always`, `RestartSec=5`) and a slim Docker image exposing 4437 with a `/data` volume.
- **CDN integration is a design pillar:** (a) catch-up reads from a given offset are immutable — "safe to cache indefinitely at the edge"; (b) cursor-based collapsing lets many live readers at the same offset collapse into few origin connections; (c) ETags enable conditional requests. One origin + CDN should serve massive read fan-out.

#### The documented known limitation (fsync/crash-atomicity)

Quoting the deployment docs verbatim, because it is the most important upstream operational caveat:

> **File store crash-atomicity.** The file-backed store does not atomically commit producer state with data appends. Data is written to segment files first, then producer state is updated separately. If a crash occurs between these steps, producer state may be stale on recovery. The practical impact is low. The likely failure mode is a false `409` (sequence gap) on restart, not duplicate data. Clients can recover by incrementing their epoch. See issue #143.

**Chronicle can do strictly better here.** Redis lets us commit the append payload, the stream's next-offset, and the producer `(producerId, epoch, seq)` state in a single atomic unit (`MULTI/EXEC` or a Lua/Redis Function executing atomically on the single-threaded engine). Atomic producer-state+data commit should be a stated design goal and a differentiator over the file store.

### 3.2 Community critique and practitioner gotchas

The Show HN thread (`news.ycombinator.com/item?id=46209189`, "Show HN: Durable Streams – Kafka-style semantics for client streaming over HTTP") was **small: 10 points, 3 comments** — no operational war stories landed there. What it did surface:

- **CRDT relationship** (user `novoreorx`, answered by `Mrazator`): Durable Streams is a transport with built-in reconnect/offset semantics; CRDTs are conflict resolution. They compose — Durable Streams is a good carrier for CRDT updates (hence `y-durable-streams`).
- A telling adoption observation from `novoreorx`: "I could give Durable Stream's protocol spec to a coding agent, and it could blend into the best suited implementation for my current project (say, a Go repo). **The simple yet sophisticated spec is more valuable than a bunch of SDKs.**" — i.e., independent spec-driven servers like chronicle are the expected mode of adoption.
- Kyle Mathews' framing comment (offsets opaque and monotonic, no server-side session state, CDN-cacheable, conformance suite as the compatibility bar).

Because that thread is thin, the substantive practitioner critique comes from adjacent discussions and from the upstream docs themselves:

1. **Durability before acknowledgment** (s2-lite thread, `shikhar` of s2.dev): "Every write with S2 implementations is durable before it is acknowledged." This is the bar serious stream stores set. Most damning for us, his landscape framing: **"Redis Streams and NATS allow for larger numbers of streams, but without proper durability."** Chronicle must confront this head-on: document the exact durability envelope per Redis persistence configuration (see §3.3) instead of hand-waving "Redis is durable."
2. **The flaky last mile is the whole point** (same thread): "iOS will cancel connections when users background an app. Using a durable stream for persisting the tokens means a client can ask to resume... without having to re-inference." Resume-after-disconnect is the product feature; server restarts must never corrupt offsets.
3. **Proxy buffering breaks live modes.** From the monorepo's own docs (`docs/yjs.md:208`): "**`flush_interval -1` is required — without it, Caddy buffers SSE responses and live updates stop working.**" The same class of failure applies to nginx (`proxy_buffering off` / `X-Accel-Buffering: no`), Envoy, and some CDNs. Chronicle must (a) flush after every SSE event and long-poll chunk, (b) set headers that discourage intermediary buffering, and (c) document reverse-proxy requirements prominently.
4. **Orderly shutdown is underspecified everywhere.** Neither durablestreams.com nor the Rust server's production page documents a graceful-shutdown procedure; the Caddy plugin inherits Caddy core's graceful reload/drain. For chronicle this is a first-class requirement, not an afterthought: at any moment the server may hold thousands of parked long-polls (up to `long_poll_timeout`, 30s) and open SSE connections (up to `sse_reconnect_interval`, 60s). On SIGTERM chronicle should stop accepting new requests, **wake all parked long-polls immediately** (returning `204`/up-to-date responses so clients resume cleanly against the next instance), terminate SSE connections cleanly (clients are built to reconnect — the 60s reconnect interval exists precisely so disconnects are routine), flush any pending Redis writes, and exit within a deadline compatible with `systemd`/Kubernetes termination grace periods. The protocol's offset-resume semantics make aggressive draining safe — exploit that.
5. **CDN interplay cuts both ways.** The cacheability rules are precise: immutable catch-up responses (cache forever), `no-store` on mutating/HEAD responses, cursor query parameters to make live long-poll URLs collapse at the edge, and deliberate periodic SSE closes for cache collapsing. Get one `Cache-Control` header wrong and a CDN will serve a stale tail or, worse, cache a long-poll response. Note that the Rust implementation **diverged** here (it emits `no-store` nearly everywhere and implements no cursor collapsing) — chronicle should follow the Caddy plugin's caching behavior, not the Rust server's, since CDN fan-out is a core protocol promise and the conformance suite covers caching headers.
6. **False 409s after crash recovery** (issue #143 class): clients recover by bumping the producer epoch; the Go client's `IdempotentProducer` supports `AutoClaim` for this. Chronicle's atomic Redis commits eliminate the cause, but e2e tests should still cover the recovery path.

### 3.3 Translating the operational guidance to a Redis 8 backend

| Upstream concern | File-store reality (Caddy) | Chronicle / Redis 8 mapping |
|---|---|---|
| Append durability | `fsync` per append or batched; crash-atomicity gap (#143) | AOF `appendfsync everysec` = up to ~1s loss window (must be documented); `appendfsync always` = fsync-class per write (large throughput cost — see Rust numbers in §4.3); optionally `WAIT` for replica acknowledgment |
| Producer state atomicity | Non-atomic with data | Atomic via single Lua/Function or `MULTI/EXEC`; co-locate stream data + producer hash under one key slot (`{stream}` hash tags) for Cluster compatibility |
| Long-poll wakeup | In-process notification | Same-process registry when single-node; Redis keyspace notifications or pub/sub channel per stream if chronicle ever scales horizontally behind a non-collapsing LB |
| Restart durability | LMDB metadata + segment files | RDB snapshots alone are **insufficient** (point-in-time); AOF or AOF+RDB required; `maxmemory-policy noeviction` is mandatory — eviction of stream keys silently destroys history and breaks the immutable-offset contract |
| Retention / TTL | `Stream-TTL` sliding window in spec (added 2026-04) | Maps naturally to Redis `EXPIRE` with renewal-on-access, but expiry of *parts* of a stream (offset `410 Gone` semantics) needs explicit modeling, not key TTL alone |
| Memory pressure | `max_file_handles` LRU | Redis memory monitoring; per-stream and global byte limits like the Rust server's `DS_LIMITS__MAX_STREAM_BYTES` (10 MB) / `DS_LIMITS__MAX_MEMORY_BYTES` (100 MB) defaults are a sensible pattern |
| TLS / auth | Caddy automatic TLS; `forward_auth` | Same posture: terminate TLS and auth in front (Caddy/Envoy/nginx); keep chronicle's handler protocol-only |

---

## 4. The independent Rust server: design decisions worth borrowing

`thesampaton.github.io/durable-streams-rust-server` is the most instructive prior art for a non-Caddy backend: an axum + tokio HTTP service, "protocol-only, thin handlers, pluggable storage," with **all 239 conformance tests passing** against suite `0.2.3`, pinned to spec commit `a347312a47ae510a4a2e3ee7a121d6c8d7d74e50`.

### 4.1 Architecture choices

- **Pluggable storage behind one trait**, four modes: `memory` (no restart durability), `file-fast`, `file-durable` ("durable mode fsyncs each append"), and `acid` — **sharded redb** (embedded ACID KV store) with "crash-safe ACID commits" and "fsync-class semantics on each commit."
- **Sharded single-writer:** `shard = seahash(stream_name) & (shard_count - 1)`; `DS_STORAGE__ACID_SHARD_COUNT` is a power of two, 1–256, default 16. "Writes are serialized per shard (single writer per shard). Increase shard count for highly concurrent write workloads." This preserves per-stream ordering while allowing cross-stream concurrency. **Redis gives chronicle this property for free** — the engine is single-threaded per command, so per-stream ordering is inherent; chronicle's equivalent decision is script granularity, not shard topology.
- **Auth fully externalized:** Envoy validates JWTs and "forwards the `sub` claim as `X-JWT-Sub`"; "any reverse proxy that validates JWTs and forwards claims works." The DS server is stateless about identity.
- **Infrastructure endpoints outside the protocol namespace:** `GET /healthz` (returns 200 `"ok"`) deliberately lives outside `/v1/stream/` because "health checks are infrastructure, not protocol API." Chronicle should copy this (`/healthz`, perhaps `/readyz` checking Redis connectivity).
- **Env-var configuration** with a consistent `DS_<SECTION>__<KEY>` scheme (`DS_STORAGE__MODE`, `DS_HTTP__CORS_ORIGINS`, `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS`, `DS_LIMITS__*`, `DS_TLS__CERT_PATH`/`KEY_PATH`). CORS defaults permissive (`*`), restrict in production.

### 4.2 Protocol-level decisions (their `docs/decisions.md`)

These are exactly the sharp edges a new implementation hits; chronicle should pre-adopt them:

| Decision | Rationale |
|---|---|
| SSE event type is `event: data`, not `message` | "Match PROTOCOL.md exactly; `message` is the browser default, protocol uses explicit `data` type" |
| One `event: data` per stored message | Preserves message boundaries per conformance tests |
| Binary = everything not `text/*` or `application/json` | Aligns with the protocol's base64 SSE encoding rule |
| SSE idle close default 60s, configurable (0 disables) | Spec only says "SHOULD ~60s" — documented as a spec gap |
| Idempotent close returns `204`, same on repeat | Idempotency over informativeness |
| `Stream-Closed: true` on **all** success responses for closed streams | Clients detect closure without an extra HEAD |
| ETag format `{start_offset}:{end_offset}`, `:c` suffix when the read reaches a closed tail (e.g. `-1:...001a:c`) | Deterministic conditional requests |

One **divergence to avoid**: their caching posture (`Cache-Control: no-store` on most responses, no cursor collapsing) abandons the CDN fan-out story. Follow the Caddy plugin instead.

### 4.3 Benchmarks — the cost of fsync, quantified

Their local synthetic benchmarks (load generator and server on one machine; treat as directional only):

| Configuration | RTT latency | Small-msg throughput |
|---|---|---|
| Rust memory | 0.407 ms | 377,144 msg/s |
| Caddy memory | 0.342 ms | 313,491 msg/s |
| Node memory | — | 334,944 msg/s |
| Rust file-durable | 5.379 ms | 6,442 msg/s |
| Rust acid (redb) | 5.908 ms | 6,496 msg/s |
| Caddy acid | 16.067 ms | — |

The takeaway: **true per-write durability costs ~50–60x in throughput** versus memory. For chronicle this is the strongest argument for making the Redis `appendfsync` policy (and optionally `WAIT`-based replica acks) an explicit, documented dial rather than a hidden default — and for batching appends (the protocol's producer batching exists partly for this).

### 4.4 Governance pattern (their strongest idea — see §6)

The Rust project pins the spec by **commit SHA** in a root `SPEC_VERSION.md` and resolves ambiguity through a four-tier decision hierarchy:

1. **Conformance tests** are behavioral ground truth;
2. the **pinned `PROTOCOL.md`** at that SHA is the textual reference;
3. **maintainer clarifications** in linked upstream issues/PRs;
4. **conservative fallback**, recorded in `docs/gaps.md` — "no undocumented assumptions."

Plus: `docs/decisions.md` (rationale log) and `docs/compatibility.md` (version matrix), with the policy that "every externally observable behavior (paths, headers, status codes, framing) must link to" a spec section, a conformance test, or an explicit gap. Default stance: "the least-committal interpretation that preserves forward compatibility."

---

## 5. `packages/client-go` — the e2e test harness candidate

Module `github.com/durable-streams/durable-streams/packages/client-go`, Go 1.21+ (range iterators need 1.23+), **zero dependencies** (stdlib `net/http` only), concurrency-safe. It ships `cmd/conformance-adapter` (the bridge the official client conformance suite drives) and a `DESIGN.md` documenting that the API deliberately follows google-cloud-go conventions.

Core surface (exact signatures):

```go
func NewClient(opts ...ClientOption) *Client
func (c *Client) Stream(url string) *Stream
func (c *Client) IdempotentProducer(url, producerID string, config IdempotentProducerConfig) (*IdempotentProducer, error)

// Stream lifecycle
func (s *Stream) Create(ctx context.Context, opts ...CreateOption) error   // WithContentType(...)
func (s *Stream) Append(ctx context.Context, data []byte, opts ...AppendOption) (*AppendResult, error)
func (s *Stream) Head(ctx context.Context) (*Metadata, error)
func (s *Stream) Close(ctx context.Context, opts ...CloseOption) error     // WithCloseData(...)
func (s *Stream) Delete(ctx context.Context) error

// Reads
func (s *Stream) Read(ctx context.Context, opts ...ReadOption) *ChunkIterator   // it.Next() / Done sentinel
func (s *Stream) Chunks(ctx context.Context, opts ...ReadOption) iter.Seq2[*Chunk, error]
func JSONItems[T any](ctx context.Context, stream *Stream, opts ...ReadOption) iter.Seq2[T, error]
func JSONBatches[T any](ctx context.Context, stream *Stream, opts ...ReadOption) iter.Seq2[*Batch[T], error]

type Offset string  // opaque; resume via WithOffset(o), live via WithLive(LiveModeLongPoll | LiveModeSSE)
```

Useful details for chronicle's e2e suite:

- **Sentinel errors map 1:1 to protocol status codes:** `ErrStreamNotFound` (404), `ErrStreamExists` (409), `ErrStreamClosed` (409), `ErrOffsetGone` (410), plus `StreamError{Op, StatusCode}`, `StaleEpochError`, `SequenceGapError`. Asserting on these in tests pins chronicle's status-code behavior.
- `Chunk` exposes `Data`, `NextOffset`, `UpToDate` — the catch-up→live transition signal.
- **`IdempotentProducer`** exercises the hardest server paths: batching (default 1 MB max batch, 5 ms linger), pipelining (up to 5 in-flight batches), `(producerId, epoch, seq)` dedup, zombie fencing by epoch, `AutoClaim`, `Flush`, `Restart`, `CloseStream`. A chronicle e2e suite that hammers `IdempotentProducer` against Redis with concurrent producers and forced restarts covers exactly the crash-atomicity territory of issue #143.
- Built-in retry policy (configurable exponential backoff) and SSE base64 handling (`internal/sse`) mean the client also implicitly tests chronicle's `Retry-After` and SSE encoding behavior.

**Recommendation:** use `client-go` as chronicle's e2e driver (it lives in the same monorepo as the spec and conformance suites, so it evolves in lockstep), but treat the **server conformance suite as the acceptance gate** and client-go e2e as supplementary integration/chaos coverage.

---

## 6. Tracking upstream: spec evolution, pinning, and conformance versioning

### 6.1 How fast upstream moves

The spec is a living document (versioned "1.0 DRAFT") and it moves quickly — `PROTOCOL.md` changed 7 times in 2026 alone:

| Date | Change |
|---|---|
| 2026-03-25 | PUT for awareness streams, DELETE for documents/awareness |
| 2026-04-06 | **Stream forking** added to the protocol (§4.2) |
| 2026-04-08 | **`Stream-TTL` sliding window** with auto-renewal |
| 2026-05-25 | **Reserved subscription APIs** (webhooks, pull-wake claim/ack/release, generation fencing — spec §6–7) |
| 2026-06-01 | `Stream-Fork-Sub-Offset` for arbitrary-position forks |
| 2026-06-02 | Caddy: full conformance coverage + soft-delete, fork content-type, live SSE close fixes |

The conformance suite tracks this: the Rust server passed **239 tests at suite 0.2.3**; at monorepo HEAD the suite is **0.3.5** with roughly **320+ test cases**. The 0.1.0 release had 124. In other words, the suite has nearly tripled in six months. "Passes conformance" is meaningless without a suite version attached.

### 6.2 Repo conventions chronicle must respect (`AGENTS.md`)

- `PROTOCOL.md` is the **source of truth**; everything else, including the Caddy plugin, follows it.
- **Conformance-first testing philosophy:** bug fixes land as conformance test cases *first* (YAML in `packages/client-conformance-tests/test-cases/`), then implementations are fixed. Chronicle inherits regression protection automatically by re-running newer suites — but only if it stays current.
- Pre-1.0 semantics: changes ship as `patch` releases via changesets; breaking changes can arrive in any minor. Do not assume semver stability.
- Caddy plugin Go tests run with `cd packages/caddy-plugin && go test ./...` — chronicle mirroring that layout makes diffing against upstream mechanical.

### 6.3 Recommended tracking regime for chronicle

Adopt the Rust server's governance pattern wholesale — it is the only battle-tested model for an out-of-tree server:

1. **Pin by commit SHA, not branch.** Create `SPEC_VERSION.md` at chronicle's root recording (a) the upstream monorepo commit the vendored `docs/spec/PROTOCOL.md` was taken from — currently `82f9963ae0b489566352393be9b4796c788c99c2` (2026-06-03) — and (b) the exact conformance suite version chronicle is certified against — currently `@durable-streams/server-conformance-tests@0.3.5`. (Precedent: the Rust server pins `a347312a47ae510a4a2e3ee7a121d6c8d7d74e50` + suite `0.2.3`.)
2. **Run the suite pinned, in CI:** `npx @durable-streams/server-conformance-tests@0.3.5 --run http://localhost:<port>` against chronicle + a live Redis. The CLI also supports `--watch <paths> <url>` for development. Record the pass count in `SPEC_VERSION.md` ("N/N at 0.3.5").
3. **Scheduled upgrade job:** periodically run the *latest* suite version in a non-blocking CI lane; when it fails, that is the signal the spec moved. Upgrade = bump suite version + spec SHA together in one PR, with new behaviors implemented or logged as gaps.
4. **Adopt the four-tier decision hierarchy** (conformance tests > pinned PROTOCOL.md > upstream maintainer clarifications > conservative fallback) and keep `docs/gaps.md` + `docs/decisions.md`. Every observable behavior — path, header, status code, framing byte — must trace to a spec section, a test, or a documented gap.
5. **Mirror the Caddy plugin's internal structure** (handler / store interface / memory store / persistent store split) so upstream commits to `packages/caddy-plugin` can be reviewed as candidate chronicle changes. The 2026-06-02 upstream commit (soft-delete, fork content-type, live SSE close fixes) is exactly the kind of change chronicle wants to be able to port by inspection.
6. **Go beyond black-box conformance** per upstream's `IMPLEMENTATION_TESTING.md`, which distills Electric's reliability sprint (120 days, 200+ bugs) into required white-box test areas: crash/incomplete-write recovery (idempotent across repeated restarts), torn-read prevention under concurrent write, delete-during-read, atomic offset persistence under 100-way concurrent appends, resource/handle leak checks, identical behavior across storage backends (chronicle: memory store vs Redis store under one shared test suite), startup-ordering races, and property-based random operation sequences. These are precisely the failure modes a Redis-backed store must prove out (e.g., Lua script atomicity under `SHUTDOWN NOSAVE` + AOF replay).

---

## 7. Summary of recommendations for chronicle

1. **Position chronicle as a conformance-certified, Redis-backed peer of the Caddy plugin** — upstream explicitly wants independent implementations; the conformance suite is the compatibility contract.
2. **Beat the file store on durability semantics:** atomic data+producer-state commits via Redis scripts (closes the issue-#143 gap); document the AOF `appendfsync` durability envelope honestly (the "Redis Streams aren't properly durable" critique must be answered in writing).
3. **Treat graceful shutdown as a feature:** wake parked long-polls, close SSE cleanly, flush, exit within the orchestrator's grace period — the protocol's resume semantics make aggressive draining safe.
4. **Preserve the CDN story:** immutable catch-up responses, correct `Cache-Control`/ETag (`{start}:{end}`, `:c` on closed tail), cursor collapsing, periodic SSE closes (`sse_reconnect_interval`, default 60s). Do not copy the Rust server's `no-store`-everywhere shortcut.
5. **Document proxy requirements:** flush per event; call out Caddy `flush_interval -1`, nginx `proxy_buffering off` / `X-Accel-Buffering: no`.
6. **Borrow from the Rust server:** `/healthz` outside `/v1/stream/`, env-var config scheme, per-stream/global byte limits, `Stream-Closed: true` header, SSE `event: data`, idempotent `204` close — and its entire SPEC_VERSION/gaps/decisions governance apparatus.
7. **Pin everything:** spec commit SHA `82f9963ae0b489566352393be9b4796c788c99c2`, conformance suite `0.3.5`, with a standing CI lane that runs the latest suite to detect spec drift early.
8. **Use `client-go` for e2e** (zero-dep, same-repo, exercises producer batching/pipelining/fencing and sentinel-error status mapping), with the server conformance suite as the hard acceptance gate.
