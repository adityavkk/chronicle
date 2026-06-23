I have all three research outputs already provided in the prompt. This is a synthesis task, so I will write the report directly. Let me also apply the plain-writing style the user expects.

I do not need to run any tools or read any files since the task gives me everything as structured JSON and asks only for a synthesized Markdown report. I will write it now.

# Prior-Art Report: A DBeaver-Style UI for Chronicle Durable Streams

## 1. Executive summary

Chronicle is a Go and Redis server that implements the Durable Streams protocol over plain HTTP. It runs on port 4437 by default and exposes two surfaces: a stream API under `/v1/stream/` and a subscription control plane under `/__ds/`. Today the chronicle repository ships no user interface at all. It serves only the protocol HTTP API plus a Prometheus metrics, health, and readiness endpoint. There is no in-repo UI to confirm.

The Durable Streams project upstream does ship one example interface, the `@durable-streams/example-test-ui` app. It is a React single-page app that connects to a server and lets you create streams, write to them, and tail them live. It works, but it has one disqualifying flaw for the user's goal: the server URL is hard-coded to `http://<page-hostname>:4437` in three separate source files, with no input field, no settings panel, and no environment variable. You cannot point it at an arbitrary server URL and port without editing the source and rebuilding. It is also a private demo app, not a packaged admin console, and has no authentication, no saved-connection support, and no production target beyond a basic build.

The gap is therefore clear. There is no purpose-built console that lets a person connect to any chronicle server by its URL and port, browse the streams on it, read and filter messages, tail streams live, and inspect the subscription and webhook control plane. This report describes how chronicle works so a UI can speak to it, what the existing test UI does and where it falls short, the best patterns from mature database and stream tools, and a prioritized set of requirements for the new UI.

## 2. How chronicle and Durable Streams work

### The data model a UI must represent

A **stream** is an append-only sequence of bytes addressed by a URL. Each stream has a content type, an optional time-to-live or absolute expiry, an open or closed (end-of-file) flag that is durable and only ever moves from open to closed, and optional fork metadata.

An **offset** is an opaque string token that sorts in the same order lexicographically as the data it points at. The value `-1` means the beginning of the stream, and `now` means the current tail. After any read, the server returns a `Stream-Next-Offset` header that the client passes back as the `offset=` query parameter on the next read. This is how a reader resumes. Catch-up reads are immutable and cacheable.

A **message** is a data-and-offset pair. When the stream content type is `application/json`, the server frames messages as JSON objects and a bulk read returns an array of objects. Otherwise the data is raw bytes.

A **producer** is an idempotent writer identified by a producer id, an epoch (int64), and a sequence number (int64). This gives exactly-once append within an epoch and fences out stale writers when the epoch advances.

A **fork** is a new stream created with a `Stream-Forked-From` header. It inherits the source stream's data up to a fork offset and then diverges. Forks hold a reference count on the source, which affects deletion (a source with live forks is soft-deleted).

The **subscription control plane** (the `/__ds/` surface) is a durable cursor that watches one or more streams, either by a glob pattern or by an explicit list. A subscription has a type, either **webhook** (the server pushes a signed POST to a worker) or **pull-wake** (the server writes a wake event and a worker claims it). Each subscription moves through phases: idle, waking, and live. A **generation** counter increments on state changes and is used to fence stale leases so a restarted worker cannot deliver duplicates. A **lease** gives one worker exclusive hold for a configured time (1 second to 10 minutes, default 30 seconds). Each watched stream is tracked as a **link** with a link type (glob or explicit) and an acked offset. A **wake** tells a worker that streams have pending messages; the worker reads the snapshot of links (path, acked offset, tail offset, has-pending flag), acks the offsets it processed, and then releases or marks done.

### The wire protocol the UI speaks

The transport is plain HTTP/1.1 or HTTP/2. There is no client library requirement; a UI can speak this with ordinary HTTP requests.

Stream operations:
- `PUT /v1/stream/{path}` creates a stream. Headers set content type, optional TTL or expiry, immediate closure, or fork lineage.
- `POST /v1/stream/{path}` appends one batch. Idempotent-producer headers give exactly-once. A `Stream-Closed: true` header closes the stream atomically.
- `GET /v1/stream/{path}?offset=O` reads from an offset. Use `offset=-1` for the beginning and `offset=now` for the tail. Add `live=long-poll` to block until data or a timeout, or `live=sse` for a Server-Sent Events live tail. Add `cursor=` to avoid CDN request collapsing.
- `HEAD /v1/stream/{path}` returns metadata headers without a body: content type, next offset, TTL, and the closed flag.
- `DELETE /v1/stream/{path}` deletes a stream (soft-delete if forks reference it, otherwise hard delete).

Subscription operations under `/__ds/subscriptions/{id}` cover create, fetch (with a links array), delete, add or remove explicit streams, pull-wake claim, ack (for both webhook callbacks and pull-wake), and release. A FENCED response (HTTP 409) means a stale generation tried to act. `GET /__ds/jwks.json` returns the public keys used to verify signed webhooks.

Every response carries `Stream-Next-Offset`. Other useful response headers include `Stream-Closed`, `Stream-Up-To-Date`, `Stream-Cursor`, and an `ETag` for cached historical reads.

There is one important thing the protocol does not provide: a server endpoint that lists all streams. The upstream test UI works around this by reading a special `__registry__` stream that records create and delete events. A new UI will face the same constraint and must decide how to discover streams (see Open questions).

Authentication is out of protocol scope. The spec expects a reverse proxy in front to handle auth. Chronicle keeps auth out of its core handler. The webhook layer is signed separately with an Ed25519 key published through the JWKS endpoint, but that is server-to-worker delivery auth, not UI-to-server auth.

### How to run chronicle locally and where a UI connects

1. Install Go 1.26.2 or later, plus Docker and Docker Compose.
2. From `/Users/auk000v/dev/chronicle`, start Redis: `make redis-up` (Redis 8 on port 6379 with AOF persistence).
3. Build the binary: `make build` (outputs `bin/chronicle`).
4. Run the server: `make run` (starts on `:4437`, connected to `redis://localhost:6379`, with subscriptions enabled).
5. Verify: `curl -i -X PUT http://localhost:4437/v1/stream/test` creates a stream, a `POST` with a JSON body appends, and `GET '.../test?offset=-1'` reads from the beginning.

Ports a UI cares about: **4437** is the stream and subscription API (this is what the UI connects to). **6379** is Redis (internal to the server, not for the UI). **9090** is optional Prometheus metrics if the metrics listener flag is set. **3000** is the docs-site dev server, unrelated to the UI.

So the UI connects to a chronicle server at, by default, `http://localhost:4437`. The whole point of the new UI is that this base URL and port must be a user-entered, savable value, not a constant.

## 3. What the default UI does and why it falls short

The upstream `@durable-streams/example-test-ui` is a Vite, React 19, and TypeScript single-page app. It does a lot right as a demo:

- Create and delete streams from a left sidebar, choosing a content type.
- Discover streams through the `__registry__` system stream so refreshes and multiple clients see the same list.
- Live tail: catch up from offset `-1`, then switch to long-poll live mode with automatic reconnection and backoff.
- Render by content type: continuous text for `text/plain`, JSON cards for `application/json`.
- Write interactively with a multi-line composer.
- Show per-stream live stats (entries, bytes, messages per minute), a connection badge, a JSON filter expression box, a demo producer, and a disconnect/resume toggle that simulates going offline to test resume-from-offset behavior.
- Show presence and typing indicators.

The shortcomings that matter for the user's goal:

- **The server URL is hard-coded** as `http://${window.location.hostname}:4437` in `src/routes/__root.tsx`, `src/routes/stream.$streamPath.tsx`, and `src/lib/stream-db-context.tsx`. There is no URL, host, or port input, no settings panel, and no environment variable. To target a different server you must edit source and rebuild. This is the exact limitation the user hit.
- **The port is locked to 4437 and the host is forced to the page's own hostname**, so it cannot reach a remote server, a different port, or a chronicle instance on a non-default listen address.
- **The disconnect/resume toggle is a test fixture**, not a way to switch which server you connect to.
- **Stream discovery depends on the registry hook.** Against a plain protocol server without that hook, the UI falls back to writing registry events itself, but there is no use of a real server listing API.
- **It is a private demo** (private, version 0.1.0), not a packaged or installable console. No authentication, no multi-server or profile support, no real production target.
- **No HTTPS or auth handling.** It assumes plain HTTP to localhost.

The fixes that follow from this list: make the server connection a first-class, user-entered, savable thing; support many servers; support remote hosts, custom ports, and TLS; handle auth headers; and treat stream discovery as a deliberate design decision rather than a registry side effect.

## 4. Prior-art pattern catalog

The table below pulls the strongest patterns from DBeaver and the stream, queue, and log consoles studied, and notes how each applies to a chronicle UI.

| Pattern | Where it comes from | Applies to chronicle? |
|---|---|---|
| Connection wizard: pick type, enter host:port and credentials, **Test Connection** before save | DBeaver, RedisInsight | Yes, directly. This is the core missing feature. Test = a `HEAD` or a harmless `GET`. |
| Many named saved connections, grouped in folders and workspaces | DBeaver | Yes. Lets one person manage local, staging, and load-test servers. |
| Top-level server or cluster selector to span environments without re-login | Provectus, AKHQ | Yes. Maps to switching between chronicle deployments. |
| Navigator tree as the left rail: server to namespace to object, with adaptive right-click menus | DBeaver | Yes. Server to stream-path-prefix to stream. |
| Instant partial-match filter bar over the tree, with a scope-cycling button | DBeaver | Yes. Filter streams by path quickly. |
| List view with inline metadata: counts, water marks, durability, space | Redpanda, Kafdrop | Partly. Chronicle has tail offset, closed flag, content type, TTL, fork lineage; it has no partition or replication concept. |
| Message browser as a data grid: offset, key, timestamp, headers; per-column sort and a filter bar | DBeaver grid, Kafdrop rows | Yes, adapted. Columns are offset, decoded payload, and per-message metadata. No partition or key column. |
| **Starting-position control** on every read: earliest, latest, at-timestamp, at-offset, last-N | Kinesis Data Viewer, Redpanda time-travel | Yes, essential. Maps to `offset=-1`, `offset=now`, and a returned offset. Chronicle offsets are opaque strings, so at-timestamp may not be directly supported. |
| Message inspector that auto-detects and pretty-prints encodings (JSON, hex, text) and shows full metadata | Redpanda, Kafdrop | Yes. Chronicle is content-type-aware (JSON framing vs raw bytes), and binary in SSE is base64. A hex view matters for octet-stream. |
| Real query surface for filtering: a filter bar plus JavaScript, jq, or SQL predicates over the stream | Conduktor, Redpanda | Yes, as a later feature. The test UI already has a JS expression box, so the pattern is proven against this protocol. |
| **Live tailing as a first-class toggle**, with refresh or throttle control and pause, resume, and auto-scroll | AKHQ Live Tail, RedisInsight auto-refresh | Yes, essential. Built on `live=sse` or `live=long-poll`. |
| Consumer or subscription view: members, committed vs first/last offset, lag, idle time, pending list | Provectus, Kafdrop, RedisInsight | Yes, adapted. Maps to subscriptions: links with acked vs tail offset, has-pending flag, lease state, generation, phase. |
| Consumer operations: reset offsets, ACK and CLAIM pending, purge or trim | Redpanda, RedisInsight, NATS | Yes, adapted. Maps to the `/__ds/` claim, ack, release, and stream add or remove operations. |
| Produce or publish panel: send a test message with chosen encoding | Provectus, RabbitMQ, NATS | Yes. A `POST` to a stream, with optional producer id, epoch, and seq for idempotency. |
| Non-destructive inspection (peek without committing) | RabbitMQ Get-with-requeue | Yes, and natural. Chronicle catch-up reads never advance any server-side cursor; reading is inherently non-destructive. |
| Metadata or schema panel that caches structure to keep browsing off the hot path | DBeaver, Redpanda registry | Partly. Chronicle has no schema registry, but stream config (content type, TTL, closed, fork lineage) is metadata worth caching via `HEAD`. |
| Export the current result set preserving filters and ordering | DBeaver Data Transfer | Yes, as a later feature. Export a read range as JSON, NDJSON, or raw bytes. |
| Per-server and per-stream health and metrics tiers | Conduktor, Kinesis, RabbitMQ | Yes. Chronicle exposes Prometheus metrics, health, and readiness endpoints to surface. |
| Cross-linking between objects (topic to subscription) | GCP Pub/Sub | Yes. Link a stream to the subscriptions whose pattern or list matches it. |

Three patterns from cloud consoles are worth noting as contrast. Kinesis and GCP Pub/Sub have no host-and-port connect step because the connection is implicit through cloud account and IAM. Chronicle is the opposite case: the host and port is the whole connection, which is why the DBeaver connection-manager model fits better than the cloud-console model.

## 5. Requirements for the new UI

### MVP (the must-haves that close the gap)

1. **Connection manager.** Add a server by URL and port. Support a base path so non-default stream roots work. Provide a **Test Connection** action before saving (a `HEAD` or harmless `GET`, with a clear success or failure result). Save many named connections. Show a connection-state dot and the last-used time. This single feature is the reason for the project.
2. **Auth and transport options on the connection.** Support HTTPS, an optional bearer token or basic-auth header, and a way to set custom headers, since auth lives in a proxy in front of chronicle. Do not assume plain HTTP to localhost.
3. **Stream list and navigator tree.** Show the streams on the connected server in a left rail, grouped by path prefix. Include an instant filter bar. Decide and implement a discovery method (see Open questions) and make it explicit in the UI.
4. **Stream detail and message browser.** Open a stream into a center grid of messages showing offset and decoded payload, with per-message metadata. Read with a **starting-position control**: beginning (`offset=-1`), tail (`offset=now`), or a pasted offset. Show stream metadata from `HEAD` (content type, next offset, TTL, closed flag, fork lineage).
5. **Content-aware rendering and a raw or hex view.** Pretty-print JSON, show text as text, and offer a hex or raw view for binary (octet-stream), decoding base64 from SSE where needed.
6. **Live tail toggle.** A first-class on and off control built on `live=sse` (with `live=long-poll` as a fallback), with pause, resume, and auto-scroll, and visible reconnect and backoff state.
7. **Create, append, close, and delete.** A create-stream form (content type, optional TTL or expiry), a write or publish panel (with optional producer id, epoch, and seq), a close action (`Stream-Closed: true`), and a delete action (with a clear warning about soft-delete when forks exist).

### Later (high value, not required to close the gap)

8. **Subscription and webhook console.** List subscriptions; show type (webhook or pull-wake), phase, generation, lease TTL, and per-link acked vs tail offset with the has-pending flag. Surface FENCED outcomes clearly.
9. **Subscription operations.** Create and delete subscriptions, add or remove explicit streams, and run the pull-wake lifecycle (claim, ack, release) from the UI for debugging. Show the JWKS keys for webhook verification.
10. **Query-grade filtering.** A filter expression bar, then optional JavaScript or jq predicates over the message stream, matching the proven test-UI JS box and the Conduktor and Redpanda pattern.
11. **Export.** Export the current read range, preserving the active filter and order, as JSON, NDJSON, or raw bytes.
12. **Fork awareness.** Show fork lineage (source path, fork offset) on a stream and let a user create a fork.
13. **Health and metrics.** Surface the server's Prometheus metrics, health, and readiness state per connection as a small overview.
14. **Saved views.** Persist favorite positions and filters per stream.

### Recommended information architecture

- **Connections start screen.** Saved servers as rows or cards (name, URL and port, environment badge, last-used, state dot). A **New Connection** wizard with URL, port, base path, and auth tabs, plus Test Connection. Folders or workspaces to group connections.
- **Left rail navigator** (per connected server): Server root, then stream paths grouped by prefix, then a separate Subscriptions section, and a small Server section for health and metrics. Instant filter bar at the top; right-click menus that adapt to the selected node.
- **Server overview tab.** The landing page after connecting: reachable state, stream count, and a metrics and health summary.
- **Stream detail (tabbed center pane):** Messages, Config or Metadata, Subscriptions-that-match, and Fork lineage.
- **Messages tab** is the data grid plus inspector: a toolbar with the starting-position selector, a filter bar, a format selector, and the Live Tail toggle; a grid of messages; and a right-side inspector panel showing the decoded payload, raw or hex, and metadata.
- **Subscriptions section.** A list with type, phase, and lease state; drill-in shows per-link acked vs tail offset, has-pending, generation, and the claim, ack, and release actions.
- **Publish panel.** Invoked from a stream's toolbar; choose content, optional producer id, epoch, and seq.
- **Global chrome.** A top-level server switcher, a search across streams, and an always-visible live-tail indicator.

### A note on packaging

The new UI should be a real, buildable package inside the chronicle monorepo, not a demo. The monorepo is Go-workspace based (root `go.mod`, plus `loadgen/go.mod`), with one existing Node and TypeScript surface, the Astro docs-site. It is not pnpm, turbo, nx, or lerna. A new front-end package would be a fresh Node and TypeScript app sitting beside `docs-site`. Whether chronicle should also serve the built UI from the Go binary, or whether it stays a separate static app, is an open decision (below).

## 6. Open questions and unknowns

These are the things the research could not settle. They should be answered before or early in design.

- **Stream discovery.** The protocol has no list-all-streams endpoint. The upstream UI reads a `__registry__` stream. It is unknown whether chronicle maintains any registry, whether it offers any listing or prefix-scan capability beyond the spec, or whether the UI must maintain its own registry stream. This shapes the entire navigator tree and is the single biggest open question. (Flagged: not determinable from the provided research.)
- **At-timestamp reads.** Offsets are opaque lexicographic strings. The Kinesis-style "start at timestamp" control may not be supported by chronicle's offset scheme. Whether time-based positioning is possible at all is unknown.
- **UI-to-server authentication in practice.** Auth is out of protocol scope and expected from a proxy. What auth chronicle deployments actually sit behind (bearer tokens, forward-auth, mTLS) is not specified, so the connection manager's auth tabs are a guess until a real deployment is named.
- **Whether the Go binary should serve the UI.** Chronicle ships no UI today and exposes only the protocol API plus metrics. Whether the new UI should be embedded and served by the binary, or shipped as a separate static app, is a packaging decision the research does not resolve.
- **Reusing upstream client libraries.** The test UI uses `@durable-streams/client` and `@durable-streams/state`. Chronicle does not import any durable-streams package; it only consumes the npm conformance suite. Whether the new UI should depend on `@durable-streams/client` (faster) or speak raw HTTP (zero coupling, full control) is undecided. Either is viable since the wire protocol is plain HTTP.
- **CORS.** A browser app calling a chronicle server cross-origin needs CORS headers. Whether chronicle sets any CORS headers, or whether the UI must be served same-origin or behind a proxy, is unknown and affects whether a pure client-side SPA can work against a remote server.
- **Scale of message reads.** The grid and live tail need a paging and virtualization strategy for high-volume streams. The research notes the test UI uses react-virtual, but the right read-batch size and back-pressure behavior against chronicle specifically is untested here.
- **CDN and cursor behavior.** The `Stream-Cursor` header and `cursor=` parameter exist to avoid CDN request collapsing. Whether the UI must manage cursors itself for correct long-poll behavior in front of a CDN is a detail that needs confirmation.