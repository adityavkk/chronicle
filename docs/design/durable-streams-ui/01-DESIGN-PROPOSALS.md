# Durable Streams UI — design proposals

A purpose-built, DBeaver-style UI for Durable Streams, shipped as a package inside the
chronicle monorepo. You run it, it opens a friendly desktop-like console, and you point it
at any Durable Streams server by `url:port` to browse and operate streams.

This document proposes three directions and recommends one. The prior-art research that
backs it is in [`00-PRIOR-ART.md`](./00-PRIOR-ART.md). The local spin-up of the existing
upstream UI against chronicle is written up in [`02-DEMO-NOTES.md`](./02-DEMO-NOTES.md).

## What we are solving

- chronicle ships **no UI of its own**. It only serves the protocol API on `:4437` plus a
  Prometheus `/metrics`, `/healthz`, and `/readyz` on the metrics listener.
- The "default UI" you were unhappy with is the upstream `examples/test-ui`
  (`@durable-streams/example-test-ui`, a Vite + React 19 + TanStack single-page app).
- Its central defect is exactly your complaint: the server URL is **hard-coded** to
  `http://${window.location.hostname}:4437` in three source files
  (`src/routes/__root.tsx`, `src/routes/stream.$streamPath.tsx`, `src/lib/stream-db-context.tsx`).
  There is no host or port input, no settings panel, and no environment variable. You cannot
  point it at an arbitrary server without editing the source and rebuilding.
- We confirmed it does run against chronicle on the default port (because chronicle also
  listens on `:4437`), but only by luck of the port, and only as a single-stream "chat"
  view. It is a demo, not an admin console.

### Other gaps in the existing UI (beyond the hard-coded URL)

- No connection manager and no saved connections. One server, one port, no profiles.
- Stream discovery depends on a `__registry__` convention stream that the UI itself writes
  into. There is no real "list every stream on this server" view.
- No message inspector. You see a running text or JSON feed, not a row-per-message grid with
  offset, size, headers, and a decoded detail panel.
- No offset query surface. You get a scrubber, not "read from earliest / latest / this
  offset / this timestamp / last N".
- No view of the `__ds` subscription control plane at all: no subscriptions, no consumers,
  no wakes, no lease or generation state, no lag.
- No metrics view, even though chronicle exposes Prometheus metrics.
- Demo-grade: `private: true`, version `0.1.0`, no auth, no production target.

## What a Durable Streams server actually exposes (the model the UI must speak)

This shapes the whole UI, so it is worth stating plainly.

- **Streams**: `PUT` create or fork, `POST` append or close, `GET` read, `HEAD` metadata,
  `DELETE` remove, on `/v1/stream/{path}`. Reads come in three modes: catch-up,
  long-poll, and Server-Sent Events. Every response carries `Stream-Next-Offset` for resume.
- **Offsets**: opaque but lexicographically sortable strings. `-1` means the beginning,
  `now` means the current tail. This is the stream version of a `WHERE` clause and the UI
  should expose it directly.
- **Content modes**: a stream has a content type. JSON mode frames and counts messages;
  binary and text are byte ranges. The UI must render all three well.
- **Producers**: idempotent writers identified by `(Producer-Id, Producer-Epoch, Producer-Seq)`
  with epoch fencing. A produce panel should let you set these.
- **Forks**: a stream can fork from another at an offset and then diverge. The UI should show
  fork lineage.
- **Subscriptions (`__ds` control plane)**: durable cursors over one or many streams
  (by glob pattern or explicit list), delivering wakes by webhook or by pull-wake, with
  generation fencing and leases. Each subscription exposes its links with `acked_offset`,
  `tail_offset`, and `has_pending`. This is the consumer-group analog and deserves a
  first-class view.
- **Observability**: Prometheus `/metrics`, plus `/healthz` and `/readyz`.

### One important finding: there is no native "list all streams" endpoint

Discovery today is by the `__registry__` convention, which is fragile. Because chronicle is
your own server, the strongest fix is to **add a small admin listing API to chronicle**
(for example `GET /__ds/streams` backed by a Redis `SCAN` over stream keys), and have the UI
use it for the navigator tree. The UI can fall back to the `__registry__` convention for
servers that lack the admin API. This is a design decision to make early because it changes
how the left-rail tree is populated.

## The three directions

### Direction A — "Fork and fix" the existing UI

Bring `examples/test-ui` into the monorepo as a package, then make the smallest set of
changes that remove the pain: add a connection bar (`url:port` input plus a few saved
connections and a "Test Connection" button), route every request through that connection
context instead of the three hard-coded constants, and keep its good parts (the live tail,
the JSON rendering, the resume-from-offset behavior).

- **Delivery**: build the SPA and embed it in a `chronicle ui` subcommand using Go
  `//go:embed`, so one Go binary serves the static UI. No Node needed at deploy time.
- **Effort**: smallest. Days, not weeks.
- **Downside**: you inherit the test-ui's information architecture, which is a single-stream
  chat view, not a DBeaver-style console. You fix the URL problem but you do not get the
  navigator tree, the message grid and inspector, the offset query bar, or the subscription
  views. It is a patch, not the console you described.

### Direction B — Purpose-built DBeaver-style web console (recommended)

A new front-end package built around the DBeaver pattern, served as a single Go binary.

- **Delivery**: a new Go command `cmd/dsui` that serves a built React SPA via `//go:embed`.
  Running `chronicle ui --listen :4438` (or a standalone `dsui` binary) starts the console
  and can auto-open the browser. The UI is decoupled from any one data server, so a single
  UI binary can manage many Durable Streams servers, exactly like DBeaver manages many
  databases.
- **Connection model (the core fix)**: a start screen lists saved connections as cards
  (name, `url:port`, environment badge, last-used, a live status dot). A "New Connection"
  wizard takes `url:port` plus optional auth and TLS, with a "Test Connection" step before
  save. Connections persist locally (browser storage for the web build, or a small config
  file written by the Go server). You can target any server, any port, any host.
- **Information architecture**:
  - **Left rail — navigator tree**: `Server → Streams → {stream} → (Forks, Producers)`,
    plus `Subscriptions` and `Metrics`, with an instant filter box and right-click actions.
  - **Center — tabbed workspace per stream**: `Messages | Config & Headers | Forks | Live Tail`.
  - **Messages tab — the data grid plus inspector**: a row per message showing offset, size,
    timestamp, and (in JSON mode) a key preview, with a right-side inspector that
    pretty-prints JSON, shows raw or hex for binary, and lists all headers. A top toolbar
    carries the **starting-position selector** (earliest, latest, at-offset, at-timestamp,
    last N), a **filter expression** box, a format selector, and a **Live Tail** toggle with
    pause and resume.
  - **Subscriptions view**: list of subscriptions with type (webhook or pull-wake), phase
    (idle, waking, live), generation, lease, and pending state; drill in to see per-stream
    `acked_offset` vs `tail_offset`, lag, and the wake history; actions to create, edit
    streams, and delete.
  - **Produce panel**: send a test message with a chosen content type, headers, and the
    idempotency triple, invoked from a stream's toolbar.
  - **Metrics tab**: read chronicle's Prometheus endpoint and chart throughput, lag, and
    health.
- **Tech**: Vite, React, TypeScript, TanStack Query and Router, a component kit such as
  shadcn or Radix for the desktop-like density, and the `@durable-streams/client` library
  for the protocol so we do not re-implement offset and SSE handling.
- **Effort**: medium. This is the real "amend the default" answer.
- **Downside**: it is a genuine build, not a patch, so it is the slowest of A and B to a
  first usable version, and it benefits from the small chronicle admin listing API described
  above to make stream discovery first-class.

### Direction C — Desktop app (most DBeaver-like)

The same console as Direction B, but packaged as a Tauri desktop application: a Rust shell
hosting the React front-end, giving native windows and menus, native saved-connection
storage, and no browser CORS limits because requests are native.

- **Delivery**: a desktop binary per platform; optionally the same front-end can still ship
  as the web build from Direction B.
- **Effort**: highest.
- **Downside**: it adds a Rust and Tauri toolchain to a Go and TypeScript repository, which
  is real maintenance and build-pipeline cost. I would only choose this if a downloadable
  desktop app is a hard requirement rather than a web console you open in a browser.

## Recommendation

**Direction B.** It is the design you actually asked for: a friendly, DBeaver-style console
that connects to any server by `url:port`, browses streams in a navigator tree, inspects
messages in a grid, tails live, and operates subscriptions. It fits the Go monorepo cleanly
because the front-end is built once and embedded in a Go binary, so "deploy or run it" is a
single command with no Node dependency at runtime. It avoids the desktop toolchain cost of
Direction C. It reuses the upstream client library and the strong live-tail ideas from the
existing UI without inheriting its single-stream layout.

A sensible build order: start with the connection manager and the navigator tree (the parts
that fix your complaint), then the message grid and inspector with the offset query bar, then
live tail, then the subscriptions view, then metrics. Add the chronicle `GET /__ds/streams`
admin listing early so the tree is first-class.

## Proposed package layout in the monorepo

```
chronicle/
  cmd/dsui/                 # Go command: serves the embedded SPA, optional browser auto-open
    main.go                 #   flags: --listen, --server (default target), --open
    embed.go                # //go:embed dist/* — the built front-end
  ui/                       # the front-end package (Vite + React + TS)
    src/
      connections/          # connection manager, saved profiles, Test Connection
      lib/ds-client.ts      # wraps @durable-streams/client, bound to the active connection
      navigator/            # left-rail server -> streams -> ... tree
      messages/             # data grid + inspector + offset query bar + live tail
      subscriptions/        # __ds control-plane views
      metrics/              # Prometheus charts
    package.json
    vite.config.ts
  store/                    # (existing) add GET /__ds/streams admin listing here, optional
```

## Open questions for you

1. **Direction**: confirm B, or pick A (fast patch) or C (desktop).
2. **Delivery**: a `chronicle ui` subcommand on the existing binary, or a separate `dsui`
   binary? Both are easy; the subcommand is the most "run it and a UI appears".
3. **Stream discovery**: are you willing to add the small `GET /__ds/streams` admin endpoint
   to chronicle for a first-class navigator tree, or should the UI stay on the `__registry__`
   convention only?
4. **Scope of the first cut**: smallest useful version is the connection manager plus the
   navigator tree plus the message grid with the offset query bar. Should live tail and the
   subscriptions view be in the first cut or the second?
