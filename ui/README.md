# dsui — the Durable Streams UI

dsui is a small web console for Durable Streams servers. You point it at a
running server, it discovers the streams that server holds, and you can read any
stream, page through its messages, and inspect each message down to the exact
bytes and HTTP headers that came back over the wire. Think of it as a read-and-
inspect tool for streams, in the same spirit as a database browser for tables.

It is one HTML page plus a JavaScript bundle. There is no backend of its own.
The page talks straight to whatever Durable Streams server you give it, using
plain HTTP requests from the browser.

## What it does today

- Save one or more server connections (name, base URL, stream route) in the
  browser and switch between them.
- Probe each saved server to show whether it is reachable.
- List the streams a server holds, discovered from its `__registry__` stream.
- Add a stream by path even when the registry does not list it.
- Read a stream from the beginning, from the current tail, or from an offset you
  type, and page forward batch by batch.
- Render each message according to its content type: JSON elements pretty-print,
  text shows verbatim, and binary shows as a hex dump.
- Show, on demand, the real HTTP request and response that produced what you are
  looking at, with a plain-language explanation of the protocol headers.

## The lightweight philosophy

dsui keeps its runtime dependencies to exactly two packages: `preact` and
`@preact/signals`. That is the whole stack at runtime.

There is no React, no Tailwind, no shadcn, no component library, no router, no
state-management library, no data-fetching library, and no schema-validation
library. Networking is the browser's built-in `fetch`. Styling is hand-written
CSS driven by design tokens. Icons are hand-drawn inline SVG. Runtime type
checks are small hand-written guard functions.

The reason is that the job is small and stable, and a small job does not need a
large toolchain. Fewer dependencies means a smaller bundle, fewer moving parts
to break, and code that any contributor can read top to bottom. The downside is
real and worth stating plainly: anything a big framework would have given you
for free — a router, form helpers, a validation library, a component kit — you
write by hand here. That is a deliberate trade. If you find yourself wanting one
of those libraries, the answer is almost always to write the small piece you
actually need instead of pulling in the library.

## Architecture

The code is arranged in four layers, each one depending only on the layers below
it. From bottom to top:

### 1. Design tokens (`src/styles/`)

`tokens.css` defines the whole visual system as CSS custom properties: a neutral
gray ramp and one accent ramp in the `oklch` color space, plus spacing, radius,
type, shadow, and motion scales. Components never reach for a raw scale step.
They use semantic tokens like `--bg`, `--fg`, `--border`, and `--accent`, which
are derived from the ramps. Light is the default; dark is provided both through
`prefers-color-scheme` and through a manual `[data-theme="dark"]` attribute on
the `<html>` element, so a user's explicit choice always wins over the OS.

`base.css` sets element defaults, and `app.css` holds the component styles. They
are imported in token → base → app order so the cascade is predictable.

### 2. The client (`src/lib/dsClient.ts`)

`dsClient` is a typed wrapper around `fetch` bound to a single connection. It is
the only place that touches the network. It speaks the Durable Streams HTTP
protocol: it builds stream URLs, reads streams at an offset, lists streams from
the registry, and probes a server for reachability.

Every request it makes is captured as a typed `HttpExchange` record — method,
URL, request and response headers, status, timing, and the protocol-significant
headers pulled out for convenience. That captured record is what powers the
"Under the hood" disclosure, so the UI can show exactly what went over the wire
without making the request a second time.

The client is deliberately forgiving. A 404, an empty body, malformed JSON, or a
network failure all resolve to a typed result instead of throwing. The rest of
the app never needs a `try`/`catch` around a read.

Supporting `dsClient` are several pure, dependency-free helper modules in
`src/lib/`:

- `types.ts` — the shared typed contracts every other module depends on.
- `guards.ts` — hand-written runtime type guards and parsers (registry events,
  JSON arrays, content-type classification, previews).
- `protocol.ts` — the explanatory data and pure functions behind the protocol
  disclosure (header notes, offset primer, curl reproduction).
- `messages.ts` — turning toolbar choices into a concrete offset, plus timestamp
  and byte-size formatting for the grid.
- `validation.ts` — connection-form field validation.
- `format.ts` — small display helpers (status dots, relative time, compact URL).
- `config.ts` — loads the runtime config the Go binary serves.

### 3. The signals store (`src/state/store.ts`)

All application state lives in one module, built on `@preact/signals`. The store
holds the reactive state (saved connections, the active connection, the
discovered streams, the selected stream, the last read result, the last captured
exchange, the selected row, toolbar settings, theme, and so on), the derived
state computed from it (the active connection object, a client bound to it), and
the typed action functions that are the only sanctioned way to change anything.

Components read signals and call actions. Components do not mutate state
directly. Keeping mutation in one place is what makes the data flow easy to
follow: every change goes through a named action you can find in this one file.

The store also handles persistence. Saved connections, the active connection id,
and the theme are mirrored to `localStorage` through `effect`s, so a reload
restores your session. Everything else is ephemeral and re-derived when you
connect.

### 4. The app shell and feature components (`src/app.tsx`, `src/components/`)

`app.tsx` is the top-level layout. When there is no active connection it renders
the start screen. Otherwise it renders the three-pane workspace on a CSS grid:

```
┌───────────────────────── header ─────────────────────────┐
│ brand · connection switcher · theme toggle                │
├──────────┬─────────────────────────────┬─────────────────┤
│ Navigator│  MessagesWorkspace          │  Inspector      │
│  (left)  │  (center)                   │  (right)        │
└──────────┴─────────────────────────────┴─────────────────┘
```

The grid collapses responsively through CSS container queries: the inspector
drops away on a medium width, and the navigator drops away on a narrow width.

The feature components are:

- `StartScreen` — the landing view when no server is active. Lists saved
  connections as cards with live reachability dots, and offers the new-connection
  form.
- `ConnectionForm` — a controlled form with inline validation and a "Test
  connection" step that probes a candidate before you save it.
- `ConnectionManager` — the header switcher and theme toggle.
- `Navigator` — the left rail: the connected server, the stream list with an
  instant filter and a manual-add box, and the disabled "coming next" rows.
- `MessagesWorkspace` — the center: the read toolbar, the message grid with its
  pager, and the "Under the hood" protocol disclosure.
- `Inspector` — the right panel: the decoded value, the raw bytes, and the
  captured response headers for the selected row.
- `ProtocolPanel` — the collapsible HTTP transcript and protocol primer.
- `StatusDot`, `CopyButton`, `icons` — small shared pieces.

## The Durable Streams protocol dsui speaks

A Durable Streams server exposes append-only byte streams over plain HTTP. dsui
only reads; it does not create or append. The parts of the protocol it uses are:

- **Streams are URLs.** A stream lives at `{baseUrl}{streamRoot}/{path}`. The
  default stream route is `/v1/stream`, so a stream named `orders/created` on a
  local server is `http://localhost:4437/v1/stream/orders/created`.

- **Reads take an offset.** A read is `GET {streamUrl}?offset={cursor}`. An
  offset is an opaque cursor into the stream. Two values are reserved: `-1` means
  the beginning, and `now` means the current tail. Any other value is an opaque
  cursor the server handed you earlier.

- **Reads return a batch plus the next cursor.** A response body is one batch of
  messages. The response header `Stream-Next-Offset` is the cursor to send as
  `?offset=` on your next read to resume exactly where this batch ended. That is
  how you page forward. There is no per-message offset — only a batch and the one
  next cursor — so dsui is honest about this and identifies a message by its
  index within the batch, not by a fake per-message offset.

- **Two state flags.** `Stream-Up-To-Date` is present when a read reached the
  current tail and there is nothing newer right now. `Stream-Closed` is present
  when the stream has been closed and will never get more data.

- **Content type shapes the body.** `application/json` (or any `+json`) means the
  body is a JSON array of messages, rendered one row per element. `text/*` and
  similar render as text. Anything else renders as raw bytes in a hex dump.

- **Discovery via `__registry__`.** There is no list-all endpoint. The server
  publishes stream lifecycle events to a reserved stream named `__registry__`.
  dsui reads that stream from `-1`, reduces the events to the current live set of
  stream paths (a `deleted` operation removes a path; later events win), and
  shows the result as the stream list. If the registry is empty or absent, the
  list is honestly empty and you can add stream paths by hand.

## The progressive-disclosure feature

The center workspace ends in a collapsed `<details>` panel labelled "Under the
hood." It is closed by default so a newcomer is not buried in protocol detail.
Open it and it shows the real HTTP exchange that produced what you are looking
at, laid out as a transcript rather than a debug dump:

- The **request**: method, URL, the query parameters (with the offset called out
  as the cursor that was sent), and the request headers. A "Copy as curl" button
  reproduces the exact request.
- The **response**: the status and timing, then each protocol-significant header
  with its value — or an honest "not sent on this response" when the server did
  not send it — each paired with a one-line plain-language note explaining what
  it is for.
- An **offset primer**: a short explanation tailored to the offset this read
  actually used, and, when there is a next cursor, exactly what to send to read
  the next batch.

Because it renders from the captured exchange and never re-fetches, it is honest
about exactly what happened, including which headers were absent. The
explanatory text and classification all live in `src/lib/protocol.ts` as pure,
tested functions; the component only lays them out. The same protocol-header
detection also drives the Inspector's Headers tab, which marks the protocol
headers and sorts them to the top.

## Running in development

From this `ui/` directory:

```bash
npm install
npm run dev
```

`npm run dev` starts the Vite dev server on port 3001 with hot reloading. Open
the printed URL in a browser. In dev there is no Go binary serving the runtime
config, so the request for `/dsui-config.json` simply 404s and dsui starts with
no prefilled server. Add a connection by hand pointing at any Durable Streams
server you have running.

Other scripts:

```bash
npm run typecheck   # tsc --noEmit, strict
npm run lint        # biome check .
npm run format      # biome format --write .
npm run test        # vitest run
```

## How it ships

dsui has no server of its own. It ships embedded inside a Go binary so the whole
console is a single self-contained executable.

```bash
npm run build       # vite build → ../cmd/dsui/embedded
```

`vite build` emits the production bundle into `../cmd/dsui/embedded` (see
`vite.config.ts`, where `outDir` and `emptyOutDir` are set, and keep those in
sync with `cmd/dsui/main.go`). The Go binary in `cmd/dsui` embeds that directory
with `//go:embed` (see `cmd/dsui/embed.go`) and serves it as static files with a
single-page-app fallback to `index.html`.

When you run the resulting `dsui` binary it serves the UI on `:4438` by default
and opens a browser at it. It also serves `/dsui-config.json`, which carries the
`defaultServer` value from the binary's `--server` flag. dsui fetches that file
on load and prefills it as a connection if it is set, so a packaged build can
point at a known server out of the box. The `--server` flag only prefills a
connection; the UI can connect to any server you type in regardless.

The build flow end to end:

```
ui/ source ──vite build──▶ cmd/dsui/embedded ──go:embed──▶ dsui binary ──serves──▶ browser
```

## Coming next

The Navigator shows two disabled "coming next" rows (Subscriptions and Metrics),
and the workspace is read-and-page today. The planned additions are:

- **Live tail.** Follow a stream's tail as new messages arrive, using the
  server's long-poll and SSE live-tailing support, instead of pressing "Read next
  batch" by hand.
- **Subscriptions.** A panel for the server's reserved `__ds` subscription APIs
  (signed webhook delivery and pull-wake), so you can view and manage
  subscriptions from the console.
- **A real stream-list endpoint.** Discovery today reads the `__registry__`
  stream. A dedicated `GET /__ds/streams` listing endpoint would replace the
  registry reduction with a direct, paginated list of streams.
</content>
</invoke>
