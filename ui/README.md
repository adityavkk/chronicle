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
- Follow a stream's tail live, by long-polling or over Server-Sent Events, with
  a connection-status badge, pause/resume, a stick-to-bottom auto-scroll, and a
  stop control — messages appear as they arrive.
- Render each message according to its content type: JSON elements pretty-print,
  text shows verbatim, and binary shows as a hex dump.
- Create a stream (PUT) with a chosen content type and optional TTL / expiry /
  "create closed", publish a message batch (POST) with an optional idempotent
  producer and an atomic "close after sending", close a stream, delete a stream
  (behind a confirm), fork a stream at an offset, and refresh a stream's
  metadata (HEAD).
- See the exact equivalent `curl` command for every operation, next to the
  control and under the hood, and copy it in one click.
- Bootstrap the whole API from a Playground of one-click presets on a safe
  `playground/…` sample namespace (create, publish, demo producer, live tail,
  fork, close, delete).
- Manage the server's reserved `__ds` subscription control plane: create a
  webhook or pull-wake subscription (glob pattern and/or explicit streams, lease
  TTL), view a subscription's detail (type, phase, generation, lease, and its
  linked streams with their acked/tail offsets and pending state), add or remove
  explicit stream links, and delete it. For a webhook subscription, see the
  delivery URL, the signing key id, and a link to `/__ds/jwks.json`, plus the
  exact ack-callback curl. For a pull-wake subscription, claim a lease, ack
  offsets (with done to release or as a heartbeat), and release — with the
  `409 FENCED` / `ALREADY_CLAIMED` cases surfaced as clear warnings. Because the
  control plane has no list-all endpoint, dsui remembers the subscription ids you
  create or track, per connection, in the browser.
- Scrape and read the server's Prometheus metrics from the separate
  `--metrics-listen` endpoint: a curated set of key fan-out / wake / claim
  counters plus the full list of metric families.
- Watch the whole wake loop in a dual split-screen Wake Monitor: publish on a
  source stream on the left and see the resulting wake fire on the right —
  either a webhook subscription's signed delivery (captured by the dsui binary
  and relayed live, since a browser cannot host an inbound webhook) or a
  pull-wake subscription's `wake_stream` tailed as it grows — with the signature
  parts (kid + a JWKS link), the woken streams and their offsets, and one-click
  ack / claim. A one-click "Wake demo" sets the whole thing up against a sample
  stream.
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
  instant filter, a manual-add box, a "New stream" button, the Playground, the
  Subscriptions section (the tracked-ids list, a "track existing id" box, and a
  create button), and a Metrics entry that switches the center pane.
- `MessagesWorkspace` — the center messages view: the read-mode + start toolbar,
  the publish composer, the message grid (or the live tail), the pager, and the
  "Under the hood" protocol disclosure.
- `SubscriptionWorkspace` — the center detail view for a selected subscription:
  its type / phase / generation / lease metadata, the linked-streams table
  (path · link type · acked → tail · pending) with add/remove controls, Delete,
  and the type-specific controls (a webhook URL + JWKS link + ack-callback curl,
  or the pull-wake Claim / Ack / Heartbeat / Release worker controls).
- `MetricsWorkspace` — the center metrics view: the `--metrics-listen` URL input
  and a Scrape button, a curated key-metric tile grid, and the full list of
  metric families.
- `WakeMonitorWorkspace` — the center dual split-screen for one subscription: a
  source-stream pane on the left (a compact publish composer over the selected
  linked stream, plus its live tail) and a wake-timeline pane on the right (the
  captured webhook deliveries — timestamp, wake id / generation, woken streams
  and offsets, the parsed signature with a JWKS link, and an Ack control — or
  the tailed pull-wake `wake_stream` events with claim / ack). Publishing on the
  left visibly cues the right pane so the publish → wake → hook → ack loop reads
  in one glance.
- `CreateSubscriptionDialog` — the modal to create a subscription (webhook vs
  pull-wake, glob pattern and/or explicit streams, lease TTL, the type-specific
  target), with live validation and a copy-as-curl preview.
- `Inspector` — the right panel (messages view only): the decoded value, the raw
  bytes, and the captured response headers for the selected row.
- `ProtocolPanel` — the collapsible HTTP transcript and protocol primer, plus a
  "Live connection" block while a tail is open. Every center view ends in one,
  fed by the last captured exchange.

The write, fork, live-tail, and Playground feature components are:

- `CreateStreamDialog` — the modal form to create a stream (path, content type,
  and an Advanced disclosure for TTL / Expires-At / "create closed").
- `PublishComposer` — the content-type-aware publish editor under the toolbar (a
  JSON batch editor, a text area, or a binary text/base64 input), with an
  idempotent-producer disclosure and a "close after sending" checkbox.
- `ForkDialog` — the modal form to fork a stream at an offset (and optional
  sub-offset) into a new stream.
- `StreamActionsMenu` — the workspace-header popover for per-stream lifecycle:
  Fork, Refresh metadata, Close, and Delete (behind a confirm).
- `TailPanel` — the live view shown in place of the paged grid while a tail is
  open: a connection-status badge, Pause / Resume / Clear / Start / Stop, a
  stick-to-bottom auto-scroll, and a buffered / aged-out footer.
- `Playground` — a Navigator section of one-click presets that bootstrap the
  whole API on a safe `playground/…` sample namespace.

The shared UI primitives are:

- `Modal` — the accessible dialog shell (focus move-in + restore, focus trap,
  Escape / backdrop close) used by the create and fork dialogs.
- `CurlPreview` — the collapsed "Equivalent curl" disclosure shown next to every
  operation, with one-click copy.
- `Toaster` — the transient-notification stack (success / info / warning / error)
  with per-toast auto-dismiss and an optional inline action.
- `StatusDot`, `CopyButton`, `icons` — small shared pieces.

## The Durable Streams protocol dsui speaks

A Durable Streams server exposes append-only byte streams over plain HTTP. dsui
reads streams, follows them live (long-poll / SSE), and performs the full
write/fork/lifecycle surface (create, publish, close, delete, fork). The parts
of the protocol it uses are:

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

## The stream operations

dsui can do every operation a Durable Streams server supports, not just read.
Each operation is one HTTP request. The UI builds the request, sends it once
through the client, and shows you the result and the exact equivalent curl. None
of these throw. A failure becomes a clear message in a toast, and the failed
request still shows up in the "Under the hood" panel.

- **Create a stream.** The "New stream" button in the Navigator opens a dialog.
  You give the stream a path and pick its content type (text, JSON, or binary).
  An Advanced section lets you set a time to live, an expiry time, or create the
  stream already closed. The request is a `PUT` to the stream URL. The server
  fixes the content type at creation, so you choose it once here.

- **Publish a batch of messages.** When a stream is selected, a publish editor
  sits under the toolbar. It changes shape to match the stream's content type. A
  JSON stream gets an editor for a JSON array, where each element is one message,
  with a live count and a "Format JSON" helper. A text stream gets a plain text
  box. A binary stream gets a box that accepts UTF-8 text or base64, and base64
  is decoded to the exact bytes before sending. The request is a `POST` to the
  stream URL.

- **Use an idempotent producer.** Inside the publish editor, an optional section
  lets you send a producer identity with the append: a producer id, an epoch, and
  a sequence number. The epoch fences out older producers that share the id, and
  the sequence number lets the server drop duplicates and keep order. After a
  successful publish the UI advances the sequence number for you, so the next
  publish from the same producer is the next number in line. If the server
  rejects the append because the sequence did not match, the UI shows a warning
  with the number the server expected and the number it received.

- **Close after sending, or close on its own.** The publish editor has a "close
  stream after sending" checkbox that appends your batch and closes the stream in
  the same request. You can also close a stream on its own from the stream
  actions menu in the workspace header. A closed stream takes no more messages.

- **Follow a stream live.** The toolbar has a mode picker: Catch-up, Long-poll,
  or SSE. Catch-up is the normal paged read. The two live modes open a tail that
  shows new messages as they arrive. Long-poll repeats a `GET` that the server
  holds open until there is new data. SSE opens a browser EventSource. Both show
  a connection-status badge, and you can pause buffering without dropping the
  connection, clear what you have, and stop the tail. The live view keeps a
  bounded buffer, so a fast stream cannot grow memory without limit. When the
  buffer is full the oldest messages age out, and the footer tells you how many.

- **Fork a stream.** The stream actions menu has a "Fork" item, and the dialog is
  seeded from where your current read ended. A fork is a new stream that inherits
  the source stream's data up to an offset you choose, then goes its own way. The
  request is a `PUT` that carries the source path and the fork offset.

- **Delete a stream.** The stream actions menu has a "Delete" item behind a small
  confirm step. The request is a `DELETE`. The server soft-deletes the stream if
  forks depend on it, and removes it entirely otherwise.

- **Refresh a stream's metadata.** The stream actions menu can send a `HEAD` to
  the stream. This updates the content type, the closed flag, and the next offset
  without reading the body, and the result flows into the "Under the hood" panel.

## The Playground

If you open dsui against an empty server, the stream list is empty and there is
nothing to click. The Playground solves that. It is a section in the Navigator
with one button per operation, and each button runs the real operation against a
sample stream named `playground/demo`. Nothing about the Playground is a special
code path. Each button calls the same store action the rest of the UI uses, so
what you see is exactly what happens when you do it by hand.

The presets are: create the sample JSON stream, publish a sample batch, run a
demo producer that sends five messages a short time apart so a live tail visibly
updates, tail the stream live over SSE, fork the stream at its latest offset,
close the stream, and delete the stream to reset the playground. A separate
one-click "Wake demo" row creates a sample stream, registers a webhook
subscription pointed at the dsui binary's own capture endpoint, publishes a
message, and opens the Wake Monitor so a newcomer sees a wake fire end to end.
Every preset works only on the `playground/…` sample namespace, so it can never
touch your real streams.

Each preset also explains itself before you run it. It shows a plain-language
line that says exactly what the request will do, and it shows the exact
equivalent curl. The curl is built from the same code that builds the real
request, so it is honest even when there is no server connected yet. A first-run
hint in the empty stream list points you at the Playground.

## The Wake Monitor

A subscription's job is to *wake* something when a stream it watches gets new
data. That loop is normally invisible — a message lands, the server fires a wake,
a hook runs somewhere else. The Wake Monitor makes the whole loop visible on one
screen. Open it from a subscription's "Watch wakes" action, and the center pane
splits in two:

- **Left — the source.** Pick one of the subscription's linked streams, publish a
  message into it with a compact composer, and watch the stream's own live tail.
- **Right — the wake timeline.** The wakes that the subscription produces, newest
  last, as they arrive.

Publishing on the left cues the right pane that a wake should be on its way, so
you see the causal chain — message in, wake out, hook invoked, acked — in one
glance rather than across two terminals.

The right pane shows whichever of the two wake mechanisms the subscription uses:

- **A webhook subscription** has chronicle POST a signed notification to a URL.
  A browser cannot host that URL, so the dsui binary hosts a small capture
  endpoint (`/__hooks/{id}`); the subscription's `webhook_url` points at it, the
  binary buffers each delivery, and relays it to the browser over SSE. The
  timeline shows each delivery's timestamp, the `wake_id` and generation, the
  woken streams and their offsets, and the parsed `Webhook-Signature`
  (timestamp, key id, and the Ed25519 value) with a link to the server's
  `/__ds/jwks.json`. dsui shows the signature parts but does not verify them —
  verification is asymmetric Ed25519 against the JWKS, and the link is there so
  you can. An Ack control drives the subscription's ack-callback.
- **A pull-wake subscription** has chronicle append wake events to a durable
  `wake_stream` that workers claim. The timeline tails that stream live over SSE
  and renders each wake event (the woken stream, generation, timestamp), with the
  Claim / Ack worker controls from the subscription view.

The fastest way to see it work is the Playground's one-click **Wake demo**, which
creates a sample stream, registers a webhook subscription pointed at the capture
endpoint, publishes a message, and opens the monitor — so the first wake animates
in without any setup. As with subscriptions generally, real webhook delivery
needs a Redis-backed chronicle with subscriptions enabled and the dsui *binary*
running (not `vite dev`), because only the binary can host the capture endpoint;
the monitor says so plainly when the capture endpoint is unavailable.

## Copy as curl

Every operation in dsui can be copied as a curl command. The point is that you
can learn the protocol by reading the command, paste it into a terminal to run
the same request by hand, or drop it into a script or a bug report.

The command is the exact request. It is not a rough sketch. The method, the URL
with its query string, and every header appear in the order the UI sends them. A
text body is shown as `--data-raw` with the body quoted for the shell. A binary
body cannot be put on one line safely, so the command reads the bytes from
standard input with `--data-binary @-` and a note of the byte count, rather than
corrupting the payload. An SSE tail adds curl's `-N` flag so the stream is not
buffered.

You see the curl in three places. It sits next to each form before you submit,
so you can preview what a control will send. It sits next to each Playground
preset. And it sits in the "Under the hood" panel for the request that actually
ran. The first two come from a preview of the request the UI is about to send;
the last comes from the request it already sent. Both produce the same command
for the same operation.

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
`defaultServer` value from the binary's `--server` flag plus the `captureBase`
the Wake Monitor uses. dsui fetches that file on load and prefills `defaultServer`
as a connection if it is set, so a packaged build can point at a known server out
of the box. The `--server` flag only prefills a connection; the UI can connect to
any server you type in regardless.

The binary also hosts the **webhook-capture endpoint** the Wake Monitor relies on
(`POST /__hooks/{id}` to receive a delivery, `GET /__hooks/{id}/stream` to relay
it to the browser over SSE, `GET /__hooks/{id}` to list the recent buffer). This
is a tool feature, not part of the Durable Streams protocol — it exists only
because a browser cannot host an inbound webhook. A webhook subscription's
`webhook_url` points at `<captureBase>/__hooks/<id>`; `captureBase` defaults to
`http://localhost<port>` and is overridable with `--capture-base` when chronicle
must reach this binary at a different address. The capture buffer is in-memory
and bounded, so it survives only as long as the binary runs.

The build flow end to end:

```
ui/ source ──vite build──▶ cmd/dsui/embedded ──go:embed──▶ dsui binary ──serves──▶ browser
```

## A note on subscriptions and a live server

The subscription control plane requires chronicle's Redis backend (`-store
redis`); the in-memory store rejects subscriptions. So the Subscriptions panel
is built and tested against a mocked client (the parsers handle 4xx / 5xx /
network failures and the `409` fencing path is covered by unit tests), not
against a live `__ds` server — there may not be one available in every
environment. Point dsui at a Redis-backed chronicle server to exercise it for
real. Metrics are served on a separate listener (the `--metrics-listen`
address), so you enter that `/metrics` URL in the Metrics view; it is remembered
per connection.

## Coming next

The Navigator's read/write/tail surface, the Subscriptions panel, the Metrics
view, and the Wake Monitor are all live. The remaining planned addition is:

- **A real stream-list endpoint.** Discovery today reads the `__registry__`
  stream. A dedicated `GET /__ds/streams` listing endpoint would replace the
  registry reduction with a direct, paginated list of streams.
