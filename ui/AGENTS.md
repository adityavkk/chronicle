# Working in `ui/` — a guide for agents and contributors

This is the dsui front-end: a small Preact console for reading and inspecting
Durable Streams servers. Read the `README.md` next to this file first for what
dsui is and how it fits together. This guide is the rulebook for changing the
code without breaking the things that make it small and predictable.

## The hard dependency rules

The runtime dependency surface is exactly two packages, and it stays that way:

```json
"dependencies": {
  "@preact/signals": "^2.0.4",
  "preact": "^10.25.4"
}
```

**Never add any of these:**

- React or React DOM. This is Preact. `tsconfig.json` aliases `react` and
  `react-dom` to `preact/compat` only so third-party types resolve; do not write
  React imports in app code.
- Tailwind, or any CSS framework or utility-class system. Styling is hand-written
  CSS driven by design tokens.
- shadcn, Radix, Headless UI, or any component or headless-component library.
- TanStack Query, SWR, or any data-fetching library. Networking is the browser's
  `fetch`, wrapped once in `src/lib/dsClient.ts`.
- Zustand, Redux, Jotai, or any state-management library. State is one
  `@preact/signals` store in `src/state/store.ts`.
- zod, yup, valibot, io-ts, or any schema-validation library. Validation is small
  hand-written guards in `src/lib/guards.ts` and `src/lib/validation.ts`.
- Any icon package (lucide, heroicons, react-icons, …). Icons are hand-drawn
  inline SVG in `src/components/icons.tsx`.
- A router, a date library, a clipboard library, a UUID library, or similar. Each
  of those is already covered by a few lines of hand-written code.

If you think you need one of these, you almost certainly need the small piece of
it you actually use, written by hand. Adding a dependency to `package.json` is a
significant change that needs an explicit human decision, not a default.

New **dev** dependencies (build, lint, test tooling) are also a deliberate
choice, but a softer one. The current dev stack is Vite + the Preact preset,
TypeScript, Biome, and Vitest with jsdom and `@testing-library/preact`.

## TypeScript rules

The project is strict and stays strict. `tsconfig.json` turns on `strict` plus
`noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, `noUnusedLocals`,
`noUnusedParameters`, `noFallthroughCasesInSwitch`, and `noImplicitOverride`.

- **No `any`.** Biome enforces `noExplicitAny` as an error. When a value is
  genuinely unknown (a parsed response body, a stored JSON blob), type it as
  `unknown` and narrow it with a guard from `src/lib/guards.ts`.
- **No non-null assertions (`!`).** Biome enforces `noNonNullAssertion` as an
  error. Handle the `null`/`undefined` case instead.
- **`noUncheckedIndexedAccess` is on**, so indexing an array or record yields
  `T | undefined`. Check before use. This is why you see `?? fallback` after
  array reads throughout the code.
- **`exactOptionalPropertyTypes` is on.** An optional property is "absent" or "a
  real value," not "present and `undefined`." When building an object with an
  optional override, omit the key entirely rather than setting it to `undefined`
  (see `connectionInput()` in `ConnectionForm.tsx` for the pattern).
- Use `import type { … }` for type-only imports (Biome's `useImportType`), and
  `node:` protocol for any Node built-in (there are none in app code).
- Prefer typed results over throwing. The client and the parsers all resolve to
  typed outcomes so callers never need `try`/`catch`. Follow that pattern.

## File and module layout

```
ui/
  index.html              the single page; sets the no-flash theme before paint
  vite.config.ts          dev server (3001) + build outDir → ../cmd/dsui/embedded
  tsconfig.json           strict TS config
  biome.json              lint + format config
  vitest.setup.ts         test setup (matchMedia stub)
  src/
    main.tsx              entry: initStore() then render(<App />)
    app.tsx               app shell: start screen OR the three-pane grid
    state/
      store.ts            the ONE signals store: state, derived, actions, persistence
    lib/                  pure, dependency-free logic (no DOM, no store, no I/O)
      types.ts            shared typed contracts everything else depends on
      dsClient.ts         the only network code: fetch + captured HttpExchange
      guards.ts           runtime type guards + registry/JSON parsers
      protocol.ts         protocol-disclosure data + pure helpers
      messages.ts         offset resolution + grid formatting
      validation.ts       connection-form validation
      format.ts           display helpers (dots, relative time, compact url)
      config.ts           loads /dsui-config.json
      *.test.ts           unit tests for the pure modules
    components/           the Preact components (one per feature/piece)
    styles/
      tokens.css          design tokens (imported first)
      base.css            element defaults (imported second)
      app.css             component styles (imported third)
```

### The typed seams

These are the boundaries that keep the code easy to reason about. Respect them.

- **`lib/` is pure.** No DOM, no store import, no `fetch`, no side effects, except
  for `dsClient.ts` (the one place `fetch` lives) and `config.ts`. Everything
  else in `lib/` is plain functions over plain data, which is why it is unit
  tested directly. Keep new logic here when it is logic, not layout.
- **`dsClient.ts` is the only network code.** All HTTP goes through it, and every
  request is captured as an `HttpExchange`. Do not call `fetch` from a component
  or the store.
- **`state/store.ts` is the only mutation seam.** Components read signals and call
  the exported actions. Components do not write signals directly (component-local
  UI signals like a filter input are fine — see the next point). New
  cross-component state and the action that changes it go here.
- **Component-local state stays local.** A purely local concern — a filter box, a
  confirm toggle, which inspector tab is open — uses a local `useSignal` (or a
  module-level `signal` for a singleton like the inspector tab) and never touches
  the store. This keeps the global store about real shared state.
- **Components lay out; they do not compute.** Explanatory text, classification,
  formatting, and parsing live in `lib/`. A component imports those and arranges
  the result. `ProtocolPanel` and `Inspector` are the model: all their copy and
  logic comes from `lib/protocol.ts` and `lib/messages.ts`.

## How to add a new stream tab (a view in the center workspace)

The center workspace (`components/MessagesWorkspace.tsx`) is today a single
vertical stack: head, toolbar, grid, and the protocol disclosure. To add a
distinct view — say a "Schema" tab alongside the messages grid:

1. **Put the logic in `lib/`.** Write the pure functions the view needs (parsing,
   shaping, formatting) in a new or existing `lib/` module, with a `.test.ts`
   next to it. No DOM, no store.
2. **Add any shared state to the store.** If the view's state must survive a
   stream switch or be read elsewhere, add a `signal` and an action in
   `state/store.ts`. If it is purely local to the view, use a local `useSignal`.
3. **Build the view component** under `components/`, reading the relevant store
   signals and calling `lib/` functions. Match the existing component shape:
   typed props, small sub-components, no `any`, no `!`.
4. **Wire it into the workspace.** Add a small tab strip in `MessagesWorkspace`
   (the `Inspector` tab strip is the pattern to copy: a `Tab` union, a module
   `signal` for the active tab, and a switch in the render) and render your view
   when its tab is active.
5. **Style with tokens.** Add classes under the `dsui-` prefix in `app.css` using
   semantic tokens only. Do not introduce raw colors.

## How to add a new side panel (a section in the left rail)

The Navigator (`components/Navigator.tsx`) is a stack of
`<section class="dsui-nav__section">` blocks: the connected-server header, the
streams tree, and the "coming next" rows.

1. **Logic in `lib/`, shared state in the store** — same as above.
2. **Add a `<section class="dsui-nav__section">`** to the Navigator with your
   content. To promote one of the existing "coming next" affordances (the
   disabled Subscriptions/Metrics rows rendered by `ComingNext`), replace its
   `<ComingNext>` with a real, enabled section.
3. **Keep tree/list semantics real.** The streams list uses a true
   `role="tree"`/`role="treeitem"` with roving `tabindex` and arrow-key
   navigation, and Biome's a11y rules are on. Match that rigor for any new
   interactive list.

To add a whole new region to the shell (a fourth pane), edit the grid template in
`app.css` (`.dsui-main` and the `.dsui-region--*` rules plus the container-query
breakpoints) and drop a component into a new `<aside>`/`<section>` in `app.tsx`.

## Stream operations: the write / fork / live-tail seams

dsui is no longer read-only. The write, fork, lifecycle, and live-tail surface
follows the same layering — logic and contracts in `lib/`, the only `fetch` in
`dsClient.ts`, mutations only in the store — and adds these typed seams:

- **Operation descriptors + curl (`lib/types.ts`, `lib/curl.ts`).** An
  `Operation` is `{ method, url, headers, body? }` — a protocol-level
  description of one request, independent of whether it has run. `toCurl(op)` in
  `lib/curl.ts` turns one into the exact equivalent curl string (string bodies as
  `--data-raw`, binary as `--data-binary @-`). It is pure and unit-tested
  (`curl.test.ts`). Note there are two `toCurl`s: this one works from an intended
  `Operation`; the older `protocol.ts` one reproduces a completed `HttpExchange`.
  Import whichever matches what you have in hand.
- **Write methods on `DsClient` (`lib/dsClient.ts`).** `createStream`,
  `appendMessages`, `closeStream`, `deleteStream`, and `forkStream` each map their
  typed options onto the chronicle headers (Content-Type, Stream-TTL,
  Stream-Expires-At, Stream-Closed, Producer-Id/Epoch/Seq,
  Stream-Forked-From/Fork-Offset/Fork-Sub-Offset) and resolve to a `WriteResult`
  — never throwing. A `WriteResult` carries `ok`, `nextOffset`, `location`, a
  `ProducerConflict | null` (from Producer-Expected-Seq / Producer-Received-Seq),
  the `Operation` that was sent, and the captured `HttpExchange`. Build any new
  write the same way: map options → headers, call the shared `doWrite`.
- **Live tail (`lib/dsClient.ts`).** `openLongPoll(path, fromOffset, onBatch,
  onState)` loops `GET …&live=long-poll`, honoring `Stream-Next-Offset` and
  `Stream-Up-To-Date`, with backoff + an internal `AbortController`.
  `openSse(path, fromOffset, onMessage, onState)` opens a browser `EventSource`
  on `…&live=sse`. Both return a `TailStopper` (`() => void`) and report progress
  via `TailStatus` — they never throw to the caller. A `TailBatch` is the
  delivered rows plus the resume cursor.
- **Store actions + state (`state/store.ts`).** The write actions
  (`createStream`/`appendMessages`/`closeStream`/`deleteStream`/`forkStream`)
  wrap the client, flip `operationInFlight`, record `lastOperation` + mirror
  `lastExchange`, raise a toast, and refresh the stream list. Live-tail state is
  the read mode `tailMode` (`catchup` | `long-poll` | `sse`, default `catchup`)
  plus `tailStatus`/`tailRows`/`tailPaused`/`tailDropped` with a capped buffer
  (`TAIL_BUFFER_CAP`), and `tailOperation`/`tailStartOffset` (the live GET
  descriptor + the offset it started from, for the disclosure). `startTail`
  (guarded to the two live modes) / `stopTail` own the single stopper; `stopTail`
  also clears `tailOperation`/`tailStartOffset`. Switching stream/connection,
  and `setTailMode` when the mode changes, always stop the tail. Idempotent-
  producer state is `producerIdentity` with `setProducerIdentity`/
  `bumpProducerSeq`.
- **Toasts (`state/store.ts` + `components/Toaster.tsx`).** `addToast` /
  `dismissToast` manage a `Toast[]` signal with per-toast auto-dismiss timers;
  the `Toaster` (mounted above the routing seam in `app.tsx`) is pure layout —
  two `aria-live` log regions (assertive for error/warning, polite otherwise),
  per-toast sr-only kind label, dismiss button, and an optional inline action.
  Styling is the `dsui-toast*` classes (semantic tokens, `--z-toast`).

When adding the operation UI (create/publish/fork/close/delete controls, a
Playground with presets, a live-tail view), keep building on these seams: a
control calls a store action, the store calls the client, and the equivalent
curl comes from `toCurl(result.operation)` (or a previewed `Operation`).

### The write-operation UI (built on the seams above)

The create/publish/fork/lifecycle controls and the Playground are now built.
They add no new logic to components — every one is a thin layout over a store
action plus a pure preview. The pieces and their seams:

- **`lib/streamForm.ts` (pure, tested).** All write-form validation lives here,
  next to `lib/validation.ts` in spirit: `validateStreamPath`, `validateTtl`,
  `validateExpiresAt`, `validateJsonBatch` (returns a typed count + normalized
  array), `validateProducer` / `toProducerIdentity`, `validateSubOffset`. It
  also holds the **operation previews** — `previewCreateOperation`,
  `previewAppendOperation`, `previewCloseOperation`, `previewDeleteOperation`,
  and `previewStreamUrl` — which build the exact `Operation` a form WILL send so
  the equivalent curl can show before the request runs. These mirror the private
  header builders in `dsClient` and are unit-tested against the same
  expectations (`streamForm.test.ts`), so a drift between preview and reality is
  caught. Add a write-form rule or a new preview here, not in a component.
- **Store glue (`state/store.ts`).** Dialog open-state is `activeDialog`
  (`"create" | "fork" | null`) + `forkSeed`, opened via `openCreateDialog` /
  `openForkDialog(fromPath, offset)` and closed with `closeDialog`. `refreshMeta`
  HEADs the selected stream (mirrors into `streamMeta` + `lastExchange`, upgrades
  the StreamInfo kind, toasts). `runDemoProducer(path, {total, delayMs})` streams
  a few JSON messages with a delay so a live tail visibly updates. These are the
  only new mutation entry points; components never write signals.
- **Shared UI primitives.** `components/Modal.tsx` is the dialog shell (a native
  `<dialog open>` over a backdrop, with Escape/backdrop close, focus move-in +
  restore, and a Tab focus trap). `components/CurlPreview.tsx` is the
  collapsed "Equivalent curl" disclosure over any `Operation` (it renders
  nothing when handed `null`, so a form can pass a not-yet-valid preview and let
  it hide itself). Both are pure layout.
- **The feature components.** `CreateStreamDialog` (path + content-type radios +
  an Advanced disclosure for TTL/Expires-At/closed), `ForkDialog` (seeded from
  `forkSeed`; new path + fork offset + optional sub-offset), `PublishComposer`
  (a content-type-aware editor mounted in the workspace under the toolbar — a
  JSON batch editor, a text area, or a binary text/base64 input, plus an
  idempotent-producer disclosure and a "close after sending" checkbox),
  `StreamActionsMenu` (the workspace-header popover: Fork / Refresh metadata /
  Close / Delete-with-confirm, each with its curl), and `Playground` (a Navigator
  section of one-click presets on the `playground/…` sample namespace that drive
  the real store actions). The create + fork dialogs are mounted above the
  routing seam in `app.tsx` (next to the `Toaster`) and switched by
  `activeDialog`.
- **The Playground bootstrap (`components/Playground.tsx`).** Seven presets —
  Create sample JSON stream, Publish a sample batch, Run a demo producer (the
  cancellable spaced loop via `store.runDemoProducer`), Tail live (SSE), Fork at
  latest, Close, and Delete / reset — each calls the SAME store action the rest
  of the UI uses (no special path), and each discloses, before it runs, a
  plain-language "what it does" line plus its EXACT equivalent curl. The curl
  comes from the same pure preview helpers (`lib/streamForm` + `lib/tail`,
  `tailToCurl` for the SSE `-N`), built from the active connection's origin (or a
  placeholder origin so the curl still teaches the shape when no connection is
  active). Add a preset in `buildPresets`: a label, a "what it does" line, the
  store action, and the previewed `Operation`. A first-run empty-state in the
  Navigator points a newcomer at it via `store.highlightPlayground`, a one-shot
  monotonic pulse signal the `Playground` section keys an effect off (scroll-into-
  view + a brief outline). `CurlPreview` gained an optional `command` override
  for transports `toCurl` cannot infer (SSE's `-N`).
- **New icons.** `IconFork`, `IconSend`, `IconLock`, `IconMore`, `IconSparkles`,
  `IconFilePlus`, `IconZap` were added to `components/icons.tsx` in the existing
  inline-SVG style. **New CSS** for all of the above lives under the
  `dsui-modal*`, `dsui-curl*`, `dsui-publish*`, `dsui-actions*`, `dsui-form*`,
  `dsui-radio*`, `dsui-check*`, `dsui-disclose*`, `dsui-textarea*`,
  `dsui-forksource*`, and `dsui-playground*` classes (semantic tokens only).

### The live-tail UI (built on the seams above)

Live tailing (long-poll + SSE) follows the same pattern — pure logic in `lib/`,
the connection owned by the store's `startTail`/`stopTail`, layout in the
component. Its pieces and seams:

- **`lib/tail.ts` (pure, tested).** The live-tail counterpart to
  `lib/streamForm`: `isLiveMode`/`describeTailMode` classify the read mode;
  `previewTailUrl`/`previewTailOperation` build the exact live request
  (`GET …?offset=X&live=long-poll|sse`) the tail WILL open — mirroring dsClient's
  private `tailUrl` and unit-tested against it so preview and reality cannot
  drift; `tailToCurl` reproduces it as curl, adding `-N` for SSE; and
  `tailTone`/`tailStatusLabel`/`tailStatusDetail`/`tailAnnouncePolite`/
  `isTerminalTailState` turn a `TailStatus` into the small pieces a status
  affordance needs (color tone, label, detail, aria-live politeness, terminal
  check). Add a tail rule or status mapping here, not in a component.
- **Store glue (`state/store.ts`).** Covered above: the read-mode `tailMode`
  selector, the bounded buffer, `startTail`/`stopTail`, and the
  `tailOperation`/`tailStartOffset` descriptor the disclosure reads. `startTail`
  records the live `Operation` into `tailOperation` + `lastOperation` so the
  curl works for SSE (which captures no per-request `HttpExchange`); long-poll
  also mirrors each batch's real exchange into `lastExchange`.
- **`components/TailPanel.tsx`.** The live view rendered in place of the paged
  grid when `tailMode` is a live mode. It is layout + the scroll "stick to
  bottom" (a purely visual concern): a `role="status"` aria-live badge whose
  politeness follows the status urgency, Pause/Resume (`setTailPaused`), Clear
  (`clearTailBuffer`), Start/Stop (`startTail("now")`/`stopTail`), a live
  `role="listbox"` of `role="option"` rows driving the inspector (matching the
  paged grid), a "Jump to latest" affordance when scroll is unstuck, and a
  buffered/aged-out footer. It stops the tail on unmount as a belt-and-braces
  guard (the store already stops on stream/connection/mode change), so no
  EventSource / long-poll loop leaks.
- **Toolbar + disclosure wiring (`MessagesWorkspace.tsx`, `ProtocolPanel.tsx`).**
  A reusable `Segmented` roving-tabindex picker drives both the Mode and Start
  controls. `ProtocolPanel` gained an optional `tail` prop (`TailDisclosure` =
  the live `Operation` + `TailStatus` + mode + start offset); when present it
  renders a "Live connection" block (the long-lived GET + its status, copy-as-
  curl with `-N` for SSE) above the last captured exchange and reflects the live
  status in the summary. It renders when there is an exchange OR an open tail.
- **New icons + CSS.** `IconPause`, `IconStop`, `IconBroadcast`,
  `IconArrowDownToLine` were added to `components/icons.tsx`. New CSS lives under
  the `dsui-tail*` classes (semantic tokens only); the live grid reuses the
  existing `dsui-grid__header` / `dsui-row` classes. Motion (the auto-scroll +
  the new-row flash + the connecting pulse) honors `prefers-reduced-motion` via
  the global rule in `base.css`.

### Persisted UI-layout signals (the toggle pattern)

Two layout preferences live in the store and survive a reload the same way the
`theme` does — they are the template to copy for any future persisted toggle:

- **`inspectorCollapsed`** (`boolean`, default `false` = expanded) collapses the
  right-hand Inspector pane. The `Shell` renders the inspector `<aside>` only
  when expanded and adds `.dsui-main--noinspector` to the grid when collapsed; a
  header icon button (`IconPanelRight`, `aria-pressed`) and an in-panel collapse
  chevron both call `toggleInspector()`.
- **`playgroundOpen`** (`boolean`, default `true` = open) folds the left rail's
  Playground section. Its header is a `<button aria-expanded>` with a chevron
  that rotates open; when closed only the header renders. Toggled via
  `togglePlayground()`.

The pattern, mirroring `theme` exactly: a `LS_*` key constant, a defensive
`loadBool(key, fallback)` loader (next to `loadTheme`), the signal initialized
from that loader, a persistence `effect(...)` that writes `"true"`/`"false"` on
change (next to the theme effect), and a `toggle…()` action. Keep these in
`state/store.ts`; components only read the signal and call the action. CSS for
the collapse/fold affordances reuses `.dsui-iconbtn` and the existing caret
rotation idiom (a chevron `transform: rotate(90deg)` under reduced-motion safety
from `base.css`).

## How to add another operation, end to end

Every operation in dsui follows the same path: a client method does the request,
a store action drives it, a component triggers the action, a pure preview builds
the curl, and tests pin the behavior. Add a new one in this order. Do not add a
dependency for any of it; the lightweight stack rules above still hold.

1. **Add the client method (`lib/dsClient.ts`).** This is the only place a
   request is made. Map the typed options onto headers in a small builder, then
   call the shared `doWrite` (for a write) or open a tail. Return a typed result
   and never throw: a write resolves to a `WriteResult` (the `Operation` that was
   sent, the captured `HttpExchange`, `ok`, and any conflict or error), and a
   read resolves to a `ReadResult`. Follow `createStream` / `appendMessages` as
   the model. If the result needs a new shape, add it to `lib/types.ts` first and
   keep that file dependency-free.

2. **Add the store action (`state/store.ts`).** The store is the only place state
   changes. Write an action that calls the client method, flips
   `operationInFlight` while it runs, records the result's `operation` into
   `lastOperation` and its `exchange` into `lastExchange` (so the curl and the
   "Under the hood" panel update), raises a toast through `addToast`, and
   refreshes anything the change affects (usually `refreshStreams`). A new piece
   of shared state gets a `signal` and is mutated only here. State that is local
   to one view stays a `useSignal` in the component.

3. **Add the curl preview (`lib/streamForm.ts` or `lib/tail.ts`).** A form should
   show the exact curl before it submits, so write a pure `preview…Operation`
   that builds the same `Operation` the client will send. It must mirror the
   client's header builder exactly. The previews are unit-tested against the same
   expectations as the client, which is what keeps the preview and the real
   request from drifting. Put validation rules next to the previews here too.

4. **Build the component (`components/`).** The component is layout only. It reads
   store signals, reads the pure preview, calls the store action, and arranges
   the result. Reuse the shared primitives: `Modal` for a dialog, `CurlPreview`
   for the equivalent curl, and the form, radio, check, and disclosure classes in
   `app.css`. Match the accessibility of the existing components: labelled
   controls, inline errors wired with `aria-describedby`, a roving tabindex for
   any interactive list, and motion behind `prefers-reduced-motion`. Add a new
   icon to `components/icons.tsx` in the inline-SVG style if you need one. Wire
   the component into the Navigator, the workspace, the actions menu, a dialog
   slot in `app.tsx`, or a Playground preset, depending on where it belongs.

5. **Add the tests.** Unit-test the pure pieces directly: the preview against the
   client's headers (so they cannot drift), and any new validation. Add a
   `dsClient.test.ts` case for the new method using the existing `fetch` stub.
   Add a component test with `@testing-library/preact` for the happy path and the
   error path. Run `typecheck`, `lint`, and `test` before you finish.

The same five steps describe every operation already in the tree, so the closest
existing operation is your template. A create-shaped write follows
`createStream`; an append-shaped write follows `appendMessages`; a live read
follows `openLongPoll` / `openSse` and the `startTail` / `stopTail` pair.

## Commands

Run these from the `ui/` directory.

```bash
npm run dev         # Vite dev server on :3001 with HMR
npm run build       # production build → ../cmd/dsui/embedded (the Go binary's assets)
npm run typecheck   # tsc --noEmit, strict
npm run lint        # biome check . (lint + import-organize + format check)
npm run format      # biome format --write .
npm run test        # vitest run (jsdom + @testing-library/preact)
```

Before finishing a change, run `typecheck`, `lint`, and `test`. They are fast.
Note: another agent may be building or testing concurrently — coordinate before
running `build`, since `build` empties and rewrites `../cmd/dsui/embedded`.

## Design-token conventions

All visual values come from `src/styles/tokens.css`. The rules:

- **Use semantic tokens, not raw scale steps.** Reach for `--bg`, `--bg-raised`,
  `--fg`, `--fg-muted`, `--border`, `--accent`, `--ok`, `--warn`, `--danger`, and
  the rest of the semantic set. Do not use `--gray-700` or a literal `oklch(...)`
  / hex color in component CSS; the semantic tokens are what flip correctly
  between light and dark.
- **Color is `oklch`.** New colors, if a human approves any, follow the existing
  ramp style.
- **Spacing, radius, type, shadow, motion** all have token scales
  (`--space-*`, `--radius-*`, `--text-*`, `--shadow-*`, `--dur-*`, `--ease-*`).
  Use them instead of magic numbers.
- **Theming is automatic.** Dark mode is defined once in `tokens.css` (via
  `prefers-color-scheme` and the `[data-theme="dark"]` override). If you only use
  semantic tokens, your component themes for free. A manual light/dark choice on
  `<html data-theme>` must always win over the OS preference — that is why both a
  media query and an attribute selector exist.
- **Class names use the `dsui-` prefix** with a BEM-ish `block__element--modifier`
  shape, e.g. `dsui-nav__section`, `dsui-btn--primary`, `dsui-row--timed`.
- **Icons** are inline SVG components in `icons.tsx`: a 24×24 stroke path, stroke
  width 1.6, round caps, color via `currentColor`, sized through a `size` prop.
  Add an icon by adding a component there; never add an icon dependency.

## Protocol facts an agent needs

dsui both reads and writes Durable Streams: it reads and pages, follows a tail
live, and runs the full write / fork / lifecycle surface (create, publish,
close, delete, fork) plus a metadata HEAD. The write headers it sends are listed
under "Stream operations" above (Content-Type, Stream-TTL, Stream-Expires-At,
Stream-Closed, Producer-Id / Epoch / Seq, Stream-Forked-From / Fork-Offset /
Fork-Sub-Offset). The read protocol surface it touches:

- **Stream URL.** `{baseUrl}{streamRoot}/{path}`. Default `streamRoot` is
  `/v1/stream`. Path segments are URL-encoded but slashes are kept as separators
  (see `streamUrl` / `encodeStreamPath` in `dsClient.ts`).
- **Read.** `GET {streamUrl}?offset={cursor}`. The offset is an opaque cursor.
  Reserved values: `-1` = beginning, `now` = current tail. `lib/messages.ts`
  resolves the toolbar's Earliest / Latest / At-offset choice into one of these.
- **Resume.** The response header `Stream-Next-Offset` is the cursor for the next
  read. Send it back as `?offset=` to page forward. There is no per-message
  offset — only a batch plus this one next cursor. Keep the UI honest about that:
  identify a message by its index within the batch, never invent a per-message
  offset.
- **State flags.** `Stream-Up-To-Date` (present at the tail) and `Stream-Closed`
  (present when the stream is closed forever). These are read loosely:
  present / `true` / `1` / `yes` count as true.
- **Other captured headers.** `ETag` and `Content-Type` are also surfaced. The
  protocol-significant set is centralized in `lib/protocol.ts`
  (`isSignificantHeader`, `protocolHeaderRows`); extend it there, not in a
  component.
- **Content type → render kind.** `application/json` or `+json` → JSON (body is a
  JSON array of messages, one row per element); `text/*` / `charset` / `xml` /
  `csv` → text; anything else → binary (hex dump). See `kindFromContentType`.
- **Discovery via `__registry__`.** There is no list-all endpoint. dsui reads the
  reserved `__registry__` stream from `-1`, parses each event (a JSON array or
  newline-delimited JSON of `{ key, value: { path, contentType, createdAt },
  headers: { operation } }`), and reduces them to the current live set: an
  `upsert` adds or updates a path, a `deleted` removes it, later events win. An
  empty or absent registry yields an honestly empty list; the user can add stream
  paths by hand (`addManualStream`).
- **Failure handling.** 404, 204, empty bodies, malformed JSON, and network/CORS
  errors all resolve to typed empty results, never throws. A failed request still
  produces an `HttpExchange` with `status: 0` so the disclosure can show it.
- **Runtime config.** The Go binary serves `/dsui-config.json` as
  `{ "defaultServer": "<url>" }` (empty string means none). `lib/config.ts` loads
  it and treats every failure as "nothing to prefill." Absent under `vite dev`.

When in doubt about a protocol detail, the source of truth is the server in this
repo's root (`handler.go`, `protocol/headers.go`) and the project root
`README.md`. Match those; do not guess header names or semantics.
