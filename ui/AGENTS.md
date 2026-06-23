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

dsui reads Durable Streams; it does not create or append. The protocol surface it
touches:

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
</content>
