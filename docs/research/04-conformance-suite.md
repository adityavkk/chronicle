# 04 — The Server Conformance Suite

How to run, filter, and pass `@durable-streams/server-conformance-tests` against chronicle, and how the
Caddy plugin's own harness boots its binary (the pattern we will mirror for chronicle + Redis).

All paths below refer to the reference monorepo cloned at `/Users/auk000v/dev/durable-streams`.

Sources read in full:

- `IMPLEMENTATION_TESTING.md` (monorepo root)
- `packages/server-conformance-tests/` — `package.json`, `README.md`, `src/cli.ts`, `src/index.ts` (11,466 lines — the entire suite lives in this one file), `src/test-runner.ts`, `bin/conformance-dev.mjs`
- `packages/caddy-plugin/test/conformance.test.ts`, `test/Caddyfile`, `test/Caddyfile.test`, `package.json`, `cmd/caddy/main.go`, `module.go`
- Root `package.json`, `vitest.config.ts`, `packages/server/test/conformance.test.ts`

---

## 1. Package facts

| Field | Value |
| --- | --- |
| npm name | `@durable-streams/server-conformance-tests` |
| Version (local & published) | `0.3.5` — **published on npm**, `latest` dist-tag confirmed via registry. We do *not* need the monorepo to run it. |
| License | Apache-2.0 |
| Type | ESM (`"type": "module"`), dual CJS/ESM dist via tsdown |
| Bins | `server-conformance-tests`, `durable-streams-server-conformance` (both → `dist/cli.js`), `durable-streams-server-conformance-dev` (→ `bin/conformance-dev.mjs`, runs `src/cli.ts` via `tsx` for monorepo development) |
| Runtime dependencies | `@durable-streams/client` (`workspace:*` locally; published client is `0.2.6`), `fast-check ^4.4.0`, **`vitest ^4.0.0` is a production dependency** (the CLI spawns it) |
| Engines (published pkg) | `node >= 18` |
| Engines (monorepo dev) | `node >= 22`, `packageManager: pnpm@10.25.0` |

The suite is **not** YAML-driven (that's the *client* conformance suite). The server suite is a single
TypeScript file, `src/index.ts`, exporting one function that registers ~332 vitest `test()` cases across
49 `describe` blocks.

```typescript
export interface ConformanceTestOptions {
  /** Base URL of the server to test */
  baseUrl: string
  /** Timeout for long-poll tests in milliseconds (default: 20000) */
  longPollTimeoutMs?: number
  /** Enable stream metadata subscription conformance tests. */
  subscriptions?: boolean
}

export function runConformanceTests(options: ConformanceTestOptions): void
```

`src/test-runner.ts` is a 20-line vitest entry file:

```typescript
import { runConformanceTests } from "./index.js"
const baseUrl = process.env.CONFORMANCE_TEST_URL
if (!baseUrl) throw new Error(`CONFORMANCE_TEST_URL environment variable is required. ...`)
runConformanceTests({ baseUrl })
```

---

## 2. Exact invocation

### 2.1 CLI (the path we'll use for chronicle CI)

```bash
# Run once and exit non-zero on failure (CI mode)
npx @durable-streams/server-conformance-tests --run http://localhost:4437

# Watch mode: rerun on changes under one or more source dirs (300 ms debounce)
npx @durable-streams/server-conformance-tests --watch src http://localhost:4437
npx @durable-streams/server-conformance-tests --watch src lib http://localhost:4437
```

What the CLI actually does (`src/cli.ts`):

1. Parses args — the **last non-flag argument is the base URL** (validated with `new URL()`).
2. Locates the test runner file (`dist/test-runner.js`, falling back to `src/test-runner.ts` under tsx).
3. Locates a `vitest` binary, in order: the package's own `node_modules/.bin/vitest` (vitest is a prod
   dependency, so this always exists after `npm install`), the hoisted scoped-package location, the
   monorepo root `node_modules/.bin/vitest`, then bare `vitest` from `PATH`.
4. Spawns:

```
vitest run <test-runner.js> --no-coverage --reporter=default --passWithNoTests=false
```

with env:

| Env var | Purpose |
| --- | --- |
| `CONFORMANCE_TEST_URL` | **The only required env var.** Base URL of the server under test. Set by the CLI from its URL argument. |
| `FORCE_COLOR=1` | Cosmetic. |

Exit code of vitest is propagated as the CLI's exit code. There are **no CLI flags for filtering,
long-poll timeout, or subscriptions** — CLI runs always use defaults (`longPollTimeoutMs = 20000`,
`subscriptions = false`).

### 2.2 Programmatic (the path the Caddy plugin uses; gives access to all options)

```typescript
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import { beforeAll, afterAll, describe } from "vitest"

describe(`My Server Implementation`, () => {
  // MUTABLE object: the suite reads options.baseUrl lazily via a getter,
  // precisely so you can assign a dynamic port from beforeAll.
  const config = { baseUrl: ``, subscriptions: true }

  beforeAll(async () => {
    const server = await startMyServer({ port: 0 })
    config.baseUrl = server.url
  })
  afterAll(async () => server.stop())

  runConformanceTests(config) // registers describes/tests synchronously
})
```

Internals to be aware of:

```typescript
const getBaseUrl = () => options.baseUrl
const getLongPollTestTimeoutMs = () => (options.longPollTimeoutMs ?? 20_000) + 1_000
```

- `runConformanceTests` must be called at module load (it calls `describe`/`test` synchronously);
  only `baseUrl` may be filled in later.
- `longPollTimeoutMs` is used **only as a vitest per-test timeout** (+1 s slack) on the six tests that
  may have to ride out a server-side long-poll window (lines 568, 2446, 7226, 7257, 9838). It does not
  configure the server.

### 2.3 Filtering tests

No suite-native filter exists. Use standard vitest mechanisms:

```bash
# Direct vitest invocation against the runner file (what the CLI wraps):
CONFORMANCE_TEST_URL=http://localhost:4437 \
  npx vitest run node_modules/@durable-streams/server-conformance-tests/dist/test-runner.js \
  -t "Idempotent Producer"

# In the monorepo:
cd /Users/auk000v/dev/durable-streams && vitest run --project caddy -t "Fork - Creation"
```

`-t/--testNamePattern` matches against the full describe chain, so group names in §4 below are the
filter vocabulary.

### 2.4 Base URL & stream namespace

- Every request is `${baseUrl}${streamPath}` where the suite **hardcodes the `/v1/stream/` prefix**:
  paths look like `/v1/stream/create-test-${Date.now()}`. The server must therefore serve the protocol
  under `<baseUrl>/v1/stream/*`. (The Caddy test config actually routes `/*`, which is a superset.)
- **Fresh namespace = fresh per test, not per run.** Every test mints a unique path with `Date.now()`,
  and concurrency-sensitive groups (TTL, fork-TTL, fuzzing) add `Math.random().toString(36)` suffixes,
  e.g. `` `/v1/stream/${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}` ``. There is no
  global setup/teardown and no namespace reset call.
- Consequence for chronicle: the suite does **not** require a clean datastore, but most created streams
  are never deleted, so repeated runs accumulate streams in Redis. Flush the test DB between runs for
  hygiene (e.g. dedicated `redis-cli -n 15 flushdb`), not for correctness.
- Reserved paths used by the optional subscriptions group: `/v1/stream/__ds/subscriptions/{id}`
  (+ `/streams`, `/streams/{path}`, `/claim`, `/ack`, `/release`, `/callback`) and
  `/v1/stream/__ds/jwks.json`.
- The Caddy harness's readiness probe creates and deletes `/v1/stream/__health__`.

---

## 3. What the suite requires of the server lifecycle

1. **One long-lived server process** for the whole run. Tests run concurrently within files
   (`test.concurrent` in TTL groups) and the property tests hammer the server with parallel
   readers/writers — the server must tolerate sustained concurrent load on one instance.
2. **DELETE must be implemented**: `DELETE` → `204` on success, `404` for nonexistent streams; a
   recreated stream after DELETE must be fully isolated from its predecessor (offsets, producer state,
   data). The fork-lifecycle group additionally requires **soft-delete semantics**: a deleted source
   with live forks returns `410 Gone` on GET/POST/DELETE, `409` on PUT re-create, and is garbage
   collected (HEAD → 404, polled) when the last fork is deleted — cascading through fork chains.
3. **Vitest default 5 s per-test timeout** applies to every test that doesn't pass an explicit timeout.
   In practice: all plain CRUD must complete in well under 5 s, SSE must flush data+control events within
   the suite's 2 s default `fetchSSE` window, and long-poll wakeups must deliver appended data promptly
   (test appends 500 ms after opening the poll).
4. **Explicit longer timeouts** exist only on: long-poll waits (`(longPollTimeoutMs ?? 20000)+1000`),
   the 10 MB large-payload test (30 s), and three property tests (30 s, 30 s, 60 s).
5. **Server long-poll timeout should be short for test runs.** The `204 timeout` test aborts client-side
   at 5 s and treats `AbortError` as a pass, so any server timeout works — but to actually exercise the
   `204 + Stream-Cursor + Stream-Up-To-Date + Stream-Next-Offset` path, configure the server's long-poll
   timeout below 5 s. Both reference harnesses use **500 ms** (Caddy directive `long_poll_timeout 500ms`;
   TS dev server `longPollTimeout: 500`). Chronicle needs an equivalent config knob.
6. **TTL timing assumptions** (`TTL Expiration Behavior`, 11 `test.concurrent` cases):
   - 1 s and 2 s TTLs are created; expiry is detected by polling HEAD every 200 ms for up to 5 s
     (`vi.waitFor`) after an initial sleep — so expiry must take effect within ~1–2 s of nominal, but
     exact-millisecond reaping is not required (lazy expiry on access works if HEAD/GET/POST all observe it).
   - TTL is a **sliding window renewed by reads (GET) and writes (POST), but NOT by HEAD**.
   - `Stream-Expires-At` is absolute and **never renewed** by reads or writes.
   - Expired streams: 404 on HEAD/GET/POST, and PUT re-create with *different* config must succeed (201).
7. **Cursor behavior**: `Stream-Cursor` must be a numeric string, generated even when absent from the
   request, returned on 200 and 204 long-poll responses, and **strictly greater** than an echoed cursor
   (collision → advance with jitter).
8. **ETag/304**: GET responses carry an ETag; `If-None-Match` with the current ETag → 304; ETag changes
   after appends.
9. **Read-your-writes**: data must be visible to GET immediately after the append response returns
   (no eventual consistency window) — this is also fuzzed property-style with 25 runs.
10. The suite never restarts the server; crash-recovery/durability is explicitly out of scope and
    delegated to white-box tests (see `IMPLEMENTATION_TESTING.md`, summarized in §7).

---

## 4. Complete inventory of test groups

All groups run unconditionally except the last (gated on `subscriptions`). Counts are `test()` cases.

### Core protocol

| Group | # | Asserts |
| --- | --- | --- |
| **Basic Stream Operations** | 5 | create (201); idempotent create with same config (200); create with different config → 409; delete (204); recreated stream after delete is isolated (no stale data/offsets). |
| **Append Operations** | 3 | append string data; multiple chunks concatenate in order; `Stream-Seq` ordering enforced. |
| **Read Operations** | 3 | read empty stream (empty body + up-to-date); read with data; read from offset returns only the suffix. |
| **Long-Poll Operations** | 2 | `should wait for new data with long-poll` (opens poll, appends 500 ms later, data arrives — uses the TS client); `should return immediately if data already exists`. |
| **HTTP Protocol** | 15 | PUT returns 201 + echoed `Content-Type` + `Stream-Next-Offset`; idempotent PUT → 200; conflicting PUT → 409; POST headers; POST to nonexistent → 404; content-type mismatch on POST → 409; GET headers; empty stream GET → empty body + `Stream-Up-To-Date`; offset reads; DELETE nonexistent → 404; DELETE → 204; `Stream-Seq` ordering incl. **lexicographic** comparison (`"2"` then `"10"` **rejected**, `"09"` then `"10"` accepted) and duplicate seq rejected. |
| **Browser Security Headers** | 9 | `X-Content-Type-Options: nosniff` on GET/PUT/POST/HEAD/SSE/long-poll **and error responses**; `Cross-Origin-Resource-Policy` on GET; `Cache-Control: no-store` on HEAD. |
| **TTL and Expiry Validation** | 5 | both `Stream-TTL` + `Stream-Expires-At` → 400; non-integer TTL → 400; negative TTL → 400; valid TTL accepted; valid `Expires-At` accepted. |
| **Case-Insensitivity** | 3 | content-type compared case-insensitively (incl. idempotent create with different case); request header names case-insensitive. |
| **Content-Type Validation** | 3 | append mismatch → 409; matching append OK; GET echoes the stream's content type. |
| **HEAD Metadata** | 3 | metadata without body; 404 for nonexistent; `Stream-Next-Offset` = tail offset. |
| **Offset Validation and Resumability** | 22 | `offset=-1` sentinel ≡ no offset; **`offset=now`** sentinel: returns tail offset, resumable, empty-stream behavior, `[]` body for JSON streams vs empty body otherwise, combination with long-poll (wait + receive) and SSE, 404 for nonexistent stream in plain/long-poll/SSE modes; malformed offsets (comma, spaces) → 400; resumable reads see no duplicate bytes; reading at tail → empty + up-to-date. |
| **Protocol Edge Cases** | 13 | empty POST body → 400; PUT with initial body; immutability by position; unique monotonically increasing offsets; empty `offset=` param → 400; multiple offset params → 400; case-*sensitive* seq comparison; binary data integrity; `Location` header on 201; POST missing Content-Type → 400 (vs PUT defaulting); unknown query params ignored. |
| **Long-Poll Edge Cases** | 5 | `live=long-poll` without offset → 400; `Stream-Cursor` generated; immediate 200 + `Stream-Up-To-Date` when data exists at offset; cursor echo/collision → strictly greater cursor; 204-on-timeout carries `Stream-Cursor`, `Stream-Up-To-Date: true`, `Stream-Next-Offset` (client aborts at 5 s; AbortError tolerated). |
| **TTL and Expiry Edge Cases** | 9 | TTL grammar strictness: leading zeros, `+`, floats, scientific notation → 400; invalid Expires-At → 400; `Z` and `±hh:mm` timezones accepted; idempotent PUT with same TTL → 200, different TTL → 409. |
| **HEAD Metadata Edge Cases** | 2 | HEAD reports TTL / Expires-At metadata when configured. |
| **TTL Expiration Behavior** | 11 (`test.concurrent`) | 404 on HEAD/GET/POST after 1 s TTL expiry; same after Expires-At passes (3–4 s windows); recreate after expiry → 201; **sliding TTL renewed by write and by read, NOT by HEAD; Expires-At never renewed**. |
| **Caching and ETag** | 4 | ETag generated; matching `If-None-Match` → 304; non-matching → 200; ETag changes after append. |
| **Chunking and Large Payloads** | 2 | 100 KB append read back via offset pagination — server may chunk GET responses; loop follows `Stream-Next-Offset` until `Stream-Up-To-Date` + empty body, offsets must progress monotonically, bytes must reassemble exactly; 10 MB POST may be accepted (200/204) **or** rejected with 413 (30 s timeout). |
| **Read-Your-Writes Consistency** | 3 | immediate GET visibility after one append, after multiple appends, and for offset-based reads. |

### SSE & JSON

| Group | # | Asserts |
| --- | --- | --- |
| **SSE Mode** | 32 | `Accept: text/event-stream` and `live=sse` both supported; offset required (400 without); data events stream; control events carry offset + `streamCursor`; cursor collision/jitter in SSE; JSON streams wrap data events in valid JSON arrays; empty-stream SSE; `upToDate` flag in control events; **no `Content-Length`, proper `Cache-Control`** on SSE responses; newline handling in text/plain payloads; **CRLF-injection prevention** (CRLF, LF-only, CR-only attack vectors must become literal data, never event boundaries); JSON payloads with embedded newlines; monotonic offsets in SSE; reconnection with last offset; **binary streams auto-detected → base64-encoded data events + `Stream-SSE-Data-Encoding: base64` header** (present for `application/octet-stream`, `application/x-protobuf`, `image/png`; absent for `text/plain` and `application/json`); control events stay JSON (never base64); empty/large/special-byte binary payloads; `offset=now` + base64. |
| **JSON Mode** | 19 | PUT with `[]` body creates empty stream; POST `[]` → 400 (empty batch rejected); charset parameter in content-type tolerated; **single JSON value wrapped in array; arrays are flattened batches** (each element one message); multiple appends concatenate into one array on read; mixed values/arrays; invalid JSON → 400; all JSON value types; structure/nesting preserved; client `json()` iterator integration; **double-wrapped arrays → inner arrays stored as single messages**; primitive arrays; mixed batching. |

### Property-based (fast-check)

`describe(\`Property-Based Tests (fast-check)\`)` — randomized, with bounded `numRuns` (10–50) and
`interruptAfterTimeLimit` on the heavy ones:

| Subgroup | Tests | Asserts |
| --- | --- | --- |
| Byte-Exactness Property | 2 (30 s timeouts) | 7 concurrent readers during interleaved writes: every snapshot is a **byte-exact prefix** of the final content (no torn reads); full 0–255 byte-value coverage. numRuns 20/50. |
| Operation Sequence Properties | 3 (one 60 s) | random append/read sequences keep invariants (numRuns 15); offsets always monotonically increasing (25); read-your-writes always holds (30). |
| Immutability Properties | 1 | data at an offset never changes after later appends (20). |
| Offset Validation Properties | 1 | random invalid-character offsets rejected (30). |
| Sequence Ordering Properties | 2 | lexicographically increasing seqs accepted (20); out-of-order rejected (25). |
| Concurrent Writer Stress Tests | 4 | concurrent writers with seq handled gracefully; racing incrementing seqs; concurrent appends **without** seq all persist (10); mixed readers/writers see consistent state. |
| State Hash Verification | 4 | replay of a stream produces identical FNV-1a content hash; hash changes on append; empty-stream hash stable; deterministic ordering. |

### Idempotent producers — NOT optional (runs unconditionally)

`describe(\`Idempotent Producer Operations\`)` — 26 tests. Request headers `Producer-Id`,
`Producer-Epoch`, `Producer-Seq`; response headers `Producer-Epoch` (echo), `Producer-Expected-Seq`,
`Producer-Received-Seq`. Key assertions:

- First append `(epoch=0, seq=0)` → **200** (not 204) with `Stream-Next-Offset` and `Producer-Epoch: 0` echoed; sequential seqs accepted.
- **Duplicate seq → 204** (idempotent success); duplicate response returns the **highest accepted seq**, not the request's; duplicate of seq=0 doesn't corrupt state; duplicates return 204 even when a `Stream-Seq` header is also present; duplicate body content is ignored (deduped to original).
- Epoch upgrade allowed only with `seq=0`; **epoch bump with seq≠0 → 400**; **stale epoch → 403** (zombie fencing, incl. split-brain scenario); **epoch rollback rejected**; **sequence gap → 409**.
- All three headers required together (partial sets → 400); invalid integer formats → 400; empty `Producer-Id` rejected.
- Multiple producers keep independent `(epoch, seq)` state and interleave correctly; ordering of a single producer's writes preserved; works with `Stream-Seq` simultaneously.
- Data-integrity reads after producer writes (binary and JSON modes); JSON: invalid JSON / empty array with producer headers still 400.
- Error precedence: nonexistent stream → 404; content-type mismatch → 409; empty body → 400 — all with producer headers present.

### Stream closure — also unconditional

`describe(\`Stream Closure\`)` — 36 tests in 8 subgroups, using the `Stream-Closed` header:

| Subgroup | Highlights |
| --- | --- |
| Create with Stream-Closed (3) | `PUT` + `Stream-Closed: true` creates a closed stream (optionally with body); response echoes `Stream-Closed: true`. |
| Close Operations (8) | POST empty body + `Stream-Closed: true` closes; close-with-final-append; response carries `Stream-Next-Offset` + `Stream-Closed`; closing an already-closed stream (empty body) → 204 idempotent; **close-only ignores Content-Type mismatch**; append to closed → 409 **with `Stream-Closed: true` header**; append+close to closed → 409. |
| HEAD (2) | closed → `Stream-Closed: true`; open → header absent. |
| Read catch-up (3) | `Stream-Closed: true` only when response reaches the tail; partial reads omit it; at tail: 200, empty body, header set. |
| Long-poll (2) | long-poll at tail of closed stream returns immediately (no hang); `Stream-Closed` propagated. |
| SSE (4) | final control event has `streamClosed: true`; `streamCursor` omitted when closed; **connection closes after final event**; a live reader at tail receives data appended atomically with the close. |
| Idempotent producers × closure (6) | close with final append via producer headers; close-only updates producer state; duplicate close tuple → 204; different producer/seq → 409; duplicate close-only → 204. |
| Edge cases (8) | 409-for-closed includes `Stream-Next-Offset`; close nonexistent → 404; `offset=now` on closed reports closed; **stale-epoch producer beats closure: 403, not 409**; close retry with different body dedupes to original; empty POST without `Stream-Closed` → 400; DELETE of closed stream works. |

### Forks — also unconditional

Fork creation uses `PUT` with `Stream-Forked-From` (+ optional `Stream-Fork-Offset`,
`Stream-Fork-Sub-Offset`) headers. Nine groups, ~91 tests:

| Group | # | Highlights |
| --- | --- | --- |
| Fork - Creation | 40 | fork at head (default) / explicit offset / offset 0 / exact head; 404 nonexistent source; 400 offset beyond tail; 409 path collision with different config; forking a closed stream yields an **open** fork; empty source; content-type preserved/inherited; **sub-offsets**: binary mid-message and JSON mid-batch forks, sub-offset 0 ≡ absent, overshoot → 400, malformed → 400, idempotent re-create with matching sub-offset / 409 on mismatch (binary and JSON), appends land after the boundary, content-type inheritance, **producer state never crosses a fork boundary**, sub-offset anchored mid-stream, sub-offset without fork headers → 400, `Stream-Next-Offset` on creation is consumable, sub-offset on empty source → 400, sub-offset == message length / batch count accepted, initial body appended after materialized prefix, sub-offset fork from closed source allowed. |
| Fork - Reading | 6 | full read (inherited + own); inherited-only; own-only; reads across the boundary; **source appends after the fork are invisible to the fork**; fork headers do NOT appear on HEAD/GET/PUT responses (forks are transparent). |
| Fork - Appending | 5 | append to fork; idempotent producer on fork; fork closes independently; closing source doesn't close fork; source appends stay independent. |
| Fork - Recursive | 5 | 3-level chains; fork at mid-point of inherited data; reads across three levels; independent appends per level; **sub-offsets compose across chained forks**. |
| Fork - Live Modes | 4 | long-poll returns inherited data immediately; long-poll handover at the fork offset; SSE streams fork data. |
| Fork - Deletion and Lifecycle | 14 | delete fork keeps source; **soft-delete**: deleted source with live forks still serves fork reads but returns **410** on GET/POST/DELETE and **409** on PUT re-create and on new forks; cascade GC when last fork deleted (poll HEAD→404), through 3 levels, middle-of-chain deletion preserves data; source stays alive when all forks deleted; rejected fork doesn't leak a source reference. |
| Fork - TTL and Expiry | 7 (`test.concurrent`) | fork inherits source expiry/TTL when unspecified; fork may set shorter or longer TTL (no capping) or Expires-At beyond source expiry; fork's own TTL wins when given. |
| Fork - JSON Mode | 2 | fork a JSON stream; read across boundary as one JSON array. |
| Fork - Edge Cases | 4 | fork-then-delete-source immediately; 10 forks of one stream; fork at every offset position; idempotent fork PUT. |

### Optional: Reserved subscription APIs

```typescript
describe.runIf(options.subscriptions)(`Reserved subscription APIs`, () => { ... })
```

**This is the only optional group.** 5 tests, enabled exclusively via the programmatic
`subscriptions: true` option (the CLI cannot enable it). The TS dev server's harness enables it
(`packages/server/test/conformance.test.ts`); **the Caddy harness does not** (its `config` is just
`{ baseUrl }`), even though its Caddyfile configures `webhook_callback_url`.

What it asserts (endpoints under `/v1/stream/__ds/subscriptions/{id}`):

1. *creates and idempotently re-confirms a webhook subscription* — PUT JSON body
   `{type:"webhook", pattern:"events/*", webhook:{url}, lease_ttl_ms, description}` → 201; response
   exposes `webhook.signing` (`alg: "ed25519"`, `kid` matching `/^ds_/`, `jwks_url` =
   `/v1/stream/__ds/jwks.json` serving `application/jwk-set+json` with an OKP/Ed25519/EdDSA key);
   `webhook_secret` never leaked; re-PUT → 200; GET echoes config; DELETE cleanup.
2. *rejects unsafe webhook URLs* — `http://10.0.0.1/hook` → 400 `{error:{code:"WEBHOOK_URL_REJECTED"}}`
   (note: the test receiver itself listens on `127.0.0.1`, so loopback must be allowed while private
   ranges are rejected — SSRF policy with a localhost carve-out for tests).
3. *webhook synchronous done auto-acks the wake snapshot* — creating a matching stream triggers a
   signed webhook (`Webhook-Signature: t=<unix>,kid=...,ed25519=<base64url>`, Ed25519 over
   `"<t>.<rawBody>"`, ±300 s clock window); payload carries `subscription_id`, `callback_url`,
   `streams[{path, tail_offset, has_pending}]`; replying `{done:true}` auto-acks to `tail_offset`.
4. *webhook callback acks and fences stale wake generations* — POST to `callback_url` with
   `Bearer callback_token` and `{wake_id, generation, acks, done}` → `{ok:true,next_wake:false}`;
   replay → 409 `{error:{code:"FENCED"}}`.
5. *pull-wake claim, ack, and release* — `type:"pull-wake"` with `wake_stream`; wake events appended to
   the wake stream; `/claim` → `{wake_id, generation, token, streams}`; second claim → 409
   `ALREADY_CLAIMED` with `current_holder`; `/ack` and `/release` with bearer token; lease TTLs.

Recommendation for chronicle: treat subscriptions as a later milestone; run the suite with
`subscriptions` unset (exactly like Caddy's own conformance gate) until implemented.

---

## 5. How the Caddy plugin boots its binary (mirror this for chronicle + Redis)

`packages/caddy-plugin/test/conformance.test.ts`, in full effect:

```typescript
let caddy: ChildProcess | null = null
const port = 4437
const config = { baseUrl: `http://localhost:${port}` }

beforeAll(async () => {
  const caddyBinary = path.join(__dirname, `..`, `caddy`)   // pre-built binary in package root
  const caddyfile = path.join(__dirname, `Caddyfile`)
  caddy = spawn(caddyBinary, [`run`, `--config`, caddyfile], {
    stdio: [`ignore`, `pipe`, `pipe`],
  })
  caddy.stderr?.on(`data`, (d) => process.stderr.write(`[caddy] ${d}`)) // log forwarding
  await waitForServer(config.baseUrl, 10000)
}, 15000) // beforeAll hook timeout

afterAll(async () => {
  if (caddy) { caddy.kill(`SIGTERM`); await new Promise((r) => setTimeout(r, 500)) }
})

describe(`Caddy Durable Streams Implementation`, () => {
  runConformanceTests(config)
})
```

Readiness probe — **a real protocol round-trip, not a TCP ping**:

```typescript
async function waitForServer(baseUrl: string, timeoutMs: number): Promise<void> {
  // loop every 100 ms until timeout:
  //   PUT  {baseUrl}/v1/stream/__health__  (Content-Type: text/plain)
  //   if response.ok || status === 201:
  //     DELETE {baseUrl}/v1/stream/__health__   // clean up
  //     return
  // throw after timeoutMs
}
```

Test Caddyfile (`test/Caddyfile`, identical `test/Caddyfile.test`):

```caddyfile
{
	admin off
}

:4437 {
	route /* {
		durable_streams {
			data_dir ./data
			long_poll_timeout 500ms
			webhook_callback_url http://localhost:4437
		}
	}
}
```

Pipeline details:

- **Build**: `pnpm build` in the plugin package = `go build -o caddy ./cmd/caddy`. `cmd/caddy/main.go`
  imports `caddycmd` + standard modules + the plugin, so the binary is a self-contained Caddy.
- **Run**: `pnpm conformance` = `pnpm build && cd ../.. && vitest run packages/caddy-plugin/test/conformance.test.ts`;
  CI variant `pnpm test:run` = `cd ../.. && vitest run --project caddy` (root `vitest.config.ts` defines
  the `caddy` project with `include: ["packages/caddy-plugin/**/*.test.ts"]` and **no timeout overrides**
  — defaults apply — plus an alias mapping `@durable-streams/server-conformance-tests` → `packages/server-conformance-tests/src`).
- **Port is fixed at 4437**; `data_dir ./data` is relative to the spawned process cwd (vitest runs from
  the monorepo root and `spawn` inherits cwd). Stale `./data` from previous runs is tolerated because
  stream paths are timestamp-unique.
- Caddy plugin config surface (from `module.go`, useful as chronicle's config vocabulary): `data_dir`
  (empty → in-memory store), `max_file_handles` (default 100), `long_poll_timeout` (default **30 s**),
  `sse_reconnect_interval` (default 60 s), `webhook_callback_url` (set → enables webhook subsystem).

### Chronicle equivalent

```typescript
// chronicle/test/conformance.test.ts (shape to mirror)
beforeAll(async () => {
  // 1. ensure Redis is up (docker compose or testcontainer), flush the test DB
  // 2. spawn ./chronicle --addr :4437 --redis redis://localhost:6379/15 --long-poll-timeout 500ms
  // 3. forward stderr with a [chronicle] prefix
  // 4. poll PUT /v1/stream/__health__ every 100 ms (10 s budget), DELETE on success
}, 15000)
afterAll(() => { proc.kill(`SIGTERM`) /* + grace */ })
runConformanceTests({ baseUrl: `http://localhost:4437` })
```

Or skip the vitest wrapper entirely in CI: start chronicle + Redis, then
`npx @durable-streams/server-conformance-tests --run http://localhost:4437` (loses only the
subscriptions group and custom `longPollTimeoutMs`).

---

## 6. Node / pnpm / monorepo requirements

- **Published package works standalone**: `@durable-streams/server-conformance-tests@0.3.5` is on the
  public npm registry with everything needed (vitest + fast-check + client are prod deps; the CLI finds
  its own bundled vitest). `npx` is sufficient; no pnpm/monorepo required for chronicle's CI.
- Published engine floor: **Node ≥ 18**. The monorepo itself wants **Node ≥ 22** and **pnpm 10.25.0**
  (`packageManager` field) — relevant only if we run from the cloned repo (e.g. to use unreleased test
  changes via the `caddy` vitest project + src alias, or the `durable-streams-server-conformance-dev`
  tsx wrapper).
- ~25 tests exercise the server through the official TypeScript client (`DurableStream.create`,
  `stream({live})`, `subscribeBytes`, `json()` iterator), so client-observable behavior (header names,
  SSE format) is validated through real client code paths, not just raw fetch.
- Header name constants from the client used in assertions: `Stream-Next-Offset`, `Stream-Cursor`,
  `Stream-Up-To-Date`, `Stream-Seq`.

---

## 7. Beyond conformance: IMPLEMENTATION_TESTING.md in one paragraph

The conformance suite is black-box HTTP only. `IMPLEMENTATION_TESTING.md` mandates separate white-box
suites for what HTTP can't reach, drawn from ElectricSQL production bugs: crash recovery (truncate
corrupt tails, idempotent multi-restart recovery, partial-flush divergence), concurrency (no torn reads
during writes, deletion during active reads, atomic offset persistence under 100 concurrent appends),
resource management (handle leaks, complete cleanup on delete, LRU eviction), cross-backend equivalence
(one suite over memory/file/Redis stores), startup synchronization (no writes before init), and
property-based testing (random op sequences, offset wraparound) with chaos helpers (failure-injecting
store wrappers, file corruption utilities). For chronicle this translates to: a Go store-interface test
suite run against both the Redis store and any in-memory store, Redis-restart recovery tests, and
`go test -race` stress tests over concurrent append/read/delete — kept separate from the conformance
gate, with conformance run first.

---

## 8. Practical checklist for the chronicle conformance gate

1. Serve the protocol at `<base>/v1/stream/*`; suite hardcodes that prefix.
2. Implement PUT/POST/GET/HEAD/DELETE incl. 410/409 soft-delete fork semantics — DELETE is mandatory.
3. Expose a long-poll-timeout config; set it to 500 ms in the harness (default can be 30 s like Caddy).
4. Idempotent producers, stream closure, and forks (incl. sub-offset forks) are **all mandatory**.
5. TTL must be sliding on GET/POST, inert on HEAD; Expires-At absolute; lazy expiry within ~1–2 s observable via HEAD polling.
6. SSE: 2-second flush budget, base64 auto-detection for binary content types, CRLF-injection-proof framing, `streamClosed`/`streamCursor` control-event rules.
7. Run via `npx @durable-streams/server-conformance-tests --run http://localhost:4437` in CI; mirror Caddy's spawn + PUT-probe harness in a vitest wrapper for parity and watch-mode (`--watch ./internal ...`) during development.
8. Leave `subscriptions` off until the webhook/pull-wake subsystem exists; enabling it later is a one-line programmatic flag.
