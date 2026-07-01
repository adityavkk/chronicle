# Audit — Caddy reference-implementation divergence

**Audit date:** 2026-07-01
**Reference:** `durable-streams/durable-streams` `packages/caddy-plugin` @ `82f9963ae0b489566352393be9b4796c788c99c2` (upstream `main` HEAD)
**Chronicle:** this repo, branch `claude/chronicle-caddy-audit-32c74x`

## Bottom line

Chronicle is an **unusually faithful, near-line-for-line port** of the reference
Caddy plugin across the handler, storage-contract, offset/producer, content-type,
expiry, fork, and `__ds` subscription layers. Both recent reference fixes the
audit specifically looked for are **already adopted**:

- **PR #376 / `f380bca`** — fork content-type validated *before* the source
  refCount is taken (no soft-delete pin leak) — present in the Go store **and** on
  the Redis/Lua path.
- **SSE close-race (PR #376)** — final append drained before the `streamClosed`
  control event — ported verbatim.
- **`92c0821`** — auto-claim producer-batch serialization — present (per-producer
  lock in the Go store; per-stream-atomic Lua on Redis).

The genuinely **observable** divergences are below. Only **DIV-1 is an
actionable defect**; the rest are either Chronicle being *more* spec-compliant, or
spec-acceptable status-code choices, or latent items Chronicle already tracks with
armed CI tests.

Method: each subsystem was diffed against the cached reference source at
`82f9963`; only externally-observable protocol behavior was scored (storage
mechanism, logging, and Caddy-module vs stdlib wiring were excluded by design).

---

## DIV-1 — Idempotent duplicate appends wake subscribers before the dedup short-circuit  ★ actionable

- **Severity:** behavioral divergence (defect — spurious control-plane work)
- **Reference** `handler.go:981-990`: the duplicate check returns `204` **first**,
  and the webhook notify is explicitly gated *"non-duplicate only"*:
  ```go
  if result.ProducerResult == store.ProducerResultDuplicate {
      w.WriteHeader(http.StatusNoContent)
      return nil
  }
  // Notify webhook manager of new data (non-duplicate only)
  if h.webhookManager != nil { h.webhookManager.OnStreamAppend(path) }
  ```
- **Chronicle** [`handler.go:773-795`](../../../handler.go): `h.onStreamAppend(path)`
  is called **unconditionally** right after `Append` succeeds — *before* the
  duplicate short-circuit at line 791-795.
- **Why it matters:** an idempotent-producer retry that the store deduplicates
  (`ProducerResultDuplicate`, 204) writes **no new data**, yet Chronicle still
  fires a subscription wake / webhook-delivery evaluation for it. Impact is
  bounded (the manager usually finds `acked_offset` already at the tail and
  delivers nothing, and subscribers must be idempotent anyway), but it is real,
  observable extra work on the §6 delivery path on *every* deduplicated retry, and
  it contradicts the reference's explicit intent. Not visible on the read path.
- **Remediation:** move the wake past the duplicate `return`, mirroring the
  reference ordering:
  ```go
  if result.ProducerResult == store.ProducerResultDuplicate {
      w.WriteHeader(http.StatusNoContent)
      return nil
  }
  h.onStreamAppend(path)
  ```
  (Header echoes stay where they are; only the wake call moves.)

---

## DIV-2 — `__ds` namespace is reserved even when subscriptions are disabled  (Chronicle *more* compliant)

- **Severity:** behavioral divergence — **Chronicle correct**, reference has a latent gap
- **Reference** `handler.go:69-74`: the `__ds` prefix is only intercepted when
  `webhookRoutes != nil`; otherwise a `PUT {root}/__ds/subscriptions/x` falls
  through and creates an ordinary application stream (`201`), violating PROTOCOL
  §6 ("servers MUST route the reserved `__ds` prefix before normal stream
  operations").
- **Chronicle** [`handler.go:100-112`](../../../handler.go): reserves the namespace
  regardless — with the layer off, `__ds/*` returns `501 Not Implemented` instead
  of minting a stream.
- **Assessment:** divergence in Chronicle's favor; §6 doesn't mandate a status for
  the disabled case, so `501` is fine. No action. Recorded for a complete diff and
  as an upstreamable fix for the reference.

---

## DIV-3 / DIV-4 — Control-plane 4xx codes differ from the reference (spec-acceptable)

- **Severity:** behavioral divergence (low; both are spec-legal)
- **DIV-3 — malformed callback body:** the reference validates presence and returns
  `400 INVALID_REQUEST "Missing required field: epoch"` (`webhook/routes.go:221-224`).
  Chronicle unmarshals absent `generation`→`0` / `wake_id`→`""`, which then fail
  the fence as `409 FENCED` ([`webhook/routes.go:214-228`](../../../webhook/routes.go),
  `webhook/scripts/ack.lua:36`). A syntactically-valid-but-incomplete body yields
  `409` instead of `400`.
- **DIV-4 — callback for a deleted subscription:** the reference returns a
  dedicated `410 CONSUMER_GONE` (`webhook/types.go:114,125`; `manager.go:273-282`);
  Chronicle maps missing-subscription (`NOSUB`) to `409 FENCED`
  ([`webhook/routes.go:224-227`](../../../webhook/routes.go)).
- **Assessment:** PROTOCOL §7 only requires such callbacks to *fail without
  advancing cursors*, and the conformance suite tests neither case — `409 FENCED`
  satisfies "MUST fail." Aligning is a clarity nicety, not a compliance fix. See
  the *other-improvements* report for the (optional) story.

---

## DIV-5 — Redis read model diverges from the memory oracle in two latent regions (already tracked)

- **Severity:** latent behavioral divergence — **not observable today**, CI-armed
- **DIV-5a (LB-3, ReadSeq):** the reference and Chronicle memory stores compare
  **`ByteOffset` only** in the read window (`memory_store.go:742`;
  [`store/memory_store.go:708`](../../../store/memory_store.go)), while Chronicle's
  Redis `read.lua:27` + `store/redis/keys.go:98-100` range-scan the **full**
  `offset.String()`. They agree while `ReadSeq` is always `0` ("future log
  rotation", never set). Armed in
  [`store/readseq_divergence_test.go`](../../../store/readseq_divergence_test.go).
  **Tracked:** [#48](https://github.com/adityavkk/chronicle/issues/48), [#32](https://github.com/adityavkk/chronicle/issues/32).
- **DIV-5b (LB-1, offset width):** `offset.String()` renders `%016d` (a *minimum*
  width); at a field `≥ 10^16` the Redis ZSET-lex member order inverts, whereas the
  numeric `Offset.Compare` used by the oracle stays correct. ~10 PB/stream, purely
  theoretical. **Tracked:** [#46](https://github.com/adityavkk/chronicle/issues/46),
  [#27](https://github.com/adityavkk/chronicle/issues/27), ADR-0003.
  **Note:** the reference `offset.go:22-23` has the *identical* `%016d` code, so
  this is a mirrored-from-upstream latent bug, not a Chronicle-introduced
  divergence (see the *other-improvements* report for the upstreaming angle).

---

## DIV-6 — Cosmetic: Redis fork-offset validation omits an always-false disjunct

- **Severity:** cosmetic (behavior identical)
- Reference `memory_store.go:213` and Chronicle memory `store/memory_store.go:175`
  guard `forkOffset.LessThan(ZeroOffset) || sourceMeta.CurrentOffset.LessThan(forkOffset)`;
  Chronicle's Redis path ([`store/redis/store.go:303`](../../../store/redis/store.go))
  drops the first disjunct. Offsets are unsigned so `LessThan(ZeroOffset)` is always
  false — identical behavior. Re-add only for line-by-line parity.

---

## Confirmed faithful (no findings)

Status codes and error bodies for every method (PUT 201/200/409/400/404,
POST 204/200/400/404/409/410/403, GET 200/204/304/400/404/410, HEAD, DELETE,
OPTIONS 204, 405); all response headers (`Stream-Next-Offset`, `Stream-Closed`,
`Stream-Up-To-Date`, `Stream-Cursor`, `ETag`, `Cache-Control`, CORS/`Access-Control-*`,
`X-Content-Type-Options`, `Cross-Origin-Resource-Policy`, `Stream-SSE-Data-Encoding`,
`Producer-*`, `Location`); offset parsing (`-1`/`now`/numeric/sub-offset);
`Stream-Fork-Source` / `Stream-Fork-Sub-Offset` (empty-vs-absent via `Values()`);
long-poll timeout/204/cursor; SSE framing, base64 auto-detect, CRLF-injection
splitting, control-field selection, reconnect timer; cursor epoch/interval/jitter;
producer state machine (first-contact, stale-epoch, epoch-seq, dup, seq-gap) with
identical Go↔Lua mirroring; content-type normalization (incl. the deliberate
fork-full-string vs append-media-type asymmetry); expiry strict-`>` boundary,
sliding-TTL touch order, soft-delete-on-expiry with `RefCount>0`; fork refCount
decrement/underflow-clamp/cascade and InitialData-failure rollback. Glob matching,
Ed25519 signing input + `Webhook-Signature` format, RFC-7638 `kid`, and the JWKS
JWK shape are byte-for-byte logical ports (see the webhook section of the
*other-improvements* report).

## Primary sources

- Chronicle: `handler.go`, `handler_sse.go`, `webhook/{routes,manager}.go`, `store/{memory_store,offset,producer,store}.go`, `store/redis/{store,keys,scripts/*.lua}`
- Reference @ `82f9963`: `packages/caddy-plugin/handler.go`, `store/{memory_store,offset,store}.go`, `webhook/{routes,manager,types}.go`
- Upstream: PR [#376](https://github.com/durable-streams/durable-streams/pull/376) (`f380bca`), commit `92c0821`
