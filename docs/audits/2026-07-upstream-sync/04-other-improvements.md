# Audit — Other improvements from the reference implementation

**Audit date:** 2026-07-01
**Reference:** `durable-streams/durable-streams` @ `82f9963` (upstream `main` HEAD)

Improvements the reference implementation has that Chronicle could adopt, plus the
places where Chronicle is already *ahead* of the reference (recorded so the delta
is understood and so the gap can be upstreamed).

---

## Worth porting into Chronicle

### IMP-1 — In-band pull-wake callback-token refresh  ★ recommended

- **Category:** functional gap (long-lived leases)
- **Reference** `webhook/crypto.go:36` (`tokenRefreshThreshold = 300`),
  `crypto.go:179-183` (`TokenNeedsRefresh`), `manager.go:374-378`: every successful
  callback rolls the token forward when it is within 5 minutes of expiry, and an
  expired token returns `401 TOKEN_EXPIRED` **carrying a freshly minted token**
  (`manager.go:286-297`), so a heartbeating worker never loses its token.
- **Chronicle:** [`webhook/wire.go`](../../../webhook/wire.go) `AckResponse` carries
  only `{ok, next_wake}` — no token field, no refresh path. Tokens are minted once
  at claim/wake with `tokenTTL = leaseTTLMs + 1h`
  ([`webhook/manager.go`](../../../webhook/manager.go)); `ValidateToken` collapses
  expired and invalid into one failure and returns a bare `401 TOKEN_INVALID`
  ([`webhook/routes.go`](../../../webhook/routes.go)).
- **Why it matters:** a pull-wake worker that legitimately holds a lease and
  heartbeats past `leaseTTL + 1h` (reachable with `lease_ttl_ms` up to the 10-min
  max plus continued acks) gets `401` on its next heartbeat. It then cannot
  `release` (needs a valid token) *and* cannot re-`claim` (still holds the lease →
  `ALREADY_CLAIMED`) — it self-heals only after it stops heartbeating and the lease
  lapses, wasting up to a full lease window of delivery latency. The reference's
  rolling refresh avoids this. PROTOCOL §12.9 permits refresh; §7 is silent — so
  this is spec-legal to add.
- **Remediation:** add an optional `token` field to the ack/claim/release success
  responses and re-mint when the presented token is within a refresh threshold of
  expiry; optionally add a distinct `TOKEN_EXPIRED` code. Keep it additive (gate
  the field on near-expiry) so the conformance assertion
  `toEqual({ok:true, next_wake:false})` still holds, or update the harness.

### IMP-2 — Normalize `StreamRootURL` trailing slash when building `callback_url` / `jwks_url`

- **Category:** config robustness
- **Reference** `webhook/manager.go:419-421`:
  `strings.TrimRight(m.callbackBaseURL, "/") + path` — correct regardless of a
  trailing slash on the configured base.
- **Chronicle** [`webhook/manager.go:308-313`](../../../webhook/manager.go)
  concatenates raw: `m.streamRootURL + "__ds/subscriptions/" + id + "/callback"`
  and `m.streamRootURL + "__ds/jwks.json"`. Correctness depends entirely on
  `StreamRootURL` ending in exactly one `/`. It is built at
  [`cmd/chronicle/main.go:165`](../../../cmd/chronicle/main.go) as
  `strings.TrimSuffix(cfg.PublicBaseURL, "/") + cfg.StreamRoot`, which is safe for
  the default `StreamRoot="/v1/stream/"` — but a user who sets `--stream-root
  /v1/stream` (no trailing slash) produces `…/v1/stream__ds/jwks.json`, a malformed
  URL handed to *external* webhook receivers where it silently breaks
  callbacks/verification.
- **Remediation:** normalize once in `NewManager` (e.g.
  `strings.TrimRight(streamRootURL, "/") + "/"`) or adopt the reference's
  `TrimRight`-then-append helper. Cheap, removes a foot-gun.

### IMP-3 — Optional: a list-subscriptions endpoint

- **Category:** operability convenience (not spec-required)
- **Reference:** `GET …?subscriptions` → `handleListSubscriptions`
  (`webhook/routes.go:71-74,170-182`), `Store.ListSubscriptions`
  (`store.go:63-74`).
- **Chronicle:** no list route; the store has `List()` (ids only, for the sweep)
  but nothing serializes a collection.
- **Assessment:** PROTOCOL §6.3 defines only GET/DELETE-by-id and the conformance
  suite has no list test — **not** a compliance gap. If added, define the shape in
  PROTOCOL.md first (the reference's query-param form doesn't fit the evolved
  path-based API). Low priority.

### IMP-4 — Optional: align control-plane 4xx codes with the reference

- **Category:** clarity (see DIV-3 / DIV-4 in the Caddy-divergence report)
- Return `400 INVALID_REQUEST` when a callback body is missing `generation` /
  `wake_id` (before fencing), and `410` (a dedicated "subscription gone") for a
  callback/ack/release against a deleted subscription, instead of collapsing both
  to `409 FENCED`. Spec-legal either way; improves client-debuggability. Low
  priority.

---

## Where Chronicle is already ahead of the reference (no action; upstreamable)

- **SSRF validation:** Chronicle enforces PROTOCOL §6.2/§12.8 (`webhook/ssrf.go`,
  `400 WEBHOOK_URL_REJECTED`); the reference performs **no** webhook-URL validation
  (`routes.go:125-128` only checks non-empty).
- **Webhook retry backoff:** Chronicle's `RetryDelay`
  ([`webhook/state.go`](../../../webhook/state.go)) is the spec's exponential
  1s→60s + 20% jitter (§7.1); the reference's `calculateRetryDelay`
  (`manager.go:263-269`) is a non-conformant 200 ms→30s-then-60s curve. Chronicle
  correctly does **not** mirror it.
- **Idempotency conflict hash:** Chronicle's `ConfigHash`
  ([`webhook/config.go`](../../../webhook/config.go)) covers the full §6.2 tuple
  (type, pattern, normalized streams, delivery config, lease_ttl_ms, description);
  the reference only compares `Pattern && Webhook` (`store.go:37`).
- **Key rotation / JWKS:** Chronicle persists and serves multiple/rotating signing
  keys (§6.5); the reference serves a single in-RAM key.
- **`__ds` reserved when disabled:** returns `501` rather than minting a stream
  (DIV-2).
- **Injectable clock for expiry:** `IsExpiredAt(now)` with a `Clock` seam
  ([`store/store.go`](../../../store/store.go), issue #26) — a testability win over
  the reference's direct `time.Now()`, behavior-identical.
- **Store-layer empty-body guard:** Chronicle's Redis `Append` rejects
  `len(data)==0 && !Close` with `ErrEmptyBody`
  ([`store/redis/store.go`](../../../store/redis/store.go)) as defense-in-depth
  (the handler guards both backends anyway).

### Upstreaming opportunities (contribute back to `durable-streams`)

These are cases where Chronicle's audit surfaced a latent issue that exists in the
**reference** too — worth a PR upstream, not a Chronicle change:

- **LB-1 (`%016d` offset width):** the reference `store/offset.go:22-23` has the
  identical minimum-width render; past `10^16` any string-order consumer (the
  reference doesn't have one today, but future backends might) inverts. Chronicle
  already has the analysis, counterexample fixture, and ADR-0003 — a ready-made
  upstream report/fix.
- **INV-DIFF-03 (`Stream-Seq` lex-safety):** Chronicle authored a client-precondition
  clarification (see spec report SPEC-1) that the canonical PROTOCOL.md lacks;
  upstreaming it removes Chronicle's local vendored-file delta.
- **Enable the subscription conformance block in the reference harness** (see
  conformance report CONF-1): the reference runs `runConformanceTests(config)`
  without `subscriptions: true`, so it never exercises its own `__ds` surface.

## Primary sources

- Chronicle: `webhook/{manager,routes,wire,state,config,ssrf,crypto,glob}.go`, `webhook/scripts/*.lua`, `store/{store,offset}.go`, `store/redis/store.go`, `cmd/chronicle/main.go`
- Reference @ `82f9963`: `packages/caddy-plugin/webhook/{manager,routes,crypto,store,types}.go`, `store/offset.go`
- Spec: `docs/spec/PROTOCOL.md` §6–7, §12.8–12.10
