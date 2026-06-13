# 09 — Subscription layer implementation plan

**Purpose:** turn the design in [07](07-subscription-wake-lease-durability.md) into
a concrete, conformance-passing, crash-resilient `__ds` subscription layer for
chronicle. This doc records the decisions that [07](07-subscription-wake-lease-durability.md)
left open once the wire contract was pinned to the conformance suite, and the
package/keyspace/test layout the implementation follows.

**Status:** in progress on branch `subscription-durability`. Success is defined as
the eight reserved-subscription conformance tests (suite `0.3.5`) passing with the
rest of the suite still green, plus the Jepsen-style failure matrix in §6.

---

## 1. The contract is the conformance suite, not the Caddy checkout

[07](07-subscription-wake-lease-durability.md) named the Caddy `webhook` package as
the thing to port. Pinning the wire format to
`@durable-streams/server-conformance-tests@0.3.5` (the suite chronicle is graded
against) surfaced that the pinned Caddy checkout (`webhook` @ `f28793c`) is an
**earlier** design than the protocol the suite encodes:

| | Caddy `webhook` checkout | PROTOCOL §6–7 / conformance 0.3.5 (target) |
| --- | --- | --- |
| Addressing | query param `?subscription=<id>` | REST `__ds/subscriptions/:id` |
| Unit of state | `ConsumerInstance` per (sub, stream) pair | one subscription spanning many stream links |
| Fencing counter | `Epoch` (per consumer) | `generation` (per subscription) |
| Lease holder | implicit (liveness timer only) | explicit `holder` / `current_holder` |
| Delivery modes | webhook only | `webhook` **and** `pull-wake` |
| Fence error | granular (`STALE_EPOCH`, `ALREADY_CLAIMED`, …) | single `FENCED` for callback/ack/release |
| URL safety | none | `WEBHOOK_URL_REJECTED` (SSRF) required |

So the implementation targets **PROTOCOL §6–7 + the conformance suite**, keeping
Caddy's *engine structure and vocabulary* where it survives the protocol
(`Subscription`, `Manager`, `Routes`, `Store`, the IDLE/WAKING/LIVE state machine,
`OnStreamAppend`/`OnStreamCreated`/`HasPendingWork`/`wakeConsumer`/`deliverWebhook`/
`scheduleRetry`/`resetLivenessTimeout`), and adopting the protocol's nouns on the
wire and in the records (`generation`, `holder`, `lease_ttl_ms`, `link_type`,
`acked_offset`, `wake_id`, `pull-wake`). The package keeps the name `webhook` to
match Caddy's directory so the parity diff stays file-aligned as upstream catches
up. The `epoch → generation` rename is the one deliberate vocabulary change and is
noted in [CADDY-PARITY.md](../CADDY-PARITY.md).

## 2. Cluster-safety: one `{__ds}` slot for the control plane

[07](07-subscription-wake-lease-durability.md) §4.3 proposed folding wake fan-out
into `append.lua`. That cannot hold in Redis Cluster: `append.lua` is scoped to a
single stream's `{<path>}` hash tag for cluster-safety, but a subscription fans
out across many streams (many slots). A single Lua call cannot span them.

Resolution — two rules:

1. **The entire subscription control plane shares one hash tag, `{__ds}`.** Every
   subscription record, the schedule ZSETs, the id index, the fan-out SETs, and the
   JWKS live in that one slot, so every correctness-critical transition (create,
   arm-wake, claim, fence-ack, release, heartbeat, lease-expiry) is a single-slot
   atomic Lua script — cluster-safe by construction. The high-cardinality
   per-stream log data stays sharded under `{<path>}`; only the control plane (far
   lower cardinality, like a coordinator) is centralized. This is the documented
   trade-off.
2. **Wake creation is event-driven *and* swept.** An append cannot atomically arm a
   cross-slot wake, so the append-time hook (`OnStreamAppend`, fired by the handler
   after a durable append, exactly as Caddy fires it) is a best-effort low-latency
   path. The **recovery sweep** ([07](07-subscription-wake-lease-durability.md) §8)
   is the durability backstop: it recomputes `HasPendingWork` from durable cursors
   and re-fires anything owed. A wake lost to a crash between append and hook is
   recovered by the next sweep. This is [07](07-subscription-wake-lease-durability.md)
   §6.3 made load-bearing rather than optional.

`HasPendingWork` compares each link's `acked_offset` against the stream tail. The
tail lives in the stream's `{<path>}:meta` slot, not `{__ds}`, so the comparison is
computed in Go (reading tails through the store) and the *result* is handed to the
arm-wake script — never inside a `{__ds}` Lua. This mirrors Caddy, where
`HasPendingWork` is Go code over a `getTailOffset` callback.

## 3. Keyspace (all under the `{__ds}` tag)

```
ds:{__ds}:sub:<id>            HASH  type, pattern, webhook_url, wake_stream,
                                    lease_ttl_ms, description, cfg_hash, created_at,
                                    status, phase, generation, wake_id, holder,
                                    holder_worker, lease_until_ns, retry_count,
                                    first_fail_ns, next_attempt_ns
ds:{__ds}:sub:<id>:links      HASH  field=<stream path> -> "<link_type>:<acked_offset>"
ds:{__ds}:subs                SET   all subscription ids (sweep + list)
ds:{__ds}:stream:<path>       SET   fan-out: sub ids linked to <path>
ds:{__ds}:sched:lease         ZSET  sub_id -> lease_expiry_ns
ds:{__ds}:sched:retry         ZSET  sub_id -> next_attempt_at_ns
ds:{__ds}:jwks                HASH  kid -> "<priv_b64url>:<pub_b64url>:<created>:<status>"
ds:{__ds}:active_kid          STR   current signing kid
```

The ZSETs are the two Caddy goroutine timers made durable
([07](07-subscription-wake-lease-durability.md) §4.3): `resetLivenessTimeout` →
`sched:lease`, `scheduleRetry` → `sched:retry`. They are due-scored; the worker tick
pops due members with an atomic **re-score** claim (never `ZREM` —
[07](07-subscription-wake-lease-durability.md) §6.1) so a worker that dies
mid-delivery leaves the item to fall due again.

## 4. State machine (the pure core)

`phase ∈ {idle, waking, live}`, `generation` monotonic per subscription.

- **idle → waking** (a wake is issued): `generation++`, mint `wake_id`. Webhook:
  arm the lease at issue (`lease_until = now + lease_ttl`, `ZADD sched:lease`) and
  POST. Pull-wake: append a `{type:"wake", …, generation}` event to `wake_stream`;
  no lease yet (the lease starts at claim, PROTOCOL §7.3). The gate to issue is
  "pending work AND phase == idle" — coalescing falls out of the phase check.
- **waking/idle → live** (claim or first webhook callback): set `holder`
  (`holder_worker` for pull-wake), arm/extend the lease, `phase = live`. A pull-wake
  claim while `phase == live` and the lease is unexpired is `409 ALREADY_CLAIMED`
  with `current_holder`.
- **live → live** (heartbeat: ack without `done`): extend the lease
  (`ZADD sched:lease now+ttl`).
- **→ idle** (ack/callback with `done:true`, or voluntary release): apply acks
  (forward-only cursor advance), clear holder + wake, `ZREM` both schedule ZSETs,
  recompute pending → `next_wake`. If pending, re-issue (a new generation).
- **lease expiry** (sweep pops `sched:lease`): clear holder + wake; if pending,
  re-issue (PROTOCOL §7.3).

**Fencing** (`fence.lua`, every callback/ack/release): reject unless bearer token
valid for the sub, token generation == current, request `generation` == current,
request `wake_id` == current → `409 {"error":{"code":"FENCED"}}`. The fence — not
the lease — is the correctness mechanism; the lease only bounds redelivery latency
([07](07-subscription-wake-lease-durability.md) §6.2, §9). Delivery is at-least-once
made safe by the fence.

**Backoff** (PROTOCOL §7.1): exponential from 1 s to 60 s with 20% jitter;
`next_attempt_ns` persisted in the record and in `sched:retry` (satisfies L1079).
`status` is `failed` while a retry is scheduled, else `active`.

## 5. Package layout (`webhook/`) and pure-core / imperative-shell split

Pure core (deterministic; clock and randomness are arguments):
`glob.go` (verbatim GlobMatch), `config.go` (normalize + `cfg_hash` + lease clamp),
`ssrf.go` (URL classifier; the resolver is injected), `state.go` (the reducer:
`HasPendingWork`, phase transitions → directives, `CheckFence`, `RetryDelay`),
`wire.go` (DTOs + pure mappers). Unit-tested table-driven, no Redis.

Imperative shell: `crypto.go` (Ed25519 key persisted to `ds:{__ds}:jwks`, JWKS,
`Webhook-Signature` over `<ts>.<body>`, HMAC callback/claim tokens, `wake_id`),
`store.go` + `redis_store.go` + `keys.go` + `scripts/*.lua` (the Redis
`SubscriptionStore`), `manager.go` (hooks, HTTP delivery, claim/ack/release, the
lease + retry worker ticks, the recovery sweep + pattern backfill), `routes.go`
(the `__ds` HTTP surface). Wiring: the `ServeHTTP` re-entry comment becomes a
`Routes.HandleRequest` call; `mount.go` drops the `__ds` 501; `main.go` constructs
the Manager and runs its background loops; `config.go` gains the public stream-root
URL used to build `callback_url`/`jwks_url`.

## 6. Test plan

1. **Pure-core unit tests** — glob, config hashing/idempotency, SSRF classification
   (the `10.0.0.1` reject, the `127.0.0.1` dev allow), the reducer's transitions and
   fence decisions, backoff bounds.
2. **Redis integration tests** — each Lua script against live Redis (db 15):
   create/confirm idempotency, link/unlink, arm-wake generation bump, claim CAS,
   fence-ack forward-only cursor, heartbeat extension, due-pop re-score.
3. **Conformance** — `subscriptions: true` in the harness; all eight reserved tests
   plus the existing suite green (`make conformance`).
4. **Jepsen-style failure matrix on k3d** (closes the loop with
   [07](07-subscription-wake-lease-durability.md)'s crash windows): origin restart
   mid-wake, lease-holder kill, Redis failover / AOF loss window, network partition
   between origin and Redis. Property checked: every durably-appended message is
   eventually delivered at-least-once and the fence prevents a double-advanced
   cursor. Each scenario's observed behavior documented under `docs/jepsen/`.
```
