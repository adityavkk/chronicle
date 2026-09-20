# ADR-0009: The read-only claim/verify route

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** @adityavkk
- **Tracking issue:** [#192](https://github.com/adityavkk/chronicle/issues/192)

## Context

Write fencing (ADR-0008, [WRITE-FENCING.md](../spec/WRITE-FENCING.md)) makes
the append path authority-free for the client: the write token is validated
atomically with the write, and a holder never has to decide for itself
whether it is still current. A control-plane operation that carries the same
token but performs no write has nothing to defer to. The consuming agent
platform hits this on delegation: an activation must prove it is the current
holder of its subscription before it spawns a child, sends to another entity,
or signals. The token is an HMAC over `ds-write-v1.<payload>` under a key only
chronicle holds, so the client cannot verify it locally, and accepting it on
shape alone is impersonation. The consequence in that platform (its ADR-0015):
after a restart of the component that once minted delegation authority, or on
a second replica, delegation answers `401` while appends succeed.

The alternatives were to share the HMAC key with that component (a second
root of trust, and every replica of it a place the key can leak) or to change
the token format to a signed (JWS) capability the client can verify offline
(out of scope for #192: a format change with its own rollout). Both would
leave the client deciding a fence question the server already answers on
every append.

## Decision

1. **One read-only route.** `POST /__ds/subscriptions/{id}/claim/verify`,
   no body (a body is ignored). The write token is the sole credential,
   read from the append gate's carriers in its order — `Write-Token`,
   `electric-claim-token`, then `Authorization: Bearer` — with the same
   malformed-carrier rule. As for the fenced write class, no service or agent
   principal is consulted, there is no telemetry-only path, and the rule
   binds in every `CHRONICLE_AUTH_MODE`. No new principal class.
2. **The predicate is the append pre-check.** Verify runs
   `check_write_fence.lua` — incarnation, dispatch type, phase, holder,
   generation, `wake_id`, lease — through the same `ParseWriteToken` parser
   and the same live-state arm as `AuthorizeAppendFence`. The script's `OK`
   reply gains the claim's `lease_until_ns` (additive; `FENCED` and `NOSUB`
   stay bare) so the `200` body is built from the same atomic read as the
   decision, never from a second `Get`. That single slot-homed `EVAL` is the
   linearizability argument: it shares the atomicity domain of `claim.lua`,
   `ack.lua`, and `release.lua`.
3. **Status parity, control-plane vocabulary.** `200`, `401`, and `409` are
   the statuses a fenced write under the same token would receive at that
   instant (WF-29). The `409` is byte-for-byte the append pre-check's
   envelope — `FENCED`, reason `precheck`, message `write token claim is
   fenced`, no generation or holder. The `401` codes follow the `__ds`
   convention (`TOKEN_INVALID`, `TOKEN_EXPIRED`) rather than the data
   plane's `UNAUTHENTICATED`: parity is on status and on the fence envelope,
   and a `__ds` client already speaks these codes.
4. **`TOKEN_EXPIRED` carries no refreshed token.** The ack route's in-band
   refresh (`writeTokenRejected`) mints; verify never mints, renews, or
   writes anything. The expired answer is terminal for the holder.
5. **An unknown or deleted subscription is `409`, not `404`.** `404` is
   reserved for "route absent": a server without this route answers the path
   `404`, and a client falls back to its local policy. If a deleted
   subscription's token also drew a `404`, the client could not tell the two
   apart and would fall back to accepting a dead token.
6. **`Cache-Control: no-store`, and a client cache bound (WF-30).** A client
   may cache a positive answer no longer than the smaller of its own
   heartbeat interval and the remaining lease from `lease_until_ms`; it
   never caches a `401` or `409`. Verify does not extend the lease, so
   polling it cannot keep a claim alive.
7. **`streams` is the token's scope.** The `200` body lists the token's
   normalized scope paths, not stream snapshots: verify reads no stream
   tails, so its cost does not grow with the number of links.
8. **A store failure is `500`.** The append gate maps a fence-store error to
   `401 write token fence unavailable`; verify reports `500 internal error`
   instead, because a client would act on — and per WF-30 could not
   distinguish — a `401` as a definitive negative.
9. **Specified as §9.1 of WRITE-FENCING.md.** The subsection keeps §10–§12
   and the appendix anchor stable, sits inside the §11.1 extension (additive,
   conditional on a fenced stream), and gets its own conformance rows.

## Consequences

- **`check_write_fence` reply ABI.** The `OK` variant declares one
  `unix_ns` field; the decoder and the ABI differential (`script_abi_test`)
  changed with it, and `Store` gained `VerifyWriteFence` (status plus lease)
  with `CheckWriteFence` as its status-only view over the same `EVAL`. Any
  `Store` double that does not embed the interface must add the method.
- **The seal crash window is a documented residual.** The pre-check does not
  consult the stream-slot seal. Between a `done`'s seal and the control-plane
  idle (ADR-0008, WRITE-FENCING.md §10 at-least-once completion), verify
  answers `200` while the append answers `409 sealed`. The in-slot rung
  remains the authority; WF-30's cache bound covers the window; the appendix
  records it. A per-stream read-only seal probe (one round-trip per linked
  stream) could close it later without changing the route's contract.
- **Metrics are deferred.** A `chronicle_claim_verify_total{outcome}` counter
  would touch every `Metrics` fake and adapter; the route ships without one,
  so the consumer's fallback rate is observable only from its own side for
  now.
- **No latency acceptance clause.** The issue asked for p99 within the
  append pre-check budget on the ds-bench configuration, but no such budget
  or fence scenario exists in `benchmarks/ds-bench`; the clause is dropped
  rather than asserted. Structurally the route costs one `EVAL` on a
  slot-homed hash, the same as the pre-check it reuses.
- **Consumer coupling.** The consuming platform's patch must treat
  `TOKEN_EXPIRED` without a token as terminal, cache `200` no longer than
  WF-30 allows, never cache `401`/`409`, and treat `404` as "verify
  unavailable". Landing upstream first keeps its copy of WRITE-FENCING.md a
  verbatim mirror.

## Rejected alternatives

- **Sharing the HMAC key with the consumer** — a second root of trust with as
  many copies as that component has replicas; the fence question stays
  answered client-side.
- **Signed (JWS) write tokens** the client verifies offline — a token format
  change with its own rollout; explicitly out of scope for #192, and it still
  could not answer the live-claim half (deposed, released, lapsed) offline.
- **`GET`** — puts the credential in a method that caches and logs by default.
- **`404` for an unknown subscription** — collapses "route absent" and
  "subscription gone" into one signal (decision 5).
- **A `Get`-then-decide implementation** (`RedisStore.Get` followed by the Go
  mirror `WriteFenceDecision`) — two reads, so a deposition between them
  yields a `200` for a superseded token; the issue's linearizability clause
  forbids it.
- **Consulting `authenticateCaller` / `controlDeny`** — a second principal
  class the issue rules out, and a telemetry-only verify in `insecure` mode
  would be meaningless.
- **Amending ADR-0008 in place** — Accepted ADRs are immutable
  ([README](README.md)); a course change is a new record.
