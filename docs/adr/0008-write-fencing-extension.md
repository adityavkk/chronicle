# ADR-0008: The write-fencing protocol extension

- **Status:** Accepted
- **Date:** 2026-08-31
- **Deciders:** @adityavkk
- **Tracking issue:** [#183](https://github.com/adityavkk/chronicle/issues/183)

## Context

Issues #143, #168, #169, and #182 built the claim fence as *implementation
behavior*: a write token minted at claim, a stream-slot marker checked inside
the append transaction, lease-lapse fencing against the script's own clock.
None of it was specified, none of it bound writers that never presented a
token, and the client-declared `Producer-Epoch` of PROTOCOL §5.2.1 remained a
hole — a zombie writer with a stable producer id could keep writing (or
auto-claim a higher epoch) without any credential at all.

The consuming agent platform's ADR-0003 — its write-fencing consumer
contract, clauses cited here as c1–c11 — asked for the missing half: exactly
one writer per activation stream, deposed writers refused deterministically on
their first response, completion as final as deposition, and the whole thing
as a documented protocol surface rather than a chronicle internal. Two writer
populations complicate that ask: agent runtimes write activation output under
a claim, while the agent server writes command traffic (inbox, wake, signal,
manifest, shared state) with no claim at all, and both target the same
streams.

Prior art shaped the answers: Kafka KIP-890 (server-validated producer epochs
bound to a coordinator-owned generation), HDFS QJM (writer epochs granted by a
quorum, fenced at the journal), and BookKeeper (ledger fencing plus a
sealed-at-close last-entry record).

The full design synthesis is
[docs/spec/WRITE-FENCING.md](../spec/WRITE-FENCING.md) (the wire contract)
plus the #183 design record; this ADR records the decisions and their costs.

## Decision

1. **Opt-in stream property.** A stream is fenced iff created with
   `Write-Fence: true`. The flag is stored in stream meta (`wf`), part of the
   idempotent-create comparison, echoed on `PUT`/`HEAD`, and never
   fork-inherited. No namespace-level default.
2. **Server-derived write class, default open.** On a fenced stream every
   POST is classed server-side. The class is **fenced** iff the request
   presents a write-token carrier or asserts the class with `Write-Fence:
   true`; it is **open** otherwise. A bound producer id does not change the
   class: an open write naming one stays open and is refused in-slot
   (decision 3). Command writers keep their 3-line pass-through; no client
   negotiates a mode.
3. **Producer binding.** A producer id accepted on the fenced class is bound
   (`wfbind:<id>` in meta): an open write naming it is `409 FENCED
   reason=bound`. This closes the real zombie shape — a stable
   `Producer-Id: entity-<url>` that lost its token.
4. **The assertion header.** `Write-Fence: true` on POST demands a valid
   write token on any stream in any mode (`401` without one), so a gateway
   can make a lost token loud instead of silently downgrading to an open
   write. When the assertion rides with a routed service principal, both
   must verify; the principal is evaluated first, so a denied principal is
   reported as its own `401`/`403` and the token arm runs only for an
   allowed principal (a deliberate ordering deviation from the design's
   "both valid" phrasing — the request is refused either way).
5. **Writer epoch ≡ claim generation.** `Producer-Epoch` on the fenced class
   must equal the token's generation, compared in the stream slot before
   `validate_producer`. No second epoch register.
6. **Per-authority seal in stream meta.** `done`/release/delete/unlink seal
   the current generation per authority (`wfseal:<auth>` =
   `<gen>:<wake>:<offset>`), a successor's grant seals its live predecessor
   (supersession), and grant/append refuse `generation <= seal` for that
   authority. A recreated subscription is a new authority and starts
   unsealed.
7. **Derived webhook holder.** Webhook wakes hold the claim as
   `wake:<wake_id>` (derived in Go; no control-plane Lua change), giving
   webhook consumers full token parity: minted before delivery, refreshed on
   callback heartbeats, sealed at done — fail-open delivery, fail-closed
   token. Consequently an explicit `POST /claim` on a webhook-dispatch
   subscription is refused `400 INVALID_REQUEST` before any lease is
   granted: the claim is the §7.2 pull-wake acquisition, and a worker-held
   webhook claim is a shape the fence liveness rules (ack.lua heartbeats,
   `check_write_fence.lua`) deliberately reject. Before #183 such a claim
   answered 200 with a write token; that token now could never verify, so
   the category error is made explicit.
8. **`409 FENCED` with disclosure.** Fence rejections are `409` with the JSON
   envelope (`reason`, `generation`, `current_holder`) and — when producer
   headers were sent — `Producer-Epoch: <current generation>` plus the
   terminal gap pair `Producer-Expected-Seq == Producer-Received-Seq ==
   <request seq>`, which the pinned Durable Streams `IdempotentProducer`
   treats as a clean stop on the first response. Never
   `Stream-Next-Offset`.
9. **Mode split.** Fence semantics (token validity, producer rules,
   marker/seal/epoch/bound checks, the wake-token 403) bind in every
   `CHRONICLE_AUTH_MODE`; only the open-class principal requirement follows
   the mode (telemetry in `insecure`). A MAC-valid token on an *unfenced*
   stream keeps today's mode-dependent posture. In `enforce` the anonymous
   open write is refused by the base phase-1 credential gate *before* the
   stream lookup, so it is the plain `401` with no fence disclosure — no
   `reason=principal` is ever emitted (WF-26: an unauthenticated caller
   learns nothing about the stream); the wire vocabulary of WF-24 therefore
   omits `principal`, and the code keeps it only as a classify backstop.
10. **Shard 0 only.** Both token mints hardcode shard 0; a token naming any
    other shard is refused in phase 1 and no marker is ever granted for it.
11. **Wrap, never modify, `ValidateProducer`.** The fence is a rung *before*
    the base producer state machine; the §5.2.1 semantics and its Lua/Go
    mirrors are untouched.
12. **MemoryStore parity.** The in-memory store implements markers, seals,
    and binding over the same pure predicate (`store.EvaluateWriteFence`),
    so the equivalence harness and the root handler tests cover the fence.
13. **The cross-slot pre-check stays.** `check_write_fence.lua` (now with a
    webhook branch) keeps refusing before the store call; the in-slot rung
    remains the authority.

## Consequences

- **Base protocol unchanged.** Streams that never opt in take exactly the
  pre-#183 path; the pinned conformance suite stays 332/332 and
  `PROTOCOL.md` is untouched — the extension lives in
  [WRITE-FENCING.md](../spec/WRITE-FENCING.md).
- **Meta growth.** A fenced stream's meta hash gains one field per authority
  (`wfseal:<auth>`), one per bound producer (`wfbind:<id>`), and three
  summaries (`wf`, `wfSealGen`/`wfSealOff`, `wfLastOff`). All are read by
  name; old replicas ignore them.
- **Create-probe ABI shift.** `create.lua`'s config-match probe gained a
  `writeFence` ARGV, shifting the meta-field-count argument and everything
  after it. The script and its caller ship in one binary, so the shift is
  internal — but a mixed-version rollout means an old replica ignores
  `Write-Fence` on PUT and creates the stream *unfenced*. Rollout rule: roll
  chronicle fully before any producer starts sending the header, and smoke
  the `HEAD` echo.
- **The marker-reap residual on unfenced streams.** On streams that never opt
  in, no seal is written, so the pre-#183 tombstone-retention window (a
  delayed grant after the tombstone is reaped) remains exactly as it was.
  Fenced streams close it via the per-authority seal.
- **The recreated-subscription epoch limitation.** A recreated subscription
  starts at generation 0 while the stream's stored producer epoch survives,
  so a runtime reusing its stable producer id gets the base `403` stale-epoch
  answer (then a terminal `409 epoch` after its auto-claim bump) until the
  new authority's generation passes the stored epoch. Liveness, not safety;
  the recreate-starts-unsealed half is pinned by
  `TestAppendFenceSealPerAuthorityIsolated`.
  **Follow-up:** seed a recreated subscription's generation above its
  predecessor's from a surviving per-id high-water key.
- **One authority per fenced stream** is a documented deployment MUST
  (WRITE-FENCING.md §10); cross-authority non-interleaving is deliberately
  not claimed (INV-FENCE-05 is per authority).

### Departures from the consumer contract (ADR-0003)

| Clause | Position taken | Why |
|---|---|---|
| c3 "the same generation re-presented returns the identical grant" | Keep chronicle: a busy claim answers `409 ALREADY_CLAIMED {current_holder, generation}`; the token is re-minted on every heartbeat. | The intent — a timed-out claimant never fences itself — holds; the fence does not rotate while the lease is live. |
| c5 "no append is ever judged against a wall clock" | Keep chronicle: the lease check compares `lease_until_ns` to the single `nowNs` argument of the same atomic script. | Settled by #177/#182; the deadline is one the holder itself extended, and the clock is the transaction's, not a second reader's. |
| c6 supersession *record* | Chronicle records the supersession **seal** (meta + `HEAD` headers); an `activation_superseded` *event* stays a consumer responsibility. | A server-fabricated event would change stream content — a base-protocol violation. |
| c7 "`done` bumps the epoch" | `done` **seals** the generation; the successor's claim bumps it. | Equivalent for the invariant (INV-FENCE-02 unchanged); leaves the control-plane model untouched. |
| c8 "the existing 403" | Fence rejections are `409 FENCED`. | The pinned client's 403 branch auto-claims (retry loop); its 409 branch with the terminal pair stops cleanly on the first response. Status contract over letter. |

## Rejected alternatives

- **Class ≡ credential presence alone** — an assertion-less design cannot
  make a lost token loud (decision 4) and leaves the bound-producer hole to
  convention.
- **A per-request class header as the sole signal** — every command writer
  would need the header before any stream could be fenced, breaking the
  pass-through that makes the opt-in deployable.
- **Fenced-by-default streams** (or a namespace-default env,
  `CHRONICLE_WRITE_FENCE_NAMESPACES`) — flipping the env breaks a base
  client's idempotent re-`PUT` (config mismatch on a header it never sent),
  and default-fenced forces every command writer to change first.
- **A separate writer-epoch register** — a second counter needs its own
  bump/seal rules for no invariant gain; the claim generation already *is*
  the writer epoch both mints hardcode.
- **A marker-borne seal** (or encoding the binding in the `prod` value) —
  markers are reaped (a seal must outlive them), and the `prod` encoding is
  parsed by every replica on every `Get`: changing it breaks old replicas
  mid-rollout. Meta fields read by name are ignored by old code.
- **A seal *event* appended to the stream** — server-fabricated content on a
  data stream; see departure c6.
- **A generation-only (global) seal** — bricks a recreated or second
  subscription, which restarts at generation 0 below the global seal. The
  per-authority key gives the strong `<=` refusal *and* recreate safety; the
  `globalseal` fault model must (and does) violate `SealIsolation`.
- **A `(gen, wake)`-equality seal** — overwritten by the next seal, so a
  delayed grant for an older sealed generation revives it after tombstone
  reap; the `noseal` fault model's `DelayedGrant` trace is exactly this.
- **`403` for fence rejections** — see departure c8.
- **Extra response headers on the 409** (`Write-Fence-Generation`,
  `Stream-*` variants) — no reader exists; the JSON envelope plus the
  producer echo/pair is sufficient and keeps the CORS expose list small.
- **Removing the cross-slot pre-check** — unrequested scope with its own
  pinned tests; under a delayed grant it is a second line of defense.
- **Refusing MemoryStore parity** (501 on the in-memory store) — would leave
  the fence outside the equivalence harness and the handler tests, exactly
  where an atomicity bug would hide.
- **ARGV-passed class/principal attestations in Lua** — vacuous inputs the
  script cannot verify; every fence input is read from meta or derived from
  the marker key inside the transaction.
