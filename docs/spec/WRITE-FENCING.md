# Durable Streams Extension: Write Fencing

This document specifies **Write Fencing**, an extension to the Durable Streams
Protocol ([PROTOCOL.md](./PROTOCOL.md)). It binds the append data plane to the
subscription generation fence of §7.3: a stream that opts in accepts writes
from a woken worker only under a **write token** minted for the current claim,
and a deposed, lapsed, or completed claim can never land another byte.

Status: implemented (the [appendix](#12-appendix-chronicle-implementation-notes)
holds the implementation notes); written as a §11.1 protocol extension so
sections 1–11 can be proposed upstream verbatim.

## 1. Scope and conformance language

This extension is a **pure superset** of the base protocol in the sense of
§11.1: every rule below is conditional on a stream that was created with the
`Write-Fence: true` header, and base protocol operations remain functional
without extension support. On a stream that never opts in, a conforming server
behaves byte-for-byte as the base protocol requires, and a base client never
observes this extension.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in BCP 14 [RFC 2119] [RFC 8174] when, and only when,
they appear in all capitals, as shown here.

Rule identifiers `[WF-nn]` mark the testable obligations of this extension;
the appendix maps each identifier to its conformance test.

## 2. Terminology

**Fenced stream**: a stream created with `Write-Fence: true`. The fencing
rules of this document apply only to fenced streams.

**Write token**: an opaque credential minted by the server for one
subscription claim (§7.2), scoped to the claim's linked streams, that
authorizes the **fenced** write class for exactly the claim's
`(generation, wake_id, holder)`.

**Claim generation**: the §7.3 subscription-level fencing counter. Under this
extension it is also the **writer epoch** of the fenced class.

**Authority**: one incarnation of one subscription. The fence, its seal, and
the non-interleaving guarantee are per authority; deleting and recreating a
subscription creates a new authority.

**Fenced write**: a POST classed as riding the write token (§5 of this
document). Subject to the marker, seal, and epoch checks.

**Open write**: every other POST — the base §5.2/§5.2.1 write of an
authenticated principal. Unchanged by this extension except for the bound
producer rule (§6) on fenced streams.

**Bound producer**: a `Producer-Id` for which a fenced write has been accepted
on a given fenced stream. From then on that id belongs to the fenced class on
that stream.

**Seal**: the durable per-authority record that a claim generation is closed
on a stream. Written when the holder completes (`done`), releases, or is
superseded, and when the subscription or stream is torn down.

## 3. Creating a fenced stream

A stream opts in at creation:

```http
PUT /v1/stream/agents/e1/session
Content-Type: application/json
Write-Fence: true
```

- The server MUST treat `Write-Fence: true` on `PUT` as part of the stream's
  configuration for the idempotent-create comparison of §5.1: a re-`PUT` of an
  existing stream MUST agree on the fence (else
  `409 stream exists with different configuration`), and a re-`PUT` that
  agrees is a `200` as today. **[WF-01]**
- The server MUST echo `Write-Fence: true` on the `PUT` response (`201` and
  idempotent `200`) and on `HEAD` for a fenced stream. **[WF-02]**
- A fork of a fenced stream MUST NOT inherit the fence: the fork is fenced iff
  its own `PUT` says so. **[WF-03]**
- A server (or storage backend) that does not implement this extension's
  atomic fence MUST refuse the opt-in with `501 Not Implemented` rather than
  create a stream whose fence it cannot enforce. **[WF-04]**
- Any `Write-Fence` value other than `true` is ignored, as for `Stream-Closed`.

## 4. The write token

The write token is the fenced class's credential. Its lifecycle follows the
claim lifecycle of §7.1–§7.3:

- It is minted by a successful §7.2 claim (`write_token` in the claim
  response), refreshed by every non-`done` ack or webhook callback
  (`write_token` in the ack response), and delivered to webhook consumers in
  the §7.1 wake notification (`write_token` field) — see §9 of this document.
- It is carried on a POST in the `Write-Token` header or as
  `Authorization: Bearer`.
- It is bound to one claim — the subscription, its incarnation, the
  generation, the `wake_id`, and the holder — and scoped to the exact stream
  paths of the claim's links. A valid token appends to a linked fenced stream
  **[WF-05]**; a token presented against a stream outside its scope MUST be
  rejected `403` **[WF-06]**; an expired or otherwise invalid token MUST be
  rejected `401` **[WF-07]**.
- A presented-but-malformed carrier (a duplicated `Write-Token` header, or one
  with an empty value) MUST be treated as a presented, invalid credential
  (`401`) — it MUST NOT fall through to another carrier or downgrade the
  request to the open class.
- The server MUST install the stream-side fence state for a claim before it
  releases that claim's write token to any consumer, so no token exists whose
  fence could not yet refuse its successor. **[WF-08]**

The write token proves *capability* (the current claim), not liveness alone. A
credential that only identifies a wake or callback context MUST NOT be
accepted in its place (§5).

## 5. Write classes

On a fenced stream, the server derives one of two classes for every POST —
append, append-and-close, or close-only — before any mutation. The class is
**fenced** iff the request presents a write-token carrier or asserts the class
with `Write-Fence: true`; it is **open** otherwise. The class is derived
server-side from what the request carries; there is no client-negotiated mode.

Rules for the fenced class:

- A POST asserting `Write-Fence: true` without a write token MUST be rejected
  `401` — on every stream, fenced or not, so a gateway can make the assertion
  unconditionally on routes that must never write unfenced. **[WF-09]**
- A fenced write MUST carry all three producer headers (`Producer-Id`,
  `Producer-Epoch`, `Producer-Seq`); the server MUST reject `400` otherwise.
  **[WF-10]**
- `Producer-Epoch` MUST equal the token's claim generation; the server MUST
  reject `409` (reason `epoch`) otherwise. The claim generation is the fenced
  class's writer epoch, so a writer cannot self-declare an epoch the control
  plane did not grant it. **[WF-11]**
- The server MUST evaluate the fence **atomically with the write**, against
  current fence state: a token whose claim is no longer the stream's live,
  unexpired claim marker MUST be rejected `409` (reason `marker`) with the
  stream tail unchanged. **[WF-12]** A claim whose lease has lapsed MUST be
  fenced the same way, judged against the same clock as the append itself.
  **[WF-13]**
- Only after the fence accepts does the base §5.2.1 producer validation run,
  unchanged.

Rules for the open class on a fenced stream:

- An open write MUST be attributable to an authenticated principal; the
  authentication mechanism is implementation-defined per §12.1 of the base
  protocol. An unauthenticated open write on a fenced stream MUST be rejected
  `401` when the server enforces authentication. **[WF-14]**
- A credential that proves only wake or callback identity (liveness, not the
  claim capability) MUST NOT write a fenced stream: reject `403`. **[WF-15]**
- Otherwise the open class is the base protocol, byte-for-byte: §5.2 body and
  header semantics, §5.2.1 producer state machine, closure, and content-type
  rules are unchanged (subject only to §6).

## 6. Bound producers

The fenced class carries stable producer ids, so a writer that loses its token
must not be able to keep the same identity and epoch sequence going as an
"open" writer — that would be the zombie this extension exists to stop.

- After a fenced write is accepted for a `Producer-Id` on a fenced stream, an
  open write naming that id MUST be rejected `409` (reason `bound`).
  **[WF-16]**
- This includes a byte-identical retry of an accepted fenced tuple arriving
  without the token: idempotent replay of the fenced class is fenced-class
  only. **[WF-17]**
- For producer ids never bound, the §5.2.1 state machine on the open class is
  unchanged: an unbound producer establishes its epoch on a fenced stream
  exactly as on any stream. **[WF-18]**

The binding is per stream and lives as long as the stream. Writers SHOULD
partition producer-id namespaces between the two classes (see §10).

## 7. Seal

Completion must be as final as deposition. When a holder finishes (`done`),
releases, or is superseded — and when the subscription or the stream link is
torn down — the generation is **sealed** on every linked fenced stream:

- The server MUST seal the current generation on every linked fenced stream
  **before** the control plane completes the `done`/release and the
  subscription becomes claimable again. After the seal, any write presenting
  that generation (or an earlier one) of that authority MUST be rejected `409`
  (reason `sealed`). **[WF-19]**
- `HEAD` on a fenced stream with at least one seal MUST expose the latest seal
  as `Write-Fence-Sealed-Generation` and `Write-Fence-Sealed-Offset` — the
  definite last offset the sealed generation's fenced class reached.
  **[WF-20]**
- When a successor's claim supersedes a live predecessor, the grant MUST
  record the superseded generation's seal (its final fenced offset) as part of
  installing the new generation's fence. **[WF-21]**
- Sealing MUST be idempotent: a redelivered `done` (at-least-once delivery)
  re-seals the same generation with no state change. **[WF-22]**
- The seal is **per authority**: it fences every generation of its authority
  at or below the sealed generation, forever — a grant for such a generation
  MUST be refused no matter how delayed — but a new incarnation of the
  subscription is a new authority and starts unsealed. Recreating a
  subscription MUST NOT be bricked by its predecessor's seals. **[WF-23]**

The seal gives readers a truncation guarantee: after `done` at generation *g*,
the offset in the seal is the last byte generation *g* wrote, and nothing of
generation ≤ *g* of that authority can ever append after it.

## 8. Rejection disclosure

A fence rejection tells the writer enough to stand down without a read:

- Data-plane fence rejections use `409 Conflict` with the JSON error envelope
  of §7.2, code `FENCED`, extended with a `reason` field naming the rule that
  refused the write, plus `generation` (the current generation, when the
  fence state knows one) and `current_holder` (when a live, unexpired claim
  holds the stream):

  ```json
  {
    "error": {
      "code": "FENCED",
      "message": "write token claim is fenced",
      "reason": "marker",
      "generation": 9,
      "current_holder": "worker-B"
    }
  }
  ```

  `reason` values: `credential`, `producer_required`, `principal`,
  `wake_token`, `precheck`, `marker`, `sealed`, `epoch`, `bound`, `store`
  (implementations MAY add values). **[WF-24]**
- When the rejected request carried producer headers, a `409 FENCED` MUST also
  carry `Producer-Epoch: <current generation>` (when known) and the **terminal
  gap pair** `Producer-Expected-Seq` == `Producer-Received-Seq` == the
  request's `Producer-Seq`. The pair is impossible in the base protocol (a
  real sequence gap always has expected < received), so a base §5.2.1 client
  library observing it stops cleanly on the first response instead of
  retrying or re-reading. **[WF-25]**
- The pre-credential rejections (`401`/`403` of §5) MUST NOT disclose fence
  state: no generation, no holder, no producer echo — an unauthenticated
  caller learns nothing about the stream. **[WF-26]**
- A `409 FENCED` MUST NOT carry `Stream-Next-Offset`: a deposed writer stands
  down; it does not resume.

Precedence on a fenced stream: the fence is evaluated before the base closed,
content-type, producer, and `Stream-Seq` checks, so a deposed writer always
learns it is fenced rather than a coincidental base error.

## 9. Subscription delivery additions

The write token rides the §7 delivery surfaces as three additive JSON fields,
all `omitempty` — a base client ignores them:

- §7.2 claim response: `write_token` (alongside the claim `token`).
- §7.2 ack response (non-`done`): `write_token`, re-minted on every heartbeat
  so a long-running holder outlives the token TTL; the heartbeat also renews
  the fence state it rides on.
- §7.1 webhook wake notification: `write_token`, minted before delivery.

Webhook and pull-wake delivery MUST be symmetric under this extension: a
webhook consumer can write fenced streams, heartbeat to refresh its token, and
complete with `done` exactly as a pull-wake worker can — including the seal —
with the holder identity derived from the wake when no worker claims it.
**[WF-27 (webhook), WF-28 (pull-wake)]**

A failure to mint the token for a webhook delivery SHOULD NOT abort the
delivery (fail-open delivery, fail-closed token): the notification goes out
without `write_token`, and the consumer's fenced writes fail closed.

## 10. Security considerations

**Token custody.** The write token is a bearer capability for the fenced
class. Consumers MUST NOT log it or persist it beyond the claim; transport
MUST be protected as §12.11 requires.

**Stateless gateways.** A gateway forwarding writes for a woken runtime SHOULD
pass the runtime's token through in `Write-Token` while authenticating itself
in `Authorization`, keeping the two credentials separate; the server evaluates
both (the gateway's principal for routing, the token for the fenced class).

**The trusted-gateway residual.** A writer behind a trusted gateway that drops
*both* its token and its producer headers is an open write under the gateway's
authority — the fence cannot distinguish it from any other gateway write. A
gateway MUST assert `Write-Fence: true` on routes that carry fenced-class
output (activation output, not command traffic), turning a lost token into a
loud `401` instead of a silent downgrade (§5, [WF-09]).

**At-least-once completion.** `done` and the seal are at-least-once; §7's
idempotence rules apply. A crash between the seal and the control-plane
completion leaves the subscription live but its streams sealed: the holder's
retry completes the seal, and its heartbeat cannot revive the sealed
generation.

**Linking discipline.** The non-interleaving guarantee is per authority.
Deployments MUST link at most one subscription to a fenced stream; two
authorities on one stream each get correct fencing, but their writes may
interleave with each other. Similarly, deployments SHOULD reserve a producer-id
namespace for the fenced class (e.g. a prefix used only by woken runtimes) so
the bound-producer rule (§6) partitions cleanly.

## 11. IANA considerations

This extension defines four HTTP header fields, to be registered per §13.2 of
the base protocol:

| Field | On |
|---|---|
| `Write-Fence` | request (`PUT`, `POST`); response (`PUT`, `HEAD`) |
| `Write-Token` | request (`POST`) |
| `Write-Fence-Sealed-Generation` | response (`HEAD`) |
| `Write-Fence-Sealed-Offset` | response (`HEAD`) |

## 12. Appendix: Chronicle implementation notes

Everything above is implementation-independent. This appendix records how
[Chronicle](https://github.com/adityavkk/chronicle) implements the extension,
and the limits of that implementation.

- **Carrier alias.** Chronicle also accepts the write token in the
  `electric-claim-token` header (the pre-extension spelling of its Electric
  integration). Carrier order: `Write-Token`, `electric-claim-token`, then
  `Authorization: Bearer` — the bearer only when it was not already consumed
  as a service or wake credential. The malformed-carrier rule of §4 applies to
  both named headers.
- **Shard 0 only.** Chronicle's fence state lives in the stream slot of claim
  shard 0, and both token mints hardcode shard 0. A write token naming any
  other shard is refused `401 write token shard is not fenceable`
  (reason `shard`) before any stream access, and no fence is ever granted for
  it.
- **Fence state.** The claim marker is a per-authority stream-slot key with a
  bounded retention (tombstones are reaped); the seal (`wfseal:<authority>`),
  the summary the `HEAD` headers read (`wfSealGen`/`wfSealOff`), the last
  fenced offset (`wfLastOff`), and the producer bindings
  (`wfbind:<producer-id>`) are fields of the stream meta hash and live as long
  as the stream. Because the seal is per authority and refused at grant time
  (`generation <= seal`), a delayed grant cannot revive a sealed generation
  even after its marker tombstone is reaped. Deleting the stream deletes its
  fence state with it; a fenced re-create is a new stream and needs the
  header again.
- **Access-control modes.** The fence semantics of §3–§8 bind in every
  `CHRONICLE_AUTH_MODE` — token validity, producer rules, marker/seal/epoch
  checks, and the wake-token 403 do not soften in `insecure` mode. Only the
  open-class principal requirement (WF-14) follows the mode: `enforce`
  rejects `401`; `insecure` admits the write and logs the decision as
  telemetry, which is the base posture for unfenced streams. See
  [docs/DEPLOYMENT.md](https://github.com/adityavkk/chronicle/blob/main/docs/DEPLOYMENT.md).
- **Capability probe.** `PUT` with `Write-Fence: true` is `501` unless the
  configured store implements the atomic fence (`store.WriteFenceStore`; the
  Redis backend and the in-memory store both do). Creating a fenced stream
  with no append authorizer configured logs a warning: every fenced write
  will fail closed (WF-09/WF-14) until one is wired.
- **Recreated-subscription epoch limitation.** A deleted-and-recreated
  subscription starts a new authority (unsealed, per WF-23) at generation 0,
  but the stream's §5.2.1 producer state survives. A runtime reusing its
  stable producer id then hits the base `403` stale-epoch response until the
  new authority's generation passes the stored epoch — a liveness (not
  safety) limitation. The planned fix is control-plane: seed a recreated
  subscription's generation above its predecessor's.
- **Departures from its consumer contract** are recorded in
  [ADR-0008](https://github.com/adityavkk/chronicle/blob/main/docs/adr/0008-write-fencing-extension.md);
  the formal model and invariants (INV-FENCE-05/06/07) in
  [docs/specs/formal-verification/INVARIANTS.md](https://github.com/adityavkk/chronicle/blob/main/docs/specs/formal-verification/INVARIANTS.md)
  and [formal/tla/WriteFence.tla](https://github.com/adityavkk/chronicle/blob/main/formal/tla/WriteFence.tla).

### Conformance test mapping

Chronicle's extension conformance suite (`test/conformance-ext/`,
`make conformance-ext`) implements one test per rule id; it is separate from
the pinned base suite so the certified base count is untouched. The table also
names the in-repo Go test that pins the same rule where one exists.

| Rule | Extension test (`test/conformance-ext/write-fencing.test.ts`) | In-repo pin |
|---|---|---|
| WF-01 | idempotent re-PUT must agree on the fence | `TestHandleCreateWriteFence`, `store/config_match_property_test.go` |
| WF-02 | `Write-Fence: true` echoed on PUT and HEAD | `TestHandleCreateWriteFenceEchoesOnRedis`, `TestHandleHeadWriteFence` |
| WF-03 | forks do not inherit the fence | `TestCreateForkDoesNotInheritWriteFence` |
| WF-04 | 501 without the fence capability | `TestHandleCreateWriteFence` |
| WF-05 | valid token appends | `TestHandleAppendAllowsCurrentWriteToken` |
| WF-06 | out-of-scope token is 403 | `TestAppendClassDerivation` |
| WF-07 | expired/invalid token is 401 | `TestAppendClassDerivation` |
| WF-08 | no token before its fence is installed | `TestDeliverWebhookGrantsMarkerBeforePost` |
| WF-09 | asserted class without token is 401 | `TestAppendDeclaredWithoutTokenIs401InEveryMode` |
| WF-10 | fenced write without producer headers is 400 | `TestHandleAppendFencedDisclosure` |
| WF-11 | epoch must equal generation (409 `epoch`) | `TestHandleAppendFencedStreamRequiresProducerEpochEqualGeneration` |
| WF-12 | deposed claim fenced atomically, tail unchanged | `TestHandleAppendRejectsDeposedWriteToken`, `TestHandleCloseRejectsDeposedWriteToken` |
| WF-13 | lapsed lease is fenced | `TestWriteFenceDifferential` (lease branch) |
| WF-14 | open class needs a principal (enforce 401) | `TestHandleAppendOpenClassOnFencedStream` |
| WF-15 | wake-identity credential cannot write (403) | `TestAppendClassDerivation` |
| WF-16 | bound producer refused on the open class | `TestHandleAppendBoundProducerWithoutToken`, `TestHandleAppendBoundProducerFencedInSlot` |
| WF-17 | replayed accepted tuple refused without token | `TestHandleAppendBoundProducerWithoutToken` |
| WF-18 | unbound producer keeps the base §5.2.1 SM | `TestHandleAppendOpenClassOnFencedStream` |
| WF-19 | done seals every linked stream before idle | `TestWebhookAutoAckDoneSeals`, `TestDoneSealCrashWindowFailsClosed` |
| WF-20 | HEAD exposes the sealed generation/offset | `TestHandleHeadWriteFence` |
| WF-21 | supersession records the predecessor's seal | `TestAppendFenceGrantSupersessionSealsPredecessor` |
| WF-22 | redelivered done is idempotent (`already`) | `TestDoneSealCrashWindowFailsClosed` |
| WF-23 | new incarnation starts unsealed | `TestAppendFenceSealPerAuthorityIsolated` |
| WF-24 | 409 envelope: reason/generation/current_holder | `TestHandleAppendFencedDisclosure` |
| WF-25 | terminal gap pair + epoch echo on 409 | `TestHandleAppendFencedDisclosure` |
| WF-26 | no fence disclosure on the pre-credential 401 | `TestHandleAppendFencedDisclosure` |
| WF-27 | webhook end-to-end parity | `TestWebhookCallbackHeartbeatRefreshesWriteToken`, `TestWebhookAutoAckDoneSeals` |
| WF-28 | pull-wake end-to-end | `TestHeartbeatRefreshesWriteTokenForLongLiveHolder` |
