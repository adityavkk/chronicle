# Chronicle notes on the vendored protocol

This document holds Chronicle's own annotations on the vendored Durable
Streams spec, kept **separate** from [PROTOCOL.md](./PROTOCOL.md) so that file
stays a pristine, byte-identical mirror of upstream (see
[README.md](./README.md)). Vendoring pristine means we can diff against a new
upstream commit and see exactly what changed, with zero noise from our own
edits — pulling a local addition back out of a merge conflict is exactly the
workflow this file exists to avoid.

Each note below cites the PROTOCOL.md section it annotates. Nothing here
changes the protocol; it records implementation-relevant precision Chronicle
found worth writing down while hardening the Redis/Go implementation.

## Section 5.2, Append to Stream — `Stream-Seq` header (the `INV-DIFF-03` note)

Annotates the `Stream-Seq` request header in
[PROTOCOL.md §5.2](./PROTOCOL.md#52-append-to-stream), specifically the
conditional-append / regression-check text around the `Stream-Seq` bullet.

**Lex-safe client precondition (INV-DIFF-03).** Because the comparison is byte-wise — not numeric — clients **MUST** choose `Stream-Seq` values that are lexicographically monotonic. A naive **unpadded decimal counter is unsafe**: `"10"` sorts *before* `"9"` byte-wise, so the valid advance `"9"` → `"10"` is wrongly rejected with `409 Conflict` at every digit-width boundary (the same class of footgun as a non-fixed-width offset encoding). Clients **SHOULD** use a representation that keeps byte-wise order equal to the intended order, such as **fixed-width zero-padded decimals** (`"0000000010" > "0000000009"`), monotonic timestamps/ULIDs, or any other lexicographically-monotone scheme. The server compares exactly the bytes it is given and applies no numeric interpretation, so this is a client-side obligation, not a server normalization.

### Provenance

This note was lifted verbatim out of PROTOCOL.md (issue #80) to restore the
vendored file to pristine. It originated as the documentation half of the
**LB-2** finding — see
[docs/specs/formal-verification/FINDINGS.md](../specs/formal-verification/FINDINGS.md)
— which named `Stream-Seq`'s bytewise regression check "the same digit-width
hazard as **LB-1**" (`Offset.String()`'s `%016d` minimum-width footgun,
tracked in [ADR-0003](../adr/0003-offset-string-width-migration-lb1.md)) and
recommended stating the lex-safe-`Stream-Seq` precondition explicitly. The
invariant itself is cataloged as `INV-DIFF-03` in
[docs/specs/formal-verification/INVARIANTS.md](../specs/formal-verification/INVARIANTS.md)
and enforced identically by `store/redis/scripts/append.lua` and
`store/memory_store.go`.

## Section 5.2.1, Idempotent Producers — epoch establishment on a write-fenced stream

Annotates the client-declared-epoch design in
[PROTOCOL.md §5.2.1](./PROTOCOL.md#521-idempotent-producers).

On a stream created with `Write-Fence: true`, the fenced write class gates
epoch establishment by the §7.3 claim generation: `Producer-Epoch` **must
equal** the generation of the presented write token, so a fenced writer cannot
self-declare an epoch the control plane did not grant it, and a producer id an
accepted fenced write has bound cannot advance its epoch as an open write —
see [WRITE-FENCING.md §5–§6](./WRITE-FENCING.md#5-write-classes). **This
changes nothing in the base protocol**: on streams that never opt in — and for
unbound producer ids on the open class of streams that do — the §5.2.1 state
machine (client-declared epochs, auto-claim, sequence rules, all four
producer response headers) is unchanged, byte for byte.

## Section 7.3, Generation Fencing and Leases — the append-side fence

Annotates the fencing rules of
[PROTOCOL.md §7.3](./PROTOCOL.md#73-generation-fencing-and-leases).

§7.3 fences the *control plane*: callbacks, acks, and releases are judged
against the current `(generation, wake_id)`. Chronicle's write-fencing
extension extends the same fence to the *data plane* on streams that opt in:
the claim mints a write token, appends under it are checked against the live
claim marker atomically with the write, and `done`/release/supersession seal
the generation per authority — see
[WRITE-FENCING.md §4 and §7](./WRITE-FENCING.md#4-the-write-token). **This
changes nothing in the base protocol**: §7.3's rejection rules, lease
semantics, and `409 FENCED` control-plane responses are untouched, and a
subscription over streams that never opt in behaves exactly as before.

## Section 7.1, Webhook Delivery and Callback — the request correlation header

Annotates the webhook `POST` and its callback in
[PROTOCOL.md §7.1](./PROTOCOL.md#71-webhook-delivery-and-callback). This is an
implementation addition (§11.1 pure superset): the protocol defines no
correlation header, and nothing below changes any protocol rule.

**The header.** Chronicle reads one correlation header on every request,
echoes it on every response and sends it on every webhook delivery. Its name
is deployment configuration (`CHRONICLE_REQUEST_ID_HEADER`, default
`X-Request-ID`; see [DEPLOYMENT.md](../DEPLOYMENT.md#request-correlation-and-logging)),
so a platform that already owns a request-id header keeps its name end to end.
Both CORS lists advertise the configured name.

**Grammar and normalization.** A value is 1 to 128 bytes, the first
alphanumeric, the rest alphanumeric or one of `.`, `_`, `:`, `-`
(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`), so an id is always one single-line log
field and a valid header value. Surrounding whitespace is ignored. A request
that carries a value fitting the grammar keeps it; a request that carries none,
or a value that does not fit, is given a fresh UUIDv4. Normalization never
fails a request: a client cannot make Chronicle reject a call by sending a
strange id, it only loses its own value.

**On the wake.** The id of the append that armed a wake rides the process-local
hint queue to the arm and is then sent as the header on that wake's webhook
`POST` and on every retry of it, and logged on the delivery, on each callback or
ack and on the release. Two limits follow from the design and receivers
**MUST** treat the header accordingly:

- *Coalescing.* Appends are coalesced per stream before fan-out (PROTOCOL §7:
  one wake per generation). Appends coalesced onto a hint that is still queued
  share the id the hint was queued with; an append that lands while the hint is
  being processed re-queues it under that newer id. A wake therefore carries
  the id of *one* of the appends it covers, never all of them.
- *The `wake-<wake_id>` fallback.* The id is remembered only by the replica
  that accepted the append, only while the wake is in use, and for at most the
  subscription's lease plus the longest retry gap of idleness. A wake armed by
  the recovery sweep or a re-wake, a retry after a restart, a callback that
  lands on another replica, or a wake whose memory has lapsed carries or logs
  `wake-<wake_id>` instead, the same value on every replica. Every callback and
  release record therefore logs both `request_id` (the incoming request's own
  id) and `wake_request_id` (the id that armed the wake, or the fallback), so
  the two sides join even when a receiver does not echo the header it was sent.

**Not signed, never identity.** The header is outside `Webhook-Signature`,
which covers only the timestamp and the body (§7.1), and it is never consulted
by authentication or authorization: callback tokens, wake tokens and write
tokens carry identity, the correlation header carries a log-join hint. A
receiver **MUST NOT** derive trust from it, and **SHOULD** echo the value it
received on the callback so Chronicle's ack record and the receiver's own
records carry the same `request_id`.

## Section 7.1, Webhook Delivery and Callback — the `traceparent` header

Annotates the webhook `POST` and its callback in
[PROTOCOL.md §7.1](./PROTOCOL.md#71-webhook-delivery-and-callback). Like the
correlation header above this is an implementation addition (§11.1 pure
superset): the protocol defines no trace context and nothing below changes any
protocol rule.

**When it is sent.** Only by a Chronicle whose operator turned tracing on
([DEPLOYMENT.md](../DEPLOYMENT.md#tracing)). A deployment with tracing off sends
no `traceparent` at all, and no Chronicle ever sends `tracestate`.

**What it names.** The header is a W3C Trace Context `traceparent` whose
trace id is that of the append that armed the wake, when the delivering replica
still remembers it, and of a new trace otherwise; its parent id is the span of
this delivery attempt. The append's identity follows the request id's rules
exactly, because the two are remembered together: a wake carries the trace of
*one* of the appends it covers (the coalescing rule above), and the same
conditions that make a wake fall back to `wake-<wake_id>` make it start a new
trace. A retry of the same wake carries the same trace id under a new parent
id, one span per attempt. The sampled flag is the caller's decision carried
through: an unsampled append yields an unsampled delivery.

**What a receiver does with it.** A receiver **MAY** continue the trace by
parenting its own span on the header, which joins the delivery to the append
that caused it end to end, or **MAY** start its own trace and record the header
as a link, which is the safer reading when the receiver treats one wake as
covering many appends. Either is conformant; Chronicle does not care which. The
callback the receiver then makes carries the receiver's own `traceparent`, and
Chronicle's callback span is a child of that.

**Not signed, never identity.** `traceparent` is outside `Webhook-Signature`,
which covers only the timestamp and the body (§7.1), and no authentication or
authorization decision reads it. A receiver **MUST NOT** derive trust from it.
