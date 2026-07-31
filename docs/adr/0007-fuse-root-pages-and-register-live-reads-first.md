# ADR-0007: Fuse root-owned pages and register live reads before capture

- **Status:** Accepted
- **Date:** 2026-07-31
- **Deciders:** @adityavkk
- **Tracking issue:** [#172](https://github.com/adityavkk/chronicle/issues/172)

## Context

ADR-0004 introduced bounded `ReadPage` snapshots. ADR-0005 introduced the
shared SSE hub and closed its attach race with subscribe then durable refresh.
After the read-only fast path, one ordinary root-owned page still ran
`read.lua` twice: once to validate and capture root metadata, then again to
validate the selected root segment and fetch its bounded range. A new SSE hub
also performed the client's first page and a second empty no-touch page after
subscription. An `offset=now` long-poll performed the first page, a
post-subscribe attach recheck, and its eventual wake, poll, or timeout page.

These calls were individually correct, but the boundaries repeated atomic work
that one ordering could prove unnecessary. Removing them is safe only if fixed
snapshot tails, fork inheritance, expiry, stream incarnation, and Pub/Sub's
lossy delivery model remain explicit.

## Decision

### Root-owned pages

The first root `read.lua` invocation classifies the requested range after it
atomically loads and validates metadata:

1. **empty** when the request is `now`, at the fixed response tail, or beyond
   it;
2. **inherited** when a fork request begins below the effective fork offset;
3. **root-owned** for every other nonempty range.

`store.ClassifyRootReadRange` is the pure typed Go oracle. Lua mirrors the full
`ReadSeq` and byte-offset comparison as decimal strings, never as Lua numbers.
For a root-owned range, the same script returns bounded frames through the
atomically captured first-page tail or the caller's fixed continuation tail.
For an inherited range, Go retains the established chronological source-chain
traversal and later slot-local scripts. Empty ranges do not touch the message
ZSET.

### Register-first live reads

`NotificationSubscriber` remains the optional capability. Redis implements it,
the immutable-segment wrapper exposes the authoritative primary through
`NotificationSubscriberProvider`, and `MemoryStore` exposes its existing wake
registration through the same interface.

For a new SSE hub, the handler confirms one subscription before its
authoritative first page. That page seeds the hub's incarnation, content type,
tail, and closed state, and the hub takes ownership of the subscription without
an initial readiness refresh. Immediately before exact indexed attach, the
handler performs one bounded no-touch incarnation confirmation; this is an
identity fence, not a same-offset data reread. An existing hub is reserved
before page capture and then validated against the page. A concurrent creator
closes its redundant subscription. A stale-incarnation reservation is replaced
only after a new registration and a fresh no-touch page fenced to the request's
first logical page. A failure discovered after SSE headers are committed aborts
the handler transport instead of writing an HTTP error into the event stream.

For `offset=now` long-poll, the handler confirms registration before the first
page and blocks without an immediate attach recheck. Numeric offsets retain the
existing first-page, subscribe, and no-touch-recheck path. Redis implements
that optional `PageWaiter` seam through `SubscribeNotifications`, so numeric
compatibility waits share the same bounded physical connection multiplexer.

Let `R` be the atomic page captured after registration. An append linearized
before `R` is in `R`'s tail. An append linearized after `R` is covered by the
confirmed subscription. Redis orders a boundary race on one side or the other.
A lost wake is recovered by the one-second durable poll. Append and close hints
and defensive polls preserve the ordinary access-touch semantics. A
notification reconnect or replacement subscription generation instead forces
an immediate durable no-touch refresh, because transport recovery must not
extend the stream lifetime. Every page must match `R`'s stream incarnation and
content type.

## Consequences

### Positive

- A one-frame root-owned page uses one `EVALSHA`, one metadata `HGETALL`, and
  one bounded `ZRANGEBYLEX`, with no producer read.
- A new empty SSE hub reaches readiness with one metadata-only page instead of
  two; exact attach still performs its bounded no-touch identity confirmation.
- An `offset=now` long-poll timeout uses its first page and final timeout page,
  without a third attach page.
- The first logical read still performs the only sliding-TTL renewal. Valid
  persistent and absolute-expiry-only reads remain absent from AOF and
  replication deltas.

### Costs and constraints

- The Go and Lua range classifiers are independent implementations and require
  live differential parity tests.
- Inherited fork reads deliberately keep extra traversal and source-slot work.
  Root fusion does not introduce a cross-slot script.
- Register-first transfers subscription ownership across handler and hub
  lifecycles. Every losing or cancelled path must close its subscription, and
  every discarded immutable page must release its snapshot lease.
- Pub/Sub remains a hint. The defensive durable poll and incarnation fence are
  correctness requirements, not optional performance knobs.

## Rejected alternatives

- **Fetch the root ZSET before classifying forks.** Rejected because an
  inherited prefix must be returned first and may live in another slot.
- **Use only byte offsets in Lua.** Rejected because the protocol offset is the
  ordered pair `(ReadSeq, ByteOffset)`.
- **Subscribe after the first `offset=now` page and remove the recheck anyway.**
  Rejected because an append in that window could be missed.
- **Put payloads in Pub/Sub.** Rejected because notification loss would become
  data loss and Redis durable state would no longer be authoritative.
