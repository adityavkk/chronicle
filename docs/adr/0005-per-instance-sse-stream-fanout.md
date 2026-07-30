# ADR-0005: Share live SSE reads through per-instance stream hubs

- **Status:** Accepted
- **Date:** 2026-07-29
- **Deciders:** @adityavkk
- **Tracking issue:** GPA/chronicle#4

## Context

The original SSE path performed Redis work once per connected HTTP client.
Every client owned a Pub/Sub wait loop, reread the same durable suffix, fetched
metadata again, formatted the same payload, and flushed it independently.

The sealed benchmark run `20260729T151520Z-675c412d` measured the amplification:

- One stream with 1,000 clients turned 863 publishes into 837,579
  `ZRANGEBYLEX` calls, or 970.5 reads per append.
- One hundred streams with 2,048 clients turned 26,900 publishes into 539,956
  `ZRANGEBYLEX` calls, or 20.1 reads per append.
- Increasing Chronicle and Redis from 4 to 16 vCPU did not raise delivered SSE
  throughput beyond about 25,000 to 32,000 events per second.

Redis Pub/Sub cannot be the source of truth because its delivery is at most
once. A shared reader also creates new failure cases around client attachment,
slow consumers, stream deletion and recreation, and process or Redis restarts.

## Decision

Each Chronicle replica keeps one live hub for each stream that has local SSE
clients.

The hub follows these rules:

1. It confirms one Redis subscription, then reads durable state before the HTTP
   handler completes the catch-up to live handoff. This closes the append window
   between subscription and the first shared read.
2. A Pub/Sub append or close message is only a wake hint. The hub rereads
   durable state after a hint and once per second to recover a lost hint. The
   fallback is a real read, not a metadata lookup, so an active live reader also
   renews a positive sliding TTL. The cadence is capped at half the TTL when
   that is shorter than one second.
3. A delete message is also only a wake hint. The hub confirms deletion or
   replacement against durable state before terminating. Every create persists
   a random opaque incarnation ID, and every metadata check compares it. The ID
   is independent of the wall clock, so a lost or spurious delete notification
   cannot stop a valid hub or let an old hub cross into a newly created stream
   at the same path even when the clock is frozen or moves backward.
4. Each durable suffix is read once per hub refresh. Messages are split only at
   durable message boundaries and according to their actual retained payload
   plus formatted-event bytes. One formatted, immutable SSE data event is
   shared by all local clients.
5. The hub retains a byte-bounded replay ring. Each client owns one coalesced
   wake signal rather than a payload queue. A client outside the retained
   window disconnects and resumes from its last control offset.
6. One message is never split, so a single message may exceed the replay bound.
   The configured batch target cannot exceed the replay bound. Retained chunks
   own their slice storage so an evicted prefix cannot remain reachable through
   a retained slice.
7. One client write has a ten-second default deadline. A client that cannot
   accept one data-and-control update within that time is disconnected. It
   cannot retain a hub, Redis subscription, goroutine, or unbounded socket
   buffer indefinitely. HTTP response wrappers expose their underlying writer
   so the connection-level deadline remains effective through a mounted route.
8. A control event never advances beyond data already written for that client.
   If an append lands between an empty catch-up read and its metadata check, the
   handler waits for the hub event rather than checkpointing the new tail.
   The first metadata snapshot is also carried through hub attachment, so a
   delete and recreate cannot mix the old tail or content encoding with the new
   stream.
9. Automatic Redis resubscriptions are observed explicitly. A reconnect causes
   an immediate durable recheck and increments a reconnect metric.
10. The last client removes the hub and closes its subscription. A new stream
    incarnation may replace a terminal hub even while an old client lease is
    still unwinding.
11. Request cancellation and the periodic CDN reconnect timer are checked
    before every retained event, so a continuous replay backlog cannot keep a
    connection alive past the configured reconnect interval.

## Correctness gates

This optimization does not weaken the Durable Streams protocol.

- The complete 332-test conformance suite remains a merge blocker.
- The Jepsen and Porcupine suites remain maintained merge blockers. The SSE
  scenario verifies attach timing, an exactly omitted notification, a verified
  duplicate notification, Redis reconnect and replacement, Chronicle
  replacement by pod UID, independent replay-lag resume and write-timeout
  faults, per-session replay, data-to-control checkpoints, final close ordering,
  and an independent durable read.
- Producer fencing, append atomicity, expiry, forks, closure, offsets, and
  persistence behavior are unchanged.
- A conformance, Jepsen, or linearizability regression blocks the change even
  when throughput improves.

## Consequences

### Positive

- Steady live-read work scales with active streams per Chronicle replica rather
  than local HTTP clients.
- Payload formatting and binary base64 encoding happen once per shared batch.
- Replay and per-client scheduling memory have explicit bounds.
- Lost notifications, Redis reconnects, stream reincarnation, and slow clients
  have observable recovery paths.

### Costs and remaining limits

- The hub adds shared mutable state and a correctness-sensitive catch-up to live
  transition.
- Redis Pub/Sub connections still scale with active streams per replica. A
  later shard-level subscription multiplexer can reduce that to connections per
  Redis shard.
- The one-second correctness fallback performs an idle durable read for every
  active hub. This preserves sliding TTL and lost-hint recovery, but high stream
  counts may require a purpose-built atomic touch-and-tail operation or a shared
  polling scheduler.
- Initial client catch-up and the underlying Redis `ZRANGEBYLEX` read remain
  unbounded. This ADR does not solve large historical replay or the transient
  allocation from a hub catching up after a long outage.
- Hubs do not linger after the last client. The required 60-second SSE
  reconnect can therefore recreate a hub for a stream with only one client.

## Alternatives considered

- **Keep one wait loop per client.** Rejected because the measured Redis work is
  proportional to subscribers and did not improve with larger nodes.
- **Put payloads in Pub/Sub and skip durable reads.** Rejected because a dropped
  Pub/Sub message would become data loss.
- **Use an unbounded client channel.** Rejected because one slow client could
  consume unbounded memory and block shared progress.
- **Build the shard-level subscription multiplexer first.** Deferred. The
  per-stream hub removes the measured client amplification with a smaller
  correctness surface. Connection multiplexing remains a separate scaling
  improvement.
