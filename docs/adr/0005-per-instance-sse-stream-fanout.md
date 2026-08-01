# ADR-0005: Share live SSE reads through per-instance stream hubs

- **Status:** Accepted, amended 2026-07-31
- **Date:** 2026-07-29
- **Deciders:** @adityavkk
- **Tracking issues:** [GPA/chronicle#4](https://gecgithub01.walmart.com/GPA/chronicle/issues/4), [GPA/chronicle#13](https://gecgithub01.walmart.com/GPA/chronicle/issues/13)

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

1. It confirms one logical Redis notification registration before the first
   client page captures an incarnation and tail. That authoritative page seeds
   the first confirmed hub generation, which becomes ready without rereading
   the same offset. Immediately before exact live attach, the handler performs
   one bounded no-touch incarnation confirmation. This closes the append and
   delete/recreate windows without a duplicate initial readiness refresh.
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
4. Each durable suffix is paged through one exact tail snapshot. Both client
   catch-up and hub refresh require `store.PageReader`; neither can fall back to
   `Store.Read`. One formatted, immutable SSE data event is shared by all local
   clients.
5. The hub retains a byte-bounded replay ring. It stores formatted wire bytes
   and an offset boundary index without a second raw payload copy. It reports
   raw, wire, index, and total retained bytes as exact component gauges. Each
   client owns one coalesced wake signal rather than a payload queue. A client
   outside the retained window disconnects and resumes from its last control
   offset.
6. One message is never split, so a single message may exceed the replay bound.
   The configured batch target cannot exceed the replay bound. Retained chunks
   own their slice storage so an evicted prefix cannot remain reachable through
   a retained slice.
7. One client write has a ten-second default deadline. A client that cannot
   accept one data-and-control update within that time is disconnected. It
   cannot retain a hub, Redis subscription, goroutine, or unbounded socket
   buffer indefinitely. HTTP response wrappers expose their underlying writer
   so the connection-level deadline remains effective through a mounted route.
8. A control event never advances beyond data already written and flushed for
   that client. The handler advances its attach offset only after that control
   flush succeeds. It confirms the stream identity before attaching at the
   exact offset. An indexed sequence lookup finds that boundary in constant or
   logarithmic time. If eviction removed it, Chronicle disconnects instead of
   skipping ahead. If this confirmation or any later SSE operation fails after
   the response is committed, Chronicle aborts the HTTP handler. It never
   appends an ordinary HTTP error payload to the SSE event stream.
9. One store-owned multiplexer serves logical stream registrations through a
   bounded number of physical Redis Pub/Sub connections. The default is one
   connection for standalone Redis and one global Pub/Sub connection for the
   supported cluster topology. Readiness requires a subscription acknowledgement
   from the current connection generation. A stale acknowledgement cannot make
   a registration ready.
10. A multiplexer reconnect invalidates its acknowledgement generation,
    restores all desired channels, and sends one coalesced reconnect wake to
    each logical registration. Every affected hub immediately performs a
    durable no-touch refresh. A replacement subscription generation does the
    same before waiting. Ordinary append and close hints and the defensive poll
    remain access-touching reads. Delivery to one hub is nonblocking, so a
    blocked hub cannot stop another.
11. The last client removes the hub and closes its logical registration. A new stream
    incarnation may replace a terminal hub even while an old client lease is
    still unwinding.
12. Request cancellation and the periodic CDN reconnect timer are checked
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
- Physical notification connections have a fixed operator bound. Logical
  registration and physical connection metrics remain separate.
- Lost notifications, Redis reconnects, stream reincarnation, and slow clients
  have observable recovery paths.

### Costs and remaining limits

- The hub adds shared mutable state and a correctness-sensitive catch-up to live
  transition.
- A multiplexer actor serializes subscription topology changes for its assigned
  channels. Operators can add connection groups when one actor becomes busy,
  but each group adds one possible Redis Pub/Sub connection.
- The one-second correctness fallback performs an idle durable read for every
  active hub. Append and close hints and defensive polls renew a sliding TTL
  once per hub refresh; reconnect recovery and final attach confirmation do
  not. Persistent and absolute-expiry-only streams perform no read-side write.
  High stream counts may still require a shared polling scheduler.
- Initial client catch-up remains one read path per client. It now uses bounded
  `ReadPage` calls with a 1 MiB target and a 1,024-frame cap, so Redis and
  Chronicle no longer materialize the complete historical suffix at once. The
  per-client work and socket bytes remain.
- Hubs do not linger after the last client. The required 60-second SSE
  reconnect can therefore recreate a hub for a stream with only one client.

## Alternatives considered

- **Keep one wait loop per client.** Rejected because the measured Redis work is
  proportional to subscribers and did not improve with larger nodes.
- **Put payloads in Pub/Sub and skip durable reads.** Rejected because a dropped
  Pub/Sub message would become data loss.
- **Use an unbounded client channel.** Rejected because one slow client could
  consume unbounded memory and block shared progress.
- **Keep one Pub/Sub connection per active stream.** Rejected because physical
  connection count would still grow with stream count after live reads were
  shared.
- **Use Redis sharded Pub/Sub.** Deferred because Chronicle supports Redis 6.0
  and global Pub/Sub works in the supported cluster topology. Sharded Pub/Sub
  would raise the minimum Redis version and require slot-aware channel routing.
