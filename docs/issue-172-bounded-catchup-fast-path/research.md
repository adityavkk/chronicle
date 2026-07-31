# Issue 172 bounded catch-up fast path: research

## Scope and baseline

The worktree was refreshed before editing. On 2026-07-31, `HEAD`, the branch
base, and `origin/main` were all
`b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035` (`docs: align endpoint flows with
read fast path`). The worktree was clean.

While validation was running, the shared remote-tracking ref advanced to
`49e58662f1b863f5019a2f55ddea8bdc4f42e6af` with the independent async
subscription-fanout change. The branch was fast-forwarded and the issue 172
work reapplied. Its one conflict was confined to the Redis MONITOR test helper;
the newer mainline raw-connection helper was retained because it gives the
stream one shutdown owner. No result below relies on the earlier mainline after
that integration point.

The untouched one-frame root-owned `ReadPage` path was measured against an
isolated Redis 8 container with AOF enabled and `appendfsync always`. Redis
`MONITOR`, scoped to `TestReadPageOneFrameUsesOneRangeCall`, observed:

- 2 `EVALSHA` calls to `read.lua`;
- 2 nested `HGETALL` calls for the metadata hash;
- 1 bounded `ZRANGEBYLEX` call;
- 0 reads of the producer hash.

The first script performed root visibility, expiry, incarnation, and touch
validation without fetching frames. Go then selected the root segment and ran
the same script again to fetch the frame. A public persistent GET of the same
five-byte stream left `aof_current_size` at 950 bytes and
`master_repl_offset` at 10 before and after the request. The command duplication
therefore costs latency and Redis work, but does not currently add steady-state
persistence work.

## Sources read

- GitHub issue 172, including its Phase 1, Phase 2, invariant, evidence, and
  release-gate requirements.
- `AGENTS.md`, `docs/PLAN.md`, ADRs 0001, 0004, 0005, and 0006, and the complete
  `docs/spec/PROTOCOL.md`.
- The HTTP read, long-poll, SSE catch-up, SSE hub, and notification paths.
- `MemoryStore`, Redis metadata and read orchestration, `read.lua`, the common
  Lua prelude, immutable-segment snapshot leases, and their surrounding unit,
  integration, differential, cancellation, race, and fault tests.
- Endpoint flows, bounded catch-up and SSE explainers, integration notes,
  existing read-only fast-path results, and the Jepsen runbook and results.

## Phase 1 decision: classify the first root range once

The semantic decision is a pure three-way classification over typed offsets:

1. **empty**: `offset=now`, or the requested offset is at or beyond the fixed
   response tail;
2. **inherited**: a fork request begins below its effective fork offset;
3. **root-owned**: every other nonempty range.

An unforked root owns every nonempty range through the captured tail. A fork
owns a nonempty range beginning at or above its effective fork offset. A range
below that offset must keep the existing recursive inherited traversal. Offset
comparison uses `store.Offset`, including `ReadSeq`; it is not reduced to a Lua
floating-point number or byte-offset-only comparison.

The Go classifier is the oracle. `read.lua` mirrors it with exact decimal
component comparison after it has loaded root metadata. On a root-owned range,
the first script uses the fixed response tail as its inclusive upper bound,
keeps the existing bounded first-candidate and byte-budget lookup, and requires
the known nonempty range to contain a candidate. On an inherited or empty
range, it returns metadata without touching the message ZSET.

The script order remains:

1. load metadata exactly once;
2. lazy expiry cleanup and direct-read soft-delete visibility;
3. one-time legacy incarnation migration;
4. expected-incarnation validation;
5. the first logical sliding-TTL touch, when required;
6. classify and, only for a root-owned nonempty range, fetch bounded frames.

For a continuation, the upper bound is the original snapshot tail, never the
root's newer current tail. If an inherited page crosses into root-owned data,
Go preserves chronological traversal and invokes the root script only after the
inherited ranges have been consumed. That mixed fork page is intentionally not
collapsed into one cross-slot operation.

## Phase 2 decision: register first with the existing capability

No new store capability is required. `store.NotificationSubscriber` already
represents a confirmed, reusable wake registration and remains optional for
third-party stores. Redis provides it directly, immutable segments expose the
authoritative primary through `NotificationSubscriberProvider`, and
`MemoryStore` can mirror it with its existing registered wake channel.

### New SSE hub

The handler first reserves an existing per-path hub, if one exists. That keeps
the shared hub alive while the client captures and validates its own page. If
there is no existing hub, the handler confirms a notification subscription
before `ReadPage`. The resulting page supplies the incarnation, content type,
closed state, and tail used to initialize the hub. The hub takes ownership of
the already-confirmed subscription and does not perform the old duplicate
empty no-touch refresh. A concurrent creator may win the registry race; in that
case the redundant confirmed subscription is closed and the existing compatible
hub is reused.

If an existing hub proves to name an earlier incarnation, the handler releases
that lease, confirms a new subscription, and performs a fresh no-touch page.
The earlier page already represented the logical access, so this retry neither
touches twice nor permits the replacement lifetime to cross the old hub fence.

### `offset=now` long-poll

When the notification capability is available, subscription confirmation
precedes the first bounded page. If that page is open and caught up, the handler
reuses the same subscription and blocks immediately. It does not issue the old
post-subscribe attach recheck. Wake, defensive poll, and timeout paths still end
in a fresh no-touch authoritative page and enforce the initial incarnation
fence. Numeric long-poll keeps its pre-read and existing `PageWaiter` path.

### Linearization argument

Let `R` be the atomic root page read after subscription confirmation.

- An append linearized before `R` is included in `R`'s captured tail. A normal
  page returns it during bounded catch-up; `offset=now` intentionally starts at
  that tail.
- An append linearized after `R` is covered by the already-confirmed
  subscription. If Pub/Sub delivery is lost, the existing defensive poll reads
  durable state and recovers it.
- An append racing the boundary is ordered by Redis either before or after the
  Lua read, so it falls into exactly one of those cases.
- The first page performs the one logical sliding-TTL touch. Rechecks are
  no-touch. Persistent and absolute-expiry-only reads remain write-free.
- Incarnation and content type fence every page, hub attach, and wait result.
  Delete and recreate therefore terminates the old lifetime instead of joining
  it to the new one.

This preserves SSE replay de-duplication: client catch-up stops at its fixed
snapshot tail and attaches to the hub at the last fully flushed control offset.
The hub starts at the same captured tail, so replay contains only later durable
work.

## Risks and proof obligations

- Lua and Go range classification can drift. A live Redis differential table
  must compare the returned Lua classification with the Go oracle, including
  unforked, before/at/above fork, zero tail, `now`, stale `ReadSeq`, and beyond
  tail cases.
- Root fusion could accidentally use the current tail on a continuation. Tests
  must append after capture and prove no later frame enters the snapshot.
- A discarded wait recheck can leak an immutable-segment lease. Every nonfinal
  registered recheck must release its snapshot.
- A transient SSE subscription can leak when another creator wins. Ownership
  transfer and close behavior must be deterministic and tested.
- Read-side command reductions must not come from hidden writes. Redis command
  traces, key TTLs, AOF size, and primary replication offset are independent
  gates.
