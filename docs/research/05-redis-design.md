# 05 — Redis data-model design study

**Purpose:** design the Redis 8 data model for chronicle's implementation of the
Durable Streams `store.Store` contract (see [03-caddy-store.md](03-caddy-store.md) §1).
Written 2026-06-10. Supplementary capability notes in
[08-srch-supplementary.md](08-srch-supplementary.md).

**Recommendation (TL;DR):** hybrid model — per-stream metadata HASH + fixed-size
chunk STRINGs (`APPEND`/`GETRANGE`) + a fixed-width message index STRING for JSON
mode + producer HASH with `HEXPIRE` + sharded pub/sub for tail notifications, all
keys under one `{path}` hash tag, every mutation a single Lua `EVALSHA`, client:
`go-redis/v9`. Details in §2.4, schema in §8, decision log in §9.

---

## 1. Requirements recap — what the protocol demands of storage

Distilled from the `store.Store` contract (03 §1) and the protocol semantics it
encodes. Every candidate model in §2 is judged against this list.

1. **Atomic append returning the new tail offset.** `Append` must validate
   (existence, expiry, closed, content type, `Stream-Seq`, producer epoch/seq),
   write the bytes, advance the tail, and return the new `Offset` — all as one
   indivisible operation. Two concurrent appends must serialize; no reader may
   ever observe a tail that points past bytes that are not yet readable, or bytes
   beyond the published tail.
2. **Byte-offset range reads.** `Read(path, offset)` starts at an arbitrary byte
   position previously issued by the store. `Offset = ReadSeq uint64 + ByteOffset
   uint64`, wire format `"%016d_%016d"`, lexicographically sortable. `ByteOffset`
   counts *data bytes only* — no framing may leak into the offset arithmetic.
   chronicle keeps `ReadSeq = 0` (no log rotation in v1) but must parse/emit the
   full format.
3. **Message-boundary preservation for JSON mode.** For `application/json`
   streams, appends of JSON arrays are split into individual messages, and `Read`
   must return `[]Message` with one entry per message and a per-message offset.
   The byte payload alone is not enough — the store needs a boundary index.
4. **Live-tail wakeups.** `WaitForMessages(ctx, path, offset, timeout)` must
   return promptly when data lands after `offset`, when the stream closes, on
   timeout, or on context cancellation — and must return *immediately* if data
   already exists at `offset`. Strict message-order preservation across catch-up,
   long-poll, and SSE reads is a conformance axis (08 §Ecosystem).
5. **Metadata.** Per-stream: content type, current offset, `LastSeq`
   (`Stream-Seq`), TTL fields, created/last-accessed timestamps, closed flag +
   `ClosedBy`, fork fields, `RefCount`, `SoftDeleted` (03 §1.2
   `StreamMetadata`). `Create` is idempotent under `ConfigMatches`, which is
   compared in Go (media-type normalization, nil-ness rules) — the store just
   needs an atomic create-if-absent that returns existing metadata.
6. **Producer records.** `producerId -> (epoch, lastSeq, lastUpdated)` with
   fencing: stale epoch rejected (return current epoch), new epoch must start at
   seq 0, `seq <= lastSeq` is a duplicate (succeed without writing, 204),
   `seq > lastSeq+1` is a gap error. Validation must be atomic *with* the append.
7. **TTL / ExpiresAt.** `IsExpired()` is `now > ExpiresAt` OR
   `now > LastAccessedAt + TTLSeconds` — a *sliding* TTL refreshed by access. The
   Caddy stores expire lazily on access; expired streams surface as
   `ErrStreamNotFound`.
8. **Closure.** `CloseStream` / `CloseStreamWithProducer` are idempotent;
   close-with-producer needs `ClosedBy` for duplicate detection; waiters must be
   woken with `streamClosed = true`.
9. **Forks.** A fork records `ForkedFrom`, `ForkOffset` (reads below it delegate
   to the source stream's bytes), `ForkSubOffset`, and holds a `RefCount` on the
   source; deleting a referenced source soft-deletes it (`SoftDeleted`) until the
   last fork releases it.
10. **Deletes.** Hard delete of all stream state, fork-aware as above.
11. **Error fidelity.** Unwrapped sentinel errors (`ErrStreamNotFound`,
    `ErrStaleEpoch`, …) compared with `==` — scripts must return distinguishable
    status codes that Go maps 1:1 to sentinels.

**Redis-imposed constraints that shape everything below** (08 §Redis): a single
STRING value caps at **512 MB**; `proto-max-bulk-len` bounds every bulk argument
and reply (default 512 MB, often *lowered* on managed Redis) — so both append
arguments and `GETRANGE` replies must stay below a configurable ceiling, and
multi-GB streams require chunked storage in every model.

## 2. Candidate data models

### 2.1 Model A — per-stream STRING + APPEND/GETRANGE, chunked

One STRING per stream is the naive fit: `APPEND` returns the post-append length
(= new ByteOffset directly), `GETRANGE start end` is exactly a byte-offset read.
The redis.io APPEND docs even document this pattern (fixed-size samples +
GETRANGE random access). But the 512 MB cap and `proto-max-bulk-len` kill the
single-key version, so the real Model A is **fixed-size chunks**:
`{path}:c:0, {path}:c:1, …` each holding exactly `S` bytes (last chunk partial).

- **Byte-offset read mapping:** chunk `n = O / S`, intra-chunk start `O mod S`.
  A read from `O` to `min(tail, O+maxRead)` is a pipeline of `GETRANGE
  {path}:c:n start end` calls — pure arithmetic, no index needed for raw streams.
- **Atomicity:** `MULTI/EXEC` cannot express "validate then conditionally write",
  so appends need server-side logic: Lua `EVALSHA` (or a Redis Function). One
  script reads meta, validates, splits the payload across the tail chunk and at
  most ⌈len/S⌉ successor chunks via `APPEND`, updates meta. Scripts execute
  atomically (server blocks for the duration) — no partial append is ever
  visible.
- **Memory / fragmentation:** `APPEND` is amortized O(1) via sds doubling;
  fixed-size chunks cap the preallocation waste at < `S` per stream (only the
  tail chunk is growing). Sealed chunks never reallocate again. Many small
  streams cost one key + sds header each — fine. jemalloc handles the churn of
  growing tail chunks well; with `S` ≤ a few MB the allocations stay in
  well-behaved size classes.
- **Cluster mode:** all keys carry the `{path}` hash tag, so the whole stream
  lives in one slot and one multi-key script is legal. Caveat: the set of chunk
  keys a script touches depends on the *current* tail, which the script learns
  only after reading meta — but cluster scripting requires all keys declared in
  `KEYS`. Resolution in §8 (client passes candidate chunk keys from its cached
  tail; script returns `RETRY` if the tail has moved past them).
- **Eviction risk:** catastrophic. Evicting one middle chunk silently corrupts
  the stream. Requires `noeviction` (§5) plus integrity checks on read
  (`STRLEN(tail chunk) == tail mod S`, missing chunk → corruption error, not an
  empty range).
- **TTL:** key TTLs alone can't express the sliding `LastAccessedAt + TTL`
  semantics, and per-chunk TTL refresh on every read is O(chunks). Lazy expiry
  check on access (like the Caddy stores) is authoritative; key TTLs serve only
  as a leak backstop (§2.4).
- **JSON mode:** raw bytes lose message boundaries — Model A alone fails
  requirement 3 and needs the index from §2.4 bolted on.

**Verdict:** the right storage *primitive* (byte-addressable, O(1) append,
cheap range reads), incomplete as a *model* (no boundaries, no notify). It is
the data plane of the recommended hybrid.



### 2.2 Model B — Redis Streams (XADD/XRANGE) with a byte-offset index

One Redis Stream per durable stream; one entry per message. The elegant trick:
use **custom entry IDs equal to the message's starting ByteOffset**
(`XADD {path}:s <startOffset>-0 d <payload>`). Cumulative byte offsets are
strictly monotonic, so custom IDs are always valid, and a read from offset `O`
becomes `XRANGE {path}:s O-0 + COUNT n` with no separate index. Message
boundaries are free (one entry = one message), which makes JSON mode trivial.
`XREAD BLOCK` even gives native long-polling.

Why it still loses:

- **Byte-granular reads don't exist.** `XRANGE` returns whole entries. A single
  large message must be fetched in one bulk reply (bounded by
  `proto-max-bulk-len`), and a catch-up read can't stop mid-message to respect a
  response-size budget. The byte-offset model degenerates into a message-offset
  model with byte-flavored names. Workable only if chronicle caps message size
  well below the bulk limit — an extra protocol-visible restriction Model D
  doesn't need.
- **Atomicity still needs Lua.** Producer fencing, `Stream-Seq`, closed checks,
  and multi-message JSON appends all have to wrap `XADD` in a script anyway — so
  Streams buy no atomicity simplification over Model A.
- **Offset arithmetic is fragile.** IDs-as-byte-offsets only stay truthful if
  entries are never trimmed or deleted out from under the ID sequence. Any future
  compaction/trim feature breaks the invariant; Model A's invariant
  (`tail = Σ appended bytes`) lives in one meta field instead of being smeared
  across thousands of entry IDs.
- **Memory:** good for mid-size messages (listpack-packed macro nodes amortize
  per-entry overhead to a few bytes), but each entry stores its ID + field name
  alongside the payload — for very small JSON messages overhead is tens of
  percent, vs ~16 bytes of index per message in Model D. For very large messages
  the entry is one giant listpack/raw allocation with no partial fetch.
- **Cluster mode:** fine — one key per stream, hash-tagged. `XREAD BLOCK` holds a
  connection per blocked call and can't be multiplexed the way one pub/sub
  channel subscription can; with thousands of concurrent long-pollers that's a
  connection-pool problem (one blocked conn per *stream* at minimum).
- **Eviction / TTL:** same story as all models — `noeviction` required; key TTL
  is whole-stream so the lazy-expiry design carries over unchanged.

**Verdict:** credible runner-up; cleanest JSON-mode story; rejected because the
protocol's read path is byte-ranged and response-size-budgeted, and Streams
force message-at-a-time I/O plus a per-message size cap tied to
`proto-max-bulk-len`.



### 2.3 Model C — HASH / LIST segment models

**HASH-of-chunks** (`HSET {path}:data <n> <chunk>`): superficially tidy (one key,
fields as segments) but Redis has **no HAPPEND and no HGETRANGE**. Every append
to the tail chunk is read-modify-write of the whole chunk inside Lua —
O(chunk size) per append instead of O(payload) — and every partial read fetches
a full chunk. Big-hash encoding also abandons listpack early, so there's no
memory win. A hash *is* the right shape for metadata and producer records
(small, heterogeneous, field-TTL via HEXPIRE), just not for the data plane.

**LIST-of-messages** (`RPUSH {path}:l <msg>`): preserves boundaries like Streams
but with weaker tooling. Byte offset → list index needs a separate index
structure (the same index Model D needs, minus Model D's byte-ranged data
plane). `LRANGE` is O(S+N) from the head for arbitrary indexes on a quicklist;
whole-element fetch only; no partial reads of large messages; per-element
overhead similar to Streams without `XRANGE`'s ID-addressed seeks or
`XREAD BLOCK`. Strictly dominated by Model B in every dimension.

**Verdict:** rejected for data. Retained insight: HASH for metadata/producers,
and "fixed-width records in an appendable blob" (the APPEND-docs time-series
pattern) for the message index, which a HASH cannot do but a STRING can.



### 2.4 Model D (recommended) — hybrid: meta HASH + chunk STRINGs + message index + pub/sub

Take Model A's data plane and add the three pieces it lacks. Per stream (full
key schema in §8):

| Piece | Structure | Role |
|---|---|---|
| `ds:{path}:meta` | HASH | All `StreamMetadata` fields; `tail` (ByteOffset) is the single source of truth for the append cursor |
| `ds:{path}:c:<n>` | STRING ×N | Fixed-size data chunks (default `S` = 1 MiB), `APPEND`-grown tail chunk |
| `ds:{path}:idx` | STRING | JSON streams only: fixed-width record per message = start ByteOffset as 16-digit zero-padded ASCII (mirrors the offset wire format; sliced with `GETRANGE`, appended with `APPEND`) |
| `ds:{path}:prod` | HASH | `producerId -> "epoch:lastSeq:lastUpdatedMs"`, per-field `HEXPIRE` |
| `ds:{path}:notify` | pub/sub channel | Tail notifications: payload `"<newOffset>|<closedFlag>"` |

The index uses ASCII rather than packed binary deliberately: Redis Lua is 5.1
(no `string.pack`; the bundled `struct` lib plus Lua's 52-bit doubles make
8-byte binary packing fiddly), 16-digit decimal matches the `%016d` offset
encoding exactly, is debuggable with `redis-cli`, and costs 16 bytes/message —
noise next to the payload. Message `i`'s boundaries come from index records `i`
and `i+1` (or `tail` for the last message); a read at boundary offset `O` finds
its record by direct computation when chronicle issued `O` as message `k`'s
offset, or by binary search over `GETRANGE` probes (O(log msgCount) round trips,
pipelined) in the rare cold-cache case.

- **Atomicity:** every mutation is exactly one `EVALSHA` (script catalog in §8):
  expiry-check → validation → producer fencing → chunk `APPEND`s → `idx`
  `APPEND` → meta `HSET` → `HEXPIRE` → `(S)PUBLISH`, executed atomically.
  `MULTI/EXEC` is rejected (cannot branch on validation); Redis Functions are
  rejected — see [ADR-0001](../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md):
  they give no correctness or replication benefit over effects-replicated scripts,
  regress failover ergonomics under go-redis (no `FCall` auto-reload), and are
  blocked on AWS ElastiCache Serverless / MemoryDB Multi-Region while `EVAL` runs
  everywhere. go-redis's `Script` helper does the
  `NOSCRIPT → full-source EVAL → retry` dance automatically — it re-runs the full
  source via `EVAL`, which re-caches it (*not* `SCRIPT LOAD`); the script cache is
  not persistent and is flushed on restart/failover.
- **Byte-offset read mapping:** identical to Model A (arithmetic + pipelined
  `GETRANGE`), plus one `GETRANGE` on `idx` for JSON streams. Reads run as plain
  pipelined commands from Go, *not* in Lua — Lua replies are also bounded by
  `proto-max-bulk-len`, and assembling in Go keeps each bulk ≤ `S`.
  Read snapshot consistency without a transaction: read `meta` first, treat its
  `tail` as the snapshot bound, and never return bytes past it; chunks are
  append-only so any concurrently-landing bytes beyond the snapshot are simply
  ignored.
- **Memory / fragmentation:** as Model A; plus one small hash (ziplist/listpack
  encoded — keep meta values short), the index string (16 B/msg), and zero
  overhead for raw streams (no `idx` key at all).
- **Cluster mode:** every key shares the `{path}` hash tag → one slot → scripts
  legal, `SPUBLISH`-able, and slot migration moves the stream as a unit. The
  only cross-slot operations are fork bookkeeping (source and fork are different
  paths/slots) — decomposed into two single-slot scripts with reconciliation
  (§8, `ds_fork`/`ds_release`).
- **Eviction:** `noeviction` required (§5). Defense in depth: every read
  validates `STRLEN` of the touched chunks against meta-derived expectations and
  returns a corruption sentinel on mismatch rather than serving a hole.
- **TTL:** two layers. (1) *Authoritative lazy expiry*, exactly like the Caddy
  stores: meta carries `ttlSeconds` / `expiresAtMs` / `lastAccessedAtMs`; every
  script's preamble and every Go read path evaluates `IsExpired` and, if
  expired, deletes the stream's keys and reports `NOTFOUND`. (2) *Backstop key
  TTLs* so streams nobody ever touches again don't leak: on each append (and at
  most once per minute on reads) set `EXPIRE` on all stream keys to
  `remaining + slack`, with chunk/idx/prod slack strictly larger than meta's so
  meta always disappears first (no half-visible stream). Streams with neither
  TTL nor ExpiresAt get `PERSIST`-ed keys. `lastAccessedAt` updates on read are
  throttled (skip if < 1 s or < TTL/10 since last write) to avoid turning every
  read into a write.

### 2.5 Comparison matrix

| Dimension | A: chunked STRING | B: Redis Streams | C: HASH/LIST | D: hybrid |
|---|---|---|---|---|
| Atomic append + validation | Lua required | Lua required | Lua required | Lua, one script |
| Byte-offset read | arithmetic + GETRANGE | message-granular only | full-element only | arithmetic + GETRANGE |
| JSON boundaries | ✗ (needs index) | ✓ native | ✓ (LIST) | ✓ via idx string |
| Large message (≫ bulk limit budget) | ✓ split across chunks/reads | ✗ one bulk per entry | ✗ | ✓ |
| Per-small-message overhead | 0 | ID+field per entry | pointer/entry | 16 B index |
| Live tail | needs pub/sub | XREAD BLOCK (conn/stream) | needs pub/sub | pub/sub + poll fallback |
| Cluster | hash tag, declared-keys retry | hash tag, trivial keys | hash tag | hash tag, declared-keys retry |
| Eviction sensitivity | corruption on chunk loss | whole-key loss only | whole-key loss | corruption, but detected on read |
| TTL | lazy + backstop | lazy + backstop | lazy + backstop | lazy + backstop |
| Verdict | incomplete | runner-up | rejected | **recommended** |

## 3. Live tailing — waking up long-pollers

`WaitForMessages` maps to a **publish-on-append + waiter loop**:

1. The append/close script ends with `PUBLISH ds:{path}:notify
   "<newTailOffset>|<closed>"` (`SPUBLISH` in cluster mode — classic `PUBLISH`
   broadcasts to every cluster node; sharded pub/sub (Redis 7+) routes by the
   channel's slot, which the `{path}` hash tag co-locates with the data).
2. chronicle runs **one shared pub/sub connection** (per shard) with an
   in-process waiter registry: `path -> set of waiting goroutines`. A waiter
   registers, then the connection `(S)SUBSCRIBE`s to that stream's channel if
   not already subscribed; last waiter out unsubscribes.
3. **The missed-wakeup race** — data can land between the caller's initial read
   (which found nothing) and the subscription becoming active; the publish for
   it is gone forever (pub/sub has no replay). Mandatory ordering:
   *subscribe first, then re-check the tail, then wait.* Concretely:
   register waiter → await subscription confirmation → `GetCurrentOffset` (or a
   direct read from the caller's offset) → if data exists, return immediately;
   else block on {notification, timeout, ctx}. On notification, parse the
   payload: if `closed`, return `streamClosed`; if offset ≤ caller's offset
   (stale/duplicate wakeup), keep waiting; else re-read.
4. **Bounded poll fallback:** even a correctly-ordered subscriber can lose
   messages — pub/sub is fire-and-forget; a failover drops the subscription, and
   `client-output-buffer-limit pubsub` (default 32mb/8mb/60s) makes the server
   *disconnect* slow subscribers. So every waiter also wakes on a poll tick
   (default 5 s, configurable) and re-checks the tail, and any pub/sub
   reconnect triggers an immediate re-check broadcast to all registered waiters.
   Worst-case added latency after a lost wakeup is one poll interval; the
   common-case latency is one publish hop (~sub-ms).

**Alternatives considered:**

- *Pure polling* — correct and simple, but it's a hard latency/load tradeoff:
  1 s polls × 10k waiters = 10k reads/s of mostly-empty tails. Kept only as the
  fallback tick.
- *Keyspace notifications* — require `notify-keyspace-events` via `CONFIG SET`,
  which managed Redis routinely denies (and Walmart-managed Redis offers no
  CONFIG, §5); events carry no payload (just "key was touched"), and they're
  still fire-and-forget pub/sub underneath. Strictly worse than publishing our
  own message with the new tail in it.
- *`XREAD BLOCK`* — only meaningful for Model B; holds a blocked connection per
  call and multiplexes poorly compared to one subscription connection serving
  every waiter.

**Recommendation:** pub/sub publish-on-append with tail-in-payload + subscribe →
re-check → wait ordering + bounded poll fallback + re-check on reconnect.

## 4. Idempotency records — producer state

Producer state lives in a **dedicated HASH per stream**, `ds:{path}:prod`,
field = `producerId`, value = `"<epoch>:<lastSeq>:<lastUpdatedMs>"` (compact,
splittable in Lua with `string.match`, no cjson cost). A separate hash (rather
than fields inside `:meta`) keeps meta listpack-small, lets the whole producer
map age out independently, and gives **HEXPIRE (Redis ≥ 7.4, native in 8.x)**
a clean target: each accepted producer write re-arms a per-field TTL (the
idempotency window, default 24 h), so dead producers garbage-collect themselves
without touching live ones.

Two HEXPIRE caveats to encode in tests (08 §Redis):
- `HSET` on an existing field **discards that field's TTL** — harmless here
  because the script re-arms `HEXPIRE` immediately after every `HSET` of the
  field, in the same atomic script.
- On pre-7.4 Redis (if chronicle ever runs there), fall back to lazy pruning:
  `lastUpdatedMs` is already in the value; the script treats an over-age record
  as absent, and `ds_delete`/a maintenance sweep HDELs stale fields.

**Validation is inside the same Lua script as the append** — this is the entire
point. The fencing decision and the byte write must be one atomic step or two
racing producers can both pass validation. `ds_append`'s producer block (Lua
pseudocode):

```lua
-- ARGV: pid, epoch, seq  (all empty => no producer headers)
local cur = redis.call('HGET', prodKey, pid)
if cur then curEpoch, curSeq = cur:match('^(%-?%d+):(%-?%d+):') end
if cur and epoch < curEpoch then return {'ERR_STALE_EPOCH', curEpoch} end
if (not cur or epoch > curEpoch) and seq ~= 0 then return {'ERR_EPOCH_SEQ'} end
if cur and epoch == curEpoch then
  if seq <= curSeq then return {'DUPLICATE', curSeq, tail} end  -- no write, 204
  if seq > curSeq + 1 then return {'ERR_SEQ_GAP', curSeq + 1, seq} end
end
-- accepted: perform append, then:
redis.call('HSET', prodKey, pid, epoch .. ':' .. seq .. ':' .. nowMs)
redis.call('HEXPIRE', prodKey, windowSec, 'FIELDS', 1, pid)
```

Notes:
- `ErrPartialProducer` (some-but-not-all headers) is validated in Go before any
  Redis call — it needs no state.
- Duplicates return the *current tail* so the handler can respond 204 with the
  right offset, and `lastSeq` for the response headers (03 §1.2
  `AppendResult`).
- `CloseStreamWithProducer` runs the same fencing in `ds_close`; on success it
  stores `closedBy = pid:epoch:seq` in meta so a retried close is recognized as
  a duplicate even after the producer field expires.
- Everything is keyed under `{path}` — producer state replicates and fails over
  *with* the data it fences, which is what makes the failover story in §6
  coherent (state and data regress together, never independently).

## 5. Designing for Walmart-managed Redis

Assume the least-privileged managed profile and degrade gracefully; everything
below is a startup-probe + config-flag, not a hard dependency.

| Constraint | Design response |
|---|---|
| **Cluster mode possible** | `{path}` hash tags on every key and channel from day one (zero cost on standalone). All scripts single-slot with fully declared `KEYS`. Sharded pub/sub (`SSUBSCRIBE`/`SPUBLISH`) when cluster is detected, classic otherwise. Cross-slot fork bookkeeping decomposed (§8). Slot migrations surface as `MOVED`/`ASK` — the client retries; atomicity is preserved because a stream never spans slots. |
| **No `CONFIG`** | Never rely on `CONFIG SET` (rules out keyspace notifications, raising `proto-max-bulk-len`, changing `maxmemory-policy`). At startup, *attempt* `CONFIG GET maxmemory-policy appendonly proto-max-bulk-len`; if denied, log that guarantees are operator-asserted and continue. `noeviction` becomes a **documented deployment requirement** enforced by runtime integrity checks (chunk `STRLEN` validation) that turn silent eviction into loud corruption errors. |
| **Restricted Lua?** | `EVAL`/`EVALSHA`/`SCRIPT LOAD` are the most widely permitted programmability surface; **Redis Functions (`FUNCTION LOAD`) are not used** — not because they are unavailable (they are supported on the operator's managed enterprise Redis) but because they are blocked on ElastiCache Serverless / MemoryDB Multi-Region while `EVAL` is universal, and give no correctness gain (effects replication makes plain scripts equally safe). See [ADR-0001](../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md). Scripts: no globals, no `redis.breakpoint`, all keys declared, **`nowNs` passed as ARGV** (one clock — the Go process — for all TTL/expiry math; this gives single-clock consistency between Go's framing and Lua's TTL math — *not* a replication-determinism requirement, since under effects replication `TIME` is legal). Script cache is volatile (flushed on restart/failover) → go-redis `Script.Run`'s automatic NOSCRIPT→full-source-EVAL fallback. If even EVAL is denied, the model has **no non-Lua fallback for writes** — `MULTI/EXEC` cannot do conditional validation; this is a stated platform requirement, verified by a startup self-test that EVALs a trivial script. |
| **Lowered `proto-max-bulk-len`** | Chunk size `S` (default 1 MiB) and `max_append_size` are configuration, with the constraint `S ≤ limit` and `max_append_size ≤ limit` (the whole append body travels as one ARGV). Reads are assembled in Go from ≤ `S`-sized GETRANGE replies, never as one giant Lua reply. A startup probe appends/reads a `S`-sized value to a scratch key to verify the effective limit. |
| **ACL-restricted commands** | No `KEYS`, no `DEBUG`, no `FLUSH*`. `SCAN` used only by the optional orphan-sweeper (per-node in cluster). `WAIT` treated as optional (§6) and feature-flagged. |
| **Connection limits / TLS** | One pooled client for request traffic + one dedicated pub/sub connection per shard; pool sizing config. TLS + AUTH assumed. |

Degradation summary: on the most restricted plausible deployment (cluster, no
CONFIG, EVAL allowed, low bulk limit), chronicle runs at full fidelity with
smaller chunks and operator-asserted `noeviction`/AOF. The only true hard
requirements are **EVAL and pub/sub**.

## 6. Failure analysis — what chronicle can honestly claim

**Partial failure on append: impossible at the data level.** A Lua script
executes atomically — the server is blocked for its duration and either all of
its effects happen or none are visible. There is no state where chunk bytes
exist but the tail wasn't advanced, or producer state advanced without bytes.
The multi-key layout *can* be partially destroyed from outside (eviction,
manual DEL, chunk TTL firing before meta) — mitigated by `noeviction`, the
meta-expires-first TTL slack ordering, and read-time `STRLEN` integrity checks
that return a corruption sentinel instead of fabricating data.

**Connection loss mid-EVAL: ambiguous outcome, by design recoverable.** Once
Redis starts a script it runs to completion regardless of client disconnect —
so the append may or may not have happened from chronicle's point of view.
- *With producer headers:* the retry is safe — if the script ran, the retry hits
  the duplicate branch and returns 204 with the recorded offset. This is exactly
  what the protocol's idempotent-producer mechanism exists for.
- *Without producer headers:* a retry may double-append. chronicle should
  surface the ambiguous error to the caller rather than auto-retrying writes.
  **Claim: exactly-once append only with producer headers; at-least-once
  otherwise.** Same as the protocol intends.

**Replica failover loses acked writes — the headline caveat.** Redis
replication is asynchronous: a master can ack an append (even an AOF-fsynced
one) and die before the replica receives it; after promotion those bytes are
gone. Consequences and mitigations:
- A client may hold an offset **greater than the new tail**. Reads at
  `offset > tail` must be detected (cheap: compare against meta tail) and
  treated as a distinguishable error, not an empty long-poll — otherwise the
  reader silently waits until *different* bytes occupy "its" offsets and it
  reads a frankenstream. This check is mandatory in `Read`/`WaitForMessages`.
- Producer state and data live in the same slot and regress *together*, so a
  surviving producer recovers coherently: its next append sees the regressed
  `lastSeq`, gets a gap/duplicate verdict consistent with the regressed data,
  and (for a well-behaved client re-sending from its unacked queue) re-appends
  the lost suffix.
- **`WAIT numreplicas timeout`** after each append narrows the window:
  it blocks until the write reaches N replicas' buffers. It is *not* a
  durability guarantee (no rollback on failure — the write stays locally,
  un-replicated; replica receipt ≠ replica fsync) and costs ~RTT per append.
  Offer as opt-in (`wait_replicas`, `wait_timeout_ms`) for streams that prefer
  latency-for-safety; note `WAIT` may itself be ACL-blocked (§5).
- Honest durability statement for the README: *appends are as durable as the
  underlying Redis deployment — AOF `everysec` bounds loss to ~1 s on
  single-node crash; async replication means failover can drop a recently-acked
  suffix; chronicle detects and reports the resulting offset regressions rather
  than hiding them.* chronicle is a protocol-faithful store over Redis's
  guarantees; it cannot exceed them.

**Pub/sub message loss** (failover, slow-subscriber disconnect): liveness-only
impact, bounded by the poll fallback tick (§3); never a correctness issue since
notifications carry no unique data.

**Cluster resharding mid-operation:** scripts target one slot; the client
follows `MOVED`/redirects and re-runs. A re-run append is covered by the same
ambiguity analysis as connection loss (idempotent with producers).

**Process crash in two-phase fork creation** (cross-slot, §8): can leak a
source `RefCount` increment with no fork meta. Self-healing via the
`ds:{src}:forks` registry — `ds_release`/the sweeper reconciles registry
entries whose fork meta is absent.

## 7. Go client choice — go-redis/v9 vs rueidis

Both live under the `redis` GitHub org and are actively maintained (08 §Go
client). What this workload actually exercises: `EVALSHA` on the hot write
path, long-lived pub/sub with reconnect handling, pipelined `GETRANGE` reads,
cluster routing, RESP3.

| Axis | go-redis/v9 | rueidis |
|---|---|---|
| Lua scripts | `redis.Script` — SHA caching + automatic NOSCRIPT→EVAL→retry (re-runs full source via `EVAL`, which re-caches; *not* `SCRIPT LOAD`) | `rueidis.NewLuaScript` — equivalent, fine |
| Pub/sub ergonomics | `PubSub` type: channel-based API, **automatic reconnect + resubscribe**, `SSubscribe` for sharded | `Dedicate()`/`Receive()` loops; reconnect/resubscribe is hand-rolled by the app |
| Cluster | mature `ClusterClient`, script routed by first key, MOVED/ASK handled | mature, same coverage |
| RESP3 | supported (default protocol 3) | RESP3-first by design |
| Raw throughput | per-conn round trips; explicit pipelining | auto-pipelining — significantly higher ops/s in benchmarks; latency tradeoffs at low concurrency (rueidis#609/#626) |
| Ecosystem / familiarity | canonical client, broadest adoption, otel hooks, most examples | smaller; API less conventional (command builder) |

**Recommendation: `github.com/redis/go-redis/v9`.**

Reasoning: chronicle's correctness-critical component is the **waiter loop**,
and go-redis's `PubSub` gives exactly the lifecycle the §3 design needs
(auto-reconnect with resubscribe, a hook point for the "re-check on reconnect"
broadcast) out of the box, where rueidis requires hand-rolling the most
bug-prone part. The hot path is one `EVALSHA` per append whose cost is
dominated by Redis-side byte copying, not client CPU — rueidis's
auto-pipelining advantage targets a bottleneck this workload doesn't have at
expected scale. Read paths use explicit pipelines, which go-redis expresses
fine. `Script.Run`'s NOSCRIPT fallback directly covers the volatile-script-cache
failure mode (§5). And ubiquity matters in a Walmart codebase: more reviewers,
more internal precedent, otel integration. If profiling ever shows
client-side ceilings, the store interface contains the blast radius of a
rueidis migration.

## 8. Recommended key schema and Lua script catalog

### 8.1 Key schema

`path` below is the stream path verbatim (e.g. `/v1/stream/orders/123`),
embedded in `{…}` so every key and the channel hash to the same cluster slot.

| Key / channel | Type | Contents | TTL |
|---|---|---|---|
| `ds:{<path>}:meta` | HASH | `ctype`, `tail` (ByteOffset, uint64 decimal), `readSeq` (always `0` in v1), `msgCount`, `chunkSize`, `lastSeqHdr` (Stream-Seq), `ttlSec`, `expiresAtMs`, `createdAtMs`, `lastAccessedAtMs`, `closed` (0/1), `closedBy` (`pid:epoch:seq`), `forkedFrom`, `forkOffset`, `forkOffsetReq`, `forkSubOffset`, `refCount`, `softDeleted` | backstop = remaining + slack |
| `ds:{<path>}:c:<n>` | STRING | data bytes `[n·S, (n+1)·S)`; `n` decimal from 0; `S` frozen per-stream in meta at create | backstop, slack > meta's |
| `ds:{<path>}:idx` | STRING | JSON streams only; record `i` = bytes `[16i, 16i+16)` = `%016d` start ByteOffset of message `i` | backstop, slack > meta's |
| `ds:{<path>}:prod` | HASH | `producerId -> "epoch:lastSeq:lastUpdatedMs"` | per-field HEXPIRE (idempotency window) |
| `ds:{<path>}:forks` | SET | child fork paths (refcount reconciliation registry) | none while refCount > 0 |
| `ds:{<path>}:notify` | pub/sub channel | payload `"<readSeq>_<byteOffset>|<closed01>"`; `SPUBLISH` in cluster | n/a |

### 8.2 Lua script catalog

> **Correction (ADR-0001).** This catalog is a design-time spec and names the
> clock parameter `nowMs` throughout; the **implemented** scripts pass and compute
> in **nanoseconds** (`nowNs` / `UnixNano`, see `store/redis/scripts/common.lua`).
> Read every `nowMs`/`*Ms` below as the nanosecond equivalent. The clock is passed
> from Go for single-clock consistency (not replication determinism). See
> [ADR-0001](../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md).

Shared preamble for every script: load meta; if absent → `{'NOTFOUND'}`; else
evaluate lazy expiry against `ARGV nowMs` (`expiresAtMs`, or
`lastAccessedAtMs + ttlSec`); if expired → delete all declared stream keys,
return `{'NOTFOUND'}`. All scripts return a flat array whose first element is a
status token that Go maps 1:1 onto the sentinel errors (03 §1.1). `nowMs`
always comes from Go (single clock; no `TIME` in scripts).

| Script | KEYS | ARGV | Returns |
|---|---|---|---|
| `ds_create` | `meta, prod, idx, c:0` | `nowMs, ctype, ttlSec?, expiresAtMs?, closed01, forkedFrom?, forkOffset?, forkOffsetReq?, forkSubOffset?, chunkSize, initialData?, initialMsgStarts?` (CSV) | `{'CREATED', tailOffset}` \| `{'EXISTS', <full meta field/value list>}` — config comparison (`ConfigMatches`, media-type normalization) happens in Go against the returned meta; the script only guarantees atomic create-if-absent |
| `ds_append` | `meta, prod, idx, c:<n>, c:<n+1>, …, c:<n+k>` (candidate chunks computed in Go from a cached tail) | `nowMs, data, msgStarts` (CSV, relative; empty for raw)`, ctypeHdr?, seqHdr?, close01, pid?, epoch?, seq?, prodWindowSec, backstopSec` | `{'OK', readSeq, byteOffset, producerResult, lastSeq, closed01}` \| `{'RETRY', actualTail}` (tail moved past candidate chunk keys — Go recomputes keys and re-EVALs; converges because the same script is the only tail-mover) \| `{'ERR_CLOSED'}` \| `{'ERR_CTYPE'}` \| `{'ERR_SEQ_CONFLICT', lastSeqHdr}` \| `{'ERR_STALE_EPOCH', curEpoch}` \| `{'ERR_EPOCH_SEQ'}` \| `{'ERR_SEQ_GAP', expected, received}` \| `{'DUPLICATE', lastSeq, tailOffset, closed01}` \| `{'NOTFOUND'}`; ends with `(S)PUBLISH` on success/close |
| `ds_close` | `meta, prod` | `nowMs, pid?, epoch?, seq?, prodWindowSec` | `{'OK', tailOffset, alreadyClosed01, producerResult, …}` — close-only; producer branch consults/updates `closedBy` for idempotent duplicate detection; publishes `closed=1` |
| `ds_touch` | `meta, prod, idx, c:0…c:<n>` | `nowMs, backstopSec` | `{'OK'}` — throttled lastAccessedAt update + backstop TTL refresh (called by read path at most ~1/min/stream); chunk keys enumerated from Go's view of meta, RETRY-on-growth like `ds_append` |
| `ds_delete` | `meta, prod, idx, forks, c:0…c:<n>` | `nowMs` | `{'OK', refCount, softDeleted01, forkedFrom}` — if `refCount > 0`: set `softDeleted=1`, keep keys; else `DEL` all and return `forkedFrom` so Go can call `ds_release` on the source's slot |
| `ds_fork` (phase 1, runs on **source** slot) | `srcMeta, srcForks` | `nowMs, forkPath, forkOffset?` | `{'OK', resolvedForkOffset, srcTail, srcCtype}` \| `{'ERR_FORK_OFFSET'}` — validates offset ≤ source tail, `INCR refCount`, `SADD` registry; phase 2 = `ds_create` on the fork's slot with fork fields; crash between phases is reconciled from the registry |
| `ds_release` (runs on **source** slot) | `srcMeta, srcForks, srcProd, srcIdx, srcC:0…` | `nowMs, forkPath` | `{'OK', refCount}` — `SREM` + `DECR refCount`; if `refCount == 0 && softDeleted` → physical delete (cascading release if the source is itself a fork is driven from Go, slot by slot) |

Read paths (`Get`, `Has`, `Read`, `GetCurrentOffset`, the re-check in
`WaitForMessages`) are plain pipelined commands, not scripts: `HGETALL meta`
(+ Go-side `IsExpired` check; expired → fire `ds_delete` and report not-found)
→ for reads, `GETRANGE idx` slice (JSON) + `GETRANGE c:<n>…` data slices,
bounded by the response budget, with `STRLEN` integrity validation. Fork reads
below `forkOffset` re-enter the read path against `forkedFrom`'s keys
(different slot; plain reads need no cross-slot atomicity — each stream's own
meta/tail snapshot bounds its segment).

### 8.3 Worked example — `ds_append` chunk math

Stream tail = 2,621,440 (2.5 MiB), `S` = 1 MiB, payload = 1.5 MiB, raw mode.
Go passes `KEYS = meta, prod, idx, c:2, c:3, c:4`. Script: verifies
`tail/S = 2` is within passed chunk keys; `APPEND c:2` the first 0.5 MiB
(filling it to exactly `S`), `APPEND c:3` the remaining 1 MiB (`c:4` was passed
defensively but ends up untouched); new tail = 4,194,304; `HSET meta tail … lastAccessedAtMs …`; backstop `EXPIRE`s;
`SPUBLISH ds:{path}:notify "0000000000000000_0000000004194304|0"`; returns
`{'OK', 0, 4194304, …}`. A concurrent append that raced ahead would have made
`tail/S = 5` ≥ the passed key range → `{'RETRY', newTail}` with no writes.

## 9. Decision log

1. **Data model: hybrid (Model D)** — meta HASH + fixed-size chunk STRINGs +
   fixed-width message-index STRING (JSON only) + producer HASH + pub/sub
   channel. Chunked STRING is the only model giving true byte-ranged,
   budget-bounded reads under `proto-max-bulk-len`.
2. **Chunk size `S` = 1 MiB default, configurable, frozen per stream at create**
   (stored in meta) — small enough for lowered bulk limits, large enough that
   per-key overhead is noise.
3. **All mutations via single Lua `EVALSHA` scripts; no MULTI/EXEC, no Redis
   Functions** — MULTI can't branch on validation; Functions are less portable
   on managed platforms with zero benefit under effects replication.
4. **All keys + channel share the `{path}` hash tag** — single-slot streams make
   scripts cluster-legal and failover-coherent; the only cross-slot flows are
   fork create/release, decomposed with a registry-based reconciliation.
5. **Cluster declared-keys problem solved with candidate-chunk-keys + RETRY** —
   Go passes chunk keys computed from a cached tail; the script verifies and
   returns `RETRY` with the actual tail if stale.
6. **JSON message index = 16-digit ASCII start-offsets in an APPEND-grown
   STRING** — mirrors the `%016d` offset wire format, GETRANGE-sliceable,
   no binary packing in Lua 5.1, 16 bytes/message.
7. **Live tail = publish-on-append (tail offset + closed flag in payload),
   sharded pub/sub in cluster, one shared subscriber connection + waiter
   registry, subscribe → re-check → wait ordering, bounded poll fallback
   (5 s) + re-check on reconnect.** Keyspace notifications rejected (need
   CONFIG, no payload); pure polling rejected (latency/load).
8. **Producer state in a dedicated `:prod` HASH with per-field HEXPIRE
   (re-armed every write because HSET discards field TTLs), validated inside
   the same script as the append**; `closedBy` in meta makes producer-close
   idempotent beyond the field's lifetime.
9. **TTL = lazy authoritative expiry on access (Caddy-compatible sliding
   semantics) + backstop key TTLs with meta-expires-first slack ordering**;
   `lastAccessedAt` writes throttled on reads.
10. **`noeviction` is a deployment requirement, not an assumption** — verified
    when CONFIG is permitted, otherwise enforced by read-time STRLEN integrity
    checks that convert silent eviction into explicit corruption errors.
11. **Honest guarantees:** exactly-once appends only with producer headers
    (retry-after-ambiguity hits the duplicate branch); async replication means
    failover can drop acked suffixes — chronicle detects `offset > tail` and
    reports regression instead of serving a frankenstream; optional `WAIT`
    narrows but does not close the window.
12. **Client: `go-redis/v9`** — PubSub auto-reconnect/resubscribe covers the
    most bug-prone component, `Script.Run` handles the volatile script cache,
    canonical adoption; rueidis's auto-pipelining targets a bottleneck this
    workload doesn't have, and the store interface contains any future
    migration.
