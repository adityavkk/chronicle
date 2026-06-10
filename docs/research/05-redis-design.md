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
  rejected for portability (§5) — `FUNCTION LOAD` is admin-flavored and blocked
  on more managed platforms than `EVALSHA`/`SCRIPT LOAD`, and effects-based
  replication makes plain scripts equally safe. go-redis's `Script` helper does
  the `NOSCRIPT → SCRIPT LOAD → retry` dance automatically (the script cache is
  not persistent and is flushed on restart/failover).
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

TODO

## 4. Idempotency records — producer state

TODO

## 5. Designing for Walmart-managed Redis

TODO

## 6. Failure analysis — what chronicle can honestly claim

TODO

## 7. Go client choice — go-redis/v9 vs rueidis

TODO

## 8. Recommended key schema and Lua script catalog

TODO

## 9. Decision log

TODO
