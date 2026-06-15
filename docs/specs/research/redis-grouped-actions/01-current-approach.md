# 01 — How chronicle does grouped Redis actions today

This is a code-accurate description of the status quo, so the comparison docs
([02](02-redis-functions.md), [03](03-managed-platform-support.md),
[04](04-alternatives-and-prior-art.md)) and the
[recommendation](05-recommendation.md) argue against something real, not a
strawman.

## The shape of the problem

Chronicle never mutates a stream with a single Redis command. Every write is a
*group* of reads + conditional branches + writes + a notify that must commit as
one atomic unit, because the Durable Streams protocol attaches correctness
guarantees to the group:

- an append must validate **existence → soft-delete → lazy TTL expiry → closed
  → content-type → idempotent-producer epoch/seq fencing → `Stream-Seq`
  regression → optimistic tail** *in that precedence order*, then write frames +
  advance the tail + update meta + `PUBLISH`, or do none of it
  (`store/redis/scripts/append.lua:34-142`);
- a subscription claim is a compare-and-set on a lease with fence-id rotation,
  so two workers racing the same wake collide on one fence instead of both
  "winning" (`webhook/scripts/claim.lua:16-39`);
- a close records a `closedBy = id:epoch:seq` tuple so a retried close is
  recognized as a duplicate even after the producer field expires
  (`store/redis/scripts/close.lua:27-62`).

None of this is expressible as one plain command, and it cannot branch inside a
`MULTI/EXEC` transaction (see [04](04-alternatives-and-prior-art.md)). So
chronicle runs the whole group as **server-side Lua, executed atomically on
Redis's single-threaded engine.**

## The mechanism: embedded Lua + a concatenated prelude + `Script.Run`

Two subsystems each own a directory of `.lua` files embedded into the binary
with `//go:embed`:

| Subsystem | Scripts | Shared prelude |
|---|---|---|
| `store/redis/scripts/` | `append`, `create`, `close`, `read`, `delete`, `incr_ref`, `decr_ref` (7) | `common.lua` (~104 lines) |
| `webhook/scripts/` | `create_sub`, `link_stream`, `unlink_stream`, `arm_wake`, `claim`, `ack`, `release`, `expire_lease`, `claim_due`, `schedule_retry`, `record_success`, `record_wake_sent`, `delete_sub`, `get_or_create_key` (14) | `common.lua` (~31 lines) |

~859 lines of Lua total, in ~21 script bodies plus 2 preludes.

The shared helpers are injected by **string concatenation at load time**, not by
any Redis-level module system (`store/redis/scripts.go:17-27`,
`webhook/scripts.go:16-26`):

```go
func loadScript(name string) *redis.Script {
    prelude, _ := scriptFS.ReadFile("scripts/common.lua")
    body, _    := scriptFS.ReadFile("scripts/" + name)
    return redis.NewScript(string(prelude) + "\n" + string(body))
}
```

So **every script body ships a full duplicate copy of its `common.lua`.** The
`append`, `create`, `close`, `delete`, `incr_ref`, `decr_ref` scripts each carry
the entire 104-line store prelude (`meta_map`, `is_expired`, `expire_cleanup`,
`refresh_backstop`, `norm_ct`, `make_reply`, `validate_producer`); the 14
webhook scripts each carry the 31-line webhook prelude (`offset_greater`,
`split_link`, `fenced`).

Invocation is uniform: everything runs through go-redis's `*redis.Script`
helper, i.e. `EVALSHA` with an automatic fallback (`store/redis/store.go` and
`webhook/redis_store.go:37` both call `script.Run(...)`). go-redis computes the
SHA1 of the *concatenated* source, so the SHA is a function of `prelude + body`
together.

> **Correction worth recording.** The repo currently describes this fallback as
> "`NOSCRIPT → SCRIPT LOAD → retry`" (`docs/research/05-redis-design.md:223,460`;
> `docs/PLAN.md`). That is **not** what `redis.NewScript(...).Run()` does. The
> go-redis v9.20.0 source is `r := s.EvalSha(...); if errors.Is(r.Err(),
> ErrNoScript) { return s.Eval(...) }` — on `NOSCRIPT` it re-runs the **full
> source via `EVAL`** (which also re-populates the cache), and it *never* issues
> `SCRIPT LOAD`. The only go-redis constructor that uses `SCRIPT LOAD` is the
> newer `NewScriptServerSHA` (added v9.19.0). This matters because Redis 7.4+
> (inherited by Redis 8) added LRU eviction of **`EVAL`-loaded** scripts (hard
> cap ~500) and explicitly **exempts** `SCRIPT LOAD`-ed ones — so chronicle's
> scripts currently sit in the *evictable* bucket. See
> [02](02-redis-functions.md) and [05](05-recommendation.md).

## Design properties worth preserving (whatever the mechanism)

These are deliberate and correct, and any alternative must keep them:

- **One clock.** `nowNs` is passed as `ARGV[1]`, never read from the server's
  `TIME`. All TTL/expiry/lease math uses the Go process's clock, which keeps the
  scripts deterministic and keeps expiry decisions consistent with the Go read
  paths that share the same `IsExpired` logic
  (`store/redis/scripts/common.lua:19-26`).
- **Fixed-shape string replies.** Mutations return a 9-element array of strings
  decoded in Go (`store/redis/scripts.go:63-109`); ints travel as strings so
  int64 fidelity survives Lua's 53-bit doubles.
- **Cluster-single-slot by construction.** Every key and pub/sub channel for a
  stream shares the `{path}` hash tag; every subscription-control key shares
  `{__ds}`. Scripts therefore touch exactly one slot and are legal in cluster
  mode; cross-slot fork bookkeeping is deliberately decomposed into separate
  single-slot scripts reconciled from Go (`store/redis/scripts/common.lua:1-3`,
  `webhook/scripts/common.lua:4-6`).
- **Bounded Lua stack.** Frame writes are chunked into ≤1000-arg `ZADD` batches
  because `unpack` is C-stack bounded (`append.lua:96-107`).
- **Effects-replicated, atomic.** Because the engine is single-threaded and a
  script blocks it for its duration, either all effects land or none are
  visible — there is no partial-append state. Replication is by effects, so
  replicas and AOF see the same writes a transaction would produce.

## The prior decision this research re-opens

Two documents already record a deliberate choice to **avoid Redis Functions**:

- `docs/research/05-redis-design.md` §5 ("Designing for Walmart-managed Redis"),
  row *Restricted Lua?*: "`EVAL`/`EVALSHA`/`SCRIPT LOAD` are the most widely
  permitted programmability surface; **Redis Functions (`FUNCTION LOAD`) are
  avoided entirely** — more often ACL-blocked, and effects replication gives
  plain scripts the same replication safety." It declares the only true hard
  requirements to be **EVAL and pub/sub**.
- `docs/PLAN.md` §4: "Redis Functions were considered (nicer packaging) but
  `FUNCTION LOAD` is more often restricted on managed Redis than
  `EVALSHA`/`SCRIPT LOAD`; chronicle uses classic scripts with automatic
  NOSCRIPT reload."

So the original rationale rests on two load-bearing claims — (1) **portability**:
`FUNCTION LOAD` is blocked on more managed platforms than `EVAL`; and (2)
**parity**: effects replication makes plain scripts "equally safe," so Functions
buy nothing that matters. The new context — Redis 8 in a managed *enterprise*
offering — is precisely what could undercut claim (1) for chronicle's actual
deployment target. Whether it should is the subject of
[03](03-managed-platform-support.md) and [05](05-recommendation.md).

## The one genuine wart

The status quo's weak point is **not** correctness or performance — it is the
prelude-concatenation pattern. It is a hand-rolled substitute for a shared-library
system Redis does not give plain scripts:

- **N copies in the script cache.** The store prelude exists in the cache 6×, the
  webhook prelude 14×.
- **SHA churn on shared edits.** Touching `common.lua` changes *every* dependent
  script's SHA, so the next call to each re-incurs the `NOSCRIPT → SCRIPT LOAD`
  round trip across the fleet.
- **No introspection or named addressing.** Scripts are anonymous SHAs; there is
  no `FUNCTION LIST`/`STATS` equivalent, and versioning is implicit.

This is exactly the pain Redis Functions are designed to remove — which is why
the comparison is worth making carefully rather than dismissing on the prior
decision. [02](02-redis-functions.md) takes that on.
