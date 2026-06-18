# Deploying chronicle

Chronicle is a single static binary plus a Redis instance. This page covers
what the Redis deployment must provide and what guarantees you get back.

## Redis requirements

| Requirement | Why | If unavailable |
| --- | --- | --- |
| Redis ≥ 7.4 (8.x recommended) | `HEXPIRE` for producer-state aging; otherwise standard commands | producer state never ages out (bounded by stream lifetime — acceptable) |
| `EVAL`/`EVALSHA` permitted | every mutation is one atomic Lua script | chronicle cannot run — this is a hard requirement |
| Pub/sub permitted | long-poll/SSE wakeups | hard requirement (waiters would degrade to pure polling) |
| `maxmemory-policy noeviction` | eviction silently truncates stream data | chronicle warns at startup when it can read the config; reads detect missing data and fail loudly rather than serve corrupt streams |
| AOF (`appendonly yes`, `everysec`) recommended | crash durability of acked appends | RDB-only widens the data-loss window on crash |

### Managed Redis (e.g. Walmart-managed)

- `CONFIG GET/SET` is often denied: chronicle treats config checks as
  best-effort and never requires them at runtime.
- Lowered `proto-max-bulk-len` caps the largest single append chronicle can
  store (each append is one ZSET member). The protocol allows rejecting
  oversized appends with `413 Payload Too Large`.
- Cluster mode: every key for a stream carries a `{path}` hash tag, so each
  stream lives in exactly one slot and Lua scripts stay cluster-legal. Fork
  creation and cascade deletion touch two streams (two slots) and execute as
  two single-slot steps; the in-between window is reconciled via the fork
  registry set.

## Durability and consistency guarantees

Within a healthy Redis primary:

- Appends are atomic and strictly ordered per stream; validation (closure,
  content type, `Stream-Seq`, producer epoch/seq) commits in the same script
  as the write — there is no crash window between producer-state update and
  data append.
- Read-your-writes holds: a `GET` issued after an append's response sees the
  data.

Across failover:

- Redis replication is **asynchronous**. A failover can lose the last moments
  of acknowledged writes. Producers using idempotent headers recover exactness
  by retrying into the new primary (the producer state machine de-duplicates);
  plain producers get at-least-once across failover.
- For tighter windows, run chronicle with `WAIT`-on-append enabled (opt-in
  flag; adds replica round-trip latency to every append). This narrows but
  does not eliminate the window — see PLAN.md §4.7.

## Sizing

A stream's full history lives in one sorted set on one shard: plan node memory
for your largest streams (same operational envelope as the reference
implementation's memory store). Use TTLs (`Stream-TTL`) or absolute expiry
(`Stream-Expires-At`) on ephemeral streams — expired streams are reaped lazily
on access and by backstop key TTLs.

## Fronting chronicle

The protocol is designed for CDNs/proxies (cursor-based collapsing, ETags,
`Cache-Control` on historical reads). When proxying:

- Disable response buffering for SSE (`X-Accel-Buffering: no` is set by
  chronicle; honor it or configure the proxy equivalent).
- Don't cache `204` long-poll responses.
- Pass `X-Forwarded-Proto`/`X-Forwarded-Host` so `Location` headers on stream
  creation are correct.
- TLS termination is the proxy's job; chronicle speaks plain HTTP.
