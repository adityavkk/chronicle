# Deploying chronicle

Chronicle is a single static binary plus a Redis instance. This page covers
what the Redis deployment must provide and what guarantees you get back.

## Redis requirements

| Requirement | Why | If unavailable |
| --- | --- | --- |
| Redis ≥ 6.0 (managed Redis 8 recommended) | `EVALSHA`, pub/sub, `ZRANGEBYLEX`, and key-level `PEXPIRE` / `PERSIST` | chronicle cannot run without these commands |
| `EVAL`/`EVALSHA` permitted | every mutation is one atomic Lua script | chronicle cannot run — this is a hard requirement |
| Pub/sub permitted | long-poll/SSE wakeups | hard requirement (waiters would degrade to pure polling) |
| `maxmemory-policy noeviction` | eviction silently truncates stream data | chronicle warns at startup when it can read the config; reads detect missing data and fail loudly rather than serve corrupt streams |
| AOF (`appendonly yes`, `everysec`) recommended | crash durability of acked appends | RDB-only widens the data-loss window on crash |

### Managed Redis (e.g. Walmart-managed)

- `CONFIG GET/SET` is often denied. A permission or unsupported-command error
  on the append-ceiling probe is an explicit operator-enforcement warning.
  Transport, timeout, malformed, and missing-value failures are fatal because
  silently accepting an indeterminate ceiling would hide a broken startup.
- Lowered `proto-max-bulk-len` caps the largest single frame chronicle can
  store. Set `--max-append-bytes` (or `CHRONICLE_MAX_APPEND_BYTES`) when the
  deployment needs a finite request and oversized-first-frame bound. The
  configured body size plus the 34-byte stored-frame prefix must fit
  `proto-max-bulk-len`. Chronicle validates the relationship when `CONFIG GET`
  is permitted and otherwise logs that the operator must enforce it. Bodies
  over the configured ceiling return `413 Payload Too Large` before store
  access. Zero retains the compatible unlimited HTTP policy.
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
