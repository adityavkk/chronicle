# 03 — Managed-platform support: `EVAL` vs `FUNCTION`

This is the load-bearing fact for the whole decision. Chronicle's original
rejection of Redis Functions rests on one empirical claim: *"`FUNCTION LOAD` is
blocked on more managed platforms than `EVALSHA`/`SCRIPT LOAD`."*
([01](01-current-approach.md)). So: is that still true in 2026, and is it true on
the platform chronicle actually targets?

The facts below were verified directly against vendor docs (URLs inline). Where a
cell says *unconfirmed*, the vendor doc did not state it unambiguously and it is
flagged for the recommendation rather than asserted.

## The matrix

| Platform | Redis version | `EVAL`/`EVALSHA`/`SCRIPT LOAD` | `FUNCTION LOAD` / `FCALL` | Source |
|---|---|---|---|---|
| **AWS ElastiCache — Serverless** | Valkey/Redis OSS 7.x | ✅ supported | ❌ **blocked** (`function`, `function load`, `fcall`, `fcall_ro` in the serverless restricted list; also `script kill`, `wait`) | [docs.aws.amazon.com](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/SupportedCommands.html) |
| **AWS ElastiCache — node-based** | Redis OSS / Valkey 7.x | ✅ supported | ⚠️ *likely supported* — `FUNCTION` is **not** in the "unavailable for Redis OSS caches" list; only the *serverless* addendum blocks it. Confirm on your version. | same |
| **AWS MemoryDB** | Redis 7.x / Valkey | ✅ supported | ⚠️ *unconfirmed* in vendor docs (7.0 added; verify `FCALL` exposure) | [aws.amazon.com](https://aws.amazon.com/about-aws/whats-new/2023/05/amazon-memorydb-support-redis-7/) |
| **Azure Managed Redis** (Enterprise-based) | 7.4+ | ✅ supported | ✅ **supported** — default access is `+@all ~*`; blocked list is only some `CLUSTER` subcommands + `FLUSHALL/FLUSHDB` (geo). `FUNCTION`/`FCALL` not blocked. | [learn.microsoft.com](https://learn.microsoft.com/en-us/azure/redis/best-practices-client-libraries) |
| **Azure Cache for Redis** (Basic/Std/Premium) | historically 6.0 | ✅ supported | ❌ **absent** when on 6.x (Functions need 7.0+); version-dependent | vendor version pages |
| **GCP Memorystore for Redis** | 7.2-era | ✅ supported | ✅ **supported** (`FUNCTION LOAD/…`, `FCALL`, `FCALL_RO` all listed) | [docs.cloud.google.com](https://docs.cloud.google.com/memorystore/docs/redis/supported-commands) |
| **GCP Memorystore for Redis Cluster** | 7.2-era | ✅ supported | ✅ **supported** | [docs.cloud.google.com](https://docs.cloud.google.com/memorystore/docs/cluster/supported-commands) |
| **Redis Cloud / Redis Enterprise Software** | up to 8.x | ✅ supported | ✅ supported (Functions are a first-class Redis feature; Enterprise exposes them) | redis.io |
| **Valkey** (fork) | 7.2+/8.x | ✅ supported | ✅ supported (Functions forked in at 7.2) | valkey.io |

Two refinements from the cross-checked workflow research, both decision-relevant:

- **AWS MemoryDB Multi-Region (active-active)** lists `FUNCTION`, `FCALL`,
  `FCALL_RO` as **unsupported** (single-region MemoryDB added Functions with
  Redis 7). So *two* AWS managed tiers block Functions while permitting `EVAL`.
- **Redis Enterprise / Redis Cloud** — chronicle's likely "managed enterprise
  Redis 8" — supports `FCALL`/`FUNCTION LOAD` on **both Standard and
  Active-Active** databases (only `SCRIPT DEBUG` is unsupported). So the
  historical objection genuinely **does not apply** to that specific target.
- **Sharded OCI Cache** blocks `EVAL` *and* `FUNCTION` together — i.e. Functions
  are *never strictly more* portable than `EVAL`, and sometimes strictly less.

## What the matrix actually says

1. **The portability concern is real but *narrow and shrinking*.** In 2026 the
   one big platform that flatly blocks `FUNCTION` while allowing `EVAL` is **AWS
   ElastiCache Serverless** — and notably, it blocks it *as a managed-experience
   restriction*, not because of the engine version. Old **Azure Cache Basic/
   Standard/Premium** lack Functions only because they predate Redis 7.0. Every
   other current managed target — GCP Memorystore (both flavors), Azure Managed
   Redis, Redis Cloud/Enterprise, Valkey, and node-based ElastiCache — exposes
   the full `FUNCTION`/`FCALL` family.

2. **`EVAL`/`EVALSHA`/`SCRIPT LOAD` is still the universal floor.** Every
   platform in the matrix supports it. There is no managed Redis that blocks
   `EVAL` but allows `FUNCTION`. So **Lua-via-`EVAL` is a strict superset of
   deployment targets** — anything that runs Functions also runs `EVAL`, but not
   vice-versa. This is the single most important asymmetry for chronicle's
   "runs on any managed Redis" pitch.

3. **One managed nuance cuts the other way:** ElastiCache Serverless documents
   `SCRIPT FLUSH` as *"Currently a no-op, script cache is managed by the
   service"* — i.e. the provider can manage/repopulate the script cache, which
   softens (but does not remove) the volatile-cache argument *on that platform*.
   It does not generalize.

## What it means for chronicle

The original claim holds, with a sharper edge than the prose in
`docs/research/05-redis-design.md`:

- If chronicle must run on **the broadest possible managed Redis** — including
  ElastiCache Serverless and legacy Azure tiers — then **`EVAL` is mandatory and
  `FUNCTION` cannot be a hard dependency.** Functions would *remove* deployment
  targets.
- If chronicle's **actual** target is a **managed enterprise Redis 8 offering**
  (Redis Enterprise / Redis Cloud-class, or Memorystore/Azure Managed Redis),
  then **Functions are available** and the portability objection does not apply
  *to that deployment*.

That split is the entire decision, and it points at a **conditional** answer
rather than a flat one: keep `EVAL` as the portable floor, and treat Functions
as an opt-in that's only worth taking where the target guarantees them. The
[recommendation](05-recommendation.md) makes that precise. The open action it
depends on: **confirm whether the specific managed enterprise Redis 8 plan
permits `FUNCTION LOAD`** (run `ACL GETUSER` / attempt a trivial `FUNCTION LOAD`
at startup) — it almost certainly does, but it is the one fact worth checking
before betting on it.
