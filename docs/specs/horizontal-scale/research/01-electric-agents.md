# 01 — Electric Agents: what it actually does

**Hypothesis tested:** Electric Agents' managed Durable Streams runs subscription /
wake management on Cloudflare Durable Objects (one DO per agent/entity).

**Verdict: not supported.** Verified against the open-source repo and Electric's own
docs/blog. Electric Agents uses a **PostgreSQL control plane** plus a **separate,
closed-source streams store fronted by a Cloudflare CDN**. Durable Objects appear
only as a *sync target* and as a *contrast* in Electric's marketing — not as the
runtime.

## Two layers

Electric separates **transport** (the streams) from **control** (the entities).

**Control plane** — open source, `electric-sql/electric`,
`packages/agents-server` + `agents-runtime`. Each agent is an addressable *entity*
with a stable URL (e.g. `/coder/landing-page-build`) backed by its own durable
stream + inbox. `ElectricAgentsTenantRuntime` wires an `EntityManager`, a
`PostgresRegistry`, and a `WakeRegistry`. A `process-wake` loop drives lifecycle:
the `WakeRegistry` evaluates wake conditions and the `EntityManager` appends a
`WakeMessage` onto the subscriber entity's stream. Coordination is **per-entity rows
in Postgres**, not actor placement:

- `entity_dispatch_state` — `activeConsumerId`, `activeRunnerId`, `activeEpoch`,
  `activeLeaseExpiresAt`, `outstandingWakeId`/`lastWakeId`.
- `consumer_claims` — `consumerId`, `epoch`, `leaseExpiresAt`,
  `status` (active/released/expired/failed), `runnerId`, `ackedStreams`,
  `claimedAt`, `lastHeartbeatAt`.
- `wake_registrations` — `subscriberUrl`, `sourceUrl`, `condition` (JSONB),
  `debounceMs`, `timeoutMs`, `oneShot`.

A consumer (any stateless server/runner) claims an entity via `materializeActiveClaim`
(writes a claim row + dispatch state with a `leaseExpiresAt`), heartbeats via
`materializeHeartbeatClaim` (bumps `lastHeartbeatAt`; **does not** extend the lease
unless `leaseExpiresAt` is explicitly passed), and releases via
`materializeReleasedClaim`. **Epoch fencing** rejects a deposed consumer's stale
writes — the same shape as chronicle's `generation`/`wake_id` fence, but keyed
per-entity rather than per-`{__ds}`-slot.

**Storage plane** — the hosted "Electric Streams" is a *closed-source* managed
implementation of the open Durable Streams protocol (URL-addressable append-only byte
sequences; PUT/POST/GET; opaque, lexicographically-sortable offset tokens;
`Stream-Next-Offset`/`Stream-Cursor`/`Stream-Up-To-Date` resume headers). The SDK runs
in *your* process (Express/Next/Hono webhook handlers holding tools+secrets); the
managed control plane handles lifecycle/routing/persistence.

## How it scales

- **Reads** fan out through **Electric Cloud's Sync CDN (Cloudflare CDN)** — the
  hosted-streams launch claims testing to **1M concurrent connections per stream** and
  **240K writes/sec** for small messages. Reads are served from the edge cache keyed by
  offset; resumability rides ordinary HTTP caching. Reads never hit origin.
- **Control plane** scales as **many stateless runners racing on Postgres claim rows**
  with epoch fencing + lease expiry — no central coordinator, no per-stream actor
  placement.
- **Execution scales to zero**: durability lives in the stream + Postgres, so an idle
  agent costs nothing; it wakes on message delivery, replays its stream from the last
  offset to recover, then sleeps ("a thousand agents, pay for the ones thinking").

## Where Durable Objects actually appear

1. **A sync *target*.** Electric documents syncing shapes *into* Workers/DOs behind
   the Cloudflare CDN — the only first-party Electric+DO relationship.
2. **A contrast.** Electric's blog cites Cloudflare's "an agent *is* a Durable Object"
   as the proprietary, single-vendor pattern it deliberately reimplements in an open,
   infra-portable way ("agents are webhook handlers" on your own stack).

## What chronicle can take from this

- **Per-entity claim rows instead of one hot key.** Replace global serialization in
  the single `{__ds}` slot with one ownership record per subscription/stream carrying
  `(activeConsumer, epoch, leaseExpiresAt, lastHeartbeatAt)`. Many stateless workers
  then race safely on independent records. chronicle would implement this on **Redis 8**
  (per-key Lua CAS + TTL leases + per-stream registration sets), not Postgres — the
  *pattern*, not a drop-in.
- **Decouple wake *registration* from *dispatch*** (a `wake_registrations`-style record
  with condition/debounce/oneShot).
- **Front reads with an offset-keyed HTTP/CDN cache** so the read path scales
  independently of the control plane.

## The catch

- Electric's substrate is Postgres; chronicle's constraint is managed Redis 8 +
  cloud-agnostic object storage (no AWS). Adopt the claim/lease *pattern* on Redis,
  not the Postgres schema.
- The hosted streams store is **closed-source** — only the open protocol and the OSS
  Postgres control-plane schema are inspectable. We **cannot** rule out that the
  proprietary store uses DOs internally, but no source claims it, and the listed
  reference backends (in-memory, log-file+LMDB, Postgres/SQLite, **S3**) plus
  "Sync CDN serves reads" strongly imply object-storage + CDN, not DOs.
- Multi-region DR / RTO / RPO for the managed control plane is **undocumented**.

## Sources

- `electric-sql/electric` — `packages/agents-server/src/db/schema.ts` (the claim/dispatch/wake schema), `runtime.ts`, `entity-manager.ts`, `wake-registry.ts`
- DeepWiki queries confirming **no** DurableObject/Workers/Agents-SDK in agents-server/agents-runtime, and the Postgres control plane
- durablestreams.com — *Building a Server* (reference backends: in-memory, file+LMDB, Postgres/SQLite, S3 — **no DOs**)
- electric.ax blog — *Announcing Hosted Durable Streams* (Sync CDN, 1M conns/stream, 240K writes/s), *Introducing Electric Agents* ("the agent is the durable stream"), *Serverless agents*
- electric.ax docs — *Cloudflare integration* (DOs as a sync target)

**Verified:** the Postgres control plane and the *absence* of DOs in the OSS runtime
(repo schema + grep). **Unconfirmed:** internals of the closed hosted store; managed
control-plane DR.
