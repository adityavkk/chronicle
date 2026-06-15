# 06 — Adversarial review + corrections

A skeptic's pass over docs 01–04: which claims survive, which were wrong or
overstated, and the deeper Electric finding. The corrections here are folded into
[05-proposed-architecture.md](05-proposed-architecture.md); this doc records *why*.

## Critique of the chronicle options (04)

The structural diagnosis holds — single `{__ds}` slot, `O(N·K)` sweep, lease/retry
de-duped by `claim_due.lua` but the sweep not, the fence as the correctness
mechanism (all verified against `keys.go`, `manager.go`, `webhook/scripts/*.lua`).
But three options leaned on claims that don't survive scrutiny:

**O1 — fence-safety was conflated with liveness.** "The fence already makes
mis-sharding harmless" is true only for *safety*: a double-sweep gets `BUSY` at
`arm_wake.lua:13-15`, a stale ack gets `FENCED`. It says nothing about a **coverage
gap**: if `hash(subId) % N` is briefly inconsistent during a pod-kill/rebalance, a
slice can be owned by *nobody* and is not swept until membership re-converges. The
fence is a safety mechanism with **zero liveness guarantee**. (04's own line 142 quietly
admits the handoff safety is unproven.) **Fix:** work-sharding must be an optimization
over a still-correct full-sweep baseline — never a correctness dependency.

**O2 — understated, and partly self-defeating.** Today the fan-out index
`ds:{__ds}:stream:<path>` is co-slotted with every sub, so `OnStreamAppend` is one
`SMEMBERS` and the subsequent `arm_wake` touches sub + lease ZSET in one slot. Naively
"shard the `{__ds}` state across slots" breaks **two** things, not one: (a) the
lease/retry ZSETs are single keys feeding `claim_due.lua` — split them or they *are*
the surviving ceiling, defeating the premise; (b) `OnStreamAppend` must scatter-gather
across slots. And `ack.lua` (4 keys) / `delete_sub.lua` (5 keys) are **multi-key atomic
Lua** — splitting a sub across slots breaks their atomicity (`CROSSSLOT`). **Fix
([05]):** slot-home *whole* subscriptions (every key for a sub shares one `{__ds:h}`
tag) so the scripts stay byte-for-byte single-slot, and shard the schedule *with* the
subs. The cross-slot cost then collapses to `OnStreamAppend` doing `S` *parallel*
`SMEMBERS` — a real, measurable regression, owned not hand-waved.

**O3 — relocates the dual-write it claims to remove.** "Arm on append, clear on ack" is
structurally identical to the existing `arm_wake → AppendWakeEvent → RecordWakeEventSent`
3-step write the sweep already reconciles (`manager.go`). It **adds** a `ZADD`/`ZREM` to
the append/ack hot path and still needs the sweep as reconciler — because **only the
sweep re-derives owed work from durable cursors** after a failover that loses a
webhook sub's lease-ZSET tail. **Fix:** keep the outbox (the recovery-latency win is
real — fire before the next sweep tick), but call it what it is: the sweep is *demoted*,
not deleted, and the write amplification must be measured.

**O4/O5 — honestly hedged**, so little to refute, except O4 inherits all of O1's
membership risk plus O2's cross-slot problem, so "O1 + the fast path" understates it.

### What holds (verified, load-bearing)

Single-slot capacity ceiling; `O(K)`/`O(N·K)` sweep; `claim_due.lua` de-dup vs the
un-deduped sweep; the fence (not the lease) as correctness; the append cannot atomically
arm a cross-slot wake (so the sweep is a *necessary* backstop); "don't split the binary
yet" (the one binary held at K=10k).

## Verification of the external claims (01–03)

All six load-bearing claim families **hold** against primary sources; only minor wording
nits:

- **Orleans `RedisGrainDirectory`** — confirmed: `IGrainDirectory`, `GrainAddress` JSON
  under a `ClusterId+GrainId` key, Lua CAS on `ActivationId`, survives cluster restart.
  Per-entity placement with a Redis-backed map: real.
- **Redis `WAIT`/`WAITAOF`** — confirmed *not* strong consistency (Redis docs say so
  verbatim); `WAITAOF` (7.2+) adds local/replica AOF fsync, client must check the
  returned pair, can't run on a replica. In Redis Software HA `numreplicas` is always 1,
  so `WAIT 1` is the realistic per-shard ceiling.
- **Redis Enterprise Active-Active** — CRDT-based, strong-*eventual*, **not**
  linearizable; only CRDT-mapped ops converge; registers (strings) are LWW.
- **Cloudflare DO alarms** — one pending per-object timer, at-least-once with
  exponential backoff (2 s initial, ≤6 retries); ~1,000 req/s soft cap; global read
  replicas are **D1-only**, plain DOs single-region. (10 GB/object confirmed at GA.)
- **Kafka KIP-848** — GA in 4.0, broker-driven *incremental* rebalance. Nit: doc-03's
  "cooperative" conflates KIP-848 with the older incremental-cooperative assignors
  (KIP-429); the no-stop-the-world claim is right, the label is loose.
- **Electric agents-server Postgres schema** — confirmed file-by-file (see below).

## The Electric finding (corrects 01)

Doc 01 was broadly right but **conflated three layers**. Subscriptions/wake exist at:

1. **Streams-protocol wake** (a live reader catching appends): an **in-memory waiter
   map** in the OSS reference servers — and critically, **even the persistent FileStore
   keeps the wake registry in-process** (bbolt/LMDB + segment files persist the *data*,
   not the *subscriptions*). *Data persistence ≠ subscription persistence.* This is the
   Caddy-plugin layer the user correctly identified as not powering managed agents.
2. **The hosted streams store** (Electric Cloud): its subscription/wake internals are
   **not publicly disclosed**. The only public facts are "Sync CDN serves all reads,"
   1M conns/stream, 240K writes/s. The 1M figure is attributed to **CDN
   request-collapsing** (coalescing concurrent long-polls at the edge into one upstream
   read) — so the managed store likely does *not* hold 1M sockets itself. The store
   stays a black box; do **not** claim it uses Postgres claim-rows.
3. **The agents control plane** (`packages/agents-server`, open source): this *is* where
   the user's intuition pays off. Migration `0005_pull_wake_control_plane.sql` (the name
   is the tell) creates `entity_dispatch_state`, `wake_notifications`, `consumer_claims`,
   `runners`; claims are `materializeActiveClaim` via `onConflictDoUpdate` + epoch
   fencing + `lease_expires_at`/`last_heartbeat_at`; wake delivery appends a `WakeEvent`
   to the entity's stream then a `DurableStreamsRouter` fans out to the registered
   webhook. `SELECT … FOR UPDATE SKIP LOCKED` is used **only** for the tag-stream
   outbox, not the entity claim.

The relevant transferable shape (Electric 1.1 storage engine): **reader/writer-decoupled,
chunked immutable log files + a sparse offset index, local-disk today with
object-storage planned, request-collapsing at the CDN keyed by offset.** That read-scale
pattern — front the read path with an offset-keyed cache that collapses concurrent
readers — is the most directly portable idea for chronicle's serving tier.

## Tunable consistency, corrected (informs 04/05)

The biggest trap is calling any Redis tier "linearizable." Redis is **AP**;
`WAIT`/`WAITAOF` are durability, not consistency. So:

- **The real strong guarantee is the monotonic generation fence** enforced by
  compare-and-set + reject-if-stale at the resource (Kleppmann's fencing-token result —
  Redlock and plain `SETNX` do *not* provide it). chronicle already has this; the fence,
  **not the lease TTL and not `WAIT`,** is the safety boundary. This also immunizes
  correctness against cross-region clock skew (skew corrupts TTL/LWW, never a monotonic
  generation).
- **Active-Active CRDB is business continuity, not tight-RPO DR.** On region loss,
  un-reconciled writes are lost and the live risk is *consistency divergence*. A CRDB
  **LWW register cannot hold a correctness-critical single-holder fence** — two regions
  setting it concurrently silently lose one. Keep the fence single-writer per
  shard/region.
- **`WAIT`-on-writer ≠ fresh reader.** Read-your-writes on a replica needs a freshness
  token — the **D1 Sessions API bookmark** (a strictly-monotonic Lamport timestamp the
  read carries until the replica has applied it; `first-unconstrained` vs
  `first-primary`) or a Cosmos session token. Azure Cosmos DB's 5-level knob
  (strong/bounded-staleness/session/consistent-prefix/eventual) is the model to mirror —
  minus "strong," which Redis can't offer.
- **Verify the managed Redis 8 SKU.** A plain managed offering likely exposes only
  single-primary + async replica + `WAIT`/`WAITAOF` — *not* Enterprise Active-Active. So
  the DR floor is active-passive Replica-Of with lag-bounded RPO; design for that.

## Net

The research's *direction* is sound and the prior art checks out; the *chronicle options*
needed sharpening on three points (liveness, atomicity, the dual-write), the Electric
picture needed the three-layer correction, and "tunable consistency" needed reframing as
durability + freshness. [05] is the corrected proposal.
