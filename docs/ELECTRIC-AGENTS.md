# Running ElectricSQL Agents on chronicle

This is a tested, copy-paste runbook for using **chronicle** as the Durable
Streams backend for **ElectricSQL's agents runtime** (`@electric-ax/agents-*`),
instead of the bundled reference Durable Streams server.

Validated end-to-end on 2026-06-13 with the `agents-chat-starter` example:
entity spawn → webhook subscription created on chronicle → append-triggered
signed-webhook wake → agent runs and reads its inbox from chronicle. The only
piece that needs your own secret is an `ANTHROPIC_API_KEY` for the LLM call.

> **TL;DR.** chronicle is a drop-in backend. Point the agents-server at it with
> `ELECTRIC_AGENTS_DURABLE_STREAMS_URL=http://localhost:4437/v1/stream`, run
> chronicle with **`--webhook-allow-private`** (so it can deliver webhooks to
> `localhost`), and run an **Electric** sync service alongside Postgres. Miss
> either of those last two and entities spawn but never wake — silently.

---

## Why this works (read this first)

Electric's agents runtime **requires** the Durable Streams backend to implement
the §6–7 *Reserved Subscription APIs* (`/__ds/subscriptions/*`, webhook +
pull-wake). This is not optional in practice:

- The agents-server stores wake *conditions* itself (Postgres + an Electric
  shape) and writes a wake *event* into an entity's stream via a plain append.
- But **writing a wake event does not run anything.** The runtime is woken only
  by (a) a signed webhook POST to its serve endpoint, or (b) a pull-wake
  `wake_stream` it tails — and **the backend produces both.** On every spawn the
  agents-server calls `linkEntityDispatchSubscription`, which creates a
  subscription on the backend (`StreamClient.putSubscription` →
  `PUT {backend}/__ds/subscriptions/:id`).

So a subscription-less Durable Streams server is a hard gap — agents would never
run. chronicle is fine because it **fully implements §6–7** (`subscriptions.go`
+ the `webhook/` package: webhook + pull-wake, claim/ack/release, Ed25519/JWKS,
Redis-backed durable cursor/lease), enabled by default. See
[CADDY-PARITY.md](CADDY-PARITY.md) for the parity map.

> ⚠️ **Stale docs warning.** Older copies of chronicle's own README and some
> handler doc-comments say "subscriptions are deferred / 501". That is wrong for
> the current code — subscriptions are implemented and the conformance suite runs
> with `subscriptions: true`. Trust the code, not that prose.

---

## The components

Five processes. The agents-server is the front door; chronicle is its storage +
wake engine; Electric and Postgres are the agents-server's own state plane.

```
            ┌─────────────────────┐
 you ─curl─▶│  example / runtime  │  (agents-chat-starter)  :4700
            │  registers types,   │◀── signed webhook (wake) ──┐
            │  runs the agent     │                            │
            └──────────┬──────────┘                            │
            spawn/send │ HTTP                                   │
                       ▼                                        │
            ┌─────────────────────┐   plain stream ops    ┌─────┴──────────┐
            │   agents-server     │──────────────────────▶│   chronicle    │ :4437
            │  (entity runtime)   │  + PUT __ds/subscript.│  (Redis-backed │
            │       :4500         │◀──────────────────────│   Durable      │
            └───┬───────────┬─────┘   subscription mgmt   │   Streams)     │
       state    │           │ shape                       └───────┬────────┘
       (drizzle)│           │ sync                                │
                ▼           ▼                                     ▼
          ┌──────────┐  ┌──────────┐                        ┌──────────┐
          │ Postgres │  │ Electric │                        │  Redis   │
          │  :5432   │◀─│  :3100   │                        │  :6379   │
          └──────────┘  └──────────┘                        └──────────┘
```

The wake path (steady state), which is what proves the integration:

```mermaid
sequenceDiagram
    participant U as you (curl)
    participant AS as agents-server :4500
    participant CH as chronicle :4437
    participant RT as runtime :4700
    U->>AS: POST /_electric/shared-state/:room (a message)
    AS->>CH: append to shared-state stream (plain)
    AS->>AS: evaluate wake conditions (observers)
    AS->>CH: append wake event to /type/id/main (plain)
    CH->>RT: signed webhook POST (subscription fires)
    RT->>CH: GET /type/id/main (read inbox)
    RT->>RT: run agent (LLM call)
```

---

## Prerequisites

| Need | Version used | Notes |
| --- | --- | --- |
| Go | 1.26 | to build chronicle |
| Node | 24.x | to build/run the agents packages |
| pnpm | 10.12.1 | `packageManager` pinned in the electric repo |
| Docker | running | Redis + Postgres + Electric |
| `ANTHROPIC_API_KEY` | — | only for real LLM responses; the plumbing works without it |

Repos assumed at `~/dev/chronicle` and `~/dev/electric`.

---

## Run recipe

Run each step in its own terminal (or background them). Order matters only in
that chronicle needs Redis, and Electric needs Postgres.

### 1. Redis + chronicle (the backend)

```bash
cd ~/dev/chronicle
make redis-up                          # Redis 8 on :6379 (docker compose)
go build -o bin/chronicle ./cmd/chronicle

./bin/chronicle \
  --listen :4437 \
  --redis-url redis://localhost:6379 \
  --subscriptions \                    # default-on, but be explicit
  --webhook-allow-private \            # REQUIRED for localhost webhook delivery
  --public-url http://localhost:4437 \ # correct callback_url / jwks_url
  --log-level debug
```

Startup log must say `subscriptions enabled` and `subscriptions=true`.
Smoke test: `curl -s http://localhost:4437/v1/stream/__ds/jwks.json` returns an
Ed25519 key.

This local recipe uses Chronicle's default insecure telemetry mode. In WCNP,
use the agents-server SPIFFE identity, set `CHRONICLE_AUTH_MODE=enforce`, and
mount an explicit service policy. Keep `CHRONICLE_SERVICE_BEARER` only as a
compatibility fallback for a client that cannot use mesh identity. The complete
marker, strict-mTLS, sidecar-bypass, policy, Akeyless, and rotation requirements
are in [DEPLOYMENT.md](DEPLOYMENT.md#service-identity-and-access-policy).

### 2. Postgres (agents-server state plane)

```bash
docker run -d --name electric-agents-pg \
  -e POSTGRES_DB=electric_agents \
  -e POSTGRES_USER=electric_agents \
  -e POSTGRES_PASSWORD=electric_agents \
  -p 5432:5432 postgres:18-alpine \
  -c wal_level=logical -c max_connections=300
```

`wal_level=logical` is required because Electric replicates from it.

### 3. Electric (sync service)

```bash
docker run -d --name electric-sync \
  -e DATABASE_URL="postgresql://electric_agents:electric_agents@host.docker.internal:5432/electric_agents?sslmode=disable" \
  -e ELECTRIC_INSECURE=true \
  -p 3100:3000 \                       # 3000 is often taken (Grafana etc.) → use 3100
  electricsql/electric:latest

curl -s http://localhost:3100/v1/health   # {"status":"active"}
```

### 4. agents-server (from source, pointed at chronicle)

```bash
cd ~/dev/electric
pnpm install
pnpm --filter "@electric-ax/agents-server..." --filter "@electric-ax/agents..." build

cd packages/agents-server
ELECTRIC_AGENTS_DURABLE_STREAMS_URL=http://localhost:4437/v1/stream \
DATABASE_URL=postgresql://electric_agents:electric_agents@localhost:5432/electric_agents \
ELECTRIC_AGENTS_ELECTRIC_URL=http://localhost:3100 \
ELECTRIC_AGENTS_PORT=4500 \
ELECTRIC_AGENTS_HOST=127.0.0.1 \
ELECTRIC_AGENTS_BASE_URL=http://localhost:4500 \
node dist/entrypoint.js
```

It auto-runs DB migrations on boot. The boot log should print
`Durable Streams: http://localhost:4437/v1/stream` and `Electric: http://localhost:3100`.
Setting `ELECTRIC_AGENTS_DURABLE_STREAMS_URL` makes it skip the embedded
reference DS server (`entrypoint-lib.ts` `createEmbeddedStreamsServer`).

### 5. A sample runtime (registers agent types + serves the webhook)

```bash
cd ~/dev/electric/examples/agents-chat-starter
AGENTS_URL=http://localhost:4500 PORT=4700 SERVE_URL=http://localhost:4700 \
npx tsx src/server/index.ts
```

Log should show `3 entity types ready: socrates, camus, simone`. This registers
each type with a default `webhook` dispatch policy pointing at
`http://localhost:4700/webhook`.

### 6. Drive it

> ⚠️ The chat-starter's built-in spawn helper posts to `PUT /:type/:id`, which on
> the current agents-server just creates a raw stream and **does not create an
> entity**. Use the real entity endpoint instead (see Gotchas).

```bash
# Spawn an entity (creates the backend subscription on chronicle)
curl -s -X PUT http://localhost:4500/_electric/entities/camus/demo1 \
  -H 'Content-Type: application/json' \
  -d '{"args":{"chatroomId":"room-demo"},"tags":{"room_id":"room-demo"},
       "initialMessage":"You joined. Wait for messages."}'

# Send a message the agent observes (wakes it via chronicle's webhook)
KEY=$(uuidgen)
curl -s -X POST http://localhost:4500/_electric/shared-state/room-demo \
  -H 'Content-Type: application/json' \
  -d "{\"type\":\"shared:message\",\"key\":\"$KEY\",
       \"headers\":{\"operation\":\"insert\"},
       \"value\":{\"key\":\"$KEY\",\"role\":\"user\",\"sender\":\"user\",
                  \"senderName\":\"You\",\"text\":\"Camus, is the absurd liberating?\",
                  \"timestamp\":0}}"
```

Without an `ANTHROPIC_API_KEY` in the **runtime's** environment (step 5), the
agent wakes and runs up to the LLM call, then logs
`No API key for provider: anthropic`. Everything before that is chronicle
working. Add the key to `examples/agents-chat-starter/.env` and restart step 5
to get real replies.

---

## Turning the write fence on

chronicle's write-fencing extension
([docs/spec/WRITE-FENCING.md](spec/WRITE-FENCING.md)) makes activation-output
streams accept a runtime's writes only under the current claim's write token.
With fencing off (no `Write-Fence` header anywhere), nothing above changes —
the agents-server never sends the extension headers and the additive
`write_token` JSON fields are ignored. Turning it on for activation output is
a five-step pass-through against the pinned `0.6.3` sources (file/line cites
below are into `packages/agents-server` and `packages/agents-runtime` at that
version):

1. **Create entity session streams fenced.** The agents-server's
   entity-manager create path issues the stream `PUT`; add
   `Write-Fence: true`. The re-`PUT` on entity reuse stays idempotent because
   the header is sent both times (it is part of chronicle's config-match).
2. **Forward the runtime's token on the append route.** In
   `src/routing/durable-streams-router.ts:599-616` the runtime-facing append
   proxy calls `forwardFetchRequest` with `durableStreamsBearerMode:
   'overwrite'`, and `src/utils/server-utils.ts:465` then overwrites
   `Authorization` with the gateway's own bearer — which would silently drop
   the runtime's capability. Before that overwrite, copy the runtime's bearer
   (read exactly as `writeTokenFromHeaders` does in
   `src/routing/stream-append.ts:190-199`) into `Write-Token`, and set
   `Write-Fence: true` on the forwarded request. Two lines: the gateway keeps
   authenticating itself, the runtime's token rides untouched, and a runtime
   that lost its token becomes a loud `401` instead of an unfenced write.
3. **Return chronicle's write token from the claim callback.** Both claim
   replies in `src/routing/internal-router.ts` — the local reply at
   `:714-726` and the forwarded-claim decoration at `:779-790` — call
   `mintClaimWriteToken(...)` (`:904`) to fabricate a gateway-local token.
   Return chronicle's instead: `WakeNotification.write_token` in webhook
   mode, `ClaimResponse.write_token` in pull mode. `isValidWriteToken`
   becomes a shape check (chronicle validates), and the gateway's
   `ClaimWriteTokenStore` can be deleted.
4. **Refresh the token from heartbeat replies.** The runtime captures
   `writeToken` once per wake (`src/process-wake.ts:1225` in
   `agents-runtime`) and its heartbeat handler (`:1245-1261`) already reads
   the reply body; also map chronicle's re-minted `write_token`
   (`AckResponse.write_token`) into that variable — one assignment. Required:
   chronicle's write-token TTL is lease + 5 s (35 s at the default lease)
   while activations run longer. The producer identity already matches the
   fenced class: `Producer-Id: entity-<url>` with `epoch = generation`
   (`src/process-wake.ts:536-538`) and `Producer-Seq` from 0 per wake.
5. **Leave command appends alone.** Inbox, wake, signal, manifest, and
   shared-state writes carry the agents-server's service identity and are the
   **open** class on a fenced stream — no change. Shared-state streams stay
   unfenced until opted in the same way (fenced `PUT` + the token forwarded
   as in step 2).

Roll chronicle fully before step 1 (an old replica ignores the header and
would create the stream unfenced — verify with the `HEAD` echo), and see
[docs/adr/0008-write-fencing-extension.md](adr/0008-write-fencing-extension.md)
for the recreated-subscription epoch caveat: after deleting and recreating an
entity's subscription, activations with the old stable producer id fail with
the base stale-epoch `403` until the new subscription's generation passes the
stored epoch.

---

## Gotchas (don't repeat these)

| Symptom | Cause | Fix |
| --- | --- | --- |
| Entities spawn but never wake; chronicle log shows no outbound webhook | chronicle's SSRF guard silently drops webhook URLs on `localhost`/RFC1918 | run chronicle with **`--webhook-allow-private`** (`webhook/ssrf.go`) |
| No `/__ds/subscriptions` ever hits chronicle; entity created but no wake | no **Electric** running — dispatch-subscription creation flows through the entity-bridge-manager's Electric shape | run the Electric service and set `ELECTRIC_AGENTS_ELECTRIC_URL` |
| `PUT /camus/x` returns a stream-creation 201 and no entity appears in the `entities` table | wrong endpoint — that path hits the DS proxy (`durable-streams-router.ts` `all('*', proxyPassThrough)`) | spawn via **`PUT /_electric/entities/:type/:instanceId`** (`entitiesRouter` base `/_electric/entities`) |
| `Cannot find module .../dist/index.js` | the `@electric-ax/*` workspace packages resolve to `dist/` and aren't built | `pnpm install` then build the agents chain (step 4) |
| chronicle and agents-server fight over a port | both default to `:4437` | keep chronicle on 4437; run agents-server on another port (`ELECTRIC_AGENTS_PORT=4500`) |
| Electric container won't bind `:3000` | something else owns 3000 (Grafana, etc.) | publish on another host port, e.g. `-p 3100:3000`, and point `ELECTRIC_AGENTS_ELECTRIC_URL` there |
| Agent never gets the key even though it's exported | the LLM call runs in the **runtime/example** process, not the agents-server | put `ANTHROPIC_API_KEY` in the example's env (step 5) |
| Mystery `POST /__presence__` in chronicle's log | an enterprise browser / local service probing `localhost:4437`, not Electric | ignore |

---

## Verifying it actually worked

- **chronicle Redis** has the subscription:
  `docker exec chronicle-redis-1 redis-cli --scan --pattern '*sub*'` →
  `ds:{__ds}:sub:webhook:camus:…` plus `…:links` and `ds:{__ds}:stream:camus/demo1/main`.
- **agents-server Postgres**:
  `select * from subscription_webhooks;` → a row mapping the subscription id to
  `http://localhost:4700/webhook`; `select url,type,status from entities;` →
  your entity `running`/`idle`.
- **runtime log** shows `wake received (epoch=N)` → `invoking handler` →
  `agent.run starting provider=anthropic …` with the message text.
- **chronicle log** shows the append to `/camus/demo1/main` and the runtime's
  follow-up `GET /camus/demo1/main` (inbox read).

---

## Teardown

```bash
# stop app processes (chronicle :4437, agents-server :4500, runtime :4700)
for p in 4437 4500 4700; do kill $(lsof -ti :$p) 2>/dev/null; done
# stop containers
docker rm -f electric-sync electric-agents-pg
make -C ~/dev/chronicle redis-down
```

---

## Projection snapshots: opt-in upstream patches

The corrected [Electric/SDK bundle](../experimental/electric-checkpoints/README.md)
implements the explicit
[bounded-input recovery contract](adr/0013-bounded-electric-runtime-recovery.md)
and is verified locally. The configuration below requires those patches, not the
[historical full-raw-replay bundle](../experimental/electric-snapshots/README.md).
**Do not deploy that historical bundle:** its SDK update mode can discard fields
from Electric's partial inbox updates, including when snapshots are disabled.
It remains unchanged as measurement evidence, not a deployment recommendation.

Chronicle now implements the optional [projection snapshot HTTP extension](spec/SNAPSHOTS.md)
for Redis and MemoryStore, behind `CHRONICLE_ENABLE_SNAPSHOTS=true`. It stores
opaque full images with source incarnation, consumed offset, integrity metadata
and conditional publication. **Enabling Chronicle alone does not make an existing
Electric installation use snapshots.** The SDK/runtime changes are local upstream
patches, not published npm packages or merged upstream changes.

The [research and source map](research/13-checkpointing-and-agent-recovery.md#proposed-electric-integration)
pins the upstream source revisions and explains the correctness constraints.
Apply and build the Durable Streams patch first, then Electric, following the
bundle's dependency recipe. The changes belong in `durable-streams/durable-streams`
and `electric-sql/electric`; Chronicle does not vendor those repositories.

The integration spans these layers:

- **Durable Streams client:** discover `Stream-Snapshot: v1`; load/publish
  `?snapshot=<projection-version>` images; check format, digest, source identity
  and ETag; preserve `If-Stream-Incarnation` across catch-up pages, long-poll,
  SSE and reconnects. Keep snapshot responses distinct from event responses.
  Missing/incompatible images may fall back to replay; source 410/errors must
  not become an empty database.
- **`@durable-streams/state` / `createStreamDB`:** bootstrap before subscription
  and export committed rows with their exact next-read offset. Reading the
  unpatched `db.offset` separately from visible collections is unsafe: it can
  advance before commit, and visible rows can contain optimistic edits. Hydration
  must not produce writes, event callbacks or readiness; catch-up establishes
  readiness, and failed hydration must never leave a usable partial image.
  Direct SDK callers must enable `onCommittedBatch` to use `exportState()`;
  direct EntityStreamDB callers must enable `onCommittedSnapshot` to use
  `utils.exportSnapshot()`. A no-op callback enables capture. Export during an
  uncommitted `onBeforeBatch`/`onBatch` hook is rejected; use the committed hook
  or export afterward. The runtime snapshot/bounded policies enable this capture
  automatically, without changing default legacy sync visibility.
- **`EntityStreamDB`:** a versioned codec covering built-in and
  custom collections, `_seq`, the explicit next sequence counter (including
  deleted rows), `_timeline_order`, row event/fork pointers, existing-key indexes
  and source position. Codec `electric-entity-image/v3` uses canonical stream-root
  pointers plus a count of **all flattened source items**, distinct from the
  known-schema sequence. Sixteen-digit ordering tokens cover safe-integer counts.
  HTTP grouping and pending optimistic edits must not change equal-cut image
  bytes. Do not synthesize inserts to restore this metadata. Root-based late forks
  can scan more history. The image must fit Chronicle's 1 MiB bound or fall back
  to retained-history replay; truncation or an LLM summary is not equivalent.
- **`processWake`:** legacy `full-replay` remains the default. Opt-in
  `bounded-checkpoint-v1` changes setup, `ctx.events` and self-observation to the
  post-completion delta. Handlers must consume that entire delta; consumers that
  require arbitrary historical callbacks must stay on full replay. On an aligned
  image hit, one guarded state reader also supplies raw inputs; no independent
  prefix reader runs. A miss replays retained history under the same bounded-input
  contract. Unknown change-event types fail bounded recovery rather than silently
  changing sequence counts; retained history must remain schema-readable. No
  handler or signal effect runs before a valid claim.
- **Completed work:** a canonical `runtime_checkpoint` records successful
  handler/tracked-effect completion, not notification, reader or producer progress.
  Unresolved fresh input, queued/paused inbox work, signals or processing failures
  prevent certification. Final acknowledgment comes from an aligned drained
  checkpoint; appending A's output after pending B does not acknowledge B.
  Source/incarnation failure attempts release without ack and still runs cleanup.
  Later teardown or callback failure cannot erase an already-durable marker;
  retry may skip completed work. A lost callback reply can leave its outcome
  uncertain. External effects still need application idempotency.
- **Publication policy:** freeze a committed cut, then publish asynchronously
  under a dedicated source-scoped `snapshot-publish` service grant. Observe the
  saved ETag, and tolerate a concurrent publisher winning. Saving an image does
  not establish work completion, renew a lease, or authorize an external effect.
  A failed save leaves the old image/log usable; completion is checked separately.
  Bound write frequency by measured replay work and image size, back off misses,
  and avoid snapshotting when image/hydration cost exceeds the replay work saved. The
  [HTTP/Redis measurements](../benchmarks/snapshots/README.md) include regressions
  for short and append-only streams; they are not Electric activation timings.
- **Agents-server / gateways:** preserve the caller's credential on snapshot
  PUT and DELETE instead of substituting a broad backend bearer. Chronicle then
  enforces dedicated publication authority. Forward query parameters,
  `If-Match`, `If-None-Match`, `If-Stream-Incarnation`, `Stream-Snapshot`,
  `Stream-Snapshot-Offset`, `Content-Digest` and response headers. A browser read
  credential must never become a privileged snapshot publisher via the proxy.
  Finish all-replica/gateway rollout before enabling publication.

**Bounded-mode trust prerequisite:** untrusted inputs must use Electric's
authenticated write-token gate. Direct Chronicle append principals are trusted
infrastructure; Chronicle does not classify event types or authenticate a marker's
semantic claim. Ordinary inbox payloads cannot supply top-level progress events.
Opaque external `notification.wakeEvent` payloads are rejected without ack in
bounded mode: the same offset can describe either redelivery or new out-of-log
work. Standard timers, cron and forks already append canonical source facts.

Add the policy to an application's existing `createRuntimeRouter` configuration
after applying the corrected patches and accepting the bounded-input contract.
Handlers keep their existing `ctx.db` interface:

```ts
import type { ProjectionSnapshotRecoveryPolicy } from '@electric-ax/agents-runtime'

const projectionSnapshots: ProjectionSnapshotRecoveryPolicy = {
  projectionVersion: 'my-agent-checkpoint-v3',
  rawInputRecovery: 'bounded-checkpoint-v1',
  // Load credentials from the runtime's secret provider, never browser code.
  publisherHeaders: () => publisherHeadersFromSecretProvider(),
  readerHeaders: () => readerHeadersFromSecretProvider(),
  maxImageBytes: 1024 * 1024,
  minReplayEvents: 100,
  minReplayMs: 25,
  minPublishIntervalMs: 60_000,
  missBackoffMs: 5 * 60_000,
}
// createRuntimeRouter({ ...existingConfig, projectionSnapshots })
```

The two secret-provider functions are application placeholders, not SDK exports.
Use `rawInputRecovery: 'full-replay'` for handlers requiring historical inputs.
To compare without images under the same bounded contract, omit
`projectionSnapshots` and set the router's top-level `rawInputRecovery` instead.
Rotate `projectionVersion` whenever schemas, reducer behavior or serialization
change. Conditional `DELETE ?snapshot=<old-version>` frees quota after old readers
are retired; use the old image's ETag and source incarnation. Chronicle permits
at most eight versions and 4 MiB of image bodies per source.

**Earlier experimental images must be rebuilt.** Older builds could omit a row,
lose partial-update fields, or retain optimistic/batch-dependent ordering despite
valid digests and source incarnations. Codec v3 rejects older image codecs; use a
fresh `projectionVersion` and rebuild from retained history. Updating the reader
does not repair an incomplete image. Image rotation also does **not** invalidate
existing `bounded-checkpoint-v1` source markers: discard disposable streams from
buggy pre-release bounded builds. If those builds processed real input, audit and
rebuild progress under the application's replay/idempotency policy before reuse.

**Real runtime coverage:** the companion harness runs Chronicle signed webhooks
through ElectricAgentsServer and the runtime's actual `processWake`, with real
Postgres and Electric shapes. Tests cover restoration, deletes, inclusive forks
with control events, pending signals, live cancellation, concurrent inputs,
pending-B noack, claim/source failures, handler retry, and checkpoint save followed
by failed `done:true` delivery. Two claim-fault tests use forwarding/stall fetch
shims at Electric's locally answered claim route; denial is an injected HTTP 401,
not a demonstrated backend claim decision. A failed callback is not a process
kill. The benchmark separately checks handler-observed hydrated state, exact
input/reply multiplicity, final acknowledgment, Redis lease release, complete
observed fixture history, and every captured source GET's cut/incarnation guard.

The frozen corrected sources pass 199 targeted runtime tests, 110 client/state
tests, 116 filtered memory/file fork cases, and 10 real integration cases. Runtime
and state typechecks and SDK declaration builds pass. The
[independent export-guard follow-up](../experimental/electric-checkpoints/independent-review/README.md)
passes four real-client SDK export cases and three Entity export cases:
capture opt-in, bootstrap-only rejection, optimistic persistence, unchanged
legacy visibility, and exact rows/cuts/indexes after synchronous dispatch.
This is scoped evidence, not a proof of all races. The wider client typecheck has
113 diagnostics also reproduced at the untouched pin; those failures remain.

**Corrected bounded-mode performance:** the
[final-source campaigns](../benchmarks/snapshots/README.md#bounded-electric-recovery-avoids-prefix-reads)
pass 360 measured activations and independently verify 180 no-prefix checkpoint
hits. At 10k updates over 16 keys, p50/p95 falls from 107.25/136.66 to
60.18/72.42 ms; combined Node CPU p50 falls from 89.06 to 48.39 ms. At 3k
append-only rows, activation p50 rises from 155.12 to 184.13 ms. Default-size
workloads and the 100-input pending inbox also regress. Both paths use the same
bounded-input contract, warm processes and forced publication with synthetic
handlers. These are workload-specific results, not a universal speedup or a
measurement of the default save cadence.

**Historical performance result:** the
[real runtime campaigns](../benchmarks/snapshots/README.md#electric-runtime-measurements-use-the-actual-activation-path)
passed 360 measured activations, but found no end-to-end median win with full
raw replay and forced publication. At 10k updates over 16 keys, p50 was
74.26 → 76.08 ms; at 3k append-only rows, 63.56 → 143.90 ms. These are local
synthetic handlers, not LLM or cold-process-start measurements. Default cadence
was deliberately disabled to measure publication costs. Do not infer that a
state image alone removes the runtime's historical-input work.

**Remaining rollout work:**

- Compare full replay with restore at arbitrary committed batch boundaries,
  including deletes, multiple changes in
  one batch, pending signals, cancellation, tool results, fork ordering,
  schema upgrades and crash/retry. Assert handler inputs and effects as well as
  final rows for the application's own schemas and consumers.
- Measure cold-activation p50/p95, CPU, transferred bytes, heap and publication
  cost using the actual application and its intended publication cadence. The
  synthetic runtime campaigns do not establish that application's performance.
- Reuse the bootstrap in `observe(entity)` only after specifying
  whether observers receive historical callbacks. Snapshot hydration itself
  must not masquerade as replayed historical events.

See the patch bundle for exact test commands, HTTP/Redis evidence and baseline
typecheck limitations. The options above exist only in the patched sources, not
the pinned released packages. Upstream acceptance and deployment are separate.

## Notes / known-good versions

- electric `@electric-ax/agents-*` built from source at the repo state of
  2026-06-13; `@durable-streams/*` client/server pulled from npm.
- chronicle `main` with the `webhook/` subscription package present
  (`config.go` defaults `Subscriptions: true`).
- The chat-starter spawn-endpoint mismatch is an **Electric example** issue, not
  a chronicle one — flagged here so the next person doesn't chase it in chronicle.
