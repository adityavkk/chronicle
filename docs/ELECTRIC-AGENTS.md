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

## Notes / known-good versions

- electric `@electric-ax/agents-*` built from source at the repo state of
  2026-06-13; `@durable-streams/*` client/server pulled from npm.
- chronicle `main` with the `webhook/` subscription package present
  (`config.go` defaults `Subscriptions: true`).
- The chat-starter spawn-endpoint mismatch is an **Electric example** issue, not
  a chronicle one — flagged here so the next person doesn't chase it in chronicle.
