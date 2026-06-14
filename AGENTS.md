# AGENTS.md — working on chronicle

Orientation for the next implementer (human or agent): the map of the codebase,
the cheat sheets, and how to pick up the open work. For *using* chronicle see
`README.md`; for the *protocol* see `docs/spec/PROTOCOL.md`.

## Map

| Area | Where |
|---|---|
| Protocol core (pure) | `protocol/` — headers, cursors, producer rules |
| Storage contract + Redis backend | `store/`, `store/redis/` — Lua scripts, frames, pub/sub (mirrors the Caddy plugin) |
| HTTP layer | `handler.go`, `mount.go` |
| Subscriptions (`__ds`) | `subscriptions.go`, `webhook/` — webhook + pull-wake, fencing, leases, the recovery sweep |
| Observability | `metrics/` — Prometheus `/metrics` + `/healthz` + `/readyz` (enable with `-metrics-listen`) |
| Server binary | `cmd/chronicle/` |
| Load-test rig | `loadtest/`, `loadgen/` — GKE + managed Redis, the sweep-scale driver |
| Fault injection | `jepsen/` — k3d durability harness |

## Cheat sheets & runbooks — start here

- **`docs/PLAN.md`** — architecture and its tradeoffs.
- **`docs/research/`** — design studies; `07` / `09` / `10` / `11` are the
  subscription wake / lease / hardening series.
- **`docs/spec/PROTOCOL.md`** — the wire contract. `docs/spec/README.md` pins the
  upstream Caddy commit to diff against.
- **`jepsen/README.md`** — the fault-injection durability harness (k3d).
- **`docs/ELECTRIC-AGENTS.md`** — chronicle as an ElectricSQL Agents backend, with gotchas.
- **`loadtest/AGENTS.md`** — ⭐ the GKE load-test rig's "don't repeat my mistakes":
  pre-flight quota checks, Cloud Build, Connect Gateway, the deployment contract,
  methodology. Read it before running the rig.
- **`loadtest/README.md`** — rig overview + the one-command flow.
- **`loadtest/RESULTS-gke.md`** — worked runs and the numbers.
- Live docs: <https://adityavkk.github.io/chronicle/> (the `/subscriptions` page
  covers the sweep, its scaling, and the open questions).

## Dev loop

```bash
make redis-up && make run     # local server on :4437
make test                     # unit + integration (-race; integration needs redis)
make test-unit                # pure-core only, <1s
make lint                     # golangci-lint — CI gates on this
make conformance              # ~330 black-box protocol tests vs live redis
```

Run `go test ./...` **without** a global `REDIS_URL` override: the webhook (db14)
and store/redis (db15) packages default to different dbs; pointing both at one db
makes their parallel `FlushDB`s wipe each other (a confusing false failure).

## The load-test rig (the open scaling work)

The subscription recovery sweep is `O(subscriptions × links)` per tick. The
batched form (pipelined reads) holds well under the 2 s interval into the tens of
thousands; the rig measures it on real infra:

```bash
cd loadtest && make all SPEC=spec/sweep-10k.yaml   # provision → run → ALWAYS tear down
```

- **Metrics the SUT exposes:** `chronicle_sweep_tick_seconds`,
  `chronicle_sweep_subs_evaluated`, `chronicle_sweep_tails_batched`,
  `chronicle_sweep_wakes_total`, `chronicle_wake_delivery_seconds`,
  `chronicle_wake_event_seconds`, `chronicle_worker_due_items`.
- **Gotchas:** `loadtest/AGENTS.md`. **Numbers:** `loadtest/RESULTS-gke.md`.
- **Cheap per-change guard:** `BenchmarkSweepOnce` + `benchstat` vs a `main`
  baseline catches round-trip regressions without a cluster.
- **Open next:** sweep the K curve to 100k; raise `sut.replicas` and read the
  managed-Redis CPU for the `O(N·K)` redundancy; then shard the sweep across
  replicas or add the doc-10 delivery outbox.

## Redis requirements (verified against the code)

chronicle's store uses `EVALSHA` Lua, pub/sub, ZSET-lex, and **key-level**
`PEXPIRE` / `PERSIST` — there is **no `HEXPIRE` / hash-field TTL** in the code
(grep is clean) — so it runs on Redis 6.0+ (the rig validated on Memorystore
Redis 7.2). The project standardizes on the managed Redis 8 offering; target it
for production-representative numbers. `maxmemory-policy noeviction` is the hard
requirement — any eviction silently truncates streams (chronicle warns at boot).

## Hard rules

- **No AI co-author trailers** in commit messages.
- `golangci-lint` must pass — CI gates on lint, test, and conformance.
- Subscriptions require the redis backend; the `{__ds}` control plane lives in a
  single hash-tag slot (cluster-safe by construction).
