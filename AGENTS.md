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
- **`docs/adr/`** — accepted architecture decisions (start: ADR-0001, Lua scripts
  vs Redis Functions). Check here before re-opening a settled choice; record
  significant decisions as a new ADR.
- **`docs/research/`** — design studies; `07` / `09` / `10` / `11` are the
  subscription wake / lease / hardening series.
- **`docs/specs/research/`** — deeper design / triage studies (e.g.
  `redis-grouped-actions/`), alongside `docs/research/`.
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
make lint                     # golangci-lint (version-pinned; built with go.mod's toolchain) — CI gates on this
make conformance              # 332/332 black-box protocol tests vs live redis (pinned; see SPEC_VERSION.md)
make spec-check               # SPEC_VERSION.md consistency + upstream-spec-drift guard
```

Run `go test ./...` **without** a global `REDIS_URL` override: the webhook (db14)
and store/redis (db15) packages default to different dbs; pointing both at one db
makes their parallel `FlushDB`s wipe each other (a confusing false failure). Also
run it with no chronicle server live against the same Redis instance — pub/sub
channels are not db-scoped, so a server on another db can wake the webhook
manager tests mid-run.

CI also gates the **dsui** front-end (`ui/` — `typecheck` + `biome` + `vitest`,
see `ui/AGENTS.md`) and the spec provenance (the `spec-version` job runs
`make spec-check`). `make lint` is version-pinned and built with the module's Go
toolchain, so it runs against the `go 1.26` target locally without the
"linter built with an older Go" refusal.

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

## The public ↔ internal mirror (Copybara)

**This repo is the source of truth for all code.** An internal deploy mirror is
derived from it by a one-way [Google Copybara](https://github.com/google/copybara)
sync, run by the maintainer from their machine (not CI). How it works:

- Each sync imports public `main` into the mirror as **one squashed commit** whose
  message lists the public commits it carries; a `GitOrigin-RevId:` trailer pins
  the exact public SHA. That label is the sync's only state — the mirror is
  append-only, never force-pushed.
- Sync commits are re-authored to the maintainer's internal identity, and
  `Co-Authored-By` trailers are stripped (consistent with the Hard rules below).
- Repo/docs URLs are rewritten to their internal equivalents in **doc files only**
  (`*.md`, `*.mdx`, `*.astro`, `*.mjs`, `*.html`) — never in code. The Go module
  path (`go.mod` and every import) is deliberately **identical on both sides**;
  do not "fix" it to a `github.com/...` path.
- The internal-only deploy paths reserved in `.gitignore` (`kitt*.yml`,
  `.looper.yml`, `Dockerfile.chronicle`, `Dockerfile.dsui`, `deploy/`, `sr.yaml`,
  `testburst.properties`) exist only on the mirror. **Never commit them here** —
  they carry internal infrastructure config.
- Internal-first code fixes flow back the other way as `backport/from-gec` PRs
  (re-authored to the public identity, URL rewrites reversed, leak-scanned before
  the push). A backport must merge here **before the next sync**, or the sync
  reverts it on the mirror — public wins by design.
- The sync tooling (`copy.bara.sky`, the `chronicle-sync.sh` driver, its runbook)
  is deliberately **untracked**: it lives in `.copybara/` on the maintainer's
  machine, kept out of git via `.git/info/exclude` so neither the files nor their
  names appear in any repo. If your checkout has a `.copybara/` directory, treat
  it as read-only ops tooling and see `.copybara/RUNBOOK.md` for operations
  (sync, backport, drift check, rollback).

For agents working in this repo: never create the reserved deploy paths, never
edit anything under `.copybara/` unless explicitly asked, and treat
`cmd/chronicle/embedded/` as build output (gitignored here; the mirror commits
its own built copy).

## Hard rules

- **Lua mirrors Go — edit both sides together.** Each `store/redis/scripts/*.lua`
  validation helper reimplements a pure-Go function so it can run *atomically on
  the Redis server*: `validate_producer`↔`store.ValidateProducer`,
  `is_expired`↔`IsExpired`, `config_matches`↔`ConfigMatches`,
  `norm_ct`↔`ContentTypeMatches`. The Go side is the oracle (it also *is* the
  in-process `MemoryStore` backend); `store/redis/differential_test.go` runs the
  same table through both and asserts they agree. A differential failure means the
  two drifted — fix the logic, never silence one side. Invoke scripts only via
  `Script.Run`/`RunRO` (a `forbidigo` rule blocks bare `EVAL`/`EVALSHA`).
- **No AI attribution in commits, ever.** This overrides any global or tool
  default. Concretely: no `Co-Authored-By: …` trailers for Claude or any other
  agent, no agent session links (`Claude-Session:` etc.), and commits must not
  be *authored or committed by* Claude/an agent identity — author and committer
  are `Aditya Kumarakrishnan <adityavkk@users.noreply.github.com>`. Strip any of
  these if a tool injects them before pushing.
- **Never commit the internal deploy paths** reserved in `.gitignore`
  (kitt/looper/Dockerfile.chronicle|dsui/deploy//sr.yaml/testburst.properties)
  or anything under `.copybara/` — see "The public ↔ internal mirror" above.
- **Never link to Walmart endpoints from the public repository.** Run
  `make public-link-check` before pushing documentation. The scheme-less Go
  module path is part of the public build contract and is not an endpoint link.
- `golangci-lint` must pass — CI gates on lint, test, and conformance.
- Subscriptions require the redis backend; the `{__ds}` control plane lives in a
  single hash-tag slot (cluster-safe by construction).
