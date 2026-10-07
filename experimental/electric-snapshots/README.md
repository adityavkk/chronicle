# Chronicle projection snapshot upstream patches

Frozen against:

- `durable-streams/durable-streams` `461b40267aabd644558f9b19dbb9507dd5f691cf`
- `electric-sql/electric` `bb397424db0e1c153dc356713fd3dfd40315470c`
- Chronicle source-tree archive fingerprint `1c6eb4255a6b4a605f5e1dcb6985faf76b66aa742a40188d3d551ddedcde8f57`

## Fresh apply and dependency wiring

```sh
git clone https://github.com/durable-streams/durable-streams.git durable-streams
git -C durable-streams checkout 461b40267aabd644558f9b19dbb9507dd5f691cf
git -C durable-streams apply --index /path/to/bundle/durable-streams-461b402-snapshots.patch

git clone https://github.com/electric-sql/electric.git electric
git -C electric checkout bb397424db0e1c153dc356713fd3dfd40315470c
git -C electric apply --index /path/to/bundle/electric-bb39742-snapshots.patch

# Install BOTH trees before resolving Electric's TanStack path.
pnpm --dir electric install --frozen-lockfile
pnpm --dir durable-streams install --frozen-lockfile

pnpm --dir durable-streams --filter @durable-streams/client build
pnpm --dir durable-streams --filter @durable-streams/server build
rm durable-streams/packages/state/node_modules/@tanstack/db
ln -s "$(realpath electric/packages/agents-runtime/node_modules/@tanstack/db)" \
  durable-streams/packages/state/node_modules/@tanstack/db
pnpm --dir durable-streams --filter @durable-streams/state build
pnpm --dir durable-streams --filter @durable-streams/state typecheck

pnpm --dir electric --filter @electric-sql/client build
for p in electric/packages/agents-runtime electric/packages/agents-server; do
  rm "$p/node_modules/@durable-streams/client" "$p/node_modules/@durable-streams/state"
  ln -s "$(realpath durable-streams/packages/client)" "$p/node_modules/@durable-streams/client"
  ln -s "$(realpath durable-streams/packages/state)" "$p/node_modules/@durable-streams/state"
done
```

## Exact checks executed

```sh
# Durable Streams
pnpm test --run packages/client/test/stream.test.ts packages/state/test/stream-db.test.ts
# 104 passed: client 48, state 56
pnpm --filter @durable-streams/client build
pnpm --filter @durable-streams/server build
pnpm --filter @durable-streams/state build
pnpm --dir packages/state run typecheck

# Electric
pnpm --dir packages/agents-runtime test --run test/process-wake.test.ts test/entity-stream-db-snapshot.test.ts
# 66 passed: processWake 64, EntityStreamDB snapshot 2
pnpm --dir packages/agents-server test --run test/electric-agents-routes.test.ts test/server-utils.test.ts
# 70 passed: routes 43, server utils 27
pnpm --dir packages/agents-runtime run typecheck
pnpm --dir packages/agents-runtime run build
pnpm --dir packages/agents-server run typecheck
pnpm --dir packages/agents-server run build
```

Durable state, Electric runtime, and Electric server target typechecks pass. The
pinned Durable client package-wide typecheck is not green before this feature:
it reports existing Vitest/fetch mock narrowing errors plus unrelated test union
narrowing. Its source build and all 48 client tests pass; no snapshot-added line
appears in that typecheck output.

## Reproduce the real Chronicle/Redis HTTP run

`electric-snapshot-e2e.mts` is the executed driver and `e2e-result.json` is its
sanitized stdout. Build Chronicle from the reviewed Chronicle checkout (the
bundle intentionally does not contain a whole source archive), then create a
private token directory and the exact policy used by the run:

```sh
cd /path/to/chronicle-checkout
mkdir -p .tmp/electric-snapshot-e2e
go build -o .tmp/electric-snapshot-e2e/chronicle-e2e ./cmd/chronicle
cd .tmp/electric-snapshot-e2e

umask 077
openssl rand -hex 32 > .writer-token
openssl rand -hex 32 > .publisher-token
openssl rand -hex 32 > .browser-token
cat > e2e-service-policy.json <<'JSON'
{
  "services": [
    {
      "identity": "writer",
      "actions": ["read", "append", "create", "delete", "subscribe", "link"],
      "namespaces": ["e2e", "principal"]
    },
    {
      "identity": "publisher",
      "actions": ["read", "snapshot-publish"],
      "namespaces": ["e2e"]
    },
    {
      "identity": "browser",
      "actions": ["read", "append"],
      "namespaces": ["e2e"]
    }
  ]
}
JSON
```

The `browser` credential is a valid Chronicle **service principal** with only
read/append authority. It models the maximum authority a browser-facing caller
may carry for this test; it is not a browser JWT or interactive browser session.

Start a dedicated Redis with durability enabled:

```sh
docker run --name chronicle-snapshot-redis --rm \
  -p 127.0.0.1:6387:6379 redis:7-alpine \
  redis-server --appendonly yes --appendfsync always
```

In another shell, start Chronicle. This command expands token files into the
process environment; do not print that environment or commit the token files.
These foreground commands are for ordinary local terminals. In an Amp orb, use
`amp orb service start` for long-lived services, as in the
[Chronicle benchmark recipe](../../benchmarks/snapshots/README.md#reproduce-locally),
without exposing a portal for these private fixtures.

```sh
cd /path/to/chronicle-checkout/.tmp/electric-snapshot-e2e
export CHRONICLE_LISTEN=127.0.0.1:18437
export CHRONICLE_REDIS_URL=redis://127.0.0.1:6387/0
export CHRONICLE_STORE=redis
export CHRONICLE_STREAM_ROOT=/v1/stream/
export CHRONICLE_ENABLE_SNAPSHOTS=true
export CHRONICLE_AUTH_MODE=enforce
export CHRONICLE_SERVICE_POLICY_FILE="$PWD/e2e-service-policy.json"
export CHRONICLE_SERVICE_BEARER="writer:$(cat .writer-token),publisher:$(cat .publisher-token),browser:$(cat .browser-token)"
export CHRONICLE_UI=false
exec ./chronicle-e2e
```

Run the E2E from the bundle directory. All four inputs are required; the driver
has no checkout or server defaults. It refuses non-loopback roots and roots not
ending in `/`, generates `e2e/snapshot-<UUID>`, and deletes it in `finally`.

```sh
export DURABLE_STREAMS_CHECKOUT=/path/to/patched/durable-streams
export ELECTRIC_CHECKOUT=/path/to/patched/electric
export CHRONICLE_STREAM_ROOT=http://127.0.0.1:18437/v1/stream/
export CHRONICLE_TOKEN_DIR=/path/to/chronicle-checkout/.tmp/electric-snapshot-e2e
$DURABLE_STREAMS_CHECKOUT/packages/server-conformance-tests/node_modules/.bin/tsx \
  electric-snapshot-e2e.mts > e2e-result.json
```

This was an **actual DurableStream + EntityStreamDB HTTP test** against the
extracted Chronicle Go binary and a dedicated Redis 7 instance. It verified
publish/load/hydrate/suffix replay, exact sequence and replacement rows, stale
CAS, 1 MiB image rejection, quota headers, conditional retirement, denial for
writer/browser retirement, and 412 after source deletion/recreation. It is not
a `processWake` activation test or an activation performance measurement. The
separate real runtime harness below supplements the deterministic mocked
`processWake` interleaving tests.

The checked-in sanitized result has `cleanedUp: true`; no token or source URL
with credentials is present. Stop Chronicle and the dedicated Redis container
after the run (`docker stop chronicle-snapshot-redis`).

## Real `processWake` E2E and cold-activation campaign

Files under `process-wake-e2e/` use the repository's real runtime DSL: real
ElectricAgentsServer, Postgres migrations, Electric shapes, signed Chronicle
webhook delivery, runtime handler HTTP, Chronicle `done:true`, persisted outputs,
and subscription ack/release. StreamDB, producers, source responses, and
claim/ack handlers are not mocked. The source gate only instruments source
traffic. The callback gate forwards Chronicle's advertised callback/JWKS URLs
and can inject one fixture-scoped `done:true` 503.

Use `e2e-service-policy.json`. The backend writer needs `read`, `append`,
`create`, `delete`, `subscribe`, and `link` on `e2e` and `principal`, but no
snapshot authority. Only the publisher has `snapshot-publish`. The browser
credential is a valid read/append-only service principal modeling browser
authority, not a browser JWT/session. Generate tokens under `umask 077` and
never print them.

Anonymous Docker Hub manifest pulls were denied despite public tag metadata, so
the executed Electric image was built from the patched, pinned Electric checkout:

```sh
cd /path/to/patched/electric
docker buildx build --load -t electric-snapshot-processwake:bb39742 \
  --build-context electric-telemetry=packages/electric-telemetry \
  --build-arg ELECTRIC_VERSION=1.8.1 --target runner \
  -f packages/sync-service/Dockerfile packages/sync-service
```

Image digests are in the raw JSON. Start isolated Postgres 18 and this image
under a unique Compose project. Never point this harness at shared databases:
the server runs migrations and the harness writes/deletes fixture data. Bind
fixture ports to loopback. Keep this shell's project name for teardown:

```sh
cd /path/to/chronicle-checkout/.tmp/electric-snapshot-e2e
export ELECTRIC_CHECKOUT=/path/to/patched/electric
export COMPOSE_PROJECT_NAME="chronicle-snapshot-$(date +%s)"
export PG_HOST_PORT=127.0.0.1:55432 ELECTRIC_HOST_PORT=127.0.0.1:53060
cat > electric-compose.override.yml <<'YAML'
services:
  electric:
    image: electric-snapshot-processwake:bb39742
YAML
docker compose -f "$ELECTRIC_CHECKOUT/packages/agents-server/docker-compose.dev.yml" \
  -f "$PWD/electric-compose.override.yml" up -d --wait postgres electric
```

In another terminal, start a dedicated Redis. The executed `redis:7-alpine`
resolved to 7.4.11; pin that version when reproducing. AOF used `appendfsync
always`, with default RDB save rules and `noeviction`:

```sh
docker run --name chronicle-processwake-redis --rm \
  -p 127.0.0.1:6388:6379 redis:7.4.11-alpine \
  redis-server --appendonly yes --appendfsync always --maxmemory-policy noeviction
```

Start the callback gate from the bundle directory in its own terminal, then
start Chronicle in another terminal, advertising that gate as its public origin:

```sh
node process-wake-e2e/chronicle-callback-gate.mjs
```

```sh
cd /path/to/chronicle-checkout/.tmp/electric-snapshot-e2e
export CHRONICLE_LISTEN=127.0.0.1:18438
export CHRONICLE_PUBLIC_URL=http://127.0.0.1:18439
export CHRONICLE_REDIS_URL=redis://127.0.0.1:6388/0
export CHRONICLE_STORE=redis CHRONICLE_STREAM_ROOT=/v1/stream/
export CHRONICLE_SUBSCRIPTIONS=true CHRONICLE_WEBHOOK_ALLOW_PRIVATE=true
export CHRONICLE_ENABLE_SNAPSHOTS=true CHRONICLE_AUTH_MODE=enforce
export CHRONICLE_SERVICE_POLICY_FILE="$PWD/e2e-service-policy.json"
export CHRONICLE_SERVICE_BEARER="writer:$(cat .writer-token),publisher:$(cat .publisher-token),browser:$(cat .browser-token)"
export CHRONICLE_KEYS_FILE="$PWD/e2e-keys.json" CHRONICLE_UI=false
exec ./chronicle-e2e
```

Node 26 must use the installed Undici implementation consistently. The patched
dispatcher-backed Durable proxy/JWKS paths call matching `undici.fetch`; mixing
Node's global fetch with an installed-Undici `Agent` dropped response metadata
headers. E2E and benchmark paths use the corrected transport.

Exact E2E commands from `packages/agents-runtime`:

```sh
export DATABASE_URL=postgres://electric_agents:electric_agents@127.0.0.1:55432/electric_agents
export ELECTRIC_URL=http://127.0.0.1:53060
export ELECTRIC_AGENTS_TEST_BACKEND_MANAGED=0
export CHRONICLE_STREAM_ROOT=http://127.0.0.1:18438/v1/stream/
export CHRONICLE_CALLBACK_GATE_URL=http://127.0.0.1:18439
export CHRONICLE_REDIS_URL=redis://127.0.0.1:6388
export CHRONICLE_TOKEN_DIR=/path/to/private/token-directory
CHRONICLE_E2E_SNAPSHOTS=off pnpm exec vitest run test/chronicle-process-wake-e2e.test.ts
CHRONICLE_E2E_SNAPSHOTS=on pnpm exec vitest run test/chronicle-process-wake-e2e.test.ts
```

Both modes assert the observed unstamped-input fixture result
`["one","one","two"]`; this is not a universal all-history guarantee because
stamped events remain subject to existing offset filters. They also assert exact
persisted replies, cancellation, final ack, and automatic redelivery after a
real callback 503, without claiming exactly-once external effects.

The benchmark driver requires `ELECTRIC_CHECKOUT`, `DURABLE_STREAMS_CHECKOUT`,
`CHRONICLE_BINARY`, `CHRONICLE_BENCH_OUTPUT`, and the fixture variables above.
The final run uses 15 measured samples in each opposite-order phase
(30/path/scenario), plus one warmup per run. It hashes patches (including
untracked harness files), harness, and binary before and after and fails on
drift.

```sh
export ELECTRIC_CHECKOUT=/path/to/patched/electric
export DURABLE_STREAMS_CHECKOUT=/path/to/patched/durable-streams
export CHRONICLE_BINARY=/path/to/chronicle-checkout/.tmp/electric-snapshot-e2e/chronicle-e2e
export CHRONICLE_BENCH_OUTPUT=/path/to/final-results.json
export DATABASE_URL=postgres://electric_agents:electric_agents@127.0.0.1:55432/electric_agents
export ELECTRIC_URL=http://127.0.0.1:53060
export ELECTRIC_AGENTS_TEST_BACKEND_MANAGED=0
export CHRONICLE_STREAM_ROOT=http://127.0.0.1:18438/v1/stream/
export CHRONICLE_CALLBACK_GATE_URL=http://127.0.0.1:18439
export CHRONICLE_REDIS_URL=redis://127.0.0.1:6388
export CHRONICLE_TOKEN_DIR=/path/to/private/token-directory
node process-wake-e2e/run-chronicle-process-wake-benchmark.mjs

CHRONICLE_BENCH_UPDATE_EVENTS=10000 CHRONICLE_BENCH_APPEND_EVENTS=3000 \
  CHRONICLE_BENCH_INBOX_EVENTS=100 CHRONICLE_BENCH_OUTPUT=/path/to/scaled-results.json \
  node process-wake-e2e/run-chronicle-process-wake-benchmark.mjs
```

`final-results.json` contains all 192 samples: 12 warmups and 180 measured
samples. All passed; all 90 measured snapshot samples were actual image hits,
with 30 complete equivalent passing pairs per scenario and no fallback, failed,
incomplete, or drifted sample. `final-summary.json` reports nearest-rank p50/p95
without dropping failures. `scaled-results.json` and `scaled-summary.json` repeat
the same campaign at 10,000 updates, 3,000 append-only rows, and 100 pending
inputs; all 192 samples also passed and its 838,854-byte append-only image stayed
below 1 MiB. Snapshot publication gates (`minReplayEvents`, `minReplayMs`, and
`minPublishIntervalMs`) are intentionally forced to zero in both campaigns.
`e2e-replay.log` and `e2e-snapshot.log` retain the real callback-503/redelivery
runs; these are semantic/fault tests, not benchmark samples and not evidence of
exactly-once external effects. These two logs predate the sampler-only lifetime
cleanup fix. Both benchmark campaigns and the targeted cleanup regression tests
ran after that final fix.

Nearest-rank activation p50/p95 milliseconds (replay → snapshot plus full raw
replay) were:

| Campaign | Scenario | p50 | p95 |
| --- | --- | ---: | ---: |
| Default | update-heavy | 38.93 → 49.08 | 57.51 → 57.19 |
| Default | append-only | 49.24 → 68.30 | 57.26 → 75.89 |
| Default | large-inbox | 45.19 → 77.43 | 53.68 → 86.43 |
| Scaled | update-heavy | 74.26 → 76.08 | 94.18 → 102.71 |
| Scaled | append-only | 63.56 → 143.90 | 76.51 → 194.62 |
| Scaled | large-inbox | 49.08 → 78.58 | 64.58 → 99.41 |

Thus full-raw-replay snapshot recovery regressed median activation in every
measured workload; the 10,000-update case was closest to break-even.

`hashes.electricPatchSha256` and `hashes.durablePatchSha256` in the raw result
are frozen working-tree fingerprints computed from tracked diffs plus each
untracked file. They are provenance for the exact measured tree, but are not
the SHA-256 of the exported patch files. `SHA256SUMS` records the latter after
export, as well as every other transferred artifact.

Seed histories are deterministic. The default update-heavy workload emits 500
ordered state updates across 16 fixed keys and append-only emits 300 ordered item
inserts; the scaled variants use 10,000 and 3,000 respectively. Each state
history is appended as one comma-delimited Durable Streams batch. Large-inbox
holds real webhook delivery while appending 100 ordered 8192-byte inputs, then
times release through completion. The handler observes and copies pre-mutation
`ctx.state` rows; validation checks those rows independently after timing.

Timing starts after `claimHeaders` resolution at wake-received logging and ends
after successful `done:true`; it includes lookup, recovery, handler writes, and
ack, but excludes trigger append and semantic validation. The source-gate
capture window starts earlier, before `entity.send()` or
`pendingDelivery.release()`, so state-workload traffic includes the triggering
append. Its `total` is source-gate request/response bodies only; callback-gate
claim/done traffic and post-timing validation reads are excluded. Publication
duration is gate-observed snapshot HTTP only, excluding earlier export,
serialization, and hashing. CPU/memory cover the combined runtime, embedded
agents-server, test, and source-gate Node process—not runtime code alone or
server capacity. Each passing sample checks the handler's pre-mutation hydrated
state, exact raw IDs, exact reply multiplicity, one no-fault activation,
quiescent ack=head, and independent Redis
`phase=idle, holder=0, lease_until_ns=0` after timing.

Executed environment: Node 26.10.0, pnpm 10.12.1, Chronicle built with Go
1.26.2, Redis 7.4.11, PostgreSQL 18.6, and source-built Electric 1.8.1 at the
pin above. The orb exposed 4 Intel Xeon 2.60 GHz vCPUs and 8,343,158,784 bytes
RAM on Linux 6.1.158 x86-64. Raw results pin the exact Electric image repository,
manifest, and config digests.

After running, stop Chronicle and the callback gate, then stop the dedicated
Redis container with `docker stop chronicle-processwake-redis`. In the original
Compose shell, remove only this disposable project and its volume:

```sh
docker compose -f "$ELECTRIC_CHECKOUT/packages/agents-server/docker-compose.dev.yml" \
  -f "$PWD/electric-compose.override.yml" down -v
```

## Scope and limitations

- Runtime recovery is selective and explicit: `rawInputRecovery: "full-replay"`
  replays raw history separately so signals, inbox selection/cancellation,
  setup, `ctx.events`, and effects preserve existing behavior. Raw and state
  readers converge at one exact guarded next-read cut before activation. Full
  raw replay can erase most or all recovery gains; this is **not suffix-only**.
- No bounded raw-input contract is implemented. Arbitrary consumers are not
  claimed safe. Observer historical callbacks remain unchanged.
- Older experimental images affected by canonical optimistic-echo omission
  remain digest/codec-valid; reader updates cannot repair them. Retire those
  images, or rotate `projectionVersion` and rebuild from retained history.
- Images capture only canonical TanStack synced rows, never optimistic overlays;
  nested values are deep-cloned. Pending insert/update/delete plus rollback,
  failed batches, deletion sequence, replacement semantics, pointers, stable
  order, prior-batch anchors, and hydrate-without-callback suffix export have
  regression coverage. The disabled/default path does not deep-copy rows per
  batch; it captures on demand only when `exportState()` is called.
- Hydration happens before subscription without fake insert callbacks. Image and
  response offsets must match. Hydration fallback keeps the loaded incarnation.
  A guarded 410/412 aborts the whole activation, releases a concurrent claim,
  and acknowledges no offset.
- Publication uses the incarnation from the same committed reader cut, runs
  asynchronously behind measured gates, and uses dedicated publisher headers.
  CAS loss invalidates expectations and forces a later lookup rather than being
  cached as a missing image.
- Checkpoint mode treats collection commit failures as terminal and publishes
  no candidate. The legacy known-live-query-error swallowing behavior remains
  only when checkpointing is disabled. Custom publishers must configure
  `onCommittedBatch` (or `onCommittedSnapshot` for EntityStreamDB); calling
  `exportState()` alone does not enable strict commit-error handling. The pinned
  integration reads TanStack's internal synced-row map, so dependency upgrades
  require rerunning these compatibility tests.
- A post-join guard failure while claim acquisition is delayed aborts the whole
  activation without running the handler or acking. Joined live inputs remain
  queued for `ctx.events` and runtime side effects until a valid claim exists.
- Agents-server preserves caller credentials for snapshot PUT and conditional
  DELETE. Tests prove ordinary/browser append credentials cannot retire images.
