# Chronicle ds-bench campaign

`benchmarks/ds-bench/dsbench.py` runs Chronicle with Electric's official
`ds-bench` client and result formats. The adapter pins ds-bench to commit
`93a1a066a511ad2ce5114dc429afb1fd0f6d99bf`. It also pins the Rust server
release source and the Node reference server source in `sources.json`.

The publication profile gives each system one server node budget of 4 vCPUs,
16 GiB of memory, and one local NVMe disk. Chronicle and Redis share the 4 vCPU
and 16 GiB budget. The metrics sidecar reports the combined Chronicle and Redis
process cost and the whole pod working set.

The downside is campaign time and cloud cost. The matrix contains 24 sequential
suites. Write cells use three confirmation runs after the load ladder finds a
plateau. Read and mixed cells use the current upstream single run method.

## What the campaign compares

The campaign runs four systems:

- The Rust durable-streams server uses its local WAL and memory modes.

- The Node reference server uses its memory mode.

- Ursula uses its disk WAL and memory modes.

- Chronicle uses Redis AOF `always` and Redis AOF `everysec`.

The report keeps these durability classes separate. Redis AOF `everysec` can
lose about one second of acknowledged writes after a host failure. Memory modes
can lose all process state. Neither belongs in the same durability row as a
local WAL or Redis AOF `always`.

The workload set contains:

- Write saturation at 10,000 and 100,000 streams.

- The blog fanout case with one stream, 50 appends per second, and 1, 10, 100,
  and 1,000 SSE subscribers.

- The current upstream SSE connection scale workload.

- The current upstream catchup workload.

- The current upstream read and write interference workload.

- The current upstream SSE delivery under write load workload.

The adapter does not copy the June blog numbers into the new table. Those runs
predate the current fleet barrier and latency method. Every comparison system
must run again with the pinned current harness.

Catchup setup is stricter than the pinned upstream revision. The client retries
failed seed appends, probes every stream, and refuses to measure unless each
stream contains exactly the declared number of bytes. The raw client result
records seed attempts, failures, retry rounds, and the verified size range. This
rule applies to every server. Validation uses the stored size probes, not a
bytes-per-read inference. This matters for servers such as Node that add record
framing which is not returned in the response payload.

S2 is available in upstream ds-bench, but it is not part of this profile. The
Electric blog comparison named the Rust server, Node, and Ursula. Adding S2
would create a separate object store durability class.

The official harness also starts MinIO on the server node. Every system uses it
to exchange benchmark results. Rust and Ursula can also use it as a cold tier,
while Chronicle and Node do not. MinIO sits outside the 4 vCPU primary SUT
limit. This remains a comparability limit. The campaign retains both server and
MinIO logs so cold-tier activity during a measurement can be audited.

The upstream 8 vCPU, six SSD Rust suites remain available in the prepared
checkout as a separate canonical profile. Their results must not be mixed into
the 4 vCPU parity table.

## Files

- `campaign.json` defines the resource budget, systems, durability labels, and
  workloads.

- `sources.json` pins comparison source commits and external image tags.

- `patches/chronicle.patch` adds Chronicle, combined process metrics, and raw
  per pod artifact collection to the pinned upstream checkout.

- `overlay/gke/chronicle.yaml` runs Chronicle and Redis in one pod.

- `overlay/suites/chronicle-calibration.json` declares the 1 to 3, 2 to 2, and
  3 to 1 Chronicle to Redis CPU split test.

- `docs/benchmarks/ds-bench/research.md` records the research and fixed choices.

- `docs/benchmarks/ds-bench/plan.md` records the implementation slices and
  publication rules.

## Test the adapter

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py test
```

This command prepares the exact upstream commit, applies the patch, copies the
overlay, runs the adapter tests, runs every upstream Python and shell test, and
compiles and tests the Rust client with the locked dependencies. The prepared
checkout stays under `.tmp/ds-bench/`.

## Run the local smoke test

The local path requires a running Docker daemon and `kind`. If `kind` is not
installed persistently, prefix each command with `nix shell nixpkgs#kind -c`.

```bash
python3 benchmarks/ds-bench/dsbench.py preflight \
  --target local \
  --output .tmp/ds-bench/preflight-local.json

python3 benchmarks/ds-bench/dsbench.py build \
  --target local \
  --output .tmp/ds-bench/images-local.json

python3 benchmarks/ds-bench/dsbench.py run \
  chronicle-smoke \
  --target local \
  --images .tmp/ds-bench/images-local.json
```

Local numbers only test wiring. Do not publish them.

## Run the remote campaign

The remote defaults use project `adityavkk-prototyping`, GKE region
`europe-west4`, GKE zone `europe-west4-b`, and Artifact Registry region
`europe-west1`.

### 1. Prove access before any spend

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py preflight \
  --target remote \
  --phase campaign \
  --output .tmp/ds-bench/preflight-remote.json
```

The command checks the active account, billing, required APIs, the benchmarking
network, GKE list access, Artifact Registry access, and the IAM permissions
needed to create and delete the benchmark resources. Campaign preflight also
checks the exact VM topology against project, region, machine family, disk,
address, and instance quotas. Any required failure stops cluster creation.

### 2. Prepare pinned comparison sources

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py prepare-sources
```

The command fetches the Rust server release source with a sparse checkout and
the Node reference server at its full commit. Both checkouts stay under
`.tmp/ds-bench/sources/`.

### 3. Build and freeze every image

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py build \
  --target remote \
  --output .tmp/ds-bench/images-remote.json
```

The command builds Chronicle, ds-bench, the Rust server, and the Node server
once. It resolves those images, Ursula, and Redis to immutable manifest digest
references. The later run commands use only those digest references. The build
uses build phase preflight, so a pending VM quota request does not block image
creation.

The Chronicle Cloud Build upload contains only `go.mod`, `go.sum`, the public
server Dockerfile, root runtime Go files, and the packages imported by
`cmd/chronicle`. It excludes repository history, benchmark results, docs,
operator files, internal mirror deployment files, and Copybara tooling. The
image record stores a SHA-256 digest and file count for that minimal build
context.

After a harness change, use `--reuse .tmp/ds-bench/images-remote.json` with the
same output path. The builder reuses an image only when its source identity,
registry, and digest all match. It rebuilds every changed source.

For a client-only correction, keep every server image identical to a sealed
primary campaign:

```bash
python3 benchmarks/ds-bench/dsbench.py build \
  --target remote \
  --output .tmp/ds-bench/images-catchup-fixed.json \
  --reuse .tmp/ds-bench/images-remote.json \
  --reuse-suts-from-archive \
    docs/benchmarks/ds-bench/results/20260727T122510Z-bd85274b
```

The builder verifies the primary evidence seal and copies the exact Chronicle,
Rust, Node, Ursula, and Redis manifest digests. It uploads and builds only the
changed ds-bench client. Manifest creation verifies the seal and copied digests
again. The image manifest records the file count and SHA-256 tree digest of the
uploaded client context.

### 4. Calibrate the Chronicle and Redis CPU split

Run all three declared splits:

```bash
python3 benchmarks/ds-bench/dsbench.py calibrate \
  --images .tmp/ds-bench/images-remote.json \
  --output-root .tmp/ds-bench/calibration-runs \
  > .tmp/ds-bench/calibration-execution.json

CALIBRATION_RESULTS=$(
  python3 -c \
    'import json; print(json.load(open(".tmp/ds-bench/calibration-execution.json"))["results_path"])'
)

python3 benchmarks/ds-bench/dsbench.py select-calibration \
  "$CALIBRATION_RESULTS" \
  > .tmp/ds-bench/calibration.json
```

The calibration command refuses to adopt an existing `chdb-cal` cluster. It
arms the same detached deadline watchdog used by the full campaign, saves raw
results and logs, deletes the cluster, and records proof that the cluster is
gone.

The selector requires a unique highest median throughput among valid cells.
Each valid cell must have an observed plateau, aligned windows, complete pod
results, zero client errors, and zero lazy stream creation. A tie requires
another calibration run. The selector does not choose a preferred split in a
tie.

### 5. Resolve the comparison suites

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py resolve \
  --calibration "$CALIBRATION_RESULTS" \
  --output-dir .tmp/ds-bench/resolved-parity
```

The command writes one immutable suite for each system and workload pair. It
freezes the selected Chronicle and Redis CPU split in every Chronicle suite.

### 6. Freeze the campaign manifest

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py manifest \
  .tmp/ds-bench/resolved-parity \
  --images .tmp/ds-bench/images-remote.json \
  --calibration-results "$CALIBRATION_RESULTS" \
  --output .tmp/ds-bench/campaign-manifest.json
```

The manifest records the Chronicle commit and worktree diff digest, the exact
Chronicle image build-context digest, ds-bench
commit and adapter digest, comparison source commits, image digests, Redis
settings, hardware, suite hashes, calibration evidence, and result validity
rules. It rejects a resolved split that differs from the calibration winner.

### 7. Run one paid cluster at a time

Run:

```bash
python3 benchmarks/ds-bench/dsbench.py campaign \
  --manifest .tmp/ds-bench/campaign-manifest.json \
  --output-root docs/benchmarks/ds-bench/results
```

The campaign refuses to adopt an existing cluster. Each suite gets a unique
cluster name. A detached watchdog can delete only that exact cluster name. The
runner saves raw results, server and MinIO logs, and cluster diagnostics before
teardown. It then deletes the cluster and stores a GKE absence proof before it
starts the next suite. On a completed suite, the runner also waits for the
detached watchdog to write its final status and exit before it seals the
archive.

On a failed suite, the default behavior still deletes the cluster. Use
`--keep-failed-cluster` only when a person is present to inspect and delete it.
That option does not arm the detached watchdog.

To rerun a complete workload slice after a harness correction, select it
explicitly:

```bash
python3 benchmarks/ds-bench/dsbench.py campaign \
  --manifest .tmp/ds-bench/campaign-manifest.json \
  --workload reads-catchup \
  --output-root docs/benchmarks/ds-bench/results
```

The execution record names every selected suite. Validation therefore expects
only that declared subset. A partial rerun cannot silently pass as a full
campaign.

### 8. Validate and report

Run:

```bash
ARCHIVE=docs/benchmarks/ds-bench/results/<campaign-id>

python3 benchmarks/ds-bench/dsbench.py verify-seal "$ARCHIVE"
python3 benchmarks/ds-bench/dsbench.py validate "$ARCHIVE"
python3 benchmarks/ds-bench/dsbench.py report "$ARCHIVE"
```

The campaign writes `evidence-checksums.json` after teardown. It contains the
size and SHA-256 digest of every raw evidence file plus one tree digest.
`verify-seal` recomputes the inventory and fails if any raw file changed.
Reports and validation files are excluded so they can be regenerated without
changing the evidence seal. For an older unsealed archive, run `seal` once
before validation:

```bash
python3 benchmarks/ds-bench/dsbench.py seal "$ARCHIVE"
```

The validator reads the raw per pod JSON and merged files. It rejects write
headlines with missing confirmation runs, partial fleets, misaligned windows,
client errors, lazy stream creation, or no observed plateau. A load ladder that
ends before a plateau becomes a lower bound. A failed cell becomes a gap, not a
zero. A setup failure, including a short catchup seed, is invalid. A
successfully measured cell that falls below 98 percent of declared offered load
or reports request errors is an overload observation. It remains evidence, but
it is not described as clean completion at the offered rate.

For catchup, the validator reads each client's post-write
`seed_verified_streams`, `seed_verified_min_bytes`, and
`seed_verified_max_bytes` fields. Response MiB per second still measures payload
bytes, which can exclude server-specific framing.

Corrected workload archives can replace complete system and workload slices in
one report:

```bash
python3 benchmarks/ds-bench/dsbench.py report "$ARCHIVE" \
  --supplement docs/benchmarks/ds-bench/results/<corrected-campaign-id>
```

Replacement happens at the complete system and workload boundary. The report
does not choose individual favorable cells from different runs.

The report keeps the raw files as the source of truth. It does not recalculate
the HDR percentiles produced by ds-bench. Sidecar samples and fleet artifacts
are copied from immutable snapshots and accepted only when their SHA-256 digest
matches the remote snapshot. Every suite also records its full cluster wall
time for cost accounting.

## Current environment status

The preflight run on July 27, 2026 confirms that billing, required APIs, IAM
permissions, the `benchmarking` VPC and subnet, and the `ds-bench` Artifact
Registry repository are ready in `adityavkk-prototyping`. Build phase preflight
passes.

Campaign preflight requires 80 VM vCPUs for one
`c4d-standard-16-lssd` server and two `n2d-standard-32` clients. Google Cloud
approved the project wide CPU increase from 32 to 96 and a separate 64 vCPU
Spot pool in `europe-west4`. Google Cloud denied the standard N2D increase.
The clients use the approved Spot pool, so campaign preflight passes. Spot
capacity can still disappear before or during a run, which makes a failed cell
eligible for a clean rerun rather than a benchmark result.

A local Chronicle smoke run passed on July 26, 2026 with Colima and `kind`
provided by an ephemeral Nix shell. It exercised reset, deploy, write, aligned
fleet merge, per-pod JSON and HDR collection, process samples, and local server
cleanup. Local throughput is not publication data.

## Current publication status

The baseline archive is
`docs/benchmarks/ds-bench/results/20260727T122510Z-bd85274b`. It contains all 24
planned suites and no surviving benchmark cluster. Its 2,584 raw files verify
against tree SHA-256
`ad3104c8a0a10ca8b7e81b88e636517181ad33f82ecf3b56c9bf6977d1a1c917`.

The corrected catchup supplement is
`docs/benchmarks/ds-bench/results/20260728T122629Z-59260e88`. It ran only the
four declared `reads-catchup` suites and reused the exact Chronicle, Rust, Node,
Ursula, and Redis image digests from the baseline. Its 481 evidence files verify
against tree SHA-256
`c0860ab25e172de648dde9280ea749d5343a142d1e6d3298fcd8a8cbf4c569e8`.
All 56 catchup cells have exact 16 MiB stored histories. Nine are valid overload
observations and none is invalid or missing.

The final report is
`docs/benchmarks/ds-bench/results/20260727T122510Z-bd85274b/report.md`.
It contains 217 cells, 73 valid overload observations, and no invalid or missing
rows. The supplement replaces all four complete catchup slices. The other 161
cells stay in the baseline archive. An independent GKE list after the supplement
returned no owned campaign clusters.

Chronicle recovered 8,141 failed seed appends out of 3,612,621 attempts before
the exact-size gate passed. Rust, Node, and Ursula recorded no failed seed
appends. Chronicle catchup was clean at 8 and 32 readers, then overloaded at 128
and 512 readers. The downside is that this dataset measures a single node and
does not establish availability, replication behavior, or managed Redis
performance.
