# Chronicle topology scaling runbook

## Goal

Find the smallest Chronicle setup that reaches at least 80 percent of the
better durable Rust WAL or Ursula disk result in every selected benchmark cell.
The setup must also complete at least 98 percent of offered work, return no
request errors, stay within twice the reference latency, and stay within the
memory limit.

The screen keeps the same total primary SUT budget as the published comparison:
4 vCPUs, 16 GiB, one server node, and one local SSD.

## Fixed budget screen

The screen tests six configurations:

| Configuration | Chronicle replicas | Redis masters | Chronicle CPU | Redis CPU |
|---|---:|---:|---:|---:|
| `chronicle-c1-r1` | 1 | 1 | 2 vCPU | 2 vCPU |
| `chronicle-c2-r1` | 2 | 1 | 2 vCPU total | 2 vCPU |
| `chronicle-c4-r1` | 4 | 1 | 2 vCPU total | 2 vCPU |
| `chronicle-c1-r3` | 1 | 3 | 2 vCPU | 2 vCPU total |
| `chronicle-c2-r3` | 2 | 3 | 2 vCPU total | 2 vCPU total |
| `chronicle-c4-r3` | 4 | 3 | 2 vCPU total | 2 vCPU total |

All Redis configurations use AOF `always`. Three Redis masters divide the same
total Redis CPU and memory as one Redis process. All masters use the same local
SSD.

The six discriminator suites cover write saturation, one stream SSE fanout,
100 stream SSE connection scale, catchup reads, mixed writes, and mixed
delivery.

## Hypotheses

- More Chronicle replicas can spread HTTP work, SSE sockets, response
  formatting, and Redis client work.
- Three Redis masters can spread work when a test uses many streams.
- Redis sharding should not help one stream fanout because one stream stays in
  one Redis hash slot.
- More processes may not fix SSE delivery if repeated Redis subscription setup
  is the main cost.

## Separate SSE diagnostic

The SSE diagnostic compares the current 100 ms wait loop with a persistent wait
mode on the same one Chronicle and one Redis topology. The persistent mode opens
one confirmed Redis notification subscription for the lifetime of an SSE
connection. It still rereads after every wake and once per second, so a missed
notification cannot leave a reader stuck.

These results are labeled as a code diagnostic. They are not mixed into the
topology only table.

## Results

Both approved campaigns completed on 2026-07-29 UTC.

- [Topology report](../results/20260728T210619Z-f3226954/scaling-report.md)
- [Persistent SSE diagnostic](../results/20260728T210646Z-7a6173cb/scaling-report.md)
- [All-system comparison](../results/20260727T122510Z-bd85274b/report.md)

No fixed-budget topology met every Rust WAL or Ursula disk gate. The original
one Chronicle and one Redis configuration was the closest overall, with 10 of
59 checks passing. There was no universal topology winner.

One Chronicle plus three Redis masters was best for write saturation,
many-stream SSE, balanced catchup, and high-rate mixed delivery. Four Chronicle
replicas plus three Redis masters was best for the mixed write and reader burst.
More Chronicle replicas generally hurt the simpler write and SSE cells when
they divided the same total CPU.

Persistent SSE waiting improved one-stream fanout from 11.4k to 29.8k events/s
and reduced p99 from 377 ms to 79 ms. On 100 streams and 2,048 clients, it
improved throughput from 13.8k to 25.4k events/s and reduced p99 from 343 ms to
165 ms. It still did not meet the direct reference gates, and its peak working
set was higher.

The six topology suites used 4.84 cluster-hours. The two SSE diagnostic suites
used 0.49 cluster-hours. All eight suites returned zero, both evidence seals
verify, and every exact-name cluster teardown proof reports the cluster absent.

## Commands

Resolve the topology suites:

```bash
python3 benchmarks/ds-bench/dsbench.py resolve-scaling \
  --output-dir .tmp/ds-bench/scaling-resolved
```

Resolve the separate SSE diagnostic:

```bash
python3 benchmarks/ds-bench/dsbench.py resolve-sse-diagnostic \
  --output-dir .tmp/ds-bench/sse-diagnostic-resolved
```

Build Chronicle and the patched client while reusing the exact reference image
digests from the sealed comparison archive:

```bash
python3 benchmarks/ds-bench/dsbench.py build \
  --target remote \
  --output .tmp/ds-bench/scaling-images.json \
  --reuse-reference-suts-from-archive \
    docs/benchmarks/ds-bench/results/20260728T122629Z-59260e88
```

After building digest pinned remote images, freeze the paid manifests:

```bash
python3 benchmarks/ds-bench/dsbench.py manifest-scaling \
  .tmp/ds-bench/scaling-resolved \
  --images .tmp/ds-bench/images.json \
  --output .tmp/ds-bench/scaling-manifest.json

python3 benchmarks/ds-bench/dsbench.py manifest-sse-diagnostic \
  .tmp/ds-bench/sse-diagnostic-resolved \
  --images .tmp/ds-bench/images.json \
  --output .tmp/ds-bench/sse-diagnostic-manifest.json
```

Paid execution needs separate approval. Once approved, run each manifest
through the existing sequential campaign command. The command creates one
cluster per suite, archives raw evidence, and proves that every cluster was
deleted.

Render the direct comparison:

```bash
python3 benchmarks/ds-bench/dsbench.py report-scaling \
  docs/benchmarks/ds-bench/results/SCALING_CAMPAIGN
```

The command writes `scaling-report.md` and `scaling-evaluation.json` in the
archive. The report names the minimal qualifying configuration. If none
qualifies, it names the closest configuration and lists every failed gate.

## Downside

This screen can show process scaling and software sharding on one machine. It
cannot show the benefit of more disks, more server nodes, Redis replicas, or
high availability. Global Redis Pub/Sub can also add cross cluster work, so
three masters may make SSE fanout slower even when they improve multi stream
reads and writes.
