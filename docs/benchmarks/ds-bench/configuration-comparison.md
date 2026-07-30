# Chronicle configuration and cross system comparison

## Result

No Chronicle configuration came close to Rust WAL or Ursula disk across every
benchmark. The original configuration with one Chronicle replica and one Redis
master passed the most comparison checks, at 10 of 59. More Redis masters helped
workloads spread across many stream keys. More Chronicle replicas helped the
mixed request burst, but they usually hurt write saturation and SSE fanout when
they divided the same CPU.

Increasing the single Chronicle and Redis pair from 4 to 8 and then 16 total
vCPUs did not raise write, SSE, or catchup throughput. The 16 vCPU pair improved
the mixed workload to 7.0k writes/s and 93.9 read MiB/s, but it still completed
only 14 percent of the offered writes and recorded 1,076 errors.

The practical choices are:

- Use one Chronicle replica and one Redis master as the default.
- Use one Chronicle replica and three Redis masters for workloads spread across
  many stream keys.
- Use four Chronicle replicas and three Redis masters only when a mixed read and
  write burst resembles the tested workload.
- Do not use a larger single Chronicle and Redis pair as the general scaling
  strategy. It only helped the mixed workload.
- Continue the persistent SSE work. It improved SSE more than adding replicas,
  but it increased memory and Redis connection use.

Catchup remains the largest gap. The strongest Chronicle configuration replayed
about 110 MiB/s. Rust WAL, Node memory, and Ursula disk replayed about 2,600 to
2,800 MiB/s.

## Production readiness

Chronicle is suitable for a controlled pilot, but it is not ready as a general
production replacement for Rust WAL. A pilot must stay within the measured
traffic limits and accept the documented Redis failover risk.

| Area | Evidence | Assessment |
| --- | --- | --- |
| Protocol behavior | Chronicle passes all 332 pinned protocol conformance tests. Within a healthy Redis primary, one Lua script validates and commits each append atomically. | Suitable for a pilot. |
| Performance | Write saturation reached 33.8k writes/s. The largest SSE and catchup cells overloaded, and catchup reached about 4 percent of the durable reference. | Not ready for high fanout or bulk replay workloads. |
| Durability and availability | Redis replication is asynchronous, so failover can lose acknowledged writes. The local failover logic and Redis promotion mechanics passed, but the managed Redis failover test under load is still pending. | Not ready for strict high availability or zero loss requirements. |
| Security | Chronicle supports enforced bearer, OIDC, and service identity checks. The default auth mode records decisions without enforcing them, and Chronicle expects a proxy to terminate TLS. | Ready only when operators enable enforcement, configure trusted identities, and require TLS. |
| Operations | Chronicle exposes health, readiness, and Prometheus metrics. These benchmark runs did not test rolling upgrades, backup restoration, or a long production soak. | The target deployment still needs those drills before launch. |

I would approve a limited pilot for an internal workload that can tolerate the
measured latency and Redis recovery window. I would not approve Chronicle yet
for critical durable data, strict high availability, high fanout SSE, or bulk
replay. Persistent SSE is still an opt in diagnostic and should remain off until
its Redis connection cost has been tested at the intended scale.

The [deployment guide](../../DEPLOYMENT.md) defines the Redis, durability, and
proxy requirements. The [testing guide](../../TESTING.md) separates proven,
sampled, and unprovable properties. The
[failover report](../../../loadtest/RESULTS-gate5-failover.md) records the
managed test that remains pending. The pinned protocol result is in
[`SPEC_VERSION.md`](../../../SPEC_VERSION.md).

## Test scope

The equal-compute campaigns gave every system 4 vCPUs, 16 GiB of memory, one
server node, and one local NVMe disk. Chronicle and Redis shared this budget.
Adding Chronicle replicas or Redis masters divided the existing CPU and memory.
All Redis masters also shared the same disk.

The durable comparisons are Rust WAL and Ursula disk. The Node reference server
only has a memory configuration in this campaign, so its numbers are a useful
performance reference but not a durability equivalent.

`c1-r3` means one Chronicle replica and three Redis masters. The persistent SSE
rows come from a separate diagnostic on `c1-r1`. We did not combine persistent
SSE with Redis sharding.

The vertical scaling runs kept one Chronicle replica, one Redis master, 16 GiB
of total memory, and one local NVMe disk. The 4 vCPU pair assigned 2 vCPUs to
each process. The 8 vCPU pair assigned 4 vCPUs to each process. The 16 vCPU pair
assigned 8 vCPUs to each process. Memory stayed at 4 GiB for Chronicle and
12 GiB for Redis. Persistent SSE was enabled in the 8 and 16 vCPU runs.

The 8 and 16 vCPU runs are not equal-compute comparisons with Rust, Node, or
Ursula. Those reference results used 4 vCPUs. The larger runs answer whether a
single Chronicle and Redis pair scales vertically. They do not show Chronicle
matching the references under the same resources.

These tests did not cover replication, failover, recovery, or separate disks.
Three Redis masters on one node measures software sharding, not a production
Redis cluster.

## Vertical CPU scaling

| Workload | 4 vCPU `c1-r1` | 8 vCPU `c1-r1` | 16 vCPU `c1-r1` | Result |
| --- | ---: | ---: | ---: | --- |
| Write saturation, 100,000 streams | 31.5k writes/s, 15.2 ms p99 | 30.5k writes/s, 15.3 ms p99 | 31.0k writes/s, 11.2 ms p99 | Throughput stayed flat. |
| One stream SSE, 1,000 subscribers | 29.8k events/s, 79.0 ms p99 | 31.7k events/s, 80.3 ms p99 | 31.4k events/s, 72.6 ms p99 | Throughput changed by less than 7 percent. |
| SSE, 100 streams and 2,048 connections | 25.4k events/s, 164.6 ms p99 | 27.3k events/s, 178.8 ms p99 | 25.7k events/s, 174.3 ms p99 | Throughput and latency stayed flat. |
| Catchup, 100 streams and 512 readers | 58.7 MiB/s, 34.0 s p99 | 59.7 MiB/s, 33.3 s p99 | 57.6 MiB/s, 34.1 s p99 | Throughput and latency stayed flat. |
| Mixed writes with 100,000 readers | 3.3k writes/s, 60.5 read MiB/s | 6.1k writes/s, 86.1 read MiB/s | 7.0k writes/s, 93.9 read MiB/s | The 16 vCPU pair beat the 8 vCPU pair by 15 percent on writes and 9 percent on reads. All three cells overloaded. |

The 4 vCPU SSE values use the persistent SSE diagnostic. The other 4 vCPU
values use the fixed-budget topology campaign. The original all-system campaign
measured 5.6k writes/s and 82.5 read MiB/s for the same 4 vCPU mixed cell, so
the apparent mixed gain over 4 vCPUs includes run-to-run variation. The 8 to
16 vCPU comparison used the same harness and gives the more reliable scale
increment.

The larger runs increased Chronicle and Redis CPU together, so they do not
isolate which process limited each workload. They also did not add Redis
masters, disks, or memory. The flat write, SSE, and catchup results rule out a
general benefit from buying a larger single node under this topology.

## Chronicle configuration results

| Configuration | Strongest result | Limitation |
| --- | --- | --- |
| `c1-r1` | Closest overall at 10 of 59 checks. It reached 31.5k writes/s, 14.1k events/s in the 100 stream SSE cell, and 58.7 MiB/s catchup. | It reached only 2.1 percent of the catchup reference and slowed sharply in the mixed workload. |
| `c2-r1` | One stream SSE p99 fell from 345 ms to 305 ms. | Throughput changed little or fell. The evaluator marked this configuration as dominated by `c1-r1`. |
| `c4-r1` | Mixed writes rose from 3.3k to 8.5k writes/s, while read bandwidth rose from 60.5 to 100.5 MiB/s. | Write saturation fell to 27.5k writes/s, and one stream SSE fell to 7.7k events/s. |
| `c1-r3` | This was the strongest general configuration for work spread across many keys. It reached 33.8k writes/s, 16.8k events/s in the 100 stream SSE cell, 106.7 MiB/s catchup, and about 9.0k events/s in high rate mixed delivery. | One stream SSE fell to 7.8k events/s because one hot stream stayed on one Redis master. |
| `c2-r3` | It reached 108.8 MiB/s catchup and the highest low rate mixed delivery result. | It did not beat `c1-r3` on write saturation, many stream SSE, or high rate delivery. |
| `c4-r3` | It produced the strongest mixed read and write result at 9.3k writes/s and 104.0 MiB/s, with zero request errors. It also reached 109.9 MiB/s catchup. | Write saturation fell to 26.7k writes/s, and many stream SSE fell to 13.3k events/s. |
| `c1-r1` with persistent SSE | One stream fanout rose from 11.4k to 29.8k events/s. The 100 stream cell rose from 13.8k to 25.4k events/s. | Peak memory rose from 249 to 376 MiB in the one stream cell and from 503 to 644 MiB in the 100 stream cell. Each SSE connection held one Redis notification subscription. |

No row passed all gates. A passing cell required at least 80 percent of the
better Rust WAL or Ursula disk throughput, at least 98 percent completion, zero
errors, latency within twice the reference, and memory within the declared
limit.

## Comparison with Rust, Node, and Ursula

| Workload | 16 vCPU `c1-r1` | Strongest Chronicle result | Rust WAL | Node memory | Ursula disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| Write saturation, 100,000 streams | 31.0k writes/s | 33.8k writes/s on 4 vCPU `c1-r3` | At least 64.3k writes/s | 50.9k writes/s | 4.9k writes/s |
| One stream SSE, 1,000 subscribers | 31.4k events/s, overloaded | 31.7k events/s on 8 vCPU `c1-r1`, overloaded | 50.1k events/s, complete | 42.1k events/s, 84 percent complete | 50.0k events/s, complete |
| SSE, 100 streams and 2,048 connections | 25.7k events/s, 25 percent complete | 27.3k events/s on 8 vCPU `c1-r1`, 27 percent complete | 102.5k events/s, complete | 37.6k events/s, 37 percent complete | 80.1k events/s, 78 percent complete |
| Catchup, 100 streams and 512 readers | 57.6 MiB/s, overloaded | 109.9 MiB/s on 4 vCPU `c4-r3`, overloaded | About 2,800 MiB/s | About 2,600 MiB/s | About 2,700 MiB/s |
| Mixed writes with 100,000 readers | 7.0k writes/s and 93.9 read MiB/s, 1,076 errors | 9.3k writes/s and 104.0 read MiB/s on 4 vCPU `c4-r3`, zero errors but incomplete | 50.0k writes/s and 303.3 read MiB/s, complete | 23.8k writes/s and 175.4 read MiB/s, 12 errors | 1.4k writes/s and 55.3 read MiB/s, 5,882 errors |

Chronicle was competitive with Ursula disk on writes. It was about seven times
faster in the 100,000-stream write test, and `c4-r3` also beat Ursula disk in the
mixed write test. Ursula disk was much stronger on SSE and catchup.

Giving `c1-r1` four times the reference CPU did not change that ordering. The
16 vCPU pair reached 48 percent of Rust WAL write throughput, 63 percent of its
one stream SSE rate, 25 percent of its 100 stream SSE rate, 2 percent of its
catchup bandwidth, and 14 percent of its mixed write rate.

Rust WAL was the only durable configuration that stayed strong across every
workload. It sustained at least 64.3k writes/s, completed the full SSE rates, and
replayed about 2.8 GiB/s in the 100 stream catchup cell.

Node memory usually sat between Chronicle and Rust. It beat every Chronicle
configuration on SSE and catchup, but it overloaded at the largest SSE and mixed
write levels. Its memory storage also makes it a weaker durability comparison.

At the clean mixed delivery ceiling, Rust WAL sustained 66k offered writes/s.
Chronicle and Ursula disk sustained 4k writes/s. Node also completed the 4k
level, but its unthrottled run recorded errors. Redis sharding raised Chronicle's
observed delivery at higher offered rates to about 9k events/s, but those cells
still failed completion and latency gates.

## Recommendation

Keep `c1-r1` as the minimum starting configuration unless measurements from a
specific workload justify a change. A larger single `c1-r1` node is not a useful
general upgrade. Choose `c1-r3` when traffic covers many independent stream
keys. Choose `c4-r3` only for request heavy mixed traffic and only after
accepting its higher memory use and weaker simple write performance.

Persistent SSE is the most useful next optimization. It does not solve catchup,
and one Redis subscription per SSE connection may become its next limit.
Catchup needs a change to the read and storage path rather than more Chronicle
replicas or more CPU for one Redis master.

## Source reports

- [Report from the sealed all-system comparison](results/20260727T122510Z-bd85274b/report.md)
- [Report from the sealed Chronicle topology comparison](results/20260728T210619Z-f3226954/scaling-report.md)
- [Report from the sealed persistent SSE diagnostic](results/20260728T210646Z-7a6173cb/scaling-report.md)
- [Report from the sealed 8 vCPU vertical scaling run](results/20260729T134759Z-ec2c6c55/scaling-report.md)
- [Report from the sealed 16 vCPU vertical scaling run](results/20260729T151520Z-675c412d/scaling-report.md)
