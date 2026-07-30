# Chronicle topology scaling report

No fixed-budget configuration met every gate. Closest candidate: `chronicle-c1-r1-sse-persistent`.

| configuration | Chronicle replicas | Redis masters | checks passed | worst throughput ratio | qualifies | dominated by |
|---|---:|---:|---:|---:|---|---|
| chronicle-c1-r1-legacy | 1 | 1 | 0/12 | 13.4% | no | chronicle-c1-r1-sse-persistent |
| chronicle-c1-r1-sse-persistent | 1 | 1 | 0/12 | 24.7% | no | none |

## Direct cell comparison

| configuration | workload | streams and level | throughput observed / target | latency observed / limit | memory observed / limit | cell passes |
|---|---|---|---|---|---|---|
| chronicle-c1-r1-legacy | blog-sse | 1 / 1000 | ops_per_sec=11.4k/50.1k (22.8%) | p50=138.2/6.2<br>p99=377.1/9.3 | 249.4/43.6 MiB | no |
| chronicle-c1-r1-legacy | reads-sse | 100 / 2048 | ops_per_sec=13.8k/102.5k (13.4%) | p50=267.8/1.4<br>p99=343.0/3.3 | 503.3/79.2 MiB | no |
| chronicle-c1-r1-sse-persistent | blog-sse | 1 / 1000 | ops_per_sec=29.8k/50.1k (59.5%) | p50=62.2/6.2<br>p99=79.0/9.3 | 375.9/43.6 MiB | no |
| chronicle-c1-r1-sse-persistent | reads-sse | 100 / 2048 | ops_per_sec=25.4k/102.5k (24.7%) | p50=133.5/1.4<br>p99=164.6/3.3 | 643.9/79.2 MiB | no |

## Failed gates

### chronicle-c1-r1-legacy

- blog-sse, 1 streams, level 1000: classification=overload; completion=0.22794099999999998<0.98; ops_per_sec_ratio=0.2277<0.8000; p50=138.239>limit=6.1700; p99=377.087>limit=9.2780; memory=249.4453125>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.13430989583333333<0.98; ops_per_sec_ratio=0.1341<0.8000; p50=267.775>limit=1.4060; p99=343.039>limit=3.3320; memory=503.25390625>limit=79.2MiB

### chronicle-c1-r1-sse-persistent

- blog-sse, 1 streams, level 1000: classification=overload; completion=0.5961609999999999<0.98; ops_per_sec_ratio=0.5954<0.8000; p50=62.239>limit=6.1700; p99=78.975>limit=9.2780; memory=375.94140625>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.24777604166666664<0.98; ops_per_sec_ratio=0.2474<0.8000; p50=133.503>limit=1.4060; p99=164.607>limit=3.3320; memory=643.9453125>limit=79.2MiB

## Downside

All Redis masters share one server node and one local SSD. This report measures software sharding and process scaling, not added disks, machines, replication, or availability.
