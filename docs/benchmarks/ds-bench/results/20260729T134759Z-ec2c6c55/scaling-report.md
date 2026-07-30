# Chronicle configuration scaling report

No evaluated configuration met every gate. Closest candidate: `chronicle-c1-r1-cpu8`.

| configuration | Chronicle replicas | Redis masters | checks passed | worst throughput ratio | qualifies | dominated by |
|---|---:|---:|---:|---:|---|---|
| chronicle-c1-r1-cpu8 | 1 | 1 | 5/32 | 2.2% | no | none |

## Direct cell comparison

| configuration | workload | streams and level | throughput observed / target | latency observed / limit | memory observed / limit | cell passes |
|---|---|---|---|---|---|---|
| chronicle-c1-r1-cpu8 | write | 100000 | throughput=30.5k/64.3k (47.5%) | p50_ms=8.0/48.5<br>p99_ms=15.3/373.2 | 584.0/1.6k MiB | no |
| chronicle-c1-r1-cpu8 | blog-sse | 1 / 1000 | ops_per_sec=31.7k/50.1k (63.2%) | p50=60.0/6.2<br>p99=80.3/9.3 | 310.3/43.6 MiB | no |
| chronicle-c1-r1-cpu8 | reads-sse | 100 / 2048 | ops_per_sec=27.3k/102.5k (26.6%) | p50=122.4/1.4<br>p99=178.8/3.3 | 673.0/79.2 MiB | no |
| chronicle-c1-r1-cpu8 | reads-catchup | 100 / 512 | bytes_per_sec=62.63M/2892.39M (2.2%)<br>ops_per_sec=3.7/172.4 (2.2%) | p50=31.1k/6.2k<br>p99=33.3k/9.2k | 4.9k/16.4k MiB | no |
| chronicle-c1-r1-cpu8 | mixed-writes | 10000 / 100000 | write_ops_per_sec=6.1k/50.0k (12.2%)<br>read_mib_per_sec=86.1/303.3 (28.4%) | write_p50=22.7k/8.7<br>write_p99=54.0k/6.9k<br>read_p50=1.4k/3.1<br>read_p99=21.3k/2.5k | 2.6k/2.6k MiB | no |

## Failed gates

### chronicle-c1-r1-cpu8

- write, 100000 streams: throughput_ratio=0.4750<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.6330359999999999<0.98; ops_per_sec_ratio=0.6323<0.8000; p50=59.967>limit=6.1700; p99=80.319>limit=9.2780; memory=310.3203125>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.26624739583333334<0.98; ops_per_sec_ratio=0.2659<0.8000; p50=122.431>limit=1.4060; p99=178.815>limit=3.3320; memory=673.01171875>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0217<0.8000; ops_per_sec_ratio=0.0217<0.8000; p50=31064.063>limit=6221.8220; p99=33259.519>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.12205433333333332<0.98; write_ops_per_sec_ratio=0.1220<0.8000; read_mib_per_sec_ratio=0.2839<0.8000; write_p50=22708.223>limit=8.6940; write_p99=54001.663>limit=6946.8140; read_p50=1357.823>limit=3.1080; read_p99=21250.047>limit=2527.2300; memory=2584.0390625>limit=2557.8MiB

## Downside

All Redis masters share one server node and one local SSD. This report measures software sharding and process scaling, not added disks, machines, replication, or availability.
