# Chronicle configuration scaling report

No evaluated configuration met every gate. Closest candidate: `chronicle-c1-r1-cpu16`.

| configuration | Chronicle replicas | Redis masters | checks passed | worst throughput ratio | qualifies | dominated by |
|---|---:|---:|---:|---:|---|---|
| chronicle-c1-r1-cpu16 | 1 | 1 | 6/32 | 2.1% | no | none |

## Direct cell comparison

| configuration | workload | streams and level | throughput observed / target | latency observed / limit | memory observed / limit | cell passes |
|---|---|---|---|---|---|---|
| chronicle-c1-r1-cpu16 | write | 100000 | throughput=31.0k/64.3k (48.2%) | p50_ms=8.3/48.5<br>p99_ms=11.2/373.2 | 563.0/1.6k MiB | no |
| chronicle-c1-r1-cpu16 | blog-sse | 1 / 1000 | ops_per_sec=31.4k/50.1k (62.7%) | p50=60.5/6.2<br>p99=72.6/9.3 | 350.5/43.6 MiB | no |
| chronicle-c1-r1-cpu16 | reads-sse | 100 / 2048 | ops_per_sec=25.7k/102.5k (25.1%) | p50=131.1/1.4<br>p99=174.3/3.3 | 651.3/79.2 MiB | no |
| chronicle-c1-r1-cpu16 | reads-catchup | 100 / 512 | bytes_per_sec=60.40M/2892.39M (2.1%)<br>ops_per_sec=3.6/172.4 (2.1%) | p50=29.9k/6.2k<br>p99=34.1k/9.2k | 5.0k/16.4k MiB | no |
| chronicle-c1-r1-cpu16 | mixed-writes | 10000 / 100000 | write_ops_per_sec=7.0k/50.0k (14.1%)<br>read_mib_per_sec=93.9/303.3 (31.0%) | write_p50=26.1k/8.7<br>write_p99=52.4k/6.9k<br>read_p50=1.2k/3.1<br>read_p99=21.0k/2.5k | 2.5k/2.6k MiB | no |

## Failed gates

### chronicle-c1-r1-cpu16

- write, 100000 streams: throughput_ratio=0.4820<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.627635<0.98; ops_per_sec_ratio=0.6269<0.8000; p50=60.479>limit=6.1700; p99=72.575>limit=9.2780; memory=350.45703125>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.2512805989583333<0.98; ops_per_sec_ratio=0.2509<0.8000; p50=131.071>limit=1.4060; p99=174.335>limit=3.3320; memory=651.28125>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0209<0.8000; ops_per_sec_ratio=0.0209<0.8000; p50=29900.799>limit=6221.8220; p99=34144.255>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.140854<0.98; write_ops_per_sec_ratio=0.1408<0.8000; read_mib_per_sec_ratio=0.3096<0.8000; write_p50=26099.711>limit=8.6940; write_p99=52396.031>limit=6946.8140; read_p50=1198.079>limit=3.1080; read_p99=21037.055>limit=2527.2300

## Downside

All Redis masters share one server node and one local SSD. This report measures software sharding and process scaling, not added disks, machines, replication, or availability.
