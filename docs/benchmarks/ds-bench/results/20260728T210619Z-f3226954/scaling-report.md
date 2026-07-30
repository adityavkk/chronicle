# Chronicle topology scaling report

No fixed-budget configuration met every gate. Closest candidate: `chronicle-c1-r1`.

| configuration | Chronicle replicas | Redis masters | checks passed | worst throughput ratio | qualifies | dominated by |
|---|---:|---:|---:|---:|---|---|
| chronicle-c1-r1 | 1 | 1 | 10/59 | 2.1% | no | none |
| chronicle-c2-r1 | 2 | 1 | 9/59 | 2.1% | no | chronicle-c1-r1 |
| chronicle-c4-r1 | 4 | 1 | 9/59 | 2.2% | no | none |
| chronicle-c1-r3 | 1 | 3 | 9/59 | 3.9% | no | none |
| chronicle-c2-r3 | 2 | 3 | 9/59 | 3.9% | no | none |
| chronicle-c4-r3 | 4 | 3 | 9/59 | 4.0% | no | none |

## Direct cell comparison

| configuration | workload | streams and level | throughput observed / target | latency observed / limit | memory observed / limit | cell passes |
|---|---|---|---|---|---|---|
| chronicle-c1-r1 | write | 100000 | throughput=31.5k/64.3k (49.1%) | p50_ms=7.8/48.5<br>p99_ms=15.2/373.2 | 562.0/1.6k MiB | no |
| chronicle-c1-r1 | blog-sse | 1 / 1000 | ops_per_sec=11.1k/50.1k (22.1%) | p50=147.3/6.2<br>p99=344.6/9.3 | 240.6/43.6 MiB | no |
| chronicle-c1-r1 | reads-sse | 100 / 2048 | ops_per_sec=14.1k/102.5k (13.8%) | p50=265.2/1.4<br>p99=334.8/3.3 | 465.0/79.2 MiB | no |
| chronicle-c1-r1 | reads-catchup | 100 / 512 | bytes_per_sec=61.52M/2892.39M (2.1%)<br>ops_per_sec=3.7/172.4 (2.1%) | p50=28.7k/6.2k<br>p99=34.0k/9.2k | 4.8k/16.4k MiB | no |
| chronicle-c1-r1 | mixed-writes | 10000 / 100000 | write_ops_per_sec=3.3k/50.0k (6.6%)<br>read_mib_per_sec=60.5/303.3 (20.0%) | write_p50=12.3k/8.7<br>write_p99=60.0k/6.9k<br>read_p50=10.1k/3.1<br>read_p99=37.2k/2.5k | 2.4k/2.6k MiB | no |
| chronicle-c1-r1 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (100.0%)<br>events_per_sec=3.6k/3.3k (108.3%) | write_p50=193.7/2.8<br>write_p99=1.3k/219.5<br>delivery_p50=333.1/0.8<br>delivery_p99=1.5k/261.8 | 768.5/125.3 MiB | no |
| chronicle-c1-r1 | mixed-delivery | 2000 / 8 | write_ops_per_sec=9.0k/16.1k (55.8%)<br>events_per_sec=6.7k/16.0k (42.1%) | write_p50=7.0k/3.1<br>write_p99=13.9k/107.1<br>delivery_p50=454.1/1.0<br>delivery_p99=665.1/101.5 | 877.6/163.5 MiB | no |
| chronicle-c1-r1 | mixed-delivery | 2000 / 33 | write_ops_per_sec=10.9k/65.9k (16.5%)<br>events_per_sec=5.4k/65.7k (8.3%) | write_p50=16.7k/20.2<br>write_p99=25.0k/614.4<br>delivery_p50=456.4/19.3<br>delivery_p99=1.3k/184.2 | 889.6/230.6 MiB | no |
| chronicle-c2-r1 | write | 100000 | throughput=30.5k/64.3k (47.4%) | p50_ms=7.8/48.5<br>p99_ms=24.6/373.2 | 610.0/1.6k MiB | no |
| chronicle-c2-r1 | blog-sse | 1 / 1000 | ops_per_sec=10.8k/50.1k (21.5%) | p50=138.2/6.2<br>p99=304.9/9.3 | 263.4/43.6 MiB | no |
| chronicle-c2-r1 | reads-sse | 100 / 2048 | ops_per_sec=11.4k/102.5k (11.1%) | p50=295.9/1.4<br>p99=447.7/3.3 | 504.7/79.2 MiB | no |
| chronicle-c2-r1 | reads-catchup | 100 / 512 | bytes_per_sec=61.52M/2892.39M (2.1%)<br>ops_per_sec=3.7/172.4 (2.1%) | p50=33.4k/6.2k<br>p99=38.5k/9.2k | 5.0k/16.4k MiB | no |
| chronicle-c2-r1 | mixed-writes | 10000 / 100000 | write_ops_per_sec=6.9k/50.0k (13.7%)<br>read_mib_per_sec=91.1/303.3 (30.0%) | write_p50=21.8k/8.7<br>write_p99=52.7k/6.9k<br>read_p50=1.4k/3.1<br>read_p99=19.7k/2.5k | 2.9k/2.6k MiB | no |
| chronicle-c2-r1 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (100.0%)<br>events_per_sec=3.2k/3.3k (96.1%) | write_p50=202.5/2.8<br>write_p99=1.3k/219.5<br>delivery_p50=351.2/0.8<br>delivery_p99=1.5k/261.8 | 801.2/125.3 MiB | no |
| chronicle-c2-r1 | mixed-delivery | 2000 / 8 | write_ops_per_sec=9.5k/16.1k (59.4%)<br>events_per_sec=6.3k/16.0k (39.2%) | write_p50=7.0k/3.1<br>write_p99=13.2k/107.1<br>delivery_p50=471.0/1.0<br>delivery_p99=685.1/101.5 | 927.8/163.5 MiB | no |
| chronicle-c2-r1 | mixed-delivery | 2000 / 33 | write_ops_per_sec=9.5k/65.9k (14.4%)<br>events_per_sec=6.3k/65.7k (9.7%) | write_p50=13.8k/20.2<br>write_p99=25.6k/614.4<br>delivery_p50=471.0/19.3<br>delivery_p99=677.9/184.2 | 885.1/230.6 MiB | no |
| chronicle-c4-r1 | write | 100000 | throughput=27.5k/64.3k (42.8%) | p50_ms=6.1/48.5<br>p99_ms=58.1/373.2 | 532.0/1.6k MiB | no |
| chronicle-c4-r1 | blog-sse | 1 / 1000 | ops_per_sec=7.7k/50.1k (15.4%) | p50=197.5/6.2<br>p99=386.0/9.3 | 325.2/43.6 MiB | no |
| chronicle-c4-r1 | reads-sse | 100 / 2048 | ops_per_sec=14.0k/102.5k (13.6%) | p50=227.3/1.4<br>p99=388.4/3.3 | 520.0/79.2 MiB | no |
| chronicle-c4-r1 | reads-catchup | 100 / 512 | bytes_per_sec=63.75M/2892.39M (2.2%)<br>ops_per_sec=3.8/172.4 (2.2%) | p50=29.6k/6.2k<br>p99=36.3k/9.2k | 5.2k/16.4k MiB | no |
| chronicle-c4-r1 | mixed-writes | 10000 / 100000 | write_ops_per_sec=8.5k/50.0k (17.0%)<br>read_mib_per_sec=100.5/303.3 (33.1%) | write_p50=22.3k/8.7<br>write_p99=52.7k/6.9k<br>read_p50=731.6/3.1<br>read_p99=20.2k/2.5k | 3.2k/2.6k MiB | no |
| chronicle-c4-r1 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (99.9%)<br>events_per_sec=3.7k/3.3k (109.8%) | write_p50=219.6/2.8<br>write_p99=1.3k/219.5<br>delivery_p50=371.7/0.8<br>delivery_p99=1.5k/261.8 | 907.5/125.3 MiB | no |
| chronicle-c4-r1 | mixed-delivery | 2000 / 8 | write_ops_per_sec=11.6k/16.1k (72.3%)<br>events_per_sec=4.5k/16.0k (28.0%) | write_p50=6.6k/3.1<br>write_p99=14.1k/107.1<br>delivery_p50=430.8/1.0<br>delivery_p99=2.2k/101.5 | 907.3/163.5 MiB | no |
| chronicle-c4-r1 | mixed-delivery | 2000 / 33 | write_ops_per_sec=9.2k/65.9k (14.0%)<br>events_per_sec=5.9k/65.7k (8.9%) | write_p50=13.9k/20.2<br>write_p99=25.8k/614.4<br>delivery_p50=485.9/19.3<br>delivery_p99=762.4/184.2 | 941.5/230.6 MiB | no |
| chronicle-c1-r3 | write | 100000 | throughput=33.8k/64.3k (52.6%) | p50_ms=6.8/48.5<br>p99_ms=12.9/373.2 | 677.0/1.6k MiB | no |
| chronicle-c1-r3 | blog-sse | 1 / 1000 | ops_per_sec=7.8k/50.1k (15.6%) | p50=177.7/6.2<br>p99=490.2/9.3 | 258.8/43.6 MiB | no |
| chronicle-c1-r3 | reads-sse | 100 / 2048 | ops_per_sec=16.8k/102.5k (16.4%) | p50=238.0/1.4<br>p99=287.0/3.3 | 412.1/79.2 MiB | no |
| chronicle-c1-r3 | reads-catchup | 100 / 512 | bytes_per_sec=111.85M/2892.39M (3.9%)<br>ops_per_sec=6.7/172.4 (3.9%) | p50=24.5k/6.2k<br>p99=28.5k/9.2k | 6.3k/16.4k MiB | no |
| chronicle-c1-r3 | mixed-writes | 10000 / 100000 | write_ops_per_sec=5.8k/50.0k (11.7%)<br>read_mib_per_sec=82.4/303.3 (27.2%) | write_p50=21.6k/8.7<br>write_p99=53.7k/6.9k<br>read_p50=1.3k/3.1<br>read_p99=20.6k/2.5k | 3.3k/2.6k MiB | no |
| chronicle-c1-r3 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (100.0%)<br>events_per_sec=3.3k/3.3k (98.1%) | write_p50=220.3/2.8<br>write_p99=1.3k/219.5<br>delivery_p50=375.6/0.8<br>delivery_p99=1.5k/261.8 | 814.8/125.3 MiB | no |
| chronicle-c1-r3 | mixed-delivery | 2000 / 8 | write_ops_per_sec=9.5k/16.1k (59.0%)<br>events_per_sec=9.0k/16.0k (56.6%) | write_p50=6.6k/3.1<br>write_p99=12.4k/107.1<br>delivery_p50=388.9/1.0<br>delivery_p99=531.5/101.5 | 858.5/163.5 MiB | no |
| chronicle-c1-r3 | mixed-delivery | 2000 / 33 | write_ops_per_sec=9.5k/65.9k (14.3%)<br>events_per_sec=9.0k/65.7k (13.7%) | write_p50=13.2k/20.2<br>write_p99=25.6k/614.4<br>delivery_p50=382.7/19.3<br>delivery_p99=520.4/184.2 | 846.2/230.6 MiB | no |
| chronicle-c2-r3 | write | 100000 | throughput=28.7k/64.3k (44.6%) | p50_ms=4.4/48.5<br>p99_ms=57.7/373.2 | 656.0/1.6k MiB | no |
| chronicle-c2-r3 | blog-sse | 1 / 1000 | ops_per_sec=8.6k/50.1k (17.2%) | p50=190.3/6.2<br>p99=372.7/9.3 | 320.9/43.6 MiB | no |
| chronicle-c2-r3 | reads-sse | 100 / 2048 | ops_per_sec=14.2k/102.5k (13.9%) | p50=247.9/1.4<br>p99=343.8/3.3 | 434.4/79.2 MiB | no |
| chronicle-c2-r3 | reads-catchup | 100 / 512 | bytes_per_sec=114.09M/2892.39M (3.9%)<br>ops_per_sec=6.8/172.4 (3.9%) | p50=26.8k/6.2k<br>p99=33.2k/9.2k | 5.8k/16.4k MiB | no |
| chronicle-c2-r3 | mixed-writes | 10000 / 100000 | write_ops_per_sec=8.2k/50.0k (16.4%)<br>read_mib_per_sec=99.2/303.3 (32.7%) | write_p50=24.1k/8.7<br>write_p99=50.6k/6.9k<br>read_p50=1.2k/3.1<br>read_p99=16.2k/2.5k | 3.9k/2.6k MiB | no |
| chronicle-c2-r3 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (100.0%)<br>events_per_sec=3.9k/3.3k (117.8%) | write_p50=230.7/2.8<br>write_p99=1.2k/219.5<br>delivery_p50=370.2/0.8<br>delivery_p99=820.2/261.8 | 973.7/125.3 MiB | no |
| chronicle-c2-r3 | mixed-delivery | 2000 / 8 | write_ops_per_sec=11.3k/16.1k (70.3%)<br>events_per_sec=6.9k/16.0k (43.5%) | write_p50=8.4k/3.1<br>write_p99=11.0k/107.1<br>delivery_p50=398.1/1.0<br>delivery_p99=570.4/101.5 | 892.2/163.5 MiB | no |
| chronicle-c2-r3 | mixed-delivery | 2000 / 33 | write_ops_per_sec=9.1k/65.9k (13.9%)<br>events_per_sec=8.3k/65.7k (12.7%) | write_p50=13.3k/20.2<br>write_p99=25.8k/614.4<br>delivery_p50=393.5/19.3<br>delivery_p99=563.2/184.2 | 937.7/230.6 MiB | no |
| chronicle-c4-r3 | write | 100000 | throughput=26.7k/64.3k (41.6%) | p50_ms=2.5/48.5<br>p99_ms=78.5/373.2 | 632.0/1.6k MiB | no |
| chronicle-c4-r3 | blog-sse | 1 / 1000 | ops_per_sec=9.3k/50.1k (18.6%) | p50=122.1/6.2<br>p99=396.8/9.3 | 356.0/43.6 MiB | no |
| chronicle-c4-r3 | reads-sse | 100 / 2048 | ops_per_sec=13.3k/102.5k (13.0%) | p50=249.3/1.4<br>p99=383.7/3.3 | 495.0/79.2 MiB | no |
| chronicle-c4-r3 | reads-catchup | 100 / 512 | bytes_per_sec=115.20M/2892.39M (4.0%)<br>ops_per_sec=6.9/172.4 (4.0%) | p50=25.6k/6.2k<br>p99=31.0k/9.2k | 6.9k/16.4k MiB | no |
| chronicle-c4-r3 | mixed-writes | 10000 / 100000 | write_ops_per_sec=9.3k/50.0k (18.6%)<br>read_mib_per_sec=104.0/303.3 (34.3%) | write_p50=25.0k/8.7<br>write_p99=49.3k/6.9k<br>read_p50=1.0k/3.1<br>read_p99=7.8k/2.5k | 3.5k/2.6k MiB | no |
| chronicle-c4-r3 | mixed-delivery | 2000 / 2 | write_ops_per_sec=4.1k/4.1k (100.0%)<br>events_per_sec=3.9k/3.3k (117.1%) | write_p50=207.0/2.8<br>write_p99=1.2k/219.5<br>delivery_p50=344.1/0.8<br>delivery_p99=824.3/261.8 | 980.0/125.3 MiB | no |
| chronicle-c4-r3 | mixed-delivery | 2000 / 8 | write_ops_per_sec=8.8k/16.1k (54.5%)<br>events_per_sec=7.6k/16.0k (47.6%) | write_p50=7.2k/3.1<br>write_p99=13.9k/107.1<br>delivery_p50=412.9/1.0<br>delivery_p99=638.0/101.5 | 957.6/163.5 MiB | no |
| chronicle-c4-r3 | mixed-delivery | 2000 / 33 | write_ops_per_sec=8.5k/65.9k (12.9%)<br>events_per_sec=7.1k/65.7k (10.8%) | write_p50=13.5k/20.2<br>write_p99=26.2k/614.4<br>delivery_p50=443.4/19.3<br>delivery_p99=691.7/184.2 | 988.6/230.6 MiB | no |

## Failed gates

### chronicle-c1-r1

- write, 100000 streams: throughput_ratio=0.4909<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.221393<0.98; ops_per_sec_ratio=0.2211<0.8000; p50=147.327>limit=6.1700; p99=344.575>limit=9.2780; memory=240.62109375>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.1378203125<0.98; ops_per_sec_ratio=0.1376<0.8000; p50=265.215>limit=1.4060; p99=334.847>limit=3.3320; memory=465.01953125>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0213<0.8000; ops_per_sec_ratio=0.0213<0.8000; p50=28655.615>limit=6221.8220; p99=33980.415>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.06644833333333333<0.98; write_ops_per_sec_ratio=0.0664<0.8000; read_mib_per_sec_ratio=0.1995<0.8000; write_p50=12320.767>limit=8.6940; write_p99=60030.975>limit=6946.8140; read_p50=10141.695>limit=3.1080; read_p99=37224.447>limit=2527.2300
- mixed-delivery, 2000 streams, level 2: write_p50=193.663>limit=2.8360; write_p99=1281.023>limit=219.5180; delivery_p50=333.055>limit=0.8500; delivery_p99=1475.583>limit=261.7580; memory=768.46875>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.5599729166666667<0.98; write_ops_per_sec_ratio=0.5581<0.8000; events_per_sec_ratio=0.4207<0.8000; write_p50=6979.583>limit=3.0840; write_p99=13918.207>limit=107.0700; delivery_p50=454.143>limit=0.9980; delivery_p99=665.087>limit=101.5020; memory=877.58984375>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.1651181818181818<0.98; write_ops_per_sec_ratio=0.1653<0.8000; events_per_sec_ratio=0.0829<0.8000; write_p50=16728.063>limit=20.2220; write_p99=24985.599>limit=614.3980; delivery_p50=456.447>limit=19.3100; delivery_p99=1329.151>limit=184.1900; memory=889.6328125>limit=230.6MiB

### chronicle-c2-r1

- write, 100000 streams: throughput_ratio=0.4744<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.215247<0.98; ops_per_sec_ratio=0.2150<0.8000; p50=138.239>limit=6.1700; p99=304.895>limit=9.2780; memory=263.37109375>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.11132682291666668<0.98; ops_per_sec_ratio=0.1112<0.8000; p50=295.935>limit=1.4060; p99=447.743>limit=3.3320; memory=504.74609375>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0213<0.8000; ops_per_sec_ratio=0.0213<0.8000; p50=33357.823>limit=6221.8220; p99=38502.399>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.13740333333333335<0.98; write_ops_per_sec_ratio=0.1373<0.8000; read_mib_per_sec_ratio=0.3004<0.8000; write_p50=21790.719>limit=8.6940; write_p99=52723.711>limit=6946.8140; read_p50=1394.687>limit=3.1080; read_p99=19693.567>limit=2527.2300; memory=2870.25>limit=2557.8MiB
- mixed-delivery, 2000 streams, level 2: write_p50=202.495>limit=2.8360; write_p99=1321.983>limit=219.5180; delivery_p50=351.231>limit=0.8500; delivery_p99=1546.239>limit=261.7580; memory=801.171875>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.5958229166666666<0.98; write_ops_per_sec_ratio=0.5939<0.8000; events_per_sec_ratio=0.3920<0.8000; write_p50=7008.255>limit=3.0840; write_p99=13180.927>limit=107.0700; delivery_p50=471.039>limit=0.9980; delivery_p99=685.055>limit=101.5020; memory=927.80078125>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.1442580808080808<0.98; write_ops_per_sec_ratio=0.1445<0.8000; events_per_sec_ratio=0.0966<0.8000; write_p50=13803.519>limit=20.2220; write_p99=25608.191>limit=614.3980; delivery_p50=471.039>limit=19.3100; delivery_p99=677.887>limit=184.1900; memory=885.05078125>limit=230.6MiB

### chronicle-c4-r1

- write, 100000 streams: throughput_ratio=0.4278<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.154204<0.98; ops_per_sec_ratio=0.1540<0.8000; p50=197.503>limit=6.1700; p99=386.047>limit=9.2780; memory=325.18359375>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.13645703125<0.98; ops_per_sec_ratio=0.1363<0.8000; p50=227.327>limit=1.4060; p99=388.351>limit=3.3320; memory=519.9765625>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0220<0.8000; ops_per_sec_ratio=0.0220<0.8000; p50=29556.735>limit=6221.8220; p99=36306.943>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.1696983333333333<0.98; write_ops_per_sec_ratio=0.1696<0.8000; read_mib_per_sec_ratio=0.3314<0.8000; write_p50=22282.239>limit=8.6940; write_p99=52690.943>limit=6946.8140; read_p50=731.647>limit=3.1080; read_p99=20168.703>limit=2527.2300; memory=3175.5>limit=2557.8MiB
- mixed-delivery, 2000 streams, level 2: write_p50=219.647>limit=2.8360; write_p99=1279.999>limit=219.5180; delivery_p50=371.711>limit=0.8500; delivery_p99=1490.943>limit=261.7580; memory=907.5>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.7249708333333333<0.98; write_ops_per_sec_ratio=0.7226<0.8000; events_per_sec_ratio=0.2801<0.8000; write_p50=6574.079>limit=3.0840; write_p99=14057.471>limit=107.0700; delivery_p50=430.847>limit=0.9980; delivery_p99=2224.127>limit=101.5020; memory=907.28515625>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.13988535353535353<0.98; write_ops_per_sec_ratio=0.1401<0.8000; events_per_sec_ratio=0.0894<0.8000; write_p50=13893.631>limit=20.2220; write_p99=25772.031>limit=614.3980; delivery_p50=485.887>limit=19.3100; delivery_p99=762.367>limit=184.1900; memory=941.48046875>limit=230.6MiB

### chronicle-c1-r3

- write, 100000 streams: throughput_ratio=0.5265<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.15604700000000002<0.98; ops_per_sec_ratio=0.1559<0.8000; p50=177.663>limit=6.1700; p99=490.239>limit=9.2780; memory=258.76171875>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.16377864583333335<0.98; ops_per_sec_ratio=0.1636<0.8000; p50=237.951>limit=1.4060; p99=286.975>limit=3.3320; memory=412.09375>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0387<0.8000; ops_per_sec_ratio=0.0387<0.8000; p50=24494.079>limit=6221.8220; p99=28524.543>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.11667466666666668<0.98; write_ops_per_sec_ratio=0.1166<0.8000; read_mib_per_sec_ratio=0.2718<0.8000; write_p50=21643.263>limit=8.6940; write_p99=53706.751>limit=6946.8140; read_p50=1307.647>limit=3.1080; read_p99=20594.687>limit=2527.2300; memory=3260.73828125>limit=2557.8MiB
- mixed-delivery, 2000 streams, level 2: write_p50=220.287>limit=2.8360; write_p99=1277.951>limit=219.5180; delivery_p50=375.551>limit=0.8500; delivery_p99=1545.215>limit=261.7580; memory=814.83984375>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.5921145833333333<0.98; write_ops_per_sec_ratio=0.5902<0.8000; events_per_sec_ratio=0.5657<0.8000; write_p50=6586.367>limit=3.0840; write_p99=12378.111>limit=107.0700; delivery_p50=388.863>limit=0.9980; delivery_p99=531.455>limit=101.5020; memory=858.48046875>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.1432969696969697<0.98; write_ops_per_sec_ratio=0.1435<0.8000; events_per_sec_ratio=0.1370<0.8000; write_p50=13230.079>limit=20.2220; write_p99=25624.575>limit=614.3980; delivery_p50=382.719>limit=19.3100; delivery_p99=520.447>limit=184.1900; memory=846.171875>limit=230.6MiB

### chronicle-c2-r3

- write, 100000 streams: throughput_ratio=0.4463<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.17217200000000002<0.98; ops_per_sec_ratio=0.1720<0.8000; p50=190.335>limit=6.1700; p99=372.735>limit=9.2780; memory=320.9453125>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.13914518229166667<0.98; ops_per_sec_ratio=0.1390<0.8000; p50=247.935>limit=1.4060; p99=343.807>limit=3.3320; memory=434.4140625>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0394<0.8000; ops_per_sec_ratio=0.0394<0.8000; p50=26755.071>limit=6221.8220; p99=33161.215>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.16422066666666665<0.98; write_ops_per_sec_ratio=0.1642<0.8000; read_mib_per_sec_ratio=0.3270<0.8000; write_p50=24100.863>limit=8.6940; write_p99=50593.791>limit=6946.8140; read_p50=1174.527>limit=3.1080; read_p99=16203.775>limit=2527.2300; memory=3913.2734375>limit=2557.8MiB
- mixed-delivery, 2000 streams, level 2: write_p50=230.655>limit=2.8360; write_p99=1223.679>limit=219.5180; delivery_p50=370.175>limit=0.8500; delivery_p99=820.223>limit=261.7580; memory=973.72265625>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.7052666666666666<0.98; write_ops_per_sec_ratio=0.7029<0.8000; events_per_sec_ratio=0.4346<0.8000; write_p50=8396.799>limit=3.0840; write_p99=11026.431>limit=107.0700; delivery_p50=398.079>limit=0.9980; delivery_p99=570.367>limit=101.5020; memory=892.15234375>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.1384090909090909<0.98; write_ops_per_sec_ratio=0.1386<0.8000; events_per_sec_ratio=0.1271<0.8000; write_p50=13262.847>limit=20.2220; write_p99=25804.799>limit=614.3980; delivery_p50=393.471>limit=19.3100; delivery_p99=563.199>limit=184.1900; memory=937.7265625>limit=230.6MiB

### chronicle-c4-r3

- write, 100000 streams: throughput_ratio=0.4161<0.8000
- blog-sse, 1 streams, level 1000: classification=overload; completion=0.185917<0.98; ops_per_sec_ratio=0.1857<0.8000; p50=122.111>limit=6.1700; p99=396.799>limit=9.2780; memory=355.96875>limit=43.6MiB
- reads-sse, 100 streams, level 2048: classification=overload; completion=0.130337890625<0.98; ops_per_sec_ratio=0.1302<0.8000; p50=249.343>limit=1.4060; p99=383.743>limit=3.3320; memory=494.96875>limit=79.2MiB
- reads-catchup, 100 streams, level 512: classification=overload; bytes_per_sec_ratio=0.0398<0.8000; ops_per_sec_ratio=0.0398<0.8000; p50=25624.575>limit=6221.8220; p99=31014.911>limit=9150.4620
- mixed-writes, 10000 streams, level 100000: classification=overload; completion=0.18615866666666664<0.98; write_ops_per_sec_ratio=0.1861<0.8000; read_mib_per_sec_ratio=0.3429<0.8000; write_p50=25018.367>limit=8.6940; write_p99=49283.071>limit=6946.8140; read_p50=1001.471>limit=3.1080; read_p99=7815.167>limit=2527.2300; memory=3491.94140625>limit=2557.8MiB
- mixed-delivery, 2000 streams, level 2: write_p50=206.975>limit=2.8360; write_p99=1223.679>limit=219.5180; delivery_p50=344.063>limit=0.8500; delivery_p99=824.319>limit=261.7580; memory=979.96484375>limit=125.3MiB
- mixed-delivery, 2000 streams, level 8: classification=overload; completion=0.5469895833333334<0.98; write_ops_per_sec_ratio=0.5452<0.8000; events_per_sec_ratio=0.4756<0.8000; write_p50=7245.823>limit=3.0840; write_p99=13869.055>limit=107.0700; delivery_p50=412.927>limit=0.9980; delivery_p99=637.951>limit=101.5020; memory=957.57421875>limit=163.5MiB
- mixed-delivery, 2000 streams, level 33: classification=overload; completion=0.1286459595959596<0.98; write_ops_per_sec_ratio=0.1288<0.8000; events_per_sec_ratio=0.1083<0.8000; write_p50=13492.223>limit=20.2220; write_p99=26165.247>limit=614.3980; delivery_p50=443.391>limit=19.3100; delivery_p99=691.711>limit=184.1900; memory=988.5546875>limit=230.6MiB

## Downside

All Redis masters share one server node and one local SSD. This report measures software sharding and process scaling, not added disks, machines, replication, or availability.
