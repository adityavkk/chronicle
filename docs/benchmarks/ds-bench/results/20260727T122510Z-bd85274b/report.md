# Chronicle ds-bench comparison: 20260727T122510Z-bd85274b

This report compares Chronicle with the official Rust durable-streams server, the Node reference server, and Ursula. Every result uses the same pinned upstream ds-bench commit and single-node 4 vCPU, 16 GiB, one-local-NVMe SUT budget. Corrected adapter revisions are recorded per archive. Chronicle and Redis share that budget.

Durability classes are reported separately. Redis AOF `everysec` and all memory arms are weaker than the local-WAL or Redis AOF `always` arms.

Corrected supplement archives replace the complete matching system and workload slice. They never fill individual missing cells from an older run.

## Executive findings

- At 100,000 streams, Chronicle AOF `always` reached 34.6k append/s. Rust WAL reached at least 64.3k append/s because its load ladder ended before a plateau. Ursula disk reached 4.9k append/s. Chronicle was no more than 0.54 times the Rust lower bound and 6.99 times Ursula disk.

- At 1,000 subscribers and 50 appends/s, Chronicle delivered 14.8% to 15.3% of the declared event rate. The detailed table keeps this as an overload observation rather than a completed 50,000-event/s cell.

- Every 512-reader catchup row passed exact seed validation. Chronicle replayed 56.5 to 61.9 MiB/s across its tested arms and stream cardinalities. Chronicle recovered 8.1k failed seed appends out of 3.61M attempts (0.225%) before measurement. Rust, Node, and Ursula recorded no failed seed appends.

- The combined dataset contains 73 valid overload observations and 0 invalid or missing rows. Overload is measured behavior. Invalid setup is not a performance result.

## Write saturation

| system | configuration | durability | streams | append/s | p50 ms | p99 ms | peak MiB | verdict |
|---|---|---|---:|---:|---:|---:|---:|---|
| rust | rust-wal | local-wal | 10000 | 62.7k | 3.5 | 28.8 | 101 | headline |
| rust | rust-wal | local-wal | 100000 | 64.3k | — | — | 802 | lower_bound (confirmation_reps=1/3, ladder_exhausted) |
| rust | rust-memory | memory | 10000 | 457.5k | 2.0 | 5.5 | 200 | headline |
| rust | rust-memory | memory | 100000 | 413.6k | 2.1 | 5.8 | 665 | headline |
| node | node-memory | memory | 10000 | 51.0k | 4.5 | 9.7 | 1.3k | headline |
| node | node-memory | memory | 100000 | 50.9k | 4.6 | 9.7 | 1.3k | headline |
| ursula | ursula-disk | local-wal | 10000 | 5.5k | 16.4 | 178.7 | 1.8k | headline |
| ursula | ursula-disk | local-wal | 100000 | 4.9k | 24.2 | 186.6 | 1.9k | headline |
| ursula | ursula-memory | memory | 10000 | 42.9k | 1.1 | 42.0 | 3.8k | headline |
| ursula | ursula-memory | memory | 100000 | 34.3k | 1.0 | 55.0 | 4.0k | headline |
| chronicle | chronicle-redis-aof-always | redis-aof-always | 10000 | 34.0k | 7.3 | 14.4 | 502 | headline |
| chronicle | chronicle-redis-aof-always | redis-aof-always | 100000 | 34.6k | 7.2 | 14.4 | 598 | headline |
| chronicle | chronicle-redis-aof-everysec | redis-aof-everysec | 10000 | 38.3k | 6.7 | 9.8 | 554 | headline |
| chronicle | chronicle-redis-aof-everysec | redis-aof-everysec | 100000 | 38.5k | 6.6 | 9.8 | 654 | headline |

`lower_bound` means the offered-load ladder ended before a plateau. It is not presented as a measured ceiling.

## Blog fanout at 1,000 subscribers

| system | configuration | delivered events/s | completion | p50 ms | p99 ms | verdict |
|---|---|---:|---:|---:|---:|---|
| chronicle | chronicle-redis-aof-always | 7.4k | 14.8% | 184.8 | 544.3 | overload (completion_ratio=0.1478<0.9800) |
| chronicle | chronicle-redis-aof-everysec | 7.7k | 15.3% | 178.4 | 495.4 | overload (completion_ratio=0.1534<0.9800) |
| node | node-memory | 42.1k | 84.2% | 39.6 | 56.6 | overload (completion_ratio=0.8422<0.9800) |
| rust | rust-memory | 50.0k | 100.1% | 2.2 | 3.9 | result |
| rust | rust-wal | 50.1k | 100.1% | 3.1 | 4.6 | result |
| ursula | ursula-disk | 50.0k | 100.1% | 3.3 | 5.2 | result |
| ursula | ursula-memory | 50.0k | 100.1% | 2.6 | 4.6 | result |

The declared offered rate is 50,000 event deliveries per second. Completion below 98 percent is labeled overload.

## SSE scale at 2,048 connections

| system | configuration | streams | delivered events/s | completion | p99 ms | verdict |
|---|---|---:|---:|---:|---:|---|
| chronicle | chronicle-redis-aof-always | 10 | 11.2k | 11.0% | 469.2 | overload (completion_ratio=0.1097<0.9800) |
| chronicle | chronicle-redis-aof-always | 100 | 10.8k | 10.5% | 462.6 | overload (completion_ratio=0.1051<0.9800) |
| chronicle | chronicle-redis-aof-everysec | 10 | 11.2k | 11.0% | 502.3 | overload (completion_ratio=0.1095<0.9800) |
| chronicle | chronicle-redis-aof-everysec | 100 | 11.2k | 10.9% | 454.1 | overload (completion_ratio=0.1093<0.9800) |
| node | node-memory | 10 | 41.8k | 40.9% | 57.8 | overload (completion_ratio=0.4087<0.9800) |
| node | node-memory | 100 | 37.6k | 36.7% | 60.2 | overload (completion_ratio=0.3675<0.9800) |
| rust | rust-memory | 10 | 102.5k | 100.1% | 1.7 | result |
| rust | rust-memory | 100 | 102.5k | 100.1% | 1.6 | result |
| rust | rust-wal | 10 | 102.5k | 100.1% | 3.6 | result |
| rust | rust-wal | 100 | 102.5k | 100.1% | 1.7 | result |
| ursula | ursula-disk | 10 | 102.5k | 100.1% | 5.4 | result |
| ursula | ursula-disk | 100 | 80.1k | 78.2% | 61.5 | overload (completion_ratio=0.7825<0.9800) |
| ursula | ursula-memory | 10 | 102.5k | 100.1% | 3.4 | result |
| ursula | ursula-memory | 100 | 102.5k | 100.1% | 2.1 | result |

The declared offered rate is 102,400 event deliveries per second.

## Catchup at 512 readers

| system | configuration | streams | full replays/s | MiB/s | p50 ms | p99 ms | verdict |
|---|---|---:|---:|---:|---:|---:|---|
| chronicle | chronicle-redis-aof-always | 10 | 3.7 | 59.7 | 33.3k | 37.4k | overload (errors=900) |
| chronicle | chronicle-redis-aof-always | 100 | 3.5 | 56.5 | 36.6k | 37.2k | overload (errors=919) |
| chronicle | chronicle-redis-aof-everysec | 10 | 3.5 | 56.5 | 39.2k | 39.3k | overload (errors=975) |
| chronicle | chronicle-redis-aof-everysec | 100 | 3.9 | 61.9 | 35.6k | 37.6k | overload (errors=826) |
| node | node-memory | 10 | 171.0 | 2.7k | 2.8k | 17.4k | result |
| node | node-memory | 100 | 162.4 | 2.6k | 1.2k | 25.1k | result |
| rust | rust-memory | 10 | 173.6 | 2.8k | 3.5k | 7.0k | result |
| rust | rust-memory | 100 | 173.5 | 2.8k | 3.5k | 7.1k | result |
| rust | rust-wal | 10 | 101.9 | 1.6k | 4.1k | 15.1k | overload (errors=7500) |
| rust | rust-wal | 100 | 172.4 | 2.8k | 3.7k | 4.6k | result |
| ursula | ursula-disk | 10 | 176.0 | 2.8k | 3.2k | 14.1k | result |
| ursula | ursula-disk | 100 | 169.7 | 2.7k | 3.1k | 13.3k | result |
| ursula | ursula-memory | 10 | 176.1 | 2.8k | 3.1k | 14.5k | result |
| ursula | ursula-memory | 100 | 169.1 | 2.7k | 3.4k | 14.3k | result |

One operation replays a stream whose stored size was verified at exactly 16 MiB before measurement. MiB/s counts response payload bytes, which can exclude server-specific record framing.

## Mixed writes with 100,000 readers

| system | configuration | writes/s | baseline retained | read MiB/s | write p99 ms | errors | verdict |
|---|---|---:|---:|---:|---:|---:|---|
| chronicle | chronicle-redis-aof-always | 5.6k | 32.0% | 82.5 | 55.3k | 348 | overload (errors=348, completion_ratio=0.1111<0.9800) |
| chronicle | chronicle-redis-aof-everysec | 5.7k | 31.9% | 83.4 | 54.5k | 4289 | overload (errors=4289, completion_ratio=0.1131<0.9800) |
| node | node-memory | 23.8k | 72.8% | 175.4 | 31.5k | 12 | overload (errors=12, completion_ratio=0.4757<0.9800) |
| rust | rust-memory | 50.0k | 100.0% | 304.0 | 2.7k | 0 | result |
| rust | rust-wal | 50.0k | 100.0% | 303.3 | 3.5k | 0 | result |
| ursula | ursula-disk | 1.4k | 59.9% | 55.3 | 60.0k | 5882 | overload (errors=5882, completion_ratio=0.0280<0.9800) |
| ursula | ursula-memory | 29.4k | 87.0% | 205.7 | 26.3k | 0 | overload (completion_ratio=0.5888<0.9800) |

Baseline retained compares each arm with its own zero-reader cell. Every arm was offered 50,000 writes per second.

## Mixed SSE delivery ceiling

| system | configuration | highest clean offered writes/s | observed writes/s | delivery p99 ms there | unthrottled writes/s | unthrottled events/s | verdict |
|---|---|---:|---:|---:|---:|---:|---|
| chronicle | chronicle-redis-aof-always | 4.0k | 4.1k | 572.4 | 12.4k | 6.5k | result |
| chronicle | chronicle-redis-aof-everysec | 4.0k | 4.1k | 609.3 | 12.5k | 6.5k | result |
| node | node-memory | 4.0k | 4.1k | 192.3 | 10.2k | 9.8k | overload (errors=151) |
| rust | rust-memory | 66.0k | 65.9k | 3.5 | 141.6k | 141.2k | result |
| rust | rust-wal | 66.0k | 65.9k | 92.1 | 79.3k | 79.0k | result |
| ursula | ursula-disk | 4.0k | 4.1k | 193.5 | 5.5k | 4.7k | result |
| ursula | ursula-memory | 16.0k | 16.1k | 1.2 | 39.0k | 32.0k | result |

`writer_rate=0` is the upstream unthrottled sentinel. It does not mean zero writes.

## Complete read and mixed appendix

| workload | system | configuration | streams | level | primary rate | p99 ms | verdict |
|---|---|---|---:|---:|---:|---:|---|
| blog-sse | rust | rust-memory | 1 | 1 | 50.0 | 0.4 | result |
| blog-sse | rust | rust-memory | 1 | 10 | 500.5 | 0.5 | result |
| blog-sse | rust | rust-memory | 1 | 100 | 5.0k | 0.9 | result |
| blog-sse | rust | rust-memory | 1 | 1000 | 50.0k | 3.9 | result |
| blog-sse | rust | rust-wal | 1 | 1 | 49.9 | 0.5 | result |
| blog-sse | rust | rust-wal | 1 | 10 | 500.5 | 0.7 | result |
| blog-sse | rust | rust-wal | 1 | 100 | 5.0k | 1.1 | result |
| blog-sse | rust | rust-wal | 1 | 1000 | 50.1k | 4.6 | result |
| blog-sse | node | node-memory | 1 | 1 | 50.0 | 0.6 | result |
| blog-sse | node | node-memory | 1 | 10 | 500.0 | 0.9 | result |
| blog-sse | node | node-memory | 1 | 100 | 5.0k | 3.8 | result |
| blog-sse | node | node-memory | 1 | 1000 | 42.1k | 56.6 | overload (completion_ratio=0.8422<0.9800) |
| blog-sse | ursula | ursula-disk | 1 | 1 | 50.0 | 1.5 | result |
| blog-sse | ursula | ursula-disk | 1 | 10 | 500.5 | 1.6 | result |
| blog-sse | ursula | ursula-disk | 1 | 100 | 5.0k | 2.0 | result |
| blog-sse | ursula | ursula-disk | 1 | 1000 | 50.0k | 5.2 | result |
| blog-sse | ursula | ursula-memory | 1 | 1 | 50.0 | 0.5 | result |
| blog-sse | ursula | ursula-memory | 1 | 10 | 500.5 | 0.7 | result |
| blog-sse | ursula | ursula-memory | 1 | 100 | 5.0k | 1.1 | result |
| blog-sse | ursula | ursula-memory | 1 | 1000 | 50.0k | 4.6 | result |
| blog-sse | chronicle | chronicle-redis-aof-always | 1 | 1 | 50.0 | 1.0 | result |
| blog-sse | chronicle | chronicle-redis-aof-always | 1 | 10 | 500.5 | 1.8 | result |
| blog-sse | chronicle | chronicle-redis-aof-always | 1 | 100 | 1.3k | 306.9 | overload (completion_ratio=0.2545<0.9800) |
| blog-sse | chronicle | chronicle-redis-aof-always | 1 | 1000 | 7.4k | 544.3 | overload (completion_ratio=0.1478<0.9800) |
| blog-sse | chronicle | chronicle-redis-aof-everysec | 1 | 1 | 50.0 | 0.7 | result |
| blog-sse | chronicle | chronicle-redis-aof-everysec | 1 | 10 | 500.5 | 1.4 | result |
| blog-sse | chronicle | chronicle-redis-aof-everysec | 1 | 100 | 1.2k | 377.3 | overload (completion_ratio=0.2357<0.9800) |
| blog-sse | chronicle | chronicle-redis-aof-everysec | 1 | 1000 | 7.7k | 495.4 | overload (completion_ratio=0.1534<0.9800) |
| reads-sse | rust | rust-memory | 10 | 64 | 3.2k | 0.7 | result |
| reads-sse | rust | rust-memory | 10 | 256 | 12.8k | 1.3 | result |
| reads-sse | rust | rust-memory | 10 | 1024 | 51.3k | 1.3 | result |
| reads-sse | rust | rust-memory | 10 | 2048 | 102.5k | 1.7 | result |
| reads-sse | rust | rust-memory | 100 | 64 | 3.2k | 0.5 | result |
| reads-sse | rust | rust-memory | 100 | 256 | 12.8k | 1.2 | result |
| reads-sse | rust | rust-memory | 100 | 1024 | 51.3k | 1.3 | result |
| reads-sse | rust | rust-memory | 100 | 2048 | 102.5k | 1.6 | result |
| reads-sse | rust | rust-wal | 10 | 64 | 3.2k | 0.7 | result |
| reads-sse | rust | rust-wal | 10 | 256 | 12.8k | 1.3 | result |
| reads-sse | rust | rust-wal | 10 | 1024 | 51.3k | 1.9 | result |
| reads-sse | rust | rust-wal | 10 | 2048 | 102.5k | 3.6 | result |
| reads-sse | rust | rust-wal | 100 | 64 | 3.2k | 0.6 | result |
| reads-sse | rust | rust-wal | 100 | 256 | 12.8k | 1.5 | result |
| reads-sse | rust | rust-wal | 100 | 1024 | 51.3k | 1.0 | result |
| reads-sse | rust | rust-wal | 100 | 2048 | 102.5k | 1.7 | result |
| reads-sse | node | node-memory | 10 | 64 | 3.2k | 0.8 | result |
| reads-sse | node | node-memory | 10 | 256 | 12.8k | 1.6 | result |
| reads-sse | node | node-memory | 10 | 1024 | 39.9k | 33.2 | overload (completion_ratio=0.7793<0.9800) |
| reads-sse | node | node-memory | 10 | 2048 | 41.8k | 57.8 | overload (completion_ratio=0.4087<0.9800) |
| reads-sse | node | node-memory | 100 | 64 | 3.2k | 0.7 | result |
| reads-sse | node | node-memory | 100 | 256 | 12.8k | 1.9 | result |
| reads-sse | node | node-memory | 100 | 1024 | 30.1k | 45.3 | overload (completion_ratio=0.5883<0.9800) |
| reads-sse | node | node-memory | 100 | 2048 | 37.6k | 60.2 | overload (completion_ratio=0.3675<0.9800) |
| reads-sse | ursula | ursula-disk | 10 | 64 | 3.2k | 1.5 | result |
| reads-sse | ursula | ursula-disk | 10 | 256 | 12.8k | 1.7 | result |
| reads-sse | ursula | ursula-disk | 10 | 1024 | 51.3k | 2.5 | result |
| reads-sse | ursula | ursula-disk | 10 | 2048 | 102.5k | 5.4 | result |
| reads-sse | ursula | ursula-disk | 100 | 64 | 3.1k | 41.1 | overload (completion_ratio=0.9709<0.9800) |
| reads-sse | ursula | ursula-disk | 100 | 256 | 12.0k | 43.1 | overload (completion_ratio=0.9354<0.9800) |
| reads-sse | ursula | ursula-disk | 100 | 1024 | 45.6k | 56.0 | overload (completion_ratio=0.8903<0.9800) |
| reads-sse | ursula | ursula-disk | 100 | 2048 | 80.1k | 61.5 | overload (completion_ratio=0.7825<0.9800) |
| reads-sse | ursula | ursula-memory | 10 | 64 | 3.2k | 0.7 | result |
| reads-sse | ursula | ursula-memory | 10 | 256 | 12.8k | 1.1 | result |
| reads-sse | ursula | ursula-memory | 10 | 1024 | 51.3k | 1.4 | result |
| reads-sse | ursula | ursula-memory | 10 | 2048 | 102.5k | 3.4 | result |
| reads-sse | ursula | ursula-memory | 100 | 64 | 3.2k | 0.5 | result |
| reads-sse | ursula | ursula-memory | 100 | 256 | 12.8k | 1.1 | result |
| reads-sse | ursula | ursula-memory | 100 | 1024 | 51.3k | 1.7 | result |
| reads-sse | ursula | ursula-memory | 100 | 2048 | 102.5k | 2.1 | result |
| reads-sse | chronicle | chronicle-redis-aof-always | 10 | 64 | 1.3k | 233.6 | overload (completion_ratio=0.4123<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 10 | 256 | 2.5k | 408.8 | overload (completion_ratio=0.1971<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 10 | 1024 | 7.1k | 560.6 | overload (completion_ratio=0.1382<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 10 | 2048 | 11.2k | 469.2 | overload (completion_ratio=0.1097<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 100 | 64 | 1.4k | 237.3 | overload (completion_ratio=0.4432<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 100 | 256 | 3.6k | 391.7 | overload (completion_ratio=0.2795<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 100 | 1024 | 6.9k | 521.5 | overload (completion_ratio=0.1353<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-always | 100 | 2048 | 10.8k | 462.6 | overload (completion_ratio=0.1051<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 10 | 64 | 1.2k | 313.1 | overload (completion_ratio=0.3697<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 10 | 256 | 2.3k | 428.8 | overload (completion_ratio=0.1807<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 10 | 1024 | 7.6k | 488.4 | overload (completion_ratio=0.1475<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 10 | 2048 | 11.2k | 502.3 | overload (completion_ratio=0.1095<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 100 | 64 | 1.4k | 238.8 | overload (completion_ratio=0.4275<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 100 | 256 | 3.2k | 416.3 | overload (completion_ratio=0.2463<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 100 | 1024 | 6.9k | 529.4 | overload (completion_ratio=0.1341<0.9800) |
| reads-sse | chronicle | chronicle-redis-aof-everysec | 100 | 2048 | 11.2k | 454.1 | overload (completion_ratio=0.1093<0.9800) |
| mixed-writes | rust | rust-memory | 10000 | 0 | 50.1k | 1.2k | result |
| mixed-writes | rust | rust-memory | 10000 | 1000 | 50.1k | 1.2k | result |
| mixed-writes | rust | rust-memory | 10000 | 10000 | 50.1k | 1.3k | result |
| mixed-writes | rust | rust-memory | 10000 | 100000 | 50.0k | 2.7k | result |
| mixed-writes | rust | rust-wal | 10000 | 0 | 50.0k | 1.4k | result |
| mixed-writes | rust | rust-wal | 10000 | 1000 | 50.1k | 1.2k | result |
| mixed-writes | rust | rust-wal | 10000 | 10000 | 50.1k | 1.4k | result |
| mixed-writes | rust | rust-wal | 10000 | 100000 | 50.0k | 3.5k | result |
| mixed-writes | node | node-memory | 10000 | 0 | 32.7k | 22.5k | overload (errors=902, completion_ratio=0.6538<0.9800) |
| mixed-writes | node | node-memory | 10000 | 1000 | 32.3k | 24.5k | overload (errors=884, completion_ratio=0.6467<0.9800) |
| mixed-writes | node | node-memory | 10000 | 10000 | 31.0k | 35.5k | overload (errors=755, completion_ratio=0.6194<0.9800) |
| mixed-writes | node | node-memory | 10000 | 100000 | 23.8k | 31.5k | overload (errors=12, completion_ratio=0.4757<0.9800) |
| mixed-writes | ursula | ursula-disk | 10000 | 0 | 2.3k | 60.0k | overload (completion_ratio=0.0467<0.9800) |
| mixed-writes | ursula | ursula-disk | 10000 | 1000 | 2.3k | 60.0k | overload (completion_ratio=0.0465<0.9800) |
| mixed-writes | ursula | ursula-disk | 10000 | 10000 | 2.3k | 60.0k | overload (completion_ratio=0.0460<0.9800) |
| mixed-writes | ursula | ursula-disk | 10000 | 100000 | 1.4k | 60.0k | overload (errors=5882, completion_ratio=0.0280<0.9800) |
| mixed-writes | ursula | ursula-memory | 10000 | 0 | 33.8k | 21.1k | overload (completion_ratio=0.6769<0.9800) |
| mixed-writes | ursula | ursula-memory | 10000 | 1000 | 33.7k | 21.3k | overload (completion_ratio=0.6745<0.9800) |
| mixed-writes | ursula | ursula-memory | 10000 | 10000 | 33.0k | 22.3k | overload (completion_ratio=0.6602<0.9800) |
| mixed-writes | ursula | ursula-memory | 10000 | 100000 | 29.4k | 26.3k | overload (completion_ratio=0.5888<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-always | 10000 | 0 | 17.4k | 39.1k | overload (completion_ratio=0.3472<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-always | 10000 | 1000 | 17.2k | 39.4k | overload (completion_ratio=0.3434<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-always | 10000 | 10000 | 14.1k | 43.2k | overload (errors=1, completion_ratio=0.2821<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-always | 10000 | 100000 | 5.6k | 55.3k | overload (errors=348, completion_ratio=0.1111<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-everysec | 10000 | 0 | 17.7k | 38.8k | overload (completion_ratio=0.3542<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-everysec | 10000 | 1000 | 16.9k | 39.7k | overload (completion_ratio=0.3387<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-everysec | 10000 | 10000 | 14.6k | 42.6k | overload (errors=1, completion_ratio=0.2928<0.9800) |
| mixed-writes | chronicle | chronicle-redis-aof-everysec | 10000 | 100000 | 5.7k | 54.5k | overload (errors=4289, completion_ratio=0.1131<0.9800) |
| mixed-delivery | rust | rust-memory | 2000 | 0 | 141.6k | 47.0 | result |
| mixed-delivery | rust | rust-memory | 2000 | 2 | 4.1k | 102.3 | result |
| mixed-delivery | rust | rust-memory | 2000 | 8 | 16.1k | 27.6 | result |
| mixed-delivery | rust | rust-memory | 2000 | 20 | 40.0k | 5.2 | result |
| mixed-delivery | rust | rust-memory | 2000 | 33 | 65.9k | 54.1 | result |
| mixed-delivery | rust | rust-wal | 2000 | 0 | 79.3k | 87.4 | result |
| mixed-delivery | rust | rust-wal | 2000 | 2 | 4.1k | 109.8 | result |
| mixed-delivery | rust | rust-wal | 2000 | 8 | 16.1k | 53.5 | result |
| mixed-delivery | rust | rust-wal | 2000 | 20 | 40.0k | 132.2 | result |
| mixed-delivery | rust | rust-wal | 2000 | 33 | 65.9k | 307.2 | result |
| mixed-delivery | node | node-memory | 2000 | 0 | 10.2k | 194.0 | overload (errors=151) |
| mixed-delivery | node | node-memory | 2000 | 2 | 4.1k | 189.3 | result |
| mixed-delivery | node | node-memory | 2000 | 8 | 13.4k | 19.6k | overload (errors=253, completion_ratio=0.8388<0.9800) |
| mixed-delivery | node | node-memory | 2000 | 20 | 10.2k | 23.6k | overload (errors=85, completion_ratio=0.2549<0.9800) |
| mixed-delivery | node | node-memory | 2000 | 33 | 10.8k | 27.6k | overload (errors=530, completion_ratio=0.1634<0.9800) |
| mixed-delivery | ursula | ursula-disk | 2000 | 0 | 5.5k | 697.9 | result |
| mixed-delivery | ursula | ursula-disk | 2000 | 2 | 4.1k | 189.1 | result |
| mixed-delivery | ursula | ursula-disk | 2000 | 8 | 5.5k | 21.3k | overload (completion_ratio=0.3436<0.9800) |
| mixed-delivery | ursula | ursula-disk | 2000 | 20 | 5.5k | 26.2k | overload (completion_ratio=0.1371<0.9800) |
| mixed-delivery | ursula | ursula-disk | 2000 | 33 | 5.5k | 27.7k | overload (completion_ratio=0.0835<0.9800) |
| mixed-delivery | ursula | ursula-memory | 2000 | 0 | 39.0k | 100.6 | result |
| mixed-delivery | ursula | ursula-memory | 2000 | 2 | 4.1k | 111.0 | result |
| mixed-delivery | ursula | ursula-memory | 2000 | 8 | 16.1k | 3.0 | result |
| mixed-delivery | ursula | ursula-memory | 2000 | 20 | 38.5k | 2.0k | overload (errors=83, completion_ratio=0.9619<0.9800) |
| mixed-delivery | ursula | ursula-memory | 2000 | 33 | 39.1k | 15.3k | overload (completion_ratio=0.5918<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-always | 2000 | 0 | 12.4k | 233.6 | result |
| mixed-delivery | chronicle | chronicle-redis-aof-always | 2000 | 2 | 4.1k | 327.2 | result |
| mixed-delivery | chronicle | chronicle-redis-aof-always | 2000 | 8 | 12.4k | 9.1k | overload (completion_ratio=0.7722<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-always | 2000 | 20 | 10.4k | 22.2k | overload (completion_ratio=0.2591<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-always | 2000 | 33 | 10.8k | 25.0k | overload (completion_ratio=0.1633<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-everysec | 2000 | 0 | 12.5k | 227.5 | result |
| mixed-delivery | chronicle | chronicle-redis-aof-everysec | 2000 | 2 | 4.1k | 364.3 | result |
| mixed-delivery | chronicle | chronicle-redis-aof-everysec | 2000 | 8 | 12.5k | 8.9k | overload (completion_ratio=0.7805<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-everysec | 2000 | 20 | 12.6k | 20.6k | overload (completion_ratio=0.3138<0.9800) |
| mixed-delivery | chronicle | chronicle-redis-aof-everysec | 2000 | 33 | 12.5k | 24.2k | overload (completion_ratio=0.1894<0.9800) |
| reads-catchup | rust | rust-memory | 10 | 8 | 142.9 | 112.5 | result |
| reads-catchup | rust | rust-memory | 10 | 32 | 144.3 | 662.5 | result |
| reads-catchup | rust | rust-memory | 10 | 128 | 149.7 | 2.0k | result |
| reads-catchup | rust | rust-memory | 10 | 512 | 173.6 | 7.0k | result |
| reads-catchup | rust | rust-memory | 100 | 8 | 142.9 | 108.9 | result |
| reads-catchup | rust | rust-memory | 100 | 32 | 144.3 | 776.7 | result |
| reads-catchup | rust | rust-memory | 100 | 128 | 150.3 | 2.1k | result |
| reads-catchup | rust | rust-memory | 100 | 512 | 173.5 | 7.1k | result |
| reads-catchup | rust | rust-wal | 10 | 8 | 135.7 | 65.8 | result |
| reads-catchup | rust | rust-wal | 10 | 32 | 141.8 | 350.0 | result |
| reads-catchup | rust | rust-wal | 10 | 128 | 147.2 | 1.3k | result |
| reads-catchup | rust | rust-wal | 10 | 512 | 101.9 | 15.1k | overload (errors=7500) |
| reads-catchup | rust | rust-wal | 100 | 8 | 134.5 | 66.8 | result |
| reads-catchup | rust | rust-wal | 100 | 32 | 141.7 | 333.1 | result |
| reads-catchup | rust | rust-wal | 100 | 128 | 147.6 | 1.4k | result |
| reads-catchup | rust | rust-wal | 100 | 512 | 172.4 | 4.6k | result |
| reads-catchup | node | node-memory | 10 | 8 | 138.2 | 90.4 | result |
| reads-catchup | node | node-memory | 10 | 32 | 144.0 | 579.6 | result |
| reads-catchup | node | node-memory | 10 | 128 | 150.7 | 2.2k | result |
| reads-catchup | node | node-memory | 10 | 512 | 171.0 | 17.4k | result |
| reads-catchup | node | node-memory | 100 | 8 | 111.1 | 110.7 | result |
| reads-catchup | node | node-memory | 100 | 32 | 134.8 | 417.3 | result |
| reads-catchup | node | node-memory | 100 | 128 | 140.8 | 6.8k | result |
| reads-catchup | node | node-memory | 100 | 512 | 162.4 | 25.1k | result |
| reads-catchup | ursula | ursula-disk | 10 | 8 | 141.8 | 81.7 | result |
| reads-catchup | ursula | ursula-disk | 10 | 32 | 143.7 | 535.6 | result |
| reads-catchup | ursula | ursula-disk | 10 | 128 | 149.4 | 2.9k | result |
| reads-catchup | ursula | ursula-disk | 10 | 512 | 176.0 | 14.1k | result |
| reads-catchup | ursula | ursula-disk | 100 | 8 | 141.6 | 83.9 | result |
| reads-catchup | ursula | ursula-disk | 100 | 32 | 143.5 | 493.6 | result |
| reads-catchup | ursula | ursula-disk | 100 | 128 | 149.7 | 2.8k | result |
| reads-catchup | ursula | ursula-disk | 100 | 512 | 169.7 | 13.3k | result |
| reads-catchup | ursula | ursula-memory | 10 | 8 | 142.0 | 84.6 | result |
| reads-catchup | ursula | ursula-memory | 10 | 32 | 143.9 | 714.8 | result |
| reads-catchup | ursula | ursula-memory | 10 | 128 | 149.1 | 2.9k | result |
| reads-catchup | ursula | ursula-memory | 10 | 512 | 176.1 | 14.5k | result |
| reads-catchup | ursula | ursula-memory | 100 | 8 | 141.8 | 88.7 | result |
| reads-catchup | ursula | ursula-memory | 100 | 32 | 143.7 | 480.3 | result |
| reads-catchup | ursula | ursula-memory | 100 | 128 | 148.8 | 2.6k | result |
| reads-catchup | ursula | ursula-memory | 100 | 512 | 169.1 | 14.3k | result |
| reads-catchup | chronicle | chronicle-redis-aof-always | 10 | 8 | 18.7 | 469.2 | result |
| reads-catchup | chronicle | chronicle-redis-aof-always | 10 | 32 | 19.2 | 1.8k | result |
| reads-catchup | chronicle | chronicle-redis-aof-always | 10 | 128 | 3.2 | 25.6k | overload (errors=117) |
| reads-catchup | chronicle | chronicle-redis-aof-always | 10 | 512 | 3.7 | 37.4k | overload (errors=900) |
| reads-catchup | chronicle | chronicle-redis-aof-always | 100 | 8 | 18.1 | 476.4 | result |
| reads-catchup | chronicle | chronicle-redis-aof-always | 100 | 32 | 21.1 | 1.8k | result |
| reads-catchup | chronicle | chronicle-redis-aof-always | 100 | 128 | 3.3 | 25.9k | overload (errors=109) |
| reads-catchup | chronicle | chronicle-redis-aof-always | 100 | 512 | 3.5 | 37.2k | overload (errors=919) |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 10 | 8 | 18.7 | 466.4 | result |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 10 | 32 | 19.2 | 1.8k | result |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 10 | 128 | 3.7 | 26.3k | overload (errors=118) |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 10 | 512 | 3.5 | 39.3k | overload (errors=975) |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 100 | 8 | 19.2 | 451.3 | result |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 100 | 32 | 19.2 | 1.8k | result |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 100 | 128 | 3.6 | 25.8k | overload (errors=117) |
| reads-catchup | chronicle | chronicle-redis-aof-everysec | 100 | 512 | 3.9 | 37.6k | overload (errors=826) |

## Run inventory and cost boundary

| campaign | suites | cluster hours | server vCPU-hours | Spot client vCPU-hours | total vCPU-hours | timing source |
|---|---:|---:|---:|---:|---:|---|
| 20260727T122510Z-bd85274b | 24 | 11.67 | 186.7 | 746.9 | 933.6 | filesystem-mtime |
| 20260728T122629Z-59260e88 | 4 | 3.01 | 48.2 | 192.9 | 241.2 | execution |

This inventory bounds billable machine use. It is not a dollar invoice. Google Cloud SKU prices, Spot discounts, control-plane charges, taxes, and billing credits can change, so the authoritative dollar cost must come from the project billing export.

## Provenance and validity

| campaign | role | upstream commit | adapter | Chronicle image | client image | Chronicle build context | client build context |
|---|---|---|---|---|---|---|---|
| 20260727T122510Z-bd85274b | primary | `93a1a066a511` | `eee390c42e3c` | `sha256:e750a3223c04` | `sha256:49673e701d61` | `legacy worktree f24` | `adapter eee390c42e3` |
| 20260728T122629Z-59260e88 | supplement | `93a1a066a511` | `0260541bc072` | `sha256:e750a3223c04` | `sha256:5b71a66d6031` | `legacy worktree f24` | `bc2401a8338bb00298d` |

Evidence seals verified before report generation:

- `20260727T122510Z-bd85274b`: `2584` raw files; tree SHA-256 `ad3104c8a0a10ca8b7e81b88e636517181ad33f82ecf3b56c9bf6977d1a1c917`.
- `20260728T122629Z-59260e88`: `481` raw files; tree SHA-256 `c0860ab25e172de648dde9280ea749d5343a142d1e6d3298fcd8a8cbf4c569e8`.

Recorded seal repairs:

- `20260728T122629Z-59260e88`: The campaign sealed the archive before the final detached watchdog observed its done marker and flushed its normal terminal log line. No measurement result, raw aggregate, manifest, image record, execution summary, diagnostic, teardown proof, or cluster absence proof changed.

Server image reuse:
- `20260728T122629Z-59260e88` uses the exact SUT image digests from `20260727T122510Z-bd85274b`.

- Chronicle SUT source: `ecf1a8a8f8ad9283cedcbd17b465acb410ccfec0`; worktree diff digest `f2472e04ebe27ad8c2f3c1e927e7842db9dd4c0dd29d83e124cccd5dc4187a2d`.
- ds-bench: `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf`; adapter `eee390c42e3c39dc1bd6c2041b796e80e4c3f6ada0244a9db758195067e228ff`.
- Chronicle to Redis CPU split: `2:2`.
- Invalid or missing result records: `0`.
- Valid overload observations: `73`.
- Data archives: `20260727T122510Z-bd85274b` (primary); `20260728T122629Z-59260e88` (supplement).
- Raw per-pod JSON, HDR input, merged JSON, samples, resolved suites, logs, image digests, and teardown proofs are retained in this archive.

## Threats to validity

- This is a single-node throughput comparison, not an availability or replication comparison.
- Chronicle includes an in-pod Redis process. The report counts both processes and the whole pod working set.
- The official harness runs MinIO outside the 4 vCPU primary SUT budget. Rust and Ursula can use it as a cold tier, while Chronicle and Node do not. Server and MinIO logs are retained so overlapping cold-tier work can be audited.
- Local WAL, Redis AOF `always`, Redis AOF `everysec`, and memory modes have different acknowledgement guarantees. Their numbers are not merged.
- A result marked as a gap or invalid is not treated as zero.
