# SSE fanout adversarial review dispositions

- Review baseline: `b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035`
- Review date: 2026-07-31
- Scope: SSE fanout scaling after the per-stream hub

The required fresh adversarial review found the following gaps on the clean
baseline. Every finding is accepted. The dispositions below were recorded
before implementation files were changed.

| ID | Severity | Disposition |
|---|---:|---|
| SSE-F001 | High | Accepted. Require `store.PageReader` for SSE catch-up and hub refresh. Keep the compatibility reader only for non-SSE HTTP reads, and add a store that fails if either SSE path calls `Store.Read`. |
| SSE-F002 | High | Accepted. Split hub registration from snapshot binding so one path registration is acknowledged before the first client page is captured. Preserve one shared logical registration per active stream. |
| SSE-F003 | Critical | Accepted. Replace per-hub Redis `PubSub` objects with a bounded, store-owned notification multiplexer. Default standalone and global cluster Pub/Sub to one physical connection group. |
| SSE-F004 | High | Accepted. Track desired channels and connection generations, reject stale acknowledgements, restore registrations after reconnect, wake every hub for a durable refresh, and use nonblocking coalesced per-registration signals. |
| SSE-F005 | High | Accepted. Add monotonic retained-batch sequences and indexed boundary lookup. Normal watcher advancement will use its cached sequence; first attachment will use logarithmic lookup. |
| SSE-F006 | High | Accepted. Retain one shared wire representation with compact offset boundaries instead of complete raw and wire copies. Account raw, wire, index, and total bytes independently and clear all four on eviction and cleanup. |
| SSE-F007 | Medium | Accepted. Clear every installed write deadline with deferred cleanup on success and failure, including the initial header flush and wrapped response writers. |
| SSE-F008 | High | Accepted. Separate logical registrations from physical connections and add bounded reason, refresh, page, ring-component, and lookup metrics without stream-path labels. |
| SSE-F009 | Critical | Accepted. Instrument `ReadPage` in `BenchmarkSSEHubReadAmplification1000Clients`, report pages and Redis commands per publish separately, and retain the `<= 1.2` gate. |
| SSE-F010 | Medium | Accepted. Benchmark the existing map/buffer control path against typed control encoding, reusable buffers, and combined writes under a real HTTP server. Production changes require the performance gate's 10 percent improvement and 5 percent non-regression rule. |
| SSE-F011 | High | Accepted. Add the full deterministic PageReader-only, paging, attach/eviction, multiplexer, framing, oversized-frame, TTL, fork, reconnect, and deadline-failure matrix, and run concurrent coverage under the race detector. |
| SSE-F012 | High | Accepted. Add standalone and supported-cluster multiplexer integration coverage, a Pub/Sub connection-kill recovery test with continuous appends, and extend the SSE Jepsen scenario for delete/recreate and page-boundary attachment. |
| SSE-F013 | High | Accepted. Produce a minimal sealed before/after evidence set for connection counts, command/page amplification, throughput, p99, the 30-minute slow-client run, reconnect storm, runtime resources, and supporting profiles. No paid resources will be provisioned without approval. |
| SSE-F014 | Medium | Accepted. Amend ADR 0004 and update operator configuration, metrics documentation, the performance-improvements page, and the SSE explainer. |

Completion requires evidence for every disposition above. A passing narrow unit
test is not a disposition for a broader performance, topology, fault, or
integration finding.
