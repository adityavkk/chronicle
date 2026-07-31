# Issue 13 adversarial review dispositions

- Review baseline: `b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035`
- Internal issue: the shared-hub implementation
- Review date: 2026-07-31
- Scope: SSE fanout scaling after the per-stream hub

The required fresh adversarial review found the following gaps on the clean
baseline. Every finding is accepted. The dispositions below were recorded
before implementation files were changed.

| ID | Severity | Disposition |
|---|---:|---|
| I13-F001 | High | Accepted. Require `store.PageReader` for SSE catch-up and hub refresh. Keep the compatibility reader only for non-SSE HTTP reads, and add a store that fails if either SSE path calls `Store.Read`. |
| I13-F002 | High | Accepted. Split hub registration from snapshot binding so one path registration is acknowledged before the first client page is captured. Preserve one shared logical registration per active stream. |
| I13-F003 | Critical | Accepted. Replace per-hub Redis `PubSub` objects with a bounded, store-owned notification multiplexer. Default standalone and global cluster Pub/Sub to one physical connection group. |
| I13-F004 | High | Accepted. Track desired channels and connection generations, reject stale acknowledgements, restore registrations after reconnect, wake every hub for a durable refresh, and use nonblocking coalesced per-registration signals. |
| I13-F005 | High | Accepted. Add monotonic retained-batch sequences and indexed boundary lookup. Normal watcher advancement will use its cached sequence; first attachment will use logarithmic lookup. |
| I13-F006 | High | Accepted. Retain one shared wire representation with compact offset boundaries instead of complete raw and wire copies. Account raw, wire, index, and total bytes independently and clear all four on eviction and cleanup. |
| I13-F007 | Medium | Accepted. Clear every installed write deadline with deferred cleanup on success and failure, including the initial header flush and wrapped response writers. |
| I13-F008 | High | Accepted. Separate logical registrations from physical connections and add bounded reason, refresh, page, ring-component, and lookup metrics without stream-path labels. |
| I13-F009 | Critical | Accepted. Instrument `ReadPage` in `BenchmarkSSEHubReadAmplification1000Clients`, report pages and Redis commands per publish separately, and retain the `<= 1.2` gate. |
| I13-F010 | Medium | Accepted. Benchmark the existing map/buffer control path against typed control encoding, reusable buffers, and combined writes under a real HTTP server. Production changes require the issue's 10 percent improvement and 5 percent non-regression rule. |
| I13-F011 | High | Accepted. Add the full deterministic PageReader-only, paging, attach/eviction, multiplexer, framing, oversized-frame, TTL, fork, reconnect, and deadline-failure matrix, and run concurrent coverage under the race detector. |
| I13-F012 | High | Accepted. Add standalone and supported-cluster multiplexer integration coverage, a Pub/Sub connection-kill recovery test with continuous appends, and extend the SSE Jepsen scenario for delete/recreate and page-boundary attachment. |
| I13-F013 | High | Accepted. Produce a minimal sealed before/after evidence set for connection counts, command/page amplification, throughput, p99, the 30-minute slow-client run, reconnect storm, runtime resources, and supporting profiles. No paid resources will be provisioned without approval. |
| I13-F014 | Medium | Accepted. Amend ADR 0004 and update operator configuration, metrics documentation, the performance-improvements page, and the SSE explainer, including internal issue 4 and 13 links where permitted. |

Completion requires evidence for every disposition above. A passing narrow unit
test is not a disposition for a broader performance, topology, fault, or
integration finding.
