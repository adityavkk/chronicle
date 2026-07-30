# Integrating bounded catch-up with the pending SSE fanout hub

## Merge order

Land issue 5 first, then rebase `perf/sse-fanout-hub` from
`/private/tmp/chronicle-p0-sse`. Bounded storage pages are a prerequisite for
the hub. Do not resolve `handler_sse.go` by choosing either file in full. The
correct result combines the fanout branch's subscribe-before-read ordering with
issue 5's captured, bounded page drain.

The main overlapping files are:

- `handler.go` and `handler_sse.go`
- `config.go`, `config_test.go`, and `cmd/chronicle/main.go`
- `metrics/metrics.go` and `metrics/metrics_test.go`
- `store/store.go`, `store/memory_store.go`, `store/redis/meta.go`,
  `store/redis/notify.go`, and `store/redis/store.go`
- `jepsen/checker/main.go`, `jepsen/deploy/deploy.yaml`, and `jepsen/README.md`

Both branches add stream incarnation support. Keep one 128-bit generator and
one persisted metadata field. Update both branches' tests to use the shared
helper. Do not keep two names for the same identity.

`store.PageReader` and the fanout branch's notification subscriber should
remain optional capability interfaces beside `store.Store`. This preserves
source compatibility for external store implementations.

## Required SSE sequence

The combined request path should use this order:

1. Read request metadata and acquire the per-stream hub for that exact
   incarnation.
2. Let the hub establish its Redis subscription, perform its durable fallback
   read, and report ready.
3. Call `ReadPage` from the client's requested offset with no supplied
   snapshot. This first page captures the response tail, content type, closed
   state, and incarnation.
4. Reject the request if the captured page incarnation differs from the hub
   incarnation.
5. Send headers, then write one data event and one control event for each
   storage page. Reuse the first page's snapshot on every following call.
6. Flush each complete page. Advance the client position only after its control
   event has been written.
7. When the captured snapshot is up to date, attach to the already acquired
   watcher at the exact last control offset.
8. Let the watcher return only messages after that offset. If its bounded
   replay ring no longer contains the attach point, close the connection and
   let the client resume from the last control offset. Never skip ahead to the
   hub's current offset.

Acquiring the hub before the first page closes the append race. Capturing the
storage snapshot after the hub is ready is safe. Appends included in both the
hub replay ring and the storage snapshot are removed by attaching at the exact
sent offset. Appends after the snapshot are delivered by the hub. This gives no
gap and no duplicate.

Delete and recreate must remain terminal for the old request. Check the
incarnation after hub readiness, on every storage page through
`ReadSnapshot.Incarnation`, and before live attach.

## Bound hub reads too

The fanout branch currently calls `Store.Read` in its initial catch-up and
`sseHub.refresh`. After the rebase, no hub path may call that compatibility
method for a stream suffix. `Store.Read` now concatenates bounded pages for
legacy callers, but it still materializes the complete logical result in Go.

Change each hub refresh to:

1. Capture one `ReadPage` snapshot at the hub's current offset.
2. Drain that snapshot page by page.
3. Publish each page as one or more replay events, with the existing
   `SSEHubBatchBytes` retained-size split.
4. Recheck cancellation and incarnation between pages.

The storage target should be no larger than `ReadPageBytes`. It can be smaller
when `SSEHubBatchBytes` is smaller. The 1,024-frame cap remains mandatory.
One oversized valid frame is still allowed and must stay intact.

This change preserves the fanout branch's main benefit: one durable page read
and one formatted replay event per stream per replica, shared by all local
clients. It also prevents a burst of missed notifications from becoming one
unbounded hub read.

## Conflict details

The final `Handler` needs both groups of fields:

- Issue 5: `ReadPageBytes` and `ReadMetrics`
- Fanout: replay bytes, batch bytes, poll interval, client write timeout,
  fanout metrics, and the hub registry

Keep the metric families separate. Page metrics describe storage and response
work. Hub metrics describe shared live delivery and replay pressure.

The issue 5 SSE writer streams JSON framing and base64 directly. Keep that
writer for initial catch-up. The fanout ring may retain formatted live events,
but it must not build a second full catch-up body.

The fanout branch caps its defensive poll by sliding TTL. Preserve that rule.
Every root `ReadPage` is an active read and renews the root stream. Reading an
inherited fork page must not renew its source.

## Combined tests

Keep both branches' tests, then add these merge-specific cases:

- Use a one-frame page cap. Append after hub readiness but during catch-up.
  Storage must stop at its captured tail and the hub must deliver the append
  exactly once.
- Append and close while the last catch-up page is being flushed. Final data
  must precede the closed control event.
- Delete and recreate between pages and again just before live attach. No data
  from the new incarnation may appear.
- Make catch-up slower than the replay window. The connection may close as
  lagged, but resume from its last control offset must cover every durable frame
  exactly once.
- Drop a Redis notification. The bounded hub poll must recover the page without
  a gap.
- Cancel before the first page, between pages, while queued for Redis, during a
  client write, and while waiting on the watcher. No goroutine, pool waiter, or
  replay reference may remain.
- Run the paged-catchup fault scenario and the fanout SSE scenario together
  across Chronicle restart, Redis restart, connection interruption, fork
  inheritance, concurrent append, and close.

The release gate remains 332 of 332 conformance tests plus all maintained
Jepsen and Porcupine histories.
