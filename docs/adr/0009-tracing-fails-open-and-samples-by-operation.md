# ADR-0009: Tracing fails open, samples its own roots by operation, and hands receivers an unsigned `traceparent`

- **Status:** Accepted
- **Date:** 2026-09-28
- **Deciders:** @adityavkk

## Context

Chronicle sits between a caller that appends and a receiver that is woken by
a webhook, asynchronously and possibly on another replica. Operators want one
trace across that hop, and they want it from a destination they choose, with
credentials mounted as secrets. Two things about the first implementation
(a deployment-first change, since backported) were wrong for a public,
generic server: it refused to start when a credential file was unreadable,
so a secret-mount problem took the stream authority down for the sake of
telemetry; and it sampled every root, which on a busy deployment means every
console asset fetch and every health probe.

The request id (`docs/DEPLOYMENT.md`, request correlation) already established
the shape of an unsigned, bounded, process-local join hint that rides the
append hint queue to the wake and falls back to a stable derived value when
the memory does not have it.

## Decision

1. **Opt-in, generic configuration.** Tracing is on iff `CHRONICLE_OTLP_ENDPOINT`
   is set. The destination is any OTLP/HTTP traces URL; a basic-auth credential
   and a private CA come from files. Nothing in the public code or docs names a
   vendor; a deployment's specifics are its configuration.
2. **Fail open, loudly.** An unusable credential or CA file, or an exporter
   that cannot be built, disables tracing with one `tracing_disabled` warning
   and `chronicle_tracing_setup_failures_total{reason}`, and the server starts.
   A malformed configuration value is still a startup error. This is the only
   Chronicle subsystem allowed to fail open; authentication, fencing and TLS
   fail closed.
3. **Roots are Chronicle's decision, parents are the caller's.** Every span
   with a parent follows the parent's sampled flag. A root Chronicle starts is
   named `chronicle.<operation>` and is kept by an always-list of operations or
   by a trace-id ratio; a root that is not a Chronicle operation, meaning an
   instrumentation span from background Redis work, is dropped.
4. **The trace rides the append origin.** The dirty-queue hint and the bounded
   wake memory remember an `appendOrigin`, request id and trace identity
   together, under one coalescing rule and one idle TTL. Trace state, whose
   size a caller controls, is dropped at the append. A delivery is a
   `chronicle.delivery` client span, a child of the append's trace when
   remembered and a root otherwise; `traceparent` is written on the `POST`
   only when a `Tracer` is configured.
5. **Shapes and safe ids only.** Spans carry method, status, byte counts,
   subscription id, wake id and generation. Never a URL path or query, a body,
   a header value, a Redis statement or a webhook target URL. A test asserts
   that neither the path nor the query string of a request is exported.
6. **Unsigned, never identity.** Like the request id, `traceparent` is outside
   `Webhook-Signature` and no authorization reads it. Receivers choose parent
   or link (`docs/spec/CHRONICLE-NOTES.md` §7.1).

## Consequences

- A deployment that misplaces a secret keeps serving and sees a counter and a
  warning instead of a crash loop; it also loses traces until the mount is
  fixed, which is the trade this ADR makes.
- Redis spans appear only under requests that carry their context to the
  store (today the read path); the append path's store calls take no context
  and so are dropped as roots. Threading context through `store.Store` is a
  separate change.
- The wake memory holds a `SpanContext` per remembered wake (a few dozen bytes
  more than the id alone) within the same 16384-entry bound.
- `go.opentelemetry.io/otel` (API, SDK, OTLP/HTTP exporter) and
  `redisotel` become dependencies of the binary; the `webhook` and root
  packages import only the API.
