# Triage — Issue #8: guard the pipeline/`MULTI` NOSCRIPT footgun

> **Decision: IMPLEMENT (lightweight).** Confidence: high. Shipped a `forbidigo`
> lint rule + caveat comments. Tracks
> [#8](https://github.com/adityavkk/chronicle/issues/8); follows
> [ADR-0001](../../../../adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md).

## The risk (real, latent, low-severity)

`Script.Run`'s `NOSCRIPT → EVAL` self-heal does **not** fire when a script is
queued inside a pipeline/`MULTI` (go-redis [#3228], closed *not-planned*). chronicle
runs **zero** scripts in pipelines today — every invocation is a plain `Script.Run`
(the safe path). **But** chronicle does use pipelines in production
(`webhook/redis_store.go`, `store/redis/list.go`, `store/redis/store.go`), and they
live in the **same files** as the script calls. A future developer batching a
script into one of those pipelines (go-redis exposes `pipe.EvalSha`) would silently
lose the fallback and break **only after a cache flush/failover on managed Redis** —
a latent, hard-to-reproduce bug. Cheap to prevent; worth a proportionate guard.

## What we shipped (and why not comment-only)

The initial recommendation was a doc comment. Adversarial review overruled it in
favor of a **mechanical lint guard**, because the premises held on inspection
(verified):

- The repo already runs **`golangci-lint` in CI** (`.github/workflows/ci.yml`,
  `golangci/golangci-lint-action@v8`) with a curated v2 `.golangci.yml`.
- chronicle calls scripts **exclusively** via `Script.Run`/`RunRO` and has **zero**
  bare `.Eval/.EvalSha/.EvalRO/.EvalShaRO` calls anywhere (incl. tests, jepsen) —
  so a forbidigo rule is **zero-false-positive** and config-only.

A comment sits in `loadScript`, but the mistake happens at `pipe.EvalSha(...)` — the
developer may never read it. A lint **fires at the exact dangerous call site**,
carries the rationale in its message, and the documented escape hatch (SCRIPT LOAD
+ plain EVAL in a pipeline) just requires an explicit, **auditable**
`//nolint:forbidigo`. That is strictly more robust for ~the same cost.

So we did **both** (each cheap, reinforcing):

1. **`forbidigo` rule** in `.golangci.yml` forbidding bare `EVAL/EVALSHA`
   (`\.Eval(Sha)?(RO)?\b`) with a message pointing at #3228 and the escape hatch.
2. **Caveat comments** on both `loadScript` doc comments
   (`store/redis/scripts.go`, `webhook/scripts.go`), where a script-toucher reads.

Deliberately **not** done (over-engineering for a zero-violation constraint): a unit
test (can't cleanly assert "nobody pipelined a script"; forbidigo subsumes it) or a
helper wrapper (surface area for no current need).

## Validation (end-to-end)

- `go build ./...` — OK.
- `golangci-lint run ./...` — **0 issues** on existing code (no false positives;
  the custom `forbid` list replaces forbidigo's defaults, so no `fmt.Print` noise).
- Rule **fires** correctly: a temporary `c.EvalSha(...)` probe produced
  `use of c.EvalSha forbidden because "use *redis.Script.Run/RunRO, …"`; removed,
  lint clean again.
- `go vet ./...` and `go test -short ./...` — all pass.

## Prior art

go-redis #3228 is the canonical report (closed not-planned); Redis docs say to use
plain `EVAL` (or pre-`SCRIPT LOAD`) inside a pipeline. ioredis made the identical
design choice (no NOSCRIPT retry in a pipeline, to preserve command order). Mature
clients rely on the `EVALSHA → NOSCRIPT → reload` pattern at scale; chronicle now
backs the convention with a mechanical, low-cost guard.

[#3228]: https://github.com/redis/go-redis/issues/3228
