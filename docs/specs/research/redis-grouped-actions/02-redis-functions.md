# 02 — Redis Functions: what they actually change

Redis Functions (`FUNCTION LOAD` / `FCALL`, since **Redis 7.0**) are the obvious
alternative to chronicle's `EVAL` Lua, and Redis positions them as the successor
to ad-hoc scripting. This doc separates what they genuinely change from what they
don't, because the prior decision and the recommendation both turn on that line.

## What Redis itself says

From redis.io (verified, verbatim):

- *"Redis Functions is an API for managing code to be executed on the server.
  This feature, which became available in Redis 7, **supersedes the use of
  EVAL** in prior versions of Redis."*
- *"Functions provide the same core functionality as scripts but are **first-class
  software artifacts of the database**."*
- *"Functions are also **persisted to the AOF file and replicated from master to
  replicas, so they are as durable as the data itself**."*
- The five enumerated shortcomings of `EVAL` that motivated Functions — the last
  is decisive for chronicle: *"Because they are ephemeral, **a script can't call
  another script. This makes sharing and reusing code between scripts nearly
  impossible, short of client-side preprocessing.**"*

That last line is a precise description of chronicle's `loadScript()` — the
prelude-concatenation *is* the "client-side preprocessing" Redis names as the
symptom of the limitation Functions remove. ([eval-intro], [functions-intro])

Crucially, Redis does **not deprecate `EVAL`**: its command page is *"Available
since 2.6.0"* with no deprecation marker, and Functions were added *"while
avoiding breaking changes to already-established and well-liked ephemeral
scripts."* So this is an engineering trade-off, not a forced migration.

## What Functions genuinely change (the real wins)

1. **Native shared helpers.** One library (`#!lua name=chronicle_store`) defines
   `local function` helpers (`meta_map`, `is_expired`, `validate_producer`, …)
   **once** and registers many functions over them via `redis.register_function`.
   chronicle's two-group shape (7 store + 14 webhook) maps 1:1 onto two
   libraries. This deletes the duplication quantified in
   [05](05-recommendation.md) (~58% of cached source today).
2. **Durability + replication.** Libraries are persisted to RDB/AOF and
   replicated to replicas — they **survive restart and failover**. This removes
   the *root cause* of chronicle's `NOSCRIPT` machinery: there is no volatile
   cache to repopulate, so `FCALL` never re-ships source and never hits a
   `NOSCRIPT`-class miss after a primary failover.
3. **Atomic, named versioning.** `FUNCTION LOAD REPLACE` swaps a whole library
   atomically; clients keep calling `FCALL` by name. Compare the status quo,
   where editing `common.lua` re-SHAs **every** script in the group and forces a
   one-time post-deploy `NOSCRIPT → EVAL` per script per node.
4. **Introspection.** `FUNCTION LIST [WITHCODE]`, `FUNCTION STATS` (currently
   running function + per-engine counts), and `FUNCTION DUMP`/`RESTORE`
   (serialized backup/restore) — none of which scripts offer. A minor but real
   operational nicety.

## What Functions do **not** change (verified)

- **Atomicity is identical.** *"When running a script or a function, Redis
  guarantees its atomic execution. The script's execution blocks all server
  activities during its entire time."* Same single-threaded, all-or-nothing
  semantics. ([programmability])
- **Replication is identical.** Both replicate by **effects** only (the default
  since 5.0, the *only* mode since 7.0): write commands wrapped in `MULTI/EXEC`
  shipped to replicas/AOF. This is exactly why the prior decision said "effects
  replication makes plain scripts equally safe" — and it is correct.
- **Cluster key rules are identical.** All accessed keys must be declared as key
  args and hash to one slot; chronicle's `{path}`/`{__ds}` hash tags already
  satisfy this for both surfaces.
- **Flags are *not* a Functions exclusive.** `no-writes` (→ `FCALL_RO`/`EVAL_RO`,
  runnable on replicas), `allow-stale`, `allow-oom`, `allow-cross-slot-keys` are
  available to **both** Functions and shebang (`#!lua`) `EVAL` scripts. So any
  read-replica-scaling ambition does *not* require Functions (see
  [07-semantics in 04](04-alternatives-and-prior-art.md)).

**Net:** on the axes chronicle's correctness rests on — atomicity and
replication — migrating buys **zero**. The entire case for Functions is
packaging / durability / introspection ergonomics.

## The catch: the go-redis client asymmetry

This is the single most important operational fact, and it cuts *against*
Functions for chronicle specifically:

- `*redis.Script.Run` is **self-healing**: `EVALSHA`, and on `NOSCRIPT` fall back
  to full-source `EVAL` (which re-caches). After a restart / failover /
  `SCRIPT FLUSH`, the worst case is **one extra round-trip per script per node**,
  then back to single-RT `EVALSHA`. Zero operator runbook.
- go-redis has **no `Function` type and no `FCall` auto-reload**. It exposes
  `FCall`/`FCallRo` and `FunctionLoad`/`FunctionLoadReplace` as raw client
  methods, but there is no `Run`-style "on missing-library, load and retry"
  wrapper. A fresh / flushed / just-failed-over instance with no library makes
  `FCALL` return `ERR Library not loaded`, and the **application** must:
  1. `FUNCTION LOAD REPLACE` every library at startup;
  2. handle a fresh or `FUNCTION FLUSH`'d instance; and
  3. wrap every `FCall` in a catch-`Library not loaded`-then-load-retry shim.
- In **cluster mode it's worse**: libraries are **not** auto-distributed across
  masters — the operator must load on every master node (or seed via
  `--functions-rdb`), whereas `EVALSHA`'s lazy per-node reload needs no
  orchestration.

So adopting Functions means **hand-re-implementing exactly what `Script.Run`
gives for free**, and trading chronicle's verified "self-healing, zero-runbook"
property for operator-owned bootstrap. `FUNCTION FLUSH` and `SCRIPT FLUSH` are
also separate subsystems — flushing one does not touch the other.

## Bottom line for chronicle

Functions are a genuinely better *packaging and durability* story and an
excellent structural fit for chronicle's two script groups — but they buy nothing
on correctness, they regress chronicle's failover ergonomics under go-redis, and
(per [03](03-managed-platform-support.md)) they cost the most portable deployment
surface. The duplication win they offer is real but, as [05](05-recommendation.md)
argues, largely recoverable *without* them via a build-time include.

[eval-intro]: https://redis.io/docs/latest/develop/programmability/eval-intro/
[functions-intro]: https://redis.io/docs/latest/develop/programmability/functions-intro/
[programmability]: https://redis.io/docs/latest/develop/programmability/
