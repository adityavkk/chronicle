# Redis 8 as the substrate: atomicity, durability honesty, lease safety

*Redis gives Chronicle exactly one strong, free guarantee — a single `EVAL` over one-slot keys runs to completion uninterrupted — and nothing about durability. This note separates what we may **assume**, what we must **prove via the fence**, and what we can only **document and chaos-test**, then maps each layer onto the files that already implement it.*

## Why this matters

Chronicle's correctness story rests on three claims that are easy to conflate and dangerous to confuse: that an append is atomic and serialized per stream, that no two workers ever act on the same subscription shard at the same generation, and that an acknowledged write is not lost. Only the first is a free Redis guarantee. The second is a property Chronicle *builds* on top of Redis with a monotonic fence. The third Redis explicitly refuses to provide. This note pins down the boundary so the design doc, the differential test, the formal model, and the chaos rig each own the right slice.

The conclusion in one line: **prove the fence, assume-and-differential-test the Lua atomicity, and chaos-test (never prove) durability.**

## Key findings

Each finding below is a verified claim from the substrate research, with the redis.io / Jepsen / primary-source citation that backs it and the Chronicle subsystem it constrains.

### 1. A single `EVAL`/`EVALSHA` is atomic and run-to-completion

Redis runs commands on one thread and **blocks all server activity for a script's entire runtime**. The [programmability docs](https://redis.io/docs/latest/develop/programmability/) state it verbatim: "all of the script's effects either have yet to happen or had already happened. The blocking semantics of an executed script apply to all connected clients at all times." See also [Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/).

This is the **one guarantee Chronicle may assume without testing the substrate**. A whole append — validate producer, Stream-Seq check, `ZADD` write, tail `HSET`, `PUBLISH` — wrapped in one `EVAL` is therefore atomic and serialized per stream. The obligation that remains is not to test Redis but to test the *mirror*: that the Lua re-implementation matches the Go oracle.

| Constrains | Chronicle artifact |
|---|---|
| The append mutation | [`store/redis/append.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/append.lua) mirrors [`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go) `ValidateProducer` |
| Mirror correctness | [`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) (Go oracle vs Lua) |

### 2. Atomicity is per-slot, not per-stream by magic — the `{path}` tag is the precondition

Atomic Lua execution only serializes a Chronicle mutation if **every key it touches lives in one hash slot**. The [Redis Cluster spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) hashes only the substring between the first `{` and the following `}`, and states that complex multi-key operations "are implemented for cases where all of the keys involved in the operation hash to the same slot."

Chronicle's [`store/redis/keys.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/keys.go) builds `tag(path) = "{" + escapePath(path) + "}"` for the meta/msg/prod/forks keys, and **escapes `{` and `}` in user paths** so a hostile path cannot break out of the tag and scatter keys across slots. Without this escaping, the atomicity guarantee in Finding 1 silently stops applying — the script would still run, but across slots a multi-key script is rejected (cross-slot) or, worse on a non-cluster deployment, would mask a latent assumption. The differential and fuzz tests are what keep this honest.

### 3. A write script that has begun writing cannot be killed

Once a script performs **any** write it is uninterruptible. Per [programmability](https://redis.io/docs/latest/develop/programmability/) and [eval-intro](https://redis.io/docs/latest/develop/programmability/eval-intro/): past the default 5 s `busy-reply-threshold` (formerly `lua-time-limit`) Redis logs a warning, resumes accepting connections, but replies `BUSY` to all normal commands and allows only `SCRIPT KILL` / `FUNCTION KILL` / `SHUTDOWN NOSAVE`. And `SCRIPT KILL` works only on read-only scripts: "If the script had already performed even a single write operation, the only command allowed is `SHUTDOWN NOSAVE`."

This bounds how much work a single script may do. `append.lua` already chunks `ZADD`/`unpack` to stay C-stack bounded. The testable obligation: **no bounded-but-large path** — a fork with many frames, a `decr_ref` / delete cascade, a GC sweep — may approach the 5 s threshold under load, because crossing it `BUSY`-blocks the whole server. This is a load-test target, not a proof target.

### 4. Replication is asynchronous; `WAIT` does not make Redis strongly consistent

`WAIT` blocks until a write is acknowledged in memory by N replicas, but the [`WAIT` docs](https://redis.io/docs/latest/commands/wait/) are explicit: "WAIT does not make Redis a strongly consistent store" and "it is possible to still lose a write synchronously replicated to multiple replicas." `WAIT` is a **memory-ack count, not disk durability, and never an ordering or ownership signal**.

Chronicle's [`webhook/consistency.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/consistency.go) encodes this honestly: Tier B uses `WAITAOF` (not bare `WAIT`), and the doc comment forbids any code path from reading a `WAIT` count to infer ordering or who holds the lease. Laundering a `WAIT` count into an exclusivity decision would be a correctness bug — see Pitfalls.

### 5. `WAITAOF` tightens durability to AOF fsync but still loses data on failover

The [`WAITAOF` docs](https://redis.io/docs/latest/commands/waitaof/) guarantee that returned writes "are guaranteed to be fsynced to the AOF of at least the number of masters and replicas returned" — but "similarly to WAIT, WAITAOF does not make Redis a strongly-consistent store. Unless waiting for all members of a cluster to fsync writes to disk, data can still be lost during a failover or a Redis restart." `numlocal` must be `0` if AOF is disabled.

Chronicle's Tier B plan is exactly `(NumLocal=1, NumReplicas=1)` `WAITAOF` on the fence-minting write, and `InterpretWaitAOF` in [`webhook/consistency.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/consistency.go) surfaces a typed `DurabilityShortError` (never swallowed) when acks fall short. Durability is treated as **best-effort and observable**, not guaranteed.

### 6. AOF fsync policy sets the data-loss window — honestly

The [persistence docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) name the windows directly: `appendfsync everysec` "you may lose 1 second of data if there is a disaster"; `appendfsync always` is "Very very slow, very safe" (and still batches, fsyncing once before replies); RDB-only "you should be prepared to lose the latest minutes of data." None of these change the async-replication failover loss from Finding 7 — they compound with it.

For Chronicle this means the durability of any acknowledged append or fence-write is **bounded below by the configured fsync policy** and is a deployment parameter to document and chaos-test, not a protocol invariant.

### 7. Redis Cluster explicitly cannot be strongly consistent

The [Cluster spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) states it plainly: "Redis Cluster uses asynchronous replication between nodes, and last failover wins implicit merge function"; "There is always a window of time when it is possible to lose writes during partitions"; "If the master dies without the write reaching the replicas, the write is lost forever." Its stated goal is "weak but reasonable forms of data safety."

This is the **substrate-level reason Chronicle must not rest exclusivity or no-loss on Redis acknowledgment alone**. The fence (Finding 8) is what survives this.

### 8. The Redlock debate resolves in Chronicle's favor — because Chronicle uses fencing tokens

Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) argues a lock is safe **regardless of timing** (GC pauses, clock drift, network delay) if and only if it issues a **fencing token**: a strictly monotonic number, where the resource rejects any write carrying a token lower than the highest it has seen. A paused client that resumes and writes with a stale token is rejected — so safety becomes independent of the clock. He distinguishes locks-for-efficiency (failure = wasted work) from locks-for-correctness (failure = corruption) and argues Redlock alone is unsafe for correctness because it issues no fencing token.

Chronicle's `(gen, wake_id)` generation counter **is** that fencing token. [`webhook/consistency.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/consistency.go) states the single strong guarantee "stays the monotonic `(gen, wake_id)` fence … immunizes correctness against cross-region clock skew," and [`webhook/ownership.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/ownership.go) records that the owner-epoch lease "only SUPPRESSES a deposed owner's wasted work; the `(gen,wake_id)` fence remains the safety boundary that makes any leaked duplicate harmless." Chronicle's lease is an **efficiency lock**; the fence is the **correctness mechanism** — exactly the configuration Kleppmann endorses.

### 9. antirez's rebuttal points the same way — token + check-and-set at the resource

antirez's [Is Redlock safe?](http://antirez.com/news/101) concedes the key point: "If your data store can always accept the write only if your token is greater than all the past tokens, then it's a linearizable store," and proposes a unique token used with check-and-set ("we operate the read-modify-write only if the token is still the same when we write"). He frames Redlock for efficiency/high-throughput locking and agrees that if the resource itself enforces mutual exclusion "you don't need a lock with strong guarantees."

Both authors therefore agree: **safety must come from the resource-side monotonic-token CAS, not the lock's timing.** Chronicle implements that CAS inside its webhook Lua scripts, so it sits on the safe side of *both* positions.

### 10. RedisRaft is not a mature substitute

Jepsen's [Redis-Raft 1b3fbf6 analysis](https://jepsen.io/analyses/redis-raft-1b3fbf6) found the build "essentially unusable" (infinite loops or data loss by config); nodes "would diverge from a shared prefix of the history" (split-brain); stale reads "in healthy clusters without faults." A later build resolved all but one issue but remained development software ("we can prove the presence of bugs, but not their absence"). The [project](https://github.com/RedisLabs/redisraft) itself is unreleased.

Relying on RedisRaft for correctness would import substantial unproven risk for a property the generation fence already supplies more cheaply and verifiably. It is at most a future durability option to weigh, never a current dependency.

## Techniques and their maturity

| Technique | What it is | Maturity | Effort |
|---|---|---|---|
| Per-stream single-slot Lua atomicity (assume + differential oracle) | Home a stream's keys into one slot via `{path}`; run the whole mutation in one `EVAL` for strict per-stream serialization. Assume the Redis guarantee; **test the mirror** against the Go oracle and assert single-slot homing. | mature | low |
| Monotonic fencing token at the resource (gen + CAS) | Mint a strictly-increasing generation per logical owner; reject inside the resource's own atomic write any operation with a lower generation. The lease becomes a best-effort efficiency mechanism. | mature | low |
| Durability-honest tiering (`WAIT`/`WAITAOF` as observable best-effort barrier) | Expose durability as an explicit per-deployment tier; inspect the ack pair; surface a typed short-error. The tier governs durability/freshness only, **never** exclusivity. | mature | medium |
| Failover/partition chaos testing (Jepsen-style nemesis over real Redis) | For properties that can't be proven because they depend on a write surviving async-replication failover, fault-inject and assert the *safety* property still holds even when an acked write is lost. | mature | medium |

### Per-stream single-slot Lua atomicity

Because the run-to-completion guarantee is unconditional from redis.io, it can be **assumed**. What must be **tested** is (a) that the Lua matches the Go oracle and (b) that all keys really share the slot. This is already in place: `append.lua` / `close.lua` / `create.lua` + the `{tag}` scheme in [`keys.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/keys.go) + [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go). The design-doc action is to (1) extend the differential test to drive **all** scripts and the webhook fence/CAS scripts through both implementations, not just one table, and (2) add a startup/CI assertion that every multi-key script only ever touches one-slot keys (the hostile-path fuzz already motivated `escapePath`). This converts an assumed Redis guarantee into a continuously-verified Chronicle property. Cites: [programmability](https://redis.io/docs/latest/develop/programmability/), [cluster spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/).

### Monotonic fencing token at the resource

This is Chronicle's existing per-`(subId,g)` `(gen, wake_id)` fence ([`webhook/shard.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/shard.go), [`webhook/scripts.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts.go)) with the owner-epoch lease layered above only to suppress deposed-owner work ([`webhook/ownership.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/ownership.go)). The doc must state explicitly: **no code path may infer exclusivity or ordering from a lease TTL, a `WAIT` count, or wall-clock — only from the fence** (consistency.go already forbids laundering a `WAIT` count into an exclusivity decision). Critically, the fence's CAS must live **inside the same per-slot Lua script** as the work it gates, so it inherits the atomicity of Finding 1; that combination is what makes a leaked duplicate wake provably harmless. Cites: [Kleppmann](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html), [antirez](http://antirez.com/news/101).

### Durability-honest tiering

Tier A = no barrier (RPO = full async replication lag); Tier B = `WAITAOF` (`numlocal=1`, `numreplicas`) on the fence-minting write, with the returned ack pair inspected and a typed `DurabilityShortError` surfaced when it falls short; Tier C = read-your-writes via a freshness token. The tier governs **durability/freshness only, never exclusivity**, because redis.io states `WAIT` and `WAITAOF` "do not make Redis a strongly-consistent store" and writes can still be lost on failover. Already designed in [`webhook/consistency.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/consistency.go) (`DurabilityFor` / `InterpretWaitAOF` / `DurabilityShortError`). Remaining doc work: (1) document each tier's RPO honestly (Tier A = replication lag, Tier B ≈ AOF fsync interval, both still lose data if all members fail before fsync per the `WAITAOF` caveat), and (2) verify the never-swallowed short-error reaches an operator-visible metric. Cites: [WAIT](https://redis.io/docs/latest/commands/wait/), [WAITAOF](https://redis.io/docs/latest/commands/waitaof/), [persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).

### Failover/partition chaos testing

Chronicle already ships a Jepsen-style checker (`jepsen/checker`: `model_fence.go`, `model_shard.go`, `nemesis.go`). The work is to **extend the nemesis to drive a real Redis Sentinel/Cluster failover** (not just a model) and assert: after a failover that loses the latest fence `HINCRBY`, the recovery sweep + fence still prevent two workers from committing at the same generation, and at-least-once delivery is preserved. The GKE rig is the place to run the cloud failover variant — at roughly \$4/hr, and **always tear it down** (a crashed session once stranded a cluster). The point is to validate that lost-write windows degrade to at-least-once / duplicate-suppressed-by-fence, never to a safety violation. Cites: [Jepsen Redis-Raft](https://jepsen.io/analyses/redis-raft-1b3fbf6), [cluster spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/).

## Recommendation for Chronicle

Formalize a **three-layer correctness/durability model** in the design doc and keep all three layers strictly separated.

1. **GUARANTEED-by-Redis (assume the substrate, but test the mirror).** Per-stream serialization and atomic visibility of each mutation, because a single `EVAL` over one-slot keys runs to completion uninterrupted. Obligation: keep the Go-oracle-vs-Lua differential test ([`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go)) exhaustive across every script — including the webhook fence/CAS scripts in [`webhook/scripts.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts.go) — and assert single-slot key homing in [`store/redis/keys.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/keys.go).

2. **SAFE-because-of-fencing (prove/model, do not trust Redis timing).** Subscription exclusivity and no-double-commit rest entirely on the monotonic `(gen, wake_id)` fence enforced by a CAS **inside** the per-slot Lua script. The TTL lease and owner-epoch ([`webhook/ownership.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/ownership.go)) are demoted to efficiency-only and may **never** be read for correctness, ordering, or who-holds-the-lease. This is the Kleppmann fencing-token pattern that both Kleppmann and antirez agree is the safe construction, and it makes correctness immune to Redis failover, clock skew, and GC pauses. This layer is the natural target for the TLA+ subscription model and trace validation (see [./01-tla-and-trace-validation.md](./01-tla-and-trace-validation.md)) and the Porcupine linearizability check.

3. **DURABILITY-HONEST (document + chaos-test, do not attempt to prove).** No acknowledged write is guaranteed to survive failover even with `WAITAOF`. Expose RPO explicitly via the existing Tier A/B/C plan in [`webhook/consistency.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/consistency.go), surface `DurabilityShortError` to operators, and validate via a real Redis failover/partition nemesis (`jepsen/checker`) that any lost fence-write degrades only to at-least-once delivery (deduped by the fence), never to a safety violation.

**Do not adopt RedisRaft to chase strong consistency.** Jepsen found it immature with split-brain and data loss; the fence already provides the only correctness property that matters, at lower cost and with verifiable Lua atomicity.

Net: **prove the fence, assume + differential-test the Lua atomicity, chaos-test (never prove) durability.** This boundary is the substrate complement to the invariants catalogued in [../INVARIANTS.md](../INVARIANTS.md) and the layering in [../DESIGN.md](../DESIGN.md).

## Pitfalls

- **Conflating durability with consistency.** `WAIT` and `WAITAOF` improve RPO but redis.io explicitly says neither makes Redis strongly consistent. Reading a `WAIT`/`WAITAOF` ack count to decide ordering or lease ownership is a correctness bug. `consistency.go` already forbids this, but the doc must call it out so a future change cannot quietly reintroduce it.
- **Trusting the lease TTL or wall-clock for exclusivity.** A GC pause or clock jump can let an "expired" lease holder still believe it owns the lock. Only the monotonic fence makes a stale write harmless. Any new code path that acts on lease-held-ness without re-checking the fence reintroduces the Redlock unsafety.
- **Assuming a primary-acked write is durable.** Under async replication (single-node failover, Sentinel, or Cluster) the cluster spec says an acked write can be "lost forever" if the primary dies before propagating — worst on the minority partition side. Treat acked-but-unreplicated fence writes as potentially lost and rely on the recovery sweep + fence to converge.
- **Long or unbounded Lua scripts.** A write script past the 5 s `busy-reply-threshold` cannot be killed (only `SHUTDOWN NOSAVE`) and `BUSY`-blocks the entire server. A fork with huge frame counts or a GC cascade must stay bounded. `append.lua` already chunks `unpack`; the same discipline must hold for `delete` / `decr_ref` cascades.
- **Pub/Sub wake loss masquerading as a durability property.** Redis pub/sub is fire-and-forget (`notify.go` already adds a ~1 s defensive poll). Do not let the design doc treat a delivered `PUBLISH` as a guarantee — it is best-effort, and the fence + recovery sweep are what make missed wakes recoverable.
- **Reaching for RedisRaft to get "real" strong consistency.** Jepsen documented split-brain, stale reads, and data loss; importing it would add unproven risk for a property the fence already supplies.

## Open questions

- **What is the actual deployment substrate's fsync and replication config** — Redis 8 OSS single node vs Sentinel vs Cluster vs Redis Software HA? The Tier B plan hardcodes `NumReplicas` semantics (1 in HA, 0 on the local rig); the doc needs the real topology to state a concrete RPO per tier.
- **Does the production `WAITAOF` path actually have AOF enabled** on the primary and an in-region replica? `WAITAOF`'s `numlocal` cannot be nonzero without AOF, so Tier B silently degrades to Tier A durability if AOF is off. This should be asserted at startup.
- **Is the `(gen, wake_id)` fence CAS guaranteed to live inside the SAME single-slot Lua script** as the work it gates, in *every* webhook path (`arm_wake`, `claim`, `ack`, `release`, recovery sweep)? If any fence check and the gated write are split across two round-trips, the atomicity guarantee no longer covers the fence and a TOCTOU window opens. The presence of a `toctou_inline_test.go` suggests this was already a concern worth re-auditing.
- **Under a real Sentinel/Cluster failover that loses the latest fence `HINCRBY`, does the recovery sweep deterministically re-mint a STRICTLY higher generation** (never reuse), so that a slow pre-failover worker is fenced out? This is the key safety property to add to the Jepsen failover nemesis.
- **What is the targeted Redis version's exact `WAITAOF`/cluster behavior** — Redis 8 vs the 7.x where `WAITAOF` landed? Verify the cited docs (redis.io "latest") match the deployed major version, since fsync-on-replica semantics evolved.
