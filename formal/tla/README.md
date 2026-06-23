# SubscriptionFence — TLA+ model of Chronicle's subscription control plane

Issue #37 · Epic #25 · Track `formal-verification` · Phase P2.3

`SubscriptionFence.tla` is the **model side of Chronicle's central
concurrent-protocol claim**: it model-checks one subscription's
wake/claim/ack/release fence, lease expiry, the due-set outbox, and the four
non-atomic crash windows under TLC's exhaustive interleaving of concurrent
workers. It re-states, under exhaustive interleaving, the single-holder safety
that the Porcupine oracle `jepsen/checker/model_fence.go` only samples
(NP-hard, capped at 3–5 workers).

The fence/generation algebra is deliberately **time-free** (INV-JEP-REC-01):
`lease_until` is a *modeled* discrete deadline that gates only the claim
grant/BUSY split and the `ExpireLease` guard. The safety invariants rest on the
monotone generation alone, never on a clock.

## How to run

Java 11+ is required. The toolbox jar is pinned and downloaded on demand; it is
**not** committed.

```sh
# from this directory (formal/tla):
make tlc          # parse + both faithful configs (the CI lane)
make tlc-1x1      # 1 sub x 1 worker fast lane (smoke)
make tlc-2x2      # 2 sub x 2 worker exhaustive interleaving
make fault-expire # INV-FENCE-04 negative test: MUST violate SingleHolder
make fault-lease  # INV-LEASE-02 negative test: MUST violate NoStrandedLease
make coverage     # crash-window reachability: each NotWx MUST be violated
```

Equivalent raw invocation (what `make` runs):

```sh
curl -L -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config SubscriptionFence_1x1.cfg MC_1x1.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config SubscriptionFence_2x2.cfg MC_2x2.tla
```

`-deadlock` disables deadlock detection. **Quiescence is benign here**: a
subscription that is idle, caught up to its tail, with no in-flight wake and the
crash/clock/gen budgets exhausted has every action disabled, which is a normal
terminal state of the control plane, not a stuck-mid-protocol deadlock. We do
not declare any other terminal state benign.

`MC_1x1.tla` / `MC_2x2.tla` are thin wrappers that `EXTENDS SubscriptionFence`
and declare `Sym == Permutations(Workers) \cup Permutations(Subs)` so TLC
quotients the state space by the interchangeable worker/sub identities.

## Results (TLC 2.19, tla2tools v1.7.4)

| Config | Instance | States (distinct) | Depth | Verdict |
|---|---|---|---|---|
| `SubscriptionFence_1x1.cfg` | 1 sub × 1 worker, MaxGen=3 MaxClock=3 MaxCrashes=2 | 10,974 | 17 | `Inv` + both action props HOLD |
| `SubscriptionFence_2x2.cfg` | 2 sub × 2 worker, MaxGen=2 MaxClock=2 MaxCrashes=1 | 906,146 | 24 | `Inv` + both action props HOLD |
| `SubscriptionFence_fault_expire.cfg` | 2 worker, `ExpireClearsFence=FALSE` | 197 to CEX | 5 | **SingleHolder VIOLATED** (intended) |
| `SubscriptionFence_fault_lease.cfg` | 2 worker, `ClaimReScores=FALSE` | 22 to CEX | 3 | **NoStrandedLease VIOLATED** (intended) |
| `SubscriptionFence_coverage_W{1..4}.cfg` | 2×2 | — | — | each `NotWx` VIOLATED ⇒ window reached |

`Inv == TypeOK ∧ SingleHolder ∧ AtMostOneInflightWake ∧ CursorBounded ∧
StaleInert ∧ NoStrandedLease`. The two action properties are
`GenMonotoneProp == [][GenMonotone]_vars` and
`CursorForwardOnlyProp == [][CursorForwardOnly]_vars`.

## Invariant ⇆ catalog map

| Spec operator | Catalog | Statement |
|---|---|---|
| `SingleHolder` | INV-FENCE-01 | At no state do two distinct workers hold an ack-acceptable token for one sub. The central safety property. |
| `GenMonotone` (action) | INV-FENCE-02 | `cur.gen` is non-decreasing across every step. |
| `StaleInert` | INV-FENCE-03 | A held token whose gen ≠ cur.gen is never ack-acceptable (a stale-gen op is inert). Reinforced structurally: every mutating ack/release is guarded by `~Fenced`. |
| `ExpireClearsFence` toggle | INV-FENCE-04 / INV-LEASE-01 | `ExpireLease` idles + clears `wake_id` without rotating gen; the fault (FALSE) leaves a claimable fence and breaks `SingleHolder`. |
| `CursorForwardOnly` (action) + `CursorBounded` | INV-CURSOR-01 | Per sub the cursor never decreases; an ack only advances on `OffsetGreater`. |
| `AtMostOneInflightWake` | INV-WAKE-02 | No two ack-acceptable tokens with different `(gen,wake)` for one sub; `Arm` mints only from idle. |
| `NoStrandedLease` + `ClaimReScores` toggle | INV-LEASE-02 | A live sub whose holder is gone still has its lease member, so it is reclaimable; the fault (FALSE) ZREMs at claim and strands it. |

## Action ⇆ shipped source mirror

Each action transcribes the guard of exactly one shipped Lua/Go mirror. A
non-granting reply (BUSY / FENCED / STALE / NOSUB / ACTIVE) is a stuttering
no-op — it grants nothing and mutates no durable state, which is exactly
INV-FENCE-03 — so only the **granting** branch is a state change.

| Spec action | Source mirror | Guard transcribed |
|---|---|---|
| `Arm(s)` | `webhook/scripts/arm_wake.lua` | only `phase=idle`: `HINCRBY generation +1`, set `wake_id`, `phase=waking`; webhook arms a lease, pull-wake sets `wake_event_sent_ns=0`. Non-idle ⇒ BUSY (no-op). |
| `Claim(w,s)` | `webhook/scripts/claim.lua` + `state.go ClaimRotatesFence` | BUSY iff an unexpired live lease is held; else grant — coalesce when `phase=waking ∧ wake≠""`, otherwise rotate (`HINCRBY +1`, fresh wake). |
| `Ack(w,s,done,off)` | `webhook/scripts/ack.lua` (+ `common.lua fenced`, `offset_greater`) | fence-check first is the SOLE gate; OK advances the cursor forward-only; `done='1'`→idle+clear, `done='0'`→heartbeat. Fenced ⇒ no-op. |
| `Release(w,s)` | `webhook/scripts/release.lua` | fenced like ack; idles the sub, ZREMs the schedule + due mark. |
| `ExpireLease(s)` | `webhook/scripts/expire_lease.lua` (server step) | `phase∈{live,waking} ∧ lease≠0 ∧ reached`: clear holder/wake/lease, idle, re-owe; **never** `HINCRBY` (INV-FENCE-04). |
| `WakeAppend`/`WakeStamp` | `manager.go writeWakeEvent` + `record_wake_sent.lua` | the two non-atomic halves of the pull-wake Go follow-up; stamp is fenced on `(gen,wake)` (STALE ⇒ no-op). |
| `WebhookEmit` | `manager.go writeWakeEvent` (webhook arm) | clears the pending Go-follow-up marker (no durable fence stamp in scope). |
| `SweepReemit(s)` | `manager.go sweepOnce` (lines ~1180 and ~1196) | re-emit the SAME `(gen,wakeID)` for a stranded pull-wake (T1 and T4 branches). |
| `DueDrain(s)` | `state.go DecideDue` | reconcile a due mark: DueSkip (non-idle), DueClear (idle, caught up). The load-bearing clear (claim_due never ZREMs). |
| `Tick` | the discrete clock | advances `clock`, enabling `LeaseExpired`. |
| `Crash(w)` | the four non-atomic windows | drops the volatile Go follow-up (`pending`) and worker `w`'s in-memory tokens; durable Redis state (sub hash, dueMark, lease member) survives. |

`Fenced(...)` is the byte-for-byte mirror of `common.lua fenced` /
`state.go FenceDecision`. `ClaimRotatesFence` and `OffsetGreater` mirror their
`state.go` namesakes. `NoToken`/`leaseMem` model the in-memory token and the
durable lease-ZSET member.

## Crash windows ⇆ manager.go recovery branch

A `Crash` fires BETWEEN a durable Lua write and its non-atomic Go follow-up —
the boundary that defeats review, unit tests, and Porcupine histories. The
durable Redis state survives; the Go follow-up and the crashed worker's
in-memory token are lost. The four windows are reachable on the 2×2 config
(`make coverage`):

| Window | Spec witness | manager.go recovery |
|---|---|---|
| **W1** arm-before-emit | `WindowW1`: pull-wake `phase=waking`, `wake_sent=0`, `pending="emit"` | `sweepOnce` re-emits when `wake_event_sent_ns==0` (`manager.go:1180`, INV-RECOVER-01). |
| **W2** lua-commit-then-Go stamp | `WindowW2`: `pending="stamp"` (appended, not yet stamped) | `record_wake_sent.lua` is fenced on `(gen,wake)`; a superseded stamp is STALE. |
| **W3** post-emit / never-claimed (T4) | `WindowW3`: pull-wake `phase=waking`, `wake_sent=1`, `lease_until=0` | `sweepOnce` re-emits once stale (`now-sent_ns > 3*sweepInterval`, `manager.go:1196`, INV-RECOVER-02). |
| **W4** claim-before-ack | `WindowW4`: `phase=live`, lease set, no ack-acceptable holder, lease member survives | `claim_due.lua` re-scores the member forward, never ZREM; the lease falls due again and is reclaimed (INV-LEASE-02). |

The `3*sweepInterval` threshold (W3) is modeled only as a crash *point*; its
liveness/eventual-re-emit claim (LB-5 magic number) is the deferred
liveness/Ownership sibling, not this issue.

## Bounds rationale

TLC checks **bounded instances**; the size-independent (all-N) guarantee is the
deferred Apalache inductive-invariant sibling (research/01 §Phase 3).

- **N = 2 workers** is the smallest scope that exercises the cross-worker races
  the single-holder property is about: rotate-on-expired-takeover and the
  deposed-holder late-ack race both require a second worker. Adding a third
  worker cannot reach a new *kind* of fence state — the fence register holds a
  single `(gen,wake)`, and any third worker is symmetric to the second under
  `Permutations(Workers)` — so N=2 is the cover-all scope for this register
  (matching the Porcupine model's per-key argument).
- **2 subs** exercises that the per-subscription fences are independent (the
  model never couples two subs), with `Permutations(Subs)` quotienting the
  symmetry.
- **MaxGen / MaxClock / MaxCrashes** are state-space ceilings, enforced by both
  per-action guards and `CONSTRAINT StateConstraint`, so the model is finite and
  TLC terminates. The 1×1 lane uses larger ceilings (3/3/2) because its base
  state is far smaller; the 2×2 lane uses 2/2/1 to keep the exhaustive run near
  a million states (≈15 s, fits CI).

## Files

- `SubscriptionFence.tla` — the module (state, eight actions + Tick + Crash, the
  six safety operators, two action properties, four crash-window witnesses).
- `MC_1x1.tla` / `MC_2x2.tla` — TLC harness wrappers (symmetry sets).
- `SubscriptionFence_1x1.cfg` / `_2x2.cfg` — the two faithful configs.
- `SubscriptionFence_fault_expire.cfg` — INV-FENCE-04 negative test.
- `SubscriptionFence_fault_lease.cfg` — INV-LEASE-02 negative test.
- `SubscriptionFence_coverage_W{1..4}.cfg` — crash-window reachability witnesses.
- `Makefile` — `make tlc` (CI lane), `fault-*`, `coverage`.
