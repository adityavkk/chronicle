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
- `Makefile` — `make tlc` (CI lane), `fault-*`, `coverage`, plus `make tlc38`.

---

# Ownership + the fence-on/fence-off layering proof + the liveness encoding (issue #38)

Issue #38 · Epic #25 · Phase P2.4 / P2.5 · discharges INV-OWNER-01, INV-OWNER-02,
INV-WAKE-01, INV-RECOVER-01/02, INV-DUE-01, INV-JEP-L1-01/02.

Chronicle authorises every schedule-mutating write through **two** fences. The
inner one is the per-subscription `(generation, wake_id)` fence (`SubscriptionFence`,
#37). The outer one is the per-slot **owner-epoch** CAS (`claim_shard.lua` /
`owner_fenced`, inlined into `arm_wake` / `ack` / `expire_lease` / `schedule_retry`
/ `release`). #38 adds the owner-epoch register, composes the two, and discharges
the two claims that were prose-only:

1. **The owner-epoch fence is an optimization, never a correctness dependency** —
   single-holder is upheld by the `(gen,wake)` fence **alone**. (INV-OWNER-02,
   [FINDINGS.md "layering claim unproven"](../../docs/specs/formal-verification/FINDINGS.md).)
2. **The `3*sweepInterval` T4 re-emit threshold** is sufficient for eventual
   re-emit and cannot create a double-live holder. (LB-5,
   [FINDINGS.md LB-5](../../docs/specs/formal-verification/FINDINGS.md#lb-5-the-3--sweepinterval-t4-re-emit-threshold-is-an-unvalidated-magic-number).)

## How to run

```sh
make tlc38              # the whole #38 CI lane (all of the below)

make ownership          # standalone owner-epoch CAS, 1 + 2 slots (INV-OWNER-01)
make composed-on        # layering proof, owner_fenced Real,       1 slot
make composed-off       # layering proof, owner_fenced AlwaysPass, 1 slot (load-bearing)
make composed-on-2x2    # layering proof, Real,       scaled 2 subs x 2 workers x 2 slots
make composed-off-2x2   # layering proof, AlwaysPass, scaled 2/2/2 (load-bearing, scaled)
make composed-deposed   # NON-VACUITY: Deposed-owner-late-write MUST be reachable
make liveness           # leads-to under weak fairness (INV-WAKE-01, RECOVER-01/02)
make liveness-nofair    # NEGATIVE: leads-to MUST FAIL without fairness (non-trivial)
make liveness-safety    # NoDoubleLiveHolder under SlowConsumer + re-emit (INV-JEP-L1-01)
make liveness-sensitivity # LB-5: NoDoubleLiveHolder MUST still hold at ReemitTicks=0
```

## The layering proof (INV-OWNER-02)

`Composed.tla` reproduces the `SubscriptionFence` state machine inline and wires
`owner_fenced(slot, me, epoch)` as a guard onto **every** mutating subscription
action (`Arm`, `Ack`, `Release`, `ExpireLease` — the inlined-Lua set; `Claim` is
the load-balanced external/hot path and is **not** in that set, matching the
code). The owner register is driven by `ClaimShard` / `Depose`, so a caller can be
deposed mid-protocol. The proof is the **twin model check** under a single
`FENCE_MODE` switch that differs in nothing else:

| Config | `FENCE_MODE` | Owner fence | `[]SingleHolder` |
|---|---|---|---|
| `Composed_FenceOn.cfg`  | `Real`       | enforced | **HOLDS** |
| `Composed_FenceOff.cfg` | `AlwaysPass` | **deleted** (`owner_fenced ≡ FALSE`) | **HOLDS** |

The **AlwaysPass run is load-bearing**: with the outer fence gone, the only thing
preventing two ack-acceptable holders is the inner `(gen,wake)` fence — its holding
is exactly "owner-epoch is optimization-only." `Composed_DeposedWitness.cfg` proves
the run is **non-vacuous**: the Deposed-owner-late-write (an owner-scoped caller
whose slot was transferred, carrying a stale epoch, attempting a mutation) is
reachable under AlwaysPass, so the run genuinely exercises the write the owner
fence would have stopped. `SingleHolder` absorbs it via the `(gen,wake)` fence and
the empty `wake_id` left by `expire_lease` (the INV-FENCE-04 escape hatch).

## The liveness / fairness encoding (INV-WAKE-01, RECOVER, LB-5)

`Liveness.tla` is a deliberately small pull-wake model carrying just enough state
to state the temporal properties under weak fairness of the sweep / due-drain
loops:

- `PendingWorkLeadsToWake == [](idle ∧ HasPendingWork) ~> WakeIssued` — the headline
  liveness (INV-WAKE-01). The **no-fairness** spec (`Liveness_NoFair.cfg`,
  `SpecNoFair`) makes this **fail**, confirming the property is non-trivial.
- `StrandedT1LeadsToReemit` / `StrandedT4LeadsToReemit` — a pull-wake stranded in
  the arm-before-emit window (crash window 1, `wake_sent=0`) or the post-emit T4
  window (`wake_sent=1`, no lease, never claimed) is eventually re-emitted under
  sweep fairness (INV-RECOVER-01/02).
- `NoDoubleLiveHolder == []SingleHolder` under the `SlowConsumer` + re-emit
  scenario (`Liveness_Safety.cfg`, `SafetySpec` with `LeaseLapse`): a re-emit to a
  still-live slow consumer never yields two ack-acceptable holders — the duplicate
  degrades to **at-least-once** (INV-JEP-L1-01), backstopped by the `(gen,wake)`
  fence and the empty `wake_id` left by `expire_lease`.

### LB-5 resolution — the `3*sweepInterval` threshold

The wall-clock test `now - wake_event_sent_ns > 3*sweepInterval` is modeled
discretely: a stranded-emitted pull-wake ages one `staleTicks` per sweep tick, and
the T4 re-emit becomes enabled only once `staleTicks > ReemitTicks` (`ReemitTicks
= 3` = the `3*sweep` floor). This makes **`3x` a tuning knob for _when_ re-emit
fires** — a liveness/efficiency heuristic, **not** a safety guarantee. The
`Liveness_Sensitivity.cfg` config re-runs the safety check at the most aggressive
`ReemitTicks = 0` (re-emit as early as possible) and `NoDoubleLiveHolder` **still
holds**: **safety is threshold-independent**. The `(gen,wake)` fence is the
backstop regardless of the threshold value. This discharges LB-5: the `3x` is
validated as sufficient-for-eventual-re-emit (the leads-to holds) and
safe-against-double-live-holder at any value (the sensitivity run).

> **CONSTRAINT-vs-liveness note.** The liveness configs declare **no** state
> `CONSTRAINT`: the state space is already bounded by the action guards (`DueFire`
> / `Claim` require `gen < MaxGen`; `SweepTick` requires `staleTicks < MaxStale`),
> so a CONSTRAINT would be redundant — and a state CONSTRAINT during *liveness*
> checking can truncate behaviors and mask a real leads-to violation (Specifying
> Systems §14.3.5). Omitting it makes TLC check the temporal properties on the
> genuinely-complete state graph.

## Results (TLC 2.19, tla2tools v1.7.4)

| Config | Instance | Distinct states | Depth | Verdict |
|---|---|---|---|---|
| `Ownership_1slot.cfg` | 2 replicas × 1 slot | 27 | 8 | `OInv` + epoch props HOLD |
| `Ownership_2slot.cfg` | 2 replicas × 2 slots | 123 | 9 | `OInv` + epoch props HOLD |
| `Composed_FenceOn.cfg`  | 2 wkr × 1 sub × 1 slot, `Real`       | 53,566 | 17 | `Inv` (`SingleHolder`) HOLDS |
| `Composed_FenceOff.cfg` | 2 wkr × 1 sub × 1 slot, `AlwaysPass` | 53,566 | 17 | `Inv` (`SingleHolder`) **HOLDS (load-bearing)** |
| `Composed_FenceOn_2x2.cfg`  | 2/2/2, `Real`,       symmetry | 25,972,640 | 29 | `Inv` HOLDS (~7.5 min) |
| `Composed_FenceOff_2x2.cfg` | 2/2/2, `AlwaysPass`, symmetry | 25,972,640 | 29 | `Inv` **HOLDS (load-bearing, scaled)** (~10.5 min) |
| `Composed_DeposedWitness.cfg` | `AlwaysPass`, 1 slot | — to witness | 2 | `NotDeposedLateWrite` VIOLATED ⇒ scenario reachable (intended) |
| `Liveness.cfg` | 1 worker, `ReemitTicks=3` | 10 | 8 | all 4 leads-to props HOLD |
| `Liveness_NoFair.cfg` | `SpecNoFair` | — to CEX | — | `PendingWorkLeadsToWake` **VIOLATED** (intended; non-trivial) |
| `Liveness_Safety.cfg` | 2 workers, `ReemitTicks=3`, `SafetySpec` | 83 | — | `NoDoubleLiveHolder` HOLDS |
| `Liveness_Sensitivity.cfg` | 2 workers, `ReemitTicks=0` | 83 | — | `NoDoubleLiveHolder` HOLDS (threshold-independent) |

The two `*_2x2` configs run against `MC_ComposedSym.tla` (symmetry over
Workers ∪ Subs ∪ Slots) so the un-quotiented 2/2/2 state space stays tractable; the
1-slot runs and the Deposed witness use the un-quotiented `MC_Composed.tla` for
legible named traces. The Real and AlwaysPass runs reach the **identical**
reachable durable-state set (same distinct count) — AlwaysPass simply enables more
transitions into it — so the layering proof compares like with like.

## Action ⇆ shipped source mirror (#38 additions)

| Spec action | Source mirror | Guard transcribed |
|---|---|---|
| `ClaimShard(me,h)` | `webhook/scripts/claim_shard.lua` (`webhook/ownership.go SlotClaim`) | BUSY iff a live foreign owner; else grant — `owner=me` RENEW (epoch kept), `owner≠me` TRANSFER (`HINCRBY owner_epoch +1`, strictly up). |
| `OwnerVerdict` / `OwnerFenced` | `webhook/scripts/check_owner.lua` / `common.lua owner_fenced` | UNOWNED / FENCED (owner≠me ∨ epoch mismatch) / OWNER; `epoch=''`(=0) short-circuits to pass (external/hot path). |
| `Depose(h)` | membership drop | the slot lease lapses; `owner_id`/`owner_epoch` persist (fenced only by a later TRANSFER bump). |
| `Arm`/`Ack`/`Release`/`ExpireLease` owner guard | inlined `owner_fenced(...)` at the top of each mutating Lua | a FENCED owner check is a no-op (`return {'FENCED'}`), exactly the inlined-Lua set; `Claim` is intentionally un-guarded (load-balanced external path). |
| `DueFire` / `SweepEmitT1` / `SweepEmitT4` / `SweepTick` | `manager.go` `DecideDue` / `sweepOnce` re-emit branches | DueFire = idle+pending arm; T1 = `wake_event_sent_ns==0` immediate re-emit; T4 = `now-sent_ns > 3*sweepInterval` (modeled as `staleTicks > ReemitTicks`). |
| `LeaseLapse` | `expire_lease.lua` on a slow/crashed holder | idle + clear `wake_id`, **gen unchanged** (INV-FENCE-04): the slow holder's token is fenced by the empty `wake_id`. |

## #38 files

- `Ownership.tla` — the owner-epoch register + `ClaimShard`/`Depose`/`OTick`, the
  `OwnerVerdict` operator, `SingleOwner` + epoch action-properties, BUSY/TRANSFER
  reachability witnesses.
- `MC_Ownership.tla` + `Ownership_{1,2}slot.cfg` — standalone INV-OWNER-01 runs.
- `Composed.tla` — `SubscriptionFence` × `Ownership` with `owner_fenced` wired as a
  guard, the `FENCE_MODE` switch, and the `DeposedLateWriteEnabled` witness.
- `MC_Composed.tla` (un-quotiented, for 1 slot + witness) and `MC_ComposedSym.tla`
  (symmetry-quotiented, for the scaled 2/2/2 runs).
- `Composed_FenceOn.cfg` / `Composed_FenceOff.cfg` and their `*_2x2.cfg` scaled
  twins; `Composed_DeposedWitness.cfg` (non-vacuity).
- `Liveness.tla` + `MC_Liveness.tla` + `MC_LiveRace.tla` (two-token-race witness).
- `Liveness.cfg`, `Liveness_NoFair.cfg`, `Liveness_Safety.cfg`,
  `Liveness_Sensitivity.cfg`.

---

# Membership + HRW slot-ownership convergence + L3 lease-tail refinement (issue #40)

`Membership.tla` + `LeaseTail.tla` close the **L2/L4/L5 distributed-convergence
surface** that had no exhaustive check (INVARIANTS.md: "the distributed
membership/HRW/slot-reconcile convergence — the heart of the horizontal-scale
design — has no exhaustive consistency check"). `Ownership.tla` (#38) modeled one
slot's owner-epoch CAS **in isolation**; this models the whole loop — the members
ZSET (heartbeat + lease eviction), the per-replica HRW computation, and the
`slotReconcile` claim loop — and proves convergence under fairness.

## How to run

```sh
make tlc40                  # the whole #40 CI lane (all of the below)
make membership-safety      # AtMostOneOwner / NoLiveSplitBrain / Epoch* over full churn
make membership-convergence # <>[]Converged after churn stops (under WF/SF fairness)
make membership-nofair      # negative control: WITHOUT fairness convergence MUST FAIL
make membership-witness     # non-vacuity: a real TRANSFER and a real zero-owner GAP
make leasetail              # L3: NoSpuriousLease + a stranded lease is restored
make leasetail-witness      # non-vacuity: the stranded-lease state is reachable
make alloy                  # the two Alloy relational models (one dir up, formal/alloy)
```

## The convergence claim (INV-JEP-L4-01 / INV-HRW-01)

After membership churn stops, under **weak fairness of the slot-reconcile claim
loop + the clock + StopChurn, and STRONG fairness of each survivor's heartbeat**,
every slot ends with **exactly one unexpired owner = its HRW target, and stays
there (no oscillation)**: `EventualConvergence == churnStopped ~> []Converged`.

Two modeling decisions are load-bearing and documented in the spec header:

- **Relative remaining-TTL time**, not an absolute clock. A member/slot lease is
  a "ticks remaining" counter; a heartbeat/claim resets it to full TTL, a `Tick`
  decrements toward 0. This removes the absolute-clock ceiling that otherwise
  makes "a renewed lease stays live forever" inexpressible in a bounded model,
  and it is faithful (the store only ever uses `score − now`, the difference).
- **The heartbeat-headroom gate on `Tick`** is the model-level encoding of
  `CheckOwnershipConfig`'s `heartbeatInterval < memberLeaseTTL/2` and
  `slotReconcileInterval ≤ heartbeatInterval` (INV-MEMBER-01): `Tick` is disabled
  if it would lapse an *alive* replica's member lease, or an *alive HRW-target*
  owner's slot lease — so time can never starve a survivor. SF(Heartbeat) then
  forces the renew while `Tick` is blocked. A **bounded churn budget** (`MaxChurn`)
  caps pre-stop transfers so the epoch ceiling can never block the post-churn
  convergence transfers (the state-constraint-during-liveness hazard).

`membership-nofair` removes the fairness and TLC finds a counterexample, proving
the leads-to is non-trivial. `membership-witness` proves a genuine TRANSFER
(epoch ≥ 2) and a genuine zero-owner coverage gap (a previously-claimed slot
whose owner crashed and whose lease lapsed) are each really reached — so the
convergence run is not vacuous.

## The L3 lease-tail-drop refinement (INV-LR-01 / INV-JEP-L3-01)

`LeaseTail.tla` models the lease as recoverable from the durable record even
after the schedule ZSET entry is `ZREM`med: a `DropLeaseTail` removes the ZSET
entry with the durable hash intact, and `ReconcileLease` re-derives it from the
durable hash, **phase-conditioned** (only restores a still-live/waking record)
and **idempotent**. It checks `NoSpuriousLease` (reconcile never invents a lease
absent from the durable record — the lease analogue of INV-RECOVER-04) and the
recoverability liveness `StrandedLease ~> (Recovered ∨ Idled)` under WF of the
reconcile loop.

## Results (TLC 2.19, tla2tools v1.7.4)

| Run | Verdict |
|---|---|
| `membership-safety` (Inv + Epoch action-props, 3 replicas × 2 slots) | No error — 21038 distinct states |
| `membership-convergence` (`<>[]Converged` under fairness) | No error — both leads-to branches hold |
| `membership-nofair` (negative control) | Temporal property violated (as required) |
| `membership-witness` (NotTransferReachable / NotZeroOwnerGapReachable) | both violated (as required — non-vacuous) |
| `leasetail` (Inv + `LeaseRecoverable`) | No error |
| `leasetail-witness` (NoStranded) | violated (as required — stranded state reachable) |

## #40 files

- `Membership.tla` — members ZSET (relative-TTL) + HRW argmax + `ReconcileClaim`
  CAS + the churn-budget + the heartbeat/slot-lease headroom gates; safety
  invariants, the convergence leads-to, the no-fairness spec, and the
  TRANSFER / zero-owner-gap reachability witnesses.
- `MC_Membership.tla` — pins a concrete distinct-per-(replica,slot) HRW `Score`.
- `Membership_Safety.cfg`, `Membership_Convergence.cfg`, `Membership_NoFair.cfg`,
  `Membership_Witness.cfg`, `Membership_WitnessGap.cfg`.
- `LeaseTail.tla` + `LeaseTail.cfg` — the L3 lease-tail-drop refinement.
- Alloy relational models live in [`../alloy/`](../alloy/) (INV-RECOVER-04 +
  INV-JEP-T5-01); see that directory's `README.md`.
