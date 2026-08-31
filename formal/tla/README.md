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
make tlc          # parse + both faithful configs + the #183 write-fence gate (the CI lane)
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

TLC checks **bounded instances**; the size-independent (all-N) guarantee for the
single-holder fence is the **Apalache inductive-invariant proof landed in #41**
(`FenceCore.tla`, [`apalache/README.md`](apalache/README.md)), which discharges
`Init => IndInv`, `IndInv /\ Next => IndInv'`, and `IndInv => SingleHolder` as
symbolic SMT queries — see that README for exactly what is proved size-
independent vs what remains bounded-N (liveness, the owner-fence layering, the
cursor). The Spectacle living-documentation animation of the four crash windows
+ the rotate-vs-coalesce decision also landed in #41
([`spectacle/README.md`](spectacle/README.md)).

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

---

# Apalache inductive proof + Spectacle animation (issue #41)

Issue #41 · Epic #25 · Phase P4.1 / P4.3 · discharges INV-FENCE-01 as a
size-independent inductive invariant and turns the spec into living documentation.

`SubscriptionFence.tla` (#37) confirms `[]SingleHolder` on **bounded** instances.
`FenceCore.tla` (#41) lifts it to an **inductive** invariant proved by Apalache
(SMT, not enumeration), establishing it for all instance sizes within the cut-off
argument documented in [`apalache/README.md`](apalache/README.md). The companion
Spectacle artifacts animate the four crash windows + the rotate-vs-coalesce
decision (and a visual SingleHolder breach under the INV-FENCE-04 fault).

## How to run

```sh
make tlc41              # the whole #41 lane: apalache + spectacle-frames
make apalache           # the three inductive obligations + witnesses + negative control
make spectacle-frames   # render the offline SVG filmstrips into spectacle/frames/
```

`make apalache` downloads `apalache-mc` to `/tmp` on demand (never committed) and
prints a PASS/FAIL per obligation. `make spectacle-frames` needs a newer
tla2tools (≥ 1.8.0) + CommunityModules for the `SVGSerialize` override (also
`/tmp`, not committed).

## What Apalache proved vs what stays bounded-N

| Proved size-independent (Apalache, `FenceCore.tla`) | Stays bounded-N (TLC) |
|---|---|
| `[]SingleHolder` (INV-FENCE-01) via IndInit + IndStep + Implies | Liveness / recovery (INV-WAKE-01, RECOVER-01/02, L1) — `Liveness.tla` |
| The fence/(gen,wake) register safety under arm/claim/ack/release/expire/crash | The owner-epoch outer fence + layering (INV-OWNER-01/02) — `Composed.tla` |
| | Cursor forward-only + at-least-once (INV-CURSOR-01, INV-LEASE-02) — #37 |

The "all N" is exact for the 2×2 SMT instance and a corroborated cut-off for
larger sizes (the invariant references ≤ 2 workers + 1 sub, no cardinality
arithmetic; IndStep also re-checked at 3×2 and 4×3). Full caveats — including
that `MaxGen` is still a finite ceiling and the cut-off theorem itself is not
mechanized — are in [`apalache/README.md`](apalache/README.md). **No fake proof:
what is inductive is the fence-safety core only.**

## #41 files

- `FenceCore.tla` — the type-annotated fence core + the inductive invariant
  `IndInv` (seven clauses) and `SingleHolder`.
- `FenceCore_3x2.tla` / `FenceCore_4x3.tla` — larger-scope IndStep reruns.
- `FenceCore_Witness.tla` — non-vacuity witnesses (a live holder + a waking fence
  are reachable).
- `FenceCore_Fault.tla` — the INV-FENCE-04 negative control (unsound expire MUST
  break SingleHolder).
- `apalache/run.sh` + `apalache/README.md` — the obligation runner and the
  proved-vs-bounded writeup.
- `SubscriptionFence_anim.tla` — the Spectacle `AnimView`/`AnimAlias` animation.
- `MC_Anim.tla` + `Anim_W{1..4}.cfg` + `Anim_Violation.cfg` — the headless render
  harness.
- `spectacle/render_frames.sh` + `spectacle/frames/` + `spectacle/README.md` —
  the offline filmstrips and the browser load recipe / share links.

---

# WriteFence — the write-fence append capability (issue #183)

Issue #183 · discharges INV-FENCE-05 / INV-FENCE-06 (catalog rows) and wires the
#41 Apalache lane into `formal-nightly.yml`.

`SubscriptionFence.tla` (#37) proves the **control-plane** claim is single-holder.
`WriteFence.tla` is its **data-plane** sibling: on one write-fenced stream, can
the accepted fenced appends of two writer epochs ever *interleave*? The control
plane is abstracted to what the stream slot observes (a minimal
`(gen, wake, holder, phase)` copy of the subscription fence); the stream slot
carries the claim **marker**, the per-authority **seal** (`wfseal:<auth>`), the
per-producer-id state + `wfbind`, and the log of accepted fenced appends. Two
claimants race one stream under exhaustive interleaving of the subscription
lease lapse, the marker's own lease lapse, the `expire_lease` idle, the
tombstone reaper, the delayed-grant EVAL race (`mintWriteTokenOnAck`'s
snapshot), the delayed-seal EVAL race (`sealWriteFencesIfCurrent`'s snapshot),
the seal-before-ack done sequence, and origin crashes. `SubscriptionFence.tla`
and `FenceCore.tla` are untouched.

The model is **time-free**: `lease_until_ns > now` is a boolean per claim (the
subscription lease) and one on the marker (its own deadline), each flipped by
a nondeterministic lapse. Two facts of the real clock are kept as guards: a
marker's deadline never exceeds its holder's subscription deadline (the
heartbeat extends the sub lease in `ack.lua` *before* the re-grant extends the
marker, so `Lapse` lapses the holder's marker too, while `MarkerLapse` may
run alone — the failed-re-grant path), and on one authority a newer claim's
deadline is later than every deadline an older claim ever had (the same lease
TTL, set later), so an older claim's lease never outlives a newer one's. Every
other ordering the clock would impose is left open — an over-approximation,
conservative for the safety verdict.

Each claimant writes under its **own producer id**: a shared id would add the
base producer state machine's per-id epoch order on top of the fence and could
mask a fence hole, so per-claimant ids are the weaker, cover-all assumption
(the fence must order the generations by itself; the producer SM still runs
per id, unchanged).

## How to run

```sh
make fence                  # parse + the faithful 1x2 config (the PR gate; part of `make tlc`)
make fence-2x2              # two authorities on one stream (nightly)
make fence-fault-toctou     # NEGATIVE: out-of-slot check (pre-#169) MUST violate EpochsDoNotInterleave
make fence-fault-nobind     # NEGATIVE: no producer binding MUST violate BoundProducerNeverOpen
make fence-fault-lazyseal   # NEGATIVE: idle-before-seal MUST violate SealPrecedesIdle
make fence-fault-globalseal # NEGATIVE: a generation-only seal MUST violate SealIsolation (two authorities)
make fence-fault-noseal     # NEGATIVE: tombstone-only done MUST violate EpochsDoNotInterleave via DelayedGrant
make fence-coverage         # non-vacuity: W5..W10 MUST each be reached (W7, W9 are step witnesses)
make fence-fault-check      # self-test: every fence-fault-* target MUST fail on the faithful config
```

## Results (TLC 2.19, tla2tools v1.7.4)

| Config | Instance | Distinct states | Depth | Verdict |
|---|---|---|---|---|
| `WriteFence_1x2.cfg` | 1 authority × 2 workers, MaxGen=3 MaxSeq=2 MaxCrashes=1 | 477,672 | 24 | `Inv` + both action props HOLD (~8 s wall on 14 cores) |
| `WriteFence_2x2.cfg` | 2 authorities × 2 workers, MaxGen=2 MaxSeq=1 MaxCrashes=1 | 10,807,012 | 26 | `Inv` + both action props HOLD (3 min 14 s wall on 14 cores) |
| `WriteFence_fault_toctou.cfg` | 1×2, `CheckInSlot=FALSE`, no crashes | 9,149 to the CEX | 8 | **EpochsDoNotInterleave VIOLATED** (intended) |
| `WriteFence_fault_nobind.cfg` | 1×2, `BindProducers=FALSE`, no crashes | 60 to the CEX | 4 | **BoundProducerNeverOpen VIOLATED** (intended) |
| `WriteFence_fault_lazyseal.cfg` | 1×2, `SealBeforeIdle=FALSE` | 19 to the CEX | 3 | **SealPrecedesIdle VIOLATED** (intended) |
| `WriteFence_fault_globalseal.cfg` | 2×2, `SealKeyedByAuth=FALSE`, no crashes | 17 to the CEX | 3 | **SealIsolation VIOLATED** (intended) |
| `WriteFence_fault_noseal.cfg` | 1×2, `SealOnRevoke=FALSE`, no crashes | 43,007 to the CEX | 11 | **EpochsDoNotInterleave VIOLATED** through `DelayedGrant` (intended) |
| `WriteFence_coverage_W{5..10}.cfg` | 1×2 | — | 3–6 | each `NotWx` VIOLATED ⇒ witness reached (traces below) |

For the two faithful runs "depth" is the diameter of the complete state graph.
For the controls and the witnesses it is the number of states in the reported
counterexample, and "distinct states" is how many TLC had visited when it found
it. Both are deterministic because those runs use **one TLC worker** (`TLC1`
in the Makefile): TLC's parallel BFS can report a counterexample a step longer
than the shortest — a level-boundary race between workers, seen on this model
as a stray `MarkerLapse` in the noseal trace and a 4-state globalseal trace
where the 3-state `Claim · SealMarker` one exists — and the visit count depends
on the interleaving. The controls are small (≤ 45k states), so the sequential
run costs nothing and every trace quoted below is the shortest one.

`Inv == TypeOK ∧ EpochsDoNotInterleave ∧ BoundProducerNeverOpen ∧
SealPrecedesIdle ∧ SealIsolation`. The two action properties are
`GenMonotoneProp == [][GenMonotone]_vars` and
`SealedIsFinalProp == [][SealedIsFinal]_vars`.

The `fault_noseal` counterexample TLC reports is the K.4 shape of the design's
I.1 trace, one write shorter (11 states): `Claim(w1)[1] · SealMarker(w1) ·
Reap · Idle(w1) · Claim(w2)[2] · AppendFenced(w2,2,0) · SealMarker(w2) · Reap ·
DelayedGrant(w1) · AppendFenced(w1,1,0)` — with done only tombstoning, both
reaped tombstones leave nothing to refuse the deposed holder's delayed grant
(its lease never lapsed; no clock ordering is bent), which reinstalls
generation 1's marker and admits a generation-1 write after generation 2's.
The supersession seal at grant (`GrantSeal`) stays on under this fault, so the
control also shows supersession alone is not enough: the tombstone
`Claim(w2)` would have superseded was reaped first. In the faithful spec the
same `DelayedGrant(w1)` is refused by the seal (`1 <= sealGen`), and `make
fence-fault-noseal` fails unless the trace passes through `DelayedGrant`.

Every `fence-fault-*` target greps for its violation **by name** (`Invariant X
is violated`) and fails on either way a control can go vacuous: a run that
prints `No error has been found` (the hole did not break the invariant), and a
run that prints neither line (TLC never model-checked — a misspelled
invariant, an unloadable jar, a parse error). `make fence-fault-check` is the
self-test: it points every control at the faithful `WriteFence_1x2.cfg` (via
`FAULT_CFG`, with every worker since no trace is reported) and requires each
of the five to fail (~40 s).

**Action coverage.** `tlc2.TLC -coverage 1` on the faithful 1x2 shows every
faithful-lane action generating states (`AppendCheck` / `AppendCommit` are
`CheckInSlot=FALSE` only). Two of them discover no *new* state — `Renew` and
`DelayedGrant`, 94,106 successors each — because their only non-stuttering
effect in the faithful lane is a repair: re-arming a marker whose own lease
lapsed, or reinstalling a reaped one, back to an earlier state (W9 is that
step). A delayed grant that lands after its claim ended is refused — by the
seal (W8), by the successor's marker, or by the lease — and a FENCED reply is
a stutter; the delayed grant *lands* only under `fault_noseal`, where it is
the violating action.

## Invariant ⇆ catalog map (#183 additions)

| Spec operator | Catalog | Statement |
|---|---|---|
| `EpochsDoNotInterleave` | INV-FENCE-05 | Per authority, the accepted fenced appends are totally ordered by writer epoch = claim generation in stream order, and an epoch-equal run has one holder. The central data-plane safety property. |
| `SealedIsFinal` (action) | INV-FENCE-06 | The seal is monotone per authority, and every append accepted in a step lies strictly above its authority's seal as it stood when the write was admitted: after a seal at `g`, no append with generation ≤ `g` of that authority is ever accepted. |
| `SealPrecedesIdle` | INV-FENCE-06 (ordering clause) | An idle authority never leaves a live, unexpired marker: the seal (revoke + `wfseal`) is durable before `ack.lua` idles, and a lease lapse disarms the marker before `expire_lease.lua` idles — there is never a claimable subscription with a marker that still authorizes writes. |
| `BoundProducerNeverOpen` | INV-FENCE-05 (rule 5) | Once a producer id is bound by an accepted fenced write, only fenced writes advance it — its `(epoch, lastSeq)` is exactly the last fenced entry's. |
| `SealIsolation` | INV-FENCE-06 | An authority's seal never runs ahead of its own fence register (`sealGen[a] <= ctl[a].gen`): `wfseal:<auth>` is keyed by the marker's authority suffix, so one authority's done cannot seal another's future generations. Unfalsifiable with the per-authority key by construction — its negative control is `fault_globalseal` (the generation-only seal the design rejected, Q5), where one authority's done seals the other at a generation it never reached: `Claim(a1) · SealMarker(a1)` puts the seal at 1 on `a2` while `a2` is at generation 0, so `a2`'s first claim is refused. |
| `GenMonotone` (action) | INV-FENCE-02 | Each authority's generation is non-decreasing across every step (the control-plane copy). |
| `CheckInSlot` / `BindProducers` / `SealBeforeIdle` / `SealOnRevoke` / `SealKeyedByAuth` toggles | INV-FENCE-05/06 negative controls | Each `FALSE` injects one named hole and MUST break the invariant it is paired with above (`make fence-fault-*`). |

## Action ⇆ shipped source mirror (#183 additions)

A non-granting reply (`FENCED` with `reason ∈ {sealed, marker, epoch, bound}`,
`STALE`, a producer rejection, a duplicate) is a stuttering no-op — it grants
nothing and mutates no durable state — so only the **accepting** branch of each
action is a state change.

| Spec action | Source mirror | Guard transcribed |
|---|---|---|
| `Claim(a,w)` | `webhook/scripts/claim.lua` + `store/redis/scripts/grant_append_fence.lua` (`handleClaim`) | BUSY iff a live holder's lease is unexpired; else rotate (`HINCRBY +1`, fresh wake, holder, live) and grant the marker at the new generation **before any token leaves**: refused when `generation <= seal`; supersession fixes the predecessor marker's seal; installs `(gen, wake, holder)` live under an unexpired lease. |
| `Renew(a,w)` | `manager.go mintWriteTokenOnAck → grantWriteFences` (heartbeat) | the current holder's non-done ack inside its lease re-runs the grant at the same `(gen, wake, holder)`: extends a live marker's lease, re-arms one whose own lease lapsed, reinstalls a reaped one (K.12; the W9 step `Claim · MarkerLapse · Reap · Renew`), FENCED (ack 500) once sealed or superseded. |
| `DelayedGrant(a,w)` | the `mintWriteTokenOnAck` snapshot race (`manager.go:1213-1218`) | a grant EVAL decided against a stale view lands late with the snapshot's `(gen, wake, holder, lease)`: enabled while that deadline is unexpired, it re-runs `grant_append_fence.lua`'s guard — the per-authority seal refuses it for every past generation (K.4). In the faithful lane every delayed grant that lands after its claim ended is refused (W8 is the seal's refusal) — a stutter, as a FENCED reply must be — and one landing under the current claim is a re-grant, as `Renew`; under `fault_noseal` it is the violating action. |
| `Lapse(a,w)` | the wall clock (the subscription's `lease_until_ns <= now`) | no script: the deadline carried by `w`'s claim passes, and with it `w`'s marker (its deadline never exceeds the subscription's); `w`'s delayed grant is FENCED. Guarded by the clock's ordering fact: older claims on the authority lapse first. |
| `MarkerLapse(a)` | the wall clock (the marker's `lease_until_ns <= now`) | no script: the marker's deadline, fixed at the last *successful* grant, passes under a still-live claim (a run of failed heartbeat re-grants); the holder's writes are `marker` until `Renew`. |
| `ExpireLease(a)` | `webhook/scripts/expire_lease.lua` (server step) | a live claim whose lease lapsed is idled, gen **unchanged** (INV-FENCE-04); the marker is untouched (its own `lease_until_ns` fences the holder). |
| `Reap(a)` | the marker key's `PEXPIRE` | a revoked tombstone after `retentionMs`; a live marker after `(lease_until_ns - now) + retentionMs`, i.e. only once its holder's lease lapsed. |
| `AppendFenced(a,w,epoch,seq)` | `store/redis/scripts/append.lua` step 4 (`common.lua evaluate_write_fence`, rules 1-2 + 4) then `validate_producer` | one script: `generation > seal`, marker present/live/exact `(gen, wake, holder)`/unexpired, `Producer-Epoch = generation`, then the base producer SM; an accepted write appends to the log, advances the producer id and binds it (`wfbind`). |
| `AppendCheck` / `AppendCommit` | the pre-#169 out-of-slot check (`CheckInSlot=FALSE` only) | `AppendFenced` cut in two, nothing else changed: `AppendCheck` is its fence block (rules 1-2 + 4) decided outside the slot; `AppendCommit` is the rest of `append.lua` — `validate_producer`, then the write — landing later without re-running the fence (a write the producer state machine rejects is dropped, as the 4xx it was) — the TOCTOU #169 closed. |
| `AppendOpen(p,epoch,seq)` | `append.lua` open class, rule 5 | no token: refused iff the producer id is bound (`wfbind`); otherwise PROTOCOL.md §5.2 byte-for-byte; not in the fenced log. |
| `SealMarker(a,w)` | `manager.go sealWriteFencesIfCurrent` → `store/redis/scripts/seal_append_fence.lua` | the holder's done at the current `(gen, wake)` (accepted after the deadline like `ack.lua` done) tombstones the marker and records the per-authority seal at the claim generation, monotone and idempotent (`already` on redelivery; `recovered` when the redelivery follows an origin crash that lost the follow-up). |
| `DelayedSeal(a,w)` | the `sealWriteFencesIfCurrent` snapshot race | the seal EVAL decided against a `Get` snapshot lands after the claim was idled (`expire_lease.lua`) or superseded: STALE against a newer marker it mutates nothing (`seal_append_fence.lua`'s first check); otherwise it tombstones and seals a generation that already ended — over-sealing, never unsafe (K.6). The follow-up `ack.lua` is FENCED. |
| `Idle(a,w)` | `webhook/scripts/ack.lua` done='1' | fence-only: idles the claim, clears wake/holder, gen unchanged; faithfully only after this claim's seal committed. The token is **not** dropped: its bytes outlive the claim and the fence must refuse them. |
| `Crash` | the origin restarts (K.1 / K.2 / K.5 are origin crashes) | loses every owed done follow-up (a sealed-but-unacked done becomes `lost`, to be redelivered) and every out-of-slot in-flight append; the workers' bearer tokens and the durable ctl / marker / seal / producer / log survive (K.1). A worker's own crash is dominated by its stuttering and is not a separate action. |

`Sealed` / `MarkerOK` mirror `EvaluateWriteFence` rules 1-2; `ProdAccepts`
mirrors `store.ValidateProducer`; `Grantable` / `GrantSeal` mirror
`grant_append_fence.lua`'s check order (lease → seal → marker generation →
same-generation exactness → supersession); `SealStale` mirrors
`seal_append_fence.lua`'s staleness test; only `DelayedSeal` can take it (a
current holder's seal is never stale), and W10 (`Claim(w1) · Lapse(w1) ·
Claim(w2)`) witnesses the STALE landing.

## Crash windows ⇆ recovery (#183 additions)

Each witness is a state (W5, W6, W8, W10) or a step (W7, W9) the safety
argument must genuinely reach. `make fence-coverage` proves each is reached on
the 1x2 instance — its `NotWx` MUST be violated, an `INVARIANT` for a state
witness and an action `PROPERTY` for a step witness — and echoes each witness
trace (one worker: the shortest).

| Window | Spec witness | Recovery / why it fails closed |
|---|---|---|
| **W5** seal-before-ack (K.1), recovered | `WindowW5`: one authority has accepted appends at two generations | the crash between `seal_append_fence.lua` and `ack.lua` leaves the holder sealed and the sub live; the lease lapses ⇒ `expire_lease` idles ⇒ the successor claims at `g+1` and genuinely writes (`EpochsDoNotInterleave` is not vacuous). |
| **W6** the deposed holder's late write | `WindowW6`: a worker still carries a token the fence refuses | a token whose claim was superseded, sealed, reaped, or lapsed: the write is `FENCED` in the slot; nothing revives it. (K.2, a granted marker whose token never left, is closed trivially — `Claim` grants and mints atomically — and is not modeled.) |
| **W7** seal redelivery (K.1), entered and recovered | `RecoveredIdle` (a step): the origin crashed with the seal committed and `ack.lua` owed (`Crash` while `sealing` ⇒ `lost`, the K.1 window), the holder redelivered its done, the re-seal found `wfseal >= g` (`already` ⇒ `recovered`), and the step is `Idle` out of `recovered` | the redelivered done tombstones again and `ack.lua` idles the claim: the window entered and recovered by redelivery; over-sealing is never unsafe (K.6). Witness trace: `Claim · SealMarker · Crash · SealMarker · Idle`. |
| **W8** delayed grant (K.4) | `WindowW8`: a worker holds a token under an unexpired deadline whose generation is sealed | the grant EVAL from the old snapshot passes the lease test and is refused by the seal — independent of the tombstone's retention. |
| **W9** marker reaped under a live claim (K.12), repaired | `ReapedReinstall` (a step): the holder's subscription lease is live, its generation unsealed, its marker gone, and the step is `Renew` over the absent marker | the marker's own lease (fixed at the last successful grant) lapsed and the key was reaped while the heartbeats kept the subscription lease live; the holder's writes are `marker` (fail closed) until its next heartbeat re-grant reinstalls the marker — that re-grant is the witnessed step. Witness trace: `Claim · MarkerLapse · Reap · Renew`. |
| **W10** delayed seal lands STALE | `WindowW10`: a worker carries a token whose claim ended and whose authority's marker names a newer claim | the seal EVAL from the old snapshot (`DelayedSeal`) takes `seal_append_fence.lua`'s STALE branch and mutates nothing — a predecessor's late done never tombstones the successor's marker. Witness trace: `Claim(w1) · Lapse(w1) · Claim(w2)`. |

## Bounds rationale

- **2 workers** is the cover-all scope for one authority's marker: the marker
  register holds a single `(gen, wake, holder)`, every hole needs a deposed
  writer and a successor, and a third claimant is symmetric to the second
  under `Permutations(Workers)`.
- **1 authority** for the PR gate: the invariants are per authority (K.10 —
  cross-authority ordering is not claimed; one authority per fenced stream is
  a MUST of the extension). The **2-authority** nightly run checks the same
  per-authority invariants under cross-authority interleaving of the shared
  producer state and log (`BoundProducerNeverOpen`, `EpochsDoNotInterleave`);
  it cannot falsify `SealIsolation` by itself (every writer of `sealGen[a]` is
  bounded by `ctl[a].gen` with the per-authority key) — that is what the
  two-authority `fault_globalseal` control is for, and it is part of the PR
  gate.
- **MaxGen / MaxSeq / MaxCrashes** are state-space ceilings enforced by both
  per-action guards and `CONSTRAINT StateConstraint`. The 1x2 lane uses 3/2/1
  (≈478k distinct states, seconds); the 2x2 lane uses 2/1/1 to keep the
  exhaustive run inside the nightly budget.
- The Apalache lane (`make apalache`, `FenceCore.tla`, #41) is unchanged by
  #183 and now runs nightly (`formal-nightly.yml`, `apalache` job): the
  webhook holder is Go-side, `WakingHasNoHolder` still holds, and no
  control-plane variable changed. A `WriteFenceCore` inductive variant is a
  filed follow-up, not a gate.

## #183 files

- `WriteFence.tla` — the module (state, the fourteen actions + `Crash`, the
  four safety operators + `SealIsolation`, two action properties, six
  witnesses (W5..W10; W7 and W9 are step witnesses), five fault toggles).
- `MC_WriteFence.tla` — the TLC harness (symmetry over Workers ∪ Auths).
- `WriteFence_1x2.cfg` (PR gate) / `WriteFence_2x2.cfg` (nightly).
- `WriteFence_fault_{toctou,nobind,lazyseal,globalseal,noseal}.cfg` — negative
  controls.
- `WriteFence_coverage_W{5..10}.cfg` — non-vacuity witnesses.
- `Makefile` — `make fence` (in `make tlc`), `fence-2x2`, `fence-fault-*`,
  `fence-fault-check`, `fence-coverage`.
