# Orchestration plan — implementing the horizontal-scale epic ([#9](https://github.com/adityavkk/chronicle/issues/9))

A playbook for an autonomous **orchestration agent** to implement epic #9 and its children
(#10–#16) end to end: dynamic-workflow fan-out, `wt` worktree isolation, parallelism bounded
by the dependency DAG, a hard quality bar, and an **adversarial review gate on every PR**.

The orchestration agent runs on the epic. Per issue it spins a **dynamic workflow** that owns
the full lifecycle — design → implement → validate → PR → adversarial review → address → merge
→ close → clean up — and does not report success until the issue is merged and its worktree is
gone.

---

## 0. Mission & invariants

- **Goal:** ship #10–#16 onto `horizontal-scale`, each as **one reviewed, validated, merged
  PR**, with the issue closed and its worktree + branch removed.
- **Order:** **Phase 1 (#10 → #11)** — the per-type claim-contention collapse — is the
  priority. **Phase 2 (#12 → #16)** is deferred Option-A hardening of the axes that were
  already clean at 6 replicas. **Never start Phase 2 ahead of #11's C1/C2 characterization.**
- **Two hard gates per issue, no exceptions:** (1) the issue's V&V pyramid is green; (2) an
  adversarial reviewer has signed off after its high-leverage comments were addressed. A PR
  merges only when **both** pass.
- **Correctness is never a dependency on unfinished work** (`06` correction #1): the full
  sweep covers every subscription until #14, so each slice is independently shippable.
- **Source of truth:** [`05-proposed-architecture.md`](research/05-proposed-architecture.md)
  (the build spec + the third axis), [`07-jepsen-style-verification.md`](research/07-jepsen-style-verification.md)
  (the executable T#/L#/C# contract + gates), and the issue bodies #9–#16. The agent re-reads
  these; it does not re-derive.

---

## 1. The quality bar — the definition of "good" every PR is held to

Non-negotiable. The adversarial reviewer **rejects** a PR that misses any of these.

- **Functional core, imperative shell.** Domain logic is **pure, total functions over plain
  values** (mirror the existing `state.go` / `pure_test.go` split). Redis, HTTP, time, and IO
  live only in the thin imperative shell (`redis_store.go`, the `manager.go` loops, the
  handlers). The core is unit-testable with **no Redis and no clock**.
- **Parse, don't validate. Make invalid states unrepresentable.** Convert untrusted input to a
  precise domain type **once, at the boundary**; pass the type inward, never the raw
  `string`/`int`. Distinct concepts get distinct types — `SubID`, `SlotID`, `Generation`,
  `OwnerEpoch`, `ShardKey` — not bare primitives (no primitive obsession, no boolean-blindness).
  A constructor returns `(T, error)` and is the **only** way to build a `T`; a function that
  cannot fail does not return `error`.
- **Strong typing & honest API design.** Small interfaces at the seams (`Store` / `Streams` /
  `Metrics` are the model); accept interfaces, return structs; model alternatives as sealed
  sum types, not `bool` flags; errors are typed and wrapped with context. The public surface of
  each new mechanism is designed before it is implemented.
- **Readability.** Match the surrounding code's idiom, comment density, and naming. Comments
  explain **why**, not what. No dead code, no speculative generality, no premature abstraction.
- **Distributed safety + liveness (the load-bearing gate).** Every mechanism ships with its
  Jepsen-style checks from `07`: the safety **invariants (T#)** as porcupine / custom checkers
  over a *faulted* history, the **liveness bounds (L#)** asserted relative to quiescence, and
  the **contention suite (C#)** as rate/threshold checks under claimant fan-in. A mechanism
  whose T/L/C scenario is not green **is not done**.
- **Load + stress + observability.** The slice's doc-05 **gate** is measured and recorded; the
  **K=10k sweep baseline stays green** (`loadtest/RESULTS-gke.md`: sweep p99 509 ms → keep
  < 1500 ms); every new mechanism **exports its golden-signal metric before it is gated** (the
  `webhook.Metrics` interface is **append-only** — each method ships with `NopMetrics` +
  Prometheus + the `metrics/metrics_test.go` golden entry in the *same* PR, or CI breaks
  downstream).
- **Lua scripts** stay single-slot and atomic, with a `{status, …}` string-array reply decoded
  by `evalStrings`, the `common.lua` prelude, and a header KEYS/ARGV/Reply contract comment —
  byte-for-byte in the house style of `webhook/scripts/*.lua`.

---

## 2. Topology & parallelism

```
        ┌─▶ #11  Claim granularity (Phase 1 · P0 · #1)
#10 ────┤
        └─▶ #12 ─▶ #13 ─▶ #14 ─┬─▶ #15 ─▶ #16   (Phase 2 · deferred spine)
                               └────────────▶ #16
```

- **#10 merges fully first.** It is the shared substrate — the `porcupine` dep in `go.mod`, the
  append-only `Metrics` interface, the nemesis primitives, and the loadgen rig extensions —
  imported by every later slice. Landing it piecemeal thrashes every downstream branch.
- **After #10, #11 and #12 may run in parallel** — disjoint code (the claim/lease model vs the
  due-set). **#11 is the priority.** The Phase-2 spine (#12 → #16) then proceeds **strictly in
  order**: those slices all edit overlapping files (`manager.go` loops, `keys.go`, the Lua
  scripts, the `Metrics` interface), so concurrent spine work merge-conflicts pathologically.
- **Parallelism rule:** the orchestrator dispatches two branches concurrently **only when their
  file sets are disjoint** — compute it from `git diff --name-only horizontal-scale...<branch>`
  of every open branch before launching the next. #11's only overlap with the spine is
  `claim.lua` / `ack.lua` (#12's due `ZREM`, #14's TOCTOU check) — coordinate those edits and
  rebase before merge.

---

## 3. Worktree & git protocol (`wt`)

One worktree per issue, off `horizontal-scale`, so parallel agents never share a checkout.

**Create** (off the integration branch, *not* `main`):
```sh
wt switch -c hs/<n>-<slug> -b horizontal-scale
#   e.g. wt switch -c hs/10-harness    -b horizontal-scale
#        wt switch -c hs/11-claim-gran -b horizontal-scale
```
The slice agent works **only** inside its worktree (`{{ worktree_path }}` ≈ `~/dev/chronicle.hs-<n>-<slug>`). Hooks (`wt hook`) run lint/build/test gates on create and pre-merge — wire the V&V there so the gate cannot be skipped.

**Git etiquette inside the worktree:**
- Conventional-commit messages, present tense, scoped (`feat(webhook): …`, `test(jepsen): …`,
  `perf(loadtest): …`). **Small, logically-atomic commits** — pure core + its tests, then the
  shell, then wiring — not one mega-commit.
- **Rebase** onto the latest `horizontal-scale` before opening the PR and again before merge;
  never merge the integration branch *into* the feature branch.
- End every commit with the `Co-Authored-By:` trailer.
- No force-push to shared branches; `--force-with-lease` only on your own feature branch.

**PR:** one per issue, `gh pr create --base horizontal-scale`, title = the issue title, body
`Closes #<n>`, lists what changed, and **pastes the V&V evidence** (which T/L/C scenarios went
red→green, the gate number + measured value, the baseline still green).

**Merge & cleanup** (only after *both* gates pass):
```sh
wt merge horizontal-scale -y      # squash + rebase + fast-forward target + remove the worktree
gh issue comment <n> --body "Merged in <sha>. <T/L/C green> · <gate value> · baseline green."
gh issue close <n>
wt remove hs/<n>-<slug> -y        # belt-and-suspenders; branch auto-deletes once merged
```

---

## 4. Per-issue lifecycle — the dynamic workflow each issue runs

The orchestrator launches **one dynamic workflow per issue**. It runs this loop and returns
success **only** when the issue is merged + closed + cleaned up:

1. **Design (panel).** 2–3 agents draft the approach from the issue + `05`/`07`; a judge merges
   the strongest, enforcing §1 (the FCIS boundary, the domain types, the API surface). Output: a
   short design note — the PR's first commit, also posted to the issue.
2. **Implement.** One agent implements in the worktree, **pure core + table-driven unit tests
   first**, then the imperative shell, then wiring + the golden-signal metric. Strong types and
   parse-at-the-boundary from the first commit, not retrofitted.
3. **Self-validate — the pyramid, in order, stop on first red:**
   `unit (pure core, Lua behavior)` → `integration (local Redis)` →
   **`Jepsen-style (the issue's T/L/C on the k3d rig — flip RED→GREEN, keep baselines green)`** →
   **`load + stress (the doc-05 gate; the K=10k floor)`**. Each layer's command + pass criterion
   is in the issue's V&V section.
4. **Open the PR**, rebased, with the V&V evidence.
5. **Adversarial review** by a *separate* agent (§5); its ranked, high-leverage comments land on
   the PR.
6. **Address every comment** — fix, or a reasoned rebuttal — re-run the affected V&V layers, and
   re-request review until sign-off.
7. **Merge** (`wt merge`), **comment the issue** with the SHA + V&V summary, **close** it,
   **clean up** the worktree + branch.
8. **Report** to the orchestrator: merged SHA, gates green, anything deferred or escalated.

---

## 5. Adversarial review protocol

Every PR is reviewed by an agent whose job is **to find what is wrong, not to approve**. A fresh
reviewer perspective each round — never the implementer reviewing itself. It checks, in priority
order:

1. **Correctness & distributed safety.** Can it double-grant a lease, advance a cursor twice,
   drop or duplicate a wake, leak across slots/shards, or accept a stale generation/epoch? Does
   each T/L/C scenario actually *exercise* the failure, or is it a happy-path test in disguise?
   Is the porcupine model **partitioned per key** (else it returns *Unknown*, not a proof)?
2. **The §1 quality bar.** Is the core pure and total? Are invalid states *unrepresentable* or
   merely validated? Any boolean-blindness, primitive obsession, or IO leaked into the core? Is
   the API honest and minimal?
3. **High-leverage simplification.** The smallest change that removes the most risk/complexity —
   a type that deletes a validation path, a seam that deletes a mock, a script that removes a
   dual-write.
4. **Observability & failure modes.** Is the golden signal exported and gated? Does it fail
   safe? What happens on Redis reconnect, partition, and a slow claimant?

Comments are **specific, actionable, ranked** — `blocker` / `important` / `nit`. Blockers must be
fixed; importants fixed or rebutted with reasoning; nits optional. Sign-off requires **zero
blockers remaining**.

---

## 6. Validation gates — what "green" means per issue

| Issue | Phase | Jepsen T/L/C | doc-05 gate | Floor |
|---|---|---|---|---|
| **#10** harness + baseline | 1 | T1, T2, T4, L1, L3 *(today's code, no rebuild)* | #1 — O(N·K) premise | establishes the K=10k baseline |
| **#11** claim granularity | **1 ·#1** | **C1, C2** (reproduce the 6-clean/12-collapse) → **C3** (the fix moves the knee ~`G×`) | **#6** | baseline green |
| #12 due-set (Move 2) | 2 | T1/T2/T4/L1/L3 stay green | #3 — due write-amp | baseline green |
| #13 recovery split | 2 | L3 sharpened; T1/T2/T4 green | — | baseline green |
| #14 ownership (Move 3) | 2 | T3, L2, L4 | #4 — churn window | baseline green |
| #15 slot-homing (Move 1) | 2 | T5 | #2 — fan-out (**decides viability**) | baseline green |
| #16 DR + capstone | 2 | full T1–T5 / L1–L5, L5 combined nemesis | #2–#5 | baseline green |

---

## 7. Stop conditions — escalate to a human, do not guess

- **#15 gate #2 over budget** → do **not** slot-home; merge the deferral note per 05's
  recommendation; the Phase-2 spine ends there (#16 still runs against the single-slot ownership
  build).
- **#11 cross-repo design fork** (per-entity vs per-shard-of-type; the agents-runtime
  `subscriptionId` change) → surface the design doc for a decision before building the
  capability, and open the Electric-repo tracking issue.
- **Adversarial reviewer finds a correctness blocker** the implementer cannot resolve in ~3
  rounds → stop, summarize, escalate.
- **A T/L/C check returns *Unknown*** (not *Illegal*) repeatedly → a modeling/perf limit (07
  honest-gap #1), not a pass; reduce concurrency / partition harder, or escalate.
- **The K=10k baseline regresses** → block the merge; the slice is not done.

---

## 8. Definition of done

- **Per issue:** merged to `horizontal-scale`; its T/L/C + gate green; the K=10k baseline green;
  PR adversarially reviewed and signed off; issue commented with evidence and **closed**;
  worktree + branch removed.
- **Epic #9:** **Phase 1 ships the claim-contention fix** (C3 green — the knee moves ~`G×`);
  Phase 2 ships as far as the gates justify (slot-homing **only if** gate #2 passes); the full
  **T1–T5 / L1–L5 + C1–C3** suite is green at the capstone (#16); the docs are updated where the
  designs firmed up.

---

*Generated to drive the autonomous implementation of #9. The orchestration agent owns the loop;
a human owns the stop-conditions in §7 and the byline on every merge.*
