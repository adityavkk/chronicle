# Apalache inductive-invariant proof of the single-holder fence (issue #41 P4.1)

This directory holds the Apalache symbolic proof that `[]SingleHolder`
(INV-FENCE-01) holds by **induction** for the Chronicle subscription fence —
turning the bounded "no counterexample up to N workers" that TLC gives
(`SubscriptionFence.tla`, #37) into a guarantee that is independent of the
number of workers and subscriptions, within the precise limits stated below.

The spec under proof is [`../FenceCore.tla`](../FenceCore.tla). Reproduce the
whole result with:

```sh
make apalache          # from formal/tla; downloads apalache-mc to /tmp, runs all obligations
# or directly:
bash apalache/run.sh
```

`run.sh` downloads `apalache-mc` (a JVM tool) to `/tmp` on demand — it is
**never committed** — and prints one PASS/FAIL line per obligation.

## What was proved

A safety invariant `Inv` is **inductive** for a spec `Init /\ [][Next]_vars`
when:

1. **IndInit** — `Init => IndInv` (the invariant holds in every initial state);
2. **IndStep** — `IndInv /\ Next => IndInv'` (every step preserves it);
3. **Implies** — `IndInv => SingleHolder` (the invariant is at least as strong
   as the property we care about).

From (1)+(2), `IndInv` holds in every reachable state by induction; with (3),
`SingleHolder` does too — i.e. `[]SingleHolder`. Crucially, **none of the three
obligations enumerates the reachable state space**: each is a single SMT query
that Apalache compiles to quantifier-free constraints for Z3. Apalache
discharges all three (verbatim run, `apalache-mc 0.58.2`, Java 21):

| Obligation | Apalache invocation (abbrev.) | Verdict |
|---|---|---|
| **Implies** `IndInv => SingleHolder` | `--init=IndInv --inv=SingleHolder --length=0` | `NoError` |
| **IndInit** `Init => IndInv` | `--init=Init --inv=IndInv --length=0` | `NoError` |
| **IndStep** `IndInv /\ Next => IndInv'` | `--init=IndInv --next=Next --inv=IndInv --length=1` | `NoError` |

`run.sh` additionally checks three things that keep the proof honest:

- **Scope corroboration** — IndStep also passes at 3 workers × 2 subs
  (`FenceCore_3x2.tla`) and 4 workers × 3 subs (`FenceCore_4x3.tla`), confirming
  the inductive step is not sensitive to the instance size it was proved at.
- **Non-vacuity** — a live ack-acceptable holder *and* a `phase=waking` fence are
  each reachable from `Init` (`FenceCore_Witness.tla`). Without this, a fence
  that is never granted would satisfy `SingleHolder` trivially and the proof
  would be empty.
- **Negative control** — with the INV-FENCE-04 fault injected (an unsound
  `expire_lease` that drops the lease but leaves a *claimable* `(gen,wake)`
  fence and the deposed holder's token intact, `FenceCore_Fault.tla`),
  `SingleHolder` is **violated** in 3 steps. This proves the model genuinely
  constrains the design — the proof is not passing because the spec is too weak
  to break.

### The inductive strengthening (the actual engineering)

`SingleHolder` alone is **not** inductive: it constrains two workers' tokens in
the current state but says nothing that survives one `Next` step. The work was
finding the strengthening `IndInv` — the smallest set of clauses that (a) holds
in `Init`, (b) is preserved by `Next`, and (c) implies `SingleHolder`. Each
clause is a structural fact the shipped Lua/Go enforces:

| Clause | Statement | Enforced by |
|---|---|---|
| `WakeIffNonIdle` | `phase=idle <=> wake=0` | `arm_wake` sets wake+phase together; `ack(done)`/`release`/`expire_lease` clear both. |
| `FenceAligned` | `wake # 0 => wake = gen` | `arm_wake` and `claim`-rotate mint `wake_id` = the freshly-`HINCRBY`'d generation; coalesce reuses an aligned pair. |
| `TokenAligned` | a held token has `gen = wake`, `gen <= sub.gen`, `wake # 0` | `claim` mints `(g,g)` at the current (monotone) fence gen; tokens are only minted or cleared, never edited. |
| `HolderOwnsFence` | ack-acceptable `w` => `w = holder` and `phase=live` | the fence register is owned by the recorded holder; only a live `claim` installs it. |
| `WakingHasNoHolder` | `phase=waking` => `holder=none` and no ack-acceptable token | `arm_wake` parks the sub at `holder='0'`; this is what makes the no-rotate coalesce safe. |
| `HolderNamesLive` | `holder # none` => `phase=live` | a named holder means a live claim; the holder's token may be gone after a crash, so this is the *only* sound direction. |
| `NoForeignCurrentToken` | a token equal to the current fence with `wake # 0` is held only by the holder | direct restatement of single-holder over tokens. |

The two clauses Apalache *forced* — and which are the substance of the result:

- **`TokenAligned`'s `gen <= sub.gen` + `FenceAligned`.** The first IndStep
  attempt failed with a spurious counterexample where a held token's generation
  had run *ahead* of the fence, and a later `Arm` "caught up" to it, making a
  stale token spontaneously ack-acceptable in `phase=waking`. That state is
  unreachable in the real protocol (tokens are minted at the then-current
  monotone fence gen, so `token.gen <= sub.gen` always; with `wake = gen`, a
  stale token's wake is strictly below any future fence wake). Adding these two
  clauses closed it.
- **The `HolderIsLive` correction.** An earlier strengthening required the
  recorded holder's token to still be *held*. Apalache surfaced a real
  counterexample: `CrashToken` drops a holder's in-memory token while
  `sub.holder` still names it in the durable Redis hash until `expire_lease`/
  `ack` idles it — exactly the **W4 claim-before-ack** window. The fix was to
  weaken it to `HolderNamesLive` (a named holder => `phase=live`), the only
  direction that survives a crash. This is the inductive proof re-deriving, from
  first principles, the same crash window `SubscriptionFence.tla` enumerates.

## What "for all N" means here — and its precise limit

The three obligations are run at a fixed `ConstInit` of **2 workers × 2 subs**
(`MaxGen=4`). The size-independence claim rests on two facts, and we are explicit
about where it is airtight and where it is a (well-founded) cut-off argument:

- **Airtight:** the obligations are *symbolic*, not enumerative. Apalache does
  not unroll `Next` over reachable states; IndStep is one SMT query asserting
  "for all states satisfying `IndInv`, one `Next` step satisfies `IndInv'`". So
  the proof already covers *all states* of the 2×2 instance, including ones a
  bounded TLC depth would never reach.
- **Cut-off (the honest part):** going from "all states of the 2×2 instance" to
  "all instance sizes" is a small-scope / cut-off argument. Every clause of
  `IndInv` and of `SingleHolder` is a universal quantification mentioning **at
  most two distinct workers and one subscription**, with no arithmetic on
  `Cardinality(Workers)` or `Cardinality(Subs)`. A violation of the inductive
  step at any size would therefore involve ≤ 2 workers + 1 sub and embed into
  the 2×2 instance, where Apalache proved none exists. The 3×2 and 4×3 reruns
  in `run.sh` corroborate this empirically. We have **not** mechanized the
  cut-off theorem itself (that would need a parametric/`\A N` encoding or a
  separate cut-off lemma), so the rigorous statement is: *the fence register is
  a single `(gen,wake)` value, the invariant is per-subscription and references
  at most two workers, so 2×2 is the worst case under the standard fence/
  fencing-token cut-off — corroborated, not machine-proved, for N > 4.*

This matches the per-key argument the Porcupine model and the
`SubscriptionFence.tla` README already make for why N=2 workers is the cover-all
scope for this register.

## What remains bounded-N-only (NOT closed by this proof)

The inductive proof covers the **fence-safety core** only. Everything below is
still bounded-N (TLC) or out of scope, by design (the issue scoped this to the
fence core):

- **Liveness / recovery** (INV-WAKE-01, INV-RECOVER-01/02, INV-JEP-L1-01, the
  `3*sweepInterval` threshold): `FenceCore.tla` deliberately drops the clock,
  crash budget, pending-follow-up marker, due-set mark and lease-ZSET member, so
  it says nothing about *eventual* re-emit or at-least-once delivery. Those live
  in `Liveness.tla`/`Membership.tla` (#38/#40), checked by TLC under fairness at
  bounded N. Apalache does not prove `<>`/leads-to properties.
- **The owner-epoch outer fence + its layering** (INV-OWNER-01/02): proved by
  the `Composed.tla` fence-on/fence-off twin TLC run (#38), bounded N. Not lifted
  to an inductive invariant here.
- **The cursor / at-least-once data** (INV-CURSOR-01, INV-LEASE-02): `FenceCore`
  abstracts the cursor away (it is a forward-only watermark orthogonal to the
  fence). `NoStrandedLease` and cursor monotonicity remain TLC-checked in #37.
- **`MaxGen` is still a finite ceiling in the model.** The generation is an
  unbounded `HINCRBY` counter in production; the proof bounds it at `MaxGen` for
  type-finiteness. The inductive step never compares against `MaxGen` except in
  the `gen < MaxGen` action guards (a state-space fence), so the argument is
  insensitive to the specific ceiling — but a fully unbounded-counter proof
  would drop the ceiling and re-run, which we have not done.

## Files

- [`../FenceCore.tla`](../FenceCore.tla) — the type-annotated fence core + the
  inductive invariant `IndInv` and its seven clauses.
- [`../FenceCore_3x2.tla`](../FenceCore_3x2.tla),
  [`../FenceCore_4x3.tla`](../FenceCore_4x3.tla) — larger-scope IndStep reruns.
- [`../FenceCore_Witness.tla`](../FenceCore_Witness.tla) — non-vacuity witnesses.
- [`../FenceCore_Fault.tla`](../FenceCore_Fault.tla) — the INV-FENCE-04 negative
  control.
- [`run.sh`](run.sh) — downloads `apalache-mc` to `/tmp` and discharges every
  obligation with a PASS/FAIL verdict.
