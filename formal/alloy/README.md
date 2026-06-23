# Alloy relational models — Chronicle fan-out index + slot-homing

Issue #40 · Track `formal-verification`

Two Alloy relational models check the two relation-shaped invariants that the
TLA+ specs do not cover, over **all configurations up to a bounded scope** (the
Alloy "small scope hypothesis"):

| Model | Invariant | What it checks |
|---|---|---|
| [`FanoutIndex.als`](FanoutIndex.als) | **INV-RECOVER-04** | the per-stream fan-out index SET is exactly the projection of the canonical links HASH; reconcile rebuilds it, repairs drops, and never invents membership |
| [`SlotHoming.als`](SlotHoming.als) | **INV-JEP-T5-01** | the bitmap-gated S-slot scatter-gather subscriber set equals the reference set equals the brute-force all-slots union — no cross-subscriber leakage |

Both are grounded in the real code: `webhook/keys.go` (`slotOf`, `streamSubsKey`,
`streamSlotsKey`), `webhook/redis_store.go` (`ReconcileIndexes`, `indexStream`,
`deindexStream`), and `jepsen/checker/check_slot.go` (`computeSlotLeakage`).

## How to run (headless, scripted)

Alloy v6 ships a headless `exec` subcommand that runs every command in a model
and prints a one-line SAT/UNSAT verdict per command (plus a machine-readable
`receipt.json`). No GUI needed. The dist jar is downloaded on demand to `/tmp`
and is **not** committed.

```sh
# from this directory (formal/alloy):
bash run.sh                 # run both models
bash run.sh FanoutIndex.als # run one model

# or via the TLA Makefile (one dir up):
make -C ../tla alloy
```

Equivalent raw invocation (what `run.sh` runs):

```sh
curl -sL -o /tmp/alloy.jar \
  https://github.com/AlloyTools/org.alloytools.alloy/releases/download/v6.2.0/org.alloytools.alloy.dist.jar
java -jar /tmp/alloy.jar exec -f -o /tmp/alloy_FanoutIndex FanoutIndex.als
java -jar /tmp/alloy.jar exec -f -o /tmp/alloy_SlotHoming  SlotHoming.als
```

### Reading the verdict

- `check <assert>` → **UNSAT** means the assertion **holds** (Alloy found no
  counterexample anywhere in the scope). **SAT** would mean a counterexample
  was found — a real violation, i.e. a finding to report.
- `run <pred>` → **SAT** means an instance exists (a non-vacuity witness, or a
  deliberate diagnostic witness). **UNSAT** means the predicate is unsatisfiable.

For these **safety** models the expected verdicts are: every `check` → UNSAT,
every witness `run` → SAT.

## Results (both models, last run)

`FanoutIndex.als` (INV-RECOVER-04), scope 5:

```
check RebuildIsExactTranspose      UNSAT   index := exact transpose of links
check ReconcileCoversAllLinks      UNSAT   every link has its index tuple (superset)
check NeverInventsMembership       UNSAT   no index tuple unjustified by a link
check DropThenReconcileSelfHeals   UNSAT   drop-any-subset then reconcile fully repairs
run   RepairWitness                SAT     a real drop→reconcile repair is reachable
```

`SlotHoming.als` (INV-JEP-T5-01), scope 6:

```
check NoLeakageWhenCovered         UNSAT   scatter == reference (Foreign={}, Missing={})
check ScatterEqualsBrute           UNSAT   scatter == brute-force all-slots union
check ReferenceEqualsBrute         UNSAT   the reference is a faithful brute gather
run   MissingBitCausesLeak         SAT     a MISSING bitmap bit DOES cause a leak
run   MultiSlotFanoutWitness       SAT     a genuine >1-slot scatter is gathered correctly
```

`MissingBitCausesLeak` is **SAT by design**: it confirms the leak-freedom is
**load-bearing on the bitmap-covers-occupied discipline** (the bit is set on
every link and never cleared on deindex, `keys.go:168`). If a truly-occupied
slot were ever left unmarked, the scatter-gather would miss its subscribers —
so the assertions above are not vacuously true; they hold *because* the
implementation maintains that precondition.

## Running in the Alloy Analyzer GUI (optional, for inspecting instances)

The headless runner is the source of truth, but to *visualize* a witness or a
(hypothetical) counterexample:

1. `java -jar /tmp/alloy.jar` (no args) launches the Analyzer GUI.
2. **File → Open** `FanoutIndex.als` (or `SlotHoming.als`).
3. **Execute → Execute All** runs every `check`/`run` command. The bottom log
   shows the verdict per command; a green "No counterexample found" for each
   `check` corresponds to the headless UNSAT, and "Instance found" for each
   `run` to SAT.
4. Click a satisfied command's link in the log to open the **visualizer**;
   step through the relations (`links`, `streamSubs` / `home`, `bitmap`) to read
   the witnessed configuration. For `MissingBitCausesLeak`, the visualizer shows
   the occupied-but-unmarked slot whose subscriber is dropped by the gather.
5. **Theme** can be loaded to project on `State`/`World` for a per-snapshot view.

## Scope rationale

Alloy checks every configuration up to the given scope (number of `Sub`/`Path`/
`Slot`/`State` atoms). Scope 5–6 covers all topologies with up to that many
subscribers, paths, and slots — including multi-slot scatter (>1 occupied slot),
multiple subscribers per slot, and drop-any-subset reconciles. This is a bounded
check, not a proof for all N; the small-scope hypothesis (most relational bugs
surface in tiny instances) is the standard Alloy justification, and the
companion live differential (`check_slot.go`, GREEN) exercises the real cluster.
