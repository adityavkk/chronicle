# Spectacle animation of the subscription fence (issue #41 part B)

[Spectacle](https://github.com/will62794/spectacle) (formerly `tla-web`) runs a
TLA+ spec in the browser with a full JS TLA+ interpreter — no server, no TLC.
You step actions forward and backward, and if the spec has an animation view it
renders the state as an SVG you can watch evolve. Any trace is shareable by URL.
This realizes the "spec as living documentation" benefit (AWS / research/01
Finding 10) for Chronicle's four crash windows and the rotate-vs-coalesce
decision, replacing the `docs/research/11` prose for onboarding.

There is **no headless run** to do here (the issue notes Spectacle is a browser
tool). The committed artifacts are: the animation module
[`../SubscriptionFence_anim.tla`](../SubscriptionFence_anim.tla), the
ready-to-open share links below, and an offline SVG filmstrip per walkthrough in
[`frames/`](frames/) (rendered by `make spectacle-frames` for reviewers without
a browser).

## How the animation is defined

Spectacle auto-loads an adjacent `<Spec>_anim.tla` that `EXTENDS SVG` (the
[CommunityModules SVG module](https://github.com/tlaplus/CommunityModules/blob/master/modules/SVG.tla))
+ the base spec, and defines an **`AnimView`** operator producing an SVG element
from the state variables.
[`SubscriptionFence_anim.tla`](../SubscriptionFence_anim.tla) draws, per
subscription row:

- a **phase box** colored by phase — idle (gray) / waking (amber) / live (green)
  — showing the `(generation, wake_id)` fence, the recorded holder, and the lease;
- a **rotate-vs-coalesce badge** — what the *next* `claim` would do: COALESCE
  (reuse the in-flight `(gen,wake)`, amber) when `phase=waking ∧ wake≠""`, else
  ROTATE (mint a fresh fence, blue) — the exact `ClaimRotatesFence` branch;
- a **crash-window badge** (W1/W2/W3/W4) when the state sits in one of the four
  non-atomic recovery windows (the `WindowW1..W4` predicates of the base spec);
- one **token circle per worker** — GREEN iff that worker holds an
  *ack-acceptable* token (`AckAcceptable(w,s)`), red-rimmed-hollow for a stale/
  fenced token, light for no token.

`SingleHolder` is then literally **"never two green circles in one row"** — a
fence breach is visually unmistakable.

## Open it in Spectacle (share links)

The links load the spec from its raw GitHub URL on the
`adityavkk/formal-verification` branch; Spectacle auto-loads the adjacent
`SubscriptionFence_anim.tla` for the animation. (If you are on a different
branch, swap the branch segment in `specpath`.) Once a link is open, use the
trace explorer to step actions; the **Generate Trace** / share button mints a
`&trace=<hashes>` URL for any specific walkthrough.

- **Faithful fence (1 sub × 2 workers)** — animate the four crash windows and
  the rotate-vs-coalesce badge; `SingleHolder` holds throughout:
  <https://will62794.github.io/spectacle/#!/home?specpath=https%3A%2F%2Fraw.githubusercontent.com%2Fadityavkk%2Fchronicle%2Fadityavkk%2Fformal-verification%2Fformal%2Ftla%2FSubscriptionFence.tla&constants%5BWorkers%5D=%7Bw1%2Cw2%7D&constants%5BSubs%5D=%7Bs1%7D&constants%5BMaxGen%5D=3&constants%5BMaxClock%5D=3&constants%5BMaxCrashes%5D=1&constants%5BExpireClearsFence%5D=TRUE&constants%5BClaimReScores%5D=TRUE&initPred=Init&nextPred=Next>

- **Fence breach (INV-FENCE-04 fault, `ExpireClearsFence=FALSE`)** — drive the
  unsound expire and watch two green circles appear in one row (two ack-
  acceptable holders): `Arm(s1)` → `Claim(w1,s1)` → `Tick` → `ExpireLease(s1)`
  (leaves a claimable waking fence) → `Claim(w2,s1)` (coalesces onto the same
  `(gen,wake)`):
  <https://will62794.github.io/spectacle/#!/home?specpath=https%3A%2F%2Fraw.githubusercontent.com%2Fadityavkk%2Fchronicle%2Fadityavkk%2Fformal-verification%2Fformal%2Ftla%2FSubscriptionFence.tla&constants%5BWorkers%5D=%7Bw1%2Cw2%7D&constants%5BSubs%5D=%7Bs1%7D&constants%5BMaxGen%5D=3&constants%5BMaxClock%5D=3&constants%5BMaxCrashes%5D=0&constants%5BExpireClearsFence%5D=FALSE&constants%5BClaimReScores%5D=TRUE&initPred=Init&nextPred=Next>

- **#38 Composed layering (owner-fence OFF)** — the `Composed.tla` two-fence spec
  with `FENCE_MODE="AlwaysPass"` (the outer owner-epoch fence deleted), to watch
  the inner `(gen,wake)` fence hold `SingleHolder` alone (it has its own
  `AnimView` if/when added; today it loads with the default state view):
  <https://will62794.github.io/spectacle/#!/home?specpath=https%3A%2F%2Fraw.githubusercontent.com%2Fadityavkk%2Fchronicle%2Fadityavkk%2Fformal-verification%2Fformal%2Ftla%2FComposed.tla&constants%5BWorkers%5D=%7Bw1%2Cw2%7D&constants%5BSubs%5D=%7Bs1%7D&constants%5BSlots%5D=%7Bh1%7D&constants%5BMaxGen%5D=2&constants%5BMaxEpoch%5D=2&constants%5BMaxClock%5D=2&constants%5BMaxCrashes%5D=1&constants%5BFENCE_MODE%5D=%22AlwaysPass%22&initPred=Init&nextPred=Next>

If a `raw.githubusercontent.com` `specpath` is blocked (private repo / CORS),
the equivalent is: open <https://will62794.github.io/spectacle/#!/home>, paste
the contents of `SubscriptionFence.tla` *and* `SubscriptionFence_anim.tla` into
the editor, and set the constants in the UI to the values above.

## The four crash windows to animate (faithful link)

Each is a `WindowWn` predicate of `SubscriptionFence.tla`; the badge lights up
when reached. The offline filmstrip for each is in `frames/<name>/`.

| Window | What to do in the trace explorer | Badge state |
|---|---|---|
| **W1** arm-before-emit | `Arm(s1)` on a pull-wake sub: a wake is armed (`phase=waking`, `wake_sent=0`) but the Go `writeWakeEvent` is still owed (`pending="emit"`). | `W1 arm-before-emit` |
| **W2** commit-then-stamp | `Arm(s1)` → `WakeAppend(s1)`: the wake event is appended but `RecordWakeEventSent` has not stamped yet (`pending="stamp"`). | `W2 commit-then-stamp` |
| **W3** post-emit T4 | `Arm` → `WakeAppend` → `WakeStamp` then `Crash(w?)`: stamped (`wake_sent=1`), still waking, no lease, never claimed. | `W3 post-emit T4` |
| **W4** claim-before-ack | `Arm` → `Claim(w1,s1)` → `Crash(w1)`: the sub is live with a lease but the holder's token is gone; the lease member survives so it is reclaimable. | `W4 claim-before-ack` |

## The rotate-vs-coalesce decision

Watch the rotate-vs-coalesce badge as you step:

- From `phase=idle`, `Arm(s1)` → `phase=waking`, badge flips to **COALESCE** (the
  next claim of an already-issued wake reuses `(gen,wake)`).
- `Claim(w1,s1)` from waking → `phase=live`, gen unchanged (coalesce happened),
  badge flips back to **ROTATE** (any further claim now mints a fresh fence,
  e.g. an expired-lease takeover that fences out the deposed holder).
- Compare with the breach link: the fault's unsound expire returns to `waking`
  *keeping* the old fence, so a second `Claim` COALESCES onto a fence a live
  holder still carries — the breach.

## Offline filmstrips (`frames/`)

For reviewers without a browser, `make spectacle-frames` renders one SVG per
state of a curated trace into `frames/<walkthrough>/`, using TLC's `AnimAlias`
(the same `AnimView`, serialized headless via the CommunityModules
`SVGSerialize` override). Open any `.svg` directly. The committed set:

| Directory | Trace | Last frame shows |
|---|---|---|
| `frames/w1_arm_before_emit/` | reaches W1 | amber waking row, `W1` badge, both tokens hollow |
| `frames/w2_commit_then_stamp/` | reaches W2 | `W2` badge |
| `frames/w3_post_emit_t4/` | reaches W3 | `W3` badge, no lease |
| `frames/w4_claim_before_ack/` | reaches W4 | live row, `W4` badge, holder's token dropped |
| `frames/violation_double_holder/` | INV-FENCE-04 fault | **two GREEN token circles in one row** — the fence breach |

`make spectacle-frames` needs a newer tla2tools (≥ 1.8.0) and CommunityModules
than the #37 CI pin, because the `SVGSerialize` override uses tlc2 APIs absent
from v1.7.4; both jars download to `/tmp` on demand and are **not committed**.

## Files

- [`../SubscriptionFence_anim.tla`](../SubscriptionFence_anim.tla) — the
  `AnimView` + `AnimAlias` animation module (auto-loaded by Spectacle).
- [`../MC_Anim.tla`](../MC_Anim.tla) + `../Anim_W{1..4}.cfg`,
  `../Anim_Violation.cfg` — the headless-render harness and per-walkthrough
  configs.
- [`render_frames.sh`](render_frames.sh) — renders the filmstrips into `frames/`.
- [`frames/`](frames/) — the committed SVG filmstrips.
