# Retained executable evidence

Complete TLC 2.19 and Lean 4.31.0 output captured on 2026-10-03:

| check | generated | distinct |
|---|---:|---:|
| Safety | 2,505 | 723 |
| Placement | 170 | 50 |
| Scenario (4 commits, 3 visible, 2 producers) | 21 | 15 |
| Liveness | 2,505 | 723 |

`tlc-badack.txt`, `tlc-badownerfence.txt`, and `tlc-badpromotion.txt`
contain full intentional counterexamples and explicitly report `AckDurable`,
`AcceptedAuthorityCurrent`, and `PromotionCaughtUp` respectively. `make negative`
accepts only TLC exit 12 plus the expected named violation.

Exact aggregate command (from this directory's parent):

```sh
PATH="$HOME/.elan/bin:$PATH" make check
```

Individual TLC commands are the `positive` recipes in `../Makefile`; each evidence file
also records its exact config/module in the parser banner. `lean-build.txt` is the
warning-as-error proof build. These counts describe only the finite configurations.
