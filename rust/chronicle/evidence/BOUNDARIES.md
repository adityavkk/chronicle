# Snapshot receipt and control discovery checkpoint

Source revision `9c2ae8f3fa86230c0807c481ce3c7e1cea3a7220` ran on all four
local k3d pods; `boundary-images.json` records image digests and confirms campaign
and fault switches were absent. `make check` passed, including normal/feature
Clippy, storage/VFS regressions, Rust tests, rustdoc and 11 Python tests.

The snapshot endpoint now bounds checked `offset + chunk length` before assembly,
using the existing 256 MiB final snapshot cap. Tests cover the exact boundary,
an empty out-of-range chunk and integer overflow without large allocations.
This does not establish a process-wide memory budget for concurrent snapshots.

Controller routing uses persisted registered addresses as well as initial seeds.
The remote control handler still performs its strict read barrier; local hints
cannot authorize control state. A stub-endpoint regression demonstrates discovery
with unavailable original seeds, and failure when the registered endpoint is
unavailable. This is not a live full-seed-retirement test. Brand-new learner
initialization still needs configured reachable bootstrap addresses.

Oracle reviewed both changes and found a test-only teardown deadlock: the App
retained a store sender while waiting for actor shutdown. Dropping that App before
closing storage fixes the regression; the executed test completes.

The first post-rollout history (`boundary-history.jsonl`) hit the restarting local
port-forward. It contains no successful read or acknowledged append; its smoke
check correctly failed retention coverage. It remains retained, not counted as
durability evidence. After the supervised forwarder recovered, the separate
`boundary2-history.jsonl` run completed 60 appends and 9 strict reads. Offline
Porcupine returned `Ok` (`boundary2-porcupine.txt`). This is a smoke checkpoint,
not a capacity benchmark or a fault campaign.
