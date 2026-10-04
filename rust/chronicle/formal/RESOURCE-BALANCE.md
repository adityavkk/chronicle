# Resource-informed replica placement: contract before implementation

Keep initial placement, unhealthy-voter replacement and draining independent of
optional healthy-cluster balancing. Healthy placements must not be restored to a
fixed rotation after a resource move. All movement still uses the existing
replicated intent, learner catch-up, fenced membership and retirement paths.

Measure each bounded SQLite actor's cumulative service time, queue occupancy and
charged application-state bytes. Service time includes storage waits: it is **not
CPU utilization**. Poll only the private, recipient-bound interface. Require a
complete fresh observation of every eligible node, and at least 30 seconds of
stable placement/registry and monotonic actor counters before choosing a move.
Missing observations, restarts and control-leader term changes reset the window.
Polling checks the term again before accepting its results. Incomplete placement
or a known unhealthy eligible node prevents optional sampling entirely. No
telemetry backend participates in this decision.

Weight each shard by a base replica cost plus its maximum replica byte charge and
service utilization. Compare the sum of squared source/destination weights before
and after one replica replacement. Require a 10% pair improvement (which strictly
decreases the whole-cluster potential because other nodes are unchanged).
Preserve at least the old distinct-zone count; reject a destination with a
quarter-full actor queue. The weight is 16 base units, one per charged 256 KiB
(at most 64), and up to 64 units for the busy fraction of the observation window.
Use non-overlapping windows rather than lifetime-average demand.
These are conservative local policy defaults, not user SLOs or a CPU/disk model.
Retired data remains on disk, so movement is not a disk-space reclamation feature.
No individual stream splitting or directed leadership transfer is implied.

`Command::Balance` must recheck the expected generation, one pending movement,
eligible targets, one-replica replacement, zone preservation and a **global
60-second cooldown in replicated state**, then use ordinary `Place`. Polling
clocks cannot authorize consensus changes. Backward clock jumps conservatively
delay movement; resource observations are advisory and may become stale after
proposal. Health/repair bypass the optional balancing cooldown to restore RF3.

## Verification boundary and mapping

`ResourceBalance` maps `generation` to the placement CAS and `lastMove` to the
maximum replicated `changed_ms` across shards. Two stale controllers may propose;
only apply admits the move. Negative mutations independently bypass cooldown and
generation. This bounded model deliberately does not assert convergence under
changing workloads, clock synchronization, or that measurements predict future
load. Existing membership models own consensus safety. Rust tests must cover
asymmetric weights, zone/queue guards, stale samples, counter resets, competing
proposals, and a fixed-weight descent to a local minimum. A real loaded k3d move
must retain acknowledged records; model checking alone does not qualify it.

TLC checked 118 distinct states; both negative configurations violate their
named invariants. Lean proves the scalar hysteresis descent and same-time
cooldown exclusion for unbounded natural numbers. Neither proves workload
prediction or controller scheduling. The 10% threshold can deliberately stop
short of the minimum possible potential; it trades residual imbalance for fewer
migrations, rather than guaranteeing equal loads.
