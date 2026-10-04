# Fork compatibility on real k3d

The unchanged `@durable-streams/server-conformance-tests@0.3.5` full suite passed:
**326 passed, 0 failed, 6 upstream-default subscription skips**, 65.63 seconds.
Reports: `conformance-forks.{json,txt}`. Previous full run was 247/79/6;
no suite assertions, skip lists or dependency versions changed to obtain this pass.
Expected client-abort diagnostics in stderr are retained, not suppressed.

Source: `4ba1364`, locally committed, not pushed. Image `chronicle-raft:forks`:

* Docker config ID: `sha256:bb278f78cc216485ef32c5e09593afead2b4496f69e7bdb37867807671f8a36c`.
* k3d/containerd image ID: `sha256:308d9731bae92815a4a7e80e52d26a92418ba8a6d028ce5098c5f72e2c1985bc`.
* Release binary SHA256: `1dc3591146f96d6c58799d480f23053eacba6471ab1a4c941f4a40d0120a3f78`.

All five pods stopped before the upgrade; PVCs were retained. All five returned
Ready with the same image and zero restarts during the suite. Configuration stayed
`STREAM_TENANT=conformance-mounted`; the suite used the private NodePort origin,
not a nested tenant URL. Mixed versions and downgrade after fork writes are unsupported.

## Authority and review evidence

Fixed shard mapping is unchanged. A source Raft group captures/locks its prefix;
the target group reserves capacity and stages chunks of at most 256 KiB without
publishing. Only the source's durable decision authorizes publication. Cleanup
reconciles both sides after lost responses, including delayed Prepare after abort
and source recreation. Deleted parents retain child references until leaf cleanup.
Internal RPCs require a trusted private network; the cluster ID header is not auth.

Formal specifications preceded implementation: `ForkCommit`, `ForkStaging`,
`ForkRetirement`, their negative mutations and Lean prefix/offset/decision proofs
are in `formal/`. Full `make formal` passed. Models are bounded abstractions of
durable Raft application transitions; no mechanized code refinement is claimed.

`make check` passes, including direct DB reopen and snapshot/install/reopen across
fork phases. The HTTP regression runs three actual SQLite/OpenRaft replicas per
group, with different source/target leaders. It verifies a 600,013-byte prefix and
initial body, idempotent retries, source soft deletion/cascade, target-only late
Prepare recovery and foreground/background reconciliation. Deterministic gates
reproduce sequence reuse after source recreation and the identical-PUT race.

Independent review caught and prompted fixes for legacy framing reinterpretation,
late Prepare retirement, post-Begin transaction substitution, encoded tenant slash
parsing, and concurrent identical PUT conflicts. Focused follow-up found no blocker
for those findings. The initial real-Raft fixture failed ordinary source PUT with
default 150–300 ms election timing; both failures remain in
`fork-bootstrap-failure*.txt`. It now uses the binary's 800–1600 ms election timing.
That change and a passing test do not establish a general availability guarantee.

## Remaining qualification

This is not a power-loss test, multi-AZ deployment, general API linearizability
proof, or exhaustive fork crash/partition qualification. The local tests do not
prove arbitrary multi-transaction/expiry/cascade composition. The prior randomized
read property's barrier-timeout availability failure remains retained and open.
Resource-informed balancing and further fault/steady-state performance work are
separate outstanding parts of the overall implementation.
