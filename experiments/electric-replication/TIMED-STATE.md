# Timed protocol state: contract before implementation

Native `Store::get` expires using local wall time, and native GET renews sliding
TTL. Neither is safe when replicas apply commands at different times. A leader
must place its sampled Unix-millisecond time **in the committed command**. Each
materialization advances a logical clock with `max(previous, command.time)`.
Native expiry and access updates use that clock, never a follower wall clock.
Leader clock error can shift real-time expiration (no bounded-clock promise),
but cannot make two replicas disagree at the same applied prefix. Head advances
time without touching access; GET of a timed stream commits a touch before its
read. Expired streams cannot be revived by touch. Snapshots retain clock and
millisecond access/expiry metadata; replay uses recorded time, not restart time.

Default linearizable reads on timed streams therefore require quorum metadata
writes as well as leader confirmation. Explicit prefix/session reads do not
renew TTL; they observe expiry at the committed clock of their replica. They
must not change the access timestamp or delete based on its local wall clock.
Untimed streams retain the read-only barrier/sendfile path. A clock rollback
across leadership cannot undo expiration or decrease an access timestamp.

Within a partition, the native fork is created at its ordered apply position,
retains its native parent reference, and reads the immutable parent prefix plus
its own wire file. Producer state is not inherited. Cross-partition forks use
the irrevocable grant/import/publication transaction in `FORKS.md`; their sliding
TTL begins at grant time rather than at the end of transfer. The subscription
extension in `SUBSCRIPTIONS.md` is mandatory for this experiment. The complete
unchanged suite is run with `subscriptions:true`: 332 tests, zero custom skips.
Passing conformance is not a substitute for ownership/recovery fault histories.

`TimedApply.tla` checks deterministic timed apply under arbitrary replica lag,
read-vs-head operations, and decreasing leader clock samples. Its LocalClock
mutation intentionally diverges. It assumes matching committed command history;
it does not prove the native engine/refinement or distributed clock accuracy.
