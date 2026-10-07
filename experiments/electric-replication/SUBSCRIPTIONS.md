# Partition-owned durable subscriptions

This extends `CONTRACT.md`. These are implementation contracts, not a claim
that every qualification gate has passed. Protocol §§6–7 are authoritative.

## Ownership and state

The full mount-relative subscription base path hashes to one fixed Raft group,
independently of its linked streams. Sub-routes use that same owner. There is no
singleton control-plane partition. Subscription configuration, normalized-config
hash, cursors, stream incarnations, generation, lease, key material, wake snapshot,
dispatch reservation, and retry deadline are deterministic state-machine data.
They use the group's existing native Electric WAL and snapshot, not Redis,
SQLite, or a second log. A delete removes live state; a recreate's committed
log index supplies a different incarnation. Old tokens cannot authorize it.

Each owner leader batches observations of committed stream catalogs across data
groups. Catalog observations identify the source group, applied position, stream
incarnation and durable tail. Older observations cannot regress a link. Creation
eagerly captures existing matches at their observed tails. A later newly observed
match starts at zero, including its initial PUT body. This defines a per-data-group
creation cut, not a cross-group atomic snapshot. An append concurrent with creation
may be before or after that cut. No append after successful creation is skipped.

Explicit links override glob display; removing one preserves a matching glob
link. A deleted stream has no pending bytes. Recreating it resets that link's
cursor for the new incarnation; acknowledgements issued for the old incarnation
cannot consume new bytes. Initial explicit paths may be absent and start at zero.
Catalog repair, not transient notification delivery, is authoritative. No network
call happens inside apply or while holding a state-machine write lock.

Catalog work is partition-owned but still a scan, not an O(1) distributed index.
Metadata fanout and broad patterns require separate scale measurements. Bounded
command and link limits reject excess work explicitly; they must never silently
truncate linked streams or acknowledge an unobserved suffix.

## Lease and delivery transitions

Commands carry leader-sampled time, monotonically applied like stream TTL. A
lease is valid only while logical time is strictly before its deadline. No claim
can replace a live holder. Heartbeat extends that deadline; done/release/expiry
retires the wake. Its next generation has a different wake ID and token. Callback
tokens bind subscription path, incarnation, generation, wake and delivery kind.
They are not service JWTs. This loopback-only experimental server has no service
JWT admission layer; Internet exposure remains prohibited.

A wake records a durable snapshot and intent before delivery. A committed
reservation fences each attempt. Network I/O follows commit outside the lock.
Result application compares subscription incarnation, generation and reservation.
An old leader can still finish an already-started external call: webhook effects
are **at least once**, never exactly once. Auto-done acknowledges only the issued
snapshot, not a newer observed tail. Explicit acks may advance to a validated
observed tail on the same stream incarnation. Validation precedes every cursor
update, so one invalid ack cannot partially advance a batch.

Pull delivery uses an ordinary explicitly created JSON stream and native producer
deduplication with a subscription-incarnation producer and generation epoch. A
lost append response is retried with that identity. A stale notification may
remain in the wake stream after deletion; its claim must fail or obtain only the
current incarnation's lease. Losing an external notification cannot lose pending
work: an unclaimed wake is periodically reissued. Crashes preserve retry deadlines,
reservations and intents in snapshots/replay. Timeouts remain unknown outcomes.

Webhook retry is 1–60 seconds exponential backoff with up to 20% positive jitter;
the chosen retry deadline is committed, not recalculated on restart. Ed25519 and
HMAC keys are generated once per group and committed before use. Private keys are
never included in public JSON or debug formatting. JWKS aggregates public keys;
no key rotation is initially exposed. Replication transport is trusted loopback
only, not an encrypted production key distribution mechanism.

Creation and every delivery revalidate the URL and all resolved IPs. Only explicit
localhost/127.0.0.x development targets may use HTTP; other targets require HTTPS
and public addresses. The validated addresses are pinned for that request.
Redirects and environment proxies are disabled to prevent validation bypass.

## Formal mapping and gaps

`Subscriptions.tla` abstracts committed state in one owner group, with append,
wake, external delivery, lease expiry, stale callback, deletion/recreation and
crash. Negative mutations accept stale workers, auto-ack the latest tail, or lose
a dispatch intent. Its stable-period liveness assumes fair observation, delivery
and a successful worker; arbitrary crashes or a permanently failing endpoint do
not imply progress. Raft, DNS, cryptography and cross-group catalog correctness
are assumptions, not proved by this model. Lean adds deterministic fencing and
snapshot-ack bounds; neither constitutes a Rust refinement proof.

`Links.tla` additionally models delayed/reordered catalog observations, new pattern
discovery, source deletion/recreation, snapshot-bound acknowledgements and lost
notifications. Its three negative mutations regress catalog indices, accept an
old stream incarnation's ack, or initialize a newly discovered link at its tail
and silently skip its first data. Stable-period liveness depends on fair catalog
repair and a cooperating worker, not delivery of transient notifications. This
finite model does not prove an unbounded distributed directory or Rust refinement.

Required empirical gates include full unchanged 332-test conformance, real native
WAL snapshot/replay properties, and two-group process histories for dropped wakes,
stale workers, crash during delivery, retry schedules, explicit/glob races and
delete/recreate. Conformance alone does not qualify this control plane.
