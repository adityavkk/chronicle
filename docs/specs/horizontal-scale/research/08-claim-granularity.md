# 08 claim granularity

## Decision

Chronicle will support per-shard-of-type leases for pull-wake subscriptions.
The default granularity is `G = 16`.

A worker may claim shard `g` of a logical subscription by sending `shard` on
`POST /__ds/subscriptions/:id/claim`. The same `shard` must be sent on `ack` and
`release`. If `shard` is absent, Chronicle uses the legacy subscription-level
lease and does not filter the stream snapshot. A pull subscription chooses one
mode on its first claim: legacy omitted-shard mode or explicit-shard mode. Later
attempts to mix the two are rejected, so an omitted-shard token cannot run
concurrently with explicit shard holders over the same subscription.

This is a Chronicle extension to protocol section 7.2. It does not change
webhook delivery.

## Problem

The Electric agents runtime creates one subscription per entity type, for
example `<type>-handler`. All entities of that type and all replicas contend for
one `generation`, one `wake_id`, one holder, and one lease deadline in
`ds:{__ds}:sub:<id>`.

At 6 replicas the system stayed clean. At 12 replicas it collapsed while CPU was
low. The failure was lock contention on one hot subscription lease, not a sweep
or keyspace limit. Slot-homing and slot ownership do not split that hot lease.

## Design

Chronicle keeps one logical subscription id and adds shard-local fence state
inside the existing `{__ds}` slot.

Shard zero keeps the current field names:

```text
ds:{__ds}:sub:<id> HASH
  phase
  generation
  wake_id
  holder
  holder_worker
  lease_until_ns
```

Nonzero shards use suffixed hash fields:

```text
ds:{__ds}:sub:<id> HASH
  phase:<g>
  generation:<g>
  wake_id:<g>
  holder:<g>
  holder_worker:<g>
  lease_until_ns:<g>
```

The lease schedule remains `ds:{__ds}:sched:lease`. Its member is the plain
subscription id for shard zero, so existing schedule entries still work after an
upgrade. Nonzero shards use:

```text
@shard:<g>:<base64url(subId)>
```

All keys still share the `{__ds}` hash tag. The Lua scripts stay single-slot and
atomic.

## Fencing

The fence is scoped to `(subId, g)`.

Claim on shard `g` reads and writes only that shard's phase, generation, wake id,
holder, and lease deadline. A live unexpired holder on shard `g` returns
`ALREADY_CLAIMED`. A holder on shard `g1` does not block a holder on shard `g2`.

Ack and release compare the request generation, request wake id, token
generation, and token shard against the current shard state. A token minted for
shard `g1` cannot ack or release shard `g2`. A stale ack after an expired-lease
takeover is still `FENCED`, but the fence is now per shard.

Chronicle also filters shard-aware claim snapshots and shard-aware acks by:

```text
g = fnv32a(stream shard key) % 16
```

The implementation uses the stream path as the shard key because that is the
only stable, durable key Chronicle sees at the claim and ack boundary. Chronicle
does not know Electric's entity id; it only knows linked stream paths and ack
paths. The cross-repo client contract is therefore: Electric Agents must compute
the same shard from the exact Chronicle stream path it will process, or introduce
a path-from-entity mapping layer that is byte-for-byte equivalent before it opts
into this extension. Hashing just an entity id is safe only if the client can
prove that id maps to the same canonical stream path Chronicle hashes.

## HTTP Contract

Claim:

```json
{ "worker": "worker-a", "shard": 7 }
```

Successful claim:

```json
{
  "wake_id": "w_abc",
  "generation": 12,
  "shard": 7,
  "token": "eyJ...",
  "streams": [],
  "lease_ttl_ms": 30000
}
```

Ack:

```json
{
  "wake_id": "w_abc",
  "generation": 12,
  "shard": 7,
  "acks": [{ "stream": "events/type/id", "offset": "0000000000000002_0000000000000084" }],
  "done": true
}
```

Release:

```json
{ "wake_id": "w_abc", "generation": 12, "shard": 7 }
```

Rules:

- `shard` is optional. Missing means legacy unsharded behavior.
- A shard-aware claim token is bound to the shard.
- Missing `shard` on ack or release is accepted only for legacy tokens.
- A request shard that differs from the token shard, or a request whose shard
  presence differs from the token, returns `409 FENCED`.
- Explicit `shard: 0` opts into shard-zero filtering. Missing `shard` does not.
- A subscription may not mix omitted-shard legacy claims and explicit-shard
  claims. The first successful pull claim fixes `claim_mode` until the
  subscription is deleted and recreated.

## Metrics

Chronicle adds:

```go
ClaimContention(status, subId string)
```

Prometheus exports it as:

```text
chronicle_claim_contention_total{status,sub_id}
```

The intended status values are `claimed`, `busy`, `fenced`, `lease_lapse`,
`ack_ok`, `release_ok`, `mode_conflict`, and `nosub`.

This metric is intentionally append-only with the #10 metrics contract: interface
method, no-op implementation, Prometheus implementation, and golden test entry
land together.

## Electric Client Contract

Electric should opt in per entity type after Chronicle with this capability is
deployed.

Client behavior:

1. Set `G = 16`.
2. Compute `g = fnv32a(canonicalChronicleStreamPath) % G`.
3. Send `shard: g` on claim, ack, and release.
4. Only process streams whose canonical Chronicle stream path maps to `g`.
5. Treat `409 FENCED` as a stale claim and retry through the normal worker loop.

Tracking issue text if the external issue cannot be opened:

```markdown
Title: Agents runtime should opt into Chronicle per-shard pull-wake claims

Chronicle now supports an additive `shard` field on pull-wake claim, ack, and
release. The field splits one logical type subscription into 16 independent
lease and fence shards. The agents runtime should compute
`g = fnv32a(canonicalChronicleStreamPath) % 16` and send `shard: g` for every
pull-wake claim, ack, and release for the entity. If the runtime starts from an
entity id, it must use the same entity-to-stream-path mapping Chronicle will see
in the linked stream and ack path.

Acceptance:

- One entity type no longer serializes all replicas through one claim lease.
- The runtime only processes entity streams assigned to the claimed shard.
- Stale ack or release for a deposed shard holder treats `409 FENCED` as a
  retryable stale claim.
- The shard key used by the runtime matches Chronicle's stream path shard key.

Risk:

If the runtime hashes a different key than Chronicle filters by, workers can miss
streams or duplicate work. The change must land behind a capability check or a
config flag until both sides agree on the key.
```

## Migration

Server deployment is backward compatible.

Existing clients omit `shard`, use the old subscription-level lease, and keep the
same JSON responses. Existing lease ZSET members are plain subscription ids and
parse as shard zero. Legacy and shard-aware workers must not be mixed on one
subscription: the first pull claim records `claim_mode`, and incompatible later
claims fail. To switch a subscription from legacy to sharded mode, drain/delete
the legacy subscription and recreate it, or use a new subscription id during the
rollout.

Shard-aware clients can roll out after the server. They should start with
`G = 16`. Changing `G` later changes stream ownership, so it is a migration, not a
configuration tweak. A future migration would drain old workers, stop accepting
old shard values, and then start workers with the new count.

## Risks

The main risk is key mismatch. Chronicle can enforce stream-path sharding today.
Electric talks in entity ids. The client issue must make those keys identical, or
Chronicle needs explicit per-link shard metadata in a later change.

The second risk is skew. If one entity shard is much hotter than the others, that
shard still has a single holder. G=16 moves the contention knee but does not make
one hot entity parallel.

The third risk is metric cardinality. `sub_id` is useful for the gate because the
failure is one hot subscription. Operators should aggregate or relabel if they
create many high-cardinality subscription ids.

## Verification

Required local checks:

- Pure model tests cover shard validation, lease-member parsing, stream filtering,
  per-shard fence decisions, and token/request shard-presence matching.
- Redis integration covers two different shards claiming one subscription at the
  same time, same-shard BUSY, cross-shard stale ack fencing, and current-holder
  ack success per shard, plus the non-mixed legacy/sharded mode boundary.
- The contention driver runs G=1 and G>1 fan-in curves and feeds the C1/C2/C3
  rate checkers.

Distributed gate:

The orchestrator still needs the real GKE campaign for the production claimants,
Redis CPU, and pod-level BUSY/FENCED rates. Local runs prove the mechanism and the
direction of the knee. They do not replace the managed-Redis 12-replica gate.
