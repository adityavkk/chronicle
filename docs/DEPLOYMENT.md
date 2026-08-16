# Deploying chronicle

Chronicle is a single static binary plus a Redis instance. This page covers
what the Redis deployment must provide and what guarantees you get back.

## Redis requirements

| Requirement | Why | If unavailable |
| --- | --- | --- |
| Redis ≥ 6.0 (managed Redis 8 recommended) | `EVALSHA`, pub/sub, `ZRANGEBYLEX`, and key-level `PEXPIRE` / `PERSIST` | chronicle cannot run without these commands |
| `EVAL`/`EVALSHA` permitted | every mutation is one atomic Lua script | chronicle cannot run — this is a hard requirement |
| Pub/sub permitted | long-poll/SSE wakeups | hard requirement (waiters would degrade to pure polling) |
| `maxmemory-policy noeviction` | eviction silently truncates stream data | chronicle warns at startup when it can read the config; reads detect missing data and fail loudly rather than serve corrupt streams |
| AOF (`appendonly yes`, `everysec`) recommended | crash durability of acked appends | RDB-only widens the data-loss window on crash |

### Managed Redis (e.g. Walmart-managed)

- `CONFIG GET/SET` is often denied: chronicle treats config checks as
  best-effort and never requires them at runtime.
- Lowered `proto-max-bulk-len` caps the largest single append chronicle can
  store (each append is one ZSET member). The protocol allows rejecting
  oversized appends with `413 Payload Too Large`.
- Cluster mode: every key for a stream carries a `{path}` hash tag, so each
  stream lives in exactly one slot and Lua scripts stay cluster-legal. Fork
  creation and cascade deletion touch two streams (two slots) and execute as
  two single-slot steps; the in-between window is reconciled via the fork
  registry set.

## Durability and consistency guarantees

Within a healthy Redis primary:

- Appends are atomic and strictly ordered per stream; validation (closure,
  content type, `Stream-Seq`, producer epoch/seq) commits in the same script
  as the write — there is no crash window between producer-state update and
  data append.
- Read-your-writes holds: a `GET` issued after an append's response sees the
  data.

Across failover:

- Redis replication is **asynchronous**. A failover can lose the last moments
  of acknowledged writes. Producers using idempotent headers recover exactness
  by retrying into the new primary (the producer state machine de-duplicates);
  plain producers get at-least-once across failover.
- For tighter windows, run chronicle with `WAIT`-on-append enabled (opt-in
  flag; adds replica round-trip latency to every append). This narrows but
  does not eliminate the window — see PLAN.md §4.7.

## Sizing

A stream's full history lives in one sorted set on one shard: plan node memory
for your largest streams (same operational envelope as the reference
implementation's memory store). Use TTLs (`Stream-TTL`) or absolute expiry
(`Stream-Expires-At`) on ephemeral streams — expired streams are reaped lazily
on access and by backstop key TTLs.

## Fronting chronicle

The protocol is designed for CDNs/proxies (cursor-based collapsing, ETags,
`Cache-Control` on historical reads). When proxying:

- Disable response buffering for SSE (`X-Accel-Buffering: no` is set by
  chronicle; honor it or configure the proxy equivalent).
- Don't cache `204` long-poll responses.
- Pass `X-Forwarded-Proto`/`X-Forwarded-Host` so `Location` headers on stream
  creation are correct.
- TLS termination is the proxy's job; chronicle speaks plain HTTP.

## Service identity and access policy

Use mesh-attested SPIFFE identity for service-to-service calls in production.
Chronicle accepts a service only after the sidecar attests its exact SPIFFE URI.
It then evaluates the same explicit action and namespace policy on stream routes
and subscription control routes.

Set:

```text
CHRONICLE_AUTH_MODE=enforce
CHRONICLE_SERVICE_POLICY_FILE=/etc/chronicle/service-policy.json
CHRONICLE_XFCC_REQUIRED_HEADER=X-Chronicle-Sidecar: verified
```

The policy file is strict JSON. Unknown fields, unknown actions, duplicate
identities, malformed namespaces, and empty policies stop startup.

```json
{
  "services": [
    {
      "identity": "spiffe://cluster.local/ns/electric/sa/reporting",
      "actions": ["read"],
      "namespaces": ["tenant-a"]
    },
    {
      "identity": "spiffe://cluster.local/ns/electric/sa/agents-server",
      "trusted_gateway": true
    },
    {
      "identity": "agents-server-fallback",
      "actions": ["read", "append", "create", "delete", "subscribe", "link", "claim"],
      "namespaces": ["tenant-a"]
    }
  ]
}
```

Namespace matching uses whole path segments. `tenant-a` covers
`tenant-a/events`, but not `tenant-admin/events`. A normal policy needs at least
one action and one namespace. `trusted_gateway` is different. It delegates all
actions and namespaces to that exact identity because the gateway performs the
finer entity check upstream. A gateway entry must not also set actions or
namespaces, which would look restrictive while being ignored. Do not use
`trusted_gateway` for a general service.

SPIFFE identities in the policy become Chronicle's exact XFCC allowlist. The
older `CHRONICLE_TRUSTED_SPIFFE_IDS` input still works, but every listed identity
must also have a policy when enforcement is on. An allowlist alone grants
nothing.

### WCNP mesh contract

Chronicle's header checks are one part of the boundary. The deployment must
also enforce all of these controls:

1. Enable Istio sidecar injection for the Chronicle workload and callers.
2. Require strict mTLS for traffic to Chronicle.
3. Configure the Chronicle sidecar to remove client-supplied
   `X-Forwarded-Client-Cert`, set it from the verified immediate peer
   (`forward_client_cert_details: SANITIZE_SET` or the managed equivalent), and
   inject the exact marker named by `CHRONICLE_XFCC_REQUIRED_HEADER`. The
   sidecar must also remove any client-supplied copy of that marker.
4. Expose only the mesh-routed Service port. Do not expose the application port
   through a host port, node port, alternate ingress, or direct load balancer.
   Apply NetworkPolicy or the WCNP equivalent so only the approved mesh path can
   reach the pod.
5. Apply Service Registry or mesh authorization policy that permits only the
   expected caller SPIFFE identities. Chronicle's policy is not a replacement
   for the network policy.
6. Verify the deployed path. A request sent directly to the application with a
   forged XFCC header must fail with `401`. The same request through an approved
   mTLS caller must carry the sidecar marker and resolve to its exact SPIFFE
   subject.

Do not set `CHRONICLE_XFCC_TRUST_WITHOUT_MARKER` in production. It is a
development escape hatch for a sidecar that can prove inbound XFCC is always
sanitized.

### Static bearer compatibility

`CHRONICLE_SERVICE_BEARER` remains available for the stock Electric
agents-server and non-mesh development. It is not the preferred production
boundary. A bearer identity must have a policy with the same identity name:

```text
CHRONICLE_SERVICE_BEARER=agents-server-fallback:${DURABLE_STREAMS_BEARER}
```

For production fallback, source the value from an Akeyless-managed secret, keep
mesh transport protection, and rotate with an overlap. Chronicle accepts two
entries with the same name during rotation:

```text
CHRONICLE_SERVICE_BEARER=agents-server-fallback:${OLD_TOKEN},agents-server-fallback:${NEW_TOKEN}
```

Remove the old entry after every caller has moved to the new token. The downside
of this fallback is that a leaked long-lived bearer can be replayed. SPIFFE
binds the identity to the workload and avoids that shared-secret risk.

### Service access telemetry

Startup logs report the policy count, SPIFFE identity count, and exact
`trusted_gateway` subjects. They never log bearer values. Prometheus exposes:

```text
chronicle_service_access_total{result="spiffe_authenticated"}
chronicle_service_access_total{result="bearer_authenticated"}
chronicle_service_access_total{result="authentication_failure"}
chronicle_service_access_total{result="authorization_failure"}
chronicle_service_access_total{result="delegated_gateway"}
```

These labels are fixed and do not contain subjects, paths, or credentials.
