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

### Connecting and authenticating

Set the endpoint with `CHRONICLE_REDIS_URL` (or `--redis-url`):

| URL | Client |
| --- | --- |
| `redis://host:6379/0`, `rediss://host:6380/0` | standalone |
| `redis+cluster://h1:6379,h2:6379`, `rediss+cluster://h1:6379,h2:6379` | Redis Cluster, seeded from every listed node |

The URL never carries credentials. Chronicle refuses a URL that contains `@`,
so the URL is safe to log and no error message repeats it. Supply
authentication separately:

```text
REDIS_USERNAME=chronicle                              # optional ACL user
CHRONICLE_REDIS_CREDENTIAL_FILE=/etc/secrets/redis    # absolute path to a mounted secret
```

The credential file holds `KEY=VALUE` lines. Chronicle reads exactly one
`REDIS_PASSWORD` and at most one `REDIS_USERNAME`, and ignores other keys, so a
shared mounted secret works:

```text
REDIS_USERNAME=chronicle
REDIS_PASSWORD=example-password
OTHER_SERVICE_TOKEN=ignored
```

Startup fails, without echoing any value, when the file is missing or not a
regular file (a symlink to one, as Kubernetes projects secrets, is fine), is
group- or world-writable or executable, exceeds 64 KiB, lacks a password,
repeats a Redis key, has an empty value or CRLF line endings, spells a Redis
key another way (`export REDIS_PASSWORD=…`), or names a username that differs
from `REDIS_USERNAME`. The file is read once at startup, so restart chronicle
after rotating the password.

Upgrading from a URL with embedded `user:password@`: move the password into the
credential file and the username into `REDIS_USERNAME`; the old URL now refuses
startup.

At startup chronicle logs `redis connected` with the mode, the address or seed
list, and whether TLS is on, all read from the constructed client. With
`--metrics-listen` set, `/readyz` pings Redis; chronicle logs
`redis readiness failed` once when the ping starts failing and
`redis readiness recovered` when it succeeds again, never the raw error.

### TLS

`rediss://` and `rediss+cluster://` connect over TLS 1.2 or later, and chronicle
verifies the server by default: the certificate chain against the system roots,
and the certificate's names against the host in the URL. A cluster client
reaches nodes at the addresses `CLUSTER SLOTS` reports, which the certificate
usually does not name, so chronicle checks every node's certificate against the
seed hosts listed in the URL rather than the address it dialed. That is the only
mismatch it tolerates: each node must present a certificate that chains to a
trusted root and names one of the seeds.

For a private CA, give chronicle its PEM bundle, which replaces the system
roots:

```text
CHRONICLE_REDIS_URL=rediss+cluster://redis.example.com:6379
CHRONICLE_REDIS_CA_FILE=/etc/redis-ca/ca.pem
```

`CHRONICLE_REDIS_TLS_INSECURE_SKIP_VERIFY=true` turns verification off
entirely. It lets a deployment whose Redis CA is not yet known keep running
deliberately, and chronicle logs a warning at every start while it is set:
without verification, anyone on the network path can impersonate Redis and read
its password and every stream. Startup fails when either setting is used with a
plaintext URL, when both are set, when the CA file holds no certificate, or when
the URL carries go-redis's `skip_verify` parameter.

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

## Webhook egress adapters

Webhook delivery is governed by the SSRF rules in `webhook/ssrf.go`: a private
target is rejected at subscription create unless the whole process opts into
`--webhook-allow-private`. A deployment that needs exactly one private route
(an in-cluster receiver reached through platform routing) plugs in an egress
adapter instead: a `webhook.TargetPolicy` whose `AllowTarget` admits that one
target ahead of the SSRF rules and whose `PrepareRequest` sees every signed
delivery — retries included — immediately before it enters the HTTP client,
where it may add platform-owned routing headers but never touches the body or
the `Webhook-Signature`; and, optionally, the `*http.Client` that performs the
deliveries. Adapters live in their own `cmd/chronicle` files and register from
`init()` by appending to `webhookEgressLoaders`; `main` folds them at startup,
refuses two active adapters, and refuses an adapter combined with
`--webhook-allow-private`, because the adapter exists to make that broad mode
unnecessary. Every other target still goes through the normal SSRF rules and is
rejected with `400 WEBHOOK_URL_REJECTED`.

## Request correlation and logging

Every request gets one correlation id and one completion log record.

| Setting | Default | Meaning |
| --- | --- | --- |
| `CHRONICLE_REQUEST_ID_HEADER` / `-request-id-header` | `X-Request-ID` | The header Chronicle reads on requests, echoes on responses and sends on webhook deliveries. Must be an RFC 9110 field name that Chronicle does not already interpret: credential (`Authorization`, `Cookie`), framing (`Content-Type`, `Content-Length`, `Host`), trace-context (`traceparent`, `tracestate`), caller-identity (`X-Forwarded-Client-Cert`, `electric-claim-token`) and the protocol's `Stream-*`, `Producer-*`, `Write-*` and `Webhook-*` headers are refused, since the configured header is overwritten on every request. Startup refuses anything else too. |
| `CHRONICLE_LOG_FORMAT` / `-log-format` | `text` | `text` for development, `json` (one record per line) for a log pipeline. |
| `CHRONICLE_LOG_LEVEL` / `-log-level` | `info` | `debug`, `info`, `warn` or `error`. |

A platform that already owns a request-id header names it here and keeps it
end to end, for example:

```text
CHRONICLE_REQUEST_ID_HEADER=My-Platform-Request-ID
CHRONICLE_LOG_FORMAT=json
```

A caller value is kept when it is 1 to 128 bytes of `[A-Za-z0-9._:-]` starting
with an alphanumeric; anything else, or no value, is replaced by a fresh UUID
(the request still succeeds). The id reaches the webhook a stream append
causes: it is sent as the same header on the wake's `POST` and every retry,
and logged on the delivery, the callback or ack and the release. The
semantics a receiver can rely on, including which append's id a coalesced
wake carries and when a wake falls back to `wake-<wake_id>`, are in
[docs/spec/CHRONICLE-NOTES.md](spec/CHRONICLE-NOTES.md#section-71-webhook-delivery-and-callback--the-request-correlation-header).

**The header is unsigned and never identity.** It is outside
`Webhook-Signature` and no authentication or authorization decision reads it.
Treat it as a hint for joining logs, on both sides.

Log records are `event`-keyed with an `outcome`: `http_request_completed` per
request (Info; Error on a 5xx or a panic; Warn when a committed SSE stream is
aborted, Info when its client had already gone), `webhook_delivery_completed` per
delivery attempt, `pull_wake_delivery_completed` per wake event,
`subscription_ack_completed` and `subscription_release_completed` per callback,
ack and release. The request start line, the armed-wake trace and the per-append
hint are Debug; the first dirty-queue overflow of an epoch is a Warn. URL paths
are logged (stream paths can name your entities), query strings, bodies and
headers never are.

Chronicle remembers which request id armed each in-flight wake in a bounded,
process-local memory: an entry lapses after the subscription's lease plus the
longest retry gap (60 s) of no use, and at most 16384 entries are held, the
one used least recently going first. `chronicle_wake_correlation_evictions_total`
counts live entries dropped at capacity; a sustained rate means the replica's
in-flight wakes exceed the memory and those wakes log `wake-<wake_id>`.

## Tracing

Tracing is off until `CHRONICLE_OTLP_ENDPOINT` is set. Chronicle then continues
the W3C `traceparent` a caller sends, traces every request and every webhook
delivery, and exports over OTLP/HTTP. Spans carry shapes (method, status, byte
counts) and Chronicle's own identifiers (subscription id, wake id, generation):
never a URL path or query string, a body, a header value or a Redis statement.

| Setting | Default | Meaning |
| --- | --- | --- |
| `CHRONICLE_OTLP_ENDPOINT` | _(unset: tracing off)_ | The OTLP/HTTP traces URL, e.g. `https://traces.example.com/v1/traces`. `https` is required; plain `http` is accepted only to a loopback address (a local collector). No credentials, query or fragment. |
| `CHRONICLE_OTLP_USERNAME_FILE`, `CHRONICLE_OTLP_PASSWORD_FILE` | _(unset: no credential)_ | Files holding an HTTP basic-auth username and password, sent as `Authorization: Basic …`. Set both or neither; mount them as secrets, never pass them in the environment. |
| `CHRONICLE_OTLP_CA_FILE` | _(unset: system roots)_ | A PEM bundle that verifies the destination in place of the system roots. |
| `CHRONICLE_TRACE_SAMPLE_RATIO` | `1` | The fraction, 0 to 1, of Chronicle's root spans that are kept. |
| `CHRONICLE_TRACE_SAMPLE_ALWAYS` | _(empty)_ | Operations whose root spans are kept regardless of the ratio, comma separated: `append`, `read`, `create`, `delete`, `subscription`, `delivery`, `other`. |

```text
CHRONICLE_OTLP_ENDPOINT=https://traces.example.com/v1/traces
CHRONICLE_OTLP_USERNAME_FILE=/etc/secrets/otlp-username
CHRONICLE_OTLP_PASSWORD_FILE=/etc/secrets/otlp-password
CHRONICLE_TRACE_SAMPLE_RATIO=0.1
CHRONICLE_TRACE_SAMPLE_ALWAYS=append,subscription,delivery
```

The service is reported as `service.name=chronicle`; the standard
`OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` variables override or extend
the resource.

**What is traced.** Each request to the main listener is one server span named
`chronicle.<operation>` (`append`, `read`, `create`, `delete`, `subscription`
for the `__ds` routes, `other` for the console), a child of the caller's
`traceparent` when one arrives. Redis commands on paths that pass the request
context to the store (today reads and live waits) are children of the request
span, without the statement, so no key (and no stream path) leaves the process;
the append and create store calls do not yet carry it, so their Redis work is
not in the trace (see the consequences in
[ADR-0011](adr/0011-tracing-fails-open-and-samples-by-operation.md)). Redis
work that no request caused (slot ownership, the recovery sweep, queue polling)
is dropped rather than exported as one-span traces. A
webhook delivery attempt is a `chronicle.delivery` client span: a child of the
append that armed the wake while the delivering replica remembers it (the same
bounded memory that holds the request id above), the root of a new trace
otherwise. The `POST` carries `traceparent`; `tracestate` is never forwarded.
What a receiver may rely on is in
[docs/spec/CHRONICLE-NOTES.md](spec/CHRONICLE-NOTES.md#section-71-webhook-delivery-and-callback--the-traceparent-header).

**Sampling.** A caller's sampled flag always wins: a sampled `traceparent` is
continued, an unsampled one is recorded nowhere and costs nothing. Chronicle
decides only the roots it starts itself, by the ratio and the always list, and
a wake armed by a traced append inherits that append's decision.

**Fails open, loudly.** Tracing is the one subsystem allowed to. If a
credential or CA file is missing, unreadable or empty at startup, or the
exporter cannot be built, Chronicle logs one `tracing_disabled` warning whose
`reason` is `credentials_unavailable`, `ca_unavailable` or
`exporter_unavailable`, increments
`chronicle_tracing_setup_failures_total{reason}` and serves without traces:
alert on that counter. A malformed value (a plaintext remote endpoint,
credentials in the URL, a ratio outside 0..1, an unknown operation) still
refuses startup, like any other flag. At runtime an export failure is a
`tracing_export_failed` warning; the export queue is bounded (2048 spans) and
a slow destination drops spans rather than slowing a request.

**Logs join traces.** `http_request_started` and `http_request_completed` carry
`trace_id` on a traced request, and `webhook_delivery_completed` carries the
`trace_id` of the delivery, so a trace finds its log lines and a log line its
trace.

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

**Write-fenced streams** ([docs/spec/WRITE-FENCING.md](spec/WRITE-FENCING.md))
keep their fence in both modes: on a stream created with `Write-Fence: true`,
write-token validity, the mandatory producer headers, and the in-slot
marker/seal/epoch/bound checks bind even under `CHRONICLE_AUTH_MODE=insecure`,
and a wake-token bearer is always refused — a deposed or token-less runtime
fails closed on a shadow deployment exactly as in production. Only the *open*
class follows the mode: in `enforce` an unauthenticated open write never
reaches the fence — the base credential gate refuses it before the stream
lookup (`401 missing write credential`, with no fence disclosure) — while in
`insecure` it proceeds with a telemetry log line, the same posture as unfenced
streams. Two operational notes: creating a fenced stream on a server with no
append authorizer configured warn-logs at create (every fenced write will fail
closed until one is wired), and watch
`chronicle_append_fence_rejections_total{reason}` — sustained `marker`/`sealed`
rejections are deposed writers being stopped, which is the fence doing its job.

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

## dsui console

`dsui` is an optional developer console, separate from the chronicle binary.
Its webhook-capture endpoint (`POST /__hooks/{id}`) verifies every delivery's
`Webhook-Signature` against chronicle's JWKS before recording it:

```text
DSUI_SERVER=https://chronicle.example.com
# Optional: defaults to $DSUI_SERVER/v1/stream/__ds/jwks.json
DSUI_JWKS_URL=https://chronicle.example.com/v1/stream/__ds/jwks.json
```

A bad signature answers `401`; an unreachable key set answers `503`. When
neither variable is set, captures are unverified and dsui logs a warning at
startup, which is acceptable only on a developer machine. `GET /healthz` is the
probe endpoint.
