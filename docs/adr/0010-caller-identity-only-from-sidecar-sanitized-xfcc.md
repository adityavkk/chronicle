# ADR-0010: Caller identity only from sidecar-sanitized XFCC

- **Status:** Accepted
- **Date:** 2026-09-20 (amended 2026-09-26: strict XFCC quoting grammar)
- **Deciders:** @adityavkk
- **Related:** [#126](https://github.com/adityavkk/chronicle/issues/126)
  (service identity), [#130](https://github.com/adityavkk/chronicle/pull/130)
  (fail-closed XFCC guard), [ADR-0008](0008-write-fencing-extension.md) (write
  fencing honours service principals)

## Context

Chronicle admits an in-mesh service by the SPIFFE URI that the sidecar
(Istio/Envoy) puts into `X-Forwarded-Client-Cert` (XFCC). The allowlist is
`CHRONICLE_TRUSTED_SPIFFE_IDS` merged with every `spiffe://` identity in
`CHRONICLE_SERVICE_POLICY_FILE`, so any deployment with a policy file that
names a mesh identity has one. Whether a forged header can satisfy that
allowlist depends on two things: which XFCC element Chronicle reads, and what
the hop directly in front of Chronicle does to the header.

A deployment review found a stage that combined three conditions: the
marker-less opt-in `CHRONICLE_XFCC_TRUST_WITHOUT_MARKER=true` inherited from a
shared base configuration, no `CHRONICLE_XFCC_REQUIRED_HEADER`, and inbound
listeners running `forward_client_cert_details: APPEND_FORWARD` where
Chronicle's startup warning asked for `SANITIZE_SET`. Nothing in the binary
could refuse that combination, because nothing in the core configuration named
the deployment environment. Deployment-specific adapters read their own
variables later, and only when subscriptions are enabled.

### How Chronicle reads XFCC

`auth.VerifyXFCC` receives every XFCC header line joined in HTTP order (the
data plane in `authz.go`, the subscription control plane in
`webhook/routes_auth.go`). `parseXFCC` checks the header against Envoy's
element grammar: elements split on `,`, pairs on `;`, a double quote may open
a value only right after `=` and must close it right before `;`, `,` or the
end, and `\"` is the escape. A header outside that grammar is refused as a
whole. Otherwise Chronicle honours only the **last** element and matches its
`URI=` SANs exactly and case-sensitively against the allowlist. `By`, `Hash`,
`Subject`, `Cert`, `Chain` and every earlier element are ignored.

A marker gate runs before the parser. When `CHRONICLE_XFCC_REQUIRED_HEADER` is
set, the request must carry exactly one value of that header, equal in
constant time to the configured value. When it is unset, the gate passes only
if the marker-less opt-in is on. A failed gate, a refused header, or an
allowlist miss is `ServiceRejected`, which is a `401` and never a downgrade to
the static bearer. `AuthenticateDetail` reports a refused header as
`invalid service identity: malformed X-Forwarded-Client-Cert: <reason>`, with
one of three fixed reasons and no header content. A failed gate and an
allowlist miss share the plain `invalid service identity`.

### Why the last-element rule blunts `APPEND_FORWARD`

On the inbound listener of Chronicle's own sidecar, Envoy's modes behave like
this:

| Mode | mTLS downstream | Plaintext downstream |
|---|---|---|
| `SANITIZE` (default) | XFCC removed | XFCC removed |
| `SANITIZE_SET` | XFCC **replaced** with the peer's details | XFCC removed |
| `APPEND_FORWARD` | peer's details **appended as the last element** | XFCC removed |
| `FORWARD_ONLY` | XFCC forwarded unchanged | XFCC removed |
| `ALWAYS_FORWARD_ONLY` | XFCC forwarded unchanged | XFCC forwarded unchanged |

Under `APPEND_FORWARD` a client-supplied XFCC survives, but the sidecar's
attested element is appended last, and Chronicle reads only the last element
of a **well-formed** header. The qualifier is load-bearing. The first parser
(`splitXFCC`) toggled quote state on every `"` and tolerated an unterminated
quote, so a client prefix such as `URI=<trusted>;Subject="x` ran across the
comma Envoy inserted. The sidecar's element became the tail of the client's
"last" element, and the parser consulted the client's `URI=`. An adversarial
review in 2026-09 found this. `parseXFCC` now refuses any quoting Envoy never
emits (an unterminated quote, including a trailing lone backslash; a quote
outside a quoted value; bytes after a closing quote) instead of guessing a
boundary. A well-formed prefix is hearsay the parser never consults, and a
malformed prefix fails closed even when the appended element is trusted.

### Residual exposure

The rule protects nothing when the last element is not the sidecar's:

- `FORWARD_ONLY` or `ALWAYS_FORWARD_ONLY` forwards the client's header
  unchanged, so a forged last element wins. `ALWAYS_FORWARD_ONLY` does so even
  for plaintext downstreams. A valid marker on either mode proves the request
  crossed the sidecar; it does not prove the sidecar sanitized XFCC.
- Any path that reaches the application port without the sidecar (an excluded
  inbound port, host networking, a host or node port, or an ingress that ends
  the mesh leg and forwards the external client's header) delivers a
  client-controlled last element.
- An ingress gateway whose leg to the sidecar is mTLS makes the last element
  the gateway's identity, which is not in the allowlist, so external clients
  fail closed. That holds only while the gateway-to-sidecar leg is mutual TLS
  and the gateway does not run a forward-only mode of its own.

The binary cannot observe the listener mode or the network paths, so these
are deployment obligations.

## Decision

1. **Identity is the last element of a well-formed XFCC header, and only
   that.** `parseXFCC` enforces Envoy's quoting grammar and refuses a header
   outside it as a whole. The marker gate and the mesh configuration exist to
   make sure the last element is the sidecar's.
2. **The marker-less opt-in is dev-only.** `LoadEnv` reads a core label,
   `CHRONICLE_ENVIRONMENT`: free form, trimmed and lower-cased, unset counts as
   non-dev. `CHRONICLE_XFCC_TRUST_WITHOUT_MARKER=true` is refused at startup
   unless the label is exactly `dev`, even when a marker or allowlist is also
   set. Outside dev, the #130 guard's refusal names the marker and the listener
   modes and does not recommend the opt-in. The label grants nothing by
   itself, lives in the core package so it runs before any Redis connection,
   and is deliberately not tied to any platform's own environment variables.
3. **Outside dev, a marker and a verified listener are both required.** Set
   `CHRONICLE_XFCC_REQUIRED_HEADER="Name: value"` to a header only the
   Chronicle sidecar injects, with overwrite semantics so exactly one value
   reaches the pod. Every inbound listener that reaches the application must
   make the last XFCC element the verified peer's: `SANITIZE_SET`, or
   `APPEND_FORWARD` confirmed by a listener dump. `FORWARD_ONLY` and
   `ALWAYS_FORWARD_ONLY` are not allowed. The binary enforces the marker but
   cannot inspect the listener, so `SANITIZE_SET` without a marker still fails
   the startup guard, and a marker on a forward-only listener starts the binary
   while leaving XFCC under client control. A deployment holds its non-dev
   release until the marker, the listener dump and a negative probe (a forged
   XFCC sent from an unapproved workload is refused while the real caller
   succeeds) are recorded in the same session. Mesh peer authorization
   (`STRICT` `PeerAuthentication` plus an `AuthorizationPolicy` that admits
   only the expected `source.principals`) is independent defence and does not
   replace either half.
4. **Public first, before the next mirror sync.** This repository owns the
   parser, the configuration guard, their tests, `README.md`,
   `docs/DEPLOYMENT.md` and this ADR. The internal deploy mirror is a one-way
   sync where public wins, so a fix to any of these that lands in the mirror
   first must merge here before the next sync, or the sync reverts it there.
   The mirror's own deployment manifests, which set `CHRONICLE_ENVIRONMENT` per
   environment and hold non-dev releases, stay mirror-only.

## Consequences

- The marker and the listener mode do different jobs. The marker proves the
  request crossed the expected sidecar. `SANITIZE_SET` or a verified
  `APPEND_FORWARD` makes the last element the verified peer's. Outside dev,
  both are required.
- A header whose quoting is outside Envoy's grammar gets `401` before any
  element is read. Envoy's own output is always well-formed, so only a hop that
  hand-builds XFCC, or a client probing the parser, sees this. The refusal
  counts as a service authentication failure like any other rejection.
- Upgrading is a breaking change for a deployment that sets the opt-in without
  `CHRONICLE_ENVIRONMENT=dev`: it stops at startup with an error naming both
  variables. A deployment manifest should set the label in every environment
  and the opt-in only in dev. Local and docker-compose runs set neither the
  allowlist nor the opt-in and are unaffected.
- A non-dev environment that restores its release before the marker exists
  gets a pod that exits on the #130 guard. With a single replica and a
  recreate rollout, that is an outage until rollback, and a rollback restores
  the old marker-less posture.
- Once a marker is set, a request whose marker is absent, wrong or duplicated
  gets `401` with no fallback. Confirm the injected marker on live traffic
  before release.
- `dev` keeps the marker-less posture and the dev-only startup warning.

Out of scope, recorded so it is not mistaken for covered: under
`CHRONICLE_AUTH_MODE=insecure` a rejected service identity is telemetry only,
on the data plane and on the control routes that authenticate a caller
(create, delete, add-streams, remove-stream, claim). This decision hardens how
identity is derived; turning on enforcement is a separate decision.
Write-fenced streams keep their fence in both modes (ADR-0008).

## Tests

- Parser: `TestVerifyXFCC`, `TestParseXFCCRefusesMalformedQuoting`,
  `TestVerifyXFCCForgedPrefixCannotAbsorbSidecarElement`, and the properties
  `TestVerifyXFCCNoPrefixCanForgeSidecarElement` and
  `TestVerifyXFCCWellFormedPrefixKeepsSidecarElement` (`auth/`).
- Data plane: `TestXFCCMalformedQuotingFailsClosed` (including the marker gate
  running first and a malformed header beside a valid service bearer),
  `TestXFCCDuplicateHeaderLastElementWins`, `TestXFCCSidecarMarkerGate`.
- Control plane: `TestMalformedXFCCRefusedOnControlPlane` (`webhook/`).
- Configuration: `TestLoadEnvXFCCFailsClosed`, `TestLoadEnvEnvironment`.

## Verification a non-dev deployment records

| Item | Record |
|---|---|
| `forward_client_cert_details` on every inbound listener of the Chronicle sidecar that reaches the application | mode per listener, from a listener dump |
| The ingress gateway's own mode and its TLS mode to the sidecar, per hostname | mode and TLS mode per hostname |
| The header the sidecar injects as the marker, and that it overwrites a client copy | header name and injection mechanism |
| Mesh peer authorization and `PeerAuthentication` mode for the namespace | applied or not, and drift from source |
| Negative probe: a forged XFCC from an unapproved workload is refused while the real caller succeeds | sanitized request and response metadata, captured with the listener dump |
