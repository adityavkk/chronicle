# Paid execution screen

## Status

The remote preflight passed on July 28, 2026. Billing, required APIs, IAM,
Artifact Registry, the benchmarking VPC, the subnet, machine availability, and
all required quotas passed. No benchmark cluster is running.

The active account is `adityavkk@gmail.com`. The project is
`adityavkk-prototyping`.

## Proposed paid work

The image step submits two Cloud Build jobs. One builds Chronicle. One builds
the patched ds-bench client. The Rust, Node, Ursula, and Redis images are copied
by exact digest from the sealed prior comparison archive. Manifest creation
checks that archive seal and each copied image again.

The benchmark step runs eight suites in sequence:

| Campaign | Suites | Configurations in each suite | Deadline for each suite | Hard cluster hour limit |
|---|---:|---:|---:|---:|
| Fixed budget topology screen | 6 | 6 | 3 hours | 18 hours |
| SSE wait diagnostic | 2 | 2 | 2 hours | 4 hours |
| Total | 8 |  |  | 22 hours |

Each suite creates one GKE cluster with one on demand
`c4d-standard-16-lssd` server and two Spot `n2d-standard-32` clients. The
campaign wrapper starts an exact name teardown watchdog before each suite. It
also records a final proof that every campaign cluster is absent.

The 22 cluster hour value is a hard timeout bound, not the expected runtime.
The earlier Chronicle suites suggest roughly 5 to 7 cluster hours for this
smaller discriminator set, but catchup seeding makes that estimate uncertain.

The machine readable scope is
[`paid-screen.json`](../../../../benchmarks/ds-bench/paid-screen.json).

## Approval boundary

The earlier Cloud Build and four suite authorization covered the catchup repair.
It does not cover this new screen. The new paid scope is exactly two Cloud Build
uploads, six fixed budget topology suites, and two SSE diagnostic suites in
`adityavkk-prototyping`.

## Downside

The hard timeout still permits up to 22 cluster hours if several suites stall.
The watchdog limits leaked infrastructure risk, but it cannot make an in
progress GKE cluster free.
