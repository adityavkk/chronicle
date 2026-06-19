#!/usr/bin/env bash
# run-gate2.sh — gate #2 comprehensive cloud V&V run
#
# Provisions chronicle-gate2 GKE + Memorystore (standalone + CLUSTER), runs all
# measurement phases, writes loadtest/RESULTS-gate2.md, and tears everything down.
# Teardown is guaranteed: a trap on EXIT, INT, and TERM destroys all resources.
#
# Usage:  ./loadtest/run-gate2.sh
# Override config via env:  G2_PROJECT  G2_REGION  G2_TAG
#
# Phases:
#   1 — Horizontal scale curve  (N=1,2,4 replicas; prove fence-storm gone)
#   2 — Claim-contention knee   (single-holder-linz W=4 vs W=16; gate #6 baseline)
#   3 — Gate #2 fan-out         (S=2/4/8/256 subs on Memorystore CLUSTER)
#   4 — Fair K=10k sweep        (e2-standard-4 / cpu:2)
#   5 — Jepsen correctness      (expired-lease-takeover, single-holder-linz, pull-wake-arm-crash)
#
# Cost: ~$3/hr. Total wall-clock < 3 hr → well under $80.
set -euo pipefail

: "${G2_PROJECT:=adityavkk-prototyping}"
: "${G2_REGION:=us-central1}"
: "${G2_TAG:=gate2-v1}"

CLUSTER="chronicle-gate2"
AR_REPO="chronicle"
NS="chronicle-gate2"
GKE_CTX="gke_${G2_PROJECT}_${G2_REGION}_${CLUSTER}"
TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/terraform" && pwd)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REG="${G2_REGION}-docker.pkg.dev/${G2_PROJECT}/${AR_REPO}"
SUT_IMAGE="${REG}/chronicle:${G2_TAG}"
LOADGEN_IMAGE="${REG}/chronicle-loadgen:${G2_TAG}"
JEPSEN_BIN="${REPO_ROOT}/jepsen/bin/jepsen-checker"
RESULTS="${REPO_ROOT}/loadtest/RESULTS-gate2.md"
G=(gcloud --project "$G2_PROJECT" --quiet)
K=(kubectl -n "$NS")

log()  { printf '\n\033[1;36m▸ %s\033[0m\n' "$*" >&2; }
info() { printf '  %s\n' "$*" >&2; }
pass() { printf '\033[1;32m  PASS: %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m  FAIL: %s\033[0m\n' "$*"; }

# ─── TEARDOWN TRAP ────────────────────────────────────────────────────────────
# MANDATORY AND NON-NEGOTIABLE: this trap fires on ANY exit (success, failure,
# Ctrl-C, SIGTERM). The meter never runs past the test.
PFWD_PID=""
_torn=0
teardown_all() {
  [ "$_torn" = 1 ] && return 0
  _torn=1
  log "TEARDOWN — destroying all chronicle-gate2 resources"
  # Stop the port-forward loop if running.
  [ -n "$PFWD_PID" ] && kill "$PFWD_PID" 2>/dev/null || true

  # Terraform destroy: GKE cluster + Redis standalone + Redis CLUSTER.
  (
    cd "$TF_DIR"
    terraform destroy -auto-approve \
      -var="project_id=${G2_PROJECT}" \
      -var="cluster_name=${CLUSTER}" \
      -var="region=${G2_REGION}" \
      -var="provision_gate2_cluster=true" \
      -var="redis_tier=BASIC" \
      -var="redis_memory_gb=1" \
      -var="sut_machine_type=e2-standard-4" \
      -var="sut_node_count=3" \
      -var="loadgen_machine_type=e2-standard-4" \
      -var="loadgen_node_count=1" \
      -var="obs_machine_type=e2-standard-2" \
      -var="obs_node_count=1" \
      2>&1 || true
  )

  # Verification: no clusters remain in region.
  remaining=$("${G[@]}" container clusters list --region "$G2_REGION" \
    --filter="name=${CLUSTER}" --format='value(name)' 2>/dev/null | wc -l | tr -d ' ')
  if [ "$remaining" -gt 0 ]; then
    log "WARNING: ${CLUSTER} cluster may still be listed — verify at console.cloud.google.com"
  else
    log "TEARDOWN COMPLETE — GKE cluster verified gone; confirm Memorystore in console"
  fi
}
trap teardown_all EXIT INT TERM

# ─── HELPERS ─────────────────────────────────────────────────────────────────
wait_job() {
  local label="$1"
  local timeout="${2:-300s}"
  # Wait for complete first; on failure/timeout fall through gracefully.
  kubectl -n "$NS" wait --for=condition=complete "job" -l "app=${label}" --timeout="$timeout" 2>/dev/null ||
    kubectl -n "$NS" wait --for=condition=failed "job" -l "app=${label}" --timeout=10s 2>/dev/null || true
}

job_passed() {
  local label="$1"
  kubectl -n "$NS" get job -l "app=${label}" \
    -o jsonpath='{.items[0].status.succeeded}' 2>/dev/null | grep -qx "1"
}

job_logs() {
  local label="$1"
  kubectl -n "$NS" logs -l "app=${label}" --tail=-1 2>/dev/null || echo "(no logs)"
}

# Start a self-restarting port-forward loop for Jepsen (which kills pods).
# Stored in PFWD_PID so teardown_all can clean it up.
start_pfwd() {
  kill "$PFWD_PID" 2>/dev/null || true
  (
    while true; do
      kubectl -n "$NS" port-forward svc/chronicle 14438:4437 >/dev/null 2>&1 || true
      sleep 2
    done
  ) &
  PFWD_PID=$!
  # Wait until chronicle answers on the forwarded port.
  local i
  for i in $(seq 1 30); do
    curl -sf http://localhost:14438/readyz >/dev/null 2>&1 && return 0
    sleep 2
  done
  log "ERROR: port-forward to chronicle timed out"
  return 1
}

render_fanout() {
  local out="$1" redis_url="$2" s="$3" slo="$4"
  (
    cd "$REPO_ROOT/loadgen"
    go run ./cmd/render \
      -fanout \
      -spec "../loadtest/spec/fanout-gate2.yaml" \
      -out "$out" \
      -redis-url "$redis_url" \
      -image "$SUT_IMAGE" \
      -loadgen-image "$LOADGEN_IMAGE" \
      -subs "$s" \
      -slo-p99-ms "$slo"
  )
}

render_sweep() {
  local out="$1" redis_url="$2"
  (
    cd "$REPO_ROOT/loadgen"
    go run ./cmd/render \
      -spec "../loadtest/spec/sweep-10k.yaml" \
      -out "$out" \
      -redis-url "$redis_url" \
      -image "$SUT_IMAGE" \
      -loadgen-image "$LOADGEN_IMAGE"
  )
}

deploy_sut() {
  local manifest="$1"
  kubectl apply -f "$manifest"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=300s
}

run_fanout_job() {
  local manifest="$1" label="fanoutscale" timeout="${2:-300s}"
  kubectl -n "$NS" delete job -l "app=${label}" --ignore-not-found >/dev/null
  kubectl apply -f "$manifest"
  wait_job "$label" "$timeout"
}

# ─── PHASE 0: SETUP ──────────────────────────────────────────────────────────
log "Phase 0: Setup — APIs, images, infrastructure"

"${G[@]}" services enable \
  container.googleapis.com \
  redis.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  servicenetworking.googleapis.com \
  networkconnectivity.googleapis.com

# Artifact Registry (idempotent).
"${G[@]}" artifacts repositories describe "$AR_REPO" --location "$G2_REGION" >/dev/null 2>&1 ||
  "${G[@]}" artifacts repositories create "$AR_REPO" \
    --repository-format=docker --location "$G2_REGION"

# Build chronicle + loadgen images via Cloud Build (linux/amd64; no QEMU on arm Mac).
log "Cloud Build: ${SUT_IMAGE} and ${LOADGEN_IMAGE}"
(
  cd "$REPO_ROOT"
  "${G[@]}" builds submit --config loadtest/cloudbuild.yaml \
    --substitutions="_REG=${REG},_TAG=${G2_TAG}" .
)

# Terraform: provision GKE + Memorystore standalone (BASIC 1GB) + CLUSTER (3 shards).
log "Terraform: provisioning chronicle-gate2"
(
  cd "$TF_DIR"
  terraform init -reconfigure 2>&1 | tail -5
  terraform apply -auto-approve \
    -var="project_id=${G2_PROJECT}" \
    -var="cluster_name=${CLUSTER}" \
    -var="region=${G2_REGION}" \
    -var="provision_gate2_cluster=true" \
    -var="redis_tier=BASIC" \
    -var="redis_memory_gb=1" \
    -var="sut_machine_type=e2-standard-4" \
    -var="sut_node_count=3" \
    -var="loadgen_machine_type=e2-standard-4" \
    -var="loadgen_node_count=1" \
    -var="obs_machine_type=e2-standard-2" \
    -var="obs_node_count=1"
)

# Collect Terraform outputs.
STANDALONE_REDIS_URL="$(cd "$TF_DIR" && terraform output -raw redis_url)"
CLUSTER_REDIS_URL="$(cd "$TF_DIR" && terraform output -raw gate2_redis_discovery)"
info "Standalone Redis: ${STANDALONE_REDIS_URL}"
info "Cluster Redis:    ${CLUSTER_REDIS_URL}"

# Point kubectl at the new cluster.
"${G[@]}" container clusters get-credentials "$CLUSTER" --region "$G2_REGION"

log "Waiting for all nodes Ready"
kubectl wait --for=condition=Ready nodes --all --timeout=600s

# Pre-build the jepsen-checker binary (used in phases 2 and 5).
log "Building jepsen-checker"
mkdir -p "$REPO_ROOT/jepsen/bin"
(cd "$REPO_ROOT" && go build -o "$JEPSEN_BIN" ./jepsen/checker)

# ─── RESULTS HEADER ──────────────────────────────────────────────────────────
DATE_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat >"$RESULTS" <<HEADER
# Gate #2 cloud V&V results

| | |
|---|---|
| **Cluster** | chronicle-gate2 (${G2_REGION}) |
| **SUT image** | \`${SUT_IMAGE}\` |
| **Loadgen image** | \`${LOADGEN_IMAGE}\` |
| **Date** | ${DATE_UTC} |
| **Standalone Redis** | \`${STANDALONE_REDIS_URL}\` |
| **Cluster Redis** | \`${CLUSTER_REDIS_URL}\` |

HEADER

# ─── PHASE 1: HORIZONTAL SCALE CURVE ─────────────────────────────────────────
# Prove the fence-storm is GONE: fanout p99 must stay flat (or improve) as
# chronicle replicas grow from 1 → 4. If the DS hub were still serializing,
# p99 would degrade with concurrency.
log "Phase 1: Horizontal scale curve (N=1,2,4,8,12 replicas, S=4 subs, standalone Redis)"
# Use cpu:1 per pod so N=12 fits on 3 x e2-standard-4 (12 vCPU total).
# Phase 1 measures relative latency vs N, not absolute throughput.

P1_TMP=$(mktemp -d)
render_fanout "$P1_TMP" "$STANDALONE_REDIS_URL" 4 500
# Override cpu request 2->1 so 12 replicas schedule on 3 nodes x 4 vCPU.
sed -i.bak 's/cpu: "2"/cpu: "1"/g' "$P1_TMP/sut.yaml"
# Initial SUT deploy (N=1).
deploy_sut "$P1_TMP/sut.yaml"

printf '## Phase 1: Horizontal scale curve (fence-storm regression)\n\n' >>"$RESULTS"
printf '| N replicas | fanout_p99_ms | fanout_p50_ms | job_status |\n' >>"$RESULTS"
printf '|---|---|---|---|\n' >>"$RESULTS"

for N in 1 2 4 8 12; do
  log "Phase 1: scaling to N=${N}"
  kubectl -n "$NS" scale deployment/chronicle --replicas="$N"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=240s

  run_fanout_job "$P1_TMP/job.yaml" 300s
  LOGS=$(job_logs fanoutscale)

  P99=$(printf '%s' "$LOGS" | grep -o '"fanout_p99_ms":[0-9.]*' | head -1 | cut -d: -f2 || echo "N/A")
  P50=$(printf '%s' "$LOGS" | grep -o '"fanout_p50_ms":[0-9.]*' | head -1 | cut -d: -f2 || echo "N/A")
  STATUS=$(job_passed fanoutscale && echo "PASS" || echo "FAIL")

  printf '| %d | %s | %s | %s |\n' "$N" "${P99}" "${P50}" "${STATUS}" >>"$RESULTS"
  info "N=${N}: p99=${P99}ms p50=${P50}ms -> ${STATUS}"
done
rm -rf "$P1_TMP"

printf '\n' >>"$RESULTS"

# ─── PHASE 2: CLAIM-CONTENTION KNEE ──────────────────────────────────────────
# Gate #6 baseline (G=1, no claim sharding yet). Run single-holder-linz at
# W=4 and W=16 workers: proves the fence holds under concurrent claim pressure.
# G sharding (the actual gate #6 improvement) is a separate implementation.
log "Phase 2: Claim-contention (single-holder-linz at W=4 and W=16)"

# Ensure 2 replicas so the port-forward survives individual pod kills.
kubectl -n "$NS" scale deployment/chronicle --replicas=2
kubectl -n "$NS" rollout status deploy/chronicle --timeout=120s
start_pfwd

printf '## Phase 2: Claim-contention knee (gate #6 baseline, G=1)\n\n' >>"$RESULTS"
printf '| Workers | linearizable | result |\n' >>"$RESULTS"
printf '|---|---|---|\n' >>"$RESULTS"

for W in 4 16; do
  log "Phase 2: single-holder-linz W=${W}"
  LOG2="/tmp/jepsen-linz-w${W}.txt"

  if "$JEPSEN_BIN" \
      -base "http://localhost:14438" \
      -scenario single-holder-linz \
      -workers "$W" \
      -workload-ms 12000 \
      2>&1 | tee "$LOG2"; then
    J2_RESULT="PASS"
  else
    J2_RESULT="FAIL"
  fi

  LINZ=$(grep -c "linearizable:.*yes" "$LOG2" 2>/dev/null && echo "yes" || echo "no")
  printf '| %d | %s | %s |\n' "$W" "$LINZ" "$J2_RESULT" >>"$RESULTS"
done

printf '\n' >>"$RESULTS"

# ─── PHASE 3: GATE #2 FAN-OUT ON CLUSTER ─────────────────────────────────────
# Redeploy chronicle against the Memorystore CLUSTER (redis+cluster://), then
# measure chronicle_fanout_seconds p99 at S=2/4/8/256 subscribers.
# Gate #2 passes when p99 < 50ms at S≤8 and p99 < 100ms at S=256.
log "Phase 3: Gate #2 fan-out on Memorystore CLUSTER"

# Kill port-forward (not needed in this phase).
kill "$PFWD_PID" 2>/dev/null || true; PFWD_PID=""

# Scale back to N=1 for the gate #2 measurement (clean single-replica signal).
kubectl -n "$NS" scale deployment/chronicle --replicas=1
kubectl -n "$NS" rollout status deploy/chronicle --timeout=120s

printf '## Phase 3: Gate #2 fan-out (Memorystore CLUSTER, N=1)\n\n' >>"$RESULTS"
printf '| S subs | fanout_p99_ms | probe_p99_ms | SLO | job_status |\n' >>"$RESULTS"
printf '|---|---|---|---|---|\n' >>"$RESULTS"

for S in 2 4 8 256; do
  SLO=$([ "$S" -le 8 ] && echo 50 || echo 100)
  log "Phase 3: S=${S} subs, SLO p99 < ${SLO}ms (CLUSTER)"

  P3_TMP=$(mktemp -d)
  render_fanout "$P3_TMP" "$CLUSTER_REDIS_URL" "$S" "$SLO"

  # Redeploy SUT pointing at CLUSTER Redis (Recreate strategy swaps the pod).
  deploy_sut "$P3_TMP/sut.yaml"
  run_fanout_job "$P3_TMP/job.yaml" 300s

  LOGS=$(job_logs fanoutscale)
  P99=$(printf '%s' "$LOGS" | grep -o '"fanout_p99_ms":[0-9.]*' | head -1 | cut -d: -f2 || echo "N/A")
  PR99=$(printf '%s' "$LOGS" | grep -o '"probe_p99_ms":[0-9.]*' | head -1 | cut -d: -f2 || echo "N/A")
  STATUS=$(job_passed fanoutscale && echo "PASS" || echo "FAIL")

  printf '| %d | %s | %s | <%dms | %s |\n' "$S" "${P99}" "${PR99}" "$SLO" "${STATUS}" >>"$RESULTS"
  info "S=${S}: fanout_p99=${P99}ms probe_p99=${PR99}ms → ${STATUS}"
  rm -rf "$P3_TMP"
done

printf '\n' >>"$RESULTS"

# ─── PHASE 4: FAIR K=10k SWEEP ───────────────────────────────────────────────
# Redeploy on standalone Redis (single slot for {__ds} control-plane keys).
# SLO: sweep tick p99 < 1500ms (must beat the 2s sweep interval with headroom).
log "Phase 4: Fair K=10k sweep (e2-standard-4 / cpu:2, standalone Redis)"

P4_TMP=$(mktemp -d)
render_sweep "$P4_TMP" "$STANDALONE_REDIS_URL"
deploy_sut "$P4_TMP/sut.yaml"

kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
kubectl apply -f "$P4_TMP/job.yaml"
# K=10k with K=5 links + 120s measure window → allow 10 min total.
wait_job sweepscale 600s

SWEEP_LOGS=$(job_logs sweepscale)
SP99=$(printf '%s' "$SWEEP_LOGS" | grep -o '"sweep_p99_ms":[0-9.]*' | head -1 | cut -d: -f2 || echo "N/A")
SWEEP_STATUS=$(job_passed sweepscale && echo "PASS" || echo "FAIL")
rm -rf "$P4_TMP"

printf '## Phase 4: Fair K=10k sweep\n\n' >>"$RESULTS"
printf '| K | cpu | sweep_p99_ms | SLO (<1500ms) | job_status |\n' >>"$RESULTS"
printf '|---|---|---|---|---|\n' >>"$RESULTS"
printf '| 10000 | 2 | %s | <1500ms | %s |\n' "${SP99}" "${SWEEP_STATUS}" >>"$RESULTS"
info "K=10k: sweep_p99=${SP99}ms → ${SWEEP_STATUS}"

printf '\n' >>"$RESULTS"

# ─── PHASE 5: JEPSEN CORRECTNESS ─────────────────────────────────────────────
# Three scenarios that work over a port-forwarded chronicle with a GKE kubectl
# context (no cluster→host webhook back-channel needed):
#
#   expired-lease-takeover  (L2) — fence rotation on stale-lease takeover
#   single-holder-linz      (L4) — fence-contention linearizability
#   pull-wake-arm-crash     (T4) — durable pull-wake recovery under pod churn
#
# baseline and origin-restart (T1/T3) require a webhook receiver reachable FROM
# the GKE pods, which needs the checker deployed in-cluster. Deferred.
log "Phase 5: Jepsen correctness"

# Ensure 2 replicas (pull-wake-arm-crash kills one pod at a time; 2nd handles traffic).
kubectl -n "$NS" scale deployment/chronicle --replicas=2
kubectl -n "$NS" rollout status deploy/chronicle --timeout=120s
start_pfwd

printf '## Phase 5: Jepsen correctness\n\n' >>"$RESULTS"
printf '| Scenario | Property | Result |\n' >>"$RESULTS"
printf '|---|---|---|\n' >>"$RESULTS"

run_jepsen_scenario() {
  local scenario="$1"
  shift
  local extra=("$@")
  local logfile="/tmp/jepsen-${scenario}.txt"

  log "Jepsen: ${scenario}"
  if "$JEPSEN_BIN" \
      -base "http://localhost:14438" \
      -scenario "$scenario" \
      -context "$GKE_CTX" \
      -namespace "$NS" \
      "${extra[@]}" \
      2>&1 | tee "$logfile"; then
    echo "PASS"
  else
    echo "FAIL"
  fi
}

RESULT_ELT=$(run_jepsen_scenario "expired-lease-takeover")
printf '| expired-lease-takeover | L2: fence rotates on stale-lease takeover | %s |\n' "$RESULT_ELT" >>"$RESULTS"

RESULT_SHL=$(run_jepsen_scenario "single-holder-linz" -workers 4 -workload-ms 12000)
printf '| single-holder-linz | L4: lease fence linearizable under GC-pause nemesis | %s |\n' "$RESULT_SHL" >>"$RESULTS"

RESULT_PWAC=$(run_jepsen_scenario "pull-wake-arm-crash" -streams 6 -msgs 20 -settle 25s)
printf '| pull-wake-arm-crash | T4: arm-without-emit windows recovered by sweep | %s |\n' "$RESULT_PWAC" >>"$RESULTS"

printf '| baseline | T1: skipped — requires cluster→host webhook back-channel (GKE constraint) | SKIPPED |\n' >>"$RESULTS"
printf '| origin-restart | T3: skipped — requires cluster→host webhook back-channel (GKE constraint) | SKIPPED |\n' >>"$RESULTS"

printf '\n' >>"$RESULTS"

# ─── SUMMARY ─────────────────────────────────────────────────────────────────
cat >>"$RESULTS" <<'SUMMARY'
## Gate status

| Gate | Condition | Phase |
|---|---|---|
| Gate #2 fan-out p99 | p99 < 50ms at S≤8; < 100ms at S=256 | Phase 3 |
| Gate #1 O(N·K) | sweep p99 < 1500ms at K=10k, N=1 | Phase 4 |
| Horizontal scale | fanout p99 flat/decreasing as N grows 1→4 | Phase 1 |
| Linearizability | single-holder-linz + expired-lease-takeover PASS | Phase 2+5 |
| Pull-wake recovery | pull-wake-arm-crash PASS | Phase 5 |

SUMMARY

printf '_Generated by `loadtest/run-gate2.sh` at %s_\n' "$DATE_UTC" >>"$RESULTS"

log "All phases complete"
info "Results: ${RESULTS}"
cat "$RESULTS"
# teardown_all runs via EXIT trap
