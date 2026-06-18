#!/usr/bin/env bash
# ltctl — one-command spin-up / run / teardown for the chronicle load-test rig.
#
#   ./ltctl.sh all spec/sweep-10k.yaml   # provision → run → ALWAYS tear down
#   ./ltctl.sh up                        # provision only (idempotent)
#   ./ltctl.sh run spec/sweep-10k.yaml   # render + deploy + run the job
#   ./ltctl.sh down                      # delete cluster + Redis (keep AR images)
#
# `all` tears down on EXIT, interrupt, or failure, so the meter never runs by
# accident. Every step is idempotent: a re-run reuses what already exists.
#
# Config via env (defaults shown): LT_PROJECT LT_ZONE=us-central1-a
# LT_REGION=us-central1 LT_CLUSTER=chronicle-loadtest LT_AR_REPO=chronicle
# LT_TAG=v1 LT_MACHINE=e2-standard-2 LT_DISK_GB=50 LT_REDIS_SIZE_GB=1
# LT_REDIS_VERSION=redis_7_2
set -euo pipefail

: "${LT_PROJECT:=$(gcloud config get-value project 2>/dev/null)}"
: "${LT_ZONE:=us-central1-a}"
: "${LT_REGION:=us-central1}"
: "${LT_CLUSTER:=chronicle-loadtest}"
: "${LT_AR_REPO:=chronicle}"
: "${LT_TAG:=v1}"
: "${LT_MACHINE:=e2-standard-2}"
: "${LT_DISK_GB:=50}"
: "${LT_REDIS_SIZE_GB:=1}"
: "${LT_REDIS_VERSION:=redis_7_2}"

NS=chronicle-loadtest
REG="${LT_REGION}-docker.pkg.dev/${LT_PROJECT}/${LT_AR_REPO}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
G=(gcloud --project "$LT_PROJECT" --quiet)

log() { printf '\n\033[1;36m▸ %s\033[0m\n' "$*" >&2; }

cmd_up() {
  log "APIs"
  "${G[@]}" services enable container.googleapis.com redis.googleapis.com \
    artifactregistry.googleapis.com cloudbuild.googleapis.com

  log "Artifact Registry repo: $LT_AR_REPO"
  "${G[@]}" artifacts repositories describe "$LT_AR_REPO" --location "$LT_REGION" >/dev/null 2>&1 ||
    "${G[@]}" artifacts repositories create "$LT_AR_REPO" --repository-format=docker --location "$LT_REGION"

  log "images → $REG (Cloud Build, amd64)"
  ( cd "$REPO_ROOT" && "${G[@]}" builds submit --config loadtest/cloudbuild.yaml \
      --substitutions=_REG="$REG",_TAG="$LT_TAG" . )

  log "Memorystore Redis (basic ${LT_REDIS_SIZE_GB}G, noeviction)"
  "${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" >/dev/null 2>&1 ||
    "${G[@]}" redis instances create "${LT_CLUSTER}-redis" --size "$LT_REDIS_SIZE_GB" --region "$LT_REGION" \
      --tier basic --redis-version "$LT_REDIS_VERSION" --redis-config maxmemory-policy=noeviction

  log "GKE cluster + node pools (sut, loadgen)"
  "${G[@]}" container clusters describe "$LT_CLUSTER" --zone "$LT_ZONE" >/dev/null 2>&1 ||
    "${G[@]}" container clusters create "$LT_CLUSTER" --zone "$LT_ZONE" --num-nodes 1 \
      --machine-type "$LT_MACHINE" --disk-type pd-standard --disk-size "$LT_DISK_GB" \
      --node-labels role=sut --no-enable-autoupgrade
  "${G[@]}" container node-pools describe loadgen --cluster "$LT_CLUSTER" --zone "$LT_ZONE" >/dev/null 2>&1 ||
    "${G[@]}" container node-pools create loadgen --cluster "$LT_CLUSTER" --zone "$LT_ZONE" --num-nodes 1 \
      --machine-type "$LT_MACHINE" --disk-type pd-standard --disk-size "$LT_DISK_GB" \
      --node-labels role=loadgen --no-enable-autoupgrade

  log "kubeconfig"
  "${G[@]}" container clusters get-credentials "$LT_CLUSTER" --zone "$LT_ZONE"
  log "up complete"
}

cmd_run() {
  local spec="${1:?usage: ltctl run <spec.yaml>}"
  local spec_abs out redis_host
  spec_abs="$(cd "$(dirname "$spec")" && pwd)/$(basename "$spec")"
  redis_host="$("${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" --format='value(host)')"
  out="$(mktemp -d)"

  log "render $spec (redis=$redis_host, images=$REG/*:$LT_TAG)"
  ( cd "$REPO_ROOT/loadgen" && go run ./cmd/render -spec "$spec_abs" -out "$out" \
      -redis-url "redis://${redis_host}:6379/0" \
      -image "$REG/chronicle:$LT_TAG" -loadgen-image "$REG/chronicle-loadgen:$LT_TAG" )

  log "deploy SUT"
  kubectl apply -f "$out/sut.yaml"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=180s

  log "run sweepscale job"
  kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
  kubectl apply -f "$out/job.yaml"
  kubectl -n "$NS" wait --for=condition=complete --timeout=600s job -l app=sweepscale 2>/dev/null ||
    kubectl -n "$NS" wait --for=condition=failed --timeout=5s job -l app=sweepscale 2>/dev/null || true

  log "report"
  kubectl -n "$NS" logs -l app=sweepscale --tail=-1
  if [ "$(kubectl -n "$NS" get job -l app=sweepscale -o jsonpath='{.items[0].status.succeeded}')" = "1" ]; then
    log "SLO PASS"
  else
    log "SLO FAIL (sweep p99 over budget, or error)"; return 1
  fi
}

# cmd_chaos force-deletes the SUT pods mid-run — the rig's coarse pod-kill nemesis
# (Migration slice 0; gate #4 / 07 L2/L4). Safe to repeat: the Deployment
# reschedules the pods, and chronicle's recovery sweep re-fires owed wakes.
cmd_chaos() {
  log "chaos: force-deleting chronicle pods in $NS (the deployment reschedules them)"
  kubectl -n "$NS" delete pods -l app=chronicle --grace-period=0 --force
}

# cmd_gate1 is experiment 1 (the O(N·K) premise): ramp the SUT replicas 1→4 at a
# FIXED K and read Memorystore CPU at each N. Every replica sweeps all K, so the
# control-plane {__ds} slot's Redis CPU should grow ~N× while the per-replica
# sweep p99 stays flat (and under the SLO — the K=10k floor reproduces). Assumes
# `up` already ran. Writes a CPU-vs-N table to gate1-results.tsv.
cmd_gate1() {
  local spec="${1:-spec/sweep-10k-scale.yaml}"
  local spec_abs out redis_host p99 cpu cmax cmean
  spec_abs="$(cd "$(dirname "$spec")" && pwd)/$(basename "$spec")"
  redis_host="$("${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" --format='value(host)')"

  log "gate #1: ramp replicas 1→4 at fixed K, read Memorystore CPU"
  printf 'N\tsweep_p99_ms\tredis_cpu_max\tredis_cpu_mean\n' | tee gate1-results.tsv
  for n in 1 2 3 4; do
    out="$(mktemp -d)"
    log "gate #1 step: replicas=$n"
    ( cd "$REPO_ROOT/loadgen" && go run ./cmd/render -spec "$spec_abs" -out "$out" \
        -redis-url "redis://${redis_host}:6379/0" -replicas "$n" \
        -image "$REG/chronicle:$LT_TAG" -loadgen-image "$REG/chronicle-loadgen:$LT_TAG" )
    kubectl apply -f "$out/sut.yaml"
    kubectl -n "$NS" rollout status deploy/chronicle --timeout=180s
    kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
    kubectl apply -f "$out/job.yaml"
    kubectl -n "$NS" wait --for=condition=complete --timeout=600s job -l app=sweepscale 2>/dev/null ||
      kubectl -n "$NS" wait --for=condition=failed --timeout=5s job -l app=sweepscale 2>/dev/null || true
    p99="$(kubectl -n "$NS" logs -l app=sweepscale --tail=-1 | grep -o '"sweep_p99_ms":[ ]*[0-9.]*' | head -1 | grep -o '[0-9.]*$')"
    # Read Memorystore CPU over the just-finished measure window (the gate-#1 signal).
    cpu="$( cd "$REPO_ROOT/loadgen" && go run ./cmd/rediscpu -project "$LT_PROJECT" -instance "${LT_CLUSTER}-redis" -window 2m 2>/dev/null )"
    cmax="$(printf '%s' "$cpu" | grep -o 'max=[0-9.]*' | cut -d= -f2)"
    cmean="$(printf '%s' "$cpu" | grep -o 'mean=[0-9.]*' | cut -d= -f2)"
    printf '%s\t%s\t%s\t%s\n' "$n" "${p99:-?}" "${cmax:-?}" "${cmean:-?}" | tee -a gate1-results.tsv
  done
  log "gate #1 done — CPU-vs-N curve in gate1-results.tsv (expect Redis CPU to rise ~N× at fixed K)"
}

# cmd_gate2 is experiment 2 (THE DECIDING NUMBER for slot-homing, issue #15): the
# OnStreamAppend fan-out p99 regression under the S-slot {__ds:h} shard. It deploys
# the wide-stream webhook fan-out spec at replicas>=2 (so the {__ds:h} slots span
# Redis Cluster nodes — the max-node-RTT this gate measures; loopback erases it),
# drives the workload, and scrapes chronicle_fanout_seconds (the p99 gate) +
# chronicle_fanout_slots_probed (the bitmap effect) from a surviving pod. Because
# S=subSlots is a COMPILE-TIME const, the 2/4/8/256 sweep is one SUT IMAGE PER S:
# build chronicle with `const subSlots=<S>`, push as LT_TAG=s<S>, and run gate2 per
# tag — the per-S fan-out p99 lands in gate2-results.tsv. Assumes `up` already ran.
cmd_gate2() {
  local spec="${1:-spec/fanout-gate2.yaml}"
  local spec_abs out redis_host p99 probed sweepp99
  spec_abs="$(cd "$(dirname "$spec")" && pwd)/$(basename "$spec")"
  redis_host="$("${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" --format='value(host)')"
  out="$(mktemp -d)"

  log "gate #2: render $spec at replicas>=2 (the {__ds:h} slots must span Redis nodes), S build = ${LT_TAG}"
  ( cd "$REPO_ROOT/loadgen" && go run ./cmd/render -spec "$spec_abs" -out "$out" \
      -redis-url "redis://${redis_host}:6379/0" \
      -image "$REG/chronicle:$LT_TAG" -loadgen-image "$REG/chronicle-loadgen:$LT_TAG" )
  kubectl apply -f "$out/sut.yaml"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=180s

  log "gate #2: launch the wide-stream webhook fan-out workload"
  kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
  kubectl apply -f "$out/job.yaml"
  kubectl -n "$NS" wait --for=condition=complete --timeout=600s job -l app=sweepscale 2>/dev/null ||
    kubectl -n "$NS" wait --for=condition=failed --timeout=5s job -l app=sweepscale 2>/dev/null || true
  sweepp99="$(kubectl -n "$NS" logs -l app=sweepscale --tail=-1 | grep -o '"sweep_p99_ms":[ ]*[0-9.]*' | head -1 | grep -o '[0-9.]*$')"

  log "gate #2: fan-out p99 + slots-probed from a surviving pod (the deciding number)"
  # chronicle_fanout_seconds is the append→wake fan-out p99 (the regression gate #2
  # decides on); chronicle_fanout_slots_probed must track occupied-slots-per-stream,
  # NOT S, proving the occupied-slots bitmap mitigation holds at S=256.
  kubectl -n "$NS" exec deploy/chronicle -- sh -c 'wget -qO- http://localhost:9090/metrics' 2>/dev/null |
    grep -E 'chronicle_fanout_seconds|chronicle_fanout_slots_probed' | tee "gate2-${LT_TAG}-metrics.txt" || true
  p99="$(awk -F'[{} ]+' '/chronicle_fanout_seconds_bucket/{print}' "gate2-${LT_TAG}-metrics.txt" 2>/dev/null | tail -1)"
  printf 'S_build\tsweep_p99_ms\tfanout_p99_note\n' >> gate2-results.tsv 2>/dev/null || true
  printf '%s\t%s\t%s\n' "${LT_TAG}" "${sweepp99:-?}" "see gate2-${LT_TAG}-metrics.txt (chronicle_fanout_seconds histogram)" | tee -a gate2-results.tsv
  log "gate #2 done for ${LT_TAG} — fan-out p99 in gate2-${LT_TAG}-metrics.txt; repeat per S (2/4/8/256). WITHIN budget => ship slot-homing; OVER => defer per 05."
}

# cmd_gate4 is experiment 4 (the membership churn window) — the INVERSE of gate #1
# (issue #14, 07 L2/L4). It deploys the churn spec at replicas>=2 (HRW shards the
# slots), launches the sweepscale job, force-deletes the slot owner mid-window (the
# coarse pod-kill nemesis), and scrapes the coverage-gap / slot-ownership /
# owner-fenced metrics from a surviving pod. Pass: the coverage gap recovers within
# membership-lease TTL + RTT (NOT a sweep tick), ZERO lost wakes, ZERO double-grants,
# and total work stays O(total owed) regardless of N. Assumes `up` already ran.
cmd_gate4() {
  local spec="${1:-spec/sweep-10k-churn.yaml}"
  local spec_abs out redis_host owner pod
  spec_abs="$(cd "$(dirname "$spec")" && pwd)/$(basename "$spec")"
  redis_host="$("${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" --format='value(host)')"
  out="$(mktemp -d)"

  log "gate #4: render $spec at replicas>=2 (HRW slot-sharding)"
  ( cd "$REPO_ROOT/loadgen" && go run ./cmd/render -spec "$spec_abs" -out "$out" \
      -redis-url "redis://${redis_host}:6379/0" \
      -image "$REG/chronicle:$LT_TAG" -loadgen-image "$REG/chronicle-loadgen:$LT_TAG" )
  kubectl apply -f "$out/sut.yaml"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=180s

  log "gate #4: launch sweepscale job, then pod-kill the slot owner mid-window"
  kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
  kubectl apply -f "$out/job.yaml"
  # Let the workload warm up and ownership settle, then kill the OWNER of slot 0
  # specifically (the killSlotOwner nemesis: read owner_id, map to its pod, kill it),
  # forcing a takeover (a new-owner claim_shard CAS + eager reconcile).
  sleep 45
  owner="$(kubectl -n "$NS" exec deploy/chronicle -- sh -c "redis-cli -u redis://${redis_host}:6379/0 hget 'ds:{ownership}:slot:0' owner_id" 2>/dev/null | tr -d '\r')"
  pod="${owner%-*}" # replica_id is "<podName>-<32hex nonce>"
  if [ -n "$pod" ] && kubectl -n "$NS" get pod "$pod" >/dev/null 2>&1; then
    log "gate #4: slot-0 owner is $owner; force-deleting its pod $pod"
    kubectl -n "$NS" delete pod "$pod" --grace-period=0 --force
  else
    log "gate #4: could not resolve the slot-0 owner pod ($owner); falling back to the coarse pod-kill"
    cmd_chaos
  fi

  kubectl -n "$NS" wait --for=condition=complete --timeout=600s job -l app=sweepscale 2>/dev/null ||
    kubectl -n "$NS" wait --for=condition=failed --timeout=5s job -l app=sweepscale 2>/dev/null || true

  log "gate #4: sweepscale report"
  kubectl -n "$NS" logs -l app=sweepscale --tail=-1
  log "gate #4: ownership metrics from a surviving pod (coverage gap, double-grants, fences)"
  # The gate-#4 signals: coverage_gap p99 (<= membership-lease TTL + RTT), the
  # slot-ownership event mix (claimed on the takeover, never two live owners), and
  # the owner-fenced count (the deposed owner suppressed). Lost wakes are 0 iff the
  # sweepscale tail check passed above.
  kubectl -n "$NS" exec deploy/chronicle -- sh -c 'wget -qO- http://localhost:9090/metrics' 2>/dev/null |
    grep -E 'chronicle_coverage_gap_seconds|chronicle_slot_ownership_events_total|chronicle_owner_fenced_total' || true
  log "gate #4 done — coverage gap should recover within membership-lease TTL + RTT (NOT a sweep tick); zero lost wakes; zero double-grants"
}

_torn=0
cmd_down() {
  [ "$_torn" = 1 ] && return 0
  _torn=1
  log "teardown (deleting cluster + Redis; keeping Artifact Registry images)"
  "${G[@]}" container clusters delete "$LT_CLUSTER" --zone "$LT_ZONE" 2>/dev/null &
  local c=$!
  "${G[@]}" redis instances delete "${LT_CLUSTER}-redis" --region "$LT_REGION" 2>/dev/null &
  local r=$!
  wait "$c" || true
  wait "$r" || true
  log "down complete — meter stopped"
}

cmd_all() {
  local spec="${1:?usage: ltctl all <spec.yaml>}"
  trap cmd_down EXIT INT TERM   # ALWAYS tear down — even on failure or Ctrl-C
  cmd_up
  cmd_run "$spec"
}

case "${1:-}" in
  up) shift; cmd_up "$@" ;;
  run) shift; cmd_run "$@" ;;
  gate1) shift; cmd_gate1 "$@" ;;
  gate2) shift; cmd_gate2 "$@" ;;
  gate4) shift; cmd_gate4 "$@" ;;
  chaos) shift; cmd_chaos "$@" ;;
  down) shift; cmd_down "$@" ;;
  all) shift; cmd_all "$@" ;;
  *) echo "usage: $0 {up | run <spec> | gate1 [spec] | gate2 [spec] | gate4 [spec] | chaos | down | all <spec>}" >&2; exit 2 ;;
esac
