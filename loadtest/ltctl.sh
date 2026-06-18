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
  chaos) shift; cmd_chaos "$@" ;;
  down) shift; cmd_down "$@" ;;
  all) shift; cmd_all "$@" ;;
  *) echo "usage: $0 {up | run <spec> | gate1 [spec] | chaos | down | all <spec>}" >&2; exit 2 ;;
esac
