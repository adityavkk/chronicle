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
# LT_REGION=us-central1 LT_CLUSTER=chronicle-loadtest-codex LT_AR_REPO=chronicle-codex
# LT_TAG=v1 LT_MACHINE=e2-standard-2 LT_DISK_GB=50 LT_REDIS_SIZE_GB=5
# LT_REDIS_TIER=standard LT_REDIS_VERSION=redis_7_2 LT_CHAOS=none
set -euo pipefail

while [ "$#" -gt 0 ]; do
  case "$1" in
    --project)
      if [ "$#" -lt 2 ]; then
        echo "usage: ltctl.sh --project <project> {up|run|down|all}" >&2
        exit 2
      fi
      LT_PROJECT="$2"
      shift 2
      ;;
    --project=*)
      LT_PROJECT="${1#--project=}"
      shift
      ;;
    *)
      break
      ;;
  esac
done

: "${LT_PROJECT:=$(gcloud config get-value project 2>/dev/null)}"
: "${LT_ZONE:=us-central1-a}"
: "${LT_REGION:=us-central1}"
: "${LT_CLUSTER:=chronicle-loadtest-codex}"
: "${LT_AR_REPO:=chronicle-codex}"
: "${LT_TAG:=v1}"
: "${LT_MACHINE:=e2-standard-2}"
: "${LT_DISK_GB:=50}"
: "${LT_REDIS_SIZE_GB:=5}"
: "${LT_REDIS_TIER:=standard}"
: "${LT_REDIS_VERSION:=redis_7_2}"
: "${LT_CHAOS:=none}"
: "${LT_CHAOS_PERIOD:=15s}"

NS=chronicle-loadtest-codex
REG="${LT_REGION}-docker.pkg.dev/${LT_PROJECT}/${LT_AR_REPO}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${LT_REPORT_DIR:=$REPO_ROOT/loadtest/out/reports}"
: "${LT_KUBECONFIG:=$REPO_ROOT/loadtest/out/kubeconfig-${LT_CLUSTER}}"
mkdir -p "$(dirname "$LT_KUBECONFIG")"
export KUBECONFIG="$LT_KUBECONFIG"
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

  log "Memorystore Redis (${LT_REDIS_TIER} ${LT_REDIS_SIZE_GB}G, noeviction)"
  "${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" >/dev/null 2>&1 ||
    "${G[@]}" redis instances create "${LT_CLUSTER}-redis" --size "$LT_REDIS_SIZE_GB" --region "$LT_REGION" \
      --tier "$LT_REDIS_TIER" --redis-version "$LT_REDIS_VERSION" --redis-config maxmemory-policy=noeviction

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
	local spec_abs out redis_host start_ts end_ts report_base job_log cpu_json
	spec_abs="$(cd "$(dirname "$spec")" && pwd)/$(basename "$spec")"
	redis_host="$("${G[@]}" redis instances describe "${LT_CLUSTER}-redis" --region "$LT_REGION" --format='value(host)')"
	out="$(mktemp -d)"
	mkdir -p "$LT_REPORT_DIR"
	report_base="$LT_REPORT_DIR/$(basename "${spec%.yaml}")-$(date -u +%Y%m%dT%H%M%SZ)"
	job_log="${report_base}-sweepscale.log"
	cpu_json="${report_base}-redis-cpu.json"

	log "render $spec (redis=$redis_host, images=$REG/*:$LT_TAG)"
  ( cd "$REPO_ROOT/loadgen" && go run ./cmd/render -spec "$spec_abs" -out "$out" \
      -redis-url "redis://${redis_host}:6379/0" \
      -image "$REG/chronicle:$LT_TAG" -loadgen-image "$REG/chronicle-loadgen:$LT_TAG" )

  log "deploy SUT"
  kubectl apply -f "$out/sut.yaml"
  kubectl -n "$NS" rollout status deploy/chronicle --timeout=180s

	log "run sweepscale job"
	kubectl -n "$NS" delete job -l app=sweepscale --ignore-not-found >/dev/null
	start_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	kubectl apply -f "$out/job.yaml"
	start_chaos
	wait_sweepscale_job || true
	stop_chaos
	end_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

	log "report"
	kubectl -n "$NS" logs -l app=sweepscale --tail=-1 | tee "$job_log"
	print_redis_cpu "$start_ts" "$end_ts" "$cpu_json" || true
	log "reports written: $job_log $cpu_json"
	if [ "$(kubectl -n "$NS" get job -l app=sweepscale -o jsonpath='{.items[0].status.succeeded}')" = "1" ]; then
	log "SLO PASS"
  else
    log "SLO FAIL (sweep p99 over budget, or error)"; return 1
	fi
}

wait_sweepscale_job() {
	local deadline succeeded failed
	deadline=$((SECONDS + 600))
	while [ "$SECONDS" -lt "$deadline" ]; do
		succeeded="$(kubectl -n "$NS" get job -l app=sweepscale -o jsonpath='{.items[0].status.succeeded}' 2>/dev/null || true)"
		failed="$(kubectl -n "$NS" get job -l app=sweepscale -o jsonpath='{.items[0].status.failed}' 2>/dev/null || true)"
		if [ "$succeeded" = "1" ]; then
			return 0
		fi
		if [ -n "$failed" ] && [ "$failed" != "0" ]; then
			return 0
		fi
		sleep 2
	done
	return 1
}

CHAOS_PID=""
start_chaos() {
	if [ "$LT_CHAOS" != "pod-kill" ]; then
		return 0
	fi
	(
		while true; do
			sleep "$LT_CHAOS_PERIOD"
			pod="$(kubectl -n "$NS" get pods -l app=chronicle -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
			if [ -n "$pod" ]; then
				kubectl -n "$NS" delete pod "$pod" --grace-period=0 --force >/dev/null 2>&1 || true
				printf 'chaos: killed chronicle pod %s\n' "$pod" >&2
			fi
		done
	) &
	CHAOS_PID=$!
}

stop_chaos() {
	if [ -n "$CHAOS_PID" ]; then
		kill "$CHAOS_PID" >/dev/null 2>&1 || true
		wait "$CHAOS_PID" >/dev/null 2>&1 || true
		CHAOS_PID=""
	fi
}

print_redis_cpu() {
	local start_ts="${1:?}" end_ts="${2:?}" out="${3:?}"
	local raw
	raw="$(mktemp)"
	if ! "${G[@]}" monitoring time-series list \
		--filter="metric.type=\"redis.googleapis.com/stats/cpu_utilization\" AND resource.labels.instance_id=\"${LT_CLUSTER}-redis\"" \
		--interval-start-time="$start_ts" \
		--interval-end-time="$end_ts" \
		--format=json >"$raw"; then
		printf '{"error":"gcloud monitoring time-series list failed"}\n' | tee "$out"
		return 1
	fi
	if ! ( cd "$REPO_ROOT" && go run ./loadtest/cmd/rediscpu <"$raw" ) | tee "$out"; then
		printf '{"error":"rediscpu parser failed"}\n' >"$out"
		return 1
	fi
}

_torn=0
cmd_down() {
	[ "$_torn" = 1 ] && return 0
	_torn=1
	stop_chaos
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
  down) shift; cmd_down "$@" ;;
  all) shift; cmd_all "$@" ;;
  *) echo "usage: $0 {up | run <spec> | down | all <spec>}" >&2; exit 2 ;;
esac
