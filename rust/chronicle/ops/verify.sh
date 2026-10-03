#!/usr/bin/env bash
set -euo pipefail

CLUSTER=${CLUSTER:-chronicle-rust}
KUBECONFIG=${KUBECONFIG:-/tmp/chronicle-kubeconfig}
export KUBECONFIG
K="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/kubectl.sh"
[[ "$CLUSTER" == chronicle-rust ]] || { echo "refusing unexpected cluster: $CLUSTER" >&2; exit 2; }

"$K" -n observability wait --for=condition=available deployment/victoria-metrics deployment/victoria-logs deployment/victoria-traces deployment/otel-collector deployment/grafana --timeout=5m
probe='{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"chronicle-ops-setup-probe"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"'"$(date +%s)"'000000000","severityText":"INFO","body":{"stringValue":"synthetic marked setup probe; not server telemetry"}}]}]}]}'
"$K" -n observability run setup-probe --rm -i --restart=Never \
  --image=curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
  --command -- curl -fsS -H 'Content-Type: application/json' -d "$probe" http://otel-collector:4318/v1/logs >/dev/null
sleep 3
result=$("$K" -n observability run setup-query --rm -i --restart=Never \
  --image=curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
  --command -- curl -fsS --get --data-urlencode 'query=_msg:"synthetic marked setup probe; not server telemetry"' \
  http://victoria-logs:9428/select/logsql/query)
grep -q 'synthetic marked setup probe; not server telemetry' <<<"$result"
echo "Victoria stack ready; synthetic marked log traversed OTel -> VictoriaLogs."
