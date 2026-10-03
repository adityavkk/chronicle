#!/usr/bin/env bash
# Hook for the existing history driver. Timing and histories belong to that driver.
# Fresh test cluster state: node4 registered/draining, node2 a current voter.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
[[ "$($root/ops/kubectl.sh config current-context)" == k3d-chronicle-rust ]] || exit 2
url=${2:?private local Chronicle URL required}
case ${1:?start or restore required} in
  start) resume=4; drain=2 ;;
  restore) resume=2; drain=4 ;;
  *) exit 2 ;;
esac
# Preserve persisted address/zone; never initialize or replace an identity. Send
# once: on an unknown mutation result, inspect control state instead of rerunning.
PYTHONPATH="$root/ops" python3 - "$url" "$resume" "$drain" <<'PY'
import json
import sys
from drain import request
url, node_id, drain_id = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
state = request(url + "/admin/control")
node = state["nodes"][str(node_id)]
if not node["draining"] or len(state["placements"]) != 5 or not all(
    p["complete"] and node_id not in p["voters"] and drain_id in p["voters"]
    for p in state["placements"].values()
):
    raise RuntimeError("unexpected placement; inspect state, do not blindly repeat the hook")
node["draining"] = False
print(json.dumps(request(url + "/admin/register", [node_id, node])))
PY
python3 "$root/ops/drain.py" --url "$url" --node "$drain" --timeout 120
