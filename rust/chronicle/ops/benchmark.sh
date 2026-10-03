#!/usr/bin/env bash
# Representative closed-loop comparison, not a capacity or SLO test.
set -euo pipefail
URL=${1:?private Chronicle URL required}
OUT=${2:?new output directory required}
SEED=${3:?fresh numeric seed required}
OPERATIONS=${OPERATIONS:-256}
GO=${GO:-go}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir "$OUT"
OUT=$(realpath "$OUT")
export URL OUT ROOT OPERATIONS

# Use the documented stable mapping; both cases have eight producers and four
# strict readers. The spread case has one stream per virtual data shard.
python3 - "$SEED" "$OUT" <<'PY'
import hashlib, pathlib, sys
seed, out = int(sys.argv[1]), pathlib.Path(sys.argv[2])
for mode in ("hot", "many"):
    paths = {}
    for n in range(10000):
        path = f"bench-{seed}-{mode}-{n}"
        group = int.from_bytes(hashlib.sha256(("5:bench" + path).encode()).digest()[:8], "big") % 4 + 1
        paths.setdefault(group, path)
        if len(paths) == 4:
            break
    with (out / f"{mode}.tsv").open("x") as f:
        for group in ([1] if mode == "hot" else [1, 2, 3, 4]):
            print(group, seed + group, paths[group], file=f)
PY

run_history() {
  local name=$1 seed=$2 path=$3 producers=$4 readers=$5
  python3 "$ROOT/tests/history.py" run --url "$URL" --tenant bench --path "$path" \
    --seed "$seed" --producers "$producers" --readers "$readers" \
    --operations "$OPERATIONS" --read-interval .2 --timeout 10 --retries 3 \
    --output "$OUT/$name.jsonl" > "$OUT/$name-smoke.json" 2> "$OUT/$name-stderr.txt"
}
export -f run_history
resources() {
  "$ROOT/ops/kubectl.sh" get --raw /apis/metrics.k8s.io/v1beta1/namespaces/chronicle/pods > "$OUT/$1-resources.json"
}
failed=0
resources before
read -r group seed path < "$OUT/hot.tsv"
run_history hot "$seed" "$path" 8 4 || failed=1
resources after-hot
xargs -P4 -n3 bash -c 'run_history "many-$1" "$2" "$3" 2 1' -- < "$OUT/many.tsv" || failed=1
resources after-many
python3 "$ROOT/tests/history_stats.py" "$OUT/hot.jsonl" > "$OUT/hot-stats.json"
python3 "$ROOT/tests/history_stats.py" "$OUT"/many-[1-4].jsonl > "$OUT/many-stats.json"
for history in "$OUT/hot.jsonl" "$OUT"/many-[1-4].jsonl; do
  "$GO" run "$ROOT/../../jepsen/checker" -rust-history "$history" -rust-history-timeout 30s \
    > "${history%.jsonl}-porcupine.txt" 2>&1 || failed=1
done
gzip "$OUT/hot.jsonl" "$OUT"/many-[1-4].jsonl
exit "$failed"
