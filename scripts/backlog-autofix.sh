#!/usr/bin/env bash
# backlog-autofix.sh — mechanically repair the stale-row half of backlog drift.
#
# Deletes any docs/BACKLOG.md ranked-table row whose issue is no longer open
# (closed, deleted, or transferred) and renumbers the rank column so it stays
# contiguous. The other drift direction — a new issue not yet slotted — is
# deliberately NOT handled here: choosing a rank is judgment, owned by the
# slot-proposal workflow (.github/workflows/backlog-slot.yml) and ultimately
# by a human.
#
# Exits 0 both when rows were removed (file rewritten in place) and when there
# was nothing to do; the caller diffs the tree to tell them apart. Unlike
# backlog-check.sh, an unreachable GitHub API is a hard error here — a repair
# must not run half-blind.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "::error::$*" >&2; exit 1; }

[ -f docs/BACKLOG.md ] || fail "docs/BACKLOG.md not found"

api="https://api.github.com/repos/adityavkk/chronicle/issues?state=open&per_page=100"
auth=()
[ -n "${GITHUB_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $GITHUB_TOKEN")

body=$(mktemp)
trap 'rm -f "$body"' EXIT
curl -fsS "${auth[@]}" -H "Accept: application/vnd.github+json" "$api" -o "$body" \
  || fail "GitHub API unreachable — refusing to autofix blind"

OPEN_JSON="$body" python3 - <<'PY'
import json, os, re, sys

open_issues = set(it["number"] for it in json.load(open(os.environ["OPEN_JSON"]))
                  if "pull_request" not in it)

path = "docs/BACKLOG.md"
row_re = re.compile(r"^(\| *)(\d+)( *\| *\[#(\d+)\])")

out, removed, rank = [], [], 0
for line in open(path).read().splitlines(keepends=True):
    m = row_re.match(line)
    if not m:
        out.append(line)
        continue
    issue = int(m.group(4))
    if issue not in open_issues:
        removed.append(issue)
        continue
    rank += 1
    out.append(row_re.sub(lambda m: f"{m.group(1)}{rank}{m.group(3)}", line, count=1))

if not removed:
    print("OK: no stale rows — nothing to fix")
    sys.exit(0)

open(path, "w").write("".join(out))
print("removed row(s) for closed/gone issue(s): " + ", ".join(f"#{n}" for n in removed)
      + f"; renumbered {rank} remaining row(s)")
PY
