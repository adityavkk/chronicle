#!/usr/bin/env bash
# backlog-check.sh — guard docs/BACKLOG.md against drift from the issue tracker.
#
# The contract (stated in docs/BACKLOG.md): the rows of its "## The order"
# table are exactly the open issues of adityavkk/chronicle. This fails the
# moment they diverge:
#   (1) an open issue is missing from the table (new work filed but never
#       slotted into the order), or
#   (2) a table row references an issue that is closed or gone (work finished
#       but the row not deleted — a closing PR must also delete its row).
# Like spec-version-check.sh, the networked half is skipped non-fatally when
# the GitHub API is unreachable, so offline local runs never redden the build;
# in CI the API is always reachable. The public REST API needs no token; set
# GITHUB_TOKEN to lift the unauthenticated rate limit (CI passes it).
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "::error::$*" >&2; exit 1; }

[ -f docs/BACKLOG.md ] || fail "docs/BACKLOG.md not found"

# Issue numbers from the ranked-table rows only (lines like "| 3 | [#41](..."),
# not from the prose or the triage record.
doc_issues=$(grep -oE '^\| *[0-9]+ *\| *\[#[0-9]+\]' docs/BACKLOG.md \
               | grep -oE '#[0-9]+' | tr -d '#' || true)
[ -n "$doc_issues" ] || fail "could not parse any issue rows from the ranked table in docs/BACKLOG.md"

api="https://api.github.com/repos/adityavkk/chronicle/issues?state=open&per_page=100"
auth=()
[ -n "${GITHUB_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $GITHUB_TOKEN")

body=$(mktemp)
trap 'rm -f "$body"' EXIT
if ! curl -fsS "${auth[@]}" -H "Accept: application/vnd.github+json" "$api" -o "$body"; then
  echo "note: GitHub API unreachable — skipping the backlog drift check (non-fatal offline)"
  exit 0
fi

# The issues endpoint also returns PRs; anything carrying a pull_request key is
# dropped. The set comparison happens here too, so the shell never juggles sets.
DOC_ISSUES="$doc_issues" python3 - "$body" <<'PY'
import json, os, sys

doc = set(int(n) for n in os.environ["DOC_ISSUES"].split())
open_issues = set(it["number"] for it in json.load(open(sys.argv[1]))
                  if "pull_request" not in it)

missing = sorted(open_issues - doc)   # open on GitHub, absent from the table
stale = sorted(doc - open_issues)     # in the table, but closed or gone

def refs(nums):
    return ", ".join(f"#{n}" for n in nums)

if missing:
    print(f"::error::open issue(s) not in docs/BACKLOG.md — slot them into the order: {refs(missing)}",
          file=sys.stderr)
if stale:
    print(f"::error::docs/BACKLOG.md row(s) reference closed or missing issue(s) — delete the rows: {refs(stale)}",
          file=sys.stderr)
if missing or stale:
    sys.exit(1)

print(f"OK: docs/BACKLOG.md matches the tracker ({len(doc)} open issues, all slotted)")
PY
