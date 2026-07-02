#!/usr/bin/env bash
# spec-version-check.sh — guard SPEC_VERSION.md against drift (issue #98).
#
# Two checks:
#   (1) BLOCKING — internal consistency: the conformance-suite version recorded
#       in SPEC_VERSION.md must equal the pin in test/conformance/package.json.
#       These two are edited by hand in separate files, so they drift silently;
#       this fails CI the moment they disagree.
#   (2) INFORMATIONAL (never fails the build) — upstream drift: warn when the
#       upstream durable-streams PROTOCOL.md at `main` differs from the file at
#       the pinned commit, i.e. upstream has moved since the vendor. This is a
#       nudge to re-run the sync audit (docs/audits/), not a failure — upstream
#       advancing must never redden chronicle's main. Uses raw.githubusercontent
#       .com, so it needs no API token; if the network is unreachable it is
#       skipped, still non-fatal.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "::error::$*" >&2; exit 1; }

[ -f SPEC_VERSION.md ] || fail "SPEC_VERSION.md not found at repo root"
[ -f test/conformance/package.json ] || fail "test/conformance/package.json not found"

# (1) Internal consistency: conformance suite version.
spec_ver=$(grep -oE 'server-conformance-tests@[0-9]+\.[0-9]+\.[0-9]+' SPEC_VERSION.md \
             | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
pkg_ver=$(grep -oE '"@durable-streams/server-conformance-tests":[[:space:]]*"[^"]+"' test/conformance/package.json \
             | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)

[ -n "$spec_ver" ] || fail "could not parse the conformance version from SPEC_VERSION.md"
[ -n "$pkg_ver" ]  || fail "could not parse the conformance version from test/conformance/package.json"

if [ "$spec_ver" != "$pkg_ver" ]; then
  fail "conformance version drift: SPEC_VERSION.md pins $spec_ver but test/conformance/package.json pins $pkg_ver — update both together."
fi
echo "OK: conformance suite version consistent ($spec_ver)"

# (2) Informational: upstream PROTOCOL.md drift since the pinned commit.
sha=$(grep -oE '[0-9a-f]{40}' SPEC_VERSION.md | head -1 || true)
if [ -z "$sha" ]; then
  echo "note: no pinned commit SHA found in SPEC_VERSION.md — skipping upstream-drift check"
  exit 0
fi

base="https://raw.githubusercontent.com/durable-streams/durable-streams"
up=$(mktemp); pinned=$(mktemp)
trap 'rm -f "$up" "$pinned"' EXIT

if curl -fsS "$base/main/PROTOCOL.md" -o "$up" && curl -fsS "$base/$sha/PROTOCOL.md" -o "$pinned"; then
  if diff -q "$pinned" "$up" >/dev/null 2>&1; then
    echo "OK: upstream PROTOCOL.md unchanged since the pinned commit ($sha)"
  else
    echo "::warning::upstream durable-streams PROTOCOL.md has changed since the pinned commit $sha — re-run the sync audit (docs/audits/) and re-vendor if warranted."
  fi
else
  echo "note: could not reach raw.githubusercontent.com — skipping the upstream-drift check (non-fatal)"
fi
