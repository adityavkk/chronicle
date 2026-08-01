#!/usr/bin/env bash
# Reject links from the public repository to Walmart-hosted endpoints.
set -euo pipefail

cd "$(dirname "$0")/.."

pattern="https?://[^[:space:]<>()\"]*(gecgithub01|walmart\.(com|net)|wal-mart\.com|homeoffice|wmlink)"
hits="$(git grep -nI -i -E "$pattern" -- . ':(exclude)scripts/public-link-check.sh' || true)"

if [ -n "$hits" ]; then
  printf '%s\n' "internal endpoint links are not allowed in the public repository:" >&2
  printf '%s\n' "$hits" >&2
  exit 1
fi

echo "OK: no Walmart endpoint links in tracked public files"
