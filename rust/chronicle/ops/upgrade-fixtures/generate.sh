#!/usr/bin/env bash
# No daemon or network transport is started: only the baseline storage actor runs.
set -euo pipefail
here=$(cd -- "$(dirname -- "$0")" && pwd)
baseline=${1:-/home/user/workspace/repo/rust/chronicle}
out=${2:-"$here/../../tests/fixtures/openraft09"}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/src" "$tmp/db" "$out"
cp "$here/main.rs" "$tmp/src/main.rs"
cat > "$tmp/Cargo.toml" <<EOF
[package]
name = "chronicle-upgrade-fixture-generator"
version = "0.0.0"
edition = "2024"
[dependencies]
chronicle-raft = { path = "$baseline" }
openraft = { version = "=0.9.25", features = ["serde", "storage-v2"] }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
[patch.crates-io]
openraft = { path = "$baseline/vendor/openraft" }
EOF
# Carry the exact baseline resolution into the separate root. Cargo adds only
# this generator package; the source patch must be repeated at the new root.
cp "$baseline/Cargo.lock" "$tmp/Cargo.lock"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
cargo run --offline --manifest-path "$tmp/Cargo.toml" -- "$tmp/db"
# A separate root may add its own package but must not silently resolve a
# different dependency version/source/checksum from the baseline lock.
python3 - "$baseline/Cargo.lock" "$tmp/Cargo.lock" <<'PY'
import sys, tomllib
def packages(path):
    with open(path, 'rb') as f:
        return {(p['name'], p['version'], p.get('source'), p.get('checksum'))
                for p in tomllib.load(f)['package']}
extra = packages(sys.argv[2]) - packages(sys.argv[1])
assert extra == {('chronicle-upgrade-fixture-generator', '0.0.0', None, None)}, extra
PY
python3 - "$tmp/db" "$out" <<'PY'
import pathlib, sqlite3, sys
for db in sorted(pathlib.Path(sys.argv[1]).glob('*.sqlite')):
    with sqlite3.connect(db) as connection:
        pathlib.Path(sys.argv[2], db.stem + '.sql').write_text('\n'.join(connection.iterdump()) + '\n')
PY
cp "$tmp/Cargo.toml" "$out/generator.Cargo.toml.txt"
cp "$tmp/Cargo.lock" "$out/generator.Cargo.lock.txt"
{
  printf 'baseline HEAD: '; git -C "$baseline" rev-parse HEAD
  printf '\nBaseline source SHA256 (relative to baseline root):\n'
  (cd "$baseline"; find src vendor/openraft -type f -not -path '*/target/*' -print0 | sort -z | xargs -0 sha256sum; sha256sum Cargo.toml Cargo.lock)
  printf '\nGenerator SHA256:\n'
  (cd "$here"; sha256sum main.rs generate.sh)
  printf '\nOutput SHA256:\n'
  (cd "$out"; sha256sum ./*.sql generator.Cargo.*.txt)
} > "$out/provenance.txt"
