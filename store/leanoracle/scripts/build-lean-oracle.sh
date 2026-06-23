#!/usr/bin/env bash
# build-lean-oracle.sh — regenerate the vendored Lean→C differential oracle.
#
# Issue #31 (P1.2). This is the regeneration command of record for the vendored
# artifact in store/leanoracle/:
#
#   libchronicle_oracle.a   — the self-contained static archive (Lean export
#                             shims + the two proven cores + the slice of the
#                             Lean runtime they pull in)
#   chronicle_oracle.h      — the maintained C header (NOT regenerated here)
#
# It needs the pinned Lean toolchain on PATH (see lean/lean-toolchain and
# PROVENANCE.txt). Routine Go CI does NOT run this — it links the committed
# archive with no Lean toolchain present. A gated CI job runs this and asserts
# byte-identity with the committed archive (drift guard).
#
# Usage:
#   PATH="$HOME/.elan/bin:$PATH" store/leanoracle/scripts/build-lean-oracle.sh
#   # --check  : build into a temp dir and diff against the committed archive
#
# The macOS deployment target is pinned so the emitted object's build-version
# load command is stable and reproducible (otherwise leanc stamps the host SDK
# version and the archive is not byte-identical across machines).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORACLE_DIR="$(cd "$HERE/.." && pwd)"
LEAN_DIR="$ORACLE_DIR/lean"
CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

command -v lake >/dev/null || { echo "error: lake not on PATH (need the pinned Lean toolchain; see PROVENANCE.txt)"; exit 1; }
command -v lean >/dev/null || { echo "error: lean not on PATH"; exit 1; }

LEAN_PREFIX="$(lean --print-prefix)"
# Use the system clang (NOT leanc): leanc bakes -fvisibility=hidden, which demotes
# the @[export] symbols to private and breaks cgo linking. We want default
# visibility so lean_offset_compare etc. stay global through the partial link.
CLANG="${CLANG:-/usr/bin/clang}"
# Match the deployment target the toolchain's own prebuilt archives (gmp/uv/leanrt)
# were built with, so the final link emits no version-mismatch warnings and the
# build-version load command is stable for the byte-identity drift guard.
MACOS_MIN="${MACOS_MIN:-15.0}"

echo "== lake build (emits C for Chronicle.{Producer,Offset,Extern}) =="
( cd "$LEAN_DIR" && lake build )

IR="$LEAN_DIR/.lake/build/ir/Chronicle"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "== compile emitted C to objects (default visibility, pinned deployment target) =="
COMMON_CFLAGS=(-c -O3 -fvisibility=default "-mmacosx-version-min=$MACOS_MIN" "-I$LEAN_PREFIX/include")
for m in Producer Offset Extern; do
  "$CLANG" "${COMMON_CFLAGS[@]}" "$IR/$m.c" -o "$WORK/$m.o"
done

echo "== partial-link objects + referenced Lean runtime members into one object =="
# ld -r pulls only the archive members our symbols reference. -exported_symbol
# keeps the public entry points (and the runtime bring-up symbols) global; every
# other symbol is internalised. The only symbols left undefined afterwards are
# libc / libc++ / libSystem, which the final Go cgo link resolves.
ld -r -o "$WORK/oracle_exp.o" \
  -exported_symbol _lean_offset_compare \
  -exported_symbol '_lean_validate_producer_*' \
  -exported_symbol _initialize_chronicleoracle_Chronicle_Extern \
  -exported_symbol _lean_initialize_runtime_module \
  -exported_symbol _lean_io_mark_end_initialization \
  "$WORK/Producer.o" "$WORK/Offset.o" "$WORK/Extern.o" \
  "$LEAN_PREFIX/lib/lean/libleanrt.a" \
  "$LEAN_PREFIX/lib/lean/libInit.a" \
  "$LEAN_PREFIX/lib/libgmp.a" \
  "$LEAN_PREFIX/lib/libuv.a"

echo "== pack the static archive (deterministic: zeroed member timestamps) =="
# ZERO_AR_DATE makes macOS ar write a 0 mtime in each member header; without it
# the archive embeds wall-clock time and is never byte-identical across rebuilds,
# which would defeat the drift guard. The compile and partial-link steps are
# already deterministic (verified: oracle_exp.o is byte-identical across rebuilds).
rm -f "$WORK/libchronicle_oracle.a"
ZERO_AR_DATE=1 ar rcs "$WORK/libchronicle_oracle.a" "$WORK/oracle_exp.o"

if [ "$CHECK" = 1 ]; then
  echo "== drift guard: diff freshly-built archive against committed =="
  if cmp -s "$WORK/libchronicle_oracle.a" "$ORACLE_DIR/libchronicle_oracle.a"; then
    echo "OK: vendored libchronicle_oracle.a is byte-identical to a fresh build."
  else
    echo "DRIFT: vendored libchronicle_oracle.a differs from a fresh build."
    echo "  committed: $(shasum -a 256 "$ORACLE_DIR/libchronicle_oracle.a" | cut -d' ' -f1)"
    echo "  fresh:     $(shasum -a 256 "$WORK/libchronicle_oracle.a" | cut -d' ' -f1)"
    exit 1
  fi
else
  cp "$WORK/libchronicle_oracle.a" "$ORACLE_DIR/libchronicle_oracle.a"
  echo "== wrote $ORACLE_DIR/libchronicle_oracle.a =="
  shasum -a 256 "$ORACLE_DIR/libchronicle_oracle.a"
fi
