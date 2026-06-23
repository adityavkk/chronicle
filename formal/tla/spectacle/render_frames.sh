#!/usr/bin/env bash
# render_frames.sh — headless SVG filmstrips of the SubscriptionFence animation
# for the four crash windows + the rotate-vs-coalesce decision + the
# SingleHolder-breach negative control (issue #41 part B).
#
# Spectacle renders SubscriptionFence_anim.tla's AnimView LIVE in the browser
# (see spectacle/README.md). This script is the OFFLINE companion: it drives TLC
# with the AnimAlias to emit one SVG per state of a curated trace, so the
# walkthroughs are reproducible and reviewable without a browser. The committed
# frames in spectacle/frames/ are produced by this script.
#
# Needs a NEWER tla2tools (>= 1.8.0) than the #37 CI pin: the CommunityModules
# SVGSerialize override calls tlc2 APIs absent from v1.7.4. Both jars are
# downloaded to /tmp on demand and NEVER committed. From formal/tla:
#   bash spectacle/render_frames.sh
set -uo pipefail

TLA_TOOLS_VERSION="${TLA_TOOLS_VERSION:-v1.8.0}"
TLA_TOOLS_JAR="${TLA_TOOLS_JAR:-/tmp/tla2tools-${TLA_TOOLS_VERSION}.jar}"
TLA_TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_TOOLS_VERSION}/tla2tools.jar"

CM_VERSION="${CM_VERSION:-202604221529}"
CM_JAR="${CM_JAR:-/tmp/CommunityModules-deps-${CM_VERSION}.jar}"
CM_URL="https://github.com/tlaplus/CommunityModules/releases/download/${CM_VERSION}/CommunityModules-deps.jar"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLA_DIR="$(cd "${HERE}/.." && pwd)"
cd "${TLA_DIR}"

if command -v /usr/libexec/java_home >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home 2>/dev/null)"
fi

[ -f "${TLA_TOOLS_JAR}" ] || { echo ">>> downloading tla2tools ${TLA_TOOLS_VERSION} (not committed)"; curl -fsSL -o "${TLA_TOOLS_JAR}" "${TLA_TOOLS_URL}" || exit 2; }
[ -f "${CM_JAR}" ]        || { echo ">>> downloading CommunityModules ${CM_VERSION} (not committed)"; curl -fsSL -o "${CM_JAR}" "${CM_URL}" || exit 2; }
CP="${TLA_TOOLS_JAR}:${CM_JAR}"

OUT="${HERE}/frames"
rm -rf "${OUT}"; mkdir -p "${OUT}"

# render <label> <config> <out-subdir>
render() {
  local label="$1" cfg="$2" sub="$3"
  echo "=== ${label} (${cfg}) ==="
  rm -f SubscriptionFence_anim_*.svg
  java -XX:+UseParallelGC -cp "${CP}" tlc2.TLC -deadlock \
       -metadir "states/anim-${sub}-$$-${RANDOM}" -config "${cfg}" MC_Anim.tla \
       2>&1 | grep -iE "is violated|Serialize error" | head -3 || true
  mkdir -p "${OUT}/${sub}"
  local n=0
  for f in SubscriptionFence_anim_*.svg; do
    [ -e "$f" ] || continue
    mv "$f" "${OUT}/${sub}/$f"; n=$((n+1))
  done
  echo "    -> ${n} frame(s) in spectacle/frames/${sub}/"
}

render "W1 arm-before-emit"        Anim_W1.cfg        w1_arm_before_emit
render "W2 commit-then-stamp"      Anim_W2.cfg        w2_commit_then_stamp
render "W3 post-emit T4"           Anim_W3.cfg        w3_post_emit_t4
render "W4 claim-before-ack"       Anim_W4.cfg        w4_claim_before_ack
render "SingleHolder BREACH (neg)" Anim_Violation.cfg violation_double_holder

echo "===================================================================="
echo "Filmstrips written under spectacle/frames/. Open any .svg in a browser,"
echo "or load SubscriptionFence.tla in Spectacle for the live animation."
