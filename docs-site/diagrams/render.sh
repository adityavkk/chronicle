#!/usr/bin/env bash
# Render every seq-*.mmd to src/diagrams/*.svg with the Chronicle theme.
#
# The CSS file is generated fresh each run: it @font-face-embeds the actual
# JetBrains Mono webfont (from @fontsource) so Chromium measures text with the
# exact metrics the deployed page displays — then the @font-face blocks are
# stripped from the output SVGs (the page already loads the webfont, so they
# would only bloat each file). The SVG keeps live <text>, so it re-renders
# crisply at any size with the page's font.
set -euo pipefail
cd "$(dirname "$0")"

FONTS=../node_modules/@fontsource/jetbrains-mono/files
OUT=../src/diagrams
mkdir -p "$OUT"

emit_face() { # weight file
  printf '@font-face{font-family:"JetBrains Mono";font-style:normal;font-weight:%s;font-display:swap;src:url(data:font/woff2;base64,%s) format("woff2");}\n' \
    "$1" "$(base64 < "$FONTS/jetbrains-mono-latin-$1-normal.woff2" | tr -d '\n')"
}

{
  emit_face 400
  emit_face 600
  emit_face 700
  cat <<'CSS'
svg { background: transparent; }
text { font-family: "JetBrains Mono", ui-monospace, Menlo, monospace; }
.messageText { fill: #100F0F; font-weight: 500; }
text.actor > tspan { fill: #100F0F; font-weight: 700; }
.actor-line { stroke: #C9C7BC; stroke-dasharray: 3 5; }
rect.actor, rect.actor-top, rect.actor-bottom { fill: #F2F0E5; stroke: #DAD8CE; }
.note { fill: #E8F1EF; stroke: #24837B; }
.noteText, .noteText > tspan { fill: #143F3C; font-weight: 500; }
.messageLine0 { stroke: #24837B; stroke-width: 1.5px; }
.messageLine1 { stroke: #24837B; stroke-width: 1.5px; }
#arrowhead path, marker path { fill: #24837B; stroke: #24837B; }
.sequenceNumber { fill: #FFFEF9; font-weight: 600; }
CSS
} > chronicle.css

shopt -s nullglob
# seq-*  → sequence diagrams (endpoint, subscription, tracer flows)
# wiki-* → wiki flowcharts (the architecture map); same theme, classDef roles.
for mmd in seq-*.mmd wiki-*.mmd; do
  name="${mmd%.mmd}"
  echo "→ $name"
  npx --yes -p @mermaid-js/mermaid-cli mmdc \
    -i "$mmd" -o "$OUT/$name.svg" \
    -c chronicle-theme.json --cssFile chronicle.css -p puppeteer.json \
    -b transparent >/dev/null 2>"$OUT/$name.log"
  # Strip the render-only @font-face blocks (the page loads the webfont).
  perl -0pi -e 's/\@font-face\{[^}]*\}//g' "$OUT/$name.svg"
done
echo "done → $OUT"
