#!/usr/bin/env bash
#
# Build the standalone static dashboard bundle.
#
# The single source of truth for the dashboard is
#   erlang-fabric/priv/static/index.html
# which the fabric serves at GET / for the live stack.
#
# This script copies it into web/dist/ as a self-contained, dependency-free
# bundle suitable for static hosting (GitHub Pages, a portfolio embed, S3…).
# The page auto-detects that no live fabric is reachable and runs its built-in
# in-browser simulation, so the bundle is fully interactive with no backend.
#
# Usage:  ./scripts/build-web.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/erlang-fabric/priv/static/index.html"
OUT="$ROOT/web/dist"

mkdir -p "$OUT"
cp "$SRC" "$OUT/index.html"

bytes=$(wc -c < "$OUT/index.html" | tr -d ' ')
echo "built static dashboard bundle -> $OUT/index.html (${bytes} bytes, zero external deps)"
echo "open it directly (file://) or serve the dir; it runs in demo mode without a backend."
