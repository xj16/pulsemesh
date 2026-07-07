#!/usr/bin/env bash
#
# End-to-end demo of PulseMesh. Assumes the stack is up:
#
#   docker compose up --build
#
# It drives a small chat/presence scenario through the Clojure command/query
# HTTP API, and (if `websocat` is installed) opens a WebSocket to the Erlang
# fabric so you can watch events fan out live.
#
# Usage:  ./scripts/demo.sh [CHANNEL]
set -euo pipefail

CQ="${PULSEMESH_CQ_URL:-http://localhost:8080}"
FABRIC_WS="${PULSEMESH_FABRIC_WS:-ws://localhost:8090}"
CHANNEL="${1:-lobby}"

say() { printf '\n\033[1;36m== %s\033[0m\n' "$1"; }

cmd() {
  # cmd '<json>'
  curl -sf -X POST "$CQ/api/commands" \
    -H 'content-type: application/json' \
    -d "$1"
  echo
}

say "Health check"
curl -sf "$CQ/health"; echo
echo "(tip: open the live dashboard at http://localhost:8090/ to watch this happen)"

# Optionally tail the fabric over WebSocket in the background.
if command -v websocat >/dev/null 2>&1; then
  say "Opening WebSocket to fabric for channel '$CHANNEL' (10s)"
  timeout 10 websocat "$FABRIC_WS/ws?channel=$CHANNEL&user=observer" &
  WS_PID=$!
  sleep 1
else
  echo "(install 'websocat' to watch live fan-out over the fabric)"
  WS_PID=""
fi

say "alice joins #$CHANNEL"
cmd "{\"type\":\"join-channel\",\"channel-id\":\"$CHANNEL\",\"user-id\":\"alice\"}"

say "bob joins #$CHANNEL"
cmd "{\"type\":\"join-channel\",\"channel-id\":\"$CHANNEL\",\"user-id\":\"bob\"}"

say "alice posts a message"
cmd "{\"type\":\"post-message\",\"channel-id\":\"$CHANNEL\",\"user-id\":\"alice\",\"body\":\"hello mesh!\"}"

say "bob goes away"
cmd "{\"type\":\"set-presence\",\"channel-id\":\"$CHANNEL\",\"user-id\":\"bob\",\"presence\":\"away\"}"

say "Presence read model"
curl -sf "$CQ/api/channels/$CHANNEL/presence"; echo

say "Recent messages read model"
curl -sf "$CQ/api/channels/$CHANNEL/messages?limit=10"; echo

say "Full event history (audit view)"
curl -sf "$CQ/api/channels/$CHANNEL/history"; echo

if [ -n "$WS_PID" ]; then
  wait "$WS_PID" 2>/dev/null || true
fi

say "Demo complete"
