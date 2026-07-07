#!/bin/sh
#
# Seed PulseMesh with realistic presence + chat data so a fresh stack comes up
# already populated — useful for the live dashboard, screenshots, and poking at
# the read models without hand-typing commands.
#
# POSIX sh (works in the busybox `curlimages/curl` container AND in bash).
#
# Usage:
#   ./scripts/seed.sh                      # seed the default channels
#   PULSEMESH_CQ_URL=http://host:8080 ./scripts/seed.sh lobby engineering
#
# It waits for the command/query service to be healthy, then drives a scripted
# scenario per channel: members join, chat, and change presence.
set -eu

CQ="${PULSEMESH_CQ_URL:-http://localhost:8080}"
AUTH="${PULSEMESH_AUTH_SECRET:-}"

post() {
  # post '<json>'
  if [ -n "$AUTH" ]; then
    curl -sf -H 'content-type: application/json' -H "authorization: Bearer $AUTH" \
      -X POST "$CQ/api/commands" -d "$1" >/dev/null
  else
    curl -sf -H 'content-type: application/json' \
      -X POST "$CQ/api/commands" -d "$1" >/dev/null
  fi
}

wait_healthy() {
  echo "waiting for command/query at $CQ ..."
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -sf "$CQ/health" >/dev/null 2>&1; then echo "healthy"; return 0; fi
    i=$((i + 1)); sleep 2
  done
  echo "command/query never became healthy" >&2; exit 1
}

# seed_channel <channel> <space-separated users>
seed_channel() {
  ch="$1"; users="$2"
  echo "seeding #$ch with: $users"
  first=""; second=""; third=""
  for u in $users; do
    post "{\"type\":\"join-channel\",\"channel-id\":\"$ch\",\"user-id\":\"$u\"}"
    [ -z "$first" ] && first="$u" && continue
    [ -z "$second" ] && second="$u" && continue
    [ -z "$third" ] && third="$u"
  done
  post "{\"type\":\"post-message\",\"channel-id\":\"$ch\",\"user-id\":\"$first\",\"body\":\"morning all\"}"
  post "{\"type\":\"post-message\",\"channel-id\":\"$ch\",\"user-id\":\"$second\",\"body\":\"the durable queue survived the restart — replay worked\"}"
  post "{\"type\":\"post-message\",\"channel-id\":\"$ch\",\"user-id\":\"$first\",\"body\":\"nice, presence rehydrated from the log\"}"
  post "{\"type\":\"set-presence\",\"channel-id\":\"$ch\",\"user-id\":\"$second\",\"presence\":\"away\"}"
  [ -n "$third" ] && \
    post "{\"type\":\"set-presence\",\"channel-id\":\"$ch\",\"user-id\":\"$third\",\"presence\":\"busy\"}"
}

wait_healthy

if [ "$#" -eq 0 ]; then
  set -- lobby engineering random
fi

for ch in "$@"; do
  case "$ch" in
    engineering) seed_channel "$ch" "nova kai juno rex" ;;
    random)      seed_channel "$ch" "pip ash lux" ;;
    *)           seed_channel "$ch" "alice bob carol dave" ;;
  esac
done

echo
echo "seed complete. Try:"
echo "  curl -s $CQ/api/channels/lobby/presence"
echo "  open http://localhost:8090/            # live dashboard"
