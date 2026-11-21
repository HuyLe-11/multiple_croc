#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 3: Mode (b) Real Cloudflare Quick Tunnel (Primary Relay A on Mac + Secondary Relay B over wss://...trycloudflare.com/ws)
#
# Architecture tested:
#   - Primary Relay A runs NATIVELY on your macOS host (ports 9009..9013).
#   - Secondary Relay B runs in Docker container croc-sim-relay-b (TCP 9019..9023 + WS 127.0.0.1:8080).
#   - `cloudflared tunnel --url http://127.0.0.1:8080` exposes Relay B's WebSocket endpoint
#     to the public Internet at `wss://<random>.trycloudflare.com/ws`.
#   - Primary Relay A advertises `--peers wss://<random>.trycloudflare.com/ws` in its banner!
#   - Sender (on Mac) and Receiver (in Docker or on Android/another machine) automatically
#     discover the Cloudflare `wss://` peer from Relay A's banner and transfer chunks in parallel
#     over BOTH local TCP (Relay A) and the live Cloudflare WebSocket Tunnel (Relay B)!
#
# Usage:
#   ./03_cloudflare_tunnel_mode_b.zsh --auto
#   ./03_cloudflare_tunnel_mode_b.zsh --interactive

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-4000}"

if ! command -v cloudflared >/dev/null 2>&1; then
    echo "ERROR: cloudflared is not installed on PATH." >&2
    exit 1
fi

# Start cluster in --mac-relay mode so Primary Relay A runs directly on macOS!
ensure_cluster_up true

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario3_cloudflare.bin"
print_banner "SCENARIO 3: Mode (b) Native Mac Relay A + Live Cloudflare Quick Tunnel Relay B (${FILE_SIZE_MB} MiB)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"

CF_LOG="/tmp/croc_cloudflared_scen3.log"
rm -f "$CF_LOG"

echo ">>> [1/4] Launching live Cloudflare Quick Tunnel -> http://127.0.0.1:8080 (Relay B WebSocket bridge)..."
cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8080 >"$CF_LOG" 2>&1 &
CF_PID=$!

MAC_RELAY_PID=""
cleanup_scen3() {
    if [[ -n "$MAC_RELAY_PID" ]]; then
        kill "$MAC_RELAY_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "${CF_PID:-}" ]]; then
        kill "$CF_PID" >/dev/null 2>&1 || true
    fi
    RELAY_A_HOST="relay-a" docker compose -f "${COMPOSE_FILE}" up -d >/dev/null 2>&1 || true
}
trap cleanup_scen3 EXIT

CF_HTTPS_URL=""
for _ in {1..30}; do
    if grep -Eo 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$CF_LOG" >/dev/null 2>&1; then
        CF_HTTPS_URL="$(grep -Eo 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$CF_LOG" | head -n 1)"
        break
    fi
    sleep 1
done

if [[ -z "$CF_HTTPS_URL" ]]; then
    echo "ERROR: Timed out waiting for trycloudflare.com URL. Log:" >&2
    cat "$CF_LOG" >&2
    exit 1
fi

CF_WSS_URL="${CF_HTTPS_URL/https:\/\//wss://}/ws"
echo "    Cloudflare Quick Tunnel active: ${CF_HTTPS_URL}"
echo "    Relay B WebSocket Peer URL:     ${CF_WSS_URL}"

echo ">>> Waiting for Cloudflare edge DNS & tunnel propagation..."
for _ in {1..20}; do
    if curl -fsS --max-time 3 "${CF_HTTPS_URL}/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo ">>> [2/4] Starting Primary Relay A NATIVELY on macOS (ports 9009..9013, advertising --peers ${CF_WSS_URL})..."
"$CROC_MAC" relay \
    --host 0.0.0.0 \
    --ports "9009,9010,9011,9012,9013" \
    --peers "${CF_WSS_URL}" >/tmp/croc_mac_relay_a.log 2>&1 &
MAC_RELAY_PID=$!
sleep 0.6

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" ]]; then
    trap - EXIT
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE (b) READY — Native Mac Relay A + Live Cloudflare Tunnel Relay B]
  - Native Mac Relay A (PID ${MAC_RELAY_PID}): 127.0.0.1:9009 (log: /tmp/croc_mac_relay_a.log)
  - Cloudflare Tunnel (PID ${CF_PID}):         ${CF_WSS_URL}

Run Sender on your Mac terminal:
  CROC_SECRET=sim-cf-mode-b ${CROC_MAC} --no-local --relay 127.0.0.1:9009 send --dual-relay ${PAYLOAD_FILE}

Run Receiver in Docker (or on another device):
  docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-cf-mode-b croc --no-local --yes --overwrite --relay 127.0.0.1:9009'

When finished, stop background processes with:
  kill ${MAC_RELAY_PID} ${CF_PID} && docker start croc-sim-relay-a
--------------------------------------------------------------------------------
EOF
    exit 0
fi

SECRET="scen3-cf-$(date +%s)"
rm -f "${SHARED_DIR}/receiver-1/scenario3_cloudflare.bin"

echo ">>> [3/4] Sending ${FILE_SIZE_MB} MiB file from macOS with --dual-relay (Relay A = Mac TCP, Relay B = ${CF_WSS_URL})..."
START_MS="$(now_ms)"
CROC_SECRET="$SECRET" "$CROC_MAC" --no-local --no-compress --relay "127.0.0.1:9009" send \
    --transport relay --dual-relay --chunk-policy dynamic "$PAYLOAD_FILE" >/tmp/croc_scen3_send.log 2>&1 &
SEND_PID=$!
sleep 0.5

docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-1 sh -c \
    'cd /shared/receiver-1 && croc --no-local --yes --overwrite --relay 127.0.0.1:9009' >/tmp/croc_scen3_recv.log 2>&1

wait "$SEND_PID"
END_MS="$(now_ms)"
ELAPSED_MS=$(( END_MS - START_MS ))

GOT_HASH="$(sha256_of "${SHARED_DIR}/receiver-1/scenario3_cloudflare.bin")"
echo ">>> [4/4] Transfer completed in ${ELAPSED_MS} ms!"
echo "    Expected SHA-256: ${EXPECTED_HASH}"
echo "    Actual SHA-256:   ${GOT_HASH}"

if [[ "$GOT_HASH" != "$EXPECTED_HASH" ]]; then
    echo "FAIL: SHA-256 mismatch in Mode (b) Cloudflare Tunnel test!" >&2
    exit 1
fi
echo "SUCCESS: Mode (b) Dual-Relay (Mac Native Relay A + Cloudflare Free Tunnel ${CF_WSS_URL}) verified!"
