#!/usr/bin/env bash
set -euo pipefail

# Run Secondary Relay B on Machine #2 (macOS or Linux).
# Supports 2 modes:
#   1. Direct LAN / Public VPS TCP + WebSocket mode:
#        ./machine2_relay_b.sh direct [RELAY_A_ADDRESS] [RELAY_PASS]
#   2. Cloudflare Quick Tunnel Mode (b) (exposes Relay B behind NAT via wss://...trycloudflare.com/ws):
#        ./machine2_relay_b.sh cloudflare [RELAY_A_ADDRESS] [RELAY_PASS]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-direct}"
PEERS="${2:-}"
RELAY_PASS="${3:-pass123}"

pick_croc_bin() {
    if command -v ./croc >/dev/null 2>&1; then
        echo "./croc"
        return
    fi
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64) arch="amd64" ;;
        arm64|aarch64) arch="arm64" ;;
    esac
    local candidate="${SCRIPT_DIR}/dist/croc-${os}-${arch}"
    if [[ -x "$candidate" ]]; then
        echo "$candidate"
        return
    fi
    echo "ERROR: croc binary not found. Run build_all.zsh or place ./croc in current dir." >&2
    exit 1
}

CROC_BIN="$(pick_croc_bin)"

ARGS=(
    relay
    --host 0.0.0.0
    --ports "9019,9020,9021,9022,9023"
    --ws-listen "0.0.0.0:8080"
    --pass "$RELAY_PASS"
    --fanout-buffer 16777216
    --fanout-max-memory 268435456
    --fanout-max-receivers 8
    --fanout-min-rate 262144
    --fanout-rate-window 10s
    --fanout-stall-timeout 15s
)

if [[ -n "$PEERS" ]]; then
    ARGS+=(--peers "$PEERS")
fi

if [[ "$MODE" == "cloudflare" ]]; then
    if ! command -v cloudflared >/dev/null 2>&1; then
        echo "ERROR: 'cloudflared' is required for cloudflare mode." >&2
        exit 1
    fi
    echo "==> Starting Secondary Relay B + WebSocket bridge on 0.0.0.0:8080..."
    "$CROC_BIN" "${ARGS[@]}" >/tmp/croc_relay_b.log 2>&1 &
    RELAY_PID=$!
    CF_LOG="/tmp/croc_relay_b_cloudflared.log"
    rm -f "$CF_LOG"
    cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8080 >"$CF_LOG" 2>&1 &
    CF_PID=$!
    trap 'kill $RELAY_PID $CF_PID 2>/dev/null || true' EXIT

    CF_HTTPS_URL=""
    for _ in $(seq 1 30); do
        if grep -Eo 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$CF_LOG" >/dev/null 2>&1; then
            CF_HTTPS_URL="$(grep -Eo 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$CF_LOG" | head -n 1)"
            break
        fi
        sleep 1
    done
    if [[ -z "$CF_HTTPS_URL" ]]; then
        echo "ERROR: Failed to obtain Cloudflare Quick Tunnel URL. See $CF_LOG" >&2
        exit 1
    fi
    CF_WSS_URL="${CF_HTTPS_URL/https:\/\//wss://}/ws"
    echo ""
    echo "================================================================================"
    echo "  SECONDARY RELAY B READY VIA CLOUDFLARE QUICK TUNNEL"
    echo "  Pass this address to Machine #1 (Relay A --peers) or Sender (--relay2):"
    echo "    ${CF_WSS_URL}"
    echo "================================================================================"
    echo "Press Ctrl+C to stop Relay B and Cloudflare Tunnel."
    wait "$RELAY_PID"
else
    echo "==> Starting Secondary Relay B (TCP ports 9019..9023, WS bridge 0.0.0.0:8080)..."
    exec "$CROC_BIN" "${ARGS[@]}"
fi
