#!/usr/bin/env bash
set -euo pipefail

# Run Primary Relay A on Machine #1 (macOS or Linux).
#
# Usage:
#   ./machine1_relay_a.sh <RELAY_B_ADDRESS> [RELAY_PASS]
#
# Examples:
#   ./machine1_relay_a.sh 192.168.1.20:9019
#   ./machine1_relay_a.sh wss://xxxx-xxxx.trycloudflare.com/ws
#   ./machine1_relay_a.sh ""   # Single-relay or 1-to-N fan-out mode without Relay B

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PEERS="${1:-}"
RELAY_PASS="${2:-pass123}"

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
    --ports "9009,9010,9011,9012,9013"
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

echo "==> Starting Primary Relay A using: ${CROC_BIN}"
echo "    Ports: 9009..9013 | Peers: ${PEERS:-<none>}"
exec "$CROC_BIN" "${ARGS[@]}"
