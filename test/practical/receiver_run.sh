#!/usr/bin/env bash
set -euo pipefail

# Run Receiver on a real machine (Machine #3, or Machine #2 & #3 in 1-to-N fan-out).
#
# Usage:
#   ./receiver_run.sh <RELAY_A_ADDR> <SECRET> [MODE] [SIM_RATE_BPS]
#
# Arguments:
#   RELAY_A_ADDR : e.g. 192.168.1.10:9009
#   SECRET       : shared code phrase matching Sender
#   MODE         : "normal" (for 1-1 / dual-relay) or "fanout" (for 1-to-N fan-out)
#   SIM_RATE_BPS : optional simulated rate cap in bytes/sec (e.g. 76800 to test relay kick!)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELAY_A="${1:?missing RELAY_A_ADDR (e.g. 192.168.1.10:9009)}"
SECRET="${2:?missing SECRET}"
MODE="${3:-normal}"
SIM_RATE="${4:-0}"
RELAY_PASS="${RELAY_PASS:-pass123}"

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
    echo "ERROR: croc binary not found." >&2
    exit 1
}

CROC_BIN="$(pick_croc_bin)"

FLAGS=(--no-local --no-update-check --yes --overwrite --pass "$RELAY_PASS" --relay "$RELAY_A")
if [[ "$MODE" == "fanout" ]]; then
    FLAGS+=(--fanout)
fi
if [[ "$SIM_RATE" != "0" ]]; then
    FLAGS+=(--sim-recv-rate "$SIM_RATE")
fi

echo "==> Joining transfer on ${RELAY_A} (mode=${MODE}, sim-rate=${SIM_RATE} B/s)..."
export CROC_SECRET="$SECRET"
"$CROC_BIN" "${FLAGS[@]}"

echo ""
echo "==> Done! Verify SHA-256 of the received file with: shasum -a 256 <filename> (macOS) or sha256sum <filename> (Linux)"
