#!/usr/bin/env bash
set -euo pipefail

# Run Sender on a real machine.
#
# Usage:
#   ./sender_run.sh <RELAY_A_ADDR> <SECRET> <FILE_OR_SIZE_MB> [MODE] [POLICY] [RECEIVERS]
#
# Arguments:
#   RELAY_A_ADDR    : e.g. 192.168.1.10:9009 or 127.0.0.1:9009
#   SECRET          : shared code phrase (passed via CROC_SECRET)
#   FILE_OR_SIZE_MB : path to an existing file, OR a number like "50" to auto-generate a 50 MiB file
#   MODE            : "dual" (default), "single", or "fanout" (or "dual-fanout")
#   POLICY          : "dynamic" (default) or "parity"
#   RECEIVERS       : number of receivers for fanout modes (default: 2)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELAY_A="${1:?missing RELAY_A_ADDR (e.g. 192.168.1.10:9009)}"
SECRET="${2:?missing SECRET (e.g. demo-3-may-2026)}"
TARGET="${3:-50}"
MODE="${4:-dual}"
POLICY="${5:-dynamic}"
RECEIVERS="${6:-2}"
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

if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    PAYLOAD_FILE="/tmp/croc_practical_${TARGET}mb.bin"
    if [[ ! -f "$PAYLOAD_FILE" ]]; then
        echo "==> Generating ${TARGET} MiB random test file at ${PAYLOAD_FILE}..."
        dd if=/dev/urandom of="$PAYLOAD_FILE" bs=1048576 count="$TARGET" status=progress
    fi
else
    PAYLOAD_FILE="$TARGET"
fi

if command -v shasum >/dev/null 2>&1; then
    HASH="$(shasum -a 256 "$PAYLOAD_FILE" | awk '{print $1}')"
else
    HASH="$(sha256sum "$PAYLOAD_FILE" | awk '{print $1}')"
fi
echo "==> File: ${PAYLOAD_FILE}"
echo "==> SHA-256: ${HASH}"

SEND_FLAGS=(--transport relay --chunk-policy "$POLICY")
case "$MODE" in
    single)
        ;;
    dual)
        SEND_FLAGS+=(--dual-relay)
        ;;
    fanout)
        SEND_FLAGS+=(--receivers "$RECEIVERS" --min-receivers 1 --wait-timeout 60s)
        ;;
    dual-fanout)
        SEND_FLAGS+=(--dual-relay --receivers "$RECEIVERS" --min-receivers 1 --wait-timeout 60s)
        ;;
    *)
        echo "Unknown MODE '$MODE' (use: single, dual, fanout, dual-fanout)" >&2
        exit 1
        ;;
esac

if [[ -n "${RELAY_B:-}" ]]; then
    SEND_FLAGS+=(--relay2 "$RELAY_B")
fi

echo "==> Starting Sender (mode=${MODE}, policy=${POLICY}, relay=${RELAY_A})..."
export CROC_SECRET="$SECRET"
exec "$CROC_BIN" --no-local --no-update-check --no-compress --pass "$RELAY_PASS" --relay "$RELAY_A" send "${SEND_FLAGS[@]}" "$PAYLOAD_FILE"
