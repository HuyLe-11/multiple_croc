#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 2: Dual-Relay Mid-Transfer Failover (Kill Relay B mid-transfer)
#
# Demonstrates that when 2 relays are transmitting chunks in parallel and Secondary
# Relay B crashes/stops mid-transfer (`docker stop croc-sim-relay-b`), all unacknowledged
# chunks from Relay B are automatically requeued onto Primary Relay A and the file
# completes with 100% matching SHA-256 without dropping the session!
#
# Usage:
#   ./02_dual_relay_failover.zsh --auto
#   ./02_dual_relay_failover.zsh --interactive

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-16}"

ensure_cluster_up false

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario2_failover.bin"
print_banner "SCENARIO 2: Dual-Relay Mid-Transfer Failover (${FILE_SIZE_MB} MiB file)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"
echo "Prepared ${FILE_SIZE_MB} MiB test file: ${PAYLOAD_FILE}"
echo "SHA-256: ${EXPECTED_HASH}"

set_node_tc relay-a 20mbit 5ms
set_node_tc relay-b 20mbit 5ms

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" ]]; then
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE READY — Mid-Transfer Relay Failover]
Open 3 terminals on your Mac:

  [Terminal 1 — Sender on real Mac]
    CROC_SECRET=sim-scen2-failover ${CROC_MAC} --no-local --relay 127.0.0.1:9009 send --dual-relay --chunk-policy dynamic ${PAYLOAD_FILE}

  [Terminal 2 — Receiver in Docker container receiver-1]
    docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-scen2-failover croc --no-local --yes --overwrite --relay 127.0.0.1:9009'

  [Terminal 3 — Chaos Operator: Kill Relay B while progress bar is at ~30-50%!]
    docker stop croc-sim-relay-b
    # After the transfer completes on Relay A, restart Relay B with:
    docker start croc-sim-relay-b
--------------------------------------------------------------------------------
EOF
    exit 0
fi

SECRET="scen2-failover-$(date +%s)"
rm -f "${SHARED_DIR}/receiver-1/scenario2_failover.bin" "${SHARED_DIR}/receiver-1/scenario2_failover.bin.partial"*

echo ">>> [1/4] Starting Sender on macOS (--dual-relay --chunk-policy dynamic)..."
CROC_SECRET="$SECRET" "$CROC_MAC" --no-local --no-compress --relay "127.0.0.1:9009" send \
    --transport relay --dual-relay --chunk-policy dynamic "$PAYLOAD_FILE" >/tmp/croc_scen2_send.log 2>&1 &
SEND_PID=$!
sleep 0.4

echo ">>> [2/4] Starting Receiver in container croc-sim-receiver-1..."
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-1 sh -c \
    'cd /shared/receiver-1 && croc --no-local --yes --overwrite --relay 127.0.0.1:9009' >/tmp/croc_scen2_recv.log 2>&1 &
RECV_PID=$!

sleep 1.2
echo ">>> [3/4] CHAOS INJECTION: Stopping container 'croc-sim-relay-b' MID-TRANSFER!"
docker stop -t 0 croc-sim-relay-b >/dev/null
echo "    croc-sim-relay-b stopped! Waiting for automatic failover to Primary Relay A..."

wait "$RECV_PID"
wait "$SEND_PID"

GOT_HASH="$(sha256_of "${SHARED_DIR}/receiver-1/scenario2_failover.bin")"
echo ">>> [4/4] Transfer finished! Verifying SHA-256..."
echo "    Expected: ${EXPECTED_HASH}"
echo "    Actual:   ${GOT_HASH}"

docker start croc-sim-relay-b >/dev/null
clear_all_tc

if [[ "$GOT_HASH" != "$EXPECTED_HASH" ]]; then
    echo "FAIL: SHA-256 mismatch after Relay B failover!" >&2
    exit 1
fi
echo "SUCCESS: Zero-downtime failover from Relay B -> Relay A completed with 100% matching SHA-256!"
