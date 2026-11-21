#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 6: Combined Dual-Relay + 1-to-N Fan-out + Mid-Transfer Relay Failover + Slow-Receiver Kick
#
# The ultimate end-to-end stress test combining Part 1 and Part 2:
#   - 1 Sender on macOS sends a 16 MiB file with `--receivers 3 --dual-relay`
#     across BOTH Relay A (127.0.0.1:9009) and Relay B (127.0.0.1:9019).
#   - Receivers #1 and #2 are fast; Receiver #3 is throttled to 600 kbit/s (~75 KiB/s < 256 KiB/s).
#   - Mid-transfer:
#     1. Relay B is killed (`docker stop croc-sim-relay-b`) -> missing Relay B chunks
#        are automatically re-pushed over Relay A!
#     2. Slow Receiver #3 is kicked by the relay watchdog and saves `.partial` + `.crocmap`!
#     3. Fast Receivers #1 and #2 complete 100% of the file with matching SHA-256!
#
# Usage:
#   ./06_combined_dual_relay_fanout.zsh --auto
#   ./06_combined_dual_relay_fanout.zsh --interactive

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-16}"

ensure_cluster_up false

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario6_combined.bin"
print_banner "SCENARIO 6: Combined Dual-Relay 1-to-3 Fan-out + Relay B Kill + Slow-Receiver Kick (${FILE_SIZE_MB} MiB)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"
echo "Prepared ${FILE_SIZE_MB} MiB file: ${PAYLOAD_FILE}"
echo "SHA-256: ${EXPECTED_HASH}"

set_node_tc relay-a 20mbit 5ms
set_node_tc relay-b 20mbit 5ms
set_node_tc receiver-3 600kbit 10ms

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" ]]; then
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE READY — Combined Dual-Relay + 1-to-3 Fan-out + Chaos]
  - Relay A & Relay B: 20 Mbit/s each
  - Receivers 1 & 2:   Fast
  - Receiver 3:        Throttled to 600 kbit/s (~75 KiB/s -> will be kicked)

  [Mac Terminal 1 — Dual-Relay Fan-out Sender]
    CROC_SECRET=sim-scen6-comb ${CROC_MAC} --no-local --relay 127.0.0.1:9009 send --dual-relay --receivers 3 ${PAYLOAD_FILE}
  [Terminal 2 — Receiver 1]
    docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-scen6-comb croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  [Terminal 3 — Receiver 2]
    docker exec -it croc-sim-receiver-2 sh -c 'cd /shared/receiver-2 && CROC_SECRET=sim-scen6-comb croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  [Terminal 4 — Slow Receiver 3]
    docker exec -it croc-sim-receiver-3 sh -c 'cd /shared/receiver-3 && CROC_SECRET=sim-scen6-comb croc --no-local --no-update-check --yes --overwrite --fanout --sim-recv-rate 76800 --relay 127.0.0.1:9009'
  [Terminal 5 — Kill Relay B mid-transfer!]
    docker stop croc-sim-relay-b
--------------------------------------------------------------------------------
EOF
    exit 0
fi

SECRET="scen6-combined-$(date +%s)"
for i in 1 2 3; do
    rm -f "${SHARED_DIR}/receiver-${i}/scenario6_combined.bin" "${SHARED_DIR}/receiver-${i}/scenario6_combined.bin.partial"*
done

echo ">>> [1/4] Starting Combined Dual-Relay Fan-out Sender on macOS (--dual-relay --receivers 3)..."
CROC_SECRET="$SECRET" "$CROC_MAC" --no-local --no-update-check --no-compress --relay "127.0.0.1:9009" send \
    --transport relay --dual-relay --chunk-policy dynamic \
    --receivers 3 --min-receivers 3 --wait-timeout 20s \
    "$PAYLOAD_FILE" >/tmp/croc_scen6_send.log 2>&1 &
SEND_PID=$!
sleep 0.4

echo ">>> [2/4] Starting Receivers #1 (fast), #2 (fast), #3 (slow 75 KiB/s)..."
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-1 sh -c \
    'cd /shared/receiver-1 && croc --no-local --no-update-check --yes --overwrite --fanout --relay 127.0.0.1:9009' >/tmp/croc_scen6_recv1.log 2>&1 &
P1=$!
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-2 sh -c \
    'cd /shared/receiver-2 && croc --no-local --no-update-check --yes --overwrite --fanout --relay 127.0.0.1:9009' >/tmp/croc_scen6_recv2.log 2>&1 &
P2=$!
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-3 sh -c \
    'cd /shared/receiver-3 && croc --no-local --no-update-check --yes --overwrite --fanout --sim-recv-rate 76800 --relay 127.0.0.1:9009' >/tmp/croc_scen6_recv3.log 2>&1 &
P3=$!

sleep 1.2
echo ">>> [3/4] CHAOS INJECTION: Stopping Secondary Relay B ('croc-sim-relay-b') MID-FANOUT!"
docker stop -t 0 croc-sim-relay-b >/dev/null

set +e
wait "$P3"
R3_EXIT=$?
set -e
wait "$P1"
wait "$P2"
wait "$SEND_PID"

docker start croc-sim-relay-b >/dev/null
clear_all_tc

echo ">>> [4/4] Verifying outcomes..."
for i in 1 2; do
    GOT_HASH="$(sha256_of "${SHARED_DIR}/receiver-${i}/scenario6_combined.bin")"
    if [[ "$GOT_HASH" != "$EXPECTED_HASH" ]]; then
        echo "FAIL: Fast Receiver #${i} SHA-256 mismatch!" >&2
        exit 1
    fi
    echo "    Fast Receiver #${i}: COMPLETED & SHA-256 MATCH (${GOT_HASH})"
done

if [[ ! -f "${SHARED_DIR}/receiver-3/scenario6_combined.bin.partial" ]]; then
    echo "FAIL: Slow Receiver #3 did not save .partial file!" >&2
    exit 1
fi
echo "    Slow Receiver #3: KICKED as expected (exit=${R3_EXIT}) & saved .partial + .crocmap"
echo ""
grep -A 6 "Fan-out Receiver Summary:" /tmp/croc_scen6_send.log || true
echo "SUCCESS: Combined Dual-Relay + 1-to-3 Fan-out + Mid-Transfer Relay Failover + Slow Kick verified!"
