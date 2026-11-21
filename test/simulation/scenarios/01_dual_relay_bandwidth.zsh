#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 1: Dual-Relay Parallel Transfer (Dynamic vs Parity) with Linux tc Bandwidth Shaping
#
# Tests:
#   1A. Single Relay baseline (10 Mbit/s)
#   1B. Dual Relay Symmetric (10 Mbit/s + 10 Mbit/s) with --chunk-policy dynamic
#   1C. Dual Relay Asymmetric (24 Mbit/s + 6 Mbit/s) comparing:
#       - --chunk-policy parity  (even/odd chunk split -> bottlenecked by slow 6 Mbit/s relay)
#       - --chunk-policy dynamic (shared dynamic work queue -> utilizes full 24+6 = 30 Mbit/s)
#
# Usage:
#   ./01_dual_relay_bandwidth.zsh --auto          # Run full automated benchmark & comparison table
#   ./01_dual_relay_bandwidth.zsh --interactive   # Configure tc & print commands so you can run manually on Mac

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-1200}"

ensure_cluster_up false

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario1_payload.bin"
print_banner "SCENARIO 1: Dual-Relay Parallel Bandwidth (Dynamic vs Parity, ${FILE_SIZE_MB} MiB file)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"
echo "Prepared ${FILE_SIZE_MB} MiB test file: ${PAYLOAD_FILE}"
echo "SHA-256: ${EXPECTED_HASH}"

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" || "$MODE" == "--interactive-tc" ]]; then
    clear_all_tc
    INTERACTIVE_FILE="${SHARED_DIR}/sender/scenario1_100mb.bin"
    if [[ ! -f "$INTERACTIVE_FILE" ]]; then
        echo "Generating 100 MiB file for interactive test: ${INTERACTIVE_FILE}..."
        generate_test_file "$INTERACTIVE_FILE" 100 >/dev/null
    fi
    TC_STATUS="UNTHROTTLED (~80-100+ MB/s native local Docker speed)"
    if [[ "$MODE" == "--interactive-tc" ]]; then
        set_node_tc relay-a 24mbit 5ms
        set_node_tc relay-b 6mbit 10ms
        TC_STATUS="THROTTLED by Linux tc (Relay A = 24 Mbit/s [3.0 MB/s], Relay B = 6 Mbit/s [0.75 MB/s])"
    fi
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE READY]
  - Traffic Shaping Status: ${TC_STATUS}
  - Relay A (Self-hosted Docker): 127.0.0.1:9009 (data ports 9010..9013)
  - Relay B (Self-hosted Docker): 127.0.0.1:9019 (data ports 9020..9023)
  - Test Payload (100 MiB):       ${INTERACTIVE_FILE}

  (Tip: Run './01_dual_relay_bandwidth.zsh --interactive' for full ~85 MB/s unthrottled
   speed, or './01_dual_relay_bandwidth.zsh --interactive-tc' to cap Relay A=24mbit & Relay B=6mbit)

Open 2 terminals on your Mac:

  [Terminal 1 — Sender on real Mac (add --debug to verify every 127.0.0.1 port!)]
    CROC_SECRET=sim-scen1-secret ${CROC_MAC} --debug --no-local --no-update-check --relay 127.0.0.1:9009 send --transport relay --dual-relay --chunk-policy dynamic ${INTERACTIVE_FILE}
    # Or compare with even/odd parity mode:
    CROC_SECRET=sim-scen1-secret ${CROC_MAC} --debug --no-local --no-update-check --relay 127.0.0.1:9009 send --transport relay --dual-relay --chunk-policy parity  ${INTERACTIVE_FILE}

  [Terminal 2 — Receiver inside Docker container receiver-1]
    docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-scen1-secret croc --debug --no-local --no-update-check --yes --overwrite --relay 127.0.0.1:9009'
--------------------------------------------------------------------------------
EOF
    exit 0
fi

run_case() {
    local label="$1"
    local rate_a="$2"
    local rate_b="$3"
    local dual_flag="$4"
    local policy="$5"
    local secret="scen1-$(date +%s)-$RANDOM"

    clear_all_tc
    set_node_tc relay-a "$rate_a" 5ms
    set_node_tc relay-b "$rate_b" 5ms
    rm -f "${SHARED_DIR}/receiver-1/scenario1_payload.bin" "${SHARED_DIR}/receiver-1/scenario1_payload.bin.partial"*

    echo ""
    echo ">>> Running: ${label} (Relay A=${rate_a}, Relay B=${rate_b}, policy=${policy})"

    local start_ms="$(now_ms)"
    if [[ "$dual_flag" == "true" ]]; then
        CROC_SECRET="$secret" "$CROC_MAC" --no-local --no-update-check --no-compress --relay "127.0.0.1:9009" send \
            --transport relay --dual-relay --chunk-policy "$policy" "$PAYLOAD_FILE" >/tmp/croc_scen1_send.log 2>&1 &
    else
        CROC_SECRET="$secret" "$CROC_MAC" --no-local --no-update-check --no-compress --relay "127.0.0.1:9009" send \
            --transport relay "$PAYLOAD_FILE" >/tmp/croc_scen1_send.log 2>&1 &
    fi
    local send_pid=$!
    sleep 0.25

    docker exec -e CROC_SECRET="$secret" croc-sim-receiver-1 sh -c \
        'cd /shared/receiver-1 && croc --no-local --no-update-check --yes --overwrite --relay 127.0.0.1:9009' >/tmp/croc_scen1_recv.log 2>&1

    wait "$send_pid"
    local end_ms="$(now_ms)"
    local elapsed_ms=$(( end_ms - start_ms ))
    local got_hash="$(sha256_of "${SHARED_DIR}/receiver-1/scenario1_payload.bin")"
    if [[ "$got_hash" != "$EXPECTED_HASH" ]]; then
        echo "ERROR: SHA-256 mismatch in ${label}!" >&2
        exit 1
    fi
    local mbps="$(python3 -c "print(f'{(${FILE_SIZE_MB} * 8.0) / (${elapsed_ms} / 1000.0):.2f}')")"
    local mbytes_s="$(python3 -c "print(f'{${FILE_SIZE_MB} / (${elapsed_ms} / 1000.0):.2f}')")"
    echo "    Completed in ${elapsed_ms} ms | Throughput: ${mbytes_s} MiB/s (${mbps} Mbps) | SHA-256: OK"
    RESULTS+=("${label}|${rate_a} + ${rate_b}|${policy}|${elapsed_ms} ms|${mbps} Mbps|PASS")
}

typeset -a RESULTS
RESULTS=()

run_case "1A. Single Relay (1x12 Mbit)"       "12mbit" "12mbit" "false" "single"
run_case "1B. Dual Relay Symmetric (Dynamic)" "12mbit" "12mbit" "true"  "dynamic"
run_case "1C. Dual Relay Asymmetric (Parity)" "24mbit" "6mbit"  "true"  "parity"
run_case "1D. Dual Relay Asymmetric (Dynamic)" "24mbit" "6mbit" "true"  "dynamic"

clear_all_tc

echo ""
echo "=========================================================================================="
printf "%-36s | %-15s | %-8s | %-10s | %-11s | %-4s\n" "Test Case" "Relay A + B" "Policy" "Time" "Goodput" "Hash"
echo "------------------------------------------------------------------------------------------"
for row in "${RESULTS[@]}"; do
    IFS='|' read -r c1 c2 c3 c4 c5 c6 <<< "$row"
    printf "%-36s | %-15s | %-8s | %-10s | %-11s | %-4s\n" "$c1" "$c2" "$c3" "$c4" "$c5" "$c6"
done
echo "=========================================================================================="
