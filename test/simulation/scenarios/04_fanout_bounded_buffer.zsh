#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 4: 1-to-N Fan-out (1 Sender on Mac -> 4 Docker Receivers) with Bounded Relay Buffer
#
# Demonstrates:
#   - 1 Sender on macOS sends a 16 MiB file once (`--receivers 4`) while the relay's
#     in-memory ring buffer is bounded to 4 MiB (`--fanout-buffer 4194304`).
#   - Receiver #4 is throttled via Linux `tc` to 6 Mbit/s (~750 KiB/s, which is ABOVE
#     the 256 KiB/s kick threshold).
#   - Because Receiver #4 is above the kick threshold, the relay does NOT kick Receiver #4;
#     instead, backpressure paces the sender while keeping the relay's RSS memory strictly
#     bounded, and all 4 receivers complete with 100% matching SHA-256!
#
# Usage:
#   ./04_fanout_bounded_buffer.zsh --auto
#   ./04_fanout_bounded_buffer.zsh --interactive

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-16}"

ensure_cluster_up false

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario4_fanout.bin"
print_banner "SCENARIO 4: 1-to-4 Fan-out with 4 MiB Bounded Relay Buffer (${FILE_SIZE_MB} MiB file)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"
echo "Prepared ${FILE_SIZE_MB} MiB file: ${PAYLOAD_FILE} (Relay fanout-buffer = 4 MiB)"
echo "SHA-256: ${EXPECTED_HASH}"

# Receiver 4 is slower (6 Mbit/s = ~750 KiB/s), still above the 256 KiB/s kick threshold
set_node_tc receiver-4 6mbit 5ms

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" ]]; then
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE READY — 1-to-4 Fan-out with Bounded Buffer & Backpressure]
  - Relay A fanout-buffer: 4 MiB (file size: ${FILE_SIZE_MB} MiB -> 4x buffer size!)
  - Receivers 1, 2, 3: unthrottled
  - Receiver 4:        throttled to 6 Mbit/s (~750 KiB/s > 256 KiB/s kick threshold)

Run Sender on your Mac terminal:
  CROC_SECRET=sim-scen4-fanout ${CROC_MAC} --no-local --relay 127.0.0.1:9009 send --receivers 4 ${PAYLOAD_FILE}

Run Receivers 1..4 in separate terminals (or even run one of the receivers directly on your Mac!):
  docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-scen4-fanout croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  docker exec -it croc-sim-receiver-2 sh -c 'cd /shared/receiver-2 && CROC_SECRET=sim-scen4-fanout croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  docker exec -it croc-sim-receiver-3 sh -c 'cd /shared/receiver-3 && CROC_SECRET=sim-scen4-fanout croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  docker exec -it croc-sim-receiver-4 sh -c 'cd /shared/receiver-4 && CROC_SECRET=sim-scen4-fanout croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
--------------------------------------------------------------------------------
EOF
    exit 0
fi

SECRET="scen4-fanout-$(date +%s)"
for i in 1 2 3 4; do
    rm -f "${SHARED_DIR}/receiver-${i}/scenario4_fanout.bin" "${SHARED_DIR}/receiver-${i}/scenario4_fanout.bin.partial"*
done

echo ">>> [1/3] Starting Fan-out Sender on macOS (--receivers 4)..."
START_MS="$(now_ms)"
CROC_SECRET="$SECRET" "$CROC_MAC" --no-local --no-compress --relay "127.0.0.1:9009" send \
    --transport relay --receivers 4 --min-receivers 4 --wait-timeout 20s \
    "$PAYLOAD_FILE" >/tmp/croc_scen4_send.log 2>&1 &
SEND_PID=$!
sleep 0.5

echo ">>> [2/3] Joining 4 concurrent receivers (croc-sim-receiver-1..4)..."
typeset -a RECV_PIDS
RECV_PIDS=()
for i in 1 2 3 4; do
    docker exec -e CROC_SECRET="$SECRET" "croc-sim-receiver-${i}" sh -c \
        "cd /shared/receiver-${i} && croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009" \
        >"/tmp/croc_scen4_recv${i}.log" 2>&1 &
    RECV_PIDS+=($!)
done

# Sample Relay A RSS memory while streaming
PEAK_RSS_KB=0
while kill -0 "$SEND_PID" 2>/dev/null; do
    RSS_KB="$(docker exec croc-sim-relay-a sh -c "ps -o rss= -C croc 2>/dev/null || awk '/VmRSS/{print \$2}' /proc/1/status" 2>/dev/null | tr -dc '0-9' || echo 0)"
    if [[ -n "$RSS_KB" && "$RSS_KB" -gt "$PEAK_RSS_KB" ]]; then
        PEAK_RSS_KB="$RSS_KB"
    fi
    sleep 0.2
done

for pid in "${RECV_PIDS[@]}"; do
    wait "$pid"
done
wait "$SEND_PID"
END_MS="$(now_ms)"
ELAPSED_MS=$(( END_MS - START_MS ))

clear_all_tc

echo ">>> [3/3] All 4 receivers finished in ${ELAPSED_MS} ms (Relay A Peak RSS: $(( PEAK_RSS_KB / 1024 )) MiB)!"
for i in 1 2 3 4; do
    GOT_HASH="$(sha256_of "${SHARED_DIR}/receiver-${i}/scenario4_fanout.bin")"
    if [[ "$GOT_HASH" != "$EXPECTED_HASH" ]]; then
        echo "FAIL: Receiver #${i} SHA-256 mismatch!" >&2
        exit 1
    fi
    echo "    Receiver #${i}: SHA-256 MATCH (${GOT_HASH})"
done
echo ""
echo "--- Sender Fan-out Summary Output ---"
grep -A 6 "Fan-out Receiver Summary:" /tmp/croc_scen4_send.log || true
echo "SUCCESS: 1-to-4 Fan-out with bounded 4 MiB relay buffer completed 100%!"
