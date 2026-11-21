#!/usr/bin/env zsh
set -euo pipefail

# SCENARIO 5: 1-to-N Fan-out Slow-Receiver Kick & 1-on-1 Partial Resume (.partial + .crocmap)
#
# Demonstrates:
#   - 1 Sender on macOS -> 3 Receivers in Docker (file: 12 MiB, relay fanout-buffer: 4 MiB,
#     minimum speed threshold: 256 KiB/s over 3s window).
#   - Receivers #1 and #2 are fast; Receiver #3 is throttled via Linux `tc` to 600 kbit/s
#     (~75 KiB/s < 256 KiB/s threshold!).
#   - Relay A detects Receiver #3 is holding back the buffer below 256 KiB/s and KICKS
#     Receiver #3!
#   - Receivers #1 and #2 immediately finish at full speed with 100% matching SHA-256.
#   - Kicked Receiver #3 prints the kick reason + completion percentage and preserves
#     `scenario5_kick.bin.partial` and `scenario5_kick.bin.partial.crocmap` on disk!
#   - Next, we clear the `tc` bottleneck on Receiver #3 and run a standard 1-on-1 `croc`
#     session to resume ONLY the remaining missing chunks and verify 100% SHA-256!
#
# Usage:
#   ./05_fanout_kick_and_resume.zsh --auto
#   ./05_fanout_kick_and_resume.zsh --interactive

source "${0:A:h}/common.zsh"

MODE="${1:---auto}"
FILE_SIZE_MB="${FILE_SIZE_MB:-12}"

ensure_cluster_up false

PAYLOAD_FILE="${SHARED_DIR}/sender/scenario5_kick.bin"
print_banner "SCENARIO 5: Fan-out Slow-Receiver Kick + .partial/.crocmap 1-on-1 Resume (${FILE_SIZE_MB} MiB)"
EXPECTED_HASH="$(generate_test_file "$PAYLOAD_FILE" "$FILE_SIZE_MB")"
echo "Prepared ${FILE_SIZE_MB} MiB file: ${PAYLOAD_FILE}"
echo "SHA-256: ${EXPECTED_HASH}"

# Throttle Receiver #3 to 600 kbit/s (~75 KiB/s), well below Relay A's 256 KiB/s kick threshold!
set_node_tc receiver-3 600kbit 10ms

if [[ "$MODE" == "--interactive" || "$MODE" == "-i" ]]; then
    cat <<EOF

--------------------------------------------------------------------------------
[INTERACTIVE MODE READY — Slow-Receiver Kick & 1-on-1 Partial Resume]
  - Relay A kick threshold: 256 KiB/s (3s sliding window)
  - Receivers 1 & 2: Fast (unthrottled)
  - Receiver 3:      Throttled via Linux tc to 600 kbit/s (~75 KiB/s < 256 KiB/s -> WILL BE KICKED!)

Step 1 — Start Fan-out Transfer (1 Sender -> 3 Receivers):
  [Mac Terminal 1 — Sender]
    CROC_SECRET=sim-scen5-kick ${CROC_MAC} --no-local --relay 127.0.0.1:9009 send --receivers 3 ${PAYLOAD_FILE}
  [Terminal 2 — Fast Receiver 1]
    docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=sim-scen5-kick croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  [Terminal 3 — Fast Receiver 2]
    docker exec -it croc-sim-receiver-2 sh -c 'cd /shared/receiver-2 && CROC_SECRET=sim-scen5-kick croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  [Terminal 4 — Slow Receiver 3 (watch it get kicked and save .partial + .crocmap!)]
    docker exec -it croc-sim-receiver-3 sh -c 'cd /shared/receiver-3 && CROC_SECRET=sim-scen5-kick croc --no-local --no-update-check --yes --overwrite --fanout --sim-recv-rate 76800 --relay 127.0.0.1:9009'

Step 2 — Clear tc throttle & Resume Receiver 3 via standard 1-on-1 croc session:
  docker exec croc-sim-receiver-3 /usr/local/bin/tc_shape.sh clear
  [Mac Terminal 1 — 1-on-1 Sender]
    CROC_SECRET=sim-scen5-resume ${CROC_MAC} --no-local --no-update-check --relay 127.0.0.1:9009 send ${PAYLOAD_FILE}
  [Terminal 4 — Receiver 3 resumes missing chunks from .partial + .crocmap!]
    docker exec -it croc-sim-receiver-3 sh -c 'cd /shared/receiver-3 && CROC_SECRET=sim-scen5-resume croc --no-local --no-update-check --yes --overwrite --relay 127.0.0.1:9009'
--------------------------------------------------------------------------------
EOF
    exit 0
fi

SECRET="scen5-kick-$(date +%s)"
for i in 1 2 3; do
    rm -f "${SHARED_DIR}/receiver-${i}/scenario5_kick.bin" "${SHARED_DIR}/receiver-${i}/scenario5_kick.bin.partial"*
done

echo ">>> [1/4] Starting 1-to-3 Fan-out Sender on macOS..."
CROC_SECRET="$SECRET" "$CROC_MAC" --no-local --no-update-check --no-compress --relay "127.0.0.1:9009" send \
    --transport relay --receivers 3 --min-receivers 3 --wait-timeout 20s \
    "$PAYLOAD_FILE" >/tmp/croc_scen5_send.log 2>&1 &
SEND_PID=$!
sleep 0.4

echo ">>> [2/4] Starting Receivers #1 (fast), #2 (fast), and #3 (throttled to 75 KiB/s < 256 KiB/s kick threshold)..."
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-1 sh -c \
    'cd /shared/receiver-1 && croc --no-local --no-update-check --yes --overwrite --fanout --relay 127.0.0.1:9009' >/tmp/croc_scen5_recv1.log 2>&1 &
P1=$!
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-2 sh -c \
    'cd /shared/receiver-2 && croc --no-local --no-update-check --yes --overwrite --fanout --relay 127.0.0.1:9009' >/tmp/croc_scen5_recv2.log 2>&1 &
P2=$!
set +e
docker exec -e CROC_SECRET="$SECRET" croc-sim-receiver-3 sh -c \
    'cd /shared/receiver-3 && croc --no-local --no-update-check --yes --overwrite --fanout --sim-recv-rate 76800 --relay 127.0.0.1:9009' >/tmp/croc_scen5_recv3.log 2>&1
R3_EXIT=$?
set -e

wait "$P1"
wait "$P2"
wait "$SEND_PID"

echo "    Fast Receiver #1 & #2 completed! Verifying their SHA-256..."
for i in 1 2; do
    GOT_HASH="$(sha256_of "${SHARED_DIR}/receiver-${i}/scenario5_kick.bin")"
    if [[ "$GOT_HASH" != "$EXPECTED_HASH" ]]; then
        echo "FAIL: Fast Receiver #${i} hash mismatch!" >&2
        exit 1
    fi
    echo "    Receiver #${i}: SHA-256 MATCH"
done

echo ""
echo ">>> [3/4] Inspecting Kicked Receiver #3 status and preserved .partial + .crocmap files..."
echo "    Receiver #3 exit code: ${R3_EXIT} (expected non-zero due to relay kick)"
cat /tmp/croc_scen5_recv3.log || true
if [[ ! -f "${SHARED_DIR}/receiver-3/scenario5_kick.bin.partial" || ! -f "${SHARED_DIR}/receiver-3/scenario5_kick.bin.partial.crocmap" ]]; then
    echo "FAIL: Receiver #3 did not preserve .partial and .partial.crocmap!" >&2
    exit 1
fi
ls -lh "${SHARED_DIR}/receiver-3/scenario5_kick.bin.partial"*

echo ""
echo ">>> [4/4] Clearing tc throttle on Receiver #3 and running 1-on-1 croc resume session..."
clear_all_tc
RESUME_SECRET="scen5-resume-$(date +%s)"
CROC_SECRET="$RESUME_SECRET" "$CROC_MAC" --no-local --no-compress --relay "127.0.0.1:9009" send \
    --transport relay "$PAYLOAD_FILE" >/tmp/croc_scen5_resume_send.log 2>&1 &
RESUME_SEND_PID=$!
sleep 0.5

docker exec -e CROC_SECRET="$RESUME_SECRET" croc-sim-receiver-3 sh -c \
    'cd /shared/receiver-3 && croc --no-local --yes --overwrite --relay 127.0.0.1:9009' >/tmp/croc_scen5_resume_recv.log 2>&1
wait "$RESUME_SEND_PID"

RESUMED_HASH="$(sha256_of "${SHARED_DIR}/receiver-3/scenario5_kick.bin")"
echo "    Receiver #3 Resumed File SHA-256: ${RESUMED_HASH}"
if [[ "$RESUMED_HASH" != "$EXPECTED_HASH" ]]; then
    echo "FAIL: Resumed file SHA-256 mismatch!" >&2
    exit 1
fi
echo "SUCCESS: Slow Receiver #3 was kicked cleanly, preserved .partial + .crocmap, and resumed 1-on-1 to 100% SHA-256!"
