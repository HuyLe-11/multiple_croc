#!/usr/bin/env zsh
set -euo pipefail

# Master runner for all 6 simulation scenarios in test/simulation/scenarios.
#
# Usage:
#   ./run_all_scenarios.zsh           # Run all scenarios automatically in sequence
#   ./run_all_scenarios.zsh --menu    # Interactive menu to pick scenario + mode (Auto vs Interactive)

SCENARIOS_DIR="${0:A:h}"

if [[ "${1:-}" == "--menu" || "${1:-}" == "-m" ]]; then
    cat <<EOF
================================================================================
  CROC MULTI-RELAY & 1-TO-N FAN-OUT SIMULATION MENU
================================================================================
  1) Scenario 1: Dual-Relay Bandwidth (Dynamic vs Parity with Linux tc)
  2) Scenario 2: Dual-Relay Mid-Transfer Failover (Kill Relay B mid-stream)
  3) Scenario 3: Mode (b) Native Mac Relay A + Live Cloudflare Quick Tunnel Relay B
  4) Scenario 4: 1-to-4 Fan-out with 4 MiB Bounded Relay Buffer (Backpressure)
  5) Scenario 5: 1-to-3 Fan-out Slow-Receiver Kick + 1-on-1 Partial Resume
  6) Scenario 6: Combined Dual-Relay + 1-to-3 Fan-out + Relay B Kill + Slow Kick
  7) Run ALL scenarios automatically
================================================================================
EOF
    printf "Select scenario [1-7]: "
    read -r choice
    if [[ "$choice" == "7" ]]; then
        exec "$0"
    fi
    printf "Select mode [a = Auto-run, i = Interactive manual CLI] (default: a): "
    read -r mode_choice
    MODE_FLAG="--auto"
    if [[ "$mode_choice" == "i" || "$mode_choice" == "I" ]]; then
        MODE_FLAG="--interactive"
    fi
    case "$choice" in
        1) exec "${SCENARIOS_DIR}/01_dual_relay_bandwidth.zsh" "$MODE_FLAG" ;;
        2) exec "${SCENARIOS_DIR}/02_dual_relay_failover.zsh" "$MODE_FLAG" ;;
        3) exec "${SCENARIOS_DIR}/03_cloudflare_tunnel_mode_b.zsh" "$MODE_FLAG" ;;
        4) exec "${SCENARIOS_DIR}/04_fanout_bounded_buffer.zsh" "$MODE_FLAG" ;;
        5) exec "${SCENARIOS_DIR}/05_fanout_kick_and_resume.zsh" "$MODE_FLAG" ;;
        6) exec "${SCENARIOS_DIR}/06_combined_dual_relay_fanout.zsh" "$MODE_FLAG" ;;
        *) echo "Invalid selection: $choice" >&2; exit 1 ;;
    esac
fi

chmod +x "${SCENARIOS_DIR}"/*.zsh "${SCENARIOS_DIR:h}/docker"/*.zsh

"${SCENARIOS_DIR}/01_dual_relay_bandwidth.zsh" --auto
"${SCENARIOS_DIR}/02_dual_relay_failover.zsh" --auto
"${SCENARIOS_DIR}/04_fanout_bounded_buffer.zsh" --auto
"${SCENARIOS_DIR}/05_fanout_kick_and_resume.zsh" --auto
"${SCENARIOS_DIR}/06_combined_dual_relay_fanout.zsh" --auto

if [[ "${SKIP_CLOUDFLARE:-0}" != "1" ]]; then
    "${SCENARIOS_DIR}/03_cloudflare_tunnel_mode_b.zsh" --auto
fi

echo ""
echo "================================================================================"
echo "  ALL SIMULATION SCENARIOS COMPLETED SUCCESSFULLY!"
echo "================================================================================"
