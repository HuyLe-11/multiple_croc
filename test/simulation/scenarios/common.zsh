#!/usr/bin/env zsh
set -euo pipefail
setopt NULL_GLOB

# Shared helper functions for all simulation scenarios in test/simulation/scenarios.

SCENARIOS_DIR="${0:A:h}"
SIM_ROOT="${SCENARIOS_DIR:h}"
DOCKER_DIR="${SIM_ROOT}/docker"
BIN_DIR="${DOCKER_DIR}/bin"
SHARED_DIR="${DOCKER_DIR}/shared"
CROC_MAC="${BIN_DIR}/croc-mac"
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.yml"

ensure_cluster_up() {
    local use_mac_relay="${1:-false}"
    if [[ "$use_mac_relay" == "true" ]]; then
        "${DOCKER_DIR}/build_and_up.zsh" --mac-relay
    elif [[ ! -x "$CROC_MAC" ]] || ! docker ps --format '{{.Names}}' | grep -q '^croc-sim-relay-a$'; then
        "${DOCKER_DIR}/build_and_up.zsh"
    else
        docker start croc-sim-relay-a >/dev/null 2>&1 || true
        docker start croc-sim-relay-b >/dev/null 2>&1 || true
    fi
    clear_all_tc
}

clear_all_tc() {
    for node in croc-sim-relay-a croc-sim-relay-b croc-sim-sender croc-sim-receiver-1 croc-sim-receiver-2 croc-sim-receiver-3 croc-sim-receiver-4; do
        if docker ps --format '{{.Names}}' | grep -q "^${node}\$"; then
            docker exec "$node" /usr/local/bin/tc_shape.sh clear >/dev/null 2>&1 || true
        fi
    done
}

set_node_tc() {
    local node="$1"
    local rate="$2"
    local delay="${3:-0ms}"
    docker exec "croc-sim-${node}" /usr/local/bin/tc_shape.sh set "$rate" "$delay"
}

generate_test_file() {
    local filepath="$1"
    local size_mb="$2"
    mkdir -p "${filepath:h}"
    dd if=/dev/urandom of="$filepath" bs=1048576 count="$size_mb" status=none
    shasum -a 256 "$filepath" | awk '{print $1}'
}

sha256_of() {
    shasum -a 256 "$1" | awk '{print $1}'
}

now_ms() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

print_banner() {
    echo ""
    echo "================================================================================"
    echo "  $1"
    echo "================================================================================"
}
