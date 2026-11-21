#!/usr/bin/env bash
set -euo pipefail

ROLE="${SIM_ROLE:-client}"
RELAY_A_HOST="${RELAY_A_HOST:-relay-a}"
RELAY_B_HOST="${RELAY_B_HOST:-relay-b}"

# For client containers (sender / receiver-*), forward 127.0.0.1:9009..9013 to
# RELAY_A_HOST (relay-a container by default, or host.docker.internal in --mac-relay mode)
# and 127.0.0.1:9019..9023,8080 to RELAY_B_HOST (relay-b container).
if [[ "$ROLE" == "client" ]]; then
    for port in 9009 9010 9011 9012 9013; do
        socat TCP-LISTEN:${port},bind=127.0.0.1,reuseaddr,fork TCP:${RELAY_A_HOST}:${port} >/dev/null 2>&1 &
    done
    for port in 9019 9020 9021 9022 9023 8080; do
        socat TCP-LISTEN:${port},bind=127.0.0.1,reuseaddr,fork TCP:${RELAY_B_HOST}:${port} >/dev/null 2>&1 &
    done
fi

# Apply optional initial traffic shaping if SIM_RATE_MBIT or SIM_RATE_KBIT is set.
if [[ -n "${SIM_RATE_MBIT:-}" ]]; then
    /usr/local/bin/tc_shape.sh set "${SIM_RATE_MBIT}mbit" "${SIM_DELAY_MS:-0}ms" || true
elif [[ -n "${SIM_RATE_KBIT:-}" ]]; then
    /usr/local/bin/tc_shape.sh set "${SIM_RATE_KBIT}kbit" "${SIM_DELAY_MS:-0}ms" || true
fi

exec "$@"
