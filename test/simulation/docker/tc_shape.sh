#!/usr/bin/env bash
set -euo pipefail

# Helper to configure or clear Linux Traffic Control (tc) on eth0 inside a container.
# Uses HTB (Hierarchical Token Bucket) with a 256 KiB burst so 32 KiB croc frames and
# GSO/TSO segments on Docker veth interfaces are shaped smoothly without packet drops.
#
# Usage:
#   tc_shape.sh set <rate> [delay]   (e.g., tc_shape.sh set 12mbit 5ms, or tc_shape.sh set 600kbit 10ms)
#   tc_shape.sh clear
#   tc_shape.sh show

DEV="${TC_DEV:-eth0}"
ACTION="${1:-show}"

case "$ACTION" in
    clear)
        tc qdisc del dev "$DEV" root 2>/dev/null || true
        echo "[tc] Cleared traffic shaping on $DEV"
        ;;
    set)
        RATE="${2:?missing rate (e.g. 12mbit or 600kbit)}"
        DELAY="${3:-0ms}"
        tc qdisc del dev "$DEV" root 2>/dev/null || true
        tc qdisc add dev "$DEV" root handle 1: htb default 10
        tc class add dev "$DEV" parent 1: classid 1:10 htb rate "$RATE" ceil "$RATE" burst 256k cburst 256k
        if [[ "$DELAY" != "0ms" && "$DELAY" != "0" ]]; then
            tc qdisc add dev "$DEV" parent 1:10 handle 10: netem delay "$DELAY" limit 2000
        fi
        echo "[tc] Configured $DEV (HTB): rate=$RATE delay=$DELAY"
        ;;
    show)
        tc qdisc show dev "$DEV"
        ;;
    *)
        echo "Usage: $0 {set <rate> [delay]|clear|show}" >&2
        exit 1
        ;;
esac
