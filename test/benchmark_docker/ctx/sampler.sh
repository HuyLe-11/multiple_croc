#!/bin/sh
# In-container metric sampler. Stops when /tmp/sampler.stop exists.
#   sampler.sh recv <file> <out>   received bytes (allocated blocks) every 250 ms
#   sampler.sh proc <out>          croc RSS/CPU, eth0 tx/rx, socket memory every 500 ms
#   sampler.sh tcp <out>           per-connection TCP rtt/retrans (ss -ti) every 1 s
STOP=/tmp/sampler.stop
MODE="$1"; shift
case "$MODE" in
recv)
    F="$1"; OUT="$2"
    while [ ! -e "$STOP" ]; do
        TS=$(date +%s%3N); B=0
        for X in "$F" "$F.partial"; do
            if [ -e "$X" ]; then
                V=$(stat -c '%b %B' "$X" | awk '{print $1*$2}')
                [ "$V" -gt "$B" ] && B=$V
            fi
        done
        echo "$TS $B" >> "$OUT"
        sleep 0.25
    done ;;
proc)
    OUT="$1"
    while [ ! -e "$STOP" ]; do
        TS=$(date +%s%3N)
        PID=$(pgrep -o croc- 2>/dev/null)
        RSS=0; CPU=0
        if [ -n "$PID" ] && [ -r "/proc/$PID/status" ]; then
            RSS=$(awk '/VmRSS/{print $2}' "/proc/$PID/status" 2>/dev/null)
            CPU=$(awk '{print $14+$15}' "/proc/$PID/stat" 2>/dev/null)
        fi
        TX=$(cat /sys/class/net/eth0/statistics/tx_bytes)
        RX=$(cat /sys/class/net/eth0/statistics/rx_bytes)
        SK=$(ss -tmnH 2>/dev/null | grep -o 'skmem:([^)]*)' | awk -F'[(,]' '{for(i=2;i<=NF;i++){if($i~/^r[0-9]/)r+=substr($i,2); if($i~/^t[0-9]/)t+=substr($i,2)}} END{print r+0, t+0}')
        echo "$TS ${RSS:-0} ${CPU:-0} $TX $RX $SK" >> "$OUT"
        sleep 0.5
    done ;;
tcp)
    OUT="$1"
    while [ ! -e "$STOP" ]; do
        TS=$(date +%s%3N)
        ss -tinH state established 2>/dev/null | awk -v ts="$TS" '
            /^[^ \t]/ {peer=$4; next}
            {
                rtt=""; rtr=0; bs=0
                for (i = 1; i <= NF; i++) {
                    if ($i ~ /^rtt:/) { split(substr($i, 5), a, "/"); rtt = a[1] }
                    if ($i ~ /^retrans:/) { split(substr($i, 9), b, "/"); rtr = b[2] }
                    if ($i ~ /^bytes_sent:/) { bs = substr($i, 12) }
                }
                if (rtt != "") print ts, peer, rtt, rtr + 0, bs + 0
            }' >> "$OUT"
        sleep 1
    done ;;
esac
