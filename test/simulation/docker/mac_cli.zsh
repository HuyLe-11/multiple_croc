#!/usr/bin/env zsh
set -euo pipefail

# Helper script to run croc on the real macOS host as Relay, Sender, or Receiver,
# or open an interactive shell inside any Docker simulation container.
#
# Usage:
#   ./mac_cli.zsh relay-a                        # Run Primary Relay A natively on Mac (ports 9009-9013)
#   ./mac_cli.zsh relay-b                        # Run Secondary Relay B natively on Mac (ports 9019-9023 + WS 8080)
#   ./mac_cli.zsh send [args...]                 # Run croc send on Mac through 127.0.0.1:9009
#   ./mac_cli.zsh recv <code> [args...]          # Run croc receive on Mac through 127.0.0.1:9009
#   ./mac_cli.zsh shell <container-short-name>   # Open interactive zsh inside sender, receiver-1..4, relay-a, relay-b
#   ./mac_cli.zsh logs <relay-a|relay-b>         # Follow live logs of a Docker relay

SCRIPT_DIR="${0:A:h}"
CROC_MAC="${SCRIPT_DIR}/bin/croc-mac"
MAC_WORK_DIR="${SCRIPT_DIR}/shared/mac"
mkdir -p "${MAC_WORK_DIR}"

if [[ ! -x "$CROC_MAC" ]]; then
    echo "croc-mac binary not found; building now..."
    "${SCRIPT_DIR}/build_and_up.zsh" --build-only
fi

CMD="${1:-help}"
shift || true

case "$CMD" in
    relay-a)
        echo "==> Stopping Docker container croc-sim-relay-a (if running) so Mac owns ports 9009-9013..."
        docker stop croc-sim-relay-a >/dev/null 2>&1 || true
        echo "==> Starting Primary Relay A natively on macOS (ports 9009-9013, peers=127.0.0.1:9019,ws://127.0.0.1:8080/ws)..."
        exec "$CROC_MAC" relay \
            --host 0.0.0.0 \
            --ports "9009,9010,9011,9012,9013" \
            --peers "127.0.0.1:9019,ws://127.0.0.1:8080/ws" \
            --fanout-buffer 4194304 \
            --fanout-max-memory 67108864 \
            --fanout-max-receivers 8 \
            --fanout-min-rate 262144 \
            --fanout-rate-window 3s \
            --fanout-stall-timeout 8s \
            "$@"
        ;;
    relay-b)
        echo "==> Stopping Docker container croc-sim-relay-b (if running) so Mac owns ports 9019-9023 & 8080..."
        docker stop croc-sim-relay-b >/dev/null 2>&1 || true
        echo "==> Starting Secondary Relay B natively on macOS (ports 9019-9023, ws-listen=0.0.0.0:8080)..."
        exec "$CROC_MAC" relay \
            --host 0.0.0.0 \
            --ports "9019,9020,9021,9022,9023" \
            --ws-listen "0.0.0.0:8080" \
            --peers "127.0.0.1:9009" \
            --fanout-buffer 4194304 \
            --fanout-max-memory 67108864 \
            --fanout-max-receivers 8 \
            --fanout-min-rate 262144 \
            --fanout-rate-window 3s \
            --fanout-stall-timeout 8s \
            "$@"
        ;;
    send)
        typeset -a EXTRA_ARGS
        EXTRA_ARGS=()
        while [[ $# -gt 0 ]]; do
            if [[ "$1" == "--code" || "$1" == "-c" ]]; then
                export CROC_SECRET="$2"
                shift 2
            else
                EXTRA_ARGS+=("$1")
                shift
            fi
        done
        echo "==> Running croc send on macOS (Relay: 127.0.0.1:9009)..."
        exec "$CROC_MAC" --no-local --relay "127.0.0.1:9009" send --transport relay "${EXTRA_ARGS[@]}"
        ;;
    recv)
        if [[ $# -lt 1 ]]; then
            echo "Usage: $0 recv <code-phrase> [extra global flags e.g. --fanout]" >&2
            exit 1
        fi
        CODE="$1"
        shift
        cd "${MAC_WORK_DIR}"
        echo "==> Receiving into ${MAC_WORK_DIR} on macOS (Relay: 127.0.0.1:9009, Code: ${CODE})..."
        CROC_SECRET="$CODE" exec "$CROC_MAC" --no-local --yes --overwrite --relay "127.0.0.1:9009" "$@"
        ;;
    shell)
        NODE="${1:-receiver-1}"
        echo "==> Opening interactive zsh shell inside croc-sim-${NODE}..."
        exec docker exec -it "croc-sim-${NODE}" zsh
        ;;
    logs)
        NODE="${1:-relay-a}"
        exec docker logs -f "croc-sim-${NODE}"
        ;;
    *)
        cat <<EOF
Usage:
  $0 relay-a [extra relay flags]        Run Primary Relay A natively on your Mac terminal
  $0 relay-b [extra relay flags]        Run Secondary Relay B natively on your Mac terminal
  $0 send [--dual-relay] [--receivers N] <file>
                                        Send a file directly from your Mac terminal
  $0 recv <code-phrase> [--fanout]      Receive a file directly on your Mac terminal (saved in shared/mac/)
  $0 shell <receiver-1|sender|relay-a>  Enter interactive zsh shell inside a Docker container
  $0 logs <relay-a|relay-b>             Tail live logs of a Docker relay container
EOF
        ;;
esac
