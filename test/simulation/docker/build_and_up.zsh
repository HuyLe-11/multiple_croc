#!/usr/bin/env zsh
set -euo pipefail

# Build native macOS + Linux (ARM64 & x86_64) croc binaries and start the Docker
# simulation environment.
#
# Usage:
#   ./build_and_up.zsh               # Start full cluster (relay-a, relay-b, sender, receiver-1..4)
#   ./build_and_up.zsh --mac-relay   # Stop container relay-a so you can run Primary Relay A directly on your Mac terminal!
#   ./build_and_up.zsh --down        # Stop and remove containers
#   ./build_and_up.zsh --build-only  # Only compile binaries & Docker image

SCRIPT_DIR="${0:A:h}"
REPO_ROOT="${SCRIPT_DIR:h:h:h}"
BIN_DIR="${SCRIPT_DIR}/bin"
SHARED_DIR="${SCRIPT_DIR}/shared"

mkdir -p "${BIN_DIR}" "${SHARED_DIR}/sender" "${SHARED_DIR}/receiver-1" "${SHARED_DIR}/receiver-2" "${SHARED_DIR}/receiver-3" "${SHARED_DIR}/receiver-4" "${SHARED_DIR}/mac"

MODE="${1:-up}"

if [[ "$MODE" == "--down" || "$MODE" == "down" ]]; then
    echo "==> Stopping Docker simulation cluster..."
    docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down --remove-orphans
    exit 0
fi

GO_BIN="/opt/homebrew/bin/go"
if ! command -v "$GO_BIN" >/dev/null 2>&1; then
    GO_BIN="go"
fi

echo "==> [1/3] Checking croc binaries (Pure Go, CGO_ENABLED=0)..."
cd "${REPO_ROOT}"

NEED_BUILD="false"
if [[ ! -x "${BIN_DIR}/croc-mac" || ! -x "${BIN_DIR}/croc-linux-arm64" || ! -x "${BIN_DIR}/croc-linux-amd64" ]]; then
    NEED_BUILD="true"
elif [[ -n "$(find src -name '*.go' -newer "${BIN_DIR}/croc-mac" -print -quit 2>/dev/null)" ]]; then
    NEED_BUILD="true"
fi

if [[ "$NEED_BUILD" == "true" ]]; then
    echo "  -> Building macOS native binary: ${BIN_DIR}/croc-mac (darwin/arm64)"
    CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 "$GO_BIN" build -tags netgo,osusergo -o "${BIN_DIR}/croc-mac" .

    echo "  -> Building Linux ARM64 binary:  ${BIN_DIR}/croc-linux-arm64 (linux/arm64)"
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 "$GO_BIN" build -tags netgo,osusergo -o "${BIN_DIR}/croc-linux-arm64" .

    echo "  -> Building Linux x86_64 binary: ${BIN_DIR}/croc-linux-amd64 (linux/amd64)"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 "$GO_BIN" build -tags netgo,osusergo -o "${BIN_DIR}/croc-linux-amd64" .
else
    echo "  -> Up-to-date binaries found in ${BIN_DIR}/ (skipping Go recompile)"
fi

# Select the Linux binary matching the local Docker daemon architecture for the container image
DOCKER_ARCH="$(docker info --format '{{.Architecture}}' 2>/dev/null || uname -m)"
if [[ "$DOCKER_ARCH" == "aarch64" || "$DOCKER_ARCH" == "arm64" ]]; then
    cp -f "${BIN_DIR}/croc-linux-arm64" "${BIN_DIR}/croc-linux"
else
    cp -f "${BIN_DIR}/croc-linux-amd64" "${BIN_DIR}/croc-linux"
fi
chmod +x "${BIN_DIR}/croc-mac" "${BIN_DIR}/croc-linux" "${SCRIPT_DIR}/entrypoint.sh" "${SCRIPT_DIR}/tc_shape.sh"

echo "==> [2/3] Building lightweight Alpine Docker image (with iproute2/tc & socat)..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" build

if [[ "$MODE" == "--build-only" ]]; then
    echo "==> Build complete (--build-only)."
    exit 0
fi

echo "==> [3/3] Starting Docker simulation cluster..."
if [[ "$MODE" == "--mac-relay" ]]; then
    RELAY_A_HOST="host.docker.internal" docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d
    echo "==> Stopping container 'croc-sim-relay-a' so macOS host owns ports 9009-9013..."
    docker stop croc-sim-relay-a >/dev/null
else
    RELAY_A_HOST="relay-a" docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d
fi

if [[ "$MODE" == "--mac-relay" ]]; then
    cat <<EOF

================================================================================
[MAC-RELAY MODE READY]
Container 'croc-sim-relay-a' is stopped. Ports 9009-9013 are free on your Mac!
Container 'croc-sim-relay-b' is running on 127.0.0.1:9019 (TCP) & 127.0.0.1:8080 (WS).

Run Primary Relay A directly on your Mac terminal with:
  ${SCRIPT_DIR}/mac_cli.zsh relay-a

Or run manually:
  ${BIN_DIR}/croc-mac relay --ports 9009,9010,9011,9012,9013 --peers 127.0.0.1:9019,ws://127.0.0.1:8080/ws
================================================================================
EOF
else
    cat <<EOF

================================================================================
[DOCKER SIMULATION CLUSTER READY]
Active Containers:
  - croc-sim-relay-a    (Primary Relay A:   127.0.0.1:9009..9013)
  - croc-sim-relay-b    (Secondary Relay B: 127.0.0.1:9019..9023 & ws://127.0.0.1:8080/ws)
  - croc-sim-sender     (Docker Sender node)
  - croc-sim-receiver-1 (Docker Receiver #1)
  - croc-sim-receiver-2 (Docker Receiver #2)
  - croc-sim-receiver-3 (Docker Receiver #3)
  - croc-sim-receiver-4 (Docker Receiver #4)

Your Mac can act as Sender, Receiver, or Relay directly via:
  ${BIN_DIR}/croc-mac   (or helper: ${SCRIPT_DIR}/mac_cli.zsh)
================================================================================
EOF
fi
