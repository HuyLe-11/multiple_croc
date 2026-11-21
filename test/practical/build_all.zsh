#!/usr/bin/env zsh
set -euo pipefail

# Build standalone static croc binaries (CGO_ENABLED=0) for all platforms into
# test/practical/dist/ so you can easily copy them to 3 real machines
# (macOS Apple Silicon / Intel, Linux x86_64 / ARM64 / Android Termux, Windows).

SCRIPT_DIR="${0:A:h}"
REPO_ROOT="${SCRIPT_DIR:h:h}"
DIST_DIR="${SCRIPT_DIR}/dist"

mkdir -p "${DIST_DIR}"

GO_BIN="/opt/homebrew/bin/go"
if ! command -v "$GO_BIN" >/dev/null 2>&1; then
    GO_BIN="go"
fi

cd "${REPO_ROOT}"

echo "==> Building cross-platform static binaries into ${DIST_DIR} ..."

echo "  [1/6] darwin/arm64 (macOS Apple Silicon)"
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 "$GO_BIN" build -tags netgo,osusergo -o "${DIST_DIR}/croc-darwin-arm64" .

echo "  [2/6] darwin/amd64 (macOS Intel)"
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 "$GO_BIN" build -tags netgo,osusergo -o "${DIST_DIR}/croc-darwin-amd64" .

echo "  [3/6] linux/amd64 (Linux x86_64 PC/Laptop/VPS)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 "$GO_BIN" build -tags netgo,osusergo -o "${DIST_DIR}/croc-linux-amd64" .

echo "  [4/6] linux/arm64 (Linux ARM64 / Raspberry Pi / Android Termux)"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 "$GO_BIN" build -tags netgo,osusergo -o "${DIST_DIR}/croc-linux-arm64" .

echo "  [5/6] linux/amd64 relay-only (lightweight, -tags croc_no_tailcat)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 "$GO_BIN" build -tags netgo,osusergo,croc_no_tailcat -o "${DIST_DIR}/croc-relay-linux-amd64" .

echo "  [6/6] windows/amd64 (Windows 64-bit)"
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 "$GO_BIN" build -tags netgo,osusergo -o "${DIST_DIR}/croc-windows-amd64.exe" .

chmod +x "${DIST_DIR}"/croc-* 2>/dev/null || true

echo ""
echo "==> Done! Built binaries:"
ls -lh "${DIST_DIR}"
echo ""
echo "Tip: Run '${SCRIPT_DIR}/serve_binaries.zsh' on this Mac to let the other 2 machines"
echo "     download their binary over LAN in 1 command (curl -LO ...)."
