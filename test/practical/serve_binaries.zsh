#!/usr/bin/env zsh
set -euo pipefail

# Start a temporary HTTP server on port 8888 so the other 2 real machines on the
# same Wi-Fi/LAN can download the compiled croc binary with a single curl command.

SCRIPT_DIR="${0:A:h}"
DIST_DIR="${SCRIPT_DIR}/dist"
PORT="${1:-8888}"

if [[ ! -f "${DIST_DIR}/croc-linux-amd64" ]]; then
    "${SCRIPT_DIR}/build_all.zsh"
fi

# Best-effort detect macOS LAN IP
LAN_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo "<IP_MAC_CUA_BAN>")"

cat <<EOF
================================================================================
  HTTP Binary Distribution Server (Port ${PORT})
================================================================================
  Serving directory: ${DIST_DIR}
  Detected Mac LAN IP: ${LAN_IP}

  On the other 2 real machines, run ONE of the following commands to download:

  - Linux x86_64 (Ubuntu / Debian / Arch):
      curl -LO http://${LAN_IP}:${PORT}/croc-linux-amd64 && chmod +x croc-linux-amd64 && mv croc-linux-amd64 croc

  - Linux ARM64 / Android Termux:
      curl -LO http://${LAN_IP}:${PORT}/croc-linux-arm64 && chmod +x croc-linux-arm64 && mv croc-linux-arm64 croc

  - macOS Apple Silicon:
      curl -LO http://${LAN_IP}:${PORT}/croc-darwin-arm64 && chmod +x croc-darwin-arm64 && mv croc-darwin-arm64 croc

  - macOS Intel:
      curl -LO http://${LAN_IP}:${PORT}/croc-darwin-amd64 && chmod +x croc-darwin-amd64 && mv croc-darwin-amd64 croc

  - Windows (PowerShell):
      Invoke-WebRequest -Uri http://${LAN_IP}:${PORT}/croc-windows-amd64.exe -OutFile croc.exe

  Press Ctrl+C to stop this HTTP server once all machines have downloaded.
================================================================================
EOF

cd "${DIST_DIR}"
exec python3 -m http.server "${PORT}" --bind 0.0.0.0
