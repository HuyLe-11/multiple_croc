#!/usr/bin/env bash
# Build the two croc versions under test and the benchmark Docker image.
#
# Usage (from the repo root or anywhere):
#   test/benchmark_docker/build.sh [OLD_REF] [NEW_REF] [PATCH]
#
# OLD_REF defaults to 58a317db (origin/main before the dual-relay fixes).
# NEW_REF defaults to 18a1b529 (fix/dual-relay-throughput); pass WORKTREE to
# build the current working tree instead. PATCH is an optional diff applied on
# top of NEW_REF (e.g. an uncommitted scheduler change).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
OLD_REF="${1:-58a317db}"
NEW_REF="${2:-18a1b529}"
PATCH="${3:-}"
WT="$HERE/.worktrees"
mkdir -p "$WT" "$HERE/ctx"

build() { # <source dir> <output>
    (cd "$1" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -tags netgo,osusergo -o "$2" .)
}

echo "==> old version: $OLD_REF"
rm -rf "$WT/old"; git -C "$REPO" worktree prune
git -C "$REPO" worktree add --detach "$WT/old" "$OLD_REF" >/dev/null
build "$WT/old" "$HERE/ctx/croc-base"

if [[ "$NEW_REF" == "WORKTREE" ]]; then
    echo "==> new version: current working tree"
    build "$REPO" "$HERE/ctx/croc-fix"
else
    echo "==> new version: $NEW_REF"
    rm -rf "$WT/new"; git -C "$REPO" worktree prune
    git -C "$REPO" worktree add --detach "$WT/new" "$NEW_REF" >/dev/null
    if [[ -n "$PATCH" ]]; then
        echo "    applying $PATCH"
        git -C "$WT/new" apply "$(cd "$(dirname "$PATCH")" && pwd)/$(basename "$PATCH")"
    fi
    build "$WT/new" "$HERE/ctx/croc-fix"
fi

cp "$REPO/test/simulation/docker/tc_shape.sh" "$HERE/ctx/tc_shape.sh"
echo "==> docker image croc-bench:1"
(cd "$HERE/ctx" && docker build -q -t croc-bench:1 .)
# Containers created from an older image must be recreated.
for n in relay-a relay-b sender r1 r2 r3 r4; do docker rm -f "cb-$n" >/dev/null 2>&1 || true; done
echo "done"
