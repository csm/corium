#!/usr/bin/env bash
#
# One-shot demo: start a transactor over the chosen store, load the example
# dataset, and drop into the Clojurust REPL. The transactor is stopped when
# the REPL exits.
#
#   demo.sh [mem|fs|turso]
#
set -euo pipefail

STORE="${1:-mem}"
LISTEN="${LISTEN:-127.0.0.1:4334}"
ENDPOINT="http://$LISTEN"

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPTS/../../.." && pwd)"
cd "$ROOT"

FEATURES=()
[ "$STORE" = "turso" ] && FEATURES=(--features turso)

DATA_DIR="$(mktemp -d)"
cleanup() {
  [ -n "${TX_PID:-}" ] && kill "$TX_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "building…"
cargo build -q -p corium-cli ${FEATURES[@]+"${FEATURES[@]}"} -p corium-mbrainz

echo "starting $STORE transactor on ${LISTEN}…"
LISTEN="$LISTEN" "$SCRIPTS/transactor.sh" "$STORE" "$DATA_DIR" &
TX_PID=$!

# Wait for the transactor to accept connections.
HOST="${LISTEN%:*}"
PORT="${LISTEN##*:}"
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; then
    exec 3>&- 3<&-
    break
  fi
  sleep 0.5
done

echo "loading example data…"
"$SCRIPTS/load.sh" --transactor "$ENDPOINT"

echo
echo "entering REPL — try the queries from :help, or :quit to exit."
"$SCRIPTS/repl.sh" --transactor "$ENDPOINT" --db mbrainz
