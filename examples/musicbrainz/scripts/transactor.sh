#!/usr/bin/env bash
#
# Start a corium transactor for the MusicBrainz example over any store.
#
#   transactor.sh [mem|fs|turso] [data-dir]
#
# Environment:
#   LISTEN   transactor listen address (default 127.0.0.1:4334)
#
# `mem` keeps everything in memory for the process's lifetime (load and query
# against it while it runs; data is gone when it exits). `fs` and `turso`
# persist. `turso` also needs the `turso` cargo feature, which this script
# enables automatically.
set -euo pipefail

STORE="${1:-fs}"
DATA_DIR="${2:-./corium-mbrainz-data}"
LISTEN="${LISTEN:-127.0.0.1:4334}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

FEATURES=()
ARGS=(transactor --store "$STORE" --data-dir "$DATA_DIR" --listen "$LISTEN")
case "$STORE" in
  mem) ;;
  fs) mkdir -p "$DATA_DIR" ;;
  turso)
    mkdir -p "$DATA_DIR"
    FEATURES=(--features turso)
    ARGS+=(--turso-path "$DATA_DIR/store.db")
    ;;
  *)
    echo "usage: $(basename "$0") [mem|fs|turso] [data-dir]" >&2
    exit 1
    ;;
esac

echo "starting $STORE transactor on $LISTEN (data-dir: $DATA_DIR)" >&2
exec cargo run -q -p corium-cli ${FEATURES[@]+"${FEATURES[@]}"} --bin corium -- "${ARGS[@]}"
