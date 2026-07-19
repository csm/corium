#!/usr/bin/env bash
#
# Open the Clojurust query REPL against a running transactor. Extra arguments
# pass through to `mbrainz-repl`, e.g.
#
#   repl.sh --transactor http://127.0.0.1:4334 --db mbrainz
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

exec cargo run -q -p corium-mbrainz --bin mbrainz-repl -- "$@"
