#!/usr/bin/env bash
#
# Open the built-in corium EDN-Datalog console against a running transactor.
#
#   console.sh [db] [-- extra corium console args]
#
set -euo pipefail

DB="${1:-mbrainz}"
shift || true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

exec cargo run -q -p corium-cli --bin corium -- console "$DB" "$@"
