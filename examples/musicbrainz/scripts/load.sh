#!/usr/bin/env bash
#
# Create the `mbrainz` database and load the example dataset into a running
# transactor. Extra arguments pass through to `mbrainz-load`, e.g.
#
#   load.sh --transactor http://127.0.0.1:4334 --data /path/to/big.edn
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
EXAMPLE="examples/musicbrainz"

exec cargo run -q -p corium-mbrainz --bin mbrainz-load -- \
  --schema "$EXAMPLE/schema.edn" \
  --data "$EXAMPLE/data/sample.edn" \
  "$@"
