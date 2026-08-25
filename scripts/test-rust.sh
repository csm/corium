#!/usr/bin/env bash
set -euo pipefail

# The transaction-function runtime requires cljrs's no-gc allocator, while
# corium-cljrs and the MusicBrainz REPL require GC mode. Cargo features are
# additive, so testing both sets in one workspace command silently selects
# no-gc for all of them. Keep these invocations separate.
cargo test --workspace --exclude corium-cljrs --exclude corium-mbrainz
cargo test -p corium-cljrs -p corium-mbrainz
