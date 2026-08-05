#!/usr/bin/env bash
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXAMPLE_DIR/../.." && pwd)"

TRANSACTOR_HOST="${CORIUM_TRANSACTOR_HOST:-127.0.0.1}"
TRANSACTOR_PORT="${CORIUM_TRANSACTOR_PORT:-14335}"
POSTGRES_HOST="${CORIUM_POSTGRES_HOST:-127.0.0.1}"
POSTGRES_PORT="${CORIUM_POSTGRES_PORT:-55433}"
DATABASE="${CORIUM_DATABASE:-people}"
TRANSACTOR_ENDPOINT="http://${TRANSACTOR_HOST}:${TRANSACTOR_PORT}"

TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
if [[ "$TARGET_DIR" != /* ]]; then
  TARGET_DIR="$REPO_ROOT/$TARGET_DIR"
fi
CORIUM_BIN="$TARGET_DIR/debug/corium"

RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/corium-hibernate.XXXXXX")"
TRANSACTOR_PID=""
POSTGRES_PID=""

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  for pid in "$POSTGRES_PID" "$TRANSACTOR_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if ((status != 0)); then
    echo "Hibernate example failed; Corium process logs follow:" >&2
    for log in "$RUN_DIR"/*.log; do
      if [[ -f "$log" ]]; then
        echo "==> $log <==" >&2
        sed -n '1,240p' "$log" >&2
      fi
    done
  fi
  if [[ "${CORIUM_KEEP_RUN_DIR:-0}" == "1" ]]; then
    echo "Kept Hibernate example files in $RUN_DIR" >&2
  else
    rm -rf -- "$RUN_DIR"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$REPO_ROOT"
if [[ "${CORIUM_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p corium-cli
fi

"$CORIUM_BIN" transactor \
  --store mem \
  --data-dir "$RUN_DIR/data" \
  --listen "${TRANSACTOR_HOST}:${TRANSACTOR_PORT}" \
  >"$RUN_DIR/transactor.log" 2>&1 &
TRANSACTOR_PID=$!

for _ in {1..150}; do
  if "$CORIUM_BIN" db list --transactor "$TRANSACTOR_ENDPOINT" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$TRANSACTOR_PID" 2>/dev/null; then
    echo "Corium transactor exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.1
done

"$CORIUM_BIN" db create "$DATABASE" \
  --schema "$EXAMPLE_DIR/schema.toml" \
  --transactor "$TRANSACTOR_ENDPOINT"

"$CORIUM_BIN" postgres-server \
  --listen "${POSTGRES_HOST}:${POSTGRES_PORT}" \
  --transactor "$TRANSACTOR_ENDPOINT" \
  --database "$DATABASE" \
  --allow-writes \
  >"$RUN_DIR/postgres-server.log" 2>&1 &
POSTGRES_PID=$!

export CORIUM_JDBC_URL="jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${DATABASE}"
if command -v mvn >/dev/null 2>&1; then
  MAVEN_BIN="$(command -v mvn)"
elif [[ -x /opt/homebrew/bin/mvn ]]; then
  MAVEN_BIN=/opt/homebrew/bin/mvn
else
  echo "Maven is required to run the Hibernate example" >&2
  exit 1
fi
"$MAVEN_BIN" --quiet --file "$EXAMPLE_DIR/pom.xml" compile exec:java
