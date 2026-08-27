#!/usr/bin/env bash
#
# Publish the workspace to crates.io, in a way that survives being run twice.
#
# `cargo publish --workspace` is not resumable on its own: it uploads crates in
# dependency order and waits for each to appear in the index, so a network
# stall or a rate limit partway through leaves some crates at the new version
# and the rest not. Re-running then fails on the first crate that did land —
# "crate version already exists" — and the release can never finish.
#
# Two things make a re-run work:
#
#   * Crates already live at this version are excluded before the upload. The
#     list is recomputed on every attempt, so whatever landed during a failed
#     attempt is skipped by the next one.
#   * A rejection that only says "this is already published" is not a failure.
#     It means the registry disagreed with the exclusion list — usually because
#     the API was unreachable when it was built — so the list is rebuilt and
#     the upload retried rather than failing the release.
#
# Transport failures, 5xx responses, rate limits and index-propagation timeouts
# are retried with backoff. Anything else — a bad manifest, a missing README, a
# rejected licence field — fails on the first attempt.
#
# Environment:
#   CARGO_REGISTRY_TOKEN  publish token (required by cargo, not read here)
#   MAX_ATTEMPTS          upload attempts before giving up (default 3)
#   REGISTRY_API          crates.io API base (default https://crates.io/api/v1)
#
set -euo pipefail

MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
REGISTRY_API="${REGISTRY_API:-https://crates.io/api/v1}"
USER_AGENT="${USER_AGENT:-corium-publish/1.0 (https://github.com/csm/corium)}"

log() { printf '%s %s\n' "$(date -u '+%H:%M:%S')" "$*"; }

# 0 = this exact version is on crates.io, 1 = it is not, 2 = could not tell.
# The distinction matters: a transport error is not evidence that a crate is
# unpublished, and treating it as such is what turns a blip into a release
# that can never be retried.
crate_published() {
  local name=$1 version=$2 response code body
  response=$(curl --silent --show-error --location \
    --header "User-Agent: ${USER_AGENT}" \
    --max-time 30 --retry 3 --retry-delay 5 --retry-all-errors \
    --write-out '\n%{http_code}' \
    "${REGISTRY_API}/crates/${name}/${version}") || return 2
  code=${response##*$'\n'}
  body=${response%$'\n'*}
  case "$code" in
    200) [[ "$(printf '%s' "$body" | jq -r '.version.num // empty')" == "$version" ]] ;;
    404) return 1 ;;
    *)
      log "crates.io returned HTTP ${code} for ${name} ${version}."
      return 2
      ;;
  esac
}

# Fills `exclusions` with a `--exclude <name>` pair for every workspace member
# already live at its version, and `remaining` with the count that still needs
# uploading. Rebuilt before every attempt so a partial upload is not retried.
build_exclusions() {
  local pkg name version
  exclusions=()
  remaining=0
  while IFS= read -r pkg; do
    name="${pkg%%@*}"
    version="${pkg##*@}"
    if crate_published "$name" "$version"; then
      log "Already published: ${name} ${version} — skipping."
      exclusions+=(--exclude "$name")
    else
      remaining=$((remaining + 1))
    fi
  done < <(cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[] | "\(.name)@\(.version)"')
}

# Retrying these is the whole point; retrying anything else just burns time.
TRANSIENT_PATTERN='failed to get a 200 OK response|response status: 5[0-9][0-9]|response status: 429|too many requests|timed out|timeout|connection (reset|closed|refused)|temporary failure|network|SendRequest|error sending request|Broken pipe'
# The registry disagreeing with our exclusion list — recompute and retry.
ALREADY_PATTERN='already (been )?uploaded|already exists|crate version .* is already'

classify_failure() {
  local log_file=$1
  if grep -qiE "$ALREADY_PATTERN" "$log_file"; then
    printf 'already-published'
  elif grep -qiE "$TRANSIENT_PATTERN" "$log_file"; then
    printf 'transient'
  else
    printf 'fatal'
  fi
}

log_file=$(mktemp)
trap 'rm -f "$log_file"' EXIT

backoff=30
for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
  build_exclusions

  if ((remaining == 0)); then
    log "Every workspace crate is already on crates.io — nothing to publish."
    exit 0
  fi

  log "Publishing the workspace to crates.io (attempt ${attempt}/${MAX_ATTEMPTS})."
  if cargo publish --workspace --no-verify \
    ${exclusions[@]+"${exclusions[@]}"} 2>&1 | tee "$log_file"; then
    log "crates.io accepted the workspace."
    exit 0
  fi

  case "$(classify_failure "$log_file")" in
    already-published)
      log "crates.io reports a crate as already published; rebuilding the exclusion list."
      ;;
    transient)
      log "Transient failure talking to crates.io."
      ;;
    *)
      echo "::error::The crates.io publish failed for a reason retrying will not fix." >&2
      exit 1
      ;;
  esac

  if ((attempt == MAX_ATTEMPTS)); then
    break
  fi
  log "Retrying in ${backoff}s."
  sleep "$backoff"
  backoff=$((backoff * 2))
done

echo "::error::Publishing the workspace to crates.io failed after ${MAX_ATTEMPTS} attempts." >&2
exit 1
