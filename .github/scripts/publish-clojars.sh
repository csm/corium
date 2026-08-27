#!/usr/bin/env bash
#
# Publish the Clojure client to Clojars, in a way that survives being run
# twice.
#
# Clojars refuses to redeploy a release version, so a run that reaches the
# deploy and then loses the connection cannot simply be re-run: the second
# attempt either succeeds against a half-uploaded version or fails with
# "redeploying non-snapshots is not allowed" forever. Unlike Maven Central,
# Clojars serves an artifact as soon as it accepts it, so its own repository is
# an accurate, immediate answer to "did this already ship" — the only care
# needed is to tell a 404 apart from a failed request, because treating a blip
# as "not published" is what produces the unrecoverable second attempt.
#
# Transport failures are retried with backoff, and the published check runs
# again before each retry so a deploy that actually landed ends the loop.
#
# Environment:
#   VERSION            version being released, e.g. 0.1.86 (required)
#   GROUP_ID           Clojars group (default dev.corium)
#   ARTIFACT_ID        Clojars artifact (default corium-clojure)
#   CLOJARS_USERNAME   Clojars account name (required)
#   CLOJARS_PASSWORD   Clojars deploy token (required)
#   MAX_ATTEMPTS       deploy attempts before giving up (default 3)
#
set -euo pipefail

VERSION="${VERSION:?VERSION is required}"
GROUP_ID="${GROUP_ID:-dev.corium}"
ARTIFACT_ID="${ARTIFACT_ID:-corium-clojure}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
CLOJARS_REPO="${CLOJARS_REPO:-https://repo.clojars.org}"

log() { printf '%s %s\n' "$(date -u '+%H:%M:%S')" "$*"; }

summary() {
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
  printf '%s\n' "$1" >>"$GITHUB_STEP_SUMMARY"
}

if [[ -z "${CLOJARS_USERNAME:-}" || -z "${CLOJARS_PASSWORD:-}" ]]; then
  echo "::error::CLOJARS_USERNAME and CLOJARS_PASSWORD are required." >&2
  exit 1
fi

# 0 = published, 1 = not published, 2 = could not tell.
clojars_published() {
  local code
  code=$(curl --silent --output /dev/null --location \
    --max-time 60 --retry 3 --retry-delay 5 --retry-all-errors \
    --write-out '%{http_code}' \
    "${CLOJARS_REPO}/${GROUP_ID//./\/}/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}.pom") || return 2
  case "$code" in
    200) return 0 ;;
    404) return 1 ;;
    *)
      log "Clojars returned HTTP ${code} for ${ARTIFACT_ID} ${VERSION}."
      return 2
      ;;
  esac
}

# Clojars' own refusal counts as success only once the artifact is confirmed
# present, which the loop checks before believing it.
TRANSIENT_PATTERN='Connection reset|Connection refused|Read timed out|[Cc]onnect timed out|SocketTimeoutException|SocketException|UnknownHostException|NoHttpResponseException|SSLException|SSLHandshakeException|Premature end of Content-Length|status code: 5[0-9][0-9]|status code: 429|Transfer failed for|Could not transfer artifact|Broken pipe'
REDEPLOY_PATTERN='redeploying non-snapshots is not allowed|already deployed|Forbidden - .*non-snapshot'

log_file=$(mktemp)
trap 'rm -f "$log_file"' EXIT

backoff=30
for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
  if clojars_published; then
    log "${GROUP_ID}/${ARTIFACT_ID} ${VERSION} is already on Clojars — nothing to publish."
    summary "Clojars already holds \`${GROUP_ID}/${ARTIFACT_ID}\` ${VERSION}; the publish was skipped."
    exit 0
  fi

  log "Publishing ${GROUP_ID}/${ARTIFACT_ID} ${VERSION} to Clojars (attempt ${attempt}/${MAX_ATTEMPTS})."
  if clojure -X:deploy 2>&1 | tee "$log_file"; then
    log "Clojars accepted ${VERSION}."
    summary "Published \`${GROUP_ID}/${ARTIFACT_ID}\` ${VERSION} to Clojars."
    exit 0
  fi

  if grep -qiE "$REDEPLOY_PATTERN" "$log_file"; then
    # Clojars says the version is already there while its repository said it
    # was not. The next iteration re-checks; if the artifact really is served,
    # that is a successful resume, and if it is not, this is a genuine
    # conflict and the loop reports it.
    log "Clojars rejected the deploy as a redeploy; re-checking the repository."
  elif grep -qiE "$TRANSIENT_PATTERN" "$log_file"; then
    log "Transient failure talking to Clojars."
  else
    echo "::error::The Clojars deploy failed for a reason retrying will not fix." >&2
    exit 1
  fi

  if ((attempt == MAX_ATTEMPTS)); then
    break
  fi
  log "Retrying in ${backoff}s."
  sleep "$backoff"
  backoff=$((backoff * 2))
done

if clojars_published; then
  log "${VERSION} is on Clojars despite the failed deploy."
  summary "Published \`${GROUP_ID}/${ARTIFACT_ID}\` ${VERSION} to Clojars (recovered after a failed attempt)."
  exit 0
fi

echo "::error::Publishing ${GROUP_ID}/${ARTIFACT_ID} ${VERSION} to Clojars failed after ${MAX_ATTEMPTS} attempts." >&2
exit 1
