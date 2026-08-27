#!/usr/bin/env bash
#
# Publish the Java client and its storage plugins to Maven Central, in a way
# that survives being run twice.
#
# The Central Portal accepts a deployment, then validates and publishes it
# asynchronously. central-publishing-maven-plugin uploads one bundle for the
# whole reactor and then polls until that deployment reaches PUBLISHED. Two
# things go wrong when the Portal is slow, and both did on v0.1.86:
#
#   1. The plugin's poll gives up (`Polling for <id> timed out before the
#      deployment completed`) while the deployment is still PUBLISHING. The
#      build fails even though nothing is actually wrong.
#
#   2. Re-running the job then uploads a *second* bundle for the same
#      coordinates, which the Portal rejects outright: `Component with
#      coordinate 'dev.corium:corium-turso:0.1.86' is currently being
#      published in another deployment (<id>)`. Every re-run fails the same
#      way for as long as the first deployment is in flight.
#
# So this script never treats a slow Portal as a build failure and never
# uploads on top of a deployment that is still running:
#
#   * Before deploying it asks the Portal whether every artifact is already
#     published, and exits successfully if so. The Portal knows immediately;
#     repo1.maven.org lags a publish by tens of minutes, so it is only a
#     fallback for when the Portal cannot be reached.
#   * On a polling timeout or a coordinate conflict it recovers the deployment
#     id from Maven's output and polls that deployment to a terminal state
#     instead of re-uploading. A deployment that reaches PUBLISHED is a
#     success; one that reaches FAILED has released the coordinates, so the
#     upload can be retried.
#   * Transport-level failures are retried with backoff. Anything else — a
#     bad signature, a rejected POM, a missing native jar — fails on the first
#     attempt, because retrying it would only waste half an hour.
#
# Environment:
#   VERSION                  version being released, e.g. 0.1.86 (required)
#   NATIVE_JARS              directory holding the per-platform native jars
#   POM                      reactor root (default clients/java/pom.xml)
#   GROUP_ID                 Maven group id (default dev.corium)
#   MAVEN_CENTRAL_USERNAME   Portal token name, used for mvn and for the API
#   MAVEN_CENTRAL_TOKEN      Portal token secret
#   MAVEN_GPG_PASSPHRASE     passed through to maven-gpg-plugin
#   MAX_ATTEMPTS             upload attempts before giving up (default 3)
#   SETTLE_TIMEOUT           seconds to wait for an in-flight deployment to
#                            reach a terminal state (default 3600)
#   POLL_INTERVAL            seconds between deployment status polls (default 30)
#
set -euo pipefail

VERSION="${VERSION:?VERSION is required}"
POM="${POM:-clients/java/pom.xml}"
GROUP_ID="${GROUP_ID:-dev.corium}"
NATIVE_JARS="${NATIVE_JARS:-}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-3600}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"

CENTRAL_BASE_URL="${CENTRAL_BASE_URL:-https://central.sonatype.com}"
REPO1_BASE_URL="${REPO1_BASE_URL:-https://repo1.maven.org/maven2}"

# Every artifact the reactor deploys, parent pom included. A resumed run may
# only skip when all of them are already on Central: the Portal publishes a
# deployment atomically, so a partial answer means something is still moving.
ARTIFACTS=(corium-java corium-client corium-turso corium-postgres corium-s3)

log() { printf '%s %s\n' "$(date -u '+%H:%M:%S')" "$*"; }

summary() {
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
  printf '%s\n' "$1" >>"$GITHUB_STEP_SUMMARY"
}

# ---------------------------------------------------------------------------
# Central Portal API
# ---------------------------------------------------------------------------
# Same endpoints and the same `UserToken <base64(name:secret)>` scheme that
# central-publishing-maven-plugin uses, so the answers here and the plugin's
# own view of a deployment can never disagree.

# The token goes into a curl config file rather than onto a command line, so
# it never appears in the runner's process table or in `set -x` output.
portal_auth_file=""
if [[ -n "${MAVEN_CENTRAL_USERNAME:-}" && -n "${MAVEN_CENTRAL_TOKEN:-}" ]]; then
  portal_auth_file=$(mktemp)
  chmod 600 "$portal_auth_file"
  trap 'rm -f "$portal_auth_file"' EXIT
  printf 'header = "Authorization: UserToken %s"\n' \
    "$(printf '%s:%s' "$MAVEN_CENTRAL_USERNAME" "$MAVEN_CENTRAL_TOKEN" | base64 -w0)" \
    >"$portal_auth_file"
fi

# Echoes the response body, then a final line holding the HTTP status. Returns
# non-zero only when the request never got an answer at all.
portal_request() {
  local method=$1 path=$2
  curl --silent --show-error --location \
    --config "$portal_auth_file" \
    --request "$method" \
    --header 'Accept: application/json' \
    --max-time 60 --retry 3 --retry-delay 5 --retry-all-errors \
    --write-out '\n%{http_code}' \
    "${CENTRAL_BASE_URL}${path}"
}

# 0 = published, 1 = not published, 2 = could not tell.
portal_published() {
  local artifact=$1 response code body
  [[ -n "$portal_auth_file" ]] || return 2
  response=$(portal_request GET \
    "/api/v1/publisher/published?namespace=${GROUP_ID}&name=${artifact}&version=${VERSION}") || return 2
  code=${response##*$'\n'}
  body=${response%$'\n'*}
  if [[ "$code" != "200" ]]; then
    log "Portal published check for ${artifact} returned HTTP ${code}."
    return 2
  fi
  [[ "$(printf '%s' "$body" | jq -r '.published // false')" == "true" ]]
}

# repo1 only ever gains artifacts, so a 200 is proof and a 404 is "not yet" —
# but it lags the Portal by tens of minutes, which is exactly the window a
# same-day re-run lands in. Never used to decide "not published"; only to
# confirm a publish when the Portal is unreachable.
repo1_published() {
  local artifact=$1 code
  code=$(curl --silent --output /dev/null --location \
    --max-time 60 --retry 3 --retry-delay 5 --retry-all-errors \
    --write-out '%{http_code}' \
    "${REPO1_BASE_URL}/${GROUP_ID//./\/}/${artifact}/${VERSION}/${artifact}-${VERSION}.pom") || return 2
  case "$code" in
    200) return 0 ;;
    404) return 1 ;;
    *) return 2 ;;
  esac
}

artifact_published() {
  local artifact=$1 status=0
  portal_published "$artifact" || status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
  esac
  # The Portal could not answer; repo1 can still confirm an older publish.
  repo1_published "$artifact"
}

# 0 when every artifact is live, 1 otherwise. An artifact whose state cannot be
# determined counts as not published: deploying a version that is already there
# is recoverable (`ignorePublishedComponents` drops it from the bundle), while
# skipping a version that never shipped strands the release.
all_published() {
  local artifact
  for artifact in "${ARTIFACTS[@]}"; do
    if artifact_published "$artifact"; then
      continue
    fi
    log "${GROUP_ID}:${artifact}:${VERSION} is not on Maven Central yet."
    return 1
  done
  return 0
}

# Prints the deployment's state: PENDING, VALIDATING, VALIDATED, PUBLISHING,
# PUBLISHED or FAILED. Prints nothing when the Portal cannot be asked.
deployment_state() {
  local id=$1 response code body
  [[ -n "$portal_auth_file" ]] || return 1
  response=$(portal_request POST "/api/v1/publisher/status?id=${id}") || return 1
  code=${response##*$'\n'}
  body=${response%$'\n'*}
  [[ "$code" == "200" ]] || return 1
  printf '%s' "$body" | jq -r '.deploymentState // empty'
}

deployment_errors() {
  local id=$1 response body
  [[ -n "$portal_auth_file" ]] || return 0
  response=$(portal_request POST "/api/v1/publisher/status?id=${id}") || return 0
  body=${response%$'\n'*}
  printf '%s' "$body" | jq -r '(.errors // {}) | to_entries[] | "\(.key): \(.value | join("; "))"' 2>/dev/null || true
}

# Waits for a deployment that is already in flight. Returns 0 when it reaches
# PUBLISHED, 1 when it reaches FAILED (which releases the coordinates, so an
# upload may be retried), and 2 when it is still moving at the deadline.
await_deployment() {
  local id=$1 deadline=$((SECONDS + SETTLE_TIMEOUT)) state=""
  log "Waiting for deployment ${id} to settle (up to ${SETTLE_TIMEOUT}s)."
  while ((SECONDS < deadline)); do
    state=$(deployment_state "$id" || true)
    case "$state" in
      PUBLISHED)
        log "Deployment ${id} is PUBLISHED."
        return 0
        ;;
      FAILED)
        log "Deployment ${id} is FAILED."
        deployment_errors "$id"
        return 1
        ;;
      "")
        # A deployment the Portal will not describe — a dropped id, or an
        # expired token. Fall back to the coordinates themselves.
        if all_published; then
          return 0
        fi
        log "Deployment ${id} has no readable status; retrying."
        ;;
      *)
        log "Deployment ${id} is ${state}."
        ;;
    esac
    sleep "$POLL_INTERVAL"
  done
  log "Deployment ${id} was still ${state:-unknown} after ${SETTLE_TIMEOUT}s."
  return 2
}

# ---------------------------------------------------------------------------
# Classifying a failed `mvn deploy`
# ---------------------------------------------------------------------------

# The plugin names the deployment in both of the messages we recover from, so
# the id is always available when it matters.
deployment_id_from() {
  sed -n \
    -e "s/.*Polling for \([0-9a-f-]\{36\}\) timed out.*/\1/p" \
    -e "s/.*is currently being published in another deployment (\([0-9a-f-]\{36\}\)).*/\1/p" \
    "$1" | head -n 1
}

# Transport failures worth another upload. Deliberately narrow: a wrong GPG
# passphrase or an invalid POM must fail on the first attempt rather than
# three times over.
TRANSIENT_PATTERN='Connection reset|Connection refused|Read timed out|[Cc]onnect timed out|SocketTimeoutException|SocketException|UnknownHostException|NoHttpResponseException|SSLException|SSLHandshakeException|Premature end of Content-Length|Response status code: 5[0-9][0-9]|status code: 429|Transfer failed for|Could not transfer artifact'

classify_failure() {
  local log_file=$1
  if grep -qE 'Polling for [0-9a-f-]+ timed out' "$log_file"; then
    printf 'polling-timeout'
  elif grep -qE 'is currently being published in another deployment' "$log_file"; then
    printf 'coordinates-locked'
  elif grep -qE "$TRANSIENT_PATTERN" "$log_file"; then
    printf 'transient'
  else
    printf 'fatal'
  fi
}

# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

run_deploy() {
  local log_file=$1
  local -a args=(
    -f "$POM" --batch-mode --no-transfer-progress
    --activate-profiles 'release,native'
  )
  if [[ -n "$NATIVE_JARS" ]]; then
    args+=("-Dcorium.native.jars=${NATIVE_JARS}")
  fi
  args+=(deploy)
  mvn "${args[@]}" 2>&1 | tee "$log_file"
}

if all_published; then
  log "${GROUP_ID}:*:${VERSION} is already on Maven Central — nothing to publish."
  summary "Maven Central already holds \`${GROUP_ID}\` ${VERSION}; the publish was skipped."
  exit 0
fi

if [[ -z "$portal_auth_file" ]]; then
  echo "::warning::MAVEN_CENTRAL_USERNAME/MAVEN_CENTRAL_TOKEN are unset, so a stalled deployment cannot be polled."
fi

log_file=$(mktemp)
trap 'rm -f "$log_file" "$portal_auth_file"' EXIT

backoff=30
for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
  log "Publishing ${GROUP_ID} ${VERSION} to Maven Central (attempt ${attempt}/${MAX_ATTEMPTS})."

  if run_deploy "$log_file"; then
    log "Maven reported the deployment published."
    summary "Published \`${GROUP_ID}\` ${VERSION} to Maven Central."
    exit 0
  fi

  # The upload may well have landed even though Maven exited non-zero.
  if all_published; then
    log "The deployment completed despite Maven's failure — ${VERSION} is on Central."
    summary "Published \`${GROUP_ID}\` ${VERSION} to Maven Central (recovered after a Portal timeout)."
    exit 0
  fi

  kind=$(classify_failure "$log_file")
  case "$kind" in
    polling-timeout | coordinates-locked)
      deployment_id=$(deployment_id_from "$log_file")
      if [[ -z "$deployment_id" ]]; then
        echo "::error::Maven reported an in-flight deployment but named no id; refusing to upload again." >&2
        exit 1
      fi
      if [[ "$kind" == "polling-timeout" ]]; then
        log "The Portal did not finish publishing ${deployment_id} within Maven's poll window."
      else
        log "Deployment ${deployment_id} from an earlier run still holds these coordinates."
      fi

      set +e
      await_deployment "$deployment_id"
      settled=$?
      set -e
      case "$settled" in
        0)
          summary "Published \`${GROUP_ID}\` ${VERSION} to Maven Central (deployment ${deployment_id})."
          exit 0
          ;;
        1)
          # FAILED releases the coordinates. Retry only if there is an attempt
          # left; the deployment's own errors are already in the log above.
          log "Deployment ${deployment_id} failed; the coordinates are free again."
          ;;
        *)
          echo "::error::Deployment ${deployment_id} is still in flight after ${SETTLE_TIMEOUT}s. Do not re-run this job until it reaches PUBLISHED or FAILED at ${CENTRAL_BASE_URL}/publishing/deployments — a second upload of ${VERSION} will be rejected while it holds the coordinates." >&2
          exit 1
          ;;
      esac
      ;;
    transient)
      log "Transient failure talking to Maven Central."
      ;;
    *)
      echo "::error::The Maven Central deploy failed for a reason retrying will not fix." >&2
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

echo "::error::Publishing ${GROUP_ID} ${VERSION} to Maven Central failed after ${MAX_ATTEMPTS} attempts." >&2
exit 1
