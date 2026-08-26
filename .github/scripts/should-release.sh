#!/usr/bin/env bash
#
# Decide whether tonight's nightly should produce a release, and pick its
# version number.
#
# A release happens only when the tree has changed in a way that affects the
# published artifacts since the previous release. Documentation and website
# edits alone are not worth a version bump across four registries.
#
# Writes to $GITHUB_OUTPUT (or stdout when run locally):
#   should_release  true | false
#   version         the version to publish, e.g. 0.1.86
#   tag             v<version>
#   prev_tag        the release this one follows, empty on the first release
#   prev_base       commit that release was cut from, used as the diff base
#   head_sha        the commit this decision was made about
#   changed_files   newline-joined list of release-relevant paths (may be empty)
#
# Environment:
#   BASE_VERSION  major.minor prefix for the version series (default "0.1")
#   FORCE         "true" to release regardless of which paths changed
#   GH_TOKEN      optional; lets the script detect an unfinished release
#
set -euo pipefail

BASE_VERSION="${BASE_VERSION:-0.1}"
FORCE="${FORCE:-false}"

# Paths that never justify a release on their own. git pathspecs, matched
# against the full path from the repository root.
IGNORED_PATHS=(
  ':(exclude,glob)docs/**'
  ':(exclude,glob)web/**'
  ':(exclude,glob).claude/**'
  ':(exclude,glob)**/*.md'
)

emit() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s\n' "$1" >>"$GITHUB_OUTPUT"
  else
    printf '%s\n' "$1"
  fi
}

# A release tag points at a `chore: release X` commit that bumps every manifest
# in the workspace and in the three client trees. That commit is never merged
# back into main, so the merge base — not the tag itself — is the branch commit
# the release was cut from, and the only honest comparison point. Diffing the
# tag directly would report every manifest as changed on every run.
base_of() { git merge-base "$1" HEAD; }

gh_available() {
  command -v gh >/dev/null 2>&1 || return 1
  [[ -n "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]] || return 1
}

# A GitHub release is the marker that a version finished shipping: the workflow
# creates it only after crates.io, Maven Central, Clojars, and PyPI have all
# accepted the version. When we cannot check (no gh, no token), assume finished
# — that keeps the script from re-publishing on a hunch.
release_finished() {
  gh_available || return 0
  gh release view "$1" >/dev/null 2>&1
}

# Has this workflow ever completed a release? Tags predating the nightly
# cutover were pushed by the old publish-on-every-commit workflow, which never
# created GitHub releases, so "no release for this tag" does not mean "this tag
# was left half-published" for any of them. Requiring that at least one release
# exists keeps the resume path from firing on that history. The cost is that a
# failure during the very first nightly burns a version number instead of being
# resumed; every subsequent one resumes normally.
any_release_exists() {
  gh_available || return 1
  [[ -n "$(gh release list --limit 1 --json tagName --jq '.[].tagName' 2>/dev/null)" ]]
}

# Every release tag ever pushed, in version order. Needed because the patch
# counter restarts at 0 whenever BASE_VERSION moves: the first release of a new
# series still follows the last release of the old one, and should report its
# changes relative to that rather than to the beginning of history.
mapfile -t all_tags < <(git tag --list 'v*' \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
  | sort -V)

newest_tag=""
[[ ${#all_tags[@]} -ge 1 ]] && newest_tag="${all_tags[-1]}"

# Existing patch numbers in the current series, ascending. Tags are the source
# of truth for the version series: every published release pushes one, and
# unlike a workflow run number they survive the workflow file being replaced —
# which is exactly what this change does to the run counter the old
# publish-on-push workflow used.
mapfile -t patches < <(git tag --list "v${BASE_VERSION}.*" \
  | sed -n "s|^v${BASE_VERSION}\.\([0-9][0-9]*\)$|\1|p" \
  | sort -n)

count=${#patches[@]}
latest_tag=""
[[ $count -ge 1 ]] && latest_tag="v${BASE_VERSION}.${patches[count - 1]}"

# The release immediately preceding $1, which may be in an earlier series.
tag_before() {
  local target=$1 prev="" t
  for t in ${all_tags[@]+"${all_tags[@]}"}; do
    [[ "$t" == "$target" ]] && break
    prev=$t
  done
  printf '%s' "$prev"
}

head_sha=$(git rev-parse HEAD)
resuming=false

if [[ -n "$latest_tag" && "$(base_of "$latest_tag")" == "$head_sha" ]] \
   && any_release_exists && ! release_finished "$latest_tag"; then
  # The newest tag was cut from this very commit but never got a GitHub
  # release, so a previous attempt died partway through publishing. Finish that
  # version instead of allocating a new one: the artifacts already live under
  # it would otherwise be stranded, leaving a version where some crates or
  # client jars exist and the rest never will.
  resuming=true
  tag="$latest_tag"
  version="${tag#v}"
  prev_tag=$(tag_before "$latest_tag")
  echo "Tag $tag exists at HEAD with no GitHub release — resuming that release."
else
  next_patch=0
  [[ $count -ge 1 ]] && next_patch=$((patches[count - 1] + 1))
  version="${BASE_VERSION}.${next_patch}"
  tag="v${version}"
  # Falls back to the previous series' newest tag when this series has none
  # yet, so a base-version bump does not turn the first release into a
  # full-history changelog with the path gate bypassed.
  prev_tag="${latest_tag:-$newest_tag}"
fi

echo "Previous release tag: ${prev_tag:-<none>}"
echo "Candidate version:    $version"

if [[ -n "$prev_tag" ]]; then
  prev_base=$(base_of "$prev_tag")
  echo "Previous release base: $prev_base"
  changed=$(git diff --name-only "${prev_base}..HEAD" -- . "${IGNORED_PATHS[@]}")
  total=$(git diff --name-only "${prev_base}..HEAD" | wc -l | tr -d ' ')
  relevant=$(printf '%s' "$changed" | grep -c . || true)
  echo "Changed since ${prev_tag}: ${total} file(s), ${relevant} release-relevant."
else
  prev_base=""
  changed=""
fi

if [[ "$resuming" == "true" ]]; then
  should_release=true
elif [[ -z "$prev_tag" ]]; then
  echo "No previous release tag — releasing the initial version."
  should_release=true
elif [[ -n "$changed" ]]; then
  should_release=true
elif [[ "$FORCE" == "true" ]]; then
  echo "Only documentation/website changes, but FORCE is set — releasing anyway."
  should_release=true
else
  should_release=false
fi

if [[ "$should_release" == "true" ]]; then
  echo "Decision: release $tag"
else
  echo "Decision: skip — no non-doc, non-web changes since ${prev_tag}."
fi

emit "should_release=$should_release"
emit "version=$version"
emit "tag=$tag"
emit "prev_tag=$prev_tag"
emit "prev_base=$prev_base"
emit "head_sha=$head_sha"

# Multi-line output needs a heredoc-style delimiter.
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "changed_files<<__CHANGED_EOF__"
    printf '%s\n' "$changed"
    echo "__CHANGED_EOF__"
  } >>"$GITHUB_OUTPUT"
fi

if [[ -n "$changed" ]]; then
  echo "--- release-relevant changes ---"
  printf '%s\n' "$changed" | head -n 50
fi
