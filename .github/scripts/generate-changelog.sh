#!/usr/bin/env bash
#
# Build the release-notes body for a nightly release and write it to $1.
#
# Two sources are combined:
#   1. GitHub's own release-notes generator, which resolves commits back to the
#      pull requests that introduced them and credits their authors.
#   2. The raw commit subjects between the previous release and this one.
#
# Both are then optionally fed to Claude for a short "Highlights" section.  The
# summary is best-effort: if no API key is configured, or the call fails, the
# mechanically generated notes are used on their own and the release still
# goes out.
#
# Environment:
#   TAG                tag being released, e.g. v0.1.86       (required)
#   VERSION            version being released, e.g. 0.1.86    (required)
#   PREV_TAG           previous release tag, empty if none
#   PREV_BASE          commit the previous release was cut from, empty if none
#   TARGET_COMMITISH   commit the tag will point at; required while the tag
#                      does not exist yet, ignored once it does
#   GH_TOKEN           token for the GitHub API                (required)
#   GITHUB_REPOSITORY  owner/repo                              (required)
#   ANTHROPIC_API_KEY  optional; enables the Highlights section
#
set -euo pipefail

out="${1:?usage: generate-changelog.sh <output-file>}"
: "${TAG:?TAG is required}"
: "${VERSION:?VERSION is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

# Commit subjects since the previous release.  Merge commits are dropped
# because GitHub's generator already reports the pull requests they represent,
# and the version-bump commit each release tag carries is pure noise.
commit_range="${PREV_BASE:+${PREV_BASE}..}HEAD"
commits=$(git log --no-merges --pretty=format:'- %s' "$commit_range" \
  | grep -v '^- chore: release ' || true)

# GitHub's generator: pull requests, contributors, and a full-changelog link.
# Notes are generated before the tag is pushed, so target_commitish is what
# tells GitHub where the release would be cut from. It is ignored once the tag
# exists, which is what a resumed release sees.
notes_args=(-f "tag_name=${TAG}")
if [[ -n "${TARGET_COMMITISH:-}" ]]; then
  notes_args+=(-f "target_commitish=${TARGET_COMMITISH}")
fi
if [[ -n "${PREV_TAG:-}" ]]; then
  notes_args+=(-f "previous_tag_name=${PREV_TAG}")
fi

auto_notes=""
if auto_json=$(gh api --method POST \
      "repos/${GITHUB_REPOSITORY}/releases/generate-notes" \
      "${notes_args[@]}" 2>/dev/null); then
  auto_notes=$(jq -r '.body // ""' <<<"$auto_json" \
    | grep -v 'chore: release ' || true)
else
  echo "GitHub release-notes generation failed — falling back to commit subjects." >&2
fi

if [[ -z "${auto_notes//[[:space:]]/}" ]]; then
  auto_notes=$(printf '## What'"'"'s Changed\n\n%s' "${commits:--  No changes recorded.}")
fi

# Best-effort LLM summary.  Feed it both views of the release: the pull request
# titles carry intent, the commit subjects carry detail.
summary=""
if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
  material=$(printf 'Pull requests and generated notes:\n%s\n\nCommit subjects:\n%s\n' \
    "$auto_notes" "$commits")
  if summary=$(printf '%s' "$material" \
      | CHANGELOG_VERSION="$VERSION" \
        "$(dirname "$0")/summarize-changelog.sh"); then
    echo "Generated LLM changelog summary."
  else
    echo "Continuing without an LLM changelog summary." >&2
    summary=""
  fi
else
  echo "ANTHROPIC_API_KEY not configured — skipping the Highlights section."
fi

{
  echo "Nightly release \`${VERSION}\`."
  echo
  echo "| Artifact | Registry |"
  echo "| --- | --- |"
  echo "| Rust crates | [crates.io](https://crates.io/crates/corium-db) |"
  echo "| \`dev.corium:corium-client\` | [Maven Central](https://central.sonatype.com/artifact/dev.corium/corium-client/${VERSION}) |"
  echo "| \`dev.corium/corium-clojure\` | [Clojars](https://clojars.org/dev.corium/corium-clojure/versions/${VERSION}) |"
  echo "| \`corium\` | [PyPI](https://pypi.org/project/corium/${VERSION}/) |"
  echo
  if [[ -n "${summary//[[:space:]]/}" ]]; then
    echo "## Highlights"
    echo
    printf '%s\n' "$summary"
    echo
    echo "_Highlights are summarized by Claude from the commit log; the full list below is authoritative._"
    echo
  fi
  printf '%s\n' "$auto_notes"
} >"$out"

echo "--- release notes (${out}) ---"
cat "$out"
