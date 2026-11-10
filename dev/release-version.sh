#!/usr/bin/env bash
# Resolves the release version: the workspace version in Cargo.toml ([workspace.package]).
# On a tag push the tag must be exactly "v<version>", or this refuses with an `::error::` line
# and a non-zero exit, so no packaging job builds a release under the wrong version.
# Off a tag (branches, pull requests) it only resolves the version.
# Prints the version on stdout and, when GITHUB_OUTPUT is set, appends `version=<version>` to it.
# Inputs (environment, as GitHub Actions sets them; dev/test-release-version.sh overrides them):
#   GITHUB_REF_TYPE      "tag" or "branch" (unset: treated as not a tag)
#   GITHUB_REF_NAME      the tag or branch name
#   RELEASE_CARGO_TOML   the manifest to read (default: the repository's Cargo.toml)
#   RELEASE_CHANGELOG    the changelog to read for --notes (default: the repository's CHANGELOG.md)
# Usage: bash dev/release-version.sh           resolve (and check) the version
#        bash dev/release-version.sh --notes   print the version's CHANGELOG.md section, from the
#                                              line after its "## [x.y.z]" heading up to the next
#                                              "## [" heading, trimmed; refuses an absent or empty one
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
manifest="${RELEASE_CARGO_TOML:-$root/Cargo.toml}"
[[ -f "$manifest" ]] || { echo "::error::no manifest at $manifest"; exit 1; }

# tr: a Windows checkout may have CRLF line endings.
version="$(tr -d '\r' < "$manifest" \
  | awk '/^\[workspace\.package\]/{f=1;next} /^\[/{f=0} f && /^version *=/{gsub(/"/,"",$3);print $3;exit}')"
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
  echo "::error::no valid [workspace.package] version in $manifest (found '$version')"
  exit 1
fi

if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  tag="${GITHUB_REF_NAME:-}"
  if [[ "$tag" != "v$version" ]]; then
    echo "::error::tag '$tag' does not match the workspace version $version (expected v$version)"
    exit 1
  fi
fi

if [[ "${1:-}" == "--notes" ]]; then
  changelog="${RELEASE_CHANGELOG:-$root/CHANGELOG.md}"
  [[ -f "$changelog" ]] || { echo "::error::no changelog at $changelog"; exit 1; }
  notes="$(tr -d '\r' < "$changelog" | awk -v v="$version" '
    /^## \[/ { p = ($2 == "[" v "]"); next }
    p { l[++n] = $0 }
    END {
      while (n > 0 && l[n] == "") n--
      for (i = 1; i <= n; i++) if (l[i] != "" || s) { s = 1; print l[i] }
    }')"
  if [[ -z "$notes" ]]; then
    echo "::error::$changelog has no section for $version"
    exit 1
  fi
  printf '%s\n' "$notes"
  exit 0
fi

echo "$version"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "version=$version" >> "$GITHUB_OUTPUT"
fi
