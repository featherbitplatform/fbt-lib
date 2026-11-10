#!/usr/bin/env bash
# Tests dev/release-version.sh with fake refs: a branch and the matching tag pass with the
# workspace version; a mismatched tag, a tag without the "v" and a manifest without a version
# are refused with an `::error::` line. Also tests --notes, the changelog section extraction. The release workflow's verify job runs it.
# Usage: bash dev/test-release-version.sh
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/release-version.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# Runs the script with the given ref; sets $status, $out and $outputs (the GITHUB_OUTPUT file).
run() {
  : > "$work/output"
  status=0
  out="$(env GITHUB_REF_TYPE="$1" GITHUB_REF_NAME="$2" GITHUB_OUTPUT="$work/output" \
    ${3:+RELEASE_CARGO_TOML="$3"} bash "$script" 2>&1)" || status=$?
  outputs="$(cat "$work/output")"
}

expect_pass() {
  run "$@"
  [[ "$status" -eq 0 ]] || fail "$1 '$2' exited $status: $out"
  [[ "$out" == "$version" ]] || fail "$1 '$2' printed '$out', expected '$version'"
  [[ "$outputs" == "version=$version" ]] || fail "$1 '$2' wrote '$outputs' to GITHUB_OUTPUT"
  echo "ok: $1 '$2' -> $version"
}

expect_refusal() {
  run "$@"
  [[ "$status" -ne 0 ]] || fail "$1 '$2' was accepted: $out"
  grep -q '^::error::' <<<"$out" || fail "$1 '$2' refused without an ::error:: line: $out"
  [[ -z "$outputs" ]] || fail "$1 '$2' was refused but wrote '$outputs' to GITHUB_OUTPUT"
  echo "ok: $1 '$2' refused: $out"
}

# The real version, read off a branch.
run branch develop
[[ "$status" -eq 0 ]] || fail "branch run exited $status: $out"
version="$out"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || fail "branch run printed '$version'"

expect_pass branch develop
expect_pass "" ""
expect_pass tag "v$version"
expect_refusal tag "v$version.1"
expect_refusal tag v0.0.0-mismatch
expect_refusal tag "$version"

# A manifest without [workspace.package] version, and one with CRLF line endings.
printf '[workspace]\nmembers = []\n' > "$work/none.toml"
expect_refusal branch develop "$work/none.toml"
printf '[workspace.package]\r\nversion = "%s"\r\n' "$version" > "$work/crlf.toml"
expect_pass tag "v$version" "$work/crlf.toml"

# --notes: the version's section only, up to the next "## [" heading, trimmed.
cat > "$work/changelog.md" <<EOF
# Changelog

## [Unreleased]

### Added

- not released yet

## [$version] - 2026-01-01

### Added

- the feature ([#1](https://example.invalid/1))

### Fixed

- the fix

## [0.0.0-older] - 2025-01-01

- older entry
EOF
notes="$(RELEASE_CHANGELOG="$work/changelog.md" bash "$script" --notes)"
want="$(printf '### Added\n\n- the feature ([#1](https://example.invalid/1))\n\n### Fixed\n\n- the fix')"
[[ "$notes" == "$want" ]] || fail "--notes printed: $notes"
echo "ok: --notes extracts the $version section"
printf '## [Unreleased]\n\n- x\n' > "$work/nosection.md"
status=0
out="$(RELEASE_CHANGELOG="$work/nosection.md" bash "$script" --notes 2>&1)" || status=$?
[[ "$status" -ne 0 ]] && grep -q '^::error::' <<<"$out" || fail "--notes without a section was accepted: $out"
echo "ok: --notes refuses a missing section"

echo "release-version.sh: all checks passed"
