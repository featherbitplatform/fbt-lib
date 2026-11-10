#!/usr/bin/env bash
# Local security scans: the same scanners and thresholds as
# .github/workflows/security.yml, run via Docker (plus native cargo-deny).
# macOS/Linux twin of dev/sast.ps1; keep the two in sync.
#
# Usage:
#   bash dev/sast.sh                 # every scan plus the SBOMs
#   bash dev/sast.sh semgrep         # a single tool
#   bash dev/sast.sh semgrep deny    # any subset
#   bash dev/sast.sh sbom            # CycloneDX SBOMs into sast-out/sbom/
#
# Targets: semgrep deny grype gitleaks sbom all
# Every tool reads its config from the repo root (deny.toml, .semgrepignore,
# .gitleaks.toml, .grype.yaml), so local runs and CI
# can't drift. High/critical findings fail; lower severities are reported.
set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Tool images, the same as the gateway's dev/sast.sh. Bump deliberately.
SEMGREP_IMAGE='semgrep/semgrep:latest'
GRYPE_IMAGE='anchore/grype:latest'
GITLEAKS_IMAGE='zricethezav/gitleaks:latest'
# The same syft version as the release workflow's SBOM job.
SYFT_IMAGE='anchore/syft:v1.54.1'

# Semgrep registry rulesets: keep in sync with the semgrep job in security.yml.
SEMGREP_RULESETS='--config p/default --config p/rust'

TARGETS=("$@")
[ ${#TARGETS[@]} -eq 0 ] && TARGETS=(all)
if [ "${TARGETS[0]}" = all ]; then
    TARGETS=(semgrep deny grype gitleaks sbom)
fi

NAMES=()
CODES=()

run_scan() { # run_scan <name> <cmd...>
    local name="$1"; shift
    printf '\n=== %s ===\n' "$name"
    "$@"
    local code=$?
    NAMES+=("$name")
    CODES+=("$code")
}

for target in "${TARGETS[@]}"; do
    case "$target" in

    semgrep)
        # shellcheck disable=SC2086  # rulesets deliberately word-split
        run_scan semgrep docker run --rm -v "$REPO_ROOT:/src:ro" -w /src "$SEMGREP_IMAGE" \
            semgrep scan $SEMGREP_RULESETS --severity ERROR --error --metrics=off
        ;;

    deny)
        if command -v cargo-deny >/dev/null 2>&1; then
            run_scan cargo-deny cargo deny --manifest-path "$REPO_ROOT/Cargo.toml" --all-features check
        else
            echo 'cargo-deny is not installed; run: cargo install cargo-deny --locked'
            NAMES+=(cargo-deny); CODES+=(1)
        fi
        ;;

    grype)
        # Named volume caches the vulnerability DB between runs.
        run_scan 'grype (filesystem)' docker run --rm -v "$REPO_ROOT:/src:ro" \
            -v fbt-lib-grype-db:/db -e GRYPE_DB_CACHE_DIR=/db \
            "$GRYPE_IMAGE" dir:/src -c /src/.grype.yaml
        ;;

    gitleaks)
        # Full history, as CI scans it. The git directory is mounted on its own
        # so this also works from a linked worktree, whose .git is only a file.
        git_dir="$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir)"
        run_scan 'gitleaks (history)' docker run --rm -v "$REPO_ROOT:/src:ro" -v "$git_dir:/repo:ro" \
            "$GITLEAKS_IMAGE" git /repo --config /src/.gitleaks.toml --no-banner --redact
        # The working tree too, so files not committed yet are covered.
        run_scan 'gitleaks (working tree)' docker run --rm -v "$REPO_ROOT:/src:ro" \
            "$GITLEAKS_IMAGE" dir /src --config /src/.gitleaks.toml --no-banner --redact
        ;;

    sbom)
        # CycloneDX SBOM with the same generator, name and version as the
        # release assets (.github/workflows/release.yml, job sbom).
        version="$(bash "$REPO_ROOT/dev/release-version.sh")" || { NAMES+=(sbom); CODES+=(1); continue; }
        mkdir -p "$REPO_ROOT/sast-out/sbom"
        run_scan 'sbom (fbt-lib)' docker run --rm -v "$REPO_ROOT:/src:ro" \
            -v "$REPO_ROOT/sast-out/sbom:/out" "$SYFT_IMAGE" \
            scan file:/src/Cargo.lock --source-name fbt-lib --source-version "$version" \
            -o "cyclonedx-json=/out/fbt-lib-$version.cdx.json"
        ;;

    *)
        echo "Unknown target '$target'. Targets: semgrep deny grype gitleaks sbom all"
        exit 2
        ;;
    esac
done

printf '\n=== Summary ===\n'
failed=0
for i in "${!NAMES[@]}"; do
    if [ "${CODES[$i]}" -eq 0 ]; then
        printf '  PASS  %s\n' "${NAMES[$i]}"
    else
        printf '  FAIL  %s\n' "${NAMES[$i]}"
        failed=1
    fi
done
exit "$failed"
