<#
.SYNOPSIS
    Local security scans: the same scanners and thresholds as
    .github/workflows/security.yml, run via Docker (plus native cargo-deny).

.DESCRIPTION
    Windows twin of dev/sast.sh; keep the two in sync. Every tool reads its
    config from the repo root (deny.toml, .semgrepignore, .gitleaks.toml,
    .grype.yaml), so local runs and CI can't drift.
    High/critical findings fail; lower severities are reported.

.EXAMPLE
    ./dev/sast.ps1                 # every scan plus the SBOMs
    ./dev/sast.ps1 semgrep         # a single tool
    ./dev/sast.ps1 semgrep deny    # any subset
    ./dev/sast.ps1 sbom            # CycloneDX SBOMs into sast-out/sbom/

    Targets: semgrep deny grype gitleaks sbom all
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Targets = @('all')
)

$ErrorActionPreference = 'Continue'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

# Tool images, the same as the gateway's dev/sast.ps1. Bump deliberately.
$SemgrepImage  = 'semgrep/semgrep:latest'
$GrypeImage    = 'anchore/grype:latest'
$GitleaksImage = 'zricethezav/gitleaks:latest'
# The same syft version as the release workflow's SBOM job.
$SyftImage     = 'anchore/syft:v1.54.1'

# Semgrep registry rulesets: keep in sync with the semgrep job in security.yml.
$SemgrepRulesets = @('p/default', 'p/rust')

if ($Targets -contains 'all') {
    $Targets = @('semgrep', 'deny', 'grype', 'gitleaks', 'sbom')
}

$Results = [ordered]@{}

function Invoke-Scan {
    param([string]$Name, [scriptblock]$Body)
    Write-Host ""
    Write-Host "=== $Name ===" -ForegroundColor Cyan
    & $Body
    $Results[$Name] = ($LASTEXITCODE -eq 0)
}

foreach ($target in $Targets) {
    switch ($target) {

        'semgrep' {
            Invoke-Scan 'semgrep' {
                $configs = $SemgrepRulesets | ForEach-Object { @('--config', $_) }
                docker run --rm -v "${RepoRoot}:/src:ro" -w /src $SemgrepImage `
                    semgrep scan @configs --severity ERROR --error --metrics=off
            }
        }

        'deny' {
            Invoke-Scan 'cargo-deny' {
                if (-not (Get-Command cargo-deny -ErrorAction SilentlyContinue)) {
                    Write-Host 'cargo-deny is not installed; run: cargo install cargo-deny --locked' -ForegroundColor Yellow
                    cmd /c 'exit 1'
                    return
                }
                cargo deny --manifest-path "$RepoRoot/Cargo.toml" --all-features check
            }
        }

        'grype' {
            Invoke-Scan 'grype (filesystem)' {
                # Named volume caches the vulnerability DB between runs.
                docker run --rm -v "${RepoRoot}:/src:ro" `
                    -v fbt-lib-grype-db:/db -e GRYPE_DB_CACHE_DIR=/db `
                    $GrypeImage dir:/src -c /src/.grype.yaml
            }
        }

        'gitleaks' {
            # Full history, as CI scans it. The git directory is mounted on its
            # own so this also works from a linked worktree (.git is a file).
            $gitDir = (git -C $RepoRoot rev-parse --path-format=absolute --git-common-dir)
            Invoke-Scan 'gitleaks (history)' {
                docker run --rm -v "${RepoRoot}:/src:ro" -v "${gitDir}:/repo:ro" $GitleaksImage `
                    git /repo --config /src/.gitleaks.toml --no-banner --redact
            }.GetNewClosure()
            # The working tree too, so files not committed yet are covered.
            Invoke-Scan 'gitleaks (working tree)' {
                docker run --rm -v "${RepoRoot}:/src:ro" $GitleaksImage `
                    dir /src --config /src/.gitleaks.toml --no-banner --redact
            }
        }

        'sbom' {
            # CycloneDX SBOM with the same generator, name and version as the
            # release assets (.github/workflows/release.yml, job sbom).
            $version = (bash "$RepoRoot/dev/release-version.sh")
            if ($LASTEXITCODE -ne 0) { $Results['sbom'] = $false; continue }
            New-Item -ItemType Directory -Force "$RepoRoot/sast-out/sbom" | Out-Null
            Invoke-Scan 'sbom (fbt-lib)' {
                docker run --rm -v "${RepoRoot}:/src:ro" -v "${RepoRoot}/sast-out/sbom:/out" `
                    $SyftImage scan file:/src/Cargo.lock `
                    --source-name fbt-lib --source-version $version `
                    -o "cyclonedx-json=/out/fbt-lib-$version.cdx.json"
            }.GetNewClosure()
        }

        default {
            Write-Host "Unknown target '$target'. Targets: semgrep deny grype gitleaks sbom all" -ForegroundColor Red
            exit 2
        }
    }
}

Write-Host ""
Write-Host '=== Summary ===' -ForegroundColor Cyan
$failed = $false
foreach ($entry in $Results.GetEnumerator()) {
    if ($entry.Value) {
        Write-Host ("  PASS  {0}" -f $entry.Key) -ForegroundColor Green
    } else {
        Write-Host ("  FAIL  {0}" -f $entry.Key) -ForegroundColor Red
        $failed = $true
    }
}
if ($failed) { exit 1 }
