#!/usr/bin/env bash
# check-outbound.sh: the outbound audit gate.
#
# Every outbound operation in the engine crates must report to the fbt-core
# audit hook (crates/fbt-core/src/audit.rs): each hit of `TcpStream::connect`,
# `.connect(` (a socket dial, e.g. a TcpSocket), `lookup_host`, `.send_to(` or
# `send_to_host(` needs an `audit::record` within the 3 lines above, or a
# `// audit: <reason>` comment on the line above (a send through a trait
# sender that records in its concrete implementation, a method that is not a
# socket dial, a UDP connect that sends no packet).
#
# The scan covers crates/*/src: not fbt-api (the server stack) and not
# fbt-example, plus the crates named in CHECK_OUTBOUND_SKIP (comma-separated;
# fbt-session while its outbound is not audited yet). Each file stops at its
# #[cfg(test)] module.
#
# Prints `check-outbound: ok`, or each offending file:line and exits 1.
set -uo pipefail

skip="${CHECK_OUTBOUND_SKIP:-}"
bad=0

while IFS= read -r file; do
    crate="${file#crates/}"
    crate="${crate%%/*}"
    case ",${skip}," in
        *",${crate},"*) continue ;;
    esac
    awk -v fname="$file" '
        { lines[FNR] = $0 }
        END {
            for (i = 1; i <= FNR; i++) {
                if (stop) break
                l = lines[i]
                # A #[cfg(test)] module ends the scan; the attribute alone on
                # an import or helper does not.
                if (l ~ /^[[:space:]]*#\[cfg\(test\)\][[:space:]]*$/) {
                    j = i + 1
                    while (j <= FNR && lines[j] ~ /^[[:space:]]*$/) j++
                    if (j <= FNR && lines[j] ~ /^[[:space:]]*mod[[:space:]]/) { stop = 1; continue }
                }
                if (l !~ /TcpStream::connect/ && l !~ /\.connect\(/ && l !~ /lookup_host/ &&
                    l !~ /\.send_to\(/ && l !~ /send_to_host\(/) continue
                ok = 0
                if (i >= 2 && lines[i - 1] ~ /\/\/ audit:/) ok = 1
                for (k = i - 3; k <= i - 1 && !ok; k++)
                    if (k >= 1 && lines[k] ~ /audit::record/) ok = 1
                if (!ok) { print fname ":" i; bad = 1 }
            }
            exit bad + 0
        }
    ' "$file" || bad=1
done < <(find crates -path '*/src/*.rs' -not -path 'crates/fbt-api/*' -not -path 'crates/fbt-example/*' | sort)

if [ "$bad" -ne 0 ]; then
    echo "check-outbound: each line above needs audit::record within the 3 lines above, or a // audit: reason comment on the line above" >&2
    exit 1
fi
echo "check-outbound: ok"
