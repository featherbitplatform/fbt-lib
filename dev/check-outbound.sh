#!/usr/bin/env bash
# check-outbound.sh: the outbound audit gate.
#
# Every outbound operation in the engine crates must report to the fbt-core
# audit hook (crates/fbt-core/src/audit.rs): each hit of the patterns below
# needs an `audit::record` within the 3 lines above, or a `// audit: <reason>`
# comment (with a non-empty reason) on the line above.
#
# Patterns — what leaves the host, or names something to the resolver:
#   TcpStream::connect                 a TCP dial (tokio)
#   .connect(                          any socket dial; on a UDP socket it
#       also pins the destination, after which a bare .send( would not be
#       visible as a send_to — so the connect itself must be recorded or
#       explained
#   lookup_host                        a name handed to the system resolver
#   to_socket_addrs, ToSocketAddrs     the std name-to-addresses conversion
#   .send_to( try_send_to( poll_send_to( send_to_host(
#                                      a UDP datagram with an explicit
#                                      destination
#   UdpSocket                          every UDP socket (word boundary;
#       SharedUdpSocket, the demux wrapper, is excluded: its sends record in
#       Sender::send_to) is documented for where its outbound is recorded
#   HttpConnector, GaiResolver         hyper's dial machinery: a connector
#       built outside the guarded resolver (or on hyper's own GaiResolver)
#       would dial unrecorded, so its construction must be recorded or
#       explained
#
# Limits, documented: a lookup_host renamed on import (`as foo`) is not
# trackable at its call site — the import line is a hit, and its reason
# comment must name where the lookup records. A bare .send( on a connected
# UDP socket is not a hit of its own: the connecting .connect( and the
# UdpSocket type are, so no such socket can appear without an audited line.
#
# The scan covers $1 (default: crates), */src/*.rs: not fbt-api (the server
# stack) and not fbt-example, plus the crates named in CHECK_OUTBOUND_SKIP
# (comma-separated; fbt-session while its outbound is not audited yet). In
# each file the scan stops at the first inline #[cfg(test)] module — the
# attribute line followed (blank lines aside) by `mod <name> {`; an
# out-of-line declaration (`mod <name>;`) does not stop it, and a file that
# is one (a src/**/tests.rs module) is scanned whole.
#
# Prints `check-outbound: ok`, or each offending file:line and exits 1.
set -uo pipefail

root="${1:-crates}"
skip="${CHECK_OUTBOUND_SKIP:-}"
bad=0

while IFS= read -r file; do
    rest="${file#"$root"/}"
    crate="${rest%%/*}"
    case ",${skip}," in
        *",${crate},"*) continue ;;
    esac
    awk -v fname="$file" '
        { lines[FNR] = $0 }
        END {
            for (i = 1; i <= FNR; i++) {
                if (stop) break
                l = lines[i]
                # Only an inline #[cfg(test)] module ends the scan; the
                # attribute alone on an import or helper does not, and
                # neither does an out-of-line declaration (mod <name>;).
                if (l ~ /^[[:space:]]*#\[cfg\(test\)\][[:space:]]*$/) {
                    j = i + 1
                    while (j <= FNR && lines[j] ~ /^[[:space:]]*$/) j++
                    if (j <= FNR &&
                        lines[j] ~ /^[[:space:]]*mod[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\{/) {
                        stop = 1
                        continue
                    }
                }
                if (l !~ /TcpStream::connect/ &&
                    l !~ /\.connect\(/ &&
                    l !~ /lookup_host/ &&
                    l !~ /to_socket_addrs/ &&
                    l !~ /ToSocketAddrs/ &&
                    l !~ /\.send_to\(/ &&
                    l !~ /try_send_to\(/ &&
                    l !~ /poll_send_to\(/ &&
                    l !~ /send_to_host\(/ &&
                    l !~ /HttpConnector/ &&
                    l !~ /GaiResolver/ &&
                    l !~ /(^|[^A-Za-z0-9_])UdpSocket/) continue
                ok = 0
                # A reason comment must carry a reason.
                if (i >= 2 && lines[i - 1] ~ /\/\/ audit:[[:space:]]*[^[:space:]]/) ok = 1
                for (k = i - 3; k <= i - 1 && !ok; k++)
                    if (k >= 1 && lines[k] ~ /audit::record/) ok = 1
                if (!ok) { print fname ":" i; bad = 1 }
            }
            exit bad + 0
        }
    ' "$file" || bad=1
done < <(find "$root" -path '*/src/*.rs' -not -path "$root/fbt-api/*" -not -path "$root/fbt-example/*" | sort)

if [ "$bad" -ne 0 ]; then
    echo "check-outbound: each line above needs audit::record within the 3 lines above, or a // audit: reason comment on the line above" >&2
    exit 1
fi
echo "check-outbound: ok"
