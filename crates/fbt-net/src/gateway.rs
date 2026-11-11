//! Default IPv4 gateway discovery without FFI: `/proc/net/route` on Linux,
//! the `route` command elsewhere.
//!
//! The command runs from an absolute path (never through a shell or `PATH`),
//! for at most [`COMMAND_TIMEOUT`], and only its first [`MAX_OUTPUT`] bytes
//! are read. Its output is treated as untrusted text: the parsers never
//! panic.

use std::net::Ipv4Addr;
#[cfg(not(target_os = "linux"))]
use std::path::PathBuf;
use std::time::Duration;

/// Longest a gateway lookup (file read or command) may take.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
/// Most bytes of the route table or command output read.
pub const MAX_OUTPUT: usize = 64 * 1024;

/// The default gateway from Linux's `/proc/net/route`: the line whose
/// destination is `00000000`, with its gateway in little-endian hex.
pub fn parse_proc_net_route(s: &str) -> Option<Ipv4Addr> {
    s.lines().skip(1).find_map(|line| {
        let mut f = line.split_whitespace();
        let (_iface, dest, gw, flags) = (f.next()?, f.next()?, f.next()?, f.next()?);
        if dest != "00000000" || gw.len() != 8 {
            return None;
        }
        // RTF_UP (1) and RTF_GATEWAY (2).
        let flags = u32::from_str_radix(flags, 16).ok()?;
        if flags & 0x3 != 0x3 {
            return None;
        }
        let raw = u32::from_str_radix(gw, 16).ok()?;
        let ip = Ipv4Addr::from(raw.to_le_bytes());
        usable(ip)
    })
}

/// The default gateway from macOS/BSD `route -n get default`: the
/// `gateway:` line.
pub fn parse_route_get_default(s: &str) -> Option<Ipv4Addr> {
    s.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("gateway:")?;
        usable(rest.trim().parse().ok()?)
    })
}

/// The default gateway from Windows `route print`: the first
/// `0.0.0.0 0.0.0.0 <gateway> ...` row whose gateway is an address (not
/// `On-link`).
pub fn parse_route_print(s: &str) -> Option<Ipv4Addr> {
    s.lines().find_map(|line| {
        let mut f = line.split_whitespace();
        if f.next()? != "0.0.0.0" || f.next()? != "0.0.0.0" {
            return None;
        }
        usable(f.next()?.parse().ok()?)
    })
}

/// A gateway must be a unicast address.
fn usable(ip: Ipv4Addr) -> Option<Ipv4Addr> {
    (!ip.is_unspecified() && !ip.is_broadcast() && !ip.is_multicast()).then_some(ip)
}

/// The host's default IPv4 gateway, `None` when it cannot be found within
/// [`COMMAND_TIMEOUT`].
pub async fn default_gateway_v4() -> Option<Ipv4Addr> {
    #[cfg(target_os = "linux")]
    {
        let text = read_capped("/proc/net/route").await?;
        parse_proc_net_route(&text)
    }
    #[cfg(windows)]
    {
        let root = std::env::var_os("SystemRoot").map(PathBuf::from)?;
        if !root.is_absolute() {
            return None;
        }
        let exe = root.join("System32").join("route.exe");
        let text = run_capped(exe, &["print", "-4", "0.0.0.0"]).await?;
        parse_route_print(&text)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let exe = ["/sbin/route", "/usr/sbin/route"]
            .into_iter()
            .map(PathBuf::from)
            .find(|p| p.is_file())?;
        let text = run_capped(exe, &["-n", "get", "default"]).await?;
        parse_route_get_default(&text)
    }
}

/// Reads at most [`MAX_OUTPUT`] bytes of `path` off the runtime, within
/// [`COMMAND_TIMEOUT`].
#[cfg(target_os = "linux")]
async fn read_capped(path: &'static str) -> Option<String> {
    use std::io::Read;
    let job = tokio::task::spawn_blocking(move || {
        let mut buf = Vec::new();
        std::fs::File::open(path)
            .ok()?
            .take(MAX_OUTPUT as u64)
            .read_to_end(&mut buf)
            .ok()?;
        Some(String::from_utf8_lossy(&buf).into_owned())
    });
    tokio::time::timeout(COMMAND_TIMEOUT, job)
        .await
        .ok()?
        .ok()?
}

/// Runs `exe` (an absolute path; no shell) with `args` and returns the first
/// [`MAX_OUTPUT`] bytes of its standard output. The child is killed once
/// that is read or after [`COMMAND_TIMEOUT`], whichever comes first.
#[cfg(not(target_os = "linux"))]
async fn run_capped(exe: PathBuf, args: &'static [&'static str]) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Mutex};

    if !exe.is_absolute() {
        return None;
    }
    let child = Command::new(&exe)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let child = Arc::new(Mutex::new(child));
    let stdout = child.lock().ok()?.stdout.take()?;
    let reader = tokio::task::spawn_blocking(move || {
        let mut buf = Vec::new();
        // An error mid-read keeps what arrived; the parser decides.
        let _ = stdout.take(MAX_OUTPUT as u64).read_to_end(&mut buf);
        buf
    });
    let out = tokio::time::timeout(COMMAND_TIMEOUT, reader).await;
    // Killing closes the pipe, which also ends a reader still blocked on it;
    // reaping happens off the runtime.
    tokio::task::spawn_blocking(move || {
        if let Ok(mut c) = child.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
    });
    let bytes = out.ok()?.ok()?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const PROC: &str = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
        eth0\t0000A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0\n\
        eth0\t00000000\t0101A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0\n";

    const ROUTE_GET: &str = "   route to: default\n\
        destination: default\n\
               mask: default\n\
            gateway: 192.168.1.1\n\
          interface: en0\n\
              flags: <UP,GATEWAY,DONE,STATIC,PRCLONING,GLOBAL>\n \
        recvpipe  sendpipe  ssthresh  rtt,msec    rttvar  hopcount      mtu     expire\n \
              0         0         0         0         0         0      1500         0\n";

    const ROUTE_PRINT: &str = "===========================================================================\n\
        Interface List\n \
         12...00 15 5d 01 02 03 ......Hyper-V Virtual Ethernet Adapter\n\
        ===========================================================================\n\
        \n\
        IPv4 Route Table\n\
        ===========================================================================\n\
        Active Routes:\n\
        Network Destination        Netmask          Gateway       Interface  Metric\n          \
        0.0.0.0          0.0.0.0         On-link      10.8.0.2      5\n          \
        0.0.0.0          0.0.0.0      192.168.1.1    192.168.1.100     25\n        \
        127.0.0.0        255.0.0.0         On-link         127.0.0.1    331\n\
        ===========================================================================\n\
        Persistent Routes:\n  \
        None\n";

    #[test]
    fn proc_net_route_sample() {
        assert_eq!(
            parse_proc_net_route(PROC),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
    }

    #[test]
    fn route_get_default_sample() {
        assert_eq!(
            parse_route_get_default(ROUTE_GET),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
        // A default route over an interface has no address.
        assert_eq!(parse_route_get_default("    gateway: link#5\n"), None);
    }

    #[test]
    fn route_print_sample() {
        assert_eq!(
            parse_route_print(ROUTE_PRINT),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
    }

    proptest! {
        #[test]
        fn garbage_yields_none(s in "\\PC{0,400}") {
            // Arbitrary text never panics; text without a default route
            // yields nothing.
            let _ = (parse_proc_net_route(&s), parse_route_get_default(&s), parse_route_print(&s));
            let junk = format!("x{s}").replace(['\n', '\r'], " ");
            prop_assert_eq!(parse_proc_net_route(&junk), None);
            prop_assert_eq!(parse_route_get_default(&junk), None);
            prop_assert_eq!(parse_route_print(&junk), None);
        }
    }

    #[test]
    fn hostile_samples_yield_none() {
        for s in [
            "",
            "Iface\n",
            "Iface Destination\neth0 00000000 zzzzzzzz 0003\n",
            "Iface Destination\neth0 00000000 00000000 0003\n",
            "Iface Destination\neth0 00000000 0101A8C0 0001\n",
            "gateway:",
            "gateway: 999.1.1.1",
            "0.0.0.0 0.0.0.0",
            "0.0.0.0 0.0.0.0 0.0.0.0",
            "0.0.0.0 0.0.0.0 255.255.255.255",
        ] {
            assert_eq!(parse_proc_net_route(s), None, "{s:?}");
            assert_eq!(parse_route_get_default(s), None, "{s:?}");
            assert_eq!(parse_route_print(s), None, "{s:?}");
        }
    }
}
