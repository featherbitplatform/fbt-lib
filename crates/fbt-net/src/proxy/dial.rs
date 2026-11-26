//! The dial to the proxy itself when its host is a name: the addresses it
//! resolved to are tried with RFC 8305 staggered attempts, so a first address
//! that is dead or refuses slowly (a Windows `localhost` resolving to `::1`
//! before an IPv4-only proxy, for one) does not spend the whole budget before
//! the next one is tried. This mirrors the stagger `fbt-tracker`'s connector
//! uses for direct HTTP connections, which `fbt-net` cannot depend on.
//!
//! The proxy's own name is recorded once as the `Lookup` a proxied session
//! makes ([`resolve_proxy`]), and each attempt dials an address that name
//! already resolved to, so the attempts carry no record of their own.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use fbt_core::audit;
use tokio::net::TcpStream;

/// How long after an attempt starts the next address is tried (RFC 8305
/// section 3, the same 250 ms `fbt-tracker`'s connector uses), unless the
/// earlier attempt has already failed.
const STAGGER: Duration = Duration::from_millis(250);

/// Resolves the proxy's own name, the one lookup a proxied session makes. The
/// record is written here, in the same step as the lookup, so the audit sees
/// it.
pub(crate) async fn resolve_proxy(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    audit::record(audit::Outbound::Lookup(host.to_owned()));
    Ok(tokio::net::lookup_host((host, port)).await?.collect())
}

/// Dials the proxy's resolved addresses with staggered attempts and returns
/// the first to connect, dropping the rest. The caller bounds the whole call
/// by the deadline it has left, so no attempt carries its own timeout.
pub(crate) async fn dial_addrs(addrs: &[SocketAddr]) -> io::Result<TcpStream> {
    let order = interleaved(addrs);
    if order.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no address to dial",
        ));
    }
    staggered(order.into()).await
}

/// One attempt in flight.
type Dial = Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>;

/// Starts one attempt on `addr`. A test can hold it open with
/// [`hang_connect`], the position a blackholed route holds an attempt in
/// production, so the next address is proved to be tried without waiting on a
/// real timeout.
fn connect_attempt(addr: SocketAddr) -> Dial {
    Box::pin(async move {
        // (tests) A connect a test asked to hold open, before any dial.
        #[cfg(test)]
        if let Some(gate) = take_hang_gate(addr) {
            gate.park().await;
            return Err(io::Error::other("hung connect released"));
        }
        // audit: the proxy's own name is the Lookup recorded before resolving
        TcpStream::connect(addr).await
    })
}

/// The addresses in the order the attempts are started: the families
/// interleaved, beginning with the family of the resolver's first answer
/// (RFC 8305 section 4).
fn interleaved(addrs: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut v6: VecDeque<SocketAddr> = VecDeque::new();
    let mut v4: VecDeque<SocketAddr> = VecDeque::new();
    for addr in addrs {
        if addr.is_ipv6() {
            v6.push_back(*addr);
        } else {
            v4.push_back(*addr);
        }
    }
    // The first answer's family leads; the other follows on the next attempt.
    let (mut lead, mut follow) = if addrs.first().is_some_and(SocketAddr::is_ipv6) {
        (v6, v4)
    } else {
        (v4, v6)
    };
    let mut out = Vec::with_capacity(addrs.len());
    loop {
        if let Some(addr) = lead.pop_front() {
            out.push(addr);
        }
        if let Some(addr) = follow.pop_front() {
            out.push(addr);
        }
        if lead.is_empty() && follow.is_empty() {
            return out;
        }
    }
}

/// Starts the attempts staggered (RFC 8305), keeping the earlier ones in
/// flight, and returns the first to connect. The first attempt starts at once;
/// each next one a stagger later, or as soon as the previous one fails.
async fn staggered(mut pending: VecDeque<SocketAddr>) -> io::Result<TcpStream> {
    let mut in_flight: Vec<Dial> = Vec::new();
    let mut last: Option<io::Error> = None;
    let mut next = Some(tokio::time::Instant::now());
    let mut timer = Box::pin(tokio::time::sleep_until(tokio::time::Instant::now()));

    std::future::poll_fn(|cx| {
        loop {
            // Start every attempt whose turn has come. One at a time: after
            // each start the next one is a stagger away.
            while let Some(deadline) = next {
                if pending.is_empty() {
                    next = None;
                    break;
                }
                if deadline > tokio::time::Instant::now() {
                    break;
                }
                if let Some(addr) = pending.pop_front() {
                    in_flight.push(connect_attempt(addr));
                    next = Some(tokio::time::Instant::now() + STAGGER);
                }
            }
            // Poll what is in flight: the first to connect wins and the rest
            // are dropped with the winner.
            if !in_flight.is_empty() {
                let mut kept: Vec<Dial> = Vec::with_capacity(in_flight.len());
                for mut attempt in in_flight.drain(..) {
                    match attempt.as_mut().poll(cx) {
                        Poll::Ready(Ok(stream)) => return Poll::Ready(Ok(stream)),
                        Poll::Ready(Err(e)) => {
                            last = Some(e);
                            if !pending.is_empty() {
                                // The previous attempt failed: the next one may
                                // start at once.
                                next = Some(tokio::time::Instant::now());
                            }
                        }
                        Poll::Pending => kept.push(attempt),
                    }
                }
                in_flight = kept;
            }
            if in_flight.is_empty() && pending.is_empty() {
                return Poll::Ready(Err(last.take().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "no address to dial")
                })));
            }
            // Wait for an attempt to finish, or for the next one's turn.
            if let Some(deadline) = next {
                timer.as_mut().reset(deadline);
                if timer.as_mut().poll(cx).is_ready() {
                    continue;
                }
            }
            return Poll::Pending;
        }
    })
    .await
}

/// (tests) Connects a test has asked to hold open, keyed by address: the
/// attempt parks at it instead of dialling, the position a blackholed route
/// holds an attempt in production, so a test can prove the next address is
/// tried without waiting on a real timeout.
#[cfg(test)]
static HANG_GATES: std::sync::Mutex<Vec<(SocketAddr, DialGate)>> =
    std::sync::Mutex::new(Vec::new());

/// (tests) A dial that parks until a test releases it: see [`hang_connect`].
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct DialGate {
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl DialGate {
    /// A gate that is closed: the dial registered for it parks until
    /// [`release`](Self::release).
    pub(crate) fn new() -> DialGate {
        DialGate {
            release: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Lets the parked dial (or a later one) go on. `notify_one` keeps a
    /// permit, so a release that comes before the dial parks is not lost.
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }

    /// Parks until the gate is released (or the gate is dropped).
    async fn park(&self) {
        self.release.notified().await;
    }
}

/// (tests) Registers a gate for the next connect to `addr` and returns it: the
/// attempt parks at it until [`DialGate::release`], inside the dial, not
/// before it, so a test can hold one attempt of a dial open while the next one
/// is tried.
#[cfg(test)]
pub(crate) fn hang_connect(addr: SocketAddr) -> DialGate {
    let gate = DialGate::new();
    HANG_GATES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((addr, gate.clone()));
    gate
}

/// (tests) Takes the gate for `addr`, if one is registered: `Some` lets the
/// attempt park at it, `None` is a no-op.
#[cfg(test)]
fn take_hang_gate(addr: SocketAddr) -> Option<DialGate> {
    let mut gates = HANG_GATES.lock().unwrap_or_else(|e| e.into_inner());
    gates
        .iter()
        .position(|(a, _)| *a == addr)
        .map(|i| gates.remove(i).1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::time::Instant;

    fn v4(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    fn v6(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv6Addr::LOCALHOST, port))
    }

    /// The families are interleaved, beginning with the family of the
    /// resolver's first answer (RFC 8305 section 4).
    #[test]
    fn families_are_interleaved_from_the_first_answer() {
        let (a, b) = (v6(1), v6(2));
        let (c, d) = (v4(3), v4(4));
        assert_eq!(interleaved(&[a, b, c, d]), vec![a, c, b, d]);
        assert_eq!(interleaved(&[c, d, a, b]), vec![c, a, d, b]);
        // One family only, and the empty list.
        assert_eq!(interleaved(&[a, b]), vec![a, b]);
        assert_eq!(interleaved(&[c, d]), vec![c, d]);
        assert!(interleaved(&[]).is_empty());
    }

    /// A first address whose connect hangs (a blackholed route, or a closed
    /// IPv6 port on Windows) does not stop the second from being tried a
    /// stagger later: the dial succeeds well inside a 2 s deadline the hang
    /// alone would have spent.
    #[tokio::test]
    async fn a_hung_first_address_does_not_stop_the_second() {
        let listener = tokio::net::TcpListener::bind(v4(0)).await.unwrap();
        let live = listener.local_addr().unwrap();
        // A first address the resolver answered with: loopback, nothing
        // listens, and the gate parks the attempt before any dial.
        let hung = v4(1);
        let gate = hang_connect(hung);

        let started = Instant::now();
        let dialled = tokio::time::timeout(Duration::from_secs(2), dial_addrs(&[hung, live]))
            .await
            .expect("the dial did not finish inside 2 s")
            .expect("the dial failed");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the dial spent the whole deadline"
        );
        drop(dialled);
        gate.release();
    }
}
