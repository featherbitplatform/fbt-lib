//! The TCP connector under the HTTP clients: it opens every direct connection itself, refusing
//! an address inside the local network before dialling it, or tunnels through a SOCKS5 or HTTP
//! CONNECT proxy.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use fbt_core::audit;
use fbt_net::addr::is_internal;
use fbt_net::proxy::{ProxyConfig, ProxyKind, ProxyTarget, connect_tcp};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Connecting = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, BoxError>> + Send>>;

/// The most answers one guarded lookup judges: a name answering with more than that fails
/// the request instead of letting the rest go unchecked, so the check stays bounded however
/// many addresses a hostile name hands back.
const MAX_GUARDED_ADDRESSES: usize = 64;

/// Whether the stack this connector belongs to has been replaced.
///
/// hyper-util's legacy connection pool finishes a losing connect in a background task the
/// session's own handles cannot abort (N3): a request that was already in flight when its stack
/// was torn down can still be polled afterwards, and would then resolve a name and dial the
/// address itself — directly, past whatever proxy replaced the stack. The connector reads this
/// flag immediately before it resolves anything, and again immediately before **every single
/// dial** it makes (N1): a name can answer with several addresses and a dial can be attempted
/// one address after another, so a check made once — before the first of them — leaves every
/// later dial to run on the replaced stack. It is created once per client and shared by every
/// connector that client builds.
#[derive(Clone, Debug, Default)]
pub(crate) struct Closed(Arc<AtomicBool>);

impl Closed {
    pub(crate) fn new() -> Closed {
        Closed(Arc::new(AtomicBool::new(false)))
    }

    /// Marks the stack closed: nothing else on its connectors may resolve or dial.
    pub(crate) fn set(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// The error a request refused on a replaced stack ends with.
    pub(crate) fn refuse(&self) -> BoxError {
        io::Error::new(
            io::ErrorKind::NotConnected,
            "the stack this request belongs to was replaced",
        )
        .into()
    }
}

/// A host refused because it answers with an address inside the local network. It names no
/// address on purpose: what surfaces is [`TrackerError::InternalRedirect`](crate::TrackerError::InternalRedirect),
/// which reports the refusal without publishing where the user's own network was reached.
#[derive(Debug)]
pub(crate) struct Refused;

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("resolves inside the local network")
    }
}

impl std::error::Error for Refused {}

/// Where a name is looked up: the system resolver, or (in tests) canned answers.
#[derive(Clone, Debug)]
pub(crate) enum Resolution {
    /// `getaddrinfo` on tokio's blocking pool, which is where hyper's own resolver runs it.
    System,
    /// A test's queued answers: see [`StubResolver`].
    #[cfg(test)]
    Stub(Arc<StubState>),
}

impl Resolution {
    /// The addresses `host` answers with on `port`. With `deny_internal` every one is judged
    /// first and any inside the local network refuses the whole target: what comes back is
    /// what the caller dials, so nothing unchecked can ride along behind it. A lookup that
    /// fails, times out or answers with nothing is an error too: under a policy that denies
    /// internal targets an unanswered name is not dialled on trust.
    async fn resolve(
        &self,
        deny_internal: bool,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>, BoxError> {
        #[cfg(test)]
        if let Resolution::Stub(state) = self {
            return match state.take(host) {
                Answer::Addresses(addrs) => judge_answers(deny_internal, addrs, host),
                Answer::Gated(addrs, gate) => {
                    // The name is already in `calls()` (the answer was taken first), so a test
                    // can see the lookup is in flight and then release it.
                    gate.notified().await;
                    judge_answers(deny_internal, addrs, host)
                }
                Answer::NotFound => Err(stub_not_found(host)),
                Answer::Silence => {
                    std::future::pending::<()>().await;
                    // Never reached: only the request's own timeout ends a silent lookup.
                    Err(stub_not_found(host))
                }
            };
        }
        audit::record(audit::Outbound::Lookup(host.to_owned()));
        let found = tokio::net::lookup_host((host, port)).await?;
        if !deny_internal {
            // Nothing is judged: every answer stays dialable, as it always was.
            return Ok(found.collect());
        }
        let addrs: Vec<SocketAddr> = found.take(MAX_GUARDED_ADDRESSES + 1).collect();
        judge_addresses(deny_internal, addrs, host)
    }
}

/// Answers become the addresses to dial, judged first when nothing unchecked may be dialled:
/// more than [`MAX_GUARDED_ADDRESSES`] cannot be and fails closed, no answer at all fails too,
/// and any address inside the local network refuses the whole target rather than being skipped.
fn judge_addresses(
    deny_internal: bool,
    addrs: Vec<SocketAddr>,
    host: &str,
) -> Result<Vec<SocketAddr>, BoxError> {
    if !deny_internal {
        return Ok(addrs);
    }
    if addrs.len() > MAX_GUARDED_ADDRESSES {
        return Err(io::Error::other(format!(
            "{host} answered with more than {MAX_GUARDED_ADDRESSES} addresses"
        ))
        .into());
    }
    if addrs.is_empty() {
        // Nothing to dial: an unanswered name is not trusted past the guard either.
        return Err(
            io::Error::new(io::ErrorKind::NotFound, format!("{host} did not resolve")).into(),
        );
    }
    if addrs.iter().any(|a| is_internal(a.ip())) {
        return Err(Box::new(Refused));
    }
    Ok(addrs)
}

/// The same, for a stub's addresses, which carry no port yet (the connector puts its own on).
#[cfg(test)]
fn judge_answers(
    deny_internal: bool,
    addrs: Vec<IpAddr>,
    host: &str,
) -> Result<Vec<SocketAddr>, BoxError> {
    judge_addresses(
        deny_internal,
        addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect(),
        host,
    )
}

/// A stubbed lookup that ran out of queued answers.
#[cfg(test)]
fn stub_not_found(host: &str) -> BoxError {
    io::Error::new(io::ErrorKind::NotFound, format!("{host} did not resolve")).into()
}

/// Opens the TCP connection for one request URI.
#[derive(Debug, Clone)]
pub(crate) enum ProxyConnector {
    /// Dialled here, one address at a time: this connector opens every direct connection
    /// itself, so the check that a replaced stack dials nothing stands before each connect
    /// (N1) instead of being handed to a dialler that would dial on unchecked.
    Direct {
        /// Where a name is looked up (the stub of it, in tests).
        where_to_look: Resolution,
        /// Whether an answer inside the local network refuses the target.
        deny_internal: bool,
        /// Bound on each dial attempt (the client's HTTP timeout).
        timeout: Duration,
        /// Read before the lookup and before every dial: a replaced stack dials nothing (N1).
        closed: Closed,
    },
    /// Through `proxy`, the whole exchange bounded by `timeout`. With `guard`, a host name is
    /// resolved and judged here when the proxy would not resolve it, so the address the proxy
    /// is asked to dial is one that was checked; with `proxy_hostnames` the proxy resolves
    /// names and decides where they go, and nothing is looked up locally.
    Proxied {
        /// The proxy to tunnel through.
        proxy: ProxyConfig,
        /// Bound on dialling the proxy and the tunnel negotiation.
        timeout: Duration,
        /// Whether an address inside the local network is refused.
        guard: bool,
        /// Where a guarded lookup runs (the stub of it, in tests).
        where_to_look: Resolution,
        /// Read before any record or dial: a replaced stack resolves and dials nothing (N3).
        closed: Closed,
    },
}

impl ProxyConnector {
    /// A direct connector, or one through `proxy` when it names a proxy. `deny_internal`:
    /// this connector dials nothing inside the local network; `where_to_look` says where
    /// host names are looked up (only tests pass anything but the system resolver). `closed`
    /// is set when the stack this connector belongs to is replaced, after which it refuses to
    /// resolve or dial anything (N3).
    pub(crate) fn new(
        proxy: Option<&ProxyConfig>,
        timeout: Duration,
        deny_internal: bool,
        where_to_look: Resolution,
        closed: Closed,
    ) -> ProxyConnector {
        match proxy {
            Some(p) if p.kind != ProxyKind::None => ProxyConnector::Proxied {
                proxy: p.clone(),
                timeout,
                guard: deny_internal,
                where_to_look,
                closed,
            },
            _ => ProxyConnector::Direct {
                where_to_look,
                deny_internal,
                timeout,
                closed,
            },
        }
    }
}

/// A URI's host without the brackets around an IPv6 literal, as a resolver wants it.
pub(crate) fn without_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// The port of `uri`, else the default of its scheme (443 for `https`, 80 otherwise).
pub(crate) fn port_of(uri: &Uri) -> u16 {
    uri.port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        })
}

impl tower_service::Service<Uri> for ProxyConnector {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future = Connecting;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Both arms dial on demand: there is nothing to reserve ahead of a request.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match self {
            ProxyConnector::Direct {
                where_to_look,
                deny_internal,
                timeout,
                closed,
            } => {
                let (where_to_look, deny_internal, timeout, closed) = (
                    where_to_look.clone(),
                    *deny_internal,
                    *timeout,
                    closed.clone(),
                );
                Box::pin(async move {
                    // Read when this future is polled, which for a connect the pool started
                    // but a later stack won can be after the replacement (N3): a replaced
                    // stack must not record a connect nor dial one.
                    if closed.is_set() {
                        return Err(closed.refuse());
                    }
                    let host = uri.host().ok_or("request URI has no host")?;
                    let port = port_of(&uri);
                    let bare = without_brackets(host);
                    // An IP literal is dialled as it stands (a literal target was judged
                    // before the hop: see `RedirectPolicy`); a name is resolved here — judged
                    // whole when `deny_internal` — and every answer dialled by this connector
                    // itself, so nothing is handed to a dialler that would connect without a
                    // check of its own (N1).
                    let mut addrs = match bare.parse::<IpAddr>() {
                        Ok(ip) => vec![SocketAddr::new(ip, port)],
                        Err(_) => where_to_look.resolve(deny_internal, bare, port).await?,
                    };
                    if addrs.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("{host} did not resolve"),
                        )
                        .into());
                    }
                    // A stub's answers carry no port yet: the URI's goes on them, where a
                    // dialler used to put it back after asking the resolver for the host
                    // alone.
                    for addr in &mut addrs {
                        if addr.port() == 0 {
                            addr.set_port(port);
                        }
                    }
                    dial_direct(&addrs, timeout, &closed).await
                })
            }
            ProxyConnector::Proxied {
                proxy,
                timeout,
                guard,
                where_to_look,
                closed,
            } => {
                let (proxy, timeout, guard, where_to_look, closed) = (
                    proxy.clone(),
                    *timeout,
                    *guard,
                    where_to_look.clone(),
                    closed.clone(),
                );
                Box::pin(async move {
                    // As in the direct arm: refuse before any lookup or dial when the stack
                    // this request belongs to has been replaced (N3).
                    if closed.is_set() {
                        return Err(closed.refuse());
                    }
                    let host = uri.host().ok_or("request URI has no host")?;
                    let port = port_of(&uri);
                    // `connect_tcp` turns an IP literal into an address itself (and a literal
                    // target was judged before the hop: see `RedirectPolicy`). A name goes to
                    // the proxy as a name when the proxy resolves it; otherwise it is resolved
                    // here, so the address handed to the proxy is one that was judged.
                    let target = match (guard, proxy.proxy_hostnames) {
                        (true, false) if host.parse::<IpAddr>().is_err() => {
                            let addrs = where_to_look
                                .resolve(true, without_brackets(host), port)
                                .await?;
                            let first = addrs.first().copied().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotFound,
                                    format!("{host} did not resolve"),
                                )
                            })?;
                            ProxyTarget::Addr(first)
                        }
                        _ => ProxyTarget::Host(host.to_owned(), port),
                    };
                    // The guarded lookup above takes time: refuse the proxy dial if the stack
                    // was replaced while it ran (N3), the same gap the direct arm's resolver
                    // closes between its lookup and the dial.
                    if closed.is_set() {
                        return Err(closed.refuse());
                    }
                    let stream = connect_tcp(&proxy, target, timeout).await?;
                    Ok(TokioIo::new(stream))
                })
            }
        }
    }
}

/// Dials `addrs`, staggering the attempts (RFC 8305) so that one family that blackholes does
/// not hold up the other, and bounded as a whole by `budget`.
///
/// This connector opens every direct connection itself, so the check that a replaced stack dials
/// nothing stands **immediately before each connect** (N1), in the same step as the dial and not
/// only before the first of them: a name can answer with several addresses, and a dialler that
/// resolved once and then connected to the answers itself would run the attempt after a failure,
/// or the one its own happy-eyeballs delay starts, without ever looking at the flag again. The
/// connect is recorded in the same step, so every direct dial — by name or by IP literal,
/// successful or not — is visible to the outbound audit.
///
/// The attempts are staggered rather than sequential: the next address is tried as soon as the
/// previous attempt fails or [`STAGGER`] after it started, whichever is first, and the earlier
/// attempts keep running. A dual-stack name whose IPv6 route blackholes is then reached over IPv4
/// instead of spending the whole request budget waiting out the dead family (R1). The first
/// attempt to connect wins and the rest are dropped; a single address behaves as before.
async fn dial_direct(
    addrs: &[SocketAddr],
    budget: Duration,
    closed: &Closed,
) -> Result<TokioIo<TcpStream>, BoxError> {
    let order = interleaved(addrs);
    if order.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "no address to dial").into());
    }
    // The whole dial shares the request's budget, so a slow first attempt does not stop the last
    // address from being tried; no attempt carries a timeout of its own.
    match tokio::time::timeout(budget, staggered(order, closed)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "dialling the resolved addresses timed out",
        )
        .into()),
    }
}

/// How long after an attempt starts the next address is tried (RFC 8305 §3), unless the earlier
/// attempt has already failed. The same 250 ms hyper's own connector used.
const STAGGER: Duration = Duration::from_millis(250);

/// One address's dial: it reads the flag, records the connect and dials, all in the same step
/// (N1).
type Dial = Pin<Box<dyn Future<Output = Result<TcpStream, BoxError>> + Send>>;

/// The addresses in the order the attempts are started: the families interleaved, beginning with
/// the family of the resolver's first answer (RFC 8305 §4).
fn interleaved(addrs: &[SocketAddr]) -> VecDeque<SocketAddr> {
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
    let mut out = VecDeque::with_capacity(addrs.len());
    loop {
        if let Some(addr) = lead.pop_front() {
            out.push_back(addr);
        }
        if let Some(addr) = follow.pop_front() {
            out.push_back(addr);
        }
        if lead.is_empty() && follow.is_empty() {
            return out;
        }
    }
}

/// Starts one attempt on `addr`: the flag is read, the connect recorded and the dial made in the
/// same step, so nothing can slip a replaced stack's dial between them (N1).
fn dial_one(addr: SocketAddr, closed: Closed) -> Dial {
    Box::pin(async move {
        // (tests) A dial a test holds open, *before* the flag is read so that a test's ordering
        // matches the production one: nothing stands between the check and the dial.
        #[cfg(test)]
        park_before_dial(addr).await;
        // Read in the same step as the dial below: a replacement that lands between two attempts
        // refuses the next one (N1).
        if closed.is_set() {
            return Err(closed.refuse());
        }
        // audit: recorded here, in the same step as the dial, so a direct dial by name or by
        // literal is visible to the outbound audit (a lookup records the name alone, and this
        // connector dials the answers itself).
        audit::record(audit::Outbound::TcpConnect(addr));
        // (tests) An attempt a test holds open *inside* the connect: it already passed its
        // closed check and recorded its connect, so a test can replace the stack while the
        // dial is in flight and see what the rest of the staggered attempts do (N1). The
        // release fails the attempt, as a blackholed route would end it.
        #[cfg(test)]
        if let Some(gate) = take_hang_gate(addr) {
            gate.park().await;
            return Err(io::Error::other("hung connect released").into());
        }
        // audit: recorded above, same step as the dial (N1); the test hook between them compiles out
        TcpStream::connect(addr).await.map_err(Into::into)
    })
}

/// Starts the attempts staggered (RFC 8305), keeping the earlier ones in flight, and returns the
/// first to connect. Each attempt is [`dial_one`], so every one of them reads the flag and records
/// its connect in the same step as the dial (N1): a replaced stack starts no further attempt, and
/// an attempt already in flight is refused when it reaches its own check.
async fn staggered(
    mut pending: VecDeque<SocketAddr>,
    closed: &Closed,
) -> Result<TokioIo<TcpStream>, BoxError> {
    let mut in_flight: Vec<Dial> = Vec::new();
    let mut last: Option<BoxError> = None;
    // The first attempt starts at once; each next one a stagger later (or as soon as the previous
    // one fails).
    let mut next = Some(tokio::time::Instant::now());
    let mut timer = Box::pin(tokio::time::sleep_until(tokio::time::Instant::now()));

    std::future::poll_fn(|cx| {
        loop {
            // Start every attempt whose turn has come. One at a time: after each start the next one
            // is a stagger away.
            while let Some(deadline) = next {
                if pending.is_empty() {
                    next = None;
                    break;
                }
                if deadline > tokio::time::Instant::now() {
                    break;
                }
                if let Some(addr) = pending.pop_front() {
                    in_flight.push(dial_one(addr, closed.clone()));
                    next = Some(tokio::time::Instant::now() + STAGGER);
                }
            }
            // Poll what is in flight: the first to connect wins and the rest are dropped with the
            // winner.
            if !in_flight.is_empty() {
                let mut kept: Vec<Dial> = Vec::with_capacity(in_flight.len());
                for mut attempt in in_flight.drain(..) {
                    match attempt.as_mut().poll(cx) {
                        Poll::Ready(Ok(stream)) => return Poll::Ready(Ok(TokioIo::new(stream))),
                        Poll::Ready(Err(e)) => {
                            last = Some(e);
                            if closed.is_set() {
                                // An attempt refused on a replaced stack: start no further ones (N1).
                                pending.clear();
                                next = None;
                            } else if !pending.is_empty() {
                                // The previous attempt failed: the next one may start at once.
                                next = Some(tokio::time::Instant::now());
                            }
                        }
                        Poll::Pending => kept.push(attempt),
                    }
                }
                in_flight = kept;
            }
            if in_flight.is_empty() && pending.is_empty() {
                return Poll::Ready(Err(match last.take() {
                    Some(e) => e,
                    None if closed.is_set() => closed.refuse(),
                    None => io::Error::new(io::ErrorKind::NotFound, "no address to dial").into(),
                }));
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

/// (tests) Dials a test has asked to hold open, keyed by address. Lets a multi-address dial be
/// held after its first attempt has started, so a test can replace the stack mid-dial and see
/// what the rest of the list does (N1) — without a packet leaving the machine and without
/// depending on a platform's listen-backlog behaviour.
#[cfg(test)]
static DIAL_GATES: Mutex<Vec<(SocketAddr, DialGate)>> = Mutex::new(Vec::new());

/// (tests) Connects a test has asked to hold open, keyed by address. Unlike the dial gates,
/// which park *before* the flag is read, a hang gate parks *after* it: the attempt has passed
/// its closed check and recorded its connect, and now sits inside a slow dial — the position
/// a blackholed route holds an attempt in production (N1).
#[cfg(test)]
static HANG_GATES: Mutex<Vec<(SocketAddr, DialGate)>> = Mutex::new(Vec::new());

/// (tests) The addresses whose dials have parked, in the order they parked: a test reads this to
/// see the order the attempts were started in, without depending on how often it polls.
#[cfg(test)]
static PARK_ORDER: Mutex<Vec<SocketAddr>> = Mutex::new(Vec::new());

/// (tests) A dial that parks until the test releases it: see [`gate_dial`].
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct DialGate {
    release: Arc<tokio::sync::Notify>,
    parked: Arc<AtomicUsize>,
}

#[cfg(test)]
impl DialGate {
    /// A gate that is closed: the dial registered for it parks until [`release`](Self::release).
    pub(crate) fn new() -> DialGate {
        DialGate {
            release: Arc::new(tokio::sync::Notify::new()),
            parked: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// How many dials are parked at this gate now.
    pub(crate) fn parked(&self) -> usize {
        self.parked.load(Ordering::SeqCst)
    }

    /// Lets the parked dial (or a later one) go on. `notify_one` keeps a permit, so a release
    /// that comes before the dial parks is not lost.
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }

    /// Parks until the gate is released (or the gate is dropped).
    async fn park(&self) {
        self.parked.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        self.parked.fetch_sub(1, Ordering::SeqCst);
    }
}

/// (tests) Registers a gate for the next dial to `addr` and returns it: the dial parks at it
/// until [`DialGate::release`], so a test can hold one attempt of a dial open.
#[cfg(test)]
pub(crate) fn gate_dial(addr: SocketAddr) -> DialGate {
    let gate = DialGate::new();
    DIAL_GATES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((addr, gate.clone()));
    gate
}

/// (tests) Registers a hang gate for the next connect to `addr` and returns it: the attempt
/// passes its closed check and records its connect, then parks at the gate until
/// [`DialGate::release`] — inside the dial, not before it — so a test can hold an attempt in
/// flight while the stack is replaced and see what the rest of the staggered dial does (N1).
/// The release fails the attempt rather than opening a socket, so no packet leaves the
/// machine (N2).
#[cfg(test)]
pub(crate) fn hang_connect(addr: SocketAddr) -> DialGate {
    let gate = DialGate::new();
    HANG_GATES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((addr, gate.clone()));
    gate
}

/// (tests) Takes the hang gate for `addr`, if one is registered: `Some` lets the attempt park
/// at it (it has already passed its closed check and recorded its connect), `None` is a no-op.
#[cfg(test)]
fn take_hang_gate(addr: SocketAddr) -> Option<DialGate> {
    let mut gates = HANG_GATES.lock().unwrap_or_else(|e| e.into_inner());
    gates
        .iter()
        .position(|(a, _)| *a == addr)
        .map(|i| gates.remove(i).1)
}

/// (tests) Parks the dial to `addr` at its gate, if one is registered; a no-op otherwise. The
/// address is logged in [`PARK_ORDER`] as it parks, so a test can read the order the attempts
/// were started in.
#[cfg(test)]
async fn park_before_dial(addr: SocketAddr) {
    let gate = {
        let mut gates = DIAL_GATES.lock().unwrap_or_else(|e| e.into_inner());
        match gates.iter().position(|(a, _)| *a == addr) {
            Some(i) => gates.remove(i).1,
            None => return,
        }
    };
    PARK_ORDER
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(addr);
    gate.park().await;
}

/// Canned answers to a connector's host lookups (tests): each lookup takes the next queued
/// answer and is recorded, so a test can say what was dialled without a resolver in the loop
/// and see how many lookups were made — including that none were.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct StubResolver(Arc<StubState>);

/// What the next lookup does.
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum Answer {
    /// These hosts answered (the connector puts its own port on them).
    Addresses(Vec<IpAddr>),
    /// The name did not resolve.
    NotFound,
    /// Nothing comes back: only the request's own timeout ends it.
    Silence,
    /// These hosts answered, but only once the gate fires: a test can prove the lookup is in
    /// flight and release it after a replacement.
    Gated(Vec<IpAddr>, Arc<tokio::sync::Notify>),
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct StubState {
    /// Answers still queued, oldest lookup first.
    answers: Mutex<VecDeque<Answer>>,
    /// The names looked up, in order.
    looked_up: Mutex<Vec<String>>,
}

#[cfg(test)]
impl StubState {
    /// Records the lookup and hands out its answer; a queue with nothing left fails the name.
    fn take(&self, host: &str) -> Answer {
        let next = self
            .answers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front();
        self.looked_up
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(host.to_owned());
        next.unwrap_or(Answer::NotFound)
    }
}

#[cfg(test)]
impl StubResolver {
    /// Queues `addrs` as the answer of the next lookup.
    pub(crate) fn push(&self, addrs: &[&str]) -> &StubResolver {
        let addrs = addrs
            .iter()
            .map(|s| s.parse::<IpAddr>().expect("stub address"))
            .collect();
        self.locked().push_back(Answer::Addresses(addrs));
        self
    }

    /// The next lookup fails like an unresolvable name.
    pub(crate) fn fail_next(&self) -> &StubResolver {
        self.locked().push_back(Answer::NotFound);
        self
    }

    /// The next lookup never answers.
    pub(crate) fn hang_next(&self) -> &StubResolver {
        self.locked().push_back(Answer::Silence);
        self
    }

    /// Queues `addrs` as the next lookup's answer, but the lookup parks until the returned
    /// `Notify` fires: a test can prove a lookup is in flight (the name is in
    /// [`calls`](Self::calls) as soon as it is taken) and release it later, after replacing
    /// the stack the lookup belongs to.
    pub(crate) fn push_gated(&self, addrs: &[&str]) -> Arc<tokio::sync::Notify> {
        let addrs = addrs
            .iter()
            .map(|s| s.parse::<IpAddr>().expect("stub address"))
            .collect();
        let gate = Arc::new(tokio::sync::Notify::new());
        self.locked()
            .push_back(Answer::Gated(addrs, Arc::clone(&gate)));
        gate
    }

    /// The names looked up, in order: empty means nothing was resolved locally at all.
    pub(crate) fn calls(&self) -> Vec<String> {
        self.0
            .looked_up
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// A resolver configuration that answers from this stub.
    pub(crate) fn resolution(&self) -> Resolution {
        Resolution::Stub(Arc::clone(&self.0))
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, VecDeque<Answer>> {
        self.0.answers.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_service::Service;

    #[test]
    fn ports_default_by_scheme() {
        let p = |s: &str| port_of(&s.parse::<Uri>().unwrap());
        assert_eq!(p("http://t.example/announce"), 80);
        assert_eq!(p("https://t.example/announce"), 443);
        assert_eq!(p("https://t.example:8443/announce"), 8443);
        assert_eq!(p("http://[::1]:6969/announce"), 6969);
    }

    /// A guarded lookup judges every answer it is given, and one internal address refuses the
    /// whole target rather than being skipped over. Unguarded, nothing is judged.
    #[tokio::test]
    async fn a_guarded_lookup_refuses_any_internal_answer() {
        let stub = StubResolver::default();
        stub.push(&["93.184.216.34", "10.0.0.1"]);
        let err = Resolution::Stub(stub.0.clone())
            .resolve(true, "mixed.example", 80)
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<Refused>().is_some(),
            "an internal answer among the public ones refuses the target: {err:?}"
        );

        // The same answers, unguarded: all of them stay dialable.
        stub.push(&["93.184.216.34", "10.0.0.1"]);
        let addrs = Resolution::Stub(stub.0.clone())
            .resolve(false, "mixed.example", 80)
            .await
            .unwrap();
        assert_eq!(addrs.len(), 2, "nothing was dropped or judged");
    }

    /// An answer set larger than the guard judges fails closed, so the check stays bounded
    /// however many addresses a name hands back.
    #[tokio::test]
    async fn too_many_answers_to_judge_fail_closed() {
        let stub = StubResolver::default();
        let addrs: Vec<String> = (0..=MAX_GUARDED_ADDRESSES)
            .map(|i| format!("93.184.216.{i}"))
            .collect();
        let queued: Vec<&str> = addrs.iter().map(String::as_str).collect();
        stub.push(&queued);
        let err = Resolution::Stub(stub.0.clone())
            .resolve(true, "many.example", 80)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("more than {MAX_GUARDED_ADDRESSES} addresses")),
            "{err:?}"
        );
    }

    /// A lookup that fails or never answers is an error under a policy that denies internal
    /// targets: nothing is dialled on trust.
    #[tokio::test]
    async fn a_lookup_that_cannot_answer_is_not_dialled() {
        let stub = StubResolver::default();
        stub.fail_next();
        let err = Resolution::Stub(stub.0.clone())
            .resolve(true, "gone.example", 80)
            .await
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<io::Error>().map(|e| e.kind()),
            Some(io::ErrorKind::NotFound)
        );

        stub.hang_next();
        let silent = Resolution::Stub(stub.0.clone());
        let hung = silent.resolve(true, "silent.example", 80);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), hung)
                .await
                .is_err(),
            "only a timeout ends a lookup that never answers"
        );
        assert_eq!(stub.calls(), ["gone.example", "silent.example"]);
    }

    #[test]
    fn brackets_come_off_for_the_resolver() {
        assert_eq!(without_brackets("[2001:db8::1]"), "2001:db8::1");
        assert_eq!(without_brackets("example.com"), "example.com");
        assert_eq!(without_brackets("[::1"), "[::1");
    }

    /// A connect the pool started but did not win can be polled after its
    /// stack was replaced (hyper-util finishes a losing connect in a task the
    /// session cannot abort: N3). The connector reads the closed flag when the
    /// future is polled — not when it is made — so the replaced stack neither
    /// resolves a name nor dials an address, directly or through the proxy.
    #[tokio::test]
    async fn a_replaced_stack_resolves_and_dials_nothing() {
        let stub = StubResolver::default();
        // An answer for the name, if anything were to look it up.
        stub.push(&["127.0.0.1"]);
        let closed = Closed::new();
        let mut direct = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );
        let mut proxy = ProxyConfig::default();
        proxy.kind = ProxyKind::Socks5;
        proxy.host = "127.0.0.1".to_owned();
        proxy.port = 2;
        let mut proxied = ProxyConnector::new(
            Some(&proxy),
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );

        // The futures exist before the replacement: hyper-util's pool hands a
        // started-but-losing connect to the runtime exactly this way, and the
        // stack is replaced before anything polls them.
        let by_name = direct.call("http://tracker.invalid/announce".parse::<Uri>().unwrap());
        let by_literal = direct.call("http://127.0.0.1:1/announce".parse::<Uri>().unwrap());
        let through_proxy = proxied.call("http://tracker.invalid/announce".parse::<Uri>().unwrap());

        let cap = audit::capture();
        closed.set();
        assert!(by_name.await.is_err(), "a replaced stack must not connect");
        assert!(
            by_literal.await.is_err(),
            "a replaced stack must not dial an address"
        );
        assert!(
            through_proxy.await.is_err(),
            "a replaced stack must not dial the proxy"
        );
        let recs = cap.take();
        drop(cap);
        // The capture is process-wide and parallel tests dial their own
        // listeners: assert the absence of the dials and the lookup this test
        // would have made, and that the stub was never consulted.
        assert!(
            !recs.contains(&audit::Outbound::TcpConnect("127.0.0.1:1".parse().unwrap())),
            "a replaced stack dialled an address: {recs:?}"
        );
        assert!(
            !recs.contains(&audit::Outbound::TcpConnect("127.0.0.1:2".parse().unwrap())),
            "a replaced stack dialled the proxy: {recs:?}"
        );
        assert!(
            !recs
                .iter()
                .any(|o| matches!(o, audit::Outbound::Lookup(h) if h == "tracker.invalid")),
            "a replaced stack looked a name up: {recs:?}"
        );
        assert!(
            stub.calls().is_empty(),
            "the name was looked up on a replaced stack: {:?}",
            stub.calls()
        );
    }

    /// Hyper dials an IP-literal URI without asking the resolver, so the
    /// resolver's Lookup record cannot cover it: the connector records the
    /// connect itself, or a direct tracker at an address leaves an empty capture.
    #[tokio::test]
    async fn direct_http_dial_to_ip_literal_is_recorded() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            drop(s);
        });
        // A stub with nothing queued: a literal that went through the resolver would fail
        // here, and `calls` stays the evidence that it did not.
        let stub = StubResolver::default();
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            Closed::new(),
        );
        let cap = audit::capture();
        let uri = format!("http://{addr}/announce").parse::<Uri>().unwrap();
        let stream = connector.call(uri).await.unwrap();
        let recs = cap.take();
        drop(stream);
        drop(cap);
        accept.await.unwrap();
        assert!(
            recs.contains(&audit::Outbound::TcpConnect(addr)),
            "the direct dial to the IP literal must be recorded: {recs:?}"
        );
        assert!(
            stub.calls().is_empty(),
            "an IP literal is not a name: nothing may be looked up: {:?}",
            stub.calls()
        );

        // A bracketed IPv6 literal takes the same path, brackets stripped.
        if tokio::net::TcpListener::bind("[::1]:0").await.is_err() {
            return; // no IPv6 loopback on this host
        }
        let listener6 = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let addr6 = listener6.local_addr().unwrap();
        let accept6 = tokio::spawn(async move {
            let (s, _) = listener6.accept().await.unwrap();
            drop(s);
        });
        let cap = audit::capture();
        let uri = format!("http://[{}]:{}/announce", addr6.ip(), addr6.port())
            .parse::<Uri>()
            .unwrap();
        let stream = connector.call(uri).await.unwrap();
        let recs = cap.take();
        drop(stream);
        drop(cap);
        accept6.await.unwrap();
        assert!(
            recs.contains(&audit::Outbound::TcpConnect(addr6)),
            "the bracketed IPv6 literal must be recorded: {recs:?}"
        );
    }

    /// A direct dial by host name is recorded at the moment its connect is attempted: this
    /// connector dials the answers itself and records each attempt, so a name cannot leave the
    /// outbound audit blind (the lookup records the name alone). A loopback listener stands in
    /// for the tracker, so no test sends a packet off the machine (N2).
    #[tokio::test]
    async fn direct_http_dial_by_name_is_recorded() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
        });
        let stub = StubResolver::default();
        stub.push(&["127.0.0.1"]);
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            Closed::new(),
        );
        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{}/announce", addr.port())
            .parse::<Uri>()
            .unwrap();
        let stream = connector.call(uri).await.unwrap();
        let recs = cap.take();
        drop(stream);
        drop(cap);
        let _ = accepted.await.unwrap().unwrap();
        assert!(
            recs.contains(&audit::Outbound::TcpConnect(addr)),
            "a direct dial by name must be recorded: {recs:?}"
        );
        assert_eq!(stub.calls(), ["tracker.invalid"], "the name was looked up");
    }

    /// A name that answers with two addresses is dialled with the flag read before *each*
    /// attempt: hyper's own connector resolves once and then dials the answers itself — the next
    /// address after a failure, or after its happy-eyeballs delay — leaving every later dial
    /// nothing to check against a stack that was replaced meanwhile (N1). Holds the first attempt
    /// open (its dial parked at a gate: see [`gate_dial`]), replaces the stack while it is in
    /// flight, and asserts that **nothing at all** is recorded afterwards — not the address that
    /// was already in flight, and not the second, which is never dialled — and that the dial
    /// fails (R1 b).
    ///
    /// Both addresses are loopback and on the port the URI carries: nothing listens on the first
    /// (IPv4), so it could not connect; the second (IPv6) is a listener a later dial would reach.
    /// The gate sits *before* the flag is read, as in production nothing stands between the check
    /// and the dial, so the parked attempt must not record or dial once it runs on
    /// (the Minor finding). No packet leaves the machine (N2).
    ///
    /// Runs on a paused clock: the 250 ms stagger cannot fire before the replacement on a
    /// stalled machine, and the wait loop below advances the clock a fixed step at a time.
    /// No dial is ever issued: the parked attempt is refused at its own check and the second
    /// address is never started. The accept timeout is a virtual timer the paused clock
    /// elapses by auto-advance, and the audit assertion carries the check.
    #[tokio::test(start_paused = true)]
    async fn a_second_address_is_not_dialled_once_the_stack_is_replaced() {
        let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
            return; // no IPv6 loopback on this host
        };
        let port = listener.local_addr().unwrap().port();
        // Nothing listens on the IPv4 loopback at that port, so the first attempt could not
        // connect: hold it briefly to be sure, and skip if some other process has it.
        let Ok(probe) = tokio::net::TcpListener::bind(("127.0.0.1", port)).await else {
            return;
        };
        drop(probe);
        let accepted = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
        });

        let first: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
        let second: SocketAddr = (std::net::Ipv6Addr::LOCALHOST, port).into();
        let gate = gate_dial(first);

        let stub = StubResolver::default();
        stub.push(&["127.0.0.1", "::1"]);
        let closed = Closed::new();
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );

        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let dial = tokio::spawn(connector.call(uri));
        // The first attempt is in flight (parked at its gate, nothing opened yet): replace the
        // stack, then let it run on into the refusal at its own check.
        while gate.parked() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        closed.set();
        gate.release();

        assert!(
            dial.await.unwrap().is_err(),
            "a replaced stack must not dial the rest of the answers"
        );
        let recs = cap.take();
        drop(cap);
        // Everything this dial could have recorded: the parked attempt (which must not record
        // once it runs on) and the second address (which must never be tried). The capture is
        // process-wide, so read only the addresses this test answers with.
        let ours: Vec<&audit::Outbound> = recs
            .iter()
            .filter(|o| matches!(o, audit::Outbound::TcpConnect(a) if *a == first || *a == second))
            .collect();
        assert!(
            ours.is_empty(),
            "a replaced stack recorded a dial after its stack was replaced: {ours:?}"
        );
        assert!(
            accepted.await.unwrap().is_err(),
            "the listener accepted a direct dial a replaced stack made between two attempts"
        );
    }

    /// A first address whose attempt hangs is not waited out: the next one is started a stagger
    /// later and wins, so a family that blackholes (an IPv6 route that drops every packet) does
    /// not spend the whole request budget before the other family is tried (RFC 8305, R1 a).
    ///
    /// Both addresses are loopback and on the port the URI carries: the second is a live
    /// listener, the first is loopback too but its dial is held open by a gate — a hung
    /// attempt — so nothing leaves the machine. The hung attempt is the IPv6 loopback, held
    /// *before* its dial, so it never dials and the test runs on a host with no IPv6 loopback
    /// at all; the live listener is IPv4.
    #[tokio::test]
    async fn a_hung_first_address_does_not_hold_up_the_dial() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(3), listener.accept()).await
        });

        // The first answer is the gated IPv6 loopback: its dial is held open (a hung attempt)
        // and never reaches the socket, so no IPv6 loopback is needed. The second is the live
        // IPv4 listener.
        let first: SocketAddr = (std::net::Ipv6Addr::LOCALHOST, port).into();
        let second: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
        let gate = gate_dial(first);

        let stub = StubResolver::default();
        stub.push(&["::1", "127.0.0.1"]);
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            Closed::new(),
        );

        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let started = std::time::Instant::now();
        let dial = tokio::spawn(connector.call(uri));
        // The first attempt hangs (parked at its gate): the stagger must start the second, which
        // connects, without waiting the first one out.
        while gate.parked() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let stream = dial
            .await
            .unwrap()
            .expect("the second address must be tried while the first hangs");
        let elapsed = started.elapsed();
        let recs = cap.take();
        drop(stream);
        drop(cap);
        assert!(
            elapsed < Duration::from_secs(2),
            "the hung first address held up the dial for {elapsed:?}"
        );
        assert!(
            recs.contains(&audit::Outbound::TcpConnect(second)),
            "the second address must be the one that connected: {recs:?}"
        );
        assert!(
            accepted.await.unwrap().is_ok(),
            "the live second address was never dialled while the first hung"
        );
    }

    /// A stack replaced while the first attempt hangs starts no further attempt: the second
    /// address is neither recorded nor dialled, and the dial fails rather than opening a socket
    /// (N1, R1 b). The gate sits before the flag is read and the dial is released by the test, so
    /// the ordering does not depend on timing.
    ///
    /// Runs on a paused clock: the 250 ms stagger cannot fire before the replacement on a
    /// stalled machine, and the wait loop below advances the clock a fixed step at a time.
    /// No dial is ever issued: the parked attempt is refused at its own check and the second
    /// address is never started, and the audit assertion carries the check.
    #[tokio::test(start_paused = true)]
    async fn a_replacement_while_the_first_attempt_hangs_starts_no_further_attempt() {
        // Dead loopback addresses: nothing listens, so a dial that does run cannot connect.
        let Ok(dead) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let port = dead.local_addr().unwrap().port();
        drop(dead);

        let first: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
        let second: SocketAddr = (std::net::Ipv6Addr::LOCALHOST, port).into();
        let gate = gate_dial(first);

        let stub = StubResolver::default();
        stub.push(&["127.0.0.1", "::1"]);
        let closed = Closed::new();
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );

        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let dial = tokio::spawn(connector.call(uri));
        while gate.parked() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Replace the stack while the first attempt hangs, then let it run on: it must refuse at
        // its own check, and no further attempt may be started.
        closed.set();
        gate.release();

        assert!(
            dial.await.unwrap().is_err(),
            "a replaced stack must not dial once its attempt is refused"
        );
        let recs = cap.take();
        drop(cap);
        let ours: Vec<&audit::Outbound> = recs
            .iter()
            .filter(|o| matches!(o, audit::Outbound::TcpConnect(a) if *a == first || *a == second))
            .collect();
        assert!(
            ours.is_empty(),
            "a replaced stack recorded a dial after its stack was replaced: {ours:?}"
        );
    }

    /// A stack replaced while a connect is *in flight* — the attempt already passed its closed
    /// check and sits inside a slow dial — starts no further attempt: the stagger timer must
    /// not dial the next address once the stack it belongs to is replaced (N1). The attempt is
    /// held at a hang gate *after* the check and the record (see [`hang_connect`]), so the
    /// switch lands in the only gap the dial gates cannot cover: while an attempt is inside its
    /// connect, with the stagger still owing the next address. The second attempt is proved
    /// started — it parks at its own dial gate, before its check — and is released to be
    /// refused there, so the test cannot pass if the stagger never runs. Both attempts are
    /// released before the dial is awaited, each fails or is refused before any dial, no
    /// packet leaves the machine (N2), and the dial ends in an error either way.
    ///
    /// Runs on a paused clock: the 250 ms stagger comes due exactly when the test advances it,
    /// so the ordering does not depend on a stalled machine; the audit assertion carries the
    /// check.
    #[tokio::test(start_paused = true)]
    async fn a_replacement_while_a_connect_is_in_flight_starts_no_further_attempt() {
        // A dead loopback port: nothing listens, so a dial that does run cannot connect.
        let Ok(dead) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let port = dead.local_addr().unwrap().port();
        drop(dead);

        let first: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
        let second: SocketAddr = (std::net::Ipv6Addr::LOCALHOST, port).into();
        // The first attempt hangs inside its connect (past its check and record); the second
        // parks at a dial gate before its check, so the test can see the stagger start it.
        let gate = hang_connect(first);
        let g2 = gate_dial(second);

        let stub = StubResolver::default();
        stub.push(&["127.0.0.1", "::1"]);
        let closed = Closed::new();
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );

        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let dial = tokio::spawn(connector.call(uri));
        // The first attempt is past its closed check and recorded, and parked inside its
        // connect: replace the stack while the dial is in flight.
        while gate.parked() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        closed.set();
        // The stagger comes due while the stack is replaced: under paused time the timer is
        // what would start the second attempt, and it must find the flag set.
        tokio::time::advance(Duration::from_millis(300)).await;
        // The stagger must have started the second attempt: it parks at its own dial gate,
        // before its check. Bounded: the timer is ready the moment the clock is advanced, so
        // it runs on the next free turn of the scheduler.
        for _ in 0..100 {
            if g2.parked() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            g2.parked() == 1,
            "the stagger timer never started the second attempt"
        );
        // Release it: the attempt must refuse at its own check, recording and dialling
        // nothing. It has run once its park ends (the counter falls back to zero), so the
        // capture is complete by then.
        g2.release();
        for _ in 0..100 {
            if g2.parked() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let recs = cap.take();
        drop(cap);
        assert!(
            recs.contains(&audit::Outbound::TcpConnect(first)),
            "the in-flight attempt was recorded before the switch: {recs:?}"
        );
        assert!(
            !recs.contains(&audit::Outbound::TcpConnect(second)),
            "the stagger timer dialled a second address after the stack was replaced: {recs:?}"
        );
        gate.release();
        assert!(
            dial.await.unwrap().is_err(),
            "a replaced stack must not connect once its in-flight attempt ends"
        );
    }

    /// The attempts are started in interleaved family order, beginning with the family of the
    /// resolver's first answer (RFC 8305 §4, R1 c): a dual-stack name answered
    /// `[v6a, v6b, v4a]` is tried `v6a, v4a, v6b`, so the other family is reached on the second
    /// attempt rather than after every address of the first.
    ///
    /// Every attempt is gated, so no connect is ever issued and the addresses need not be
    /// reachable: the order they park in is the order they were started in, read from
    /// [`PARK_ORDER`] rather than inferred from timing.
    #[tokio::test]
    async fn addresses_are_attempted_in_interleaved_family_order() {
        // A port to put on the stub answers; held so no parallel test can take it. Nothing is
        // ever dialled, so nothing need listen on it.
        let Ok(holder) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let port = holder.local_addr().unwrap().port();

        let v6a = SocketAddr::new("::1".parse().unwrap(), port);
        let v6b = SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), port);
        let v4a: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
        // One gate per address: each attempt parks, so the order they park in is the attempt
        // order, and nothing is dialled.
        let gates = [gate_dial(v6a), gate_dial(v6b), gate_dial(v4a)];

        let stub = StubResolver::default();
        stub.push(&["::1", "::ffff:127.0.0.1", "127.0.0.1"]);
        let mut connector = ProxyConnector::new(
            None,
            Duration::from_secs(30),
            false,
            stub.resolution(),
            Closed::new(),
        );

        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let dial = tokio::spawn(connector.call(uri));
        // Three attempts, a stagger apart: wait (bounded) until all three have parked.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let parked: usize = gates.iter().map(|g| usize::from(g.parked() > 0)).sum();
            if parked == 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "only {parked} of the three addresses were tried in interleaved order"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let order: Vec<SocketAddr> = PARK_ORDER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .copied()
            .filter(|a| *a == v6a || *a == v6b || *a == v4a)
            .collect();
        dial.abort();
        drop(holder);
        assert_eq!(
            order,
            vec![v6a, v4a, v6b],
            "the families must be interleaved, starting with the first answer's"
        );
    }

    /// A lookup started under one stack and still in flight when the stack is replaced
    /// (hyper-util's pool finishes a losing connect in a task the session cannot abort: N3)
    /// must not dial once it answers, and must leave no connect record: this connector dials
    /// the answers itself, so a check made only *before* the lookup would open a direct socket
    /// past the proxy that replaced the stack.
    ///
    /// Uses the IPv6 loopback and the port the URI carries: the process-wide capture also
    /// holds other tests' loopback dials, so the absence assertion needs an address only this
    /// test answers with.
    #[tokio::test]
    async fn a_lookup_in_flight_when_the_stack_is_replaced_dials_nothing() {
        // The port the URI carries, and a listener the bug's dial would reach.
        let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
            return; // no IPv6 loopback on this host
        };
        let port = listener.local_addr().unwrap().port();
        let dialed = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
        });

        let stub = StubResolver::default();
        // The answer the in-flight lookup is parked on.
        let gate = stub.push_gated(&["::1"]);
        let closed = Closed::new();
        let mut direct = ProxyConnector::new(
            None,
            Duration::from_secs(5),
            false,
            stub.resolution(),
            closed.clone(),
        );

        let cap = audit::capture();
        let uri = format!("http://tracker.invalid:{port}/announce")
            .parse::<Uri>()
            .unwrap();
        let dial = tokio::spawn(direct.call(uri));
        // Wait until the lookup is in flight (its name is recorded as taken), then replace
        // the stack and release the answer.
        while stub.calls().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        closed.set();
        gate.notify_one();

        assert!(
            dial.await.unwrap().is_err(),
            "a replaced stack must not dial once its lookup answers"
        );
        let recs = cap.take();
        drop(cap);
        assert!(
            !recs.contains(&audit::Outbound::TcpConnect(SocketAddr::new(
                "::1".parse().unwrap(),
                port
            ))),
            "a replaced stack dialled an address once its lookup answered: {recs:?}"
        );
        assert!(
            dialed.await.unwrap().is_err(),
            "the listener accepted a direct dial a replaced stack made after its lookup answered"
        );
    }
}
