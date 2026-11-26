//! The TCP connector under the HTTP clients: it opens every direct connection itself, refusing
//! an address inside the local network before dialling it, or tunnels through a SOCKS5 or HTTP
//! CONNECT proxy.

#[cfg(test)]
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

/// Dials `addrs` one after another until one connects, each attempt bounded by `timeout`.
///
/// This connector opens every direct connection itself, so the check that a replaced stack
/// dials nothing stands **immediately before each connect** (N1), in the same poll as the dial
/// and not only before the first of them: a name's answers are tried one at a time, and a
/// dialler that resolved once and then connected to the answers itself would run every later
/// attempt — the next address after a failure, or after its own happy-eyeballs delay — without
/// ever looking at the flag again. No happy eyeballs here: a second address is tried only once
/// the first has failed or timed out. The connect is recorded in the same step, so every
/// direct dial — by name or by IP literal, successful or not — is visible to the outbound
/// audit.
async fn dial_direct(
    addrs: &[SocketAddr],
    timeout: Duration,
    closed: &Closed,
) -> Result<TokioIo<TcpStream>, BoxError> {
    let mut last: Option<io::Error> = None;
    for addr in addrs {
        // Read in the same poll as the dial below: a replacement that lands between two
        // attempts refuses the next one (N1).
        if closed.is_set() {
            return Err(closed.refuse());
        }
        // (tests) A dial a test holds open, so it can replace the stack while this attempt is
        // in flight.
        #[cfg(test)]
        park_before_dial(*addr).await;
        // audit: recorded here, in the same poll as the dial, so a direct dial by name or by
        // literal is visible to the outbound audit (a lookup records the name alone, and this
        // connector dials the answers itself).
        audit::record(audit::Outbound::TcpConnect(*addr));
        match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(TokioIo::new(stream)),
            Ok(Err(e)) => last = Some(e),
            Err(_) => {
                last = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("dialling {addr} timed out"),
                ));
            }
        }
    }
    Err(match last {
        Some(e) => e.into(),
        None => io::Error::new(io::ErrorKind::NotFound, "no address to dial").into(),
    })
}

/// (tests) Dials a test has asked to hold open, keyed by address. Lets a multi-address dial be
/// held after its first attempt has started, so a test can replace the stack mid-dial and see
/// what the rest of the list does (N1) — without a packet leaving the machine and without
/// depending on a platform's listen-backlog behaviour.
#[cfg(test)]
static DIAL_GATES: Mutex<Vec<(SocketAddr, DialGate)>> = Mutex::new(Vec::new());

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

/// (tests) Parks the dial to `addr` at its gate, if one is registered; a no-op otherwise.
#[cfg(test)]
async fn park_before_dial(addr: SocketAddr) {
    let gate = {
        let mut gates = DIAL_GATES.lock().unwrap_or_else(|e| e.into_inner());
        match gates.iter().position(|(a, _)| *a == addr) {
            Some(i) => gates.remove(i).1,
            None => return,
        }
    };
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

    /// A name that answers with two addresses is dialled one address at a time, with the flag
    /// read before *each* attempt: hyper's own connector resolves once and then dials the
    /// answers itself — the next address after a failure, or after its happy-eyeballs delay —
    /// leaving every later dial nothing to check against a stack that was replaced meanwhile
    /// (N1). Holds the first attempt open (its dial parked at a gate: see [`gate_dial`]),
    /// replaces the stack while it is in flight, and asserts the second address is never
    /// dialled and nothing is recorded after the replacement.
    ///
    /// Both addresses are loopback and on the port the URI carries: nothing listens on the
    /// first (IPv4), so its attempt is refused at once and the dial moves on; the second (IPv6)
    /// is a listener a later dial would reach. No packet leaves the machine (N2).
    #[tokio::test]
    async fn a_second_address_is_not_dialled_once_the_stack_is_replaced() {
        let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
            return; // no IPv6 loopback on this host
        };
        let port = listener.local_addr().unwrap().port();
        // The first attempt is refused at once only if nothing listens on the IPv4 loopback
        // at that port: hold it briefly to be sure, and skip if some other process has it.
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
        // stack, then let it run into the refusal and on to the second address.
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
        assert!(
            !recs.contains(&audit::Outbound::TcpConnect(second)),
            "a replaced stack dialled a further address after its stack was replaced: {recs:?}"
        );
        assert!(
            accepted.await.unwrap().is_err(),
            "the listener accepted a direct dial a replaced stack made between two attempts"
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
