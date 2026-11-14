//! The TCP connector under the HTTP clients: hyper's own [`HttpConnector`], resolving
//! through a resolver that can refuse an address inside the local network before dialling
//! it, or a tunnel through a SOCKS5 or HTTP CONNECT proxy.

#[cfg(test)]
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use fbt_net::addr::is_internal;
use fbt_net::proxy::{ProxyConfig, ProxyKind, ProxyTarget, connect_tcp};
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Connecting = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, BoxError>> + Send>>;
type Resolving =
    Pin<Box<dyn Future<Output = Result<std::vec::IntoIter<SocketAddr>, BoxError>> + Send>>;

/// The most answers one guarded lookup judges: a name answering with more than that fails
/// the request instead of letting the rest go unchecked, so the check stays bounded however
/// many addresses a hostile name hands back.
const MAX_GUARDED_ADDRESSES: usize = 64;

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
                Answer::NotFound => Err(stub_not_found(host)),
                Answer::Silence => {
                    std::future::pending::<()>().await;
                    // Never reached: only the request's own timeout ends a silent lookup.
                    Err(stub_not_found(host))
                }
            };
        }
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

/// hyper's resolver, with the option to refuse what it resolves: the addresses it hands back
/// are exactly the ones the connection dials, so a name whose answer changes between two
/// lookups cannot slip an internal address past a check made earlier somewhere else.
#[derive(Clone, Debug)]
pub(crate) struct GuardedResolver {
    /// Whether an answer inside the local network refuses the target. Only false for the
    /// client that asks first, where the address is the caller's own choice.
    deny_internal: bool,
    where_to_look: Resolution,
}

impl GuardedResolver {
    fn new(deny_internal: bool, where_to_look: Resolution) -> GuardedResolver {
        GuardedResolver {
            deny_internal,
            where_to_look,
        }
    }
}

impl tower_service::Service<Name> for GuardedResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = BoxError;
    type Future = Resolving;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let this = self.clone();
        // hyper hands a resolver the host alone and puts the URI's port back on the answers,
        // so they are asked for with none.
        Box::pin(async move {
            this.where_to_look
                .resolve(this.deny_internal, name.as_str(), 0)
                .await
                .map(Vec::into_iter)
        })
    }
}

/// Opens the TCP connection for one request URI.
#[derive(Debug, Clone)]
pub(crate) enum ProxyConnector {
    /// hyper's connector (`enforce_http(false)`, so TLS can wrap it), resolving through
    /// [`GuardedResolver`].
    Direct(HttpConnector<GuardedResolver>),
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
    },
}

impl ProxyConnector {
    /// A direct connector, or one through `proxy` when it names a proxy. `deny_internal`:
    /// this connector dials nothing inside the local network; `where_to_look` says where
    /// host names are looked up (only tests pass anything but the system resolver).
    pub(crate) fn new(
        proxy: Option<&ProxyConfig>,
        timeout: Duration,
        deny_internal: bool,
        where_to_look: Resolution,
    ) -> ProxyConnector {
        match proxy {
            Some(p) if p.kind != ProxyKind::None => ProxyConnector::Proxied {
                proxy: p.clone(),
                timeout,
                guard: deny_internal,
                where_to_look,
            },
            _ => {
                let mut http = HttpConnector::new_with_resolver(GuardedResolver::new(
                    deny_internal,
                    where_to_look,
                ));
                http.enforce_http(false);
                ProxyConnector::Direct(http)
            }
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

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            ProxyConnector::Direct(http) => http.poll_ready(cx).map_err(Into::into),
            ProxyConnector::Proxied { .. } => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match self {
            ProxyConnector::Direct(http) => {
                let connecting = http.call(uri);
                Box::pin(async move { connecting.await.map_err(Into::into) })
            }
            ProxyConnector::Proxied {
                proxy,
                timeout,
                guard,
                where_to_look,
            } => {
                let (proxy, timeout, guard, where_to_look) =
                    (proxy.clone(), *timeout, *guard, where_to_look.clone());
                Box::pin(async move {
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
                    let stream = connect_tcp(&proxy, target, timeout).await?;
                    Ok(TokioIo::new(stream))
                })
            }
        }
    }
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
}
