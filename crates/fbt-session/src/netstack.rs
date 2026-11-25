//! The session's swappable network stack.
//!
//! Everything a session sends with (the shared UDP sockets, uTP, the DHT, the
//! tracker client, LSD, port mapping, the listeners and their accept loops)
//! lives in one [`NetStack`], reachable as [`Shared::net`](crate::session::Shared::net)
//! and stamped with a generation. [`Session::set_network_settings`](crate::Session::set_network_settings)
//! replaces it whole: it installs a closed stack first (no socket, no dial),
//! tears the old one down, closes the connections of older generations, then
//! builds and installs the new one. Nothing a session sends is left outside
//! the stack, so a change cannot leave a socket behind.
//!
//! The order is the safety guarantee: the closed stack is installed before the
//! old one is torn down, so no dial started before or during a change can
//! reach the network on a stack that no longer matches the settings.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use fbt_core::udp::DatagramSender;
use fbt_dht::Dht;
use fbt_net::MapEvent;
use fbt_net::PortMapper;
use fbt_net::demux::{PacketKind, SharedUdpSocket};
use fbt_net::lsd::LsdPeer;
use fbt_net::proxy::{ProxyConfig, ProxyKind, Socks5Udp};
use fbt_tracker::{TrackerClient, TrackerConfig};
use fbt_utp::UtpSocket;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::error::SessionError;
use crate::net::{
    ListenError, LsdHub, bind_all, fallback_udp, start_dht, start_port_mapping, utp_for,
    utp_sockets,
};
use crate::settings::{NetworkSettings, Settings};

/// One generation of the session's network: every socket it can send from,
/// the DHT, uTP, LSD, port mapping, the tracker client and the listeners,
/// built together from a [`NetworkSettings`].
///
/// Built by [`NetStack::build`] and replaced by
/// [`Session::set_network_settings`](crate::Session::set_network_settings);
/// a torrent reads it through [`Shared::net`](crate::session::Shared::net),
/// which returns the current one. The generation a dial read is recorded so a
/// connection can be closed when the stack changed under it.
#[derive(Debug)]
pub(crate) struct NetStack {
    /// Increments on every replacement; a dial that reads generation `g` is
    /// dropped when the session's generation is no longer `g`.
    pub generation: u64,
    /// The network settings as applied (an HTTP proxy carrying peers turns
    /// outgoing uTP off here, as [`Settings::proxy_bypasses`] does).
    pub net: NetworkSettings,
    /// Shared by every torrent's announcer; a request reads it at each use,
    /// so a stack change needs no announcer restart.
    pub tracker: TrackerClient,
    /// The session-wide DHT node; `None` with `dht_enabled` off.
    pub dht: Option<Dht>,
    /// The DHT's UDP sockets' addresses (see
    /// [`dht_port_for`](crate::session::dht_port_for)).
    pub dht_addrs: Vec<SocketAddr>,
    /// One uTP socket per shared UDP socket, in the same order.
    pub utp: Vec<UtpSocket>,
    /// Every shared UDP socket (the endpoints', then the per-family
    /// fallbacks), or the one relayed socket under a SOCKS5 proxy.
    pub udp: Vec<SharedUdpSocket>,
    /// Local service discovery and its announce bookkeeping.
    pub lsd: LsdHub,
    /// UPnP and PCP/NAT-PMP port mapping; `None` with both off.
    pub portmap: Option<PortMapper>,
    /// The listen addresses this stack bound, in settings order: what a
    /// rebuild binds again, so the ports stay.
    pub local_addrs: Vec<SocketAddr>,
    /// Port of the first listener (announced over HTTP and to the DHT).
    pub listen_port: u16,
    /// Port of the first IPv4 listener, `None` without one.
    pub listen_port_v4: Option<u16>,
    /// Port of the first IPv6 listener, likewise.
    pub listen_port_v6: Option<u16>,
    /// The listen addresses that could not be bound, fully or on their UDP
    /// side.
    pub listen_errors: Vec<ListenError>,
    /// The placeholder stack installed while a change is in flight: no
    /// socket, and no dial may be attempted (see [`NetStack::closed`]).
    pub closed: bool,
    /// The TCP listeners, until the session spawns their accept loops (see
    /// [`NetStack::take_tcp`]).
    tcp: Mutex<Vec<TcpListener>>,
    /// LSD's found-peer receiver, until the session wires it.
    lsd_found: Mutex<Option<mpsc::Receiver<LsdPeer>>>,
    /// The port mapper's event receiver, until the session wires it.
    map_events: Mutex<Option<mpsc::Receiver<MapEvent>>>,
    /// The accept loops and routers running on this stack.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Signals those tasks to stop (see [`NetStack::shutdown`]).
    stop: watch::Sender<bool>,
}

impl NetStack {
    /// A stack with no socket at all, installed while a change is in flight
    /// so nothing reads the old stack and nothing new dials. Its tracker
    /// client fails every request with
    /// [`TrackerError::Proxy`](fbt_tracker::TrackerError::Proxy) and opens no
    /// socket, and [`closed`](Self::closed) keeps every dial from being
    /// attempted at all.
    pub(crate) fn closed(generation: u64) -> NetStack {
        NetStack {
            generation,
            // The requested settings are held by the session, not here; the
            // closed stack's own value is never read for a dial.
            net: NetworkSettings::default(),
            tracker: no_tracker(),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
            udp: Vec::new(),
            lsd: LsdHub::disabled(),
            portmap: None,
            local_addrs: Vec::new(),
            listen_port: 0,
            listen_port_v4: None,
            listen_port_v6: None,
            listen_errors: Vec::new(),
            closed: true,
            tcp: Mutex::new(Vec::new()),
            lsd_found: Mutex::new(None),
            map_events: Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
            stop: watch::channel(false).0,
        }
    }

    /// Builds the stack for `generation` as `net` says: binds `addrs` (the
    /// addresses the previous stack had bound, or [`Settings::listen_addrs`]
    /// at the session's start), opens the UDP side (the one socket of the
    /// SOCKS5 relay `relay` when given, else the shared sockets), starts the
    /// DHT on `dht_state` and the uTP sockets, and creates the tracker
    /// client, LSD and port mapping. The caller does the UDP association and
    /// passes its socket, so it can classify a failure.
    ///
    /// Must run inside a tokio runtime.
    pub(crate) async fn build(
        settings: &Settings,
        net: &NetworkSettings,
        addrs: &[SocketAddr],
        dht_state: Option<Vec<u8>>,
        generation: u64,
        relay: Option<Socks5Udp>,
    ) -> Result<NetStack, SessionError> {
        // The effective settings: `net` decides the proxy and the switches
        // (an HTTP proxy carrying peers turns outgoing uTP off, whatever the
        // stored value says).
        let mut effective = settings.clone();
        effective.set_network(net.clone());
        if effective.proxy.kind == ProxyKind::Http && effective.proxy.proxy_peers {
            effective.enable_outgoing_utp = false;
        }
        let applied = effective.network();
        let relay_udp = relay.is_some();
        let (endpoints, listen_errors) = bind_all(&effective, &applied, addrs, !relay_udp).await?;
        let local_addrs: Vec<SocketAddr> = endpoints.iter().map(|e| e.addr).collect();
        let udp: Vec<SharedUdpSocket> = match relay {
            Some(assoc) => vec![bind_relay(&local_addrs, assoc)?],
            None => {
                let mut udp: Vec<SharedUdpSocket> =
                    endpoints.iter().filter_map(|e| e.udp.clone()).collect();
                udp.extend(fallback_udp(&endpoints));
                udp
            }
        };
        let utp = utp_sockets(&effective, applied.enable_incoming_utp, &udp);
        let senders: Vec<Arc<dyn DatagramSender>> =
            udp.iter().map(SharedUdpSocket::sender).collect();
        let tracker = match TrackerClient::new_with(
            effective.tracker.clone(),
            senders,
            Some(effective.proxy.clone()),
        ) {
            Ok(t) => t,
            Err(e) => {
                for u in &utp {
                    u.shutdown_now();
                }
                udp.iter().for_each(SharedUdpSocket::close);
                return Err(SessionError::Tracker(e));
            }
        };
        let tracker_input = tracker.udp_handler();
        for u in &udp {
            u.set_handler(PacketKind::Tracker, Arc::clone(&tracker_input));
        }
        let dht = if applied.dht_enabled {
            start_dht(&effective, &udp, dht_state.as_deref()).await
        } else {
            None
        };
        let dht_addrs = dht.as_ref().map(Dht::local_addrs).unwrap_or_default();
        let listen_port = local_addrs.first().map_or(0, SocketAddr::port);
        let first_port = |v4: bool| {
            local_addrs
                .iter()
                .find(|a| a.is_ipv4() == v4)
                .map(SocketAddr::port)
        };
        let (listen_port_v4, listen_port_v6) = (first_port(true), first_port(false));
        let (lsd, lsd_found) = LsdHub::start(&effective, applied.enable_lsd);
        let (portmap, map_events) = start_port_mapping(&effective, &applied, &local_addrs).unzip();
        let mut tcp = Vec::new();
        for e in endpoints {
            if let Some(t) = e.tcp {
                tcp.push(t);
            }
        }
        Ok(NetStack {
            generation,
            net: applied,
            tracker,
            dht,
            dht_addrs,
            utp,
            udp,
            lsd,
            portmap,
            local_addrs,
            listen_port,
            listen_port_v4,
            listen_port_v6,
            listen_errors,
            closed: false,
            tcp: Mutex::new(tcp),
            lsd_found: Mutex::new(lsd_found),
            map_events: Mutex::new(map_events),
            tasks: Mutex::new(Vec::new()),
            stop: watch::channel(false).0,
        })
    }

    /// The listen port of `peer`'s address family (an IPv4-mapped IPv6 peer
    /// counts as IPv4): the port this stack's listener of the family the
    /// connection runs over has, else the first listener's.
    pub(crate) fn listen_port_for(&self, peer: &SocketAddr) -> u16 {
        let family = if peer.ip().to_canonical().is_ipv4() {
            self.listen_port_v4
        } else {
            self.listen_port_v6
        };
        family.unwrap_or(self.listen_port)
    }

    /// The uTP socket to dial `peer` from: the first of its address family,
    /// as for the DHT and UDP trackers. `None` without one (or on the closed
    /// stack).
    pub(crate) fn utp_for(&self, peer: &SocketAddr) -> Option<UtpSocket> {
        utp_for(&self.utp, peer).cloned()
    }

    /// A receiver for this stack's stop signal; the accept loops and routers
    /// the session spawns select on it.
    pub(crate) fn stop_receiver(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }

    /// Takes the TCP listeners waiting for their accept loops.
    pub(crate) fn take_tcp(&self) -> Vec<TcpListener> {
        std::mem::take(&mut *self.tcp.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Takes LSD's found-peer receiver, once.
    pub(crate) fn take_lsd_found(&self) -> Option<mpsc::Receiver<LsdPeer>> {
        self.lsd_found
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Takes the port mapper's event receiver, once.
    pub(crate) fn take_map_events(&self) -> Option<mpsc::Receiver<MapEvent>> {
        self.map_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Records the handles of the tasks the session spawned on this stack.
    pub(crate) fn add_tasks(&self, tasks: Vec<JoinHandle<()>>) {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(tasks);
    }

    /// Stops this stack: signals its tasks (releasing the listeners) and
    /// waits for them, takes the DHT's state for the next stack, then stops
    /// the DHT and uTP (each of which would otherwise keep sending from
    /// this stack's own address) and finally LSD, port mapping and the UDP
    /// sockets under them. Waiting for the tasks is what frees the listen
    /// ports for the next stack to bind again.
    pub(crate) async fn shutdown(&self) -> Option<Vec<u8>> {
        self.stop_tasks().await;
        // The DHT's state goes with us; the DHT itself stops at once, so
        // nothing keeps querying directly through the change.
        let dht_state = match &self.dht {
            Some(dht) => dht.state().await.ok().map(|s| s.to_bytes()),
            None => None,
        };
        if let Some(dht) = &self.dht {
            dht.shutdown().await;
        }
        // uTP (and so its UDP socket) stops before LSD and port mapping,
        // which are the slow, unroutable ones.
        for u in &self.utp {
            u.shutdown().await;
        }
        self.lsd.shutdown().await;
        // Bounded (2 s in all): an unreachable router never holds us up.
        if let Some(pm) = &self.portmap {
            pm.shutdown().await;
        }
        for u in &self.udp {
            u.close();
        }
        dht_state
    }

    /// [`shutdown`](Self::shutdown) without waiting, for `Drop` and the
    /// session's teardown: signals and aborts the tasks and closes the
    /// sockets at once.
    pub(crate) fn abort(&self) {
        let _ = self.stop.send(true);
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        for t in tasks {
            t.abort();
        }
        for u in &self.utp {
            u.shutdown_now();
        }
        for u in &self.udp {
            u.close();
        }
    }

    /// Signals the tasks and awaits them, releasing the listeners.
    async fn stop_tasks(&self) {
        let _ = self.stop.send(true);
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        for t in tasks {
            let _ = t.await;
        }
    }
}

/// A UDP socket on a free port of `local_addrs`' first IP of the relay's
/// family (IPv4 in practice), else the unspecified address.
fn bind_relay(
    local_addrs: &[SocketAddr],
    assoc: Socks5Udp,
) -> Result<SharedUdpSocket, SessionError> {
    let v4 = assoc.relay().is_ipv4();
    let ip = local_addrs.iter().find(|a| a.is_ipv4() == v4).map_or(
        if v4 {
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        } else {
            std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        },
        SocketAddr::ip,
    );
    SharedUdpSocket::bind_relayed(SocketAddr::new(ip, 0), assoc)
        .map_err(|e| SessionError::Proxy(fbt_net::proxy::ProxyError::Io(e)))
}

/// A tracker client whose every request fails with
/// [`TrackerError::Proxy`](fbt_tracker::TrackerError::Proxy) and which opens
/// no socket: its proxy has an empty host, so an HTTP request fails before
/// dialling (`ProxyError::Protocol`) and a UDP request finds no relayed
/// socket.
fn no_tracker() -> TrackerClient {
    let mut proxy = ProxyConfig::default();
    proxy.kind = ProxyKind::Socks5;
    // `new_with` only builds clients (the TLS setup); nothing about this
    // config can fail, and it dials nothing.
    TrackerClient::new_with(TrackerConfig::default(), Vec::new(), Some(proxy))
        .expect("the closed stack's tracker client")
}

/// A `NetStack` for unit tests of the torrent actors: no socket and no DHT,
/// with a direct (proxyless) tracker client, so an actor that announces does
/// not fail at once, and the listen ports the test asks for.
#[cfg(test)]
impl NetStack {
    pub(crate) fn offline(
        settings: &Settings,
        listen_port: u16,
        listen_port_v4: Option<u16>,
        listen_port_v6: Option<u16>,
    ) -> NetStack {
        NetStack {
            generation: 1,
            net: settings.network(),
            tracker: TrackerClient::new(settings.tracker.clone()).expect("the test tracker client"),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
            udp: Vec::new(),
            lsd: LsdHub::disabled(),
            portmap: None,
            local_addrs: Vec::new(),
            listen_port,
            listen_port_v4,
            listen_port_v6,
            listen_errors: Vec::new(),
            closed: false,
            tcp: Mutex::new(Vec::new()),
            lsd_found: Mutex::new(None),
            map_events: Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
            stop: watch::channel(false).0,
        }
    }
}
