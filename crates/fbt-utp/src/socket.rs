//! The tokio runtime for uTP: one [`UtpSocket`] multiplexes connections over
//! a shared datagram sender (or a UDP socket it owns), drives their timers
//! and hands established ones out as [`UtpStream`]s.
//!
//! Locking: the connection table, the accept queue and each connection slot
//! have their own mutex. No two slot locks are ever held together, no slot
//! lock is taken while the table or queue lock is held, no lock is held
//! across an await, and datagrams are sent after every lock is released.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fbt_core::udp::{DatagramHandler, DatagramSender};
use rand::RngExt as _;
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::sync::Notify;

use crate::UtpError;
use crate::conn::{ConnState, Connection, UtpConfig};
use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;

/// The receive buffer of an owned socket: the largest UDP payload.
pub const MAX_DATAGRAM: usize = 65_536;
/// `ST_RESET` replies to unknown traffic, per socket and second.
pub const RESETS_PER_SECOND: u32 = 10;
/// How long a cleanly closed connection stays in the table, so it can still
/// ack a retransmitted FIN.
pub const CLOSE_LINGER: Duration = Duration::from_secs(5);
/// Random connection ids tried before `connect` gives up on a peer.
const ID_ATTEMPTS: usize = 64;
/// The timer's sleep when no connection has a deadline.
const IDLE_SLEEP: Duration = Duration::from_secs(60);
/// The timer never spins faster than this, even on a deadline in the past.
const MIN_TIMER_SLEEP: Duration = Duration::from_millis(1);
/// Consecutive `recv_from` errors after which the loop sleeps [`ERROR_PAUSE`].
const ERROR_BURST: u32 = 16;
const ERROR_PAUSE: Duration = Duration::from_millis(50);
/// Chunk used to discard data that arrives for a dropped stream.
const DISCARD_CHUNK: usize = 4096;

/// A connection key: the peer and the id it puts in packets to us.
pub(crate) type Key = (SocketAddr, u16);
type Slot = Arc<Mutex<ConnSlot>>;
type AcceptFilter = Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One connection and the tasks waiting on it.
#[derive(Debug)]
pub(crate) struct ConnSlot {
    pub(crate) conn: Connection,
    pub(crate) peer: SocketAddr,
    pub(crate) read_waker: Option<Waker>,
    /// Also used by `connect` and `poll_shutdown`, which never run
    /// alongside a write on the same stream.
    pub(crate) write_waker: Option<Waker>,
    /// Set when the socket ended the connection itself (shutdown or reap).
    pub(crate) local_error: Option<UtpError>,
    /// No stream owns this connection any more: incoming data is discarded.
    pub(crate) stream_dropped: bool,
    /// When the slot leaves the table regardless of the connection's state.
    pub(crate) reap_at: Option<Instant>,
}

impl ConnSlot {
    fn new(conn: Connection, peer: SocketAddr) -> ConnSlot {
        ConnSlot {
            conn,
            peer,
            read_waker: None,
            write_waker: None,
            local_error: None,
            stream_dropped: false,
            reap_at: None,
        }
    }

    pub(crate) fn error(&self) -> Option<UtpError> {
        self.local_error.clone().or_else(|| self.conn.error())
    }

    fn deadline(&self) -> Option<Instant> {
        if self.local_error.is_some() {
            return None;
        }
        [self.conn.next_deadline(), self.reap_at]
            .into_iter()
            .flatten()
            .min()
    }

    pub(crate) fn reap_by(&mut self, t: Option<Instant>) {
        if let Some(t) = t {
            self.reap_at = Some(self.reap_at.map_or(t, |r| r.min(t)));
        }
    }
}

/// What to do once a slot's lock is released.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    out: Vec<Vec<u8>>,
    wakers: Vec<Waker>,
    remove: bool,
    deadline: Option<Instant>,
}

/// A `ST_RESET` packet for connection id `conn_id`.
fn reset_packet(conn_id: u16, seq_nr: u16, ack_nr: u16) -> Vec<u8> {
    let p = Packet {
        ty: PacketType::Reset,
        conn_id,
        timestamp_us: 0,
        timestamp_diff_us: 0,
        wnd_size: 0,
        seq_nr,
        ack_nr,
        sack: None,
        payload: Bytes::new(),
    };
    let mut out = Vec::with_capacity(crate::HEADER_LEN);
    p.encode(&mut out);
    out
}

/// State shared by the socket handles, its streams, its handler and tasks.
pub(crate) struct Shared {
    sender: Arc<dyn DatagramSender>,
    pub(crate) cfg: UtpConfig,
    table: Mutex<HashMap<Key, Slot>>,
    /// Accepted connections not yet handed out by `accept`.
    pending: Mutex<VecDeque<(Key, Slot)>>,
    accept_ready: Notify,
    filter: Mutex<Option<AcceptFilter>>,
    /// Wakes the timer task: a deadline moved earlier, or shutdown.
    timer: Arc<Notify>,
    /// The time the timer task sleeps until; `None` while it scans.
    armed: Mutex<Option<Instant>>,
    closed: AtomicBool,
    /// Start of the current reset window and resets sent in it.
    resets: Mutex<(Instant, u32)>,
    /// Stops the receive task of an owned socket.
    recv_stop: Option<Arc<Notify>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("local", &self.sender.local_addr())
            .field("connections", &lock(&self.table).len())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // notify_one stores a permit, so a task not yet waiting still sees it.
        self.timer.notify_one();
        if let Some(s) = &self.recv_stop {
            s.notify_one();
        }
    }
}

impl Shared {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn send_all(&self, to: SocketAddr, out: &[Vec<u8>]) {
        for d in out {
            match self.sender.send_to(d, to) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tracing::trace!(%to, "utp datagram dropped: send buffer full");
                }
                Err(e) => tracing::debug!(%to, "utp send_to: {e}"),
            }
        }
    }

    /// Makes the timer wake by `d` if it sleeps longer.
    fn reschedule(&self, d: Option<Instant>) {
        let Some(d) = d else { return };
        let mut armed = lock(&self.armed);
        if armed.is_none_or(|a| d < a) {
            *armed = Some(d);
            drop(armed);
            self.timer.notify_one();
        }
    }

    /// Collects what a slot has to send and decides its fate. Runs under
    /// the slot lock; `wake` hands its wakers out (its state changed).
    pub(crate) fn settle(&self, s: &mut ConnSlot, now: Instant, wake: bool) -> Settled {
        if s.local_error.is_some() {
            return Settled {
                wakers: take_wakers(s),
                remove: true,
                ..Settled::default()
            };
        }
        if s.stream_dropped {
            let mut sink = [0u8; DISCARD_CHUNK];
            while s.conn.read(&mut sink) > 0 {}
        }
        let mut out = Vec::new();
        while let Some(d) = s.conn.poll_transmit(now) {
            out.push(d);
        }
        let mut remove = false;
        if s.conn.state() == ConnState::Closed {
            if s.conn.error().is_some() {
                remove = true;
            } else {
                s.reap_by(now.checked_add(CLOSE_LINGER));
            }
        }
        if s.reap_at.is_some_and(|r| now >= r) {
            remove = true;
            if s.conn.state() != ConnState::Closed {
                out.push(reset_packet(s.conn.send_id(), 0, 0));
                s.local_error = Some(UtpError::Closed);
            }
        }
        Settled {
            out,
            wakers: if wake || remove {
                take_wakers(s)
            } else {
                Vec::new()
            },
            remove,
            deadline: s.deadline(),
        }
    }

    /// Acts on a settled slot after its lock is released.
    pub(crate) fn finish(&self, key: Key, slot: &Slot, peer: SocketAddr, st: Settled) {
        self.send_all(peer, &st.out);
        for w in st.wakers {
            w.wake();
        }
        if st.remove {
            self.remove(key, slot);
        } else {
            self.reschedule(st.deadline);
        }
    }

    /// Removes `slot` from the table and the accept queue.
    pub(crate) fn remove(&self, key: Key, slot: &Slot) {
        {
            let mut table = lock(&self.table);
            if table.get(&key).is_some_and(|s| Arc::ptr_eq(s, slot)) {
                table.remove(&key);
            }
        }
        lock(&self.pending).retain(|(_, s)| !Arc::ptr_eq(s, slot));
    }

    fn handle(&self, from: SocketAddr, data: &[u8]) {
        if self.is_closed() {
            return;
        }
        let Ok(p) = Packet::decode(&Bytes::copy_from_slice(data)) else {
            return;
        };
        let now = Instant::now();
        // A SYN carries the initiator's receive id; ours is one more.
        let id = if p.ty == PacketType::Syn {
            p.conn_id.wrapping_add(1)
        } else {
            p.conn_id
        };
        let key = (from, id);
        let slot = lock(&self.table).get(&key).cloned();
        match (slot, p.ty) {
            (Some(slot), _) => self.deliver(key, &slot, &p, now),
            (None, PacketType::Syn) => self.incoming_syn(key, &p, now),
            // Never answer a reset: two sockets would bounce them forever.
            (None, PacketType::Reset) => {}
            (None, _) => self.unknown(from, &p, now),
        }
    }

    fn deliver(&self, key: Key, slot: &Slot, p: &Packet, now: Instant) {
        let (peer, st) = {
            let mut s = lock(slot);
            if s.local_error.is_some() {
                return;
            }
            s.conn.on_packet(p, now);
            let st = self.settle(&mut s, now, true);
            (s.peer, st)
        };
        self.finish(key, slot, peer, st);
    }

    /// Room for one more pending connection.
    fn has_room(&self) -> bool {
        let table = lock(&self.table);
        table.len() < self.cfg.max_connections
            && lock(&self.pending).len() < self.cfg.max_pending_accept
    }

    fn incoming_syn(&self, key: Key, syn: &Packet, now: Instant) {
        // Checked before any allocation, so a flood costs a lookup each.
        if !self.has_room() {
            return;
        }
        let filter = lock(&self.filter).clone();
        if filter.is_some_and(|f| !f(key.0)) {
            return;
        }
        let isn: u16 = rand::rng().random();
        let mut conn = Connection::accept(&self.cfg, key.0, syn, isn, now);
        let mut out = Vec::new();
        while let Some(d) = conn.poll_transmit(now) {
            out.push(d);
        }
        let deadline = conn.next_deadline();
        let slot = Arc::new(Mutex::new(ConnSlot::new(conn, key.0)));
        {
            let mut table = lock(&self.table);
            let mut pending = lock(&self.pending);
            if table.contains_key(&key)
                || table.len() >= self.cfg.max_connections
                || pending.len() >= self.cfg.max_pending_accept
            {
                return;
            }
            table.insert(key, Arc::clone(&slot));
            pending.push_back((key, slot));
        }
        self.send_all(key.0, &out);
        self.accept_ready.notify_one();
        self.reschedule(deadline);
    }

    /// A packet for no connection we know: one rate-limited reset.
    fn unknown(&self, from: SocketAddr, p: &Packet, now: Instant) {
        {
            let mut r = lock(&self.resets);
            if now.saturating_duration_since(r.0) >= Duration::from_secs(1) {
                *r = (now, 0);
            }
            if r.1 >= RESETS_PER_SECOND {
                return;
            }
            r.1 += 1;
        }
        let seq: u16 = rand::rng().random();
        self.send_all(from, &[reset_packet(p.conn_id, seq, p.seq_nr)]);
    }

    /// Runs every due timer; returns when the timer should next wake.
    fn run_timers(&self) -> Instant {
        *lock(&self.armed) = None;
        let now = Instant::now();
        let slots: Vec<(Key, Slot)> = lock(&self.table)
            .iter()
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect();
        let mut next: Option<Instant> = None;
        for (key, slot) in slots {
            let (peer, st) = {
                let mut s = lock(&slot);
                let due =
                    s.local_error.is_none() && s.conn.next_deadline().is_some_and(|d| now >= d);
                if due {
                    s.conn.on_timer(now);
                }
                let st = self.settle(&mut s, now, due);
                (s.peer, st)
            };
            if !st.remove {
                next = [next, st.deadline].into_iter().flatten().min();
            }
            self.send_all(peer, &st.out);
            for w in st.wakers {
                w.wake();
            }
            if st.remove {
                self.remove(key, &slot);
            }
        }
        let floor = now.checked_add(MIN_TIMER_SLEEP).unwrap_or(now);
        let until = next
            .or_else(|| now.checked_add(IDLE_SLEEP))
            .unwrap_or(floor)
            .max(floor);
        // A deadline set during the scan left a permit: the next wait
        // returns at once and scans again.
        *lock(&self.armed) = Some(until);
        until
    }

    fn shutdown(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let slots: Vec<(Key, Slot)> = lock(&self.table).drain().collect();
        lock(&self.pending).clear();
        for (_, slot) in slots {
            let (peer, out, wakers) = {
                let mut s = lock(&slot);
                let mut out = Vec::new();
                if s.error().is_none() && s.conn.state() != ConnState::Closed {
                    out.push(reset_packet(s.conn.send_id(), 0, 0));
                    s.local_error = Some(UtpError::Closed);
                }
                (s.peer, out, take_wakers(&mut s))
            };
            self.send_all(peer, &out);
            for w in wakers {
                w.wake();
            }
        }
        self.accept_ready.notify_waiters();
        self.timer.notify_one();
        if let Some(s) = &self.recv_stop {
            s.notify_one();
        }
    }
}

fn take_wakers(s: &mut ConnSlot) -> Vec<Waker> {
    [s.read_waker.take(), s.write_waker.take()]
        .into_iter()
        .flatten()
        .collect()
}

/// Feeds datagrams to the socket. Holds it weakly: the socket ends with its
/// last handle, whoever owns the handler.
#[derive(Debug)]
struct Handler(Weak<Shared>);

impl DatagramHandler for Handler {
    fn handle(&self, from: SocketAddr, data: &[u8]) {
        if let Some(s) = self.0.upgrade() {
            s.handle(from, data);
        }
    }
}

/// The sending half of an owned UDP socket (a non-blocking std clone:
/// tokio's `try_send_to` fails until the first readiness event).
#[derive(Debug)]
struct OwnedSender {
    socket: std::net::UdpSocket,
    local: SocketAddr,
}

impl DatagramSender for OwnedSender {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(data, to)
    }
}

/// A uTP endpoint. Clones share it; it lives while any clone or stream does.
#[derive(Debug, Clone)]
pub struct UtpSocket {
    inner: Arc<Shared>,
}

impl UtpSocket {
    /// Shared mode: sends through `sender`, receives what is fed to
    /// [`UtpSocket::handler`]. Must run inside a tokio runtime.
    pub fn new(sender: Arc<dyn DatagramSender>, cfg: UtpConfig) -> UtpSocket {
        UtpSocket::build(sender, cfg, None)
    }

    fn build(
        sender: Arc<dyn DatagramSender>,
        cfg: UtpConfig,
        recv_stop: Option<Arc<Notify>>,
    ) -> UtpSocket {
        let timer = Arc::new(Notify::new());
        let inner = Arc::new(Shared {
            sender,
            cfg,
            table: Mutex::new(HashMap::new()),
            pending: Mutex::new(VecDeque::new()),
            accept_ready: Notify::new(),
            filter: Mutex::new(None),
            timer: Arc::clone(&timer),
            armed: Mutex::new(None),
            closed: AtomicBool::new(false),
            resets: Mutex::new((Instant::now(), 0)),
            recv_stop,
        });
        tokio::spawn(timer_loop(Arc::downgrade(&inner), timer));
        UtpSocket { inner }
    }

    /// Standalone mode: binds and owns a UDP socket and its receive task,
    /// which ends on [`UtpSocket::shutdown`] or when the last handle drops.
    pub async fn bind(addr: SocketAddr, cfg: UtpConfig) -> io::Result<UtpSocket> {
        let std_sock = std::net::UdpSocket::bind(addr)?;
        std_sock.set_nonblocking(true)?;
        let send = std_sock.try_clone()?;
        send.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_sock)?;
        let local = socket.local_addr()?;
        let stop = Arc::new(Notify::new());
        let s = UtpSocket::build(
            Arc::new(OwnedSender {
                socket: send,
                local,
            }),
            cfg,
            Some(Arc::clone(&stop)),
        );
        tokio::spawn(receive_loop(socket, Arc::downgrade(&s.inner), stop));
        Ok(s)
    }

    /// The handler to register for uTP datagrams (shared mode).
    pub fn handler(&self) -> Arc<dyn DatagramHandler> {
        Arc::new(Handler(Arc::downgrade(&self.inner)))
    }

    /// The address datagrams leave from.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.sender.local_addr()
    }

    /// Dials `to`; fails with `ConnectTimeout` after `connect_timeout`.
    pub async fn connect(&self, to: SocketAddr) -> Result<UtpStream, UtpError> {
        let sh = &self.inner;
        if sh.is_closed() {
            return Err(UtpError::Closed);
        }
        let now = Instant::now();
        let (key, slot, out, deadline) = {
            let mut table = lock(&sh.table);
            if table.len() >= sh.cfg.max_connections {
                return Err(UtpError::TooManyConnections);
            }
            let mut rng = rand::rng();
            let Some(id) = (0..ID_ATTEMPTS)
                .map(|_| rng.random::<u16>())
                .find(|id| !table.contains_key(&(to, *id)))
            else {
                return Err(UtpError::TooManyConnections);
            };
            let mut conn = Connection::connect(&sh.cfg, to, id, now);
            let mut out = Vec::new();
            while let Some(d) = conn.poll_transmit(now) {
                out.push(d);
            }
            let deadline = conn.next_deadline();
            let slot = Arc::new(Mutex::new(ConnSlot::new(conn, to)));
            table.insert((to, id), Arc::clone(&slot));
            ((to, id), slot, out, deadline)
        };
        sh.send_all(to, &out);
        sh.reschedule(deadline);

        // Removes the attempt if this future is dropped or fails.
        let mut guard = ConnectGuard {
            shared: sh,
            key,
            slot: &slot,
            done: false,
        };
        let r = poll_fn(|cx| {
            let mut s = lock(&slot);
            if let Some(e) = s.error() {
                return Poll::Ready(Err(e));
            }
            match s.conn.state() {
                ConnState::SynSent => {
                    s.write_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                ConnState::Closed => Poll::Ready(Err(UtpError::Closed)),
                ConnState::Connected | ConnState::FinSent => Poll::Ready(Ok(())),
            }
        })
        .await;
        r?;
        guard.done = true;
        drop(guard);
        Ok(UtpStream::new(Arc::clone(sh), key, slot, to))
    }

    /// The next incoming connection and its peer address.
    pub async fn accept(&self) -> Result<(UtpStream, SocketAddr), UtpError> {
        let sh = &self.inner;
        loop {
            let mut notified = pin!(sh.accept_ready.notified());
            notified.as_mut().enable();
            if sh.is_closed() {
                return Err(UtpError::Closed);
            }
            let next = lock(&sh.pending).pop_front();
            if let Some((key, slot)) = next {
                let (alive, peer) = {
                    let s = lock(&slot);
                    (s.error().is_none(), s.peer)
                };
                if alive {
                    return Ok((UtpStream::new(Arc::clone(sh), key, slot, peer), peer));
                }
                sh.remove(key, &slot);
                continue;
            }
            notified.await;
        }
    }

    /// Drops SYNs whose source fails `f` (never acked, so the dialer sees
    /// `ConnectTimeout`); `None` accepts everyone.
    pub fn set_accept_filter(&self, f: Option<Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>>) {
        *lock(&self.inner.filter) = f;
    }

    /// Connections in the table: pending, established and closing.
    pub fn num_connections(&self) -> usize {
        lock(&self.inner.table).len()
    }

    /// Resets every connection and stops the socket's tasks; idempotent.
    /// Streams and pending `accept`/`connect` calls wake with an error.
    pub async fn shutdown(&self) {
        self.inner.shutdown();
    }
}

struct ConnectGuard<'a> {
    shared: &'a Shared,
    key: Key,
    slot: &'a Slot,
    done: bool,
}

impl Drop for ConnectGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.shared.remove(self.key, self.slot);
        }
    }
}

async fn timer_loop(shared: Weak<Shared>, wake: Arc<Notify>) {
    loop {
        let until = {
            let Some(sh) = shared.upgrade() else { return };
            if sh.is_closed() {
                return;
            }
            sh.run_timers()
        };
        let until = tokio::time::Instant::from_std(until);
        let _ = tokio::time::timeout_at(until, wake.notified()).await;
    }
}

async fn receive_loop(socket: UdpSocket, shared: Weak<Shared>, stop: Arc<Notify>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let mut errors = 0u32;
    loop {
        let res = {
            let mut stopped = pin!(stop.notified());
            let mut rb = ReadBuf::new(&mut buf);
            let r = poll_fn(|cx| {
                if stopped.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                socket
                    .poll_recv_from(cx, &mut rb)
                    .map(|r| Some(r.map(|from| (rb.filled().len(), from))))
            })
            .await;
            match r {
                None => break,
                Some(r) => r,
            }
        };
        match res {
            Ok((n, from)) => {
                errors = 0;
                let Some(sh) = shared.upgrade() else { break };
                if sh.is_closed() {
                    break;
                }
                if let Some(data) = buf.get(..n) {
                    sh.handle(from, data);
                }
            }
            Err(e) => {
                // Windows reports ICMP port-unreachable as ConnectionReset.
                tracing::debug!("utp recv_from: {e}");
                errors = errors.saturating_add(1);
                if errors >= ERROR_BURST {
                    tokio::time::sleep(ERROR_PAUSE).await;
                }
            }
        }
    }
}
