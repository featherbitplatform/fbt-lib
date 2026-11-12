//! The tokio runtime for uTP: one [`UtpSocket`] multiplexes connections over
//! a shared datagram sender (or a UDP socket it owns), drives their timers
//! and hands established ones out as [`UtpStream`]s.
//!
//! Locking: the connection table, the accept queue, the deadline index and
//! each connection slot have their own mutex. A slot lock may be followed by
//! the deadline-index lock, never the reverse; no slot lock is taken while
//! the table or queue lock is held; no two slot locks are held together; no
//! lock is held across an await; datagrams are sent with every lock
//! released.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::poll_fn;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use crate::conn::{ConnState, Connection, HOLE_RESERVE, UtpConfig};
use crate::packet::{Packet, PacketError, PacketType};
use crate::stream::UtpStream;

/// The receive buffer of an owned socket: the largest UDP payload.
pub const MAX_DATAGRAM: usize = 65_536;
/// `ST_RESET` replies to unknown traffic, per socket and second.
pub const RESETS_PER_SECOND: u32 = 10;
/// How long a cleanly closed connection stays in the table, so it can still
/// ack a retransmitted FIN.
pub const CLOSE_LINGER: Duration = Duration::from_secs(5);
/// Half-open incoming connections (SYN acked, nothing else heard yet) kept
/// per source IP, so one host cannot take every half-open place.
pub const MAX_PENDING_PER_IP: usize = 8;
/// Random connection ids tried before `connect` gives up on a peer.
const ID_ATTEMPTS: usize = 64;
/// The timer's sleep when no connection has a deadline.
const IDLE_SLEEP: Duration = Duration::from_secs(60);
/// The timer never spins faster than this, even on a deadline in the past.
const MIN_TIMER_SLEEP: Duration = Duration::from_millis(1);
/// Consecutive `recv_from` errors after which the loop sleeps [`ERROR_PAUSE`].
const ERROR_BURST: u32 = 16;
const ERROR_PAUSE: Duration = Duration::from_millis(50);

/// A connection key: the peer and the id it puts in packets to us.
pub(crate) type Key = (SocketAddr, u16);
type Slot = Arc<Mutex<ConnSlot>>;
type AcceptFilter = Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One connection and the tasks waiting on it.
pub(crate) struct ConnSlot {
    pub(crate) conn: Connection,
    pub(crate) peer: SocketAddr,
    key: Key,
    /// Created by an incoming SYN (as opposed to `connect`).
    acceptor: bool,
    pub(crate) read_waker: Option<Waker>,
    /// Also used by `connect` and `poll_shutdown`, which never run
    /// alongside a write on the same stream.
    pub(crate) write_waker: Option<Waker>,
    /// Set when the socket ended the connection itself (shutdown or reap).
    pub(crate) local_error: Option<UtpError>,
    /// No stream owns this connection any more.
    pub(crate) stream_dropped: bool,
    /// When the slot leaves the table regardless of the connection's state.
    pub(crate) reap_at: Option<Instant>,
    /// Acceptor not yet confirmed by a non-SYN packet: reaped at this time.
    unconfirmed_until: Option<Instant>,
    /// This slot's entry in the deadline index.
    scheduled: Option<Instant>,
}

impl std::fmt::Debug for ConnSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnSlot")
            .field("peer", &self.peer)
            .field("recv_id", &self.conn.recv_id())
            .field("send_id", &self.conn.send_id())
            .field("state", &self.conn.state())
            .field("error", &self.error())
            .finish_non_exhaustive()
    }
}

/// What a waiting reader or writer may care about, compared before and
/// after a change to wake only the side whose condition moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Readiness {
    readable: usize,
    eof: bool,
    failed: bool,
    state: ConnState,
    fin_acked: bool,
    room: usize,
}

impl ConnSlot {
    fn new(conn: Connection, key: Key, acceptor: bool) -> ConnSlot {
        ConnSlot {
            conn,
            peer: key.0,
            key,
            acceptor,
            read_waker: None,
            write_waker: None,
            local_error: None,
            stream_dropped: false,
            reap_at: None,
            unconfirmed_until: None,
            scheduled: None,
        }
    }

    pub(crate) fn error(&self) -> Option<UtpError> {
        self.local_error.clone().or_else(|| self.conn.error())
    }

    pub(crate) fn readiness(&self) -> Readiness {
        Readiness {
            readable: self.conn.readable_bytes(),
            eof: self.conn.is_eof(),
            failed: self.error().is_some(),
            state: self.conn.state(),
            fin_acked: self.conn.is_fin_acked(),
            room: self.conn.send_room(),
        }
    }

    fn deadline(&self) -> Option<Instant> {
        if self.local_error.is_some() {
            return None;
        }
        [
            self.conn.next_deadline(),
            self.reap_at,
            self.unconfirmed_until,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(crate) fn reap_by(&mut self, t: Option<Instant>) {
        if let Some(t) = t {
            self.reap_at = Some(self.reap_at.map_or(t, |r| r.min(t)));
        }
    }

    fn take_wakers(&mut self, read: bool, write: bool) -> Vec<Waker> {
        let r = if read { self.read_waker.take() } else { None };
        let w = if write { self.write_waker.take() } else { None };
        [r, w].into_iter().flatten().collect()
    }
}

/// What to do once a slot's lock is released.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    out: Vec<Vec<u8>>,
    wakers: Vec<Waker>,
    remove: bool,
    /// The slot just left the half-open state.
    confirmed: bool,
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

/// The connections, and the half-open ones (incoming, not yet confirmed by
/// the dialler) in arrival order and counted per source IP.
#[derive(Debug, Default)]
struct Table {
    slots: HashMap<Key, Slot>,
    /// The slots we dialled (`connect`), a subset of `slots`, with the
    /// sequence number of their SYN.
    outgoing: HashMap<Key, u16>,
    /// Half-open slots (a subset of `slots`) and their arrival stamp.
    half_open: HashMap<Key, u64>,
    /// `half_open` by arrival stamp, oldest first.
    half_open_order: BTreeMap<u64, Key>,
    next_stamp: u64,
    half_open_per_ip: HashMap<IpAddr, usize>,
}

impl Table {
    fn half_open_from(&self, ip: IpAddr) -> usize {
        self.half_open_per_ip.get(&ip).copied().unwrap_or(0)
    }

    fn add_half_open(&mut self, key: Key) {
        if self.half_open.contains_key(&key) {
            return;
        }
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);
        self.half_open.insert(key, stamp);
        self.half_open_order.insert(stamp, key);
        *self.half_open_per_ip.entry(key.0.ip()).or_insert(0) += 1;
    }

    /// The oldest half-open slot.
    fn oldest_half_open(&self) -> Option<(Key, Slot)> {
        let (_, key) = self.half_open_order.first_key_value()?;
        Some((*key, Arc::clone(self.slots.get(key)?)))
    }

    /// Idempotent: only the first call for a key counts.
    fn drop_half_open(&mut self, key: Key) {
        if let Some(stamp) = self.half_open.remove(&key) {
            self.half_open_order.remove(&stamp);
            let ip = key.0.ip();
            if let Some(n) = self.half_open_per_ip.get_mut(&ip) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    self.half_open_per_ip.remove(&ip);
                }
            }
        }
    }

    fn holds(&self, key: Key, slot: &Slot) -> bool {
        self.slots.get(&key).is_some_and(|s| Arc::ptr_eq(s, slot))
    }
}

/// State shared by the socket handles, its streams, its handler and tasks.
pub(crate) struct Shared {
    sender: Arc<dyn DatagramSender>,
    pub(crate) cfg: UtpConfig,
    table: Mutex<Table>,
    /// Confirmed incoming connections not yet handed out by `accept`.
    pending: Mutex<VecDeque<(Key, Slot)>>,
    accept_ready: Notify,
    filter: Mutex<Option<AcceptFilter>>,
    /// Every slot's next deadline, earliest first.
    deadlines: Mutex<BTreeSet<(Instant, Key)>>,
    /// Wakes the timer task: a deadline moved earlier, or shutdown.
    timer: Arc<Notify>,
    /// The time the timer task sleeps until; `None` while it runs.
    armed: Mutex<Option<Instant>>,
    /// Slots the timer task has processed (for tests).
    timer_visits: AtomicU64,
    closed: AtomicBool,
    /// Start of the current reset window and resets sent in it.
    resets: Mutex<(Instant, u32)>,
    /// When the last undecodable datagram was logged, and how many were
    /// not logged since (at most one event per second).
    decode_log: Mutex<(Option<Instant>, u64)>,
    /// Stops the receive task of an owned socket.
    recv_stop: Option<Arc<Notify>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("local", &self.sender.local_addr())
            .field("connections", &lock(&self.table).slots.len())
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

    /// Moves the slot's deadline-index entry to `d`. Slot lock held.
    fn index(&self, s: &mut ConnSlot, d: Option<Instant>) {
        if s.scheduled == d {
            return;
        }
        let mut idx = lock(&self.deadlines);
        if let Some(old) = s.scheduled.take() {
            idx.remove(&(old, s.key));
        }
        if let Some(d) = d {
            idx.insert((d, s.key));
        }
        s.scheduled = d;
    }

    /// Collects what a slot has to send and decides its fate; runs under
    /// the slot lock. `before` is the readiness before the change being
    /// settled: the reader and writer are woken if theirs moved (`None`
    /// wakes nobody, for changes the stream made itself).
    pub(crate) fn settle(
        &self,
        s: &mut ConnSlot,
        now: Instant,
        before: Option<Readiness>,
    ) -> Settled {
        if s.local_error.is_some() {
            self.index(s, None);
            return Settled {
                wakers: s.take_wakers(true, true),
                remove: true,
                ..Settled::default()
            };
        }
        let mut out = Vec::new();
        while let Some(d) = s.conn.poll_transmit(now) {
            out.push(d);
        }
        let open = s.conn.state() != ConnState::Closed;
        let mut remove = false;
        if !open {
            if s.conn.error().is_some() {
                remove = true;
            } else {
                s.reap_by(now.checked_add(CLOSE_LINGER));
            }
        }
        if s.unconfirmed_until.is_some_and(|t| now >= t) {
            // Never confirmed: most likely a forged SYN, so no reset either.
            remove = true;
            s.local_error = Some(UtpError::TimedOut);
        } else if s.reap_at.is_some_and(|r| now >= r)
            || (s.stream_dropped && s.conn.readable_bytes() > 0)
        {
            // Lingered too long, or data arrived nobody will read.
            remove = true;
            if open {
                out.push(reset_packet(s.conn.send_id(), 0, 0));
                s.local_error = Some(UtpError::Closed);
            }
        }
        let wakers = match (remove, before) {
            (true, _) => s.take_wakers(true, true),
            (false, None) => Vec::new(),
            (false, Some(b)) => {
                let a = s.readiness();
                let changed = a.failed != b.failed || a.state != b.state;
                let read = changed || a.readable > b.readable || a.eof != b.eof;
                let write = changed || a.room > b.room || a.fin_acked != b.fin_acked;
                s.take_wakers(read, write)
            }
        };
        let deadline = if remove { None } else { s.deadline() };
        self.index(s, deadline);
        Settled {
            out,
            wakers,
            remove,
            confirmed: false,
            deadline,
        }
    }

    /// Acts on a settled slot after its lock is released.
    pub(crate) fn finish(&self, key: Key, slot: &Slot, peer: SocketAddr, st: Settled) {
        let deadline = st.deadline;
        let remove = st.remove;
        self.finish_quiet(key, slot, peer, st);
        if !remove {
            self.reschedule(deadline);
        }
    }

    /// `finish` without waking the timer (the timer itself calls this).
    fn finish_quiet(&self, key: Key, slot: &Slot, peer: SocketAddr, st: Settled) {
        self.send_all(peer, &st.out);
        for w in st.wakers {
            w.wake();
        }
        if st.remove {
            self.remove(key, slot);
        } else if st.confirmed {
            self.confirmed(key, slot);
        }
    }

    /// The dialler of a half-open slot proved it got our STATE: the slot
    /// leaves the half-open set and joins the accept queue, or is reset
    /// when that queue is full.
    fn confirmed(&self, key: Key, slot: &Slot) {
        let queued = {
            let mut table = lock(&self.table);
            if !table.holds(key, slot) {
                return;
            }
            table.drop_half_open(key);
            let mut pending = lock(&self.pending);
            if pending.len() < self.cfg.max_pending_accept {
                pending.push_back((key, Arc::clone(slot)));
                true
            } else {
                false
            }
        };
        if queued {
            self.accept_ready.notify_one();
            return;
        }
        tracing::debug!(peer = %key.0, "utp accept queue full; resetting a confirmed connection");
        let (peer, out) = {
            let mut s = lock(slot);
            if s.local_error.is_some() {
                return;
            }
            s.local_error = Some(UtpError::Closed);
            (s.peer, reset_packet(s.conn.send_id(), 0, 0))
        };
        self.send_all(peer, &[out]);
        self.remove(key, slot);
    }

    /// Drops a half-open slot to make room for a newer SYN. No reset: its
    /// SYN was most likely forged. A slot confirmed meanwhile is kept.
    fn evict(&self, key: Key, slot: &Slot) {
        {
            let mut s = lock(slot);
            if s.unconfirmed_until.is_none() || s.local_error.is_some() {
                return;
            }
            s.local_error = Some(UtpError::TimedOut);
        }
        self.remove(key, slot);
    }

    /// Removes `slot` from the table, the accept queue and the index.
    pub(crate) fn remove(&self, key: Key, slot: &Slot) {
        {
            let mut s = lock(slot);
            self.index(&mut s, None);
        }
        {
            let mut table = lock(&self.table);
            if table.holds(key, slot) {
                table.slots.remove(&key);
                table.outgoing.remove(&key);
                table.drop_half_open(key);
            }
        }
        lock(&self.pending).retain(|(_, s)| !Arc::ptr_eq(s, slot));
    }

    fn get(&self, key: Key) -> Option<Slot> {
        lock(&self.table).slots.get(&key).cloned()
    }

    fn handle(&self, from: SocketAddr, data: &[u8]) {
        if self.is_closed() {
            return;
        }
        let now = Instant::now();
        let p = match Packet::decode(&Bytes::copy_from_slice(data)) {
            Ok(p) => p,
            Err(e) => {
                self.decode_failed(from, data.len(), e, now);
                return;
            }
        };
        // A SYN carries the initiator's receive id; ours is one more.
        let id = if p.ty == PacketType::Syn {
            p.conn_id.wrapping_add(1)
        } else {
            p.conn_id
        };
        let key = (from, id);
        match (self.get(key), p.ty) {
            (Some(slot), _) => self.deliver(key, &slot, &p, now),
            (None, PacketType::Syn) => self.incoming_syn(key, &p, now),
            (None, PacketType::Reset) => self.reset_by_send_id(from, &p, now),
            (None, _) => self.unknown(from, &p, now),
        }
    }

    /// Logs an undecodable datagram, at most once per second; the events
    /// skipped meanwhile are counted in the next one's `suppressed`.
    fn decode_failed(&self, from: SocketAddr, len: usize, e: PacketError, now: Instant) {
        let suppressed = {
            let mut log = lock(&self.decode_log);
            if log
                .0
                .is_some_and(|t| now.saturating_duration_since(t) < Duration::from_secs(1))
            {
                log.1 = log.1.saturating_add(1);
                return;
            }
            log.0 = Some(now);
            std::mem::take(&mut log.1)
        };
        tracing::trace!(%from, len, error = %e, suppressed, "uTP datagram failed to decode");
    }

    /// A reset carrying a connection's send id (libutp answers with the id
    /// of the packet it got). That connection's receive id is one off.
    /// Never answered: two sockets would bounce resets forever.
    fn reset_by_send_id(&self, from: SocketAddr, p: &Packet, now: Instant) {
        for id in [p.conn_id.wrapping_add(1), p.conn_id.wrapping_sub(1)] {
            let key = (from, id);
            if let Some(slot) = self.get(key)
                && lock(&slot).conn.send_id() == p.conn_id
            {
                self.deliver(key, &slot, p, now);
                return;
            }
        }
    }

    fn deliver(&self, key: Key, slot: &Slot, p: &Packet, now: Instant) {
        let (peer, st) = {
            let mut s = lock(slot);
            if s.local_error.is_some() {
                return;
            }
            if p.ty == PacketType::Syn && !s.acceptor {
                tracing::debug!(peer = %s.peer, "utp SYN collides with an outgoing connection");
                return;
            }
            let before = s.readiness();
            s.conn.on_packet(p, now);
            // Only a packet the core accepted with a valid ack of our
            // handshake confirms: a blind spoofer cannot guess our ISN.
            let confirmed = s.conn.is_established() && s.unconfirmed_until.take().is_some();
            let mut st = self.settle(&mut s, now, Some(before));
            st.confirmed = confirmed;
            (s.peer, st)
        };
        self.finish(key, slot, peer, st);
    }

    /// Whether a SYN at `key` may take a half-open place, possibly the
    /// oldest one's: under the limits other than the half-open total, and
    /// no neighbouring id in use by that peer (as `connect` keeps them
    /// free), so a reset by send id matches one connection only; see
    /// [`syn_neighbours_free`] for the one exception.
    fn admissible(&self, table: &Table, pending: usize, key: Key, syn: &Packet) -> bool {
        table.slots.len() < self.cfg.max_connections
            && pending < self.cfg.max_pending_accept
            && table.half_open_from(key.0.ip()) < MAX_PENDING_PER_IP
            && syn_neighbours_free(table, key, syn)
    }

    /// A SYN for no known connection. It gets a half-open slot (and our
    /// STATE), but reaches `accept` only once the dialler confirms it (see
    /// `deliver`). At most `max_pending_accept` slots are half-open; a newer
    /// SYN evicts the oldest, so forged SYNs can neither fill the table
    /// nor keep an honest dialler out for longer than its round trip.
    fn incoming_syn(&self, key: Key, syn: &Packet, now: Instant) {
        // Checked before any allocation, so a flood costs a lookup each.
        {
            let table = lock(&self.table);
            let pending = lock(&self.pending).len();
            if !self.admissible(&table, pending, key, syn) {
                return;
            }
        }
        let filter = lock(&self.filter).clone();
        if filter.is_some_and(|f| !f(key.0)) {
            return;
        }
        let isn: u16 = rand::rng().random();
        let conn = Connection::accept(&self.cfg, key.0, syn, isn, now);
        let mut slot = ConnSlot::new(conn, key, true);
        slot.unconfirmed_until = now.checked_add(self.cfg.connect_timeout);
        let slot = Arc::new(Mutex::new(slot));
        // The final check, and the victim, are decided under the table
        // lock; the victim is evicted only after it is released (`evict`
        // takes the victim's slot lock, never held inside the table lock),
        // so a SYN that fails here evicts nobody.
        let victim = {
            let mut table = lock(&self.table);
            let pending = lock(&self.pending).len();
            if table.slots.contains_key(&key) || !self.admissible(&table, pending, key, syn) {
                return;
            }
            let victim = if table.half_open.len() < self.cfg.max_pending_accept {
                None
            } else {
                match table.oldest_half_open() {
                    Some(v) => Some(v),
                    None => return,
                }
            };
            // `handle` is not serialised (a shared socket's handler may be
            // called from any thread), so the victim leaves the half-open
            // set now: a concurrent SYN then picks another victim, and the
            // set never grows past `max_pending_accept`.
            if let Some((vkey, _)) = &victim {
                table.drop_half_open(*vkey);
            }
            table.slots.insert(key, Arc::clone(&slot));
            table.add_half_open(key);
            victim
        };
        if let Some((vkey, vslot)) = victim {
            self.evict(vkey, &vslot);
        }
        let st = self.settle(&mut lock(&slot), now, None);
        self.finish(key, &slot, key.0, st);
    }

    /// A packet for no connection we know: one rate-limited reset, echoing
    /// the packet's id (as libutp does).
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

    /// Runs the due timers (from the index, not a scan); returns when the
    /// timer should next wake.
    fn run_timers(&self) -> Instant {
        *lock(&self.armed) = None;
        let now = Instant::now();
        let due: Vec<(Instant, Key)> = {
            let mut idx = lock(&self.deadlines);
            let mut due = Vec::new();
            while let Some(&first) = idx.first() {
                if first.0 > now {
                    break;
                }
                idx.pop_first();
                due.push(first);
            }
            due
        };
        for (t, key) in due {
            let Some(slot) = self.get(key) else { continue };
            let (peer, st) = {
                let mut s = lock(&slot);
                if s.scheduled != Some(t) {
                    continue; // stale: the entry belonged to an older slot
                }
                s.scheduled = None;
                self.timer_visits.fetch_add(1, Ordering::Relaxed);
                let before = s.readiness();
                if s.local_error.is_none() && s.conn.next_deadline().is_some_and(|d| now >= d) {
                    s.conn.on_timer(now);
                }
                let st = self.settle(&mut s, now, Some(before));
                (s.peer, st)
            };
            self.finish_quiet(key, &slot, peer, st);
        }
        let next = lock(&self.deadlines).first().map(|e| e.0);
        let floor = now.checked_add(MIN_TIMER_SLEEP).unwrap_or(now);
        let until = next
            .or_else(|| now.checked_add(IDLE_SLEEP))
            .unwrap_or(floor)
            .max(floor);
        // A deadline set while this ran left a permit: the next wait
        // returns at once and runs again.
        *lock(&self.armed) = Some(until);
        until
    }

    fn shutdown(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let slots: Vec<(Key, Slot)> = {
            let mut table = lock(&self.table);
            table.half_open.clear();
            table.half_open_order.clear();
            table.half_open_per_ip.clear();
            table.outgoing.clear();
            table.slots.drain().collect()
        };
        lock(&self.pending).clear();
        for (_, slot) in slots {
            let (peer, out, wakers) = {
                let mut s = lock(&slot);
                let mut out = Vec::new();
                if s.error().is_none() && s.conn.state() != ConnState::Closed {
                    out.push(reset_packet(s.conn.send_id(), 0, 0));
                    s.local_error = Some(UtpError::Closed);
                }
                self.index(&mut s, None);
                (s.peer, out, s.take_wakers(true, true))
            };
            self.send_all(peer, &out);
            for w in wakers {
                w.wake();
            }
        }
        lock(&self.deadlines).clear();
        self.accept_ready.notify_waiters();
        self.timer.notify_one();
        if let Some(s) = &self.recv_stop {
            s.notify_one();
        }
    }
}

/// No slot uses an id within 2 of `key`'s for the same peer (`key` itself
/// is not checked).
fn neighbours_free(table: &Table, key: Key) -> bool {
    [1u16, 2]
        .into_iter()
        .flat_map(|d| [key.1.wrapping_add(d), key.1.wrapping_sub(d)])
        .all(|id| !table.slots.contains_key(&(key.0, id)))
}

/// [`neighbours_free`] for an incoming `syn` at `key`, with one exception:
/// the id just below may be a connection we dialled to the same address
/// whose own SYN this looks like (same id, same sequence number). That is
/// our own SYN coming back when a socket connects to itself: the dialler's
/// receive id is the SYN's id, the acceptor's one more.
///
/// The pair is not fully separable. Data packets are (to the dialler they
/// carry its receive id, to the acceptor the dialler's send id, both
/// distinct keys), but a reset that echoes the acceptor's send id (as
/// libutp answers an unknown packet) carries the dialler's receive id and
/// lands on the dialler. Every dialler starts at the same sequence number
/// (as libutp does), so a remote peer that drew our id by chance (1 in
/// 65536 per concurrent dial) qualifies too and shares that caveat; before
/// this exception its connect simply timed out.
fn syn_neighbours_free(table: &Table, key: Key, syn: &Packet) -> bool {
    let below = (key.0, key.1.wrapping_sub(1));
    let own_syn = table.outgoing.get(&below) == Some(&syn.seq_nr);
    [1u16, 2]
        .into_iter()
        .flat_map(|d| [key.1.wrapping_add(d), key.1.wrapping_sub(d)])
        .all(|id| {
            let k = (key.0, id);
            !table.slots.contains_key(&k) || (k == below && own_syn)
        })
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
    ///
    /// `recv_buffer` and `send_buffer` are raised to at least one packet
    /// (the larger payload size, and never below 1472 bytes), so a
    /// connection can always make progress.
    pub fn new(sender: Arc<dyn DatagramSender>, cfg: UtpConfig) -> UtpSocket {
        UtpSocket::build(sender, cfg, None)
    }

    fn build(
        sender: Arc<dyn DatagramSender>,
        mut cfg: UtpConfig,
        recv_stop: Option<Arc<Notify>>,
    ) -> UtpSocket {
        let floor = cfg.max_payload_v4.max(cfg.max_payload_v6).max(HOLE_RESERVE);
        cfg.recv_buffer = cfg.recv_buffer.max(floor);
        cfg.send_buffer = cfg.send_buffer.max(floor);
        let timer = Arc::new(Notify::new());
        let inner = Arc::new(Shared {
            sender,
            cfg,
            table: Mutex::new(Table::default()),
            pending: Mutex::new(VecDeque::new()),
            accept_ready: Notify::new(),
            filter: Mutex::new(None),
            deadlines: Mutex::new(BTreeSet::new()),
            timer: Arc::clone(&timer),
            armed: Mutex::new(None),
            timer_visits: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            resets: Mutex::new((Instant::now(), 0)),
            decode_log: Mutex::new((None, 0)),
            recv_stop,
        });
        tokio::spawn(timer_loop(Arc::downgrade(&inner), timer));
        UtpSocket { inner }
    }

    /// Standalone mode: binds and owns a UDP socket and its receive task,
    /// which ends on [`UtpSocket::shutdown`] or when the last handle drops.
    /// Buffers are clamped as in [`UtpSocket::new`].
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
        let (key, slot) = {
            let mut table = lock(&sh.table);
            if table.slots.len() >= sh.cfg.max_connections {
                return Err(UtpError::TooManyConnections);
            }
            // Keep the neighbouring ids free too: an incoming SYN from the
            // same peer must not land on this connection, and a reset by
            // send id must match one connection only.
            let free =
                |id: u16| !table.slots.contains_key(&(to, id)) && neighbours_free(&table, (to, id));
            let mut rng = rand::rng();
            let Some(id) = (0..ID_ATTEMPTS)
                .map(|_| rng.random::<u16>())
                .find(|id| free(*id))
            else {
                return Err(UtpError::TooManyConnections);
            };
            let conn = Connection::connect(&sh.cfg, to, id, now);
            let isn = conn.isn();
            let slot = Arc::new(Mutex::new(ConnSlot::new(conn, (to, id), false)));
            table.slots.insert((to, id), Arc::clone(&slot));
            table.outgoing.insert((to, id), isn);
            ((to, id), slot)
        };
        let st = sh.settle(&mut lock(&slot), now, None);
        sh.finish(key, &slot, to, st);

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
    ///
    /// A SYN alone is never handed out: an incoming connection is queued
    /// here only once the dialer confirmed it with a packet that acks our
    /// handshake STATE (in BitTorrent its first STATE or the data of its
    /// handshake, which the dialer always sends first). One not confirmed
    /// within `connect_timeout` of the SYN is dropped. So a protocol whose
    /// acceptor must speak first cannot run over an accepted stream.
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
        lock(&self.inner.table).slots.len()
    }

    /// Slots the timer task has processed so far (for tests).
    #[doc(hidden)]
    pub fn timer_visits(&self) -> u64 {
        self.inner.timer_visits.load(Ordering::Relaxed)
    }

    /// Resets every connection and stops the socket's tasks; idempotent.
    /// Streams and pending `accept`/`connect` calls wake with an error.
    pub async fn shutdown(&self) {
        self.shutdown_now();
    }

    /// [`UtpSocket::shutdown`] without awaiting: usable from `Drop` and
    /// other synchronous code. Idempotent.
    pub fn shutdown_now(&self) {
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
                tracing::debug!("utp recv_from: {e}");
                // Windows reports an ICMP unreachable for an earlier send as
                // an error on the next receive. That says nothing about this
                // socket, and a burst of them (replies to spoofed or dead
                // peers) must not stall the loop, so only other errors count
                // toward the pause.
                if !is_icmp_echo(&e) {
                    errors = errors.saturating_add(1);
                }
                if errors >= ERROR_BURST {
                    tokio::time::sleep(ERROR_PAUSE).await;
                }
            }
        }
    }
}

/// An error that reports an ICMP message about an earlier send (Windows
/// surfaces these on `recv_from`), not a fault of the socket.
fn is_icmp_echo(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::HostUnreachable
            | std::io::ErrorKind::NetworkUnreachable
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icmp_echoes_do_not_count_as_socket_errors() {
        use std::io::{Error, ErrorKind};
        assert!(is_icmp_echo(&Error::from(ErrorKind::ConnectionReset)));
        assert!(is_icmp_echo(&Error::from(ErrorKind::HostUnreachable)));
        assert!(!is_icmp_echo(&Error::from(ErrorKind::PermissionDenied)));
        assert!(!is_icmp_echo(&Error::from(ErrorKind::Other)));
    }

    fn syn(conn_id: u16, seq_nr: u16) -> Packet {
        Packet {
            ty: PacketType::Syn,
            conn_id,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 1 << 20,
            seq_nr,
            ack_nr: 0,
            sack: None,
            payload: Bytes::new(),
        }
    }

    /// A table holding one connection dialled to `peer` with receive id `id`.
    fn dialled(peer: SocketAddr, id: u16) -> (Table, u16) {
        let conn = Connection::connect(&UtpConfig::default(), peer, id, Instant::now());
        let isn = conn.isn();
        let mut table = Table::default();
        let slot = Arc::new(Mutex::new(ConnSlot::new(conn, (peer, id), false)));
        table.slots.insert((peer, id), slot);
        table.outgoing.insert((peer, id), isn);
        (table, isn)
    }

    #[test]
    fn only_our_own_syn_may_sit_next_to_our_dial() {
        let peer: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let id = 1000u16;
        let (table, isn) = dialled(peer, id);
        // Our own SYN coming back: its id is the dialler's, the acceptor's
        // key one more.
        let key = (peer, id.wrapping_add(1));
        assert!(syn_neighbours_free(&table, key, &syn(id, isn)));
        // Another sequence number is not our SYN.
        assert!(!syn_neighbours_free(
            &table,
            key,
            &syn(id, isn.wrapping_add(7))
        ));
        // Two above, or below the dial: the ordinary neighbour rule.
        let two_above = (peer, id.wrapping_add(2));
        assert!(!syn_neighbours_free(
            &table,
            two_above,
            &syn(id.wrapping_add(1), isn)
        ));
        let below = (peer, id.wrapping_sub(1));
        assert!(!syn_neighbours_free(
            &table,
            below,
            &syn(id.wrapping_sub(2), isn)
        ));
        // An accepted connection (not dialled) never qualifies.
        let mut accepted = Table::default();
        let conn = Connection::accept(&UtpConfig::default(), peer, &syn(id, 1), 5, Instant::now());
        let slot = Arc::new(Mutex::new(ConnSlot::new(conn, (peer, id), true)));
        accepted.slots.insert((peer, id), slot);
        assert!(!syn_neighbours_free(&accepted, key, &syn(id, isn)));
    }
}
