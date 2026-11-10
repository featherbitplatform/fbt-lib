//! Sans-IO uTP connection (BEP 29): handshake, reliable ordered stream,
//! selective acks, fast and timeout retransmission, FIN and LEDBAT.
//!
//! The caller owns the socket and the clock. It feeds decoded packets to
//! [`Connection::on_packet`], calls [`Connection::on_timer`] at
//! [`Connection::next_deadline`], and sends whatever
//! [`Connection::poll_transmit`] returns until it returns `None`.
//!
//! Every remote value is bounded here: out-of-order data by `recv_buffer`
//! bytes and [`MAX_REORDER_SPAN`] packets, unacked data by `send_buffer`
//! bytes and [`MAX_IN_FLIGHT_PACKETS`] packets.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::UtpError;
use crate::ledbat::Ledbat;
use crate::packet::{MAX_SACK_LEN, Packet, PacketType, sack_acks, seq_le, seq_lt};
use crate::rtt::RttEstimator;

/// Packets (data, SYN or FIN) sent and not yet acknowledged.
pub const MAX_IN_FLIGHT_PACKETS: usize = 1024;
/// Furthest ahead of `ack_nr` an out-of-order packet is kept, in packets.
/// The effective span is also bounded by `recv_buffer / min payload`.
pub const MAX_REORDER_SPAN: u16 = 0x4000;
/// Longest an in-order data packet waits for its ack.
pub const DELAYED_ACK: Duration = Duration::from_millis(100);
/// Duplicate acks (or later packets selectively acked) that mean a loss.
pub const DUP_ACK_THRESHOLD: u32 = 3;
/// Room out-of-order data always leaves free, so the packet that fills the
/// hole fits: at least the largest payload seen and 1472 bytes (an
/// Ethernet-MTU UDP payload, above libtorrent's 1452-byte uTP packets).
pub const HOLE_RESERVE: usize = 1472;
/// Sequence numbers a SACK mask can describe (bits of [`MAX_SACK_LEN`]).
const SACK_BITS: usize = MAX_SACK_LEN * 8;
/// Sequence number of the initiator's SYN (BEP 29).
const SYN_SEQ: u16 = 1;

/// `t + d`, or `None` (no deadline) when that overflows `Instant`.
fn after(t: Instant, d: Duration) -> Option<Instant> {
    t.checked_add(d)
}

/// `now` is at or past `t + d`; never true when the sum overflows.
fn reached(now: Instant, t: Instant, d: Duration) -> bool {
    after(t, d).is_some_and(|x| now >= x)
}

/// Connection limits and timers. Shared by the sans-IO core and the socket.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UtpConfig {
    /// Receive buffer (unread plus out-of-order bytes); also the advertised window.
    pub recv_buffer: usize,
    /// Unsent plus unacknowledged bytes accepted by `write`.
    pub send_buffer: usize,
    /// Payload bytes per packet to an IPv4 peer.
    pub max_payload_v4: usize,
    /// Payload bytes per packet to an IPv6 peer.
    pub max_payload_v6: usize,
    /// SYN at 0, retries at 1 s and 3 s, then `ConnectTimeout` at 7 s.
    pub connect_timeout: Duration,
    /// No packet received for this long gives `TimedOut`.
    pub idle_timeout: Duration,
    /// A STATE goes out after this long without sending anything.
    pub keepalive: Duration,
    /// Consecutive retransmission timeouts tolerated before `TimedOut`.
    pub max_retransmits: u32,
    /// Connections per socket (used by the socket runtime).
    pub max_connections: usize,
    /// Accepted connections waiting for `accept` (used by the socket runtime).
    pub max_pending_accept: usize,
}

impl Default for UtpConfig {
    fn default() -> Self {
        UtpConfig {
            recv_buffer: 1 << 20,
            send_buffer: 1 << 20,
            max_payload_v4: 1400,
            max_payload_v6: 1200,
            connect_timeout: Duration::from_secs(7),
            idle_timeout: Duration::from_secs(60),
            keepalive: Duration::from_secs(29),
            max_retransmits: 6,
            max_connections: 1000,
            max_pending_accept: 64,
        }
    }
}

/// Where the connection stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// SYN sent, no answer yet.
    SynSent,
    /// Established.
    Connected,
    /// Our FIN is sent; the peer may still be sending.
    FinSent,
    /// Done: cleanly (both FINs) or with [`Connection::error`].
    Closed,
}

/// A packet we sent and the peer has not acknowledged yet.
#[derive(Debug)]
struct Sent {
    seq: u16,
    ty: PacketType,
    payload: Bytes,
    sent_at: Instant,
    transmissions: u32,
    /// Known or presumed lost: not counted in flight, waiting to be resent.
    lost: bool,
    /// Selectively acknowledged (kept until the cumulative ack passes it).
    acked: bool,
    /// Already fast-retransmitted once; later losses wait for the RTO.
    fast_resent: bool,
}

/// One uTP connection, without I/O.
#[derive(Debug)]
pub struct Connection {
    cfg: UtpConfig,
    state: ConnState,
    error: Option<UtpError>,
    recv_id: u16,
    send_id: u16,
    created: Instant,
    /// Payload bytes per packet for this peer's family.
    mss: usize,
    /// Smaller of the two family payloads: sizes the reorder span.
    min_payload: usize,

    // Send side.
    /// Sequence number of the next packet we create.
    seq_nr: u16,
    unsent: VecDeque<u8>,
    outstanding: VecDeque<Sent>,
    /// Payload bytes of outstanding packets neither acked nor lost.
    flight: usize,
    /// Payload bytes of outstanding packets not acked.
    unacked_bytes: usize,
    closing: bool,
    fin_seq: Option<u16>,
    fin_acked: bool,
    peer_wnd: usize,
    /// Last cumulative ack the peer sent us.
    peer_ack: u16,
    dup_acks: u32,
    last_loss: Option<Instant>,
    rtt: RttEstimator,
    ledbat: Ledbat,
    /// Retransmission timer, running while anything is in flight.
    rto_at: Option<Instant>,
    timeouts: u32,
    /// The next packet may ignore the congestion window (fast retransmit).
    bypass_cwnd: bool,
    /// The next packet may ignore the peer's window (RTO or window probe).
    bypass_peer_wnd: bool,
    /// The packet last sent past the peer's window, resent when it reopens.
    probe_seq: Option<u16>,
    last_send: Instant,
    last_data_send: Instant,

    // Receive side.
    /// Last in-order sequence number received.
    ack_nr: u16,
    inbox: VecDeque<u8>,
    ooo: BTreeMap<u16, Vec<u8>>,
    ooo_bytes: usize,
    /// Largest DATA payload received, for the hole reserve.
    max_recv_payload: usize,
    remote_fin: Option<u16>,
    eof: bool,
    ack_now: bool,
    ack_due: Option<Instant>,
    unacked_recv: u32,
    /// Our receive time minus the peer's timestamp, echoed back.
    reply_micro: u32,
    last_recv: Instant,
    last_adv_wnd: usize,
    /// Acceptor: our first sequence number, and whether the handshake
    /// STATE (which carries it) still has to go out.
    isn: u16,
    is_acceptor: bool,
    syn_ack_pending: bool,
    /// Acceptor: the SYN's sequence number, to recognise a retransmission.
    syn_seq_seen: u16,
}

impl Connection {
    fn new(cfg: &UtpConfig, peer: SocketAddr, recv_id: u16, send_id: u16, now: Instant) -> Self {
        let mss = if peer.is_ipv4() {
            cfg.max_payload_v4
        } else {
            cfg.max_payload_v6
        }
        .max(1);
        let min_payload = cfg.max_payload_v4.min(cfg.max_payload_v6).max(1);
        Connection {
            cfg: cfg.clone(),
            state: ConnState::SynSent,
            error: None,
            recv_id,
            send_id,
            created: now,
            mss,
            min_payload,
            seq_nr: SYN_SEQ,
            unsent: VecDeque::new(),
            outstanding: VecDeque::new(),
            flight: 0,
            unacked_bytes: 0,
            closing: false,
            fin_seq: None,
            fin_acked: false,
            peer_wnd: 0,
            peer_ack: 0,
            dup_acks: 0,
            last_loss: None,
            rtt: RttEstimator::new(),
            ledbat: Ledbat::new(u32::try_from(mss).unwrap_or(u32::MAX)),
            rto_at: None,
            timeouts: 0,
            bypass_cwnd: false,
            bypass_peer_wnd: false,
            probe_seq: None,
            last_send: now,
            last_data_send: now,
            ack_nr: 0,
            inbox: VecDeque::new(),
            ooo: BTreeMap::new(),
            ooo_bytes: 0,
            max_recv_payload: 0,
            remote_fin: None,
            eof: false,
            ack_now: false,
            ack_due: None,
            unacked_recv: 0,
            reply_micro: 0,
            last_recv: now,
            last_adv_wnd: cfg.recv_buffer,
            isn: SYN_SEQ,
            is_acceptor: false,
            syn_ack_pending: false,
            syn_seq_seen: 0,
        }
    }

    /// Starts a connection to `peer`; a SYN is queued.
    /// `send_id = recv_id + 1` (BEP 29).
    pub fn connect(cfg: &UtpConfig, peer: SocketAddr, recv_id: u16, now: Instant) -> Connection {
        let mut c = Connection::new(cfg, peer, recv_id, recv_id.wrapping_add(1), now);
        c.peer_ack = SYN_SEQ.wrapping_sub(1);
        c.push_outstanding(PacketType::Syn, Bytes::new());
        c
    }

    /// Answers `syn` from `peer`; a STATE is queued.
    /// `recv_id = syn.conn_id + 1`, `send_id = syn.conn_id` (BEP 29).
    /// `initial_seq` is our first sequence number; BEP 29 asks for a random
    /// one, which the caller (the socket runtime) supplies.
    pub fn accept(
        cfg: &UtpConfig,
        peer: SocketAddr,
        syn: &Packet,
        initial_seq: u16,
        now: Instant,
    ) -> Connection {
        let mut c = Connection::new(cfg, peer, syn.conn_id.wrapping_add(1), syn.conn_id, now);
        c.state = ConnState::Connected;
        c.is_acceptor = true;
        c.seq_nr = initial_seq;
        c.peer_ack = c.seq_nr.wrapping_sub(1);
        c.ack_nr = syn.seq_nr;
        c.peer_wnd = syn.wnd_size as usize;
        c.reply_micro = c.timestamp(now).wrapping_sub(syn.timestamp_us);
        c.isn = c.seq_nr;
        c.syn_seq_seen = syn.seq_nr;
        c.syn_ack_pending = true;
        c
    }

    /// Our connection id (in packets the peer sends us).
    pub fn recv_id(&self) -> u16 {
        self.recv_id
    }

    /// The peer's connection id (in packets we send).
    pub fn send_id(&self) -> u16 {
        self.send_id
    }

    pub fn state(&self) -> ConnState {
        self.state
    }

    pub fn error(&self) -> Option<UtpError> {
        self.error.clone()
    }

    /// The peer's FIN was reached and every byte before it was read.
    pub fn is_eof(&self) -> bool {
        self.eof && self.inbox.is_empty()
    }

    /// Our FIN was sent and the peer acknowledged it.
    pub fn is_fin_acked(&self) -> bool {
        self.fin_acked
    }

    /// In-order bytes waiting for `read`.
    #[doc(hidden)]
    pub fn readable_bytes(&self) -> usize {
        self.inbox.len()
    }

    /// Bytes `write` would accept now.
    #[doc(hidden)]
    pub fn send_room(&self) -> usize {
        if self.closing || matches!(self.state, ConnState::Closed | ConnState::FinSent) {
            return 0;
        }
        self.cfg
            .send_buffer
            .saturating_sub(self.unsent.len() + self.unacked_bytes)
    }

    /// Out-of-order plus unread in-order receive bytes.
    #[doc(hidden)]
    pub fn buffered_bytes(&self) -> usize {
        self.ooo_bytes + self.inbox.len()
    }

    /// Current congestion window in bytes.
    #[doc(hidden)]
    pub fn cwnd(&self) -> u32 {
        self.ledbat.cwnd()
    }

    fn timestamp(&self, now: Instant) -> u32 {
        // Wrapping microseconds since creation.
        now.saturating_duration_since(self.created).as_micros() as u32
    }

    fn fail(&mut self, e: UtpError) {
        tracing::debug!(recv_id = self.recv_id, error = %e, "utp connection failed");
        self.state = ConnState::Closed;
        self.error = Some(e);
        self.unsent = VecDeque::new();
        self.outstanding.clear();
        self.ooo.clear();
        self.ooo_bytes = 0;
        self.flight = 0;
        self.unacked_bytes = 0;
        self.rto_at = None;
        self.ack_now = false;
        self.ack_due = None;
    }

    /// Queues up to `send_buffer` minus what is buffered (unsent plus
    /// unacknowledged). Returns how much was taken; 0 means full or closed.
    pub fn write(&mut self, data: &[u8]) -> usize {
        if self.closing || matches!(self.state, ConnState::Closed | ConnState::FinSent) {
            return 0;
        }
        let room = self
            .cfg
            .send_buffer
            .saturating_sub(self.unsent.len() + self.unacked_bytes);
        let n = data.len().min(room);
        self.unsent.extend(data.iter().take(n));
        n
    }

    /// Copies in-order received bytes into `out`. 0 means nothing to read
    /// (or end of stream, see [`Connection::is_eof`]).
    pub fn read(&mut self, out: &mut [u8]) -> usize {
        let n = out.len().min(self.inbox.len());
        for (o, b) in out.iter_mut().zip(self.inbox.drain(..n)) {
            *o = b;
        }
        // Tell a sender stalled on our window that it opened again.
        if n > 0 && self.last_adv_wnd < self.mss && self.adv_wnd() >= self.mss {
            self.ack_now = true;
        }
        n
    }

    /// Sends FIN once the queued data is out. Further writes return 0.
    pub fn close(&mut self) {
        if self.state != ConnState::Closed {
            self.closing = true;
        }
    }

    fn adv_wnd(&self) -> usize {
        self.cfg.recv_buffer.saturating_sub(self.buffered_bytes())
    }

    /// The earliest time `on_timer` has work to do.
    pub fn next_deadline(&self) -> Option<Instant> {
        if self.state == ConnState::Closed {
            return None;
        }
        let synsent = self.state == ConnState::SynSent;
        let live = |d: Option<Instant>| d.filter(|_| !synsent);
        let probe = (self.rto_at.is_none() && self.has_pending_send())
            .then(|| after(self.last_data_send, self.rtt.rto()))
            .flatten();
        [
            after(self.created, self.cfg.connect_timeout).filter(|_| synsent),
            live(after(self.last_recv, self.cfg.idle_timeout)),
            live(after(self.last_send, self.cfg.keepalive)),
            self.ack_due.filter(|_| !synsent),
            probe.filter(|_| !synsent),
            self.rto_at,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn has_pending_send(&self) -> bool {
        !self.unsent.is_empty()
            || (self.closing && self.fin_seq.is_none())
            || self.outstanding.iter().any(|s| s.lost && !s.acked)
    }

    /// Runs the timers due at `now`.
    pub fn on_timer(&mut self, now: Instant) {
        match self.state {
            ConnState::Closed => return,
            ConnState::SynSent => {
                if reached(now, self.created, self.cfg.connect_timeout) {
                    self.fail(UtpError::ConnectTimeout);
                    return;
                }
            }
            ConnState::Connected | ConnState::FinSent => {
                if reached(now, self.last_recv, self.cfg.idle_timeout) {
                    self.fail(UtpError::TimedOut);
                    return;
                }
            }
        }
        if self.rto_at.is_some_and(|r| now >= r) {
            self.on_rto();
            if self.state == ConnState::Closed {
                return;
            }
        } else if self.state != ConnState::SynSent
            && self.rto_at.is_none()
            && self.has_pending_send()
            && reached(now, self.last_data_send, self.rtt.rto())
        {
            // Nothing in flight and the peer's window holds us back: probe.
            self.bypass_peer_wnd = true;
        }
        if self.state == ConnState::SynSent {
            return;
        }
        if self.ack_due.is_some_and(|a| now >= a) {
            self.ack_now = true;
        }
        if reached(now, self.last_send, self.cfg.keepalive) {
            self.ack_now = true;
        }
    }

    fn on_rto(&mut self) {
        self.rto_at = None;
        if self.state != ConnState::SynSent {
            self.timeouts += 1;
            if self.timeouts > self.cfg.max_retransmits {
                self.fail(UtpError::TimedOut);
                return;
            }
            self.ledbat.on_timeout();
        }
        self.rtt.backoff();
        for s in self.outstanding.iter_mut() {
            if !s.acked && !s.lost {
                s.lost = true;
                self.flight = self.flight.saturating_sub(s.payload.len());
            }
        }
        self.dup_acks = 0;
        self.bypass_peer_wnd = true;
    }

    /// Handles one packet from the peer.
    pub fn on_packet(&mut self, p: &Packet, now: Instant) {
        let acceptor_syn = self.is_acceptor && p.ty == PacketType::Syn && p.conn_id == self.send_id;
        // libutp sends a reset with the id of the packet it answers, which
        // is our send id; libtorrent accepts both.
        let reset_by_send_id = p.ty == PacketType::Reset && p.conn_id == self.send_id;
        if p.conn_id != self.recv_id && !acceptor_syn && !reset_by_send_id {
            return;
        }
        if self.state == ConnState::Closed {
            // A cleanly closed connection still acks a retransmitted FIN.
            if self.error.is_none() && p.ty == PacketType::Fin {
                self.ack_now = true;
            }
            return;
        }
        if p.ty == PacketType::Reset {
            self.fail(UtpError::Reset);
            return;
        }
        if p.ty == PacketType::Syn {
            // A duplicate SYN: our STATE was lost.
            if acceptor_syn && p.seq_nr == self.syn_seq_seen {
                self.last_recv = now;
                self.syn_ack_pending = true;
            }
            return;
        }
        if self.state == ConnState::SynSent {
            if p.ty != PacketType::State || p.ack_nr != SYN_SEQ {
                return;
            }
            self.state = ConnState::Connected;
            self.ack_nr = p.seq_nr.wrapping_sub(1);
        }
        self.last_recv = now;
        self.reply_micro = self.timestamp(now).wrapping_sub(p.timestamp_us);
        let old_wnd = self.peer_wnd;
        self.peer_wnd = p.wnd_size as usize;
        if p.wnd_size == 0 {
            // Our data is refused, not lost: a live, throttled peer.
            self.timeouts = 0;
        }
        self.process_ack(p, now, old_wnd);
        if old_wnd < self.mss && self.peer_wnd >= self.mss {
            // The window reopened: the probe was most likely dropped for
            // lack of room, so resend it now.
            self.resend_probe();
        }
        match p.ty {
            PacketType::Data => self.receive_data(p, now),
            PacketType::Fin => self.receive_fin(p.seq_nr),
            _ => {}
        }
        self.check_closed();
    }

    fn resend_probe(&mut self) {
        let Some(i) = self
            .probe_seq
            .take()
            .and_then(|q| self.outstanding_index(q))
        else {
            return;
        };
        if let Some(s) = self.outstanding.get_mut(i)
            && !s.acked
            && !s.lost
        {
            s.lost = true;
            self.flight = self.flight.saturating_sub(s.payload.len());
        }
        if !self.outstanding.iter().any(|s| !s.acked && !s.lost) {
            self.rto_at = None;
        }
    }

    fn check_closed(&mut self) {
        if self.state == ConnState::FinSent && self.fin_acked && self.eof {
            self.state = ConnState::Closed;
            self.rto_at = None;
        }
    }

    fn outstanding_index(&self, seq: u16) -> Option<usize> {
        let front = self.outstanding.front()?.seq;
        let i = usize::from(seq.wrapping_sub(front));
        (i < self.outstanding.len()).then_some(i)
    }

    /// Marks one outstanding packet acked; returns its payload size and
    /// whether it gives an RTT sample (sent once, Karn's rule).
    fn ack_one(&mut self, i: usize, now: Instant) -> Option<(usize, Option<Duration>)> {
        let s = self.outstanding.get_mut(i)?;
        if s.acked {
            return None;
        }
        s.acked = true;
        let len = s.payload.len();
        // Acked data is never sent again; drop it so a SACKed-but-not-yet-
        // cumulatively-acked packet costs no buffer.
        s.payload = Bytes::new();
        if self.probe_seq == Some(s.seq) {
            self.probe_seq = None;
        }
        if !s.lost {
            self.flight = self.flight.saturating_sub(len);
        }
        self.unacked_bytes = self.unacked_bytes.saturating_sub(len);
        if Some(s.seq) == self.fin_seq {
            self.fin_acked = true;
        }
        let sample = (s.transmissions == 1).then(|| now.saturating_duration_since(s.sent_at));
        Some((len, sample))
    }

    fn process_ack(&mut self, p: &Packet, now: Instant, old_wnd: usize) {
        // The ack must lie between the last one and our newest packet.
        let newest = self.seq_nr.wrapping_sub(1);
        if !seq_le(self.peer_ack, p.ack_nr) || !seq_le(p.ack_nr, newest) {
            return;
        }
        let flight_before = self.flight;
        let mut acked_bytes = 0usize;
        let mut sample = None;
        let mut newly = 0usize;
        while let Some(front) = self.outstanding.front() {
            if !seq_le(front.seq, p.ack_nr) {
                break;
            }
            if let Some((len, s)) = self.ack_one(0, now) {
                acked_bytes += len;
                newly += 1;
                sample = s.or(sample);
            }
            self.outstanding.pop_front();
        }
        let mut sacked: Vec<u16> = Vec::new();
        if let Some(mask) = p.sack.as_deref() {
            for seq in sack_acks(mask, p.ack_nr) {
                let Some(i) = self.outstanding_index(seq) else {
                    continue; // not ours: never sent or already acked
                };
                sacked.push(seq);
                if let Some((len, s)) = self.ack_one(i, now) {
                    acked_bytes += len;
                    newly += 1;
                    sample = s.or(sample);
                }
            }
        }
        let advanced = p.ack_nr != self.peer_ack;
        if advanced {
            self.peer_ack = p.ack_nr;
            self.dup_acks = 0;
            self.timeouts = 0;
        } else if p.ty == PacketType::State
            && p.payload.is_empty()
            && p.wnd_size != 0
            && self.flight > 0
            && (p.sack.is_some() || p.wnd_size as usize == old_wnd)
        {
            // Keepalives and window updates (a changed window, no SACK)
            // are not duplicate acks.
            self.dup_acks += 1;
        }
        if newly > 0 {
            if let Some(s) = sample {
                self.rtt.on_sample(s);
            }
            if acked_bytes > 0 {
                let c = |v: usize| u32::try_from(v).unwrap_or(u32::MAX);
                self.ledbat
                    .on_ack(c(acked_bytes), p.timestamp_diff_us, c(flight_before), now);
            }
            self.rto_at = self
                .outstanding
                .iter()
                .any(|s| !s.acked && !s.lost)
                .then(|| after(now, self.rtt.rto()))
                .flatten();
        }
        self.detect_loss(&sacked, now);
    }

    fn detect_loss(&mut self, sacked: &[u16], now: Instant) {
        let mut lost_any = false;
        if self.dup_acks >= DUP_ACK_THRESHOLD {
            self.dup_acks = 0;
            if let Some(s) = self.outstanding.iter_mut().find(|s| !s.acked)
                && !s.lost
                && !s.fast_resent
            {
                s.lost = true;
                s.fast_resent = true;
                self.flight = self.flight.saturating_sub(s.payload.len());
                lost_any = true;
            }
        }
        if !sacked.is_empty() {
            // `sacked` is in ascending sequence order (from ack_nr + 2 on).
            for s in self.outstanding.iter_mut() {
                if s.acked || s.lost || s.fast_resent {
                    continue;
                }
                let later = sacked.len() - sacked.partition_point(|&q| seq_le(q, s.seq));
                if later < DUP_ACK_THRESHOLD as usize {
                    break; // later packets have even fewer acked after them
                }
                s.lost = true;
                s.fast_resent = true;
                self.flight = self.flight.saturating_sub(s.payload.len());
                lost_any = true;
            }
        }
        if lost_any {
            self.bypass_cwnd = true;
            let window = self.rtt.rtt().unwrap_or_else(|| self.rtt.rto());
            if self
                .last_loss
                .is_none_or(|l| now.saturating_duration_since(l) >= window)
            {
                self.ledbat.on_loss();
                self.last_loss = Some(now);
            }
            if !self.outstanding.iter().any(|s| !s.acked && !s.lost) {
                self.rto_at = None;
            }
        }
    }

    /// Bytes that fit the congestion and peer windows now.
    fn window_room(&self) -> (usize, usize) {
        let cwnd = self.ledbat.cwnd() as usize;
        (
            cwnd.saturating_sub(self.flight),
            self.peer_wnd.saturating_sub(self.flight),
        )
    }

    fn fits(&self, len: usize) -> bool {
        let (c, w) = self.window_room();
        len == 0 || ((len <= c || self.bypass_cwnd) && (len <= w || self.bypass_peer_wnd))
    }

    fn receive_data(&mut self, p: &Packet, now: Instant) {
        let d = p.seq_nr.wrapping_sub(self.ack_nr);
        if d == 0 || d >= 0x8000 {
            self.ack_now = true; // duplicate
            return;
        }
        if self.remote_fin.is_some_and(|f| !seq_lt(p.seq_nr, f)) {
            return; // beyond the end of the stream
        }
        let span =
            (self.cfg.recv_buffer / self.min_payload).clamp(1, usize::from(MAX_REORDER_SPAN));
        if usize::from(d) > span {
            return; // too far ahead
        }
        let len = p.payload.len();
        self.max_recv_payload = self.max_recv_payload.max(len);
        if d == 1 {
            if self.buffered_bytes() + len > self.cfg.recv_buffer {
                self.ack_now = true; // no room: the ack shows our window
                return;
            }
            self.inbox.extend(p.payload.iter());
            self.ack_nr = p.seq_nr;
            let had_holes = !self.ooo.is_empty();
            self.advance_in_order();
            if had_holes || !self.ooo.is_empty() || self.eof {
                self.ack_now = true;
            } else {
                self.unacked_recv += 1;
                if self.unacked_recv >= 2 {
                    self.ack_now = true;
                } else if self.ack_due.is_none() {
                    self.ack_due = after(now, DELAYED_ACK);
                }
            }
            return;
        }
        self.ack_now = true;
        if self.ooo.contains_key(&p.seq_nr) {
            return;
        }
        // Out-of-order data never takes the last packet's worth of room, so
        // the packet that fills the hole always fits.
        let reserve = self.mss.max(self.max_recv_payload).max(HOLE_RESERVE);
        let limit = self.cfg.recv_buffer.saturating_sub(reserve);
        if self.buffered_bytes() + len > limit {
            return;
        }
        self.ooo_bytes += len;
        self.ooo.insert(p.seq_nr, p.payload.to_vec());
    }

    fn advance_in_order(&mut self) {
        loop {
            let next = self.ack_nr.wrapping_add(1);
            if self.remote_fin == Some(next) {
                self.ack_nr = next;
                self.eof = true;
                self.ooo.clear();
                self.ooo_bytes = 0;
                return;
            }
            match self.ooo.remove(&next) {
                Some(v) => {
                    self.ooo_bytes = self.ooo_bytes.saturating_sub(v.len());
                    self.inbox.extend(v.iter());
                    self.ack_nr = next;
                }
                None => return,
            }
        }
    }

    fn receive_fin(&mut self, seq: u16) {
        self.ack_now = true;
        let d = seq.wrapping_sub(self.ack_nr);
        if d == 0 || d >= 0x8000 || self.eof {
            return; // duplicate
        }
        let span =
            (self.cfg.recv_buffer / self.min_payload).clamp(1, usize::from(MAX_REORDER_SPAN));
        if usize::from(d) > span || self.remote_fin.is_some_and(|f| f != seq) {
            return;
        }
        self.remote_fin = Some(seq);
        // Anything buffered past the FIN is not part of the stream.
        let beyond: Vec<u16> = self
            .ooo
            .keys()
            .copied()
            .filter(|&k| !seq_lt(k, seq))
            .collect();
        for k in beyond {
            if let Some(v) = self.ooo.remove(&k) {
                self.ooo_bytes = self.ooo_bytes.saturating_sub(v.len());
            }
        }
        self.advance_in_order();
    }

    fn push_outstanding(&mut self, ty: PacketType, payload: Bytes) -> u16 {
        let seq = self.seq_nr;
        self.seq_nr = self.seq_nr.wrapping_add(1);
        self.unacked_bytes += payload.len();
        self.outstanding.push_back(Sent {
            seq,
            ty,
            payload,
            sent_at: self.created,
            transmissions: 0,
            lost: true,
            acked: false,
            fast_resent: false,
        });
        seq
    }

    fn sack_mask(&self) -> Option<Vec<u8>> {
        if self.ooo.is_empty() {
            return None;
        }
        let mut mask = [0u8; MAX_SACK_LEN];
        let mut top = 0usize;
        for &k in self.ooo.keys() {
            let bit = usize::from(k.wrapping_sub(self.ack_nr).wrapping_sub(2));
            if bit < SACK_BITS
                && let Some(b) = mask.get_mut(bit / 8)
            {
                *b |= 1 << (bit % 8);
                top = top.max(bit / 8 + 1);
            }
        }
        if top == 0 {
            return None;
        }
        let len = top.div_ceil(4) * 4;
        Some(mask.get(..len).unwrap_or(&mask).to_vec())
    }

    fn packet(&mut self, ty: PacketType, seq: u16, payload: Bytes, now: Instant) -> Vec<u8> {
        let wnd = self.adv_wnd();
        let p = Packet {
            ty,
            conn_id: if ty == PacketType::Syn {
                self.recv_id
            } else {
                self.send_id
            },
            timestamp_us: self.timestamp(now),
            timestamp_diff_us: self.reply_micro,
            wnd_size: u32::try_from(wnd).unwrap_or(u32::MAX),
            seq_nr: seq,
            ack_nr: if ty == PacketType::Syn {
                0
            } else {
                self.ack_nr
            },
            sack: if ty == PacketType::State {
                self.sack_mask()
            } else {
                None
            },
            payload,
        };
        let mut out = Vec::with_capacity(crate::HEADER_LEN + 2 + MAX_SACK_LEN + p.payload.len());
        p.encode(&mut out);
        self.last_send = now;
        if ty != PacketType::Syn {
            self.last_adv_wnd = wnd;
            self.ack_now = false;
            self.ack_due = None;
            self.unacked_recv = 0;
        }
        out
    }

    /// The next datagram to send, if any. Call until it returns `None`.
    pub fn poll_transmit(&mut self, now: Instant) -> Option<Vec<u8>> {
        if self.state == ConnState::Closed {
            if self.error.is_none() && self.ack_now {
                return Some(self.packet(PacketType::State, self.seq_nr, Bytes::new(), now));
            }
            return None;
        }
        // The handshake STATE goes first and always names our first
        // sequence number: the initiator takes `ack_nr = seq_nr - 1` from it,
        // even when it answers a retransmitted SYN after data went out.
        if self.syn_ack_pending {
            self.syn_ack_pending = false;
            return Some(self.packet(PacketType::State, self.isn, Bytes::new(), now));
        }
        // An immediate ack goes first: it may carry the SACK a loss needs.
        if self.ack_now && self.state != ConnState::SynSent && self.ooo_bytes > 0 {
            return Some(self.packet(PacketType::State, self.seq_nr, Bytes::new(), now));
        }
        if let Some(d) = self.transmit_outstanding(now) {
            return Some(d);
        }
        if self.state != ConnState::SynSent {
            self.packetize();
            if let Some(d) = self.transmit_outstanding(now) {
                return Some(d);
            }
            if self.ack_now || self.ack_due.is_some_and(|a| now >= a) {
                return Some(self.packet(PacketType::State, self.seq_nr, Bytes::new(), now));
            }
        }
        None
    }

    /// Sends the oldest packet waiting for (re)transmission, if it fits.
    fn transmit_outstanding(&mut self, now: Instant) -> Option<Vec<u8>> {
        let i = self.outstanding.iter().position(|s| s.lost && !s.acked)?;
        let (len, ty, seq, payload) = {
            let s = self.outstanding.get(i)?;
            (s.payload.len(), s.ty, s.seq, s.payload.clone())
        };
        if !self.fits(len) {
            return None;
        }
        if len > self.window_room().1 {
            self.probe_seq = Some(seq); // sent past the peer's window
        }
        let s = self.outstanding.get_mut(i)?;
        s.lost = false;
        s.transmissions = s.transmissions.saturating_add(1);
        s.sent_at = now;
        self.flight += len;
        self.bypass_cwnd = false;
        self.bypass_peer_wnd = false;
        if self.rto_at.is_none() {
            self.rto_at = after(now, self.rtt.rto());
        }
        if len > 0 {
            self.last_data_send = now;
        }
        if ty == PacketType::Fin {
            self.state = ConnState::FinSent;
        }
        Some(self.packet(ty, seq, payload, now))
    }

    /// Turns unsent bytes (then the FIN) into one new outstanding packet
    /// when it fits the windows; `transmit_outstanding` then sends it.
    fn packetize(&mut self) {
        if self.outstanding.iter().any(|s| s.lost && !s.acked)
            || self.outstanding.len() >= MAX_IN_FLIGHT_PACKETS
        {
            return;
        }
        if !self.unsent.is_empty() {
            let mut len = self.unsent.len().min(self.mss);
            // A receiver whose whole window is below one packet gets smaller
            // packets rather than none, but never fragments below a quarter
            // packet (silly-window avoidance): those wait for the window or
            // the probe timer.
            if self.peer_wnd < self.mss && !self.bypass_peer_wnd {
                let (_, w) = self.window_room();
                if w < len {
                    if w < (self.mss / 4).max(1) {
                        return;
                    }
                    len = w;
                }
            }
            if !self.fits(len) {
                return;
            }
            let payload: Vec<u8> = self.unsent.drain(..len).collect();
            self.push_outstanding(PacketType::Data, Bytes::from(payload));
            return;
        }
        if self.closing && self.fin_seq.is_none() {
            let seq = self.push_outstanding(PacketType::Fin, Bytes::new());
            self.fin_seq = Some(seq);
        }
    }
}

#[cfg(test)]
mod tests;
