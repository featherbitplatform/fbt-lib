use super::*;
use crate::packet::PacketType;
use bytes::Bytes;

const DELAY: Duration = Duration::from_millis(10);

fn v4() -> SocketAddr {
    "127.0.0.1:6881".parse().unwrap()
}

fn decode(d: Vec<u8>) -> Packet {
    Packet::decode(&Bytes::from(d)).unwrap()
}

/// Every datagram `c` has to send at `now`.
fn drain(c: &mut Connection, now: Instant) -> Vec<Packet> {
    let mut v = Vec::new();
    while let Some(d) = c.poll_transmit(now) {
        v.push(decode(d));
        assert!(v.len() < 100_000, "poll_transmit never ends");
    }
    v
}

fn data_of(v: &[Packet]) -> Vec<&Packet> {
    v.iter().filter(|p| p.ty == PacketType::Data).collect()
}

fn state_pkt(conn_id: u16, seq_nr: u16, ack_nr: u16, wnd: u32) -> Packet {
    Packet {
        ty: PacketType::State,
        conn_id,
        timestamp_us: 0,
        timestamp_diff_us: 5_000,
        wnd_size: wnd,
        seq_nr,
        ack_nr,
        sack: None,
        payload: Bytes::new(),
    }
}

fn data_pkt(conn_id: u16, seq_nr: u16, len: usize) -> Packet {
    Packet {
        ty: PacketType::Data,
        payload: Bytes::from(vec![7u8; len]),
        ..state_pkt(conn_id, seq_nr, 0, 1 << 20)
    }
}

/// A connected initiator driven by hand: the fake peer's STATE (seq 500)
/// arrives 20 ms after the SYN, advertising `wnd`.
fn hand_connected(cfg: &UtpConfig, wnd: u32) -> (Connection, Instant) {
    let t0 = Instant::now();
    let mut a = Connection::connect(cfg, v4(), 100, t0);
    let syn = drain(&mut a, t0);
    assert_eq!(syn.len(), 1);
    let t = t0 + Duration::from_millis(20);
    a.on_packet(&state_pkt(100, 500, 1, wnd), t);
    assert_eq!(a.state(), ConnState::Connected);
    (a, t)
}

/// Two connections joined by a link with a fixed one-way `DELAY`.
struct Pair {
    a: Connection,
    b: Connection,
    t: Instant,
    link: Vec<(Instant, bool, Packet)>,
    /// Deliver B's packets to A.
    b_to_a: bool,
    last_to_a: Instant,
    /// (time, from A, packet type, payload length)
    log: Vec<(Instant, bool, PacketType, usize)>,
}

fn pair(cfg_a: &UtpConfig, cfg_b: &UtpConfig) -> Pair {
    let t0 = Instant::now();
    let mut a = Connection::connect(cfg_a, v4(), 100, t0);
    let syn = drain(&mut a, t0).remove(0);
    let b = Connection::accept(cfg_b, v4(), &syn, t0 + DELAY);
    let mut p = Pair {
        a,
        b,
        t: t0 + DELAY,
        link: Vec::new(),
        b_to_a: true,
        last_to_a: t0,
        log: Vec::new(),
    };
    p.run_for(Duration::from_millis(100));
    assert_eq!(p.a.state(), ConnState::Connected);
    assert_eq!(p.b.state(), ConnState::Connected);
    p
}

impl Pair {
    fn flush(&mut self) {
        for pk in drain(&mut self.a, self.t) {
            self.log.push((self.t, true, pk.ty, pk.payload.len()));
            self.link.push((self.t + DELAY, true, pk));
        }
        for pk in drain(&mut self.b, self.t) {
            self.log.push((self.t, false, pk.ty, pk.payload.len()));
            if self.b_to_a {
                self.link.push((self.t + DELAY, false, pk));
            }
        }
    }

    /// Advances to the next event (at most `end`); false once at `end`.
    fn step(&mut self, end: Instant) -> bool {
        self.flush();
        let next = self
            .link
            .iter()
            .map(|l| l.0)
            .chain(self.a.next_deadline())
            .chain(self.b.next_deadline())
            .min();
        match next {
            Some(n) if n <= end => self.t = self.t.max(n),
            _ => {
                self.t = end;
                return false;
            }
        }
        let now = self.t;
        let (due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.link)
            .into_iter()
            .partition(|l| l.0 <= now);
        self.link = rest;
        for (_, to_b, pk) in due {
            if to_b {
                self.b.on_packet(&pk, now);
            } else {
                self.last_to_a = now;
                self.a.on_packet(&pk, now);
            }
        }
        if self.a.next_deadline().is_some_and(|d| d <= now) {
            self.a.on_timer(now);
        }
        if self.b.next_deadline().is_some_and(|d| d <= now) {
            self.b.on_timer(now);
        }
        true
    }

    fn run_until(&mut self, end: Instant, mut each: impl FnMut(&mut Pair)) {
        let mut steps = 0u64;
        while self.t < end && self.step(end) {
            each(self);
            steps += 1;
            assert!(steps < 5_000_000, "simulation does not advance");
        }
        self.flush();
    }

    fn run_for(&mut self, d: Duration) {
        let end = self.t + d;
        self.run_until(end, |_| {});
    }
}

fn read_all(c: &mut Connection, into: &mut Vec<u8>) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = c.read(&mut buf);
        if n == 0 {
            return;
        }
        into.extend_from_slice(&buf[..n]);
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 251) as u8).collect()
}

#[test]
fn handshake_ids_follow_bep29() {
    let cfg = UtpConfig::default();
    let t0 = Instant::now();
    let mut a = Connection::connect(&cfg, v4(), 1000, t0);
    assert_eq!(a.state(), ConnState::SynSent);
    assert_eq!((a.recv_id(), a.send_id()), (1000, 1001));
    let syn = drain(&mut a, t0);
    assert_eq!(syn.len(), 1);
    let syn = &syn[0];
    assert_eq!(syn.ty, PacketType::Syn);
    assert_eq!(syn.conn_id, 1000);
    assert_eq!(syn.seq_nr, 1);
    assert_eq!(syn.wnd_size as usize, cfg.recv_buffer);

    let mut b = Connection::accept(&cfg, v4(), syn, t0);
    assert_eq!((b.recv_id(), b.send_id()), (1001, 1000));
    assert_eq!(b.state(), ConnState::Connected);
    let st = drain(&mut b, t0);
    assert_eq!(st.len(), 1);
    let st = &st[0];
    assert_eq!(st.ty, PacketType::State);
    assert_eq!(st.conn_id, 1000);
    assert_eq!(st.ack_nr, 1);

    a.on_packet(st, t0 + DELAY);
    assert_eq!(a.state(), ConnState::Connected);
    assert_eq!(a.write(b"hi"), 2);
    let d = drain(&mut a, t0 + DELAY);
    let d = data_of(&d);
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].conn_id, 1001);
    assert_eq!(d[0].seq_nr, 2);
    assert_eq!(d[0].ack_nr, st.seq_nr.wrapping_sub(1));

    assert_eq!(b.write(b"yo"), 2);
    let bd = drain(&mut b, t0 + DELAY);
    let bd = data_of(&bd);
    assert_eq!(bd[0].seq_nr, st.seq_nr);
    assert_eq!(bd[0].conn_id, 1000);

    b.on_packet(d[0], t0 + DELAY * 2);
    let mut got = [0u8; 8];
    assert_eq!(b.read(&mut got), 2);
    assert_eq!(&got[..2], b"hi");
    a.on_packet(bd[0], t0 + DELAY * 2);
    assert_eq!(a.read(&mut got), 2);
    assert_eq!(&got[..2], b"yo");
}

#[test]
fn syn_retries_then_connect_timeout() {
    let cfg = UtpConfig::default();
    let t0 = Instant::now();
    let mut a = Connection::connect(&cfg, v4(), 7, t0);
    let mut syn_times = Vec::new();
    let mut t = t0;
    for _ in 0..20 {
        for p in drain(&mut a, t) {
            assert_eq!(p.ty, PacketType::Syn);
            syn_times.push(t - t0);
        }
        match a.next_deadline() {
            Some(d) => {
                t = d;
                a.on_timer(t);
            }
            None => break,
        }
        if a.state() == ConnState::Closed {
            break;
        }
    }
    assert!(drain(&mut a, t).is_empty());
    let s = Duration::from_secs;
    assert_eq!(syn_times, vec![s(0), s(1), s(3)]);
    assert_eq!(a.state(), ConnState::Closed);
    assert_eq!(a.error(), Some(UtpError::ConnectTimeout));
    assert_eq!(t - t0, s(7));
    assert_eq!(a.next_deadline(), None);
}

#[test]
fn in_order_data_roundtrip() {
    let cfg = UtpConfig::default();
    let mut p = pair(&cfg, &cfg);
    let a_data = pattern(300_000);
    let b_data = pattern(5_000);
    assert_eq!(p.a.write(&a_data), a_data.len());
    assert_eq!(p.b.write(&b_data), b_data.len());
    let (mut at_b, mut at_a) = (Vec::new(), Vec::new());
    let end = p.t + Duration::from_secs(30);
    p.run_until(end, |p| {
        read_all(&mut p.b, &mut at_b);
        read_all(&mut p.a, &mut at_a);
    });
    assert_eq!(at_b, a_data);
    assert_eq!(at_a, b_data);
    assert_eq!(p.a.buffered_bytes(), 0);
    assert_eq!(p.a.state(), ConnState::Connected);
}

/// Grows the hand-driven connection's window until one flush sends at
/// least `n` data packets; every packet before that batch is acked.
fn batch_of(a: &mut Connection, t: &mut Instant, n: usize) -> Vec<Packet> {
    let data = pattern(400_000);
    assert_eq!(a.write(&data), data.len());
    for _ in 0..200 {
        let out = drain(a, *t);
        let d: Vec<Packet> = out
            .into_iter()
            .filter(|p| p.ty == PacketType::Data)
            .collect();
        if d.len() >= n {
            return d;
        }
        *t += Duration::from_millis(20);
        for p in &d {
            a.on_packet(&state_pkt(100, 500, p.seq_nr, 1 << 20), *t);
        }
    }
    panic!("window never opened to {n} packets");
}

#[test]
fn three_dup_acks_fast_retransmit() {
    let cfg = UtpConfig::default();
    let (mut a, mut t) = hand_connected(&cfg, 1 << 20);
    let batch = batch_of(&mut a, &mut t, 5);
    let lost = &batch[0];
    let before = a.cwnd();
    t += Duration::from_millis(20);
    for i in 0..3 {
        a.on_packet(
            &state_pkt(100, 500, lost.seq_nr.wrapping_sub(1), 1 << 20),
            t,
        );
        let out = drain(&mut a, t);
        let resent = data_of(&out).into_iter().any(|p| p.seq_nr == lost.seq_nr);
        assert_eq!(resent, i == 2, "dup ack {}", i + 1);
        if i == 2 {
            let r = out.iter().find(|p| p.seq_nr == lost.seq_nr).unwrap();
            assert_eq!(r.payload, lost.payload);
        }
    }
    assert!(a.cwnd() < before, "cwnd {} not below {before}", a.cwnd());
}

#[test]
fn sack_with_three_later_packets_retransmits() {
    let cfg = UtpConfig::default();
    let (mut a, mut t) = hand_connected(&cfg, 1 << 20);
    let batch = batch_of(&mut a, &mut t, 5);
    let lost = &batch[0];
    t += Duration::from_millis(20);
    // Bits 0..=2 acknowledge lost + 2, + 3 and + 4 (lost + 1 is missing too).
    let ack = lost.seq_nr.wrapping_sub(1);
    let mut st = state_pkt(100, 500, ack, 1 << 20);
    st.sack = Some(vec![0b0000_0011, 0, 0, 0]);
    a.on_packet(&st, t);
    let out = drain(&mut a, t);
    assert!(!data_of(&out).iter().any(|p| p.seq_nr == lost.seq_nr));
    st.sack = Some(vec![0b0000_0111, 0, 0, 0]);
    a.on_packet(&st, t);
    let out = drain(&mut a, t);
    let resent: Vec<u16> = data_of(&out).iter().map(|p| p.seq_nr).collect();
    assert!(resent.contains(&lost.seq_nr), "resent {resent:?}");
}

#[test]
fn rto_retransmit_and_backoff() {
    let cfg = UtpConfig::default();
    let (mut a, t) = hand_connected(&cfg, 1 << 20);
    assert_eq!(a.write(&[1u8; 1000]), 1000);
    let first = drain(&mut a, t);
    let first = data_of(&first);
    assert_eq!(first.len(), 1);
    let seq = first[0].seq_nr;
    // One 20 ms sample: rto = max(20 + 4 * 10 ms, 500 ms).
    let rto = Duration::from_millis(500);
    assert_eq!(a.next_deadline(), Some(t + rto));
    a.on_timer(t + rto - Duration::from_millis(1));
    assert!(drain(&mut a, t + rto - Duration::from_millis(1)).is_empty());

    let mut at = t + rto;
    let mut wait = rto;
    for _ in 0..3 {
        a.on_timer(at);
        let out = drain(&mut a, at);
        let d = data_of(&out);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].seq_nr, seq);
        assert_eq!(a.cwnd(), 1400);
        wait *= 2;
        assert_eq!(a.next_deadline(), Some(at + wait));
        at += wait;
    }
    assert_eq!(a.state(), ConnState::Connected);
}

#[test]
fn max_retransmits_give_timed_out() {
    // Keep the idle timer and keepalives out of the way of the RTO count.
    let cfg = UtpConfig {
        idle_timeout: Duration::from_secs(3600),
        keepalive: Duration::from_secs(3600),
        ..UtpConfig::default()
    };
    let (mut a, t) = hand_connected(&cfg, 1 << 20);
    a.write(&[1u8; 100]);
    drain(&mut a, t);
    let mut timeouts = 0;
    while a.state() != ConnState::Closed {
        let d = a.next_deadline().unwrap();
        a.on_timer(d);
        drain(&mut a, d);
        timeouts += 1;
        assert!(timeouts < 20);
    }
    assert_eq!(a.error(), Some(UtpError::TimedOut));
    assert_eq!(timeouts, cfg.max_retransmits + 1);
}

#[test]
fn fin_after_data_reads_eof() {
    let cfg = UtpConfig::default();
    let mut p = pair(&cfg, &cfg);
    let data = pattern(50_000);
    assert_eq!(p.a.write(&data), data.len());
    p.a.close();
    assert_eq!(p.a.write(b"late"), 0);
    let mut got = Vec::new();
    let end = p.t + Duration::from_secs(10);
    p.run_until(end, |p| read_all(&mut p.b, &mut got));
    assert_eq!(got, data);
    assert!(p.b.is_eof());
    assert_eq!(p.b.read(&mut [0u8; 10]), 0);
    assert_eq!(p.a.state(), ConnState::FinSent);
    assert!(!p.a.is_eof());

    p.b.close();
    p.run_for(Duration::from_secs(10));
    assert!(p.a.is_eof());
    assert_eq!(p.a.state(), ConnState::Closed);
    assert_eq!(p.b.state(), ConnState::Closed);
    assert_eq!(p.a.error(), None);
    assert_eq!(p.b.error(), None);
}

#[test]
fn reset_sets_error() {
    let cfg = UtpConfig::default();
    let (mut a, t) = hand_connected(&cfg, 1 << 20);
    let mut r = state_pkt(100, 500, 1, 0);
    r.ty = PacketType::Reset;
    a.on_packet(&r, t);
    assert_eq!(a.state(), ConnState::Closed);
    assert_eq!(a.error(), Some(UtpError::Reset));
    assert_eq!(a.write(b"x"), 0);
    assert!(drain(&mut a, t).is_empty());
    assert_eq!(a.next_deadline(), None);
}

fn accepted(cfg: &UtpConfig) -> (Connection, Instant) {
    let t0 = Instant::now();
    let mut a = Connection::connect(cfg, v4(), 100, t0);
    let syn = drain(&mut a, t0).remove(0);
    let mut b = Connection::accept(cfg, v4(), &syn, t0);
    drain(&mut b, t0);
    (b, t0)
}

#[test]
fn far_ahead_and_duplicate_packets_are_bounded() {
    let cfg = UtpConfig::default();
    // The SYN had seq 1, so the acceptor's ack_nr is 1.
    let (mut b, t) = accepted(&cfg);
    for i in 0..10_000u16 {
        b.on_packet(&data_pkt(101, 3u16.wrapping_add(i), 1400), t);
        assert!(b.buffered_bytes() <= cfg.recv_buffer);
        if i % 64 == 0 {
            assert!(drain(&mut b, t).len() <= 2);
        }
    }
    assert!(b.buffered_bytes() > 0);

    let (mut b, t) = accepted(&cfg);
    b.on_packet(&data_pkt(101, 2, 1400), t);
    b.on_packet(&data_pkt(101, 4, 1400), t);
    assert_eq!(b.buffered_bytes(), 2800);
    for _ in 0..1000 {
        b.on_packet(&data_pkt(101, 2, 1400), t);
        b.on_packet(&data_pkt(101, 4, 1400), t);
        b.on_packet(&data_pkt(101, 1, 1400), t);
        b.on_packet(&data_pkt(101, 0x8000, 1400), t);
    }
    assert_eq!(b.buffered_bytes(), 2800);
    // A duplicate is acked, with a SACK for the hole at 3.
    let acks = drain(&mut b, t);
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].ack_nr, 2);
    assert_eq!(acks[0].sack.as_deref().map(|s| s[0] & 1), Some(1));
}

fn zero_window_setup() -> Pair {
    let cfg = UtpConfig::default();
    let small = UtpConfig {
        recv_buffer: 14_000,
        ..UtpConfig::default()
    };
    let mut p = pair(&cfg, &small);
    let chunk = vec![3u8; 64 * 1024];
    let mut total = 0;
    loop {
        let n = p.a.write(&chunk);
        if n == 0 {
            break;
        }
        total += n;
    }
    assert_eq!(total, cfg.send_buffer);
    p.run_for(Duration::from_secs(5));
    assert_eq!(p.b.buffered_bytes(), small.recv_buffer);
    // Only what the peer acknowledged frees room; then write is full again.
    assert_eq!(p.a.write(&chunk), small.recv_buffer);
    assert_eq!(p.a.write(&chunk), 0, "write must stay full");
    p
}

/// Gaps between A's data packets sent after `from`.
fn probe_gaps(p: &Pair, from: Instant) -> Vec<Duration> {
    let times: Vec<Instant> = p
        .log
        .iter()
        .filter(|l| l.1 && l.2 == PacketType::Data && l.0 >= from)
        .map(|l| l.0)
        .collect();
    times.windows(2).map(|w| w[1] - w[0]).collect()
}

#[test]
fn zero_window_peer_times_out() {
    let cfg = UtpConfig::default();
    let mut p = zero_window_setup();
    let from = p.t;
    p.run_for(Duration::from_secs(90));
    assert_eq!(p.a.state(), ConnState::Connected);
    let gaps = probe_gaps(&p, from);
    assert!(!gaps.is_empty(), "no probes");
    assert!(
        gaps.iter().all(|g| *g >= Duration::from_millis(500)),
        "probes too close: {gaps:?}"
    );

    p.b_to_a = false;
    let mut closed_at = None;
    let end = p.t + cfg.idle_timeout * 2;
    p.run_until(end, |p| {
        if closed_at.is_none() && p.a.state() == ConnState::Closed {
            closed_at = Some(p.t);
        }
    });
    let closed_at = closed_at.expect("never timed out");
    assert_eq!(p.a.error(), Some(UtpError::TimedOut));
    assert!(closed_at <= p.last_to_a + cfg.idle_timeout);
}

#[test]
fn zero_window_peer_that_keeps_acking_stays_connected() {
    let cfg = UtpConfig::default();
    let mut p = zero_window_setup();
    let from = p.t;
    let end = p.t + cfg.idle_timeout * 3;
    p.run_until(end, |p| {
        assert_eq!(p.a.state(), ConnState::Connected);
        assert_eq!(p.b.state(), ConnState::Connected);
    });
    let gaps = probe_gaps(&p, from);
    assert!(gaps.iter().all(|g| *g >= Duration::from_millis(500)));

    let mut got = Vec::new();
    let end = p.t + Duration::from_secs(120);
    p.run_until(end, |p| read_all(&mut p.b, &mut got));
    assert_eq!(got.len(), cfg.send_buffer + 14_000);
    assert_eq!(p.a.buffered_bytes(), 0);
}

#[test]
fn advertised_huge_window_is_capped_by_cwnd() {
    let cfg = UtpConfig::default();
    let (mut a, t) = hand_connected(&cfg, u32::MAX);
    assert_eq!(a.write(&vec![0u8; 1 << 20]), 1 << 20);
    let out = drain(&mut a, t);
    let sent: usize = data_of(&out).iter().map(|p| p.payload.len()).sum();
    assert!(sent > 0);
    assert!(sent <= a.cwnd() as usize, "sent {sent} > cwnd {}", a.cwnd());
}

#[test]
fn remote_garbage_never_panics() {
    let cfg = UtpConfig::default();
    let (mut a, t) = hand_connected(&cfg, 1 << 20);
    a.write(&[1u8; 10_000]);
    for i in 0..5000u32 {
        let x = i.wrapping_mul(2_654_435_761);
        let p = Packet {
            ty: [
                PacketType::Data,
                PacketType::Fin,
                PacketType::State,
                PacketType::Syn,
            ][(x % 4) as usize],
            conn_id: 100,
            timestamp_us: x,
            timestamp_diff_us: x.rotate_left(7),
            wnd_size: x.rotate_left(13),
            seq_nr: x as u16,
            ack_nr: (x >> 16) as u16,
            sack: (x % 3 == 0).then(|| vec![(x >> 3) as u8; 32]),
            payload: Bytes::from(vec![0u8; (x % 1500) as usize]),
        };
        let now = t + Duration::from_millis(u64::from(i));
        a.on_packet(&p, now);
        a.on_timer(now);
        drain(&mut a, now);
        assert!(a.buffered_bytes() <= cfg.recv_buffer);
    }
}
