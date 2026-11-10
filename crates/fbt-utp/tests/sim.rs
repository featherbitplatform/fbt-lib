//! Deterministic network simulation of two uTP connections: seeded loss,
//! duplication, jitter (hence reordering) and an optional bottleneck queue,
//! driven by paused tokio time.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fbt_utp::UtpConfig;
use fbt_utp::conn::Connection;
use fbt_utp::{Packet, PacketType};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sha1::{Digest, Sha1};

#[derive(Debug, Clone, Copy)]
struct LinkCfg {
    delay: Duration,
    jitter: Duration,
    loss: f64,
    dup: f64,
    /// (bytes per second, longest queueing delay before tail drop)
    bottleneck: Option<(f64, Duration)>,
}

impl LinkCfg {
    fn lossy(loss: f64, dup: f64) -> LinkCfg {
        LinkCfg {
            delay: Duration::from_millis(20),
            jitter: Duration::from_millis(10),
            loss,
            dup,
            bottleneck: None,
        }
    }
}

/// A datagram in flight: (arrival, tie-break counter, destination, bytes).
type InFlight = Reverse<(Instant, u64, usize, Vec<u8>)>;

/// Datagrams in flight between endpoint 0 (the dialer) and 1.
struct SimNet {
    rng: StdRng,
    cfg: LinkCfg,
    queue: BinaryHeap<InFlight>,
    counter: u64,
    busy_until: [Option<Instant>; 2],
}

impl SimNet {
    fn new(seed: u64, cfg: LinkCfg) -> SimNet {
        SimNet {
            rng: StdRng::seed_from_u64(seed),
            cfg,
            queue: BinaryHeap::new(),
            counter: 0,
            busy_until: [None, None],
        }
    }

    fn send(&mut self, now: Instant, to: usize, d: Vec<u8>) {
        if self.rng.random_bool(self.cfg.loss) {
            return;
        }
        let copies = if self.rng.random_bool(self.cfg.dup) {
            2
        } else {
            1
        };
        let mut depart = now;
        if let Some((rate, max_queue)) = self.cfg.bottleneck {
            let start = self.busy_until[to].map_or(now, |b| b.max(now));
            if start - now > max_queue {
                return; // tail drop
            }
            depart = start + Duration::from_secs_f64(d.len() as f64 / rate);
            self.busy_until[to] = Some(depart);
        }
        for _ in 0..copies {
            let j = self.cfg.jitter.as_secs_f64();
            let off = if j > 0.0 {
                self.rng.random_range(-j..=j)
            } else {
                0.0
            };
            let delay = Duration::from_secs_f64((self.cfg.delay.as_secs_f64() + off).max(0.0));
            self.counter += 1;
            self.queue
                .push(Reverse((depart + delay, self.counter, to, d.clone())));
        }
    }

    fn next_arrival(&self) -> Option<Instant> {
        self.queue.peek().map(|r| r.0.0)
    }

    fn pop_due(&mut self, now: Instant) -> Option<(usize, Vec<u8>)> {
        if self.next_arrival()? <= now {
            self.queue.pop().map(|Reverse((_, _, to, d))| (to, d))
        } else {
            None
        }
    }
}

fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

fn addr(i: usize) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, i as u8 + 1], 6881))
}

/// What one side sends and what it has received.
struct Side {
    conn: Option<Connection>,
    data: Vec<u8>,
    written: usize,
    /// Keep writing a repeating pattern forever instead of `data` once.
    endless: bool,
    received: Vec<u8>,
    keep_received: bool,
    received_len: usize,
    closed: bool,
}

impl Side {
    fn new(data: Vec<u8>) -> Side {
        Side {
            conn: None,
            data,
            written: 0,
            endless: false,
            received: Vec::new(),
            keep_received: true,
            received_len: 0,
            closed: false,
        }
    }

    fn pump_app(&mut self) {
        let Some(c) = self.conn.as_mut() else { return };
        if self.endless {
            while c.write(&self.data) > 0 {}
        } else {
            while self.written < self.data.len() {
                let n = c.write(&self.data[self.written..]);
                if n == 0 {
                    break;
                }
                self.written += n;
            }
            if self.written == self.data.len() && !self.closed {
                c.close();
                self.closed = true;
            }
        }
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = c.read(&mut buf);
            if n == 0 {
                break;
            }
            self.received_len += n;
            if self.keep_received {
                self.received.extend_from_slice(&buf[..n]);
            }
        }
    }
}

struct Outcome {
    elapsed: Duration,
    /// Time-weighted average congestion window of side 0.
    avg_cwnd: f64,
}

/// Runs until `done` or `limit` of simulated time.
async fn run(
    net: &mut SimNet,
    sides: &mut [Side; 2],
    limit: Duration,
    done: impl Fn(&[Side; 2]) -> bool,
) -> Outcome {
    let cfg = UtpConfig::default();
    let start = now();
    sides[0].conn = Some(Connection::connect(&cfg, addr(1), 4242, start));
    let mut cwnd_area = 0.0f64;
    let mut last = start;
    loop {
        let t = now();
        if let Some(c) = sides[0].conn.as_ref() {
            cwnd_area += f64::from(c.cwnd()) * (t - last).as_secs_f64();
        }
        last = t;
        while let Some((to, d)) = net.pop_due(t) {
            let Ok(p) = Packet::decode(&Bytes::from(d)) else {
                continue;
            };
            match sides[to].conn.as_mut() {
                Some(c) => c.on_packet(&p, t),
                None if p.ty == PacketType::Syn => {
                    sides[to].conn = Some(Connection::accept(&cfg, addr(0), &p, t));
                }
                None => {}
            }
        }
        for s in sides.iter_mut() {
            if let Some(c) = s.conn.as_mut()
                && c.next_deadline().is_some_and(|d| d <= t)
            {
                c.on_timer(t);
            }
            s.pump_app();
        }
        for (i, s) in sides.iter_mut().enumerate() {
            if let Some(c) = s.conn.as_mut() {
                while let Some(d) = c.poll_transmit(t) {
                    net.send(t, 1 - i, d);
                }
                assert_eq!(c.error(), None, "side {i} in {:?}", c.state());
            }
        }
        let elapsed = t - start;
        if done(sides) || elapsed >= limit {
            return Outcome {
                elapsed,
                avg_cwnd: cwnd_area / elapsed.as_secs_f64().max(1e-9),
            };
        }
        let next = sides
            .iter()
            .filter_map(|s| s.conn.as_ref().and_then(Connection::next_deadline))
            .chain(net.next_arrival())
            .min()
            .unwrap_or(start + limit)
            .min(start + limit)
            .max(t);
        // The timer wheel has millisecond granularity; never wake early.
        while now() < next {
            tokio::time::sleep_until(tokio::time::Instant::from_std(next)).await;
        }
        if next == t {
            tokio::task::yield_now().await;
        }
    }
}

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    rng.fill(&mut v[..]);
    v
}

fn eof(s: &Side) -> bool {
    s.conn.as_ref().is_some_and(Connection::is_eof)
}

#[tokio::test(start_paused = true)]
async fn transfer_survives_loss_reorder_and_duplication() {
    const LEN: usize = 4 << 20;
    for seed in 1..=10u64 {
        let mut data_rng = StdRng::seed_from_u64(seed + 1000);
        let d0 = random_bytes(&mut data_rng, LEN);
        let d1 = random_bytes(&mut data_rng, LEN);
        let mut sides = [Side::new(d0.clone()), Side::new(d1.clone())];
        let mut net = SimNet::new(seed, LinkCfg::lossy(0.10, 0.05));
        let out = run(&mut net, &mut sides, Duration::from_secs(3600), |s| {
            eof(&s[0]) && eof(&s[1])
        })
        .await;
        assert_eq!(sides[1].received.len(), LEN, "seed {seed}");
        assert_eq!(sides[0].received.len(), LEN, "seed {seed}");
        assert_eq!(
            Sha1::digest(&sides[1].received),
            Sha1::digest(&d0),
            "seed {seed}"
        );
        assert_eq!(
            Sha1::digest(&sides[0].received),
            Sha1::digest(&d1),
            "seed {seed}"
        );
        println!("seed {seed}: {:?} simulated", out.elapsed);
    }
}

async fn endless_cwnd(bottleneck: Option<(f64, Duration)>) -> f64 {
    // No jitter: per-packet reordering would trigger SACK loss recovery and
    // hide the delay signal this test is about.
    let cfg = LinkCfg {
        jitter: Duration::ZERO,
        bottleneck,
        ..LinkCfg::lossy(0.0, 0.0)
    };
    let mut net = SimNet::new(7, cfg);
    let mut sender = Side::new(vec![0x5a; 64 * 1024]);
    sender.endless = true;
    let mut receiver = Side::new(Vec::new());
    receiver.keep_received = false;
    receiver.closed = true; // never closes
    let mut sides = [sender, receiver];
    let out = run(&mut net, &mut sides, Duration::from_secs(20), |_| false).await;
    assert!(sides[1].received_len > 0);
    out.avg_cwnd
}

#[tokio::test(start_paused = true)]
async fn ledbat_backs_off_under_queueing_delay() {
    let free = endless_cwnd(None).await;
    // 100 kB/s with a 300 ms queue: LEDBAT should hold the queue near its
    // 100 ms target, a window of roughly rate x (RTT + 100 ms).
    let queued = endless_cwnd(Some((100_000.0, Duration::from_millis(300)))).await;
    println!("avg cwnd: free {free:.0}, bottleneck {queued:.0}");
    assert!(queued < free, "bottleneck {queued} >= free {free}");
    assert!(
        queued < 60_000.0,
        "bottleneck window {queued} ignores the queue"
    );
}

#[tokio::test(start_paused = true)]
async fn bulk_transfer_completes_in_bounded_time() {
    const LEN: usize = 8 << 20;
    let cfg = LinkCfg {
        delay: Duration::from_millis(10),
        jitter: Duration::ZERO,
        ..LinkCfg::lossy(0.0, 0.0)
    };
    let mut net = SimNet::new(3, cfg);
    let data = random_bytes(&mut StdRng::seed_from_u64(3), LEN);
    let mut sides = [Side::new(data.clone()), Side::new(Vec::new())];
    let out = run(&mut net, &mut sides, Duration::from_secs(120), |s| {
        s[1].received.len() == LEN && eof(&s[1])
    })
    .await;
    assert_eq!(Sha1::digest(&sides[1].received), Sha1::digest(&data));
    println!("8 MiB in {:?} simulated", out.elapsed);
    assert!(
        out.elapsed < Duration::from_secs(30),
        "took {:?}",
        out.elapsed
    );
}
