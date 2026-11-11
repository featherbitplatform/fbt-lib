#![no_main]
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fbt_utp::conn::{Connection, UtpConfig};
use fbt_utp::packet::{Packet, PacketType};
use libfuzzer_sys::fuzz_target;

const RECV_BUFFER: usize = 16 * 1024;
const MAX_TRANSMITS_PER_ROUND: usize = 1 << 16;

// One accepted connection. The input is a series of chunks, each a length
// byte followed by that many bytes: the first byte of a chunk is the number
// of milliseconds the clock advances, the rest is a datagram fed to
// `on_packet`. After each chunk the timer fires and the outgoing queue is
// drained; the receive buffer bound must hold throughout.
fuzz_target!(|data: &[u8]| {
    let mut cfg = UtpConfig::default();
    cfg.recv_buffer = RECV_BUFFER;
    cfg.send_buffer = RECV_BUFFER;
    let peer: SocketAddr = ([127, 0, 0, 1], 9000).into();
    let t0 = Instant::now();
    let syn = Packet {
        ty: PacketType::Syn,
        conn_id: 0x1234,
        timestamp_us: 1000,
        timestamp_diff_us: 0,
        wnd_size: 65_535,
        seq_nr: 100,
        ack_nr: 0,
        sack: None,
        payload: Bytes::new(),
    };
    let mut c = Connection::accept(&cfg, peer, &syn, 500, t0);
    // Give the sender side something to retransmit and the SACKs something to ack.
    let _ = c.write(&[0xab; 4096]);

    let mut now = t0;
    let mut rest = data;
    let mut out = [0u8; 4096];
    while let Some((&len, tail)) = rest.split_first() {
        let n = usize::from(len).min(tail.len());
        let (chunk, next) = tail.split_at(n);
        rest = next;
        let Some((&ms, dgram)) = chunk.split_first() else {
            continue;
        };
        now += Duration::from_millis(u64::from(ms));
        if let Ok(p) = Packet::decode(&Bytes::copy_from_slice(dgram)) {
            c.on_packet(&p, now);
        }
        c.on_timer(now);
        let mut sent = 0;
        while c.poll_transmit(now).is_some() {
            sent += 1;
            assert!(sent < MAX_TRANSMITS_PER_ROUND, "poll_transmit never ends");
        }
        while c.read(&mut out) > 0 {}
        assert!(c.buffered_bytes() <= cfg.recv_buffer);
    }
});
