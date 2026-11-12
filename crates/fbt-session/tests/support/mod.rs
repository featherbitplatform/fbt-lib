//! Shared helpers for the session integration tests.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::io;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fbt_bencode::{DictBuilder, OwnedValue, encode};
use fbt_core::{BlockRef, PeerId};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage, sha1_piece};
use fbt_session::{Event, EventStream, Settings, TorrentHandle, TorrentState};
use fbt_wire::{
    ExtHandshake, Handshake, Message, MessageCodec, Reserved, read_handshake, write_handshake,
};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

/// How long a test waits for something that should happen.
pub const T: Duration = Duration::from_secs(30);

/// Settings for tests: loopback listener on a free port, fast status
/// updates, and no DHT, LSD, UPnP or NAT-PMP (so no test ever contacts a public DHT node
/// or a real router; the DHT
/// tests bootstrap from a loopback node). Transports as by default: uTP
/// first, TCP fallback.
pub fn test_settings() -> Settings {
    let mut s = Settings::default();
    s.listen_addrs = vec![SocketAddr::from(([127, 0, 0, 1], 0))];
    s.status_interval = Duration::from_millis(50);
    s.dht_enabled = false;
    s.dht_bootstrap_nodes = Vec::new();
    s.enable_lsd = false;
    s.enable_upnp = false;
    s.enable_natpmp = false;
    s
}

/// `s` dialling over TCP only: for a session whose test makes it dial a
/// scripted peer that listens on TCP alone (nothing answers on its UDP
/// port, so a uTP attempt would only wait out `utp_connect_timeout`).
pub fn dial_tcp(mut s: Settings) -> Settings {
    s.enable_outgoing_utp = false;
    s
}

/// A multi-file torrent named `torrent` over random data. Returns the
/// metainfo and the concatenated payload.
pub fn make_torrent(files: &[(&str, usize)], piece_len: u32) -> (Metainfo, Vec<u8>) {
    build_torrent(files, piece_len, false)
}

/// [`make_torrent`] with the BEP 27 private flag set.
pub fn make_private_torrent(files: &[(&str, usize)], piece_len: u32) -> (Metainfo, Vec<u8>) {
    build_torrent(files, piece_len, true)
}

fn build_torrent(files: &[(&str, usize)], piece_len: u32, private: bool) -> (Metainfo, Vec<u8>) {
    let total: usize = files.iter().map(|f| f.1).sum();
    let mut payload = vec![0u8; total];
    rand::rng().fill(payload.as_mut_slice());
    let mut pieces = Vec::new();
    for chunk in payload.chunks(piece_len as usize) {
        pieces.extend_from_slice(&sha1_piece(chunk));
    }
    let file_list = files
        .iter()
        .map(|(name, len)| {
            let mut f = DictBuilder::new();
            f.insert(b"length", *len as i64);
            f.insert(
                b"path",
                OwnedValue::List(name.split('/').map(OwnedValue::from).collect::<Vec<_>>()),
            );
            f.build()
        })
        .collect();
    let mut info = DictBuilder::new();
    info.insert(b"files", OwnedValue::List(file_list));
    info.insert(b"name", "torrent");
    info.insert(b"piece length", i64::from(piece_len));
    info.insert(b"pieces", pieces);
    if private {
        info.insert(b"private", 1i64);
    }
    let mut root = DictBuilder::new();
    root.insert(b"info", info.build());
    let bytes = encode(&root.build());
    let m = Metainfo::from_bytes(&bytes).expect("valid generated torrent");
    (m, payload)
}

/// A multi-file torrent like [`make_torrent`], with a BEP 47 pad file
/// (`attr: "p"`, zero bytes up to the next piece boundary, at least one)
/// inserted after file `pad_after`. Returns the metainfo and the payload,
/// pad bytes included.
pub fn make_torrent_with_pad(
    files: &[(&str, usize)],
    pad_after: usize,
    piece_len: u32,
) -> (Metainfo, Vec<u8>) {
    let before: usize = files.iter().take(pad_after + 1).map(|f| f.1).sum();
    let pad_len = match before % piece_len as usize {
        0 => piece_len as usize,
        r => piece_len as usize - r,
    };
    let mut entries: Vec<(String, usize, bool)> = Vec::new();
    for (i, (name, len)) in files.iter().enumerate() {
        entries.push(((*name).to_owned(), *len, false));
        if i == pad_after {
            entries.push((format!(".pad/{pad_len}"), pad_len, true));
        }
    }
    let total: usize = entries.iter().map(|e| e.1).sum();
    let mut payload = vec![0u8; total];
    let mut off = 0;
    for (_, len, pad) in &entries {
        if !pad {
            rand::rng().fill(&mut payload[off..off + len]);
        }
        off += len;
    }
    let mut pieces = Vec::new();
    for chunk in payload.chunks(piece_len as usize) {
        pieces.extend_from_slice(&sha1_piece(chunk));
    }
    let file_list = entries
        .iter()
        .map(|(name, len, pad)| {
            let mut f = DictBuilder::new();
            if *pad {
                f.insert(b"attr", "p");
            }
            f.insert(b"length", *len as i64);
            f.insert(
                b"path",
                OwnedValue::List(name.split('/').map(OwnedValue::from).collect::<Vec<_>>()),
            );
            f.build()
        })
        .collect();
    let mut info = DictBuilder::new();
    info.insert(b"files", OwnedValue::List(file_list));
    info.insert(b"name", "torrent");
    info.insert(b"piece length", i64::from(piece_len));
    info.insert(b"pieces", pieces);
    let mut root = DictBuilder::new();
    root.insert(b"info", info.build());
    let bytes = encode(&root.build());
    let m = Metainfo::from_bytes(&bytes).expect("valid generated torrent");
    (m, payload)
}

/// Writes `payload` as the torrent's files below `save_path`.
pub fn write_payload(save_path: &Path, m: &Metainfo, payload: &[u8]) {
    let mut off = 0usize;
    for f in &m.info.files {
        // Entry paths already start with the torrent's directory.
        let p = save_path.join(f.path.to_path_buf());
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        let len = f.len as usize;
        std::fs::write(&p, &payload[off..off + len]).expect("write payload");
        off += len;
    }
}

/// The next event matching `pred` within `timeout`; `None` on timeout or end.
pub async fn next_matching(
    stream: &mut EventStream,
    pred: impl Fn(&Event) -> bool,
    timeout: Duration,
) -> Option<Event> {
    tokio::time::timeout(timeout, async {
        while let Some(ev) = stream.recv().await {
            if pred(&ev) {
                return Some(ev);
            }
        }
        None
    })
    .await
    .ok()
    .flatten()
}

/// Polls `handle.status()` until its state matches `pred`.
pub async fn wait_state(
    handle: &TorrentHandle,
    pred: impl Fn(&TorrentState) -> bool,
    timeout: Duration,
) -> bool {
    let end = tokio::time::Instant::now() + timeout;
    loop {
        if pred(&handle.status().state) {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A minimal hand-driven remote peer speaking the wire protocol.
pub struct ScriptedPeer {
    pub framed: Framed<TcpStream, MessageCodec>,
    pub remote: Handshake,
}

impl ScriptedPeer {
    /// Connects to `addr` and exchanges handshakes for `info_hash` (no fast
    /// extension) under a fresh random peer id.
    pub async fn connect(addr: SocketAddr, info_hash: [u8; 20]) -> ScriptedPeer {
        Self::connect_as(addr, info_hash, random_id(), false).await
    }

    /// Like [`connect`](Self::connect), advertising the fast extension.
    pub async fn connect_fast(addr: SocketAddr, info_hash: [u8; 20]) -> ScriptedPeer {
        Self::connect_as(addr, info_hash, random_id(), true).await
    }

    /// Connects under the given peer id.
    pub async fn connect_as(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: PeerId,
        fast: bool,
    ) -> ScriptedPeer {
        let mut reserved = Reserved::default();
        if fast {
            reserved.set_fast();
        }
        Self::connect_raw(addr, info_hash, peer_id, reserved).await
    }

    /// Connects advertising exactly the `reserved` bits.
    pub async fn connect_with(
        addr: SocketAddr,
        info_hash: [u8; 20],
        reserved: Reserved,
    ) -> ScriptedPeer {
        Self::connect_raw(addr, info_hash, random_id(), reserved).await
    }

    /// Accepts one connection on `listener` (the session dialling us,
    /// without MSE) and answers its handshake with the `reserved` bits.
    pub async fn accept_with(listener: &TcpListener, reserved: Reserved) -> ScriptedPeer {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("dialled in time")
            .expect("accept");
        let remote = tokio::time::timeout(Duration::from_secs(5), read_handshake(&mut stream))
            .await
            .expect("handshake in time")
            .expect("remote handshake");
        let ours = Handshake {
            reserved,
            info_hash: remote.info_hash,
            peer_id: random_id(),
        };
        write_handshake(&mut stream, &ours)
            .await
            .expect("handshake");
        ScriptedPeer {
            framed: Framed::new(stream, MessageCodec::new(1 << 20)),
            remote,
        }
    }

    async fn connect_raw(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: PeerId,
        reserved: Reserved,
    ) -> ScriptedPeer {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let ours = Handshake {
            reserved,
            info_hash,
            peer_id,
        };
        write_handshake(&mut stream, &ours)
            .await
            .expect("handshake");
        let remote = tokio::time::timeout(Duration::from_secs(5), read_handshake(&mut stream))
            .await
            .expect("handshake in time")
            .expect("remote handshake");
        ScriptedPeer {
            framed: Framed::new(stream, MessageCodec::new(1 << 20)),
            remote,
        }
    }

    pub async fn send(&mut self, m: Message) {
        self.framed.send(m).await.expect("send");
    }

    /// Sends a BEP 10 extension handshake.
    pub async fn send_ext_handshake(&mut self, hs: &ExtHandshake) {
        self.send(Message::Extended {
            id: 0,
            payload: hs.encode(),
        })
        .await;
    }

    /// The payload of the next extended message with `id` within [`T`].
    pub async fn next_ext(&mut self, id: u8) -> Option<Bytes> {
        match self
            .next_matching(|m| matches!(m, Message::Extended { id: i, .. } if *i == id))
            .await
        {
            Some(Message::Extended { payload, .. }) => Some(payload),
            _ => None,
        }
    }

    /// The next message matching `pred` within [`T`] (30 s).
    pub async fn next_matching(&mut self, pred: impl Fn(&Message) -> bool) -> Option<Message> {
        self.next_matching_within(T, pred).await
    }

    /// The next message matching `pred` within `timeout`: for checks that
    /// expect none, which wait out the whole timeout.
    pub async fn next_matching_within(
        &mut self,
        timeout: Duration,
        pred: impl Fn(&Message) -> bool,
    ) -> Option<Message> {
        tokio::time::timeout(timeout, async {
            while let Some(Ok(m)) = self.framed.next().await {
                if pred(&m) {
                    return Some(m);
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }

    /// Reads until the remote closes the connection (messages before the
    /// close are skipped) or `timeout` passes.
    pub async fn wait_closed(&mut self, timeout: Duration) -> PeerEnd {
        let r = tokio::time::timeout(timeout, async {
            loop {
                match self.framed.next().await {
                    Some(Ok(_)) => {}
                    // A reset is a close too.
                    Some(Err(_)) | None => return PeerEnd::Closed,
                }
            }
        })
        .await;
        r.unwrap_or(PeerEnd::TimedOut)
    }

    /// Answers every request with the matching bytes of `payload` until the
    /// connection closes.
    pub async fn serve(&mut self, payload: &[u8], piece_len: u32) {
        while let Some(Ok(m)) = self.framed.next().await {
            if let Message::Request(b) = m {
                let start = b.piece as usize * piece_len as usize + b.offset as usize;
                let data = Bytes::copy_from_slice(&payload[start..start + b.len as usize]);
                let piece = Message::Piece {
                    piece: b.piece,
                    offset: b.offset,
                    data,
                };
                if self.framed.send(piece).await.is_err() {
                    return;
                }
            }
        }
    }
}

impl ScriptedPeer {
    /// [`serve`](Self::serve) that also logs every `Request` it receives
    /// into `log`.
    pub async fn serve_recording(
        &mut self,
        payload: &[u8],
        piece_len: u32,
        log: Arc<Mutex<Vec<BlockRef>>>,
    ) {
        let cancels = Arc::new(Mutex::new(Vec::new()));
        let answer = Arc::new(AtomicBool::new(true));
        self.serve_recording_with(payload, piece_len, log, cancels, answer)
            .await;
    }

    /// Logs every `Request` into `log` and every `Cancel` into `cancels`,
    /// and answers requests with the matching bytes of `payload` while
    /// `answer` is set (a request that arrives while it is clear stays
    /// unanswered), until the connection closes.
    pub async fn serve_recording_with(
        &mut self,
        payload: &[u8],
        piece_len: u32,
        log: Arc<Mutex<Vec<BlockRef>>>,
        cancels: Arc<Mutex<Vec<BlockRef>>>,
        answer: Arc<AtomicBool>,
    ) {
        while let Some(Ok(m)) = self.framed.next().await {
            match m {
                Message::Request(b) => {
                    log.lock().unwrap().push(b);
                    if !answer.load(Ordering::SeqCst) {
                        continue;
                    }
                    let start = b.piece as usize * piece_len as usize + b.offset as usize;
                    let data = Bytes::copy_from_slice(&payload[start..start + b.len as usize]);
                    let piece = Message::Piece {
                        piece: b.piece,
                        offset: b.offset,
                        data,
                    };
                    if self.framed.send(piece).await.is_err() {
                        return;
                    }
                }
                Message::Cancel(b) => cancels.lock().unwrap().push(b),
                _ => {}
            }
        }
    }
}

fn random_id() -> PeerId {
    let mut id = [0u8; 20];
    rand::rng().fill(&mut id[..]);
    PeerId(id)
}

/// How waiting for a connection to end turned out.
#[derive(Debug, PartialEq, Eq)]
pub enum PeerEnd {
    /// The remote closed (or reset) the connection.
    Closed,
    /// Still open when the wait ran out.
    TimedOut,
}

/// Polls `cond` every 10 ms until it holds or `timeout` passes.
pub async fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let end = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Memory storage that serves reads one at a time, each after `delay`: a
/// rate-throttled seeder (about `16 KiB / delay` per second).
#[derive(Debug)]
pub struct SlowStorage {
    pub inner: MemStorage,
    pub delay: Duration,
    gate: Mutex<()>,
}

impl SlowStorage {
    pub fn new(data: Vec<u8>, delay: Duration) -> Self {
        SlowStorage {
            inner: MemStorage::from_bytes(data),
            delay,
            gate: Mutex::new(()),
        }
    }
}

impl Storage for SlowStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let _g = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        std::thread::sleep(self.delay);
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

/// Memory storage whose reads take `delay` each while `slow` is set and are
/// instant otherwise: a test decides when a check or an upload is slow and
/// when it may end, so no assertion depends on wall-clock durations.
#[derive(Debug)]
pub struct ToggleStorage {
    inner: MemStorage,
    delay: Duration,
    slow: Arc<AtomicBool>,
}

impl ToggleStorage {
    pub fn new(data: Vec<u8>, delay: Duration, slow: Arc<AtomicBool>) -> Self {
        ToggleStorage {
            inner: MemStorage::from_bytes(data),
            delay,
            slow,
        }
    }
}

impl Storage for ToggleStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if self.slow.load(Ordering::SeqCst) {
            std::thread::sleep(self.delay);
        }
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

/// Memory storage that, once `armed`, returns flipped bytes for reads
/// overlapping `bad`. Armed after the initial check, it passes the check and
/// then serves corrupt data.
#[derive(Debug)]
pub struct CorruptStorage {
    pub inner: MemStorage,
    pub bad: Range<u64>,
    pub armed: AtomicBool,
}

impl CorruptStorage {
    pub fn new(data: Vec<u8>, bad: Range<u64>) -> Self {
        CorruptStorage {
            inner: MemStorage::from_bytes(data),
            bad,
            armed: AtomicBool::new(false),
        }
    }
}

impl Storage for CorruptStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, buf)?;
        if self.armed.load(Ordering::SeqCst) {
            for (i, b) in buf.iter_mut().enumerate() {
                if self.bad.contains(&(offset + i as u64)) {
                    *b ^= 0xff;
                }
            }
        }
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

// ----- fake trackers ----------------------------------------------------------

/// One announce as a fake tracker recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announce {
    pub info_hash: [u8; 20],
    /// The announcing peer's listen port.
    pub port: u16,
    /// `started`, `completed`, `stopped` or `none`.
    pub event: String,
    /// The `left` the announcer reported.
    pub left: u64,
    /// Where the announce came from.
    pub source: SocketAddr,
}

/// How a fake tracker answers; scrapes too, except that a peers reply
/// scrapes the swarm counts.
#[derive(Debug, Clone)]
pub enum Reply {
    /// Peers from its swarm.
    Peers,
    /// A bencoded `failure reason` (HTTP) or an error action (UDP).
    Failure(String),
    /// HTTP 500 (HTTP); no answer at all (UDP).
    ServerError,
    /// Peers, but a `completed` announce is answered only after the delay
    /// (HTTP only). The announce is recorded at once.
    PeersDelayCompleted(Duration),
    /// HTTP 500 for the first `completed` announce, peers otherwise (HTTP
    /// only).
    ServerErrorOnFirstCompleted,
}

#[derive(Debug, Default)]
struct TrackerState {
    /// Peers per info hash with the `left` each last reported; every peer
    /// is on 127.0.0.1.
    swarms: std::collections::HashMap<[u8; 20], Vec<(SocketAddr, u64)>>,
    announces: Vec<Announce>,
}

impl TrackerState {
    /// Records `a`, updates the swarm and returns the other peers.
    fn announce(&mut self, a: Announce) -> Vec<SocketAddr> {
        let me = SocketAddr::from(([127, 0, 0, 1], a.port));
        let swarm = self.swarms.entry(a.info_hash).or_default();
        swarm.retain(|(p, _)| *p != me);
        if a.event != "stopped" {
            swarm.push((me, a.left));
        }
        let others = swarm.iter().map(|(p, _)| *p).filter(|p| *p != me).collect();
        self.announces.push(a);
        others
    }

    /// `(complete, downloaded, incomplete)` of a swarm: seeds, `completed`
    /// announces and leechers.
    fn counts(&self, info_hash: &[u8; 20]) -> (u32, u32, u32) {
        let swarm = self.swarms.get(info_hash).map_or(&[][..], Vec::as_slice);
        let seeds = swarm.iter().filter(|(_, left)| *left == 0).count() as u32;
        let downloaded = self
            .announces
            .iter()
            .filter(|a| a.info_hash == *info_hash && a.event == "completed")
            .count() as u32;
        (seeds, downloaded, swarm.len() as u32 - seeds)
    }
}

fn compact(peers: &[SocketAddr]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in peers {
        if let SocketAddr::V4(v4) = p {
            out.extend_from_slice(&v4.ip().octets());
            out.extend_from_slice(&v4.port().to_be_bytes());
        }
    }
    out
}

/// An in-memory HTTP or UDP tracker on loopback that keeps a swarm per info
/// hash and records every announce.
#[derive(Debug)]
pub struct FakeTracker {
    pub url: String,
    state: std::sync::Arc<Mutex<TrackerState>>,
    reply: std::sync::Arc<Mutex<Reply>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeTracker {
    /// An HTTP tracker answering every announce with `reply` (until
    /// [`set_reply`](Self::set_reply)); it scrapes on `/scrape`.
    pub async fn http(reply: Reply) -> FakeTracker {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = std::sync::Arc::new(Mutex::new(TrackerState::default()));
        let reply = std::sync::Arc::new(Mutex::new(reply));
        let (st, rp) = (state.clone(), reply.clone());
        let task = tokio::spawn(async move {
            while let Ok((stream, from)) = listener.accept().await {
                let reply = rp.lock().unwrap_or_else(|e| e.into_inner()).clone();
                tokio::spawn(http_conn(stream, from, st.clone(), reply));
            }
        });
        FakeTracker {
            url: format!("http://{addr}/announce"),
            state,
            reply,
            task,
        }
    }

    /// A UDP (BEP 15) tracker answering every announce with `reply` (until
    /// [`set_reply`](Self::set_reply)); it scrapes on action 2.
    pub async fn udp(reply: Reply) -> FakeTracker {
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        let state = std::sync::Arc::new(Mutex::new(TrackerState::default()));
        let reply = std::sync::Arc::new(Mutex::new(reply));
        let task = tokio::spawn(udp_loop(sock, state.clone(), reply.clone()));
        FakeTracker {
            url: format!("udp://{addr}"),
            state,
            reply,
            task,
        }
    }

    /// Answers every request from now on with `reply`.
    pub fn set_reply(&self, reply: Reply) {
        *self.reply.lock().unwrap_or_else(|e| e.into_inner()) = reply;
    }

    /// Every announce so far.
    pub fn announces(&self) -> Vec<Announce> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .announces
            .clone()
    }

    /// The events announced by the peer listening on `port`, in order.
    pub fn events_of(&self, port: u16) -> Vec<String> {
        self.announces()
            .into_iter()
            .filter(|a| a.port == port)
            .map(|a| a.event)
            .collect()
    }
}

fn percent_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(if b[i] == b'+' { b' ' } else { b[i] });
            i += 1;
        }
    }
    out
}

async fn http_conn(
    mut stream: TcpStream,
    source: SocketAddr,
    state: std::sync::Arc<Mutex<TrackerState>>,
    reply: Reply,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 16 * 1024 {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&buf).into_owned();
    let target = head.split_whitespace().nth(1).unwrap_or("").to_owned();
    let query = target.split_once('?').map_or("", |(_, q)| q);
    if target.starts_with("/scrape") {
        let hashes: Vec<[u8; 20]> = query
            .split('&')
            .filter_map(|pair| pair.strip_prefix("info_hash="))
            .map(|v| percent_decode(v).try_into().unwrap())
            .collect();
        let (status, body) = match reply {
            Reply::ServerError => ("500 Internal Server Error", Vec::new()),
            Reply::Failure(msg) => (
                "200 OK",
                format!("d14:failure reason{}:{}e", msg.len(), msg).into_bytes(),
            ),
            _ => {
                let mut body = b"d5:filesd".to_vec();
                let mut sorted = hashes.clone();
                sorted.sort_unstable();
                sorted.dedup();
                let st = state.lock().unwrap_or_else(|e| e.into_inner());
                for h in sorted {
                    let (c, d, i) = st.counts(&h);
                    body.extend_from_slice(b"20:");
                    body.extend_from_slice(&h);
                    body.extend_from_slice(
                        format!("d8:completei{c}e10:downloadedi{d}e10:incompletei{i}ee").as_bytes(),
                    );
                }
                body.extend_from_slice(b"ee");
                ("200 OK", body)
            }
        };
        let mut resp = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        resp.extend_from_slice(&body);
        let _ = stream.write_all(&resp).await;
        let _ = stream.shutdown().await;
        return;
    }
    let mut info_hash = [0u8; 20];
    let mut port = 0u16;
    let mut event = "none".to_owned();
    let mut left = 0u64;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        match k {
            "info_hash" => info_hash.copy_from_slice(&percent_decode(v)),
            "port" => port = v.parse().unwrap(),
            "event" => event = v.to_owned(),
            "left" => left = v.parse().unwrap(),
            _ => {}
        }
    }
    let peers = state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .announce(Announce {
            info_hash,
            port,
            event: event.clone(),
            left,
            source,
        });
    let first_completed = event == "completed"
        && state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .announces
            .iter()
            .filter(|a| a.event == "completed")
            .count()
            == 1;
    if let Reply::PeersDelayCompleted(d) = &reply
        && event == "completed"
    {
        tokio::time::sleep(*d).await;
    }
    let (status, body) = match reply {
        Reply::ServerError => ("500 Internal Server Error", Vec::new()),
        Reply::ServerErrorOnFirstCompleted if first_completed => {
            ("500 Internal Server Error", Vec::new())
        }
        Reply::Peers | Reply::PeersDelayCompleted(_) | Reply::ServerErrorOnFirstCompleted => {
            let c = compact(&peers);
            let mut body = format!("d8:intervali1800e5:peers{}:", c.len()).into_bytes();
            body.extend_from_slice(&c);
            body.push(b'e');
            ("200 OK", body)
        }
        Reply::Failure(msg) => (
            "200 OK",
            format!("d14:failure reason{}:{}e", msg.len(), msg).into_bytes(),
        ),
    };
    let mut resp = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    resp.extend_from_slice(&body);
    let _ = stream.write_all(&resp).await;
    let _ = stream.shutdown().await;
}

async fn udp_loop(
    sock: tokio::net::UdpSocket,
    state: std::sync::Arc<Mutex<TrackerState>>,
    reply: std::sync::Arc<Mutex<Reply>>,
) {
    const CID: u64 = 0x0102_0304_0506_0708;
    let be32 = |b: &[u8], o: usize| u32::from_be_bytes(b[o..o + 4].try_into().unwrap());
    let mut buf = vec![0u8; 2048];
    loop {
        let Ok((n, from)) = sock.recv_from(&mut buf).await else {
            continue;
        };
        let b = &buf[..n];
        if n < 16 {
            continue;
        }
        let action = be32(b, 8);
        let tid = &b[12..16];
        let reply = reply.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if matches!(reply, Reply::ServerError) {
            continue;
        }
        let mut out = Vec::new();
        if action == 2 && b[..8] == CID.to_be_bytes() {
            if let Reply::Failure(msg) = &reply {
                out.extend_from_slice(&3u32.to_be_bytes());
                out.extend_from_slice(tid);
                out.extend_from_slice(msg.as_bytes());
            } else {
                out.extend_from_slice(&2u32.to_be_bytes());
                out.extend_from_slice(tid);
                let st = state.lock().unwrap_or_else(|e| e.into_inner());
                for h in b[16..].as_chunks::<20>().0 {
                    let (c, d, i) = st.counts(h);
                    for n in [c, d, i] {
                        out.extend_from_slice(&n.to_be_bytes());
                    }
                }
            }
        } else if action == 0 && b[..8] == 0x0417_2710_1980u64.to_be_bytes() {
            out.extend_from_slice(&0u32.to_be_bytes());
            out.extend_from_slice(tid);
            out.extend_from_slice(&CID.to_be_bytes());
        } else if action == 1 && n >= 98 && b[..8] == CID.to_be_bytes() {
            let event = match be32(b, 80) {
                1 => "completed",
                2 => "started",
                3 => "stopped",
                _ => "none",
            };
            let peers = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .announce(Announce {
                    info_hash: b[16..36].try_into().unwrap(),
                    port: u16::from_be_bytes([b[96], b[97]]),
                    event: event.to_owned(),
                    left: u64::from_be_bytes(b[64..72].try_into().unwrap()),
                    source: from,
                });
            match &reply {
                Reply::Failure(msg) => {
                    out.extend_from_slice(&3u32.to_be_bytes());
                    out.extend_from_slice(tid);
                    out.extend_from_slice(msg.as_bytes());
                }
                _ => {
                    out.extend_from_slice(&1u32.to_be_bytes());
                    out.extend_from_slice(tid);
                    out.extend_from_slice(&1800u32.to_be_bytes());
                    out.extend_from_slice(&0u32.to_be_bytes());
                    out.extend_from_slice(&(peers.len() as u32).to_be_bytes());
                    out.extend_from_slice(&compact(&peers));
                }
            }
        } else {
            continue;
        }
        let _ = sock.send_to(&out, from).await;
    }
}

/// Memory storage that counts its reads and logs each read's offset.
#[derive(Debug)]
pub struct CountingStorage {
    inner: MemStorage,
    log: Mutex<Vec<u64>>,
}

impl CountingStorage {
    pub fn new(data: Vec<u8>) -> Self {
        CountingStorage {
            inner: MemStorage::from_bytes(data),
            log: Mutex::new(Vec::new()),
        }
    }

    /// Reads so far.
    pub fn reads(&self) -> usize {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// The offset of every read so far, in order.
    pub fn offsets(&self) -> Vec<u64> {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Forgets the reads so far.
    pub fn reset(&self) {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

impl Storage for CountingStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(offset);
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

/// Zeroed memory storage whose writes fail while `fail` is set.
#[derive(Debug)]
pub struct FailWritesStorage {
    inner: MemStorage,
    fail: Arc<AtomicBool>,
}

impl FailWritesStorage {
    pub fn new(len: u64, fail: Arc<AtomicBool>) -> Self {
        FailWritesStorage {
            inner: MemStorage::new(len),
            fail,
        }
    }
}

impl Storage for FailWritesStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "disk full"));
        }
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

/// Zeroed memory storage whose writes block while `gate` is set (polled
/// every 10 ms): a test holds every write and decides when they land.
#[derive(Debug)]
pub struct GatedWriteStorage {
    inner: MemStorage,
    gate: Arc<AtomicBool>,
}

impl GatedWriteStorage {
    pub fn new(len: u64, gate: Arc<AtomicBool>) -> Self {
        GatedWriteStorage {
            inner: MemStorage::new(len),
            gate,
        }
    }
}

impl Storage for GatedWriteStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        while self.gate.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}

/// Zeroed memory storage whose reads block while `gate` is set (polled
/// every 10 ms) and that logs each read's offset as it starts: a test
/// holds a disk read in flight and knows when it is.
#[derive(Debug)]
pub struct GatedReadStorage {
    inner: MemStorage,
    gate: Arc<AtomicBool>,
    log: Mutex<Vec<u64>>,
}

impl GatedReadStorage {
    pub fn new(len: u64, gate: Arc<AtomicBool>) -> Self {
        GatedReadStorage {
            inner: MemStorage::new(len),
            gate,
            log: Mutex::new(Vec::new()),
        }
    }

    /// The offset of every read started so far, in order.
    pub fn offsets(&self) -> Vec<u64> {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Storage for GatedReadStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(offset);
        while self.gate.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.inner.delete_files()
    }
}
