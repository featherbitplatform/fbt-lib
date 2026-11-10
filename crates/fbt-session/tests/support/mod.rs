//! Shared helpers for the session integration tests.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::io;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fbt_bencode::{DictBuilder, OwnedValue, encode};
use fbt_core::PeerId;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage, sha1_piece};
use fbt_session::{Event, EventStream, Settings, TorrentHandle, TorrentState};
use fbt_wire::{Handshake, Message, MessageCodec, Reserved, read_handshake, write_handshake};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// Settings for tests: loopback listener on a free port, fast status updates.
pub fn test_settings() -> Settings {
    let mut s = Settings::default();
    s.listen_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    s.status_interval = Duration::from_millis(50);
    s
}

/// A multi-file torrent named `torrent` over random data. Returns the
/// metainfo and the concatenated payload.
pub fn make_torrent(files: &[(&str, usize)], piece_len: u32) -> (Metainfo, Vec<u8>) {
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
    /// extension).
    pub async fn connect(addr: SocketAddr, info_hash: [u8; 20]) -> ScriptedPeer {
        Self::connect_with(addr, info_hash, Reserved::default()).await
    }

    /// Like [`connect`](Self::connect), advertising the fast extension.
    pub async fn connect_fast(addr: SocketAddr, info_hash: [u8; 20]) -> ScriptedPeer {
        let mut reserved = Reserved::default();
        reserved.set_fast();
        Self::connect_with(addr, info_hash, reserved).await
    }

    async fn connect_with(
        addr: SocketAddr,
        info_hash: [u8; 20],
        reserved: Reserved,
    ) -> ScriptedPeer {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let ours = Handshake {
            reserved,
            info_hash,
            peer_id: PeerId([0x5a; 20]),
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

    /// The next message matching `pred` within five seconds.
    pub async fn next_matching(&mut self, pred: impl Fn(&Message) -> bool) -> Option<Message> {
        tokio::time::timeout(Duration::from_secs(5), async {
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
