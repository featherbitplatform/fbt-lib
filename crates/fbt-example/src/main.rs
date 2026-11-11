//! fbt-example: example downloader built on fbt.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use fbt::{
    AddTorrent, EncryptionPolicy, Event, EventMask, EventStream, Magnet, MapMethod, MapProtocol,
    Metainfo, PeerSource, RateLimit, Session, Settings, TorrentHandle, TorrentState, TorrentStatus,
    Transport,
};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};

#[derive(Parser)]
#[command(version, about = "Example BitTorrent client built on fbt")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Connection policy shared by `download` and `seed`.
#[derive(Args)]
struct NetArgs {
    /// Peer connection encryption.
    #[arg(long, value_enum)]
    encryption: Option<Encryption>,
    /// DHT bootstrap node; may be repeated and replaces the public defaults.
    #[arg(long, value_name = "HOST:PORT", value_parser = host_port)]
    dht_bootstrap: Vec<String>,
    /// Do not run the DHT (no public DHT node is contacted).
    #[arg(long)]
    no_dht: bool,
    /// Do not exchange peers with other peers (PEX).
    #[arg(long)]
    no_pex: bool,
    /// Which transports to use, in both directions.
    #[arg(long, value_enum, default_value = "both")]
    transport: TransportChoice,
    /// Address to listen on (TCP and uTP); may be repeated. Replaces
    /// `--port` when given.
    #[arg(long, value_name = "ADDR")]
    listen: Vec<SocketAddr>,
    /// Download limit in bytes per second (suffixes K and M); also applies
    /// to peers on the local network.
    #[arg(long, value_name = "BYTES/S", value_parser = parse_rate)]
    download_limit: Option<u64>,
    /// Upload limit in bytes per second (suffixes K and M); also applies
    /// to peers on the local network.
    #[arg(long, value_name = "BYTES/S", value_parser = parse_rate)]
    upload_limit: Option<u64>,
    /// Find peers on the local network (LSD, multicast).
    #[arg(long)]
    lsd: bool,
    /// Ask the router for port mappings (UPnP and NAT-PMP).
    #[arg(long)]
    nat: bool,
    /// Disk cache size in MiB.
    #[arg(long, value_name = "N")]
    cache_mb: Option<u64>,
}

#[derive(Clone, Copy, ValueEnum)]
enum TransportChoice {
    Both,
    Tcp,
    Utp,
}

impl TransportChoice {
    /// Enables exactly the chosen transports, in both directions.
    fn apply(self, settings: &mut Settings) {
        let (tcp, utp) = match self {
            TransportChoice::Both => (true, true),
            TransportChoice::Tcp => (true, false),
            TransportChoice::Utp => (false, true),
        };
        settings.enable_outgoing_tcp = tcp;
        settings.enable_incoming_tcp = tcp;
        settings.enable_outgoing_utp = utp;
        settings.enable_incoming_utp = utp;
    }
}

/// Parses `1048576`, `512K` or `1M` (powers of 1024) into bytes per second.
fn parse_rate(s: &str) -> Result<u64, String> {
    let bad = || format!("invalid rate {s:?}: expected a number with an optional K or M suffix");
    let (digits, mult) = match s.chars().last() {
        Some('k' | 'K') => (s.get(..s.len() - 1), 1024),
        Some('m' | 'M') => (s.get(..s.len() - 1), 1024 * 1024),
        _ => (Some(s), 1),
    };
    let n: u64 = digits.and_then(|d| d.parse().ok()).ok_or_else(bad)?;
    n.checked_mul(mult).ok_or_else(bad)
}

#[derive(Clone, Copy, ValueEnum)]
enum Encryption {
    Forced,
    Preferred,
    Disabled,
}

fn host_port(s: &str) -> Result<String, String> {
    match s.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => Ok(s.to_string()),
        _ => Err("expected HOST:PORT".to_string()),
    }
}

impl NetArgs {
    fn apply(&self, settings: &mut Settings) {
        settings.dht_enabled = !self.no_dht;
        // LSD multicasts on the LAN and UPnP/NAT-PMP talk to the router:
        // all off unless asked for.
        settings.enable_lsd = self.lsd;
        settings.enable_upnp = self.nat;
        settings.enable_natpmp = self.nat;
        self.transport.apply(settings);
        if self.download_limit.is_some() || self.upload_limit.is_some() {
            settings.rate_limit = RateLimit::new(
                self.download_limit.unwrap_or(0),
                self.upload_limit.unwrap_or(0),
            );
            settings.rate_limit_local_peers = true;
        }
        if let Some(mb) = self.cache_mb {
            settings.cache_size = mb.saturating_mul(1024 * 1024);
        }
        if !self.listen.is_empty() {
            settings.listen_addrs = self.listen.clone();
        }
        settings.pex_enabled = !self.no_pex;
        if let Some(e) = self.encryption {
            settings.encryption = match e {
                Encryption::Forced => EncryptionPolicy::Forced,
                Encryption::Preferred => EncryptionPolicy::Preferred,
                Encryption::Disabled => EncryptionPolicy::Disabled,
            };
        }
        if !self.dht_bootstrap.is_empty() {
            settings.dht_bootstrap_nodes = self.dht_bootstrap.clone();
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Download a torrent.
    Download {
        /// A .torrent file, or a `magnet:` link.
        torrent: String,
        /// Directory to download into.
        #[arg(long)]
        out: PathBuf,
        /// TCP port to listen on (default 6881; 0 picks a free one).
        #[arg(long)]
        port: Option<u16>,
        /// Keep seeding this many seconds after the download finishes.
        #[arg(long, value_name = "SECS")]
        seed_for: Option<u64>,
        /// Extra peer to connect to; may be repeated.
        #[arg(long, value_name = "ADDR")]
        peer: Vec<SocketAddr>,
        #[command(flatten)]
        net: NetArgs,
    },
    /// Seed a torrent whose data is already on disk.
    Seed {
        /// The .torrent file.
        torrent: PathBuf,
        /// Directory holding the torrent's files.
        #[arg(long)]
        data: PathBuf,
        /// TCP port to listen on (default 6881; 0 picks a free one).
        #[arg(long)]
        port: Option<u16>,
        /// Stop after this many seconds (default: until Ctrl-C).
        #[arg(long, value_name = "SECS")]
        seed_for: Option<u64>,
        #[command(flatten)]
        net: NetArgs,
    },
    /// Run a bare DHT node: no torrents, no default bootstrap nodes.
    DhtNode {
        /// UDP and TCP port to listen on (0 picks a free one).
        #[arg(long)]
        port: Option<u16>,
        /// Node to bootstrap from; may be repeated.
        #[arg(long, value_name = "HOST:PORT", value_parser = host_port)]
        dht_bootstrap: Vec<String>,
        /// Stop after this many seconds (default: until Ctrl-C).
        #[arg(long, value_name = "SECS")]
        run_for: Option<u64>,
    },
    /// Announce a fixed probe hash on the LAN and wait for another probe
    /// (used by the interop scripts).
    #[command(hide = true)]
    LsdProbe {
        /// Port to announce.
        #[arg(long)]
        port: u16,
        /// Give up after this many seconds.
        #[arg(long, value_name = "S", default_value_t = 3)]
        timeout_secs: u64,
    },
    /// Loopback throughput and idle-torrent benchmark (used by
    /// `dev/bench.sh`); sends nothing off this machine.
    #[command(hide = true)]
    Bench {
        /// Payload to move from the seed to the leech, in MiB (0: skip).
        #[arg(long, value_name = "MiB", default_value_t = 0, value_parser = clap::value_parser!(u64).range(0..=65536))]
        size: u64,
        /// Transport both sessions use.
        #[arg(long, value_enum, default_value = "tcp")]
        transport: TransportChoice,
        /// Seed this many small torrents idle (0: skip).
        #[arg(long, value_name = "N", default_value_t = 0)]
        torrents: u32,
        /// How long the idle torrents run.
        #[arg(long, value_name = "S", default_value_t = 60)]
        idle_secs: u64,
    },
    /// Print what a .torrent file contains.
    Info {
        /// The .torrent file.
        torrent: PathBuf,
    },
}

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match run(cli.command).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Command) -> Result<ExitCode, Error> {
    match command {
        Command::Info { torrent } => {
            info(&torrent)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Download {
            torrent,
            out,
            port,
            seed_for,
            peer,
            net,
        } => {
            let source = if torrent
                .get(..7)
                .is_some_and(|h| h.eq_ignore_ascii_case("magnet:"))
            {
                Source::Magnet(Magnet::parse(&torrent)?)
            } else {
                Source::File(load(std::path::Path::new(&torrent))?)
            };
            transfer(source, out, port, seed_for, &peer, true, &net).await
        }
        Command::Seed {
            torrent,
            data,
            port,
            seed_for,
            net,
        } => {
            let source = Source::File(load(&torrent)?);
            transfer(source, data, port, seed_for, &[], false, &net).await
        }
        Command::Bench {
            size,
            transport,
            torrents,
            idle_secs,
        } => bench(size, transport, torrents, idle_secs).await,
        Command::LsdProbe { port, timeout_secs } => lsd_probe(port, timeout_secs).await,
        Command::DhtNode {
            port,
            dht_bootstrap,
            run_for,
        } => dht_node(port, dht_bootstrap, run_for).await,
    }
}

fn load(path: &std::path::Path) -> Result<Metainfo, Error> {
    let bytes = std::fs::read(path)?;
    Ok(Metainfo::from_bytes(&bytes)?)
}

fn info(path: &std::path::Path) -> Result<(), Error> {
    let m = load(path)?;
    println!("name: {}", m.info.name);
    println!("info hash: {}", m.info_hashes);
    println!(
        "pieces: {} x {}",
        m.info.pieces.len(),
        human_bytes(u64::from(m.info.piece_len))
    );
    println!("size: {}", human_bytes(m.total_len()));
    println!("files:");
    for f in m.info.files.iter().filter(|f| !f.pad) {
        println!("  {} ({})", f.path, human_bytes(f.len));
    }
    println!("trackers:");
    for (tier, urls) in m.trackers.iter().enumerate() {
        for url in urls {
            println!("  [{tier}] {url}");
        }
    }
    let mut magnet = Magnet::from_info_hashes(m.info_hashes);
    magnet.display_name = Some(m.info.name.clone());
    magnet.trackers = m.trackers.iter().flatten().cloned().collect();
    println!("magnet: {}", magnet.to_uri());
    Ok(())
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS.get(unit).copied().unwrap_or("B"))
}

fn state_name(state: TorrentState) -> &'static str {
    match state {
        TorrentState::DownloadingMetadata => "metadata",
        TorrentState::QueuedForChecking => "queued",
        TorrentState::CheckingFiles { .. } => "checking",
        TorrentState::Downloading => "downloading",
        TorrentState::Seeding => "seeding",
        TorrentState::Paused => "paused",
        TorrentState::Error => "error",
        _ => "unknown",
    }
}

fn progress_line(s: &TorrentStatus) -> String {
    format!(
        "{:5.1}% {}/s down {}/s up {} peers {}",
        s.progress * 100.0,
        human_bytes(s.download_rate),
        human_bytes(s.upload_rate),
        s.num_peers,
        state_name(s.state),
    )
}

/// What `transfer` adds.
enum Source {
    File(Metainfo),
    Magnet(Magnet),
}

/// Runs one torrent until it finishes (plus `seed_for`), errors, or Ctrl-C.
async fn transfer(
    source: Source,
    dir: PathBuf,
    port: Option<u16>,
    seed_for: Option<u64>,
    peers: &[SocketAddr],
    download: bool,
    net: &NetArgs,
) -> Result<ExitCode, Error> {
    let mut settings = Settings::default();
    net.apply(&mut settings);
    if let Some(port) = port
        && net.listen.is_empty()
    {
        settings.listen_addrs = vec![SocketAddr::from(([0, 0, 0, 0], port))];
    }
    let session = Session::new(settings).await?;
    println!("listening on {}", session.local_addr());
    let mut events = session.subscribe(EventMask::STATUS | EventMask::PEER | EventMask::NAT);
    let params = match source {
        Source::File(m) => AddTorrent::new(m, dir),
        Source::Magnet(m) => AddTorrent::magnet(m, dir),
    };
    let handle = session.add_torrent(params).await?;
    for peer in peers {
        handle.add_peer(*peer).await?;
    }
    let code = watch(
        &handle,
        &mut events,
        seed_for.map(Duration::from_secs),
        download,
    )
    .await;
    session.shutdown().await;
    Ok(code)
}

/// Piece length of the benchmark torrents.
const BENCH_PIECE_LEN: u32 = 256 * 1024;
/// How long `bench` stays alive after each measurement line so that
/// `dev/bench.sh` can take its closing `ps` sample.
const BENCH_SETTLE: Duration = Duration::from_secs(2);

/// Deterministic payload that takes no memory: byte `i` is derived from `i`
/// alone, so the seed reads it back on demand and the leech can discard what
/// it receives (the session verifies every piece against the torrent's
/// hashes before it writes it; the leech's reads report "not found").
#[derive(Debug)]
struct BenchStorage {
    discard: bool,
}

/// SplitMix64 of `n`: the payload's 8-byte word number `n`.
fn pattern_word(n: u64) -> u64 {
    let mut z = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn fill_pattern(offset: u64, buf: &mut [u8]) {
    let mut pos = offset;
    let mut rest = buf;
    while !rest.is_empty() {
        let word = pattern_word(pos / 8).to_le_bytes();
        let skip = (pos % 8) as usize;
        let n = (8 - skip).min(rest.len());
        let (head, tail) = std::mem::take(&mut rest).split_at_mut(n);
        if let Some(src) = word.get(skip..skip + n) {
            head.copy_from_slice(src);
        }
        rest = tail;
        pos += n as u64;
    }
}

impl fbt::Storage for BenchStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        if self.discard {
            // Like a file that does not exist yet: the initial check counts
            // every piece as missing.
            return Err(std::io::ErrorKind::NotFound.into());
        }
        fill_pattern(offset, buf);
        Ok(())
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> std::io::Result<()> {
        if self.discard {
            Ok(())
        } else {
            Err(std::io::Error::other("bench seed storage is read-only"))
        }
    }

    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn delete_files(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A one-file torrent `name` of `len` bytes over the benchmark pattern.
fn bench_torrent(name: &str, len: u64, piece_len: u32) -> Result<Metainfo, Error> {
    use sha1::{Digest, Sha1};
    let mut pieces = Vec::new();
    let mut buf = vec![0u8; piece_len as usize];
    let mut off = 0u64;
    while off < len {
        let n = (len - off).min(u64::from(piece_len)) as usize;
        let chunk = buf.get_mut(..n).ok_or("piece buffer")?;
        fill_pattern(off, chunk);
        pieces.extend_from_slice(&Sha1::digest(&*chunk));
        off += n as u64;
    }
    let mut info = fbt_bencode::DictBuilder::new();
    info.insert(b"length", len as i64);
    info.insert(b"name", name);
    info.insert(b"piece length", i64::from(piece_len));
    info.insert(b"pieces", pieces);
    let mut root = fbt_bencode::DictBuilder::new();
    root.insert(b"info", info.build());
    Ok(Metainfo::from_bytes(&fbt_bencode::encode(&root.build()))?)
}

/// Settings for a benchmark session: loopback only, and no DHT, LSD, UPnP,
/// NAT-PMP, PEX or holepunching, so nothing leaves this machine.
fn bench_settings(transport: TransportChoice) -> Settings {
    let mut s = Settings::default();
    s.listen_addrs = vec![SocketAddr::from(([127, 0, 0, 1], 0))];
    s.dht_enabled = false;
    s.dht_bootstrap_nodes = Vec::new();
    s.enable_lsd = false;
    s.enable_upnp = false;
    s.enable_natpmp = false;
    s.pex_enabled = false;
    s.enable_holepunch = false;
    transport.apply(&mut s);
    s
}

/// Polls until every handle is seeding; fails on error or after `limit`.
async fn wait_seeding(handles: &[TorrentHandle], limit: Duration) -> Result<(), Error> {
    let end = Instant::now() + limit;
    loop {
        let mut done = true;
        for h in handles {
            let st = h.status();
            match st.state {
                TorrentState::Seeding => {}
                TorrentState::Error => {
                    return Err(st.error.unwrap_or_else(|| "torrent stopped".into()).into());
                }
                _ => done = false,
            }
        }
        if done {
            return Ok(());
        }
        if Instant::now() >= end {
            return Err("timed out waiting for the torrents to seed".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Seeds and leeches `size_mib` MiB in this process over loopback.
async fn bench_transfer(size_mib: u64, transport: TransportChoice) -> Result<(), Error> {
    let len = size_mib.saturating_mul(1024 * 1024);
    let m = bench_torrent("bench", len, BENCH_PIECE_LEN)?;
    let seed = Session::new(bench_settings(transport)).await?;
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(std::sync::Arc::new(BenchStorage { discard: false }));
    let seed_handle = seed.add_torrent(p).await?;
    wait_seeding(&[seed_handle], Duration::from_secs(300)).await?;
    let leech = Session::new(bench_settings(transport)).await?;
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(std::sync::Arc::new(BenchStorage { discard: true }));
    let handle = leech.add_torrent(p).await?;
    println!("bench: start");
    let t0 = Instant::now();
    handle.add_peer(seed.local_addr()).await?;
    wait_seeding(&[handle], Duration::from_secs(3600)).await?;
    let secs = t0.elapsed().as_secs_f64();
    let mbit = len as f64 * 8.0 / 1e6 / secs;
    let name = match transport {
        TransportChoice::Utp => "utp",
        TransportChoice::Tcp => "tcp",
        TransportChoice::Both => "both",
    };
    println!("bench: {name} {size_mib} MiB in {secs:.2} s = {mbit:.0} Mbit/s");
    tokio::time::sleep(BENCH_SETTLE).await;
    leech.shutdown().await;
    seed.shutdown().await;
    Ok(())
}

/// Seeds `count` small torrents with no peers for `idle_secs`.
async fn bench_idle(count: u32, idle_secs: u64) -> Result<(), Error> {
    let session = Session::new(bench_settings(TransportChoice::Both)).await?;
    let mut handles = Vec::new();
    for i in 0..count {
        let m = bench_torrent(&format!("idle-{i}"), 16 * 1024, 16 * 1024)?;
        let mut p = AddTorrent::new(m, PathBuf::from("unused"));
        p.storage = Some(std::sync::Arc::new(BenchStorage { discard: false }));
        handles.push(session.add_torrent(p).await?);
    }
    wait_seeding(&handles, Duration::from_secs(600)).await?;
    println!("bench: idle start");
    tokio::time::sleep(Duration::from_secs(idle_secs)).await;
    println!("bench: idle {count} torrents for {idle_secs} s");
    tokio::time::sleep(BENCH_SETTLE).await;
    session.shutdown().await;
    Ok(())
}

/// The `bench` subcommand: see `dev/bench.sh`.
async fn bench(
    size_mib: u64,
    transport: TransportChoice,
    torrents: u32,
    idle_secs: u64,
) -> Result<ExitCode, Error> {
    println!("bench: pid {}", std::process::id());
    if size_mib > 0 {
        bench_transfer(size_mib, transport).await?;
    }
    if torrents > 0 {
        bench_idle(torrents, idle_secs).await?;
    }
    Ok(ExitCode::SUCCESS)
}

/// The info hash `lsd-probe` announces and waits for.
const LSD_PROBE_HASH: [u8; 20] = *b"fbt-lsd-probe-hash-1";

/// Announces [`LSD_PROBE_HASH`] with `port` on the real LSD group and exits 0
/// once another process announces it too, 1 after `timeout_secs`.
async fn lsd_probe(port: u16, timeout_secs: u64) -> Result<ExitCode, Error> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let lsd = fbt_net::lsd::Lsd::spawn(fbt_net::lsd::LsdConfig::default(), tx)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    // Short-lived diagnostic bounded by `timeout_secs`, so it re-announces
    // every 500 ms instead of the library's 5-minute cadence.
    let mut announce = interval(Duration::from_millis(500));
    announce.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let found = loop {
        tokio::select! {
            _ = announce.tick() => lsd.announce(port, &[LSD_PROBE_HASH]),
            Some(peer) = rx.recv() => {
                if peer.info_hash == LSD_PROBE_HASH {
                    break Some(peer.addr);
                }
            }
            _ = tokio::time::sleep_until(deadline) => break None,
        }
    };
    lsd.shutdown().await;
    Ok(match found {
        Some(addr) => {
            println!("lsd probe: found {addr}");
            ExitCode::SUCCESS
        }
        None => {
            println!("lsd probe: nothing within {timeout_secs} s");
            ExitCode::FAILURE
        }
    })
}

/// Runs a session with only the DHT until `run_for` elapses or Ctrl-C.
async fn dht_node(
    port: Option<u16>,
    bootstrap: Vec<String>,
    run_for: Option<u64>,
) -> Result<ExitCode, Error> {
    let mut settings = Settings::default();
    settings.dht_enabled = true;
    settings.enable_lsd = false;
    settings.enable_upnp = false;
    settings.enable_natpmp = false;
    settings.dht_bootstrap_nodes = bootstrap;
    if let Some(port) = port {
        settings.listen_addrs = vec![SocketAddr::from(([0, 0, 0, 0], port))];
    }
    let session = Session::new(settings).await?;
    println!("dht node listening on {}", session.local_addr());
    let period = Duration::from_secs(10);
    let mut tick = interval_at(Instant::now() + period, period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let deadline = run_for.map(|s| Instant::now() + Duration::from_secs(s));
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    loop {
        tokio::select! {
            Ok(()) = &mut ctrl_c => break,
            _ = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => break,
            _ = tick.tick() => {
                if let Some(s) = session.dht_stats() {
                    println!(
                        "dht nodes: v4={} v6={} torrents={}",
                        s.nodes_v4, s.nodes_v6, s.stored_info_hashes
                    );
                }
            }
        }
    }
    session.shutdown().await;
    Ok(ExitCode::SUCCESS)
}

fn source_name(source: PeerSource) -> &'static str {
    match source {
        PeerSource::Tracker => "tracker",
        PeerSource::Dht => "dht",
        PeerSource::Pex => "pex",
        PeerSource::Magnet => "magnet",
        PeerSource::Lsd => "lsd",
        _ => "other",
    }
}

fn print_event(handle: &TorrentHandle, event: &Event) {
    match event {
        Event::MetadataReceived { .. } => println!("metadata received: {}", handle.status().name),
        Event::PeersFound { source, count, .. } => {
            println!("found {count} peers via {}", source_name(*source));
        }
        Event::PeerConnected {
            addr, transport, ..
        } => {
            let via = match transport {
                Transport::Tcp => "tcp",
                Transport::Utp => "utp",
                _ => "other",
            };
            println!("peer connected {addr} via {via}");
        }
        Event::PortMapped {
            protocol,
            method,
            external,
            ..
        } => {
            let proto = match protocol {
                MapProtocol::Tcp => "tcp",
                MapProtocol::Udp => "udp",
                _ => "other",
            };
            let via = match method {
                MapMethod::Upnp => "upnp",
                MapMethod::NatPmp => "natpmp",
                MapMethod::Pcp => "pcp",
                _ => "other",
            };
            println!("port mapped {proto} {external} via {via}");
        }
        _ => {}
    }
}

async fn watch(
    handle: &TorrentHandle,
    events: &mut EventStream,
    seed_for: Option<Duration>,
    download: bool,
) -> ExitCode {
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut seeding_since: Option<Instant> = None;
    // A seed with no `--seed-for` runs until Ctrl-C.
    let started = Instant::now();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    loop {
        tokio::select! {
            Ok(()) = &mut ctrl_c => {
                // Graceful stop; an unfinished download is a failure.
                let done = matches!(handle.status().state, TorrentState::Seeding);
                return if download && !done { ExitCode::FAILURE } else { ExitCode::SUCCESS };
            }
            Some(ev) = events.recv() => {
                print_event(handle, &ev);
                continue;
            }
            _ = tick.tick() => {}
        }
        let status = handle.status();
        println!("{}", progress_line(&status));
        if matches!(status.state, TorrentState::Error) {
            eprintln!(
                "error: {}",
                status.error.as_deref().unwrap_or("torrent stopped")
            );
            return ExitCode::FAILURE;
        }
        if download {
            if matches!(status.state, TorrentState::Seeding) {
                let since = *seeding_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= seed_for.unwrap_or_default() {
                    return ExitCode::SUCCESS;
                }
            }
        } else if let Some(limit) = seed_for
            && started.elapsed() >= limit
        {
            return ExitCode::SUCCESS;
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command, fill_pattern, human_bytes, parse_rate};

    #[test]
    fn parse_rate_accepts_numbers_and_suffixes() {
        assert_eq!(parse_rate("0"), Ok(0));
        assert_eq!(parse_rate("1048576"), Ok(1_048_576));
        assert_eq!(parse_rate("512K"), Ok(512 * 1024));
        assert_eq!(parse_rate("512k"), Ok(512 * 1024));
        assert_eq!(parse_rate("1M"), Ok(1024 * 1024));
        assert_eq!(parse_rate("3m"), Ok(3 * 1024 * 1024));
    }

    #[test]
    fn parse_rate_rejects_bad_input() {
        for bad in ["", "K", "M", "12Q", "abc", "-1", "1.5M", "1 M", "1MM"] {
            assert!(parse_rate(bad).is_err(), "{bad:?} should be rejected");
        }
        assert!(parse_rate("18446744073709551615").is_ok());
        assert!(parse_rate("18446744073709551616").is_err());
        assert!(parse_rate("18446744073709551615K").is_err());
        assert!(parse_rate(&format!("{}M", u64::MAX / (1024 * 1024) + 1)).is_err());
    }

    #[test]
    fn cache_mb_flag_sets_cache_size() {
        let cli = Cli::try_parse_from([
            "fbt-example",
            "seed",
            "t.torrent",
            "--data",
            "d",
            "--cache-mb",
            "8",
        ])
        .expect("parses");
        let Command::Seed { net, .. } = cli.command else {
            panic!("not seed");
        };
        let mut settings = fbt::Settings::default();
        net.apply(&mut settings);
        assert_eq!(settings.cache_size, 8 * 1024 * 1024);
    }

    #[test]
    fn bench_pattern_is_offset_independent() {
        let mut whole = [0u8; 64];
        fill_pattern(5, &mut whole);
        for start in 0..whole.len() {
            let mut part = vec![0u8; whole.len() - start];
            fill_pattern(5 + start as u64, &mut part);
            assert_eq!(part, whole[start..]);
        }
        assert_ne!(whole[..8], whole[8..16]);
    }

    #[test]
    fn bench_flags_parse() {
        let cli = Cli::try_parse_from([
            "fbt-example",
            "bench",
            "--size",
            "8",
            "--transport",
            "utp",
            "--torrents",
            "3",
            "--idle-secs",
            "1",
        ])
        .expect("parses");
        let Command::Bench {
            size,
            torrents,
            idle_secs,
            ..
        } = cli.command
        else {
            panic!("not bench");
        };
        assert_eq!((size, torrents, idle_secs), (8, 3, 1));
    }

    #[test]
    fn formats_bytes() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1_258_291), "1.2 MiB");
    }
}
