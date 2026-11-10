//! fbt-example: example downloader built on fbt.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use fbt::{
    AddTorrent, EncryptionPolicy, Event, EventMask, EventStream, Magnet, Metainfo, PeerSource,
    Session, Settings, TorrentHandle, TorrentState, TorrentStatus,
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
    if let Some(port) = port {
        settings.listen_addr.set_port(port);
    }
    let session = Session::new(settings).await?;
    println!("listening on {}", session.local_addr());
    let mut events = session.subscribe(EventMask::STATUS | EventMask::PEER);
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

/// Runs a session with only the DHT until `run_for` elapses or Ctrl-C.
async fn dht_node(
    port: Option<u16>,
    bootstrap: Vec<String>,
    run_for: Option<u64>,
) -> Result<ExitCode, Error> {
    let mut settings = Settings::default();
    settings.dht_enabled = true;
    settings.dht_bootstrap_nodes = bootstrap;
    if let Some(port) = port {
        settings.listen_addr.set_port(port);
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
        _ => "other",
    }
}

fn print_event(handle: &TorrentHandle, event: &Event) {
    match event {
        Event::MetadataReceived { .. } => println!("metadata received: {}", handle.status().name),
        Event::PeersFound { source, count, .. } => {
            println!("found {count} peers via {}", source_name(*source));
        }
        Event::PeerConnected { addr, .. } => println!("peer connected {addr}"),
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
    use super::human_bytes;

    #[test]
    fn formats_bytes() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1_258_291), "1.2 MiB");
    }
}
