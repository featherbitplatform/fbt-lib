//! fbt-example: example downloader built on fbt.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use fbt::{AddTorrent, Metainfo, Session, Settings, TorrentHandle, TorrentState, TorrentStatus};
use tokio::time::{Instant, MissedTickBehavior, interval};

#[derive(Parser)]
#[command(version, about = "Example BitTorrent client built on fbt")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download a torrent.
    Download {
        /// The .torrent file.
        torrent: PathBuf,
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
        } => transfer(&torrent, out, port, seed_for, &peer, true).await,
        Command::Seed {
            torrent,
            data,
            port,
            seed_for,
        } => transfer(&torrent, data, port, seed_for, &[], false).await,
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

/// Runs one torrent until it finishes (plus `seed_for`), errors, or Ctrl-C.
async fn transfer(
    torrent: &std::path::Path,
    dir: PathBuf,
    port: Option<u16>,
    seed_for: Option<u64>,
    peers: &[SocketAddr],
    download: bool,
) -> Result<ExitCode, Error> {
    let metainfo = load(torrent)?;
    let mut settings = Settings::default();
    if let Some(port) = port {
        settings.listen_addr.set_port(port);
    }
    let session = Session::new(settings).await?;
    println!("listening on {}", session.local_addr());
    let handle = session.add_torrent(AddTorrent::new(metainfo, dir)).await?;
    for peer in peers {
        handle.add_peer(*peer).await?;
    }
    let code = watch(&handle, seed_for.map(Duration::from_secs), download).await;
    session.shutdown().await;
    Ok(code)
}

async fn watch(handle: &TorrentHandle, seed_for: Option<Duration>, download: bool) -> ExitCode {
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
