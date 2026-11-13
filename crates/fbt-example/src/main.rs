//! fbt-example: example downloader built on fbt.

#![forbid(unsafe_code)]

use std::io::{SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use fbt::{
    AddTorrent, CreateTorrent, EncryptionPolicy, Event, EventMask, EventStream, InfoHashV1,
    InfoHashes, Magnet, MapMethod, MapProtocol, Metainfo, PeerSource, Priority, ProxyConfig,
    ProxyKind, RateLimit, ReadAhead, Session, SessionError, Settings, TorrentHandle, TorrentSource,
    TorrentState, TorrentStatus, TorrentVersion, Transport,
};
use fbt_api::{ApiConfig, ApiServer};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};

#[derive(Parser)]
#[command(version, about = "Example BitTorrent client built on fbt")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The HTTP/JSON API (`fbt-api`) next to `download` and `seed`.
#[derive(Args)]
struct ApiArgs {
    /// Serve the HTTP/JSON API on this address (port 0 picks a free one);
    /// needs `--api-token`.
    #[arg(long, value_name = "ADDR", requires = "api_token")]
    api: Option<SocketAddr>,
    /// Bearer token the API requires on every request.
    #[arg(long, value_name = "TOKEN", requires = "api")]
    api_token: Option<String>,
}

impl ApiArgs {
    /// The API config, when `--api` is given.
    fn config(&self) -> Option<ApiConfig> {
        let (bind, token) = (self.api?, self.api_token.clone()?);
        let mut config = ApiConfig::new(token);
        config.bind = bind;
        Some(config)
    }
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
    /// Proxy for peers, web seeds and trackers:
    /// `socks5://[user:pass@]host:port` or `http://[user:pass@]host:port`
    /// (credentials percent-decoded). SOCKS5 also carries the DHT, uTP and
    /// UDP trackers; an HTTP proxy cannot carry UDP, so the DHT then runs
    /// directly unless `--no-dht` is given.
    #[arg(long, value_name = "URL", value_parser = ProxyParser)]
    proxy: Option<ProxyConfig>,
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

/// Parses `socks5://[user:pass@]host:port` or `http://[user:pass@]host:port`
/// (an IPv6 host in brackets; user and password percent-decoded) into a
/// [`ProxyConfig`] that proxies peers, trackers and host names.
/// Error messages never contain the URL's credentials ([`redact_proxy_url`]).
fn parse_proxy(s: &str) -> Result<ProxyConfig, String> {
    const USAGE: &str = "expected socks5://[user:pass@]host:port or http://[user:pass@]host:port";
    let shown = redact_proxy_url(s);
    let bad = |why: &str| format!("invalid proxy URL {shown:?}: {why} ({USAGE})");
    let (scheme, rest) = s.split_once("://").ok_or_else(|| bad("no scheme"))?;
    let kind = if scheme.eq_ignore_ascii_case("socks5") {
        ProxyKind::Socks5
    } else if scheme.eq_ignore_ascii_case("http") {
        ProxyKind::Http
    } else {
        return Err(bad("unsupported scheme"));
    };
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest.contains(['/', '?', '#']) {
        return Err(bad("a path, query or fragment is not allowed"));
    }
    let (credentials, host_port) = match rest.rsplit_once('@') {
        Some((c, hp)) => (Some(c), hp),
        None => (None, rest),
    };
    let (host, port) = match host_port.strip_prefix('[') {
        // `[v6]:port`: split at the bracket, not at a colon inside it.
        Some(bracketed) => {
            let (host, after) = bracketed
                .split_once(']')
                .ok_or_else(|| bad("unclosed IPv6 bracket"))?;
            let port = after.strip_prefix(':').ok_or_else(|| bad("missing port"))?;
            (host, port)
        }
        None => {
            let (host, port) = host_port
                .rsplit_once(':')
                .ok_or_else(|| bad("missing port"))?;
            if host.contains(':') {
                return Err(bad("put an IPv6 host in brackets"));
            }
            (host, port)
        }
    };
    if host.is_empty() {
        return Err(bad("missing host"));
    }
    let port: u16 = port
        .parse()
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| bad("missing or invalid port"))?;
    let mut config = ProxyConfig::default();
    config.kind = kind;
    config.host = host.to_string();
    config.port = port;
    if let Some(c) = credentials {
        let (user, pass) = match c.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (c, None),
        };
        let user = percent_decode(user).ok_or_else(|| bad("bad percent-encoding"))?;
        if user.is_empty() {
            return Err(bad("empty user name"));
        }
        config.username = Some(user);
        config.password = pass
            .map(|p| percent_decode(p).ok_or_else(|| bad("bad percent-encoding")))
            .transpose()?;
    }
    Ok(config)
}

/// `s` with everything before the authority's last `@` (the user and
/// password) replaced, for error messages.
fn redact_proxy_url(s: &str) -> String {
    let (scheme, rest) = match s.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, s),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let authority = match authority.rsplit_once('@') {
        Some((_, host_port)) => format!("<redacted>@{host_port}"),
        None => authority.to_string(),
    };
    match scheme {
        Some(scheme) => format!("{scheme}://{authority}{tail}"),
        None => format!("{authority}{tail}"),
    }
}

/// clap parser for `--proxy`: [`parse_proxy`], with an error that names
/// the flag but, unlike a plain `value_parser` function, never repeats the
/// raw value (it may hold a password).
#[derive(Clone)]
struct ProxyParser;

impl clap::builder::TypedValueParser for ProxyParser {
    type Value = ProxyConfig;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<ProxyConfig, clap::Error> {
        let flag = arg.map_or_else(|| "--proxy <URL>".to_string(), ToString::to_string);
        let fail = |why: String| {
            clap::Error::raw(
                clap::error::ErrorKind::ValueValidation,
                format!("invalid value for '{flag}': {why}\n"),
            )
            .with_cmd(cmd)
        };
        let s = value
            .to_str()
            .ok_or_else(|| fail("the proxy URL is not valid UTF-8".to_string()))?;
        parse_proxy(s).map_err(fail)
    }
}

/// Decodes `%XX` escapes; `None` for a broken escape or non-UTF-8 result.
fn percent_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hi = char::from(bytes.next()?).to_digit(16)?;
            let lo = char::from(bytes.next()?).to_digit(16)?;
            out.push(u8::try_from(hi * 16 + lo).ok()?);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
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
        if let Some(proxy) = &self.proxy {
            settings.proxy = proxy.clone();
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
        /// Resume data file: loaded when present (its saved pieces are
        /// trusted when the files are unchanged), and written on exit,
        /// after pausing the torrent, through a temporary file and a rename.
        #[arg(long, value_name = "PATH")]
        resume: Option<PathBuf>,
        /// Once this much of the torrent is done: pause, save the resume
        /// data (with `--resume`), shut down and exit 0.
        #[arg(long, value_name = "PERCENT", value_parser = clap::value_parser!(u8).range(0..=100))]
        stop_at: Option<u8>,
        /// Web seed (BEP 19) base URL; may be repeated.
        #[arg(long, value_name = "URL")]
        web_seed: Vec<String>,
        #[command(flatten)]
        net: NetArgs,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Read one file of a torrent while it downloads, fetching only what is
    /// read plus the read-ahead window.
    Stream {
        /// The .torrent file.
        torrent: PathBuf,
        /// Index of the file to stream (see `info`; pad files count).
        #[arg(long, value_name = "INDEX")]
        file: usize,
        /// Directory the torrent's data goes into.
        #[arg(long)]
        out: PathBuf,
        /// TCP port to listen on (default 6881; 0 picks a free one).
        #[arg(long)]
        port: Option<u16>,
        /// Write the bytes here instead of to stdout.
        #[arg(long, value_name = "PATH")]
        to: Option<PathBuf>,
        /// Start reading at this offset in the file.
        #[arg(long, value_name = "BYTES", default_value_t = 0)]
        seek: u64,
        /// Read this many bytes (default: to the end of the file).
        #[arg(long, value_name = "BYTES")]
        length: Option<u64>,
        /// The stream's bytes per second (suffixes K and M); sizes the
        /// read-ahead window as a time budget.
        #[arg(long, value_name = "BYTES/S", value_parser = parse_rate)]
        bitrate: Option<u64>,
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
        #[command(flatten)]
        api: ApiArgs,
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
        /// Stream the `--size` payload while it downloads (TCP), then seek.
        #[arg(long)]
        stream: bool,
    },
    /// Print what a .torrent file contains.
    Info {
        /// The .torrent file.
        torrent: PathBuf,
    },
    /// Create a .torrent from a file or a directory.
    Create {
        /// The file or directory to share; its final name is the torrent's
        /// name.
        path: PathBuf,
        /// Where to write the .torrent.
        #[arg(long, value_name = "FILE")]
        out: PathBuf,
        /// Which metainfo versions to write.
        #[arg(long, value_enum, default_value = "hybrid")]
        format: Format,
        /// Piece length in bytes: a power of two of at least 16 KiB
        /// (default: picked from the total size).
        #[arg(long, value_name = "BYTES")]
        piece_size: Option<u32>,
        /// Tracker announce URL, one tier each; may be repeated.
        #[arg(long, value_name = "URL")]
        tracker: Vec<String>,
        /// Web seed (BEP 19) base URL; may be repeated.
        #[arg(long, value_name = "URL")]
        web_seed: Vec<String>,
        /// Set the private flag (BEP 27).
        #[arg(long)]
        private: bool,
        /// A free-form comment.
        #[arg(long, value_name = "TEXT")]
        comment: Option<String>,
    },
}

/// `create --format`.
#[derive(Clone, Copy, ValueEnum)]
enum Format {
    V1,
    V2,
    Hybrid,
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
        Command::Create {
            path,
            out,
            format,
            piece_size,
            tracker,
            web_seed,
            private,
            comment,
        } => {
            let version = match format {
                Format::V1 => TorrentVersion::V1,
                Format::V2 => TorrentVersion::V2,
                Format::Hybrid => TorrentVersion::Hybrid,
            };
            let mut create = CreateTorrent::new(path)
                .version(version)
                .trackers(tracker.into_iter().map(|t| vec![t]).collect())
                .web_seeds(web_seed)
                .private(private);
            if let Some(pl) = piece_size {
                create = create.piece_len(pl);
            }
            if let Some(c) = comment {
                create = create.comment(c);
            }
            // Hashing reads every byte, so it runs off the async workers.
            let bytes = tokio::task::spawn_blocking(move || {
                create.build(&mut |_, _| std::ops::ControlFlow::Continue(()))
            })
            .await??;
            let m = Metainfo::from_bytes(&bytes)?;
            std::fs::write(&out, &bytes)?;
            println!("created: {}", out.display());
            print_hashes(&m);
            Ok(ExitCode::SUCCESS)
        }
        Command::Download {
            torrent,
            out,
            port,
            seed_for,
            peer,
            resume,
            stop_at,
            web_seed,
            net,
            api,
        } => {
            let source = if torrent
                .get(..7)
                .is_some_and(|h| h.eq_ignore_ascii_case("magnet:"))
            {
                Source::Magnet(Magnet::parse(&torrent)?)
            } else {
                Source::File(load(std::path::Path::new(&torrent))?)
            };
            let opts = TransferOpts {
                port,
                seed_for,
                peers: peer,
                download: true,
                resume,
                stop_at,
                web_seeds: web_seed,
                api: api.config(),
            };
            transfer(source, out, opts, &net).await
        }
        Command::Seed {
            torrent,
            data,
            port,
            seed_for,
            net,
            api,
        } => {
            let source = Source::File(load(&torrent)?);
            let opts = TransferOpts {
                port,
                seed_for,
                download: false,
                api: api.config(),
                ..TransferOpts::default()
            };
            transfer(source, data, opts, &net).await
        }
        Command::Stream {
            torrent,
            file,
            out,
            port,
            to,
            seek,
            length,
            bitrate,
            peer,
            net,
        } => {
            let args = StreamArgs {
                file,
                port,
                to,
                seek,
                length,
                bitrate,
            };
            stream(load(&torrent)?, out, args, &peer, &net).await
        }
        Command::Bench {
            size,
            transport,
            torrents,
            idle_secs,
            stream,
        } => bench(size, transport, torrents, idle_secs, stream).await,
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
    print_hashes(&m);
    println!(
        "pieces: {} x {}",
        m.piece_layout().num_pieces(),
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

/// `info hash:` (the 20-byte wire hash: v1, else the truncated v2, which is
/// then qBittorrent's torrent id; qBittorrent names a hybrid by its
/// truncated v2 hash), `info hash v2:` when there is one, and `version:`.
fn print_hashes(m: &Metainfo) {
    println!("info hash: {}", InfoHashV1(m.info_hashes.wire_hash()));
    if let Some(v2) = m.info_hashes.v2 {
        println!("info hash v2: {v2}");
    }
    let version = match m.version() {
        TorrentVersion::V1 => "v1",
        TorrentVersion::V2 => "v2",
        _ => "hybrid",
    };
    println!("version: {version}");
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
        TorrentState::QueuedForChecking => "check-queued",
        TorrentState::Queued => "queued",
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

impl Source {
    fn info_hashes(&self) -> InfoHashes {
        match self {
            Source::File(m) => m.info_hashes,
            Source::Magnet(m) => m.info_hashes,
        }
    }
}

/// How `transfer` runs: `download` and `seed` differ in these.
#[derive(Default)]
struct TransferOpts {
    port: Option<u16>,
    seed_for: Option<u64>,
    peers: Vec<SocketAddr>,
    /// A download (exits once done) rather than a seed.
    download: bool,
    /// Resume data file, loaded when present and written on exit.
    resume: Option<PathBuf>,
    /// Stop at this percentage done.
    stop_at: Option<u8>,
    web_seeds: Vec<String>,
    /// Serve the HTTP/JSON API next to the session.
    api: Option<ApiConfig>,
}

/// The add parameters: from the resume file when it exists (it must be for
/// the same torrent), else fresh.
fn add_params(source: Source, dir: PathBuf, resume: Option<&Path>) -> Result<AddTorrent, Error> {
    if let Some(path) = resume
        && path.exists()
    {
        let bytes = std::fs::read(path)?;
        let mut params = AddTorrent::from_resume(&bytes)
            .map_err(|e| format!("resume file {}: {e}", path.display()))?;
        let saved = match &params.source {
            TorrentSource::Metainfo(m) => Some(m.info_hashes),
            TorrentSource::Magnet(m) => Some(m.info_hashes),
            _ => None,
        };
        if saved != Some(source.info_hashes()) {
            return Err(format!("resume file {} is for another torrent", path.display()).into());
        }
        params.save_path = dir;
        // Saved after a pause (so its stamps hold); this run downloads.
        params.paused = false;
        return Ok(params);
    }
    Ok(match source {
        Source::File(m) => AddTorrent::new(m, dir),
        Source::Magnet(m) => AddTorrent::magnet(m, dir),
    })
}

/// Writes `bytes` to `path` through a temporary file next to it and a
/// rename, so a crash never leaves a half-written resume file.
fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    let tmp = path.with_file_name(name);
    let result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Runs one torrent until it finishes (plus `seed_for`), reaches
/// `stop_at`, errors, or Ctrl-C.
async fn transfer(
    source: Source,
    dir: PathBuf,
    opts: TransferOpts,
    net: &NetArgs,
) -> Result<ExitCode, Error> {
    let mut settings = Settings::default();
    net.apply(&mut settings);
    if let Some(port) = opts.port
        && net.listen.is_empty()
    {
        settings.listen_addrs = vec![SocketAddr::from(([0, 0, 0, 0], port))];
    }
    let params = add_params(source, dir, opts.resume.as_deref())?;
    let session = Session::new(settings).await?;
    println!("listening on {}", session.local_addr());
    let api = match opts.api.clone() {
        Some(config) => match ApiServer::start(session.clone(), config).await {
            Ok(api) => {
                println!("api: listening on {}", api.local_addr());
                Some(api)
            }
            Err(e) => {
                session.shutdown().await;
                return Err(format!("api: {e}").into());
            }
        },
        None => None,
    };
    // Whatever happens below, the API stops and the session shuts down
    // (trackers get their `stopped`, the disk cache is flushed).
    let result = run_transfer(&session, params, &opts).await;
    if let Some(api) = api {
        api.shutdown().await;
    }
    session.shutdown().await;
    result
}

/// The part of [`transfer`] that runs with the session up.
async fn run_transfer(
    session: &Session,
    params: AddTorrent,
    opts: &TransferOpts,
) -> Result<ExitCode, Error> {
    let mut events = session
        .subscribe(EventMask::STATUS | EventMask::PEER | EventMask::NAT | EventMask::TRACKER);
    let handle = session.add_torrent(params).await?;
    for url in &opts.web_seeds {
        handle.add_web_seed(url.clone()).await?;
    }
    for peer in &opts.peers {
        handle.add_peer(*peer).await?;
    }
    let limits = WatchLimits {
        seed_for: opts.seed_for.map(Duration::from_secs),
        download: opts.download,
        stop_at: opts.stop_at,
    };
    let mut code = watch(&handle, &mut events, limits).await;
    let mut paused = true;
    if opts.stop_at.is_some() || opts.resume.is_some() {
        // Nothing may be written after the save, or its stamps would not
        // hold on the next start.
        if let Err(e) = handle.pause().await {
            eprintln!("warning: pausing the torrent failed: {e}");
            paused = false;
        }
    }
    if let Some(path) = &opts.resume
        && !save_resume(&handle, path, paused).await
    {
        code = ExitCode::FAILURE;
    }
    Ok(code)
}

/// Saves the torrent's resume data to `path` unless it would be worse than
/// what is there: an errored torrent, a failed pause, or a failed save
/// keeps the existing file (with a warning), and a magnet without its
/// metadata has nothing to save yet. Returns false when the save or the
/// write itself failed.
async fn save_resume(handle: &TorrentHandle, path: &Path, paused: bool) -> bool {
    let status = handle.status();
    let keep = |why: &str| {
        let what = if path.exists() {
            format!("keeping {}", path.display())
        } else {
            format!("not writing {}", path.display())
        };
        eprintln!("warning: resume data not saved ({why}); {what}");
    };
    if !status.has_metadata {
        eprintln!(
            "resume: not saved: the magnet's metadata has not arrived yet, so there is nothing to resume"
        );
        return true;
    }
    if matches!(status.state, TorrentState::Error) {
        keep("the torrent is in error");
        return true;
    }
    if !paused {
        keep("the torrent could not be paused");
        return true;
    }
    match handle.save_resume_data().await {
        Ok(bytes) => match write_atomically(path, &bytes) {
            Ok(()) => {
                println!("resume data saved to {}", path.display());
                true
            }
            Err(e) => {
                eprintln!("error: writing {}: {e}", path.display());
                keep("the write failed");
                false
            }
        },
        Err(e) => {
            eprintln!("error: saving resume data: {e}");
            keep("the save failed");
            false
        }
    }
}

/// What `stream` reads, and where it goes.
struct StreamArgs {
    file: usize,
    port: Option<u16>,
    to: Option<PathBuf>,
    seek: u64,
    length: Option<u64>,
    bitrate: Option<u64>,
}

/// Prints a line to stderr, or to stdout when `to_stderr` is false.
fn say(to_stderr: bool, line: &str) {
    if to_stderr {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

/// The `stream` subcommand: only the streamed file is wanted (every other
/// file is `Skip`), so what is fetched is what is read, the read-ahead
/// window and the boundary pieces.
async fn stream(
    meta: Metainfo,
    dir: PathBuf,
    args: StreamArgs,
    peers: &[SocketAddr],
    net: &NetArgs,
) -> Result<ExitCode, Error> {
    let mut settings = Settings::default();
    net.apply(&mut settings);
    if let Some(port) = args.port
        && net.listen.is_empty()
    {
        settings.listen_addrs = vec![SocketAddr::from(([0, 0, 0, 0], port))];
    }
    let session = Session::new(settings).await?;
    // With stdout as the sink, every other line goes to stderr.
    let to_stderr = args.to.is_none();
    say(to_stderr, &format!("listening on {}", session.local_addr()));
    let mut events = session.subscribe(EventMask::PEER);
    let priorities = (0..meta.info.files.len())
        .map(|i| {
            if i == args.file {
                Priority::Normal
            } else {
                Priority::Skip
            }
        })
        .collect();
    let mut params = AddTorrent::new(meta, dir);
    params.file_priorities = priorities;
    let handle = session.add_torrent(params).await?;
    let mut reader = match handle.open_file(args.file).await {
        Ok(r) => r,
        Err(e @ SessionError::InvalidFileIndex { .. }) => {
            eprintln!("error: {e}");
            session.shutdown().await;
            return Ok(ExitCode::from(2));
        }
        Err(e) => return Err(e.into()),
    };
    if let Some(bitrate) = args.bitrate {
        let mut ahead = ReadAhead::default();
        ahead.bitrate = Some(bitrate);
        reader.set_read_ahead(ahead)?;
    }
    for peer in peers {
        handle.add_peer(*peer).await?;
    }
    let mut sink: Box<dyn Write> = match &args.to {
        Some(path) => Box::new(std::io::BufWriter::new(std::fs::File::create(path)?)),
        None => Box::new(std::io::stdout().lock()),
    };
    let start = Instant::now();
    reader.seek(SeekFrom::Start(args.seek)).await?;
    let mut left = args.length.unwrap_or(u64::MAX);
    let mut total = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    while left > 0 {
        let want = usize::try_from(left).map_or(buf.len(), |l| l.min(buf.len()));
        let Some(slot) = buf.get_mut(..want) else {
            break;
        };
        let n = tokio::select! {
            r = reader.read(slot) => r?,
            Some(ev) = events.recv() => {
                if let Event::PeerConnected { addr, .. } = ev {
                    say(to_stderr, &format!("peer connected {addr}"));
                }
                continue;
            }
        };
        if n == 0 {
            break;
        }
        if let Some(chunk) = buf.get(..n) {
            sink.write_all(chunk)?;
        }
        total += n as u64;
        left = left.saturating_sub(n as u64);
    }
    sink.flush()?;
    drop(sink);
    eprintln!(
        "stream: {total} bytes from {} in {:.2} s",
        args.seek,
        start.elapsed().as_secs_f64()
    );
    drop(reader);
    session.shutdown().await;
    Ok(ExitCode::SUCCESS)
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
/// NAT-PMP, PEX or holepunching, so nothing leaves this machine; no queue
/// limits, so the idle row measures 1,000 running seeds.
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
    s.queue.active_downloads = None;
    s.queue.active_seeds = None;
    s.queue.active_limit = None;
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

/// Streams `size_mib` MiB front to back while the leech downloads it from a
/// seed in this process (loopback, TCP, in-memory leech storage), then reads
/// 64 KiB at ten pseudo-random offsets.
async fn bench_stream(size_mib: u64) -> Result<(), Error> {
    let len = size_mib.saturating_mul(1024 * 1024);
    if len < 1024 * 1024 {
        return Err("bench --stream needs --size of at least 1".into());
    }
    let m = bench_torrent("bench", len, BENCH_PIECE_LEN)?;
    let seed = Session::new(bench_settings(TransportChoice::Tcp)).await?;
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(std::sync::Arc::new(BenchStorage { discard: false }));
    let seed_handle = seed.add_torrent(p).await?;
    wait_seeding(&[seed_handle], Duration::from_secs(300)).await?;
    let leech = Session::new(bench_settings(TransportChoice::Tcp)).await?;
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(std::sync::Arc::new(fbt::MemStorage::new(len)));
    let handle = leech.add_torrent(p).await?;
    let mut reader = handle.open_file(0).await?;
    println!("bench: start");
    let t0 = Instant::now();
    handle.add_peer(seed.local_addr()).await?;
    let mut buf = vec![0u8; 256 * 1024];
    let mut total = 0u64;
    loop {
        let n = tokio::time::timeout(Duration::from_secs(300), reader.read(&mut buf))
            .await
            .map_err(|_| "timed out reading the stream")??;
        if n == 0 {
            break;
        }
        total += n as u64;
    }
    if total != len {
        return Err(format!("read {total} of {len} bytes").into());
    }
    let secs = t0.elapsed().as_secs_f64();
    let mbit = len as f64 * 8.0 / 1e6 / secs;
    println!("bench: stream {size_mib} MiB in {secs:.2} s = {mbit:.0} Mbit/s");
    // Fixed-seed xorshift64: `fbt-example` has no `rand` dependency.
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let span = len - 64 * 1024;
    let mut times = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    for _ in 0..10 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let t = Instant::now();
        reader.seek(SeekFrom::Start(x % span)).await?;
        tokio::time::timeout(Duration::from_secs(60), reader.read_exact(&mut chunk))
            .await
            .map_err(|_| "timed out after a seek")??;
        times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(f64::total_cmp);
    let at = |q: usize| times.get(q).copied().unwrap_or(0.0);
    println!("bench: stream seek p50 {:.2} ms p90 {:.2} ms", at(4), at(8));
    drop(reader);
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
    stream: bool,
) -> Result<ExitCode, Error> {
    println!("bench: pid {}", std::process::id());
    if stream {
        bench_stream(size_mib).await?;
    } else if size_mib > 0 {
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
/// once another process announces it too (answering it once first), 1 after
/// `timeout_secs`.
async fn lsd_probe(port: u16, timeout_secs: u64) -> Result<ExitCode, Error> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let lsd = fbt_net::lsd::Lsd::spawn(fbt_net::lsd::LsdConfig::default(), tx)?;
    let found = probe_lsd(&lsd, &mut rx, port, Duration::from_secs(timeout_secs)).await;
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

/// The probe itself: announces [`LSD_PROBE_HASH`] with `port` on `lsd` and
/// returns the first other prober heard on `rx`, or `None` after `timeout`.
async fn probe_lsd(
    lsd: &fbt_net::lsd::Lsd,
    rx: &mut tokio::sync::mpsc::Receiver<fbt_net::lsd::LsdPeer>,
    port: u16,
    timeout: Duration,
) -> Option<SocketAddr> {
    let deadline = Instant::now() + timeout;
    // Short-lived diagnostic bounded by `timeout`, so it re-announces every
    // 500 ms instead of the library's 5-minute cadence.
    let mut announce = interval(Duration::from_millis(500));
    announce.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = announce.tick() => lsd.announce(port, &[LSD_PROBE_HASH]),
            Some(peer) = rx.recv() => {
                if peer.info_hash == LSD_PROBE_HASH {
                    // The other prober may have joined after our last
                    // announce; it keeps listening until it hears one, so
                    // answer once before leaving.
                    lsd.announce(port, &[LSD_PROBE_HASH]);
                    return Some(peer.addr);
                }
            }
            _ = tokio::time::sleep_until(deadline) => return None,
        }
    }
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
        Event::TrackerReply { url, peers, .. } => println!("tracker reply {url}: {peers} peers"),
        Event::TrackerError { url, message, .. } => println!("tracker error {url}: {message}"),
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

/// When `watch` returns.
struct WatchLimits {
    /// Keep going this long after finishing (download) or at all (seed).
    seed_for: Option<Duration>,
    /// A download: done once seeding (plus `seed_for`).
    download: bool,
    /// Return once this percentage is done (download only).
    stop_at: Option<u8>,
}

/// Prints `resume: trusted (<n> pieces)` or `resume: checking` once the
/// torrent added from resume data knows which; returns whether it printed.
async fn report_resume(handle: &TorrentHandle, status: &TorrentStatus) -> bool {
    match status.resume_trusted {
        Some(true) => {
            let n = handle.pieces().await.map_or(0, |p| p.count_ones());
            eprintln!("resume: trusted ({n} pieces)");
            true
        }
        Some(false) => {
            eprintln!("resume: checking");
            true
        }
        None => false,
    }
}

async fn watch(handle: &TorrentHandle, events: &mut EventStream, limits: WatchLimits) -> ExitCode {
    let WatchLimits {
        seed_for,
        download,
        stop_at,
    } = limits;
    let mut resume_reported = false;
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
        if !resume_reported {
            resume_reported = report_resume(handle, &status).await;
        }
        if matches!(status.state, TorrentState::Error) {
            eprintln!(
                "error: {}",
                status.error.as_deref().unwrap_or("torrent stopped")
            );
            return ExitCode::FAILURE;
        }
        if download {
            if let Some(percent) = stop_at
                && matches!(
                    status.state,
                    TorrentState::Downloading | TorrentState::Seeding
                )
                && status.progress * 100.0 >= f64::from(percent)
            {
                println!("stop at {percent}%: {:.1}% done", status.progress * 100.0);
                return ExitCode::SUCCESS;
            }
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

    use super::{Cli, Command, fill_pattern, human_bytes, parse_proxy, parse_rate, probe_lsd};

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
    fn proxy_url_fields() {
        let p = parse_proxy("socks5://us%3Aer:p%40ss@socks5:1080").expect("socks5");
        assert_eq!(p.kind, fbt::ProxyKind::Socks5);
        assert_eq!((p.host.as_str(), p.port), ("socks5", 1080));
        assert_eq!(p.username.as_deref(), Some("us:er"));
        assert_eq!(p.password.as_deref(), Some("p@ss"));
        assert!(p.proxy_peers && p.proxy_trackers && p.proxy_hostnames);

        let p = parse_proxy("HTTP://[::1]:8080/").expect("http");
        assert_eq!(p.kind, fbt::ProxyKind::Http);
        assert_eq!((p.host.as_str(), p.port), ("::1", 8080));
        assert_eq!((p.username, p.password), (None, None));

        let p = parse_proxy("http://alice@proxy.test:1").expect("user only");
        assert_eq!((p.username.as_deref(), p.password), (Some("alice"), None));

        for bad in [
            "socks5://proxy.test",
            "socks5://proxy.test:0",
            "socks5://:1080",
            "socks5://@proxy.test:1080",
            "socks5://::1:1080",
            "socks5://[::1:1080",
            "socks5://u:%zz@proxy.test:1080",
            "ftp://proxy.test:21",
        ] {
            assert!(parse_proxy(bad).is_err(), "{bad:?} should be rejected");
        }
        for no_port in [
            "http://[::1]",
            "http://[::1]/",
            "socks5://[::1]x",
            "socks5://proxy.test",
        ] {
            let e = parse_proxy(no_port).expect_err(no_port);
            assert!(e.contains("missing port"), "{no_port:?}: {e}");
        }
    }

    /// No rejection repeats the user name or password.
    #[test]
    fn proxy_errors_redact_credentials() {
        for bad in [
            "socks5://secretuser:hunter2@proxy.test",
            "http://secretuser:hunter2@[::1]",
            "ftp://secretuser:hunter2@proxy.test:21",
            "socks5://secretuser:hunter2@proxy.test:1080/path",
            "socks5://secretuser:hu%zzter2@proxy.test:1080",
        ] {
            let e = parse_proxy(bad).expect_err(bad);
            assert!(
                !e.contains("hunter2") && !e.contains("secretuser"),
                "{bad:?}: {e}"
            );
            assert!(e.contains("<redacted>@"), "{bad:?}: {e}");
        }
        let cli = Cli::try_parse_from([
            "fbt-example",
            "seed",
            "t.torrent",
            "--data",
            "d",
            "--proxy",
            "socks5://secretuser:hunter2@proxy.test",
        ]);
        let Err(e) = cli else {
            panic!("a proxy URL without a port parsed");
        };
        let text = e.to_string();
        assert!(text.contains("--proxy"), "{text}");
        assert!(text.contains("missing port"), "{text}");
        assert!(
            !text.contains("hunter2") && !text.contains("secretuser"),
            "{text}"
        );
    }

    #[test]
    fn formats_bytes() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1_258_291), "1.2 MiB");
    }

    /// Two probes that do not start together: the early one hears the late
    /// one's first announce at once, and the late one must still hear the
    /// early one (unicast on loopback stands in for the multicast group).
    #[tokio::test]
    async fn lsd_probe_late_starter_hears_the_early_one() {
        use fbt_net::lsd::{Lsd, LsdConfig};
        use std::net::UdpSocket;
        use std::time::Duration;
        use tokio::sync::mpsc;

        let free = || {
            UdpSocket::bind("127.0.0.1:0")
                .and_then(|s| s.local_addr())
                .expect("free port")
        };
        let (early_addr, late_addr) = (free(), free());

        let (tx, mut rx) = mpsc::channel(16);
        let early = Lsd::spawn_unicast(LsdConfig::default(), early_addr, vec![late_addr], tx)
            .expect("early probe");
        let early_task = tokio::spawn(async move {
            let found = probe_lsd(&early, &mut rx, 7001, Duration::from_secs(5)).await;
            early.shutdown().await;
            found
        });
        // The early probe announces at 0, 500 and 1000 ms to a port nobody
        // has bound yet; the late one starts between two of its announces.
        tokio::time::sleep(Duration::from_millis(1200)).await;

        let (tx, mut rx) = mpsc::channel(16);
        let late = Lsd::spawn_unicast(LsdConfig::default(), late_addr, vec![early_addr], tx)
            .expect("late probe");
        let late_found = probe_lsd(&late, &mut rx, 7002, Duration::from_secs(2)).await;
        late.shutdown().await;

        let early_found = early_task.await.expect("early probe task");
        assert_eq!(early_found.map(|a| a.port()), Some(7002));
        assert_eq!(late_found.map(|a| a.port()), Some(7001));
    }
}
