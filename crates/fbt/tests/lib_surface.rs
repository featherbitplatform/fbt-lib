//! Pins the facade's public surface: removing a re-export breaks this build.

#![allow(dead_code, unused_imports, clippy::too_many_arguments)]

use fbt::{
    AddTorrent, Bitfield, BitfieldError, BlockRef, DEFAULT_DHT_BOOTSTRAP, DecodeError, DhtStats,
    DisconnectReason, EncryptionPolicy, Event, EventMask, EventStream, FileEntry, FileLayout,
    FileSlice, FsStorage, Info, InfoHashV1, InfoHashV2, InfoHashes, LayoutError, ListenError,
    MAX_DISPLAY_CHARS, MAX_METADATA_SIZE, Magnet, MagnetError, MemStorage, Metainfo, MetainfoError,
    ParseInfoHashError, PeerId, PeerSource, PieceLayout, Priority, RedirectPolicy, RelPath,
    RemoveMode, Session, SessionError, Settings, Storage, TorrentHandle, TorrentSource,
    TorrentState, TorrentStatus, TrackerConfig, TrackerError, Transport, display_safe,
};
use fbt::{BlockReason, FileStatus, IpFilter, MoveMode, PeerStatus, QueueSettings, TrackerStatus};
use fbt::{
    CacheStats, ChokingAlgorithm, ExternalIpSource, FileReader, MapMethod, MapProtocol, MixedMode,
    PeerClassLimits, ProxyConfig, ProxyKind, RateLimit, ReadAhead, SeedChokingAlgorithm,
    StorageError, StreamError,
};
use fbt::{
    CreateError, CreateTorrent, MAX_CREATE_FILES, MAX_PIECE_LAYER_HASHES, MAX_V2_PIECES,
    TorrentVersion, V2Piece, auto_piece_len, validate_layer,
};

fn names(
    _: Option<AddTorrent>,
    _: Option<Bitfield>,
    _: Option<BlockRef>,
    _: Option<FileSlice>,
    _: Option<LayoutError>,
    _: Option<ListenError>,
    _: Option<BitfieldError>,
    _: Option<DecodeError>,
    _: Option<DisconnectReason>,
    _: Option<DhtStats>,
    _: Option<EncryptionPolicy>,
    _: Option<PeerSource>,
    _: Option<Event>,
    _: Option<EventMask>,
    _: Option<EventStream>,
    _: Option<FileEntry>,
    _: Option<FileLayout>,
    _: Option<FsStorage>,
    _: Option<Info>,
    _: Option<InfoHashV1>,
    _: Option<InfoHashV2>,
    _: Option<InfoHashes>,
    _: Option<MemStorage>,
    _: Option<Magnet>,
    _: Option<MagnetError>,
    _: Option<TorrentSource>,
    _: Option<Metainfo>,
    _: Option<MetainfoError>,
    _: Option<ParseInfoHashError>,
    _: Option<PeerId>,
    _: Option<PieceLayout>,
    _: Option<Priority>,
    _: Option<RelPath>,
    _: Option<RemoveMode>,
    _: Option<Session>,
    _: Option<SessionError>,
    _: Option<Settings>,
    _: Option<TorrentHandle>,
    _: Option<TorrentState>,
    _: Option<CreateTorrent>,
    _: Option<CreateError>,
    _: Option<TorrentVersion>,
    _: Option<V2Piece>,
    _: Option<TorrentStatus>,
    _: Option<FileStatus>,
    _: Option<PeerStatus>,
    _: Option<TrackerStatus>,
    _: Option<TrackerConfig>,
    _: Option<TrackerError>,
    _: Option<RedirectPolicy>,
    _: Option<Transport>,
    _: Option<StorageError>,
    _: Option<RateLimit>,
    _: Option<PeerClassLimits>,
    _: Option<MixedMode>,
    _: Option<ChokingAlgorithm>,
    _: Option<SeedChokingAlgorithm>,
    _: Option<CacheStats>,
    _: Option<MapProtocol>,
    _: Option<MapMethod>,
    _: Option<ExternalIpSource>,
    _: Option<&dyn Storage>,
    _: Option<FileReader>,
    _: Option<ReadAhead>,
    _: Option<StreamError>,
    _: Option<ProxyConfig>,
    _: Option<ProxyKind>,
    _: Option<IpFilter>,
    _: Option<BlockReason>,
    _: Option<MoveMode>,
    _: Option<QueueSettings>,
) {
}

/// The fields `PeerConnected` carries.
fn peer_connected(e: &Event) -> Option<(std::net::SocketAddr, Transport, bool)> {
    match e {
        Event::PeerConnected {
            addr,
            transport,
            encrypted,
            ..
        } => Some((*addr, *transport, *encrypted)),
        _ => None,
    }
}

#[test]
fn surface_compiles() {
    let _: [&str; 3] = DEFAULT_DHT_BOOTSTRAP;
    let _: usize = MAX_METADATA_SIZE;
    let _: usize = MAX_DISPLAY_CHARS;
    let _: String = display_safe("a\u{202E}  b", 8);
    let _: [usize; 3] = [MAX_CREATE_FILES, MAX_PIECE_LAYER_HASHES, MAX_V2_PIECES];
    let _: u32 = auto_piece_len(1);
    let _: bool = validate_layer(&[0; 32], 1, 16384, &[]);
    let _ = CreateTorrent::new("x").version(TorrentVersion::V1);
    let _ = [Transport::Tcp, Transport::Utp];
    let s = Settings::default();
    let _: [bool; 4] = [
        s.enable_outgoing_tcp,
        s.enable_incoming_tcp,
        s.enable_outgoing_utp,
        s.enable_incoming_utp,
    ];
    let p: ProxyConfig = s.proxy;
    let _ = [ProxyKind::None, ProxyKind::Socks5, ProxyKind::Http];
    let _: (ProxyKind, String, u16, Option<String>, Option<String>) =
        (p.kind, p.host, p.port, p.username, p.password);
    let _: (bool, bool, bool) = (p.proxy_peers, p.proxy_trackers, p.proxy_hostnames);
    let _: Result<(), String> = ProxyConfig::default().validate();
    // Where a redirect may take a fetch: the default follows as it always did; a web seed
    // on a public address sets `allow_internal` false.
    let p = fbt::RedirectPolicy::default();
    let _: bool = p.allow_internal;
    assert!(p.allow_internal, "announces keep their redirects");
    let e = fbt::ProxyError::Refused(1);
    let _ = [
        SessionError::Proxy(e),
        SessionError::Proxy(fbt::ProxyError::Timeout),
    ];
    let _: [usize; 6] = [
        fbt::MAX_RESUME_BYTES,
        fbt::MAX_RESUME_ITEMS,
        fbt::MAX_RESUME_PEERS,
        fbt::MAX_SESSION_TORRENTS,
        fbt::MAX_IP_FILTER_RANGES,
        fbt::MAX_WEB_SEEDS,
    ];
    let _: usize = Settings::default().max_peers_per_ip;
    let _ = [
        BlockReason::IpFilter,
        BlockReason::Banned,
        BlockReason::PerIpLimit,
    ];
    let _ = DisconnectReason::Blocked(BlockReason::Banned);
    let mut f = IpFilter::default();
    let ip: std::net::IpAddr = std::net::Ipv4Addr::LOCALHOST.into();
    f.block(ip..=ip);
    f.allow(ip..=ip);
    let _: (bool, usize, bool) = (f.is_blocked(ip), f.len(), f.is_empty());
    let _: Vec<(std::ops::RangeInclusive<std::net::IpAddr>, bool)> = f.rules();
}

/// The fields `PeerBlocked` carries.
fn peer_blocked(e: &Event) -> Option<(Option<InfoHashes>, std::net::SocketAddr, BlockReason)> {
    match e {
        Event::PeerBlocked {
            torrent,
            addr,
            reason,
        } => Some((*torrent, *addr, *reason)),
        _ => None,
    }
}

/// Moving the storage and renaming files.
async fn storage_methods(h: &TorrentHandle) {
    let _ = [
        MoveMode::FailIfExists,
        MoveMode::ReplaceExisting,
        MoveMode::KeepExisting,
    ];
    assert_eq!(MoveMode::default(), MoveMode::FailIfExists);
    let _: Result<(), SessionError> = h
        .move_storage(std::path::PathBuf::new(), MoveMode::default())
        .await;
    let _: Result<(), SessionError> = h.rename_file(0, "x").await;
    // Hidden from the docs: a test hook, the piece layers the torrent holds.
    let _: usize = h.piece_layers_known().await;
    // Hidden from the docs: a test hook, how often the status tick ran.
    let _: u64 = h.status_ticks().await;
    let _: std::path::PathBuf = h.status().save_path;
    let _ = [
        SessionError::InvalidPath(String::new()),
        SessionError::Unsupported,
        SessionError::Busy,
    ];
}

/// The storage operations a custom storage may implement, and the file
/// storage's paths.
fn storage_trait(st: &dyn Storage, m: &Metainfo, r: RelPath) {
    let _: std::io::Result<()> = st.set_file_priorities(&[true]);
    let _: std::io::Result<()> = st.move_to(std::path::Path::new("x"), MoveMode::default());
    let _: std::io::Result<()> = st.rename_file(0, &r);
    let fs = FsStorage::with_paths(std::path::PathBuf::new(), m, &[(0, r)]);
    let _: std::path::PathBuf = fs.part_path();
}

/// Tracker scrapes.
async fn scrape_methods(h: &TorrentHandle, e: &Event) {
    let _: Result<(), SessionError> = h.scrape_trackers().await;
    match e {
        Event::ScrapeReply {
            torrent,
            url,
            complete,
            incomplete,
            downloaded,
        } => {
            let _: (&InfoHashes, &String, [&u32; 3]) =
                (torrent, url, [complete, incomplete, downloaded]);
        }
        Event::ScrapeFailed {
            torrent,
            url,
            message,
        } => {
            let _: (&InfoHashes, &String, &String) = (torrent, url, message);
        }
        Event::ProxyError { message } => {
            let _: &String = message;
        }
        _ => {}
    }
}

/// The fields the storage events carry.
fn storage_events(e: &Event) {
    match e {
        Event::StorageMoved { torrent, path } => {
            let _: (&InfoHashes, &std::path::PathBuf) = (torrent, path);
        }
        Event::StorageMoveFailed { torrent, message } => {
            let _: (&InfoHashes, &String) = (torrent, message);
        }
        Event::FileRenamed {
            torrent,
            index,
            path,
        } => {
            let _: (&InfoHashes, &usize, &String) = (torrent, index, path);
        }
        Event::FileRenameFailed {
            torrent,
            index,
            message,
        } => {
            let _: (&InfoHashes, &usize, &String) = (torrent, index, message);
        }
        _ => {}
    }
}

/// The session's filter and ban list.
async fn filter_methods(s: &Session, f: IpFilter) {
    s.set_ip_filter(f).await;
    let _: IpFilter = s.ip_filter();
    let _: Vec<std::net::SocketAddr> = s.banned_peers();
}

/// Queueing and seed limits.
async fn queue_methods(s: &Session, h: &TorrentHandle, e: &Event) {
    let mut q = QueueSettings::default();
    let _: (Option<usize>, Option<usize>, Option<usize>, bool) = (
        q.active_downloads,
        q.active_seeds,
        q.active_limit,
        q.dont_count_slow_torrents,
    );
    let _: (u64, u64, std::time::Duration, std::time::Duration) = (
        q.inactive_down_rate,
        q.inactive_up_rate,
        q.startup_grace,
        q.interval,
    );
    q.active_downloads = None;
    let _: Result<(), SessionError> = s.set_queue_settings(q);
    let mut settings = Settings::default();
    settings.queue = QueueSettings::default();
    settings.seed_ratio_limit = Some(2.0);
    settings.seed_time_limit = Some(std::time::Duration::from_secs(3600));
    let _: Result<(), SessionError> = h.set_auto_managed(true).await;
    let _: Result<(), SessionError> = h.set_queue_position(usize::MAX).await;
    let st = h.status();
    let _: (usize, bool, std::time::Duration, std::time::Duration) = (
        st.queue_position,
        st.auto_managed,
        st.active_time,
        st.seeding_time,
    );
    let _ = TorrentState::Queued;
    if let Event::SeedLimitReached { torrent } = e {
        let _: &InfoHashes = torrent;
    }
    let mut p = AddTorrent::new(
        Metainfo::from_bytes(b"").unwrap(),
        std::path::PathBuf::new(),
    );
    p.auto_managed = false;
}

/// Web seeds (BEP 19).
async fn web_seed_methods(h: &TorrentHandle, e: &Event, m: &Metainfo, mg: &Magnet) {
    let _: Result<(), SessionError> = h.add_web_seed(String::new()).await;
    let _: Result<(), SessionError> = h.remove_web_seed("").await;
    let _: Result<Vec<String>, SessionError> = h.web_seeds().await;
    let _: u32 = h.status().num_web_seeds;
    let _: (&Vec<String>, &Vec<String>) = (&m.web_seeds, &mg.web_seeds);
    let _ = [
        SessionError::InvalidUrl(String::new()),
        SessionError::TooManyWebSeeds,
    ];
    if let Event::WebSeedError {
        torrent,
        url,
        message,
        retired,
    } = e
    {
        let _: (&InfoHashes, &String, &String, &bool) = (torrent, url, message, retired);
    }
}

/// Resume data and session state.
async fn resume_methods(s: &Session, h: &TorrentHandle, st: &dyn Storage) {
    use fbt::{FileStamp, ResumeError, SessionState};
    let _: Result<Vec<u8>, SessionError> = h.save_resume_data().await;
    let _: Result<AddTorrent, ResumeError> = AddTorrent::from_resume(b"");
    let _: (bool, Option<bool>) = (h.status().need_save_resume, h.status().resume_trusted);
    let _: bool = h.status().is_seed;
    let _: Result<(SessionState, Vec<(InfoHashes, SessionError)>), SessionError> =
        s.save_state().await;
    let (state, _): (SessionState, Vec<(InfoHashes, SessionError)>) = s.shutdown_and_save().await;
    let _: (&Option<Vec<u8>>, &Vec<Vec<u8>>) = (&state.dht, &state.torrents);
    let _: Vec<u8> = state.to_bytes();
    let _: Result<SessionState, ResumeError> = SessionState::from_bytes(b"");
    let _: Result<(Session, Vec<(usize, SessionError)>), SessionError> =
        Session::restore(Settings::default(), &state).await;
    let _: std::io::Result<Option<Vec<Option<FileStamp>>>> = st.stamps();
    let stamp = FileStamp::new(1, 2);
    let _: (u64, i64) = (stamp.len, stamp.mtime);
    let _ = [
        ResumeError::TooLarge,
        ResumeError::Version(2),
        ResumeError::Missing("info-hash"),
        ResumeError::Invalid("pieces"),
        ResumeError::InfoHashMismatch,
    ];
    if let SessionError::Resume(e) = SessionError::Resume(ResumeError::TooLarge) {
        let _: ResumeError = e;
    }
}

/// DHT scrape (BEP 33) and `sample_infohashes` (BEP 51).
async fn dht_methods(s: &Session, addr: std::net::SocketAddr) {
    use fbt::{DhtSamples, DhtScrape};
    let r: Result<DhtScrape, SessionError> = s.dht_scrape([0; 20]).await;
    if let Ok(d) = r {
        let _: (u32, u32) = (d.seeds, d.peers);
    }
    let r: Result<DhtSamples, SessionError> = s.dht_sample_infohashes(addr, [0; 20]).await;
    if let Ok(d) = r {
        let _: (std::time::Duration, u32, &Vec<[u8; 20]>) = (d.interval, d.num, &d.samples);
        let _: Vec<(std::net::SocketAddr, [u8; 20])> =
            d.nodes.iter().map(|n| (n.addr, n.id.0)).collect();
    }
    let _ = [
        SessionError::DhtDisabled,
        SessionError::Dht(fbt::DhtError::Remote { code: 204 }),
        SessionError::Dht(fbt::DhtError::Timeout),
    ];
    if let Err(SessionError::Dht(fbt::DhtError::Remote { code })) =
        s.dht_sample_infohashes(addr, [0; 20]).await
    {
        let _: i64 = code;
    }
    let info = fbt::NodeInfo::new(fbt::NodeId([0; 20]), addr);
    let _: (fbt::NodeId, std::net::SocketAddr) = (info.id, info.addr);
    let _: [u8; 20] = info.id.0;
    // BEP 44 items
    let r: Result<Option<Vec<u8>>, SessionError> = s.dht_get_immutable([0; 20]).await;
    let _ = r;
    let r: Result<([u8; 20], usize), SessionError> = s.dht_put_immutable(b"i1e".to_vec()).await;
    let _ = r;
    let r: Result<Option<fbt::MutableItem>, SessionError> = s.dht_get_mutable([0; 32], b"").await;
    if let Ok(Some(item)) = r {
        let _: ([u8; 32], &Vec<u8>, i64, &Vec<u8>) =
            (item.public_key, &item.salt, item.seq, &item.value);
    }
    let r: Result<usize, SessionError> = s
        .dht_put_mutable([0; 32], Vec::new(), b"i1e".to_vec(), 1, Some(0))
        .await;
    let _ = r;
}

/// Detail calls and session getters.
async fn detail_methods(s: &Session, h: &TorrentHandle) {
    if let Ok(files) = h.files().await {
        for f in files {
            let _: (usize, std::path::PathBuf, u64, bool, Priority, u64) =
                (f.index, f.path, f.len, f.pad, f.priority, f.downloaded);
        }
    }
    let r: Result<Vec<PeerStatus>, SessionError> = h.peers().await;
    if let Ok(peers) = r {
        for p in peers {
            let _: (std::net::SocketAddr, Option<String>, Transport, bool, bool) =
                (p.addr, p.client, p.transport, p.encrypted, p.incoming);
            let _: (u64, u64, u64, u64) = (
                p.download_rate,
                p.upload_rate,
                p.total_downloaded,
                p.total_uploaded,
            );
            let _: (bool, bool, bool, bool) = (
                p.peer_choking,
                p.peer_interested,
                p.am_choking,
                p.am_interested,
            );
            let _: (f64, PeerSource) = (p.progress, p.source);
        }
    }
    let _ = [
        PeerSource::Incoming,
        PeerSource::Manual,
        PeerSource::Resume,
        PeerSource::Holepunch,
    ];
    let r: Result<Vec<TrackerStatus>, SessionError> = h.trackers().await;
    if let Ok(trackers) = r {
        for t in trackers {
            let _: (String, usize, [u8; 20], Option<String>) =
                (t.url, t.tier, t.info_hash, t.last_error);
            let _: (Option<std::time::SystemTime>, Option<std::time::Duration>) =
                (t.last_reply, t.next_announce);
            let _: (Option<u32>, Option<u32>, Option<u32>) =
                (t.seeders, t.leechers, t.peers_returned);
        }
    }
    let _: Vec<FileStatus> = Vec::new();
    let _: RateLimit = s.rate_limit();
    let _: PeerClassLimits = s.peer_class_limits();
    let _: QueueSettings = s.queue_settings();
    let _: bool = s.is_shut_down();
}
