//! Pins the facade's public surface: removing a re-export breaks this build.

#![allow(dead_code, unused_imports, clippy::too_many_arguments)]

use fbt::{
    AddTorrent, Bitfield, BitfieldError, BlockRef, DEFAULT_DHT_BOOTSTRAP, DecodeError, DhtStats,
    DisconnectReason, EncryptionPolicy, Event, EventMask, EventStream, FileEntry, FileLayout,
    FileSlice, FsStorage, Info, InfoHashV1, InfoHashV2, InfoHashes, LayoutError, ListenError,
    MAX_METADATA_SIZE, Magnet, MagnetError, MemStorage, Metainfo, MetainfoError,
    ParseInfoHashError, PeerId, PeerSource, PieceLayout, Priority, RelPath, RemoveMode, Session,
    SessionError, Settings, Storage, TorrentHandle, TorrentSource, TorrentState, TorrentStatus,
    TrackerConfig, TrackerError, Transport,
};
use fbt::{BlockReason, IpFilter};
use fbt::{
    CacheStats, ChokingAlgorithm, ExternalIpSource, FileReader, MapMethod, MapProtocol, MixedMode,
    PeerClassLimits, ProxyConfig, ProxyKind, RateLimit, ReadAhead, SeedChokingAlgorithm,
    StorageError, StreamError,
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
    _: Option<TorrentStatus>,
    _: Option<TrackerConfig>,
    _: Option<TrackerError>,
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

/// The session's filter and ban list.
async fn filter_methods(s: &Session, f: IpFilter) {
    s.set_ip_filter(f).await;
    let _: IpFilter = s.ip_filter();
    let _: Vec<std::net::SocketAddr> = s.banned_peers();
}
