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
    _: Option<&dyn Storage>,
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
}
