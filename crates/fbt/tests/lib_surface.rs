//! Pins the facade's public surface: removing a re-export breaks this build.

#![allow(dead_code, unused_imports, clippy::too_many_arguments)]

use fbt::{
    AddTorrent, Bitfield, BitfieldError, BlockRef, DecodeError, DisconnectReason, Event, EventMask,
    EventStream, FileEntry, FileLayout, FileSlice, FsStorage, Info, InfoHashV1, InfoHashV2,
    InfoHashes, LayoutError, MemStorage, Metainfo, MetainfoError, ParseInfoHashError, PeerId,
    PieceLayout, Priority, RelPath, RemoveMode, Session, SessionError, Settings, Storage,
    TorrentHandle, TorrentState, TorrentStatus, TrackerConfig, TrackerError,
};

fn names(
    _: Option<AddTorrent>,
    _: Option<Bitfield>,
    _: Option<BlockRef>,
    _: Option<FileSlice>,
    _: Option<LayoutError>,
    _: Option<BitfieldError>,
    _: Option<DecodeError>,
    _: Option<DisconnectReason>,
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
    _: Option<&dyn Storage>,
) {
}

#[test]
fn surface_compiles() {}
