//! fbt: pure-Rust BitTorrent library, public facade.
//!
//! This is the only crate clients depend on; its API follows SemVer.
//!
//! # Example
//!
//! Download a torrent and stop when it finishes (or turns out to be
//! complete already, or fails):
//!
//! ```no_run
//! use fbt::{AddTorrent, Event, EventMask, Metainfo, Session, Settings, TorrentState};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let bytes = std::fs::read("example.torrent")?;
//! let metainfo = Metainfo::from_bytes(&bytes)?;
//!
//! let session = Session::new(Settings::default()).await?;
//! let mut events = session.subscribe(EventMask::STATUS);
//! let torrent = session
//!     .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
//!     .await?;
//!
//! while let Some(event) = events.recv().await {
//!     match event {
//!         Event::TorrentFinished { .. }
//!         | Event::StateChanged {
//!             state: TorrentState::Seeding | TorrentState::Error,
//!             ..
//!         } => break,
//!         _ => {}
//!     }
//! }
//! println!("{:.1}% done", torrent.status().progress * 100.0);
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Magnet links
//!
//! Add a torrent by its info hash and wait for the metadata (BEP 9) to
//! arrive from peers; the torrent then runs like one added from a `.torrent`:
//!
//! ```no_run
//! use fbt::{AddTorrent, Event, EventMask, Magnet, Session, Settings};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let magnet = Magnet::parse(
//!     "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a&dn=example",
//! )?;
//! let session = Session::new(Settings::default()).await?;
//! let mut events = session.subscribe(EventMask::STATUS);
//! let torrent = session
//!     .add_torrent(AddTorrent::magnet(magnet, "downloads".into()))
//!     .await?;
//!
//! while let Some(event) = events.recv().await {
//!     if let Event::MetadataReceived { .. } = event {
//!         break;
//!     }
//! }
//! println!("got metadata for {}", torrent.status().name);
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! Peer-supplied metadata is hostile: it is size-limited
//! ([`Settings::max_metadata_size`], at most [`MAX_METADATA_SIZE`]) and
//! checked against the info hash. A magnet's privacy (BEP 27) is unknown
//! until its metadata arrives, so until then it behaves as a public torrent:
//! it advertises the DHT bit, may send `Port`, advertises `ut_pex` and runs
//! the DHT announcer, as libtorrent does. At [`Event::MetadataReceived`] a
//! private torrent stops its DHT announcer and PEX at once and ignores later
//! `Port` and `ut_pex` messages.
//!
//! # Encryption
//!
//! [`Settings::encryption`] takes an [`EncryptionPolicy`]: `Preferred` (the
//! default) tries MSE/PE on outgoing connections and falls back to a plain
//! handshake once per address, `Forced` accepts and makes encrypted
//! connections only, and `Disabled` speaks plain BitTorrent only.
//!
//! # DHT and peer exchange
//!
//! With [`Settings::dht_enabled`] (the default) the session runs a Mainline
//! DHT node (BEP 5). By default it bootstraps from the public routers in
//! [`DEFAULT_DHT_BOOTSTRAP`]; replace them (or use none, for a private
//! network or a test) through [`Settings::dht_bootstrap_nodes`]. Peers found
//! this way are reported as [`Event::PeersFound`] with [`PeerSource::Dht`],
//! and [`DhtStats`] describes the node ([`Session::dht_stats`]).
//!
//! To rejoin quickly, save [`Session::dht_state`] (typically just before
//! [`Session::shutdown`]) and pass the bytes back as [`Settings::dht_state`]
//! next time. The library never writes files: storing the state is the
//! caller's job. Peer exchange (BEP 11) is on with [`Settings::pex_enabled`].
//!
//! A seeding torrent announces itself as a seed (BEP 33).
//! [`Session::dht_scrape`] estimates a torrent's seeds and peers from the
//! DHT's bloom filters ([`DhtScrape`], BEP 33), and
//! [`Session::dht_sample_infohashes`] asks one node for samples of the info
//! hashes it stores ([`DhtSamples`], BEP 51).
//!
//! Private torrents use neither the DHT nor PEX.
//!
//! # Transports
//!
//! Peers are reached over uTP (BEP 29) first, with TCP as the fallback when
//! the uTP connect fails, and either may accept an incoming connection.
//! [`Transport`] tells which one a connection runs on
//! ([`Event::PeerConnected`]). Four switches turn them off one by one:
//! [`Settings::enable_outgoing_tcp`], [`Settings::enable_incoming_tcp`],
//! [`Settings::enable_outgoing_utp`] and [`Settings::enable_incoming_utp`].
//!
//! # Listen addresses
//!
//! [`Settings::listen_addrs`] lists the addresses to listen on (IPv4 and
//! IPv6, port 0 picks a free one). Each address serves TCP and uTP on the
//! same port number: the UDP socket is shared with the DHT, uTP and the
//! other UDP protocols. An address that cannot be bound is skipped;
//! [`Session::local_addrs`] lists the ones that were, and [`ListenError`]
//! reports when none could be.
//!
//! # Rate limits and peer classes
//!
//! [`Settings::rate_limit`] ([`RateLimit`]) caps payload and message bytes
//! per second in each direction; 0 means unlimited. Peers on the local
//! network are exempt unless [`Settings::rate_limit_local_peers`] is set.
//! [`Settings::peer_class_limits`] ([`PeerClassLimits`]) limits the `tcp`,
//! `utp` and `local` classes, always including local peers, and
//! [`MixedMode`] decides how a global limit is shared between TCP and uTP.
//! Limits change at runtime with [`Session::set_rate_limit`] and
//! [`Session::set_peer_class_limits`].
//!
//! # Cache
//!
//! [`Settings::cache_size`] bounds the memory the session spends on dirty,
//! clean and partial-piece bytes together. Only verified bytes are served
//! to peers or written to storage. Verified pieces are written once they
//! pass half the cache, once they are older than [`Settings::cache_expiry`],
//! and always when a torrent finishes, pauses, is removed or rechecks, and
//! at shutdown. Each downloading torrent also has a progress floor of a few
//! pieces beyond the bound, documented on [`Settings::cache_size`], so a
//! small cache slows it down without stalling it. [`CacheStats`] reports
//! the dirty, clean and partial bytes and the limit
//! ([`Session::cache_stats`]).
//!
//! # Local discovery and port mapping
//!
//! With [`Settings::enable_lsd`] (on by default) the session announces the
//! info hashes of its public torrents on the LAN's multicast group (BEP 14)
//! at most every five minutes, each announce naming the listen port and a
//! random cookie that identifies our own messages, and treats the answers
//! as peers ([`PeerSource::Lsd`]). Private torrents are never announced.
//! [`Settings::enable_upnp`] and [`Settings::enable_natpmp`] (also on by
//! default) ask the router to map the listen ports, with UPnP, PCP or
//! NAT-PMP, and renew the mappings. On the LAN this puts UPnP's SSDP
//! `M-SEARCH` datagrams on the multicast group 239.255.255.250:1900 (two
//! search targets, three rounds 500 ms apart, per discovery), then HTTP
//! and SOAP requests to the router that answered, and NAT-PMP and PCP
//! datagrams to the default gateway only. Results arrive as
//! [`Event::PortMapped`], [`Event::PortMappingFailed`] and
//! [`Event::ExternalIpChanged`] under [`EventMask::NAT`].
//!
//! # Holepunch
//!
//! With [`Settings::enable_holepunch`] peers that cannot reach each other
//! because of a NAT ask a common peer to relay a rendezvous (BEP 55) and
//! then connect to each other at the same time, over uTP. It takes effect
//! only when both [`Settings::enable_outgoing_utp`] and
//! [`Settings::enable_incoming_utp`] are on. The relay rides on peer
//! exchange, so private torrents never use it.
//!
//! Limits, the cache and the NAT events together:
//!
//! ```no_run
//! use fbt::{Event, EventMask, RateLimit, Session, Settings};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut settings = Settings::default();
//! settings.rate_limit = RateLimit::new(2_000_000, 500_000);
//! settings.cache_size = 128 * 1024 * 1024;
//! let session = Session::new(settings).await?;
//! let mut events = session.subscribe(EventMask::NAT);
//! while let Some(event) = events.recv().await {
//!     match event {
//!         Event::PortMapped { protocol, method, external, .. } => {
//!             println!("{protocol:?} mapped by {method:?}: {external}");
//!         }
//!         Event::ExternalIpChanged { ip, source } => println!("{ip} from {source:?}"),
//!         _ => {}
//!     }
//! }
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Streaming
//!
//! [`TorrentHandle::open_file`] returns a [`FileReader`] for one file of a
//! torrent that is still downloading. It implements tokio's `AsyncRead` and
//! `AsyncSeek`, so a player, an HTTP range server or a plain
//! `tokio::io::copy` can read it. The reader's position sets deadlines on
//! the pieces of its read-ahead window, and the torrent fetches those first,
//! from the fastest peers, whatever the file's [`Priority`] (a `Skip` file
//! still gets its window and nothing more). Seeking replaces the deadlines.
//!
//! The window is [`ReadAhead`]: at least `bytes` (16 MiB by default), or
//! `time` (10 s) of playback at `bitrate` when that is more. A client that
//! knows the stream's bitrate passes it
//! ([`FileReader::set_read_ahead`], [`TorrentHandle::open_file_with`],
//! [`Settings::read_ahead`]); without one the bitrate implied is
//! `bytes / time`.
//!
//! A read of a piece that is not there yet waits instead of failing; it
//! returns only bytes that passed their piece's hash check. A read fails
//! only with a [`StreamError`]: the torrent was removed, the session shut
//! down, or the torrent stopped with an error (an `io::Error` of kind
//! `NotFound`, `ConnectionAborted` or `Other`). The reader is not poisoned:
//! after a resume the next read works. There is no read timeout; wrap reads
//! in `tokio::time::timeout`. A paused torrent serves the pieces it has and
//! leaves the others waiting until it resumes.
//!
//! The choker and the rate limits are unchanged: the choker governs our
//! uploads, and deadline blocks count against every rate limit like any
//! other block, so a download limit below the stream's bitrate makes reads
//! wait. A torrent has at most 16 open readers
//! ([`SessionError::TooManyReaders`]); their deadlines merge by the earliest
//! per piece.
//!
//! ```no_run
//! use std::io::SeekFrom;
//! use fbt::{AddTorrent, Metainfo, Session, Settings};
//! use tokio::io::{AsyncReadExt, AsyncSeekExt};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let metainfo = Metainfo::from_bytes(&std::fs::read("example.torrent")?)?;
//! let session = Session::new(Settings::default()).await?;
//! let torrent = session
//!     .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
//!     .await?;
//!
//! let mut file = torrent.open_file(0).await?;
//! file.seek(SeekFrom::Start(1 << 20)).await?;
//! tokio::io::copy(&mut file.take(1 << 20), &mut tokio::io::sink()).await?;
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `tls` (default): HTTPS trackers over rustls with the `ring` provider,
//!   which needs a C toolchain to build.
//! - `pure-rust`: HTTPS trackers over rustls with the pure-Rust RustCrypto
//!   provider. Depend on `fbt` with
//!   `default-features = false, features = ["pure-rust"]` for a build with no
//!   C or assembly.
//! - `serde` (off by default): `Serialize` and `Deserialize` for the public
//!   config structs and enums ([`Settings`], [`ReadAhead`], [`RateLimit`],
//!   [`PeerClassLimits`], [`MixedMode`], [`ChokingAlgorithm`],
//!   [`SeedChokingAlgorithm`], [`EncryptionPolicy`], [`Priority`],
//!   [`RemoveMode`]). Structs use `#[serde(default)]`, so a saved config
//!   missing newer fields still loads; a `Duration` is `{secs, nanos}`. An
//!   [`IpFilter`] is a list of `{ start, end, blocked }` rules.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use fbt_bencode::DecodeError;
pub use fbt_core::{
    Bitfield, BitfieldError, BlockRef, FileLayout, FileSlice, InfoHashV1, InfoHashV2, InfoHashes,
    LayoutError, ParseInfoHashError, PeerId, PieceLayout,
};
pub use fbt_metainfo::{FileEntry, Info, Magnet, MagnetError, Metainfo, MetainfoError, RelPath};
pub use fbt_session::storage::{FileStamp, FsStorage, MemStorage, Storage};
pub use fbt_session::{
    AddTorrent, BlockReason, CacheStats, ChokingAlgorithm, DEFAULT_DHT_BOOTSTRAP, DhtSamples,
    DhtScrape, DhtStats, DisconnectReason, EncryptionPolicy, Event, EventMask, EventStream,
    ExternalIpSource, FileReader, IpFilter, ListenError, MAX_METADATA_SIZE, MapMethod, MapProtocol,
    MixedMode, MoveMode, NodeId, NodeInfo, PeerClassLimits, PeerSource, Priority, ProxyConfig,
    ProxyKind, QueueSettings, RateLimit, ReadAhead, RemoveMode, ResumeError, SeedChokingAlgorithm,
    Session, SessionError, SessionState, Settings, StorageError, StreamError, TorrentHandle,
    TorrentSource, TorrentState, TorrentStatus, Transport,
};
pub use fbt_tracker::{TrackerConfig, TrackerError};

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
