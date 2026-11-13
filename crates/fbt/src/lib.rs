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
//! # Creating a torrent
//!
//! [`CreateTorrent`] hashes a file or a directory and returns the bencoded
//! `.torrent`. It makes a hybrid (v1 and v2) torrent by default; hashing is
//! synchronous, so async callers run it on `spawn_blocking`.
//!
//! ```
//! use std::ops::ControlFlow;
//! use fbt::{CreateTorrent, Metainfo, TorrentVersion};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! std::fs::write(dir.path().join("hello.txt"), b"hello, swarm")?;
//!
//! let bytes = CreateTorrent::new(dir.path().join("hello.txt"))
//!     .version(TorrentVersion::Hybrid)
//!     .build(&mut |_done, _total| ControlFlow::Continue(()))?;
//!
//! let metainfo = Metainfo::from_bytes(&bytes)?;
//! assert_eq!(metainfo.version(), TorrentVersion::Hybrid);
//! assert!(metainfo.info_hashes.v1.is_some() && metainfo.info_hashes.v2.is_some());
//! # Ok(())
//! # }
//! # run().unwrap();
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
//! # Resume data and session state
//!
//! The library never writes state files; it hands out bytes and takes them
//! back. [`TorrentHandle::save_resume_data`] returns one torrent's resume
//! data (its `info` dictionary, so a magnet restarts without fetching the
//! metadata again, plus the save path, priorities, renamed files, verified
//! pieces, web seeds, queue position, counters, peers and a stamp of each
//! file) and [`AddTorrent::from_resume`] turns it back into add parameters.
//! [`Session::save_state`] collects every torrent's resume data and the DHT
//! state in queue order as a [`SessionState`]; [`Session::shutdown_and_save`]
//! does the same as it shuts down, and [`Session::restore`] starts a session
//! from it. Both return, beside the state, the torrents whose resume data
//! could not be taken, with their error.
//!
//! When a resume file is trusted: at the add, every file that holds a saved
//! piece (and the part file, when it holds one) must still have the length
//! and modification time, in whole seconds, it had at the save; then the
//! saved pieces are taken without a check. Anything else runs the normal
//! full check, and [`TorrentStatus::resume_trusted`] says which way it went.
//! Since a file written after the save no longer matches, save after
//! [`TorrentHandle::pause`] or through [`Session::shutdown_and_save`]: data
//! saved from a running torrent is valid, but the next add checks it in
//! full. A custom [`Storage`] without stamps ([`Storage::stamps`]) is always
//! trusted. [`TorrentStatus::need_save_resume`] is set whenever something
//! worth saving changed. Resume data is hostile input: anything malformed,
//! of another version or for another torrent fails with a [`ResumeError`]
//! naming the field, never a panic.
//!
//! ```no_run
//! use fbt::{AddTorrent, Metainfo, Session, SessionState, Settings};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let metainfo = Metainfo::from_bytes(&std::fs::read("example.torrent")?)?;
//! let session = Session::new(Settings::default()).await?;
//! let torrent = session
//!     .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
//!     .await?;
//!
//! // One torrent: pause first, so nothing is written after the save.
//! torrent.pause().await?;
//! std::fs::write("example.resume", torrent.save_resume_data().await?)?;
//! session.shutdown().await;
//!
//! // Next run: the saved pieces are trusted if the files are unchanged.
//! let session = Session::new(Settings::default()).await?;
//! let mut params = AddTorrent::from_resume(&std::fs::read("example.resume")?)?;
//! params.paused = false; // it was saved paused
//! let torrent = session.add_torrent(params).await?;
//! println!("{} added", torrent.status().name);
//!
//! // The whole session: every torrent and the DHT state at shutdown.
//! let (state, failed) = session.shutdown_and_save().await;
//! for (info_hash, error) in failed {
//!     eprintln!("{info_hash} not saved: {error}");
//! }
//! std::fs::write("session.state", state.to_bytes())?;
//!
//! // Next run: one call restores them all.
//! let state = SessionState::from_bytes(&std::fs::read("session.state")?)?;
//! let (session, failed) = Session::restore(Settings::default(), &state).await?;
//! for (index, error) in failed {
//!     eprintln!("torrent {index} not restored: {error}");
//! }
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Queueing
//!
//! Torrents are auto-managed by default ([`AddTorrent::auto_managed`]): in
//! queue order, at most 3 downloads and 5 seeds run at once, and 500 in all
//! ([`QueueSettings`] in [`Settings::queue`], changed at runtime with
//! [`Session::set_queue_settings`]; `None` lifts a limit). The others wait
//! as [`TorrentState::Queued`], stopped like a paused torrent. A torrent
//! slower than 2 KiB/s each way after a 60 s grace does not count against
//! the download and seed limits, so it does not hold the next one back. A
//! torrent the user paused is never started by the queue.
//! [`TorrentHandle::set_queue_position`] and
//! [`TorrentHandle::set_auto_managed`] change one torrent, and
//! [`TorrentStatus::queue_position`] reports its place. Seed limits are off
//! by default: an auto-managed seed that reaches
//! [`Settings::seed_ratio_limit`] or [`Settings::seed_time_limit`] pauses,
//! stops being auto-managed and emits [`Event::SeedLimitReached`].
//!
//! # Moving, renaming and skipped files
//!
//! [`TorrentHandle::move_storage`] moves a torrent's files to a new save
//! path ([`MoveMode`] decides what happens to files already there) after
//! writing every cached piece, and a failure moves back what moved.
//! [`TorrentHandle::rename_file`] renames one file below the save path.
//! Files skipped with [`Priority::Skip`] are never created: the parts of
//! their pieces that a wanted neighbour needs live in a part file,
//! `.<info hash>.parts` in the save path, and move into the file if it
//! becomes wanted. Colliding paths in a `.torrent` (`A.txt` and `a.txt`, or
//! one name in two Unicode normalisations) are renamed `stem.N.ext`.
//!
//! # Proxies
//!
//! [`Settings::proxy`] ([`ProxyConfig`]) sends peer connections, web seeds
//! and trackers through a SOCKS5 or HTTP `CONNECT` proxy ([`ProxyKind`]),
//! with `proxy_peers`, `proxy_trackers` and `proxy_hostnames` choosing what
//! goes through it (all on). Listening is unchanged. The caveats:
//!
//! - SOCKS5 proxies all UDP or none: with `proxy_peers` or
//!   `proxy_trackers`, outgoing uTP, the DHT and UDP trackers all go through
//!   one UDP ASSOCIATE. The session then listens on TCP only, the DHT runs
//!   over the relay's address family only (IPv4 in practice), and incoming
//!   uTP arrives only as the relay forwards it.
//! - An HTTP proxy cannot carry UDP: with `proxy_peers` outgoing uTP is off,
//!   with `proxy_trackers` UDP trackers fail, and the DHT runs directly
//!   unless [`Settings::dht_enabled`] is off.
//! - Many HTTP proxies refuse `CONNECT` to port 80, so plain-HTTP trackers
//!   may fail through them.
//! - UDP tracker and DHT bootstrap host names are resolved locally whatever
//!   `proxy_hostnames` says, and LSD, UPnP and NAT-PMP stay direct on the
//!   local network.
//!
//! # IP filter and bans
//!
//! [`Session::set_ip_filter`] installs an [`IpFilter`] (block and allow
//! ranges, the last rule wins) and closes the peers it now blocks. It
//! applies to peers from every source and to incoming connections, never
//! to trackers, DHT nodes or the proxy. A torrent also accepts at most
//! [`Settings::max_peers_per_ip`] connections from one IP address (1;
//! loopback is exempt). Smart ban: when a piece fails its hash check and
//! later passes, the peer whose blocks differed is banned for the rest of
//! the session, and a peer that keeps contributing to failed pieces loses
//! trust until it is banned; an honest co-contributor of a failed piece is
//! never banned. Bans live apart from the filter, so replacing the filter
//! lifts none ([`Session::banned_peers`] lists them). Refusals and closes
//! for any of these reasons are reported as [`Event::PeerBlocked`] with a
//! [`BlockReason`], and closes as [`DisconnectReason::Blocked`]. The
//! metadata ban is separate: a peer that sends bad metadata for a magnet
//! ([`DisconnectReason::BadMetadata`]) is never dialled again by that
//! torrent.
//!
//! # Web seeds
//!
//! HTTP and HTTPS web seeds (BEP 19) come from a `.torrent`'s `url-list`
//! ([`Metainfo::web_seeds`]), a magnet's `ws` ([`Magnet::web_seeds`]) or
//! [`TorrentHandle::add_web_seed`] (at most 16 per torrent;
//! [`TorrentHandle::remove_web_seed`] and [`TorrentHandle::web_seeds`]).
//! Each is fetched with ranged requests of up to 4 MiB of whole pieces,
//! one at a time, while the torrent downloads, under the download limits and
//! through the proxy with `proxy_peers`. A failed request backs off 30 s,
//! doubling to 30 minutes; a 4xx other than 408 and 429, or data that fails
//! the hash check, retires the seed. Both are reported as
//! [`Event::WebSeedError`].
//!
//! # DHT items
//!
//! The DHT also stores small items (BEP 44, one bencoded value of at most
//! 1000 bytes, kept about 2 hours on a node):
//! [`Session::dht_put_immutable`] and [`Session::dht_get_immutable`] by the
//! value's SHA-1, [`Session::dht_put_mutable`] and
//! [`Session::dht_get_mutable`] signed with an ed25519 key and ordered by a
//! sequence number ([`MutableItem`]). With the DHT off they fail with
//! [`SessionError::DhtDisabled`].
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
//!   [`RemoveMode`], [`MoveMode`], [`QueueSettings`], [`ProxyConfig`],
//!   [`ProxyKind`], [`TrackerConfig`]). Structs use `#[serde(default)]`, so a saved config
//!   missing newer fields still loads; a `Duration` is `{secs, nanos}`. An
//!   [`IpFilter`] is a list of `{ start, end, blocked }` rules.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use fbt_bencode::DecodeError;
pub use fbt_core::{
    Bitfield, BitfieldError, BlockRef, FileLayout, FileSlice, InfoHashV1, InfoHashV2, InfoHashes,
    LayoutError, ParseInfoHashError, PeerId, PieceLayout,
};
pub use fbt_metainfo::{
    CreateError, CreateTorrent, FileEntry, Info, MAX_CREATE_FILES, MAX_PIECE_LAYER_HASHES,
    MAX_WEB_SEEDS, Magnet, MagnetError, Metainfo, MetainfoError, RelPath, TorrentVersion, V2Piece,
    auto_piece_len, validate_layer,
};
pub use fbt_session::storage::{FileStamp, FsStorage, MemStorage, Storage};
pub use fbt_session::{
    AddTorrent, BlockReason, CacheStats, ChokingAlgorithm, DEFAULT_DHT_BOOTSTRAP, DhtError,
    DhtSamples, DhtScrape, DhtStats, DisconnectReason, EncryptionPolicy, Event, EventMask,
    EventStream, ExternalIpSource, FileReader, IpFilter, ListenError, MAX_IP_FILTER_RANGES,
    MAX_METADATA_SIZE, MAX_RESUME_BYTES, MAX_RESUME_ITEMS, MAX_RESUME_PEERS, MAX_SESSION_TORRENTS,
    MapMethod, MapProtocol, MixedMode, MoveMode, MutableItem, NodeId, NodeInfo, PeerClassLimits,
    PeerSource, Priority, ProxyConfig, ProxyError, ProxyKind, QueueSettings, RateLimit, ReadAhead,
    RemoveMode, ResumeError, SeedChokingAlgorithm, Session, SessionError, SessionState, Settings,
    StorageError, StreamError, TorrentHandle, TorrentSource, TorrentState, TorrentStatus,
    Transport,
};
pub use fbt_tracker::{TrackerConfig, TrackerError};

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
