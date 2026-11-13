//! fbt-session: Torrent session engine: peers, pieces, storage.
//!
//! A [`Session`] owns its listeners and any number of torrents, each run
//! by one actor task behind a [`TorrentHandle`]. Status is read from cheap
//! snapshots ([`TorrentStatus`]); everything else that happens is reported
//! as [`Event`]s through [`Session::subscribe`].
//!
//! # DHT
//!
//! With [`Settings::dht_enabled`] (the default) the session runs one Mainline
//! DHT node (BEP 5) on the listen addresses' UDP sockets (one per address
//! family, shared with UDP tracker announces), bootstrapping from
//! [`Settings::dht_bootstrap_nodes`] (the public routers in
//! [`DEFAULT_DHT_BOOTSTRAP`] by default). Every running public torrent looks
//! up and announces itself every [`Settings::dht_announce_interval`];
//! peers' `Port` messages add their nodes. [`Session::dht_state`] returns the
//! node's state to pass back as [`Settings::dht_state`] next time; the
//! library never writes files. Private torrents never touch the DHT. A
//! seeding torrent announces itself as a seed (BEP 33);
//! [`Session::dht_scrape`] estimates a torrent's seeds and peers (BEP 33)
//! and [`Session::dht_sample_infohashes`] asks one node for samples of the
//! info hashes it stores (BEP 51). [`Session::dht_put_immutable`],
//! [`Session::dht_get_immutable`], [`Session::dht_put_mutable`] and
//! [`Session::dht_get_mutable`] store and fetch BEP 44 items
//! ([`MutableItem`]).
//!
//! # Magnet links
//!
//! [`AddTorrent::magnet`] adds a torrent by its info hash: the info
//! dictionary is fetched from peers with BEP 9 (`ut_metadata`), checked
//! against the info hash, and then the torrent runs like one added from a
//! `.torrent` ([`Event::MetadataReceived`] marks the switch). Peer-supplied
//! metadata is hostile: sizes above [`Settings::max_metadata_size`] are never
//! requested, and a peer whose metadata is inconsistent or fails the hash is
//! disconnected with [`DisconnectReason::BadMetadata`] and never dialled
//! again. While the metadata is missing, announces report `left = 16384`.
//!
//! A magnet's privacy (BEP 27) is unknown until its metadata arrives, so
//! until then it behaves as a public torrent: it advertises the DHT bit,
//! may send `Port`, advertises `ut_pex` and runs the DHT announcer, as
//! libtorrent does. At [`Event::MetadataReceived`] a private torrent stops
//! its DHT announcer and PEX at once and ignores later `Port` and `ut_pex`
//! messages.
//!
//! # Peer filtering
//!
//! [`Session::set_ip_filter`] blocks ranges of peer addresses ([`IpFilter`])
//! from every source, incoming and outgoing; the session also keeps a ban
//! list ([`Session::banned_peers`]) and each torrent accepts at most
//! [`Settings::max_peers_per_ip`] connections from one IP. Every refusal is
//! reported as [`Event::PeerBlocked`] with its [`BlockReason`].
//!
//! # Resume data and session state
//!
//! [`TorrentHandle::save_resume_data`] returns a torrent's state as bytes
//! (the library writes no file of its own) and [`AddTorrent::from_resume`]
//! adds it back: the pieces it had are trusted without a check when every
//! file holding one is unchanged (same length and modification time, see
//! [`FileStamp`]), and checked in full otherwise
//! ([`TorrentStatus::resume_trusted`] tells which). Resume data saved while
//! a torrent writes is trusted only if nothing was written after the save:
//! save after [`TorrentHandle::pause`], or with
//! [`Session::shutdown_and_save`]. [`Session::save_state`] and
//! [`Session::shutdown_and_save`] bundle the DHT state and every torrent's
//! resume data into a [`SessionState`]; [`Session::restore`] starts a
//! session from it. Resume data is hostile input: a bad field fails with a
//! [`ResumeError`] naming it.

#![forbid(unsafe_code)]

mod bandwidth;
mod bans;
pub mod choker;
mod disk;
pub mod error;
mod event;
mod listener;
mod net;
mod peer;
pub mod picker;
mod queue;
pub mod rate;
mod resume;
mod session;
pub mod settings;
mod status;
pub mod storage;
pub mod stream;
mod torrent;

pub use bandwidth::{MixedMode, PeerClassLimits, RateLimit};
pub use bans::BlockReason;
pub use choker::{ChokingAlgorithm, SeedChokingAlgorithm};
pub use error::{SessionError, StreamError};
pub use event::{Event, EventMask, EventStream, ExternalIpSource, PeerSource};
pub use fbt_dht::{DhtError, DhtSamples, DhtScrape, DhtStats, MutableItem, NodeId, NodeInfo};
pub use fbt_net::ipfilter::IpFilter;
/// The most rules an [`IpFilter`] holds (`fbt_net::ipfilter::MAX_RANGES`).
pub use fbt_net::ipfilter::MAX_RANGES as MAX_IP_FILTER_RANGES;
pub use fbt_net::proxy::{ProxyConfig, ProxyError, ProxyKind};
pub use fbt_net::{MapMethod, MapProtocol};
pub use net::ListenError;
pub use peer::{DisconnectReason, Transport};
pub use queue::QueueSettings;
pub use rate::RateMeter;
#[doc(hidden)]
pub use resume::{DecodedResume, decode_resume};
pub use resume::{
    MAX_RESUME_BYTES, MAX_RESUME_ITEMS, MAX_RESUME_PEERS, MAX_SESSION_TORRENTS, ResumeError,
    SessionState,
};
pub use session::{AddTorrent, Priority, RemoveMode, Session, TorrentSource};
pub use settings::{
    DEFAULT_DHT_BOOTSTRAP, EncryptionPolicy, MAX_DISK_THREADS, MAX_METADATA_SIZE, Settings,
};
pub use status::{FileStatus, TorrentState, TorrentStatus, TrackerStatus};
pub use storage::{CacheStats, FileStamp, MoveMode, StorageError};
pub use stream::{FileReader, ReadAhead};
pub use torrent::TorrentHandle;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
