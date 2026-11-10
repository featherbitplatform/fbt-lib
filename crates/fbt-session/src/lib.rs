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
//! library never writes files. Private torrents never touch the DHT.
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

#![forbid(unsafe_code)]

pub mod choker;
pub mod error;
mod event;
mod listener;
mod net;
mod peer;
pub mod picker;
pub mod rate;
mod session;
pub mod settings;
mod status;
pub mod storage;
mod torrent;

pub use error::SessionError;
pub use event::{Event, EventMask, EventStream, PeerSource};
pub use fbt_dht::DhtStats;
pub use net::ListenError;
pub use peer::{DisconnectReason, Transport};
pub use rate::RateMeter;
pub use session::{AddTorrent, Priority, RemoveMode, Session, TorrentSource};
pub use settings::{DEFAULT_DHT_BOOTSTRAP, EncryptionPolicy, MAX_METADATA_SIZE, Settings};
pub use status::{TorrentState, TorrentStatus};
pub use torrent::TorrentHandle;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
