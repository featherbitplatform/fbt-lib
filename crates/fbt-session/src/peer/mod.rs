//! One task per peer connection, speaking the wire protocol and exchanging
//! [`messages::PeerEvent`]s and [`messages::PeerCommand`]s with the torrent.

pub(crate) mod conn;
pub(crate) mod ext;
pub(crate) mod messages;
pub(crate) mod transport;

pub use messages::DisconnectReason;
pub use transport::Transport;
