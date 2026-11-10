//! fbt-session: Torrent session engine: peers, pieces, storage.

#![forbid(unsafe_code)]

pub mod choker;
pub mod error;
#[allow(
    dead_code,
    reason = "the torrent actor (next task) is the only caller of the peer task"
)]
mod peer;
pub mod picker;
pub mod rate;
pub mod settings;
pub mod storage;

pub use peer::DisconnectReason;
pub use rate::RateMeter;
pub use settings::Settings;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
