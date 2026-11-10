//! fbt-session: Torrent session engine: peers, pieces, storage.

#![forbid(unsafe_code)]

pub mod choker;
pub mod error;
pub mod picker;
pub mod storage;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
