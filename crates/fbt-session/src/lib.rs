//! fbt-session: Torrent session engine: peers, pieces, storage.

#![forbid(unsafe_code)]

pub mod error;
pub mod storage;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
