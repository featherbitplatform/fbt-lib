//! Torrent ids in paths: hex of the 40-byte wire hash or the 64-byte v2
//! hash, in either case.

use std::fmt::Write as _;

use fbt::{InfoHashV1, InfoHashV2, InfoHashes};

use crate::error::ApiErrorResponse;

/// Parses a path id into the hashes `Session::torrent` looks up: 40 hex
/// is a wire hash (v1, or a truncated v2 hash), 64 hex a v2 hash.
/// `400 invalid_id` otherwise.
pub(crate) fn parse_id(id: &str) -> Result<InfoHashes, ApiErrorResponse> {
    let bad = || {
        ApiErrorResponse::new(
            axum::http::StatusCode::BAD_REQUEST,
            "invalid_id",
            "a torrent id is 40 or 64 hex digits",
        )
    };
    match id.len() {
        40 => id
            .parse::<InfoHashV1>()
            .map(InfoHashes::from_v1)
            .map_err(|_| bad()),
        64 => id
            .parse::<InfoHashV2>()
            .map(InfoHashes::from_v2)
            .map_err(|_| bad()),
        _ => Err(bad()),
    }
}

/// Lowercase hex of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_lengths_and_cases() {
        assert!(parse_id(&"ab".repeat(20)).is_ok());
        assert!(parse_id(&"AB".repeat(20)).is_ok());
        assert!(parse_id(&"ab".repeat(32)).is_ok());
        assert!(parse_id("abc").is_err());
        assert!(parse_id(&"zz".repeat(20)).is_err());
        assert!(parse_id(&"é".repeat(20)).is_err());
    }

    #[test]
    fn hex_is_lowercase() {
        assert_eq!(hex(&[0xab, 0x01]), "ab01");
    }
}
