//! RFC 3986 percent-encoding of raw bytes.

/// Encodes `bytes`: unreserved bytes (`A-Za-z0-9-._~`) pass through, every
/// other byte becomes `%XX` with uppercase hex.
pub fn url_encode_bytes(bytes: &[u8]) -> String {
    let hex = |n: u8| char::from_digit(u32::from(n), 16).map_or('0', |c| c.to_ascii_uppercase());
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(hex(b >> 4));
            out.push(hex(b & 0x0f));
        }
    }
    out
}
