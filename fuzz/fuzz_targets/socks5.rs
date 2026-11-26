#![no_main]
use fbt_net::proxy::{decode_udp, encode_udp, encode_udp_host, parse_socks5_reply};
use libfuzzer_sys::fuzz_target;

/// The bytes a target host name may hold (`ProxyTarget::Host`): letters,
/// digits, `.`, `_`, `-`.
const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789._-";

/// Reads a domain-framed datagram back the way a relay does
/// (`RSV RSV FRAG ATYP DST.ADDR DST.PORT DATA`).
fn decode_udp_host(frame: &[u8]) -> Option<(&str, u16, &[u8])> {
    let ([_, _, frag, atyp], rest) = frame.split_first_chunk::<4>()?;
    if *frag != 0 || *atyp != 3 {
        return None;
    }
    let (len, rest) = rest.split_first()?;
    let (name, rest) = rest.split_at_checked(usize::from(*len))?;
    let (port, rest) = rest.split_first_chunk::<2>()?;
    Some((
        std::str::from_utf8(name).ok()?,
        u16::from_be_bytes(*port),
        rest,
    ))
}

// The same bytes as a TCP reply (and every prefix of it, as it arrives), as a
// relayed UDP datagram, and as a target name a datagram is framed for.
fuzz_target!(|data: &[u8]| {
    if let Ok(Some((_, len))) = parse_socks5_reply(data) {
        assert!(len <= data.len());
    }
    for end in 0..data.len().min(262) {
        let _ = parse_socks5_reply(&data[..end]);
    }
    if let Some((src, payload)) = decode_udp(data) {
        let mut out = Vec::new();
        encode_udp(src, payload, &mut out);
        assert_eq!(decode_udp(&out), Some((src, payload)));
    }

    // A domain-framed datagram round trip: whatever the name, the frame
    // carries it back unchanged.
    let host: String = data
        .iter()
        .take(300)
        .map(|b| char::from(ALPHABET[usize::from(*b) % ALPHABET.len()]))
        .collect();
    let port = u16::from_be_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ]);
    let mut out = Vec::new();
    match encode_udp_host(&host, port, data, &mut out) {
        Ok(()) => assert_eq!(
            decode_udp_host(&out),
            Some((host.as_str(), port, data)),
            "a frame decode_udp_host reads back is the one it was given"
        ),
        Err(e) => assert!(
            host.is_empty() || host.len() > 255,
            "only a name a length byte cannot carry is refused: {host:?} {e:?}"
        ),
    }

    // An arbitrary name, the fuzzer's own bytes: framed exactly when a
    // length byte and the alphabet allow it.
    let raw = String::from_utf8_lossy(data.get(..data.len().min(300)).unwrap_or_default());
    let sendable = !raw.is_empty()
        && raw.len() <= 255
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    let mut out = Vec::new();
    assert_eq!(
        encode_udp_host(&raw, 1, b"", &mut out).is_ok(),
        sendable,
        "{raw:?}"
    );
});
