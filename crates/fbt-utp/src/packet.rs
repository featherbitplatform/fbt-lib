//! BEP 29 packet header, extension chain and selective ack.

use bytes::Bytes;

/// Fixed header length in bytes.
pub const HEADER_LEN: usize = 20;
/// Extension links accepted in one packet.
pub const MAX_EXTENSIONS: usize = 8;
/// Bytes of selective-ack mask kept; longer masks are cut.
pub const MAX_SACK_LEN: usize = 32;

const VERSION: u8 = 1;
const EXT_SACK: u8 = 1;

/// The packet type (high nibble of the first byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

impl PacketType {
    fn from_nibble(n: u8) -> Option<Self> {
        match n {
            0 => Some(Self::Data),
            1 => Some(Self::Fin),
            2 => Some(Self::State),
            3 => Some(Self::Reset),
            4 => Some(Self::Syn),
            _ => None,
        }
    }
}

/// A malformed uTP datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PacketError {
    #[error("datagram shorter than the 20-byte header")]
    TooShort,
    #[error("unsupported uTP version")]
    BadVersion,
    #[error("unknown packet type")]
    BadType,
    #[error("malformed extension chain")]
    BadExtension,
}

/// A decoded uTP packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub ty: PacketType,
    pub conn_id: u16,
    pub timestamp_us: u32,
    pub timestamp_diff_us: u32,
    pub wnd_size: u32,
    pub seq_nr: u16,
    pub ack_nr: u16,
    /// Selective-ack bit mask (at most [`MAX_SACK_LEN`] bytes).
    pub sack: Option<Vec<u8>>,
    pub payload: Bytes,
}

fn be16(d: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        d.get(at..at.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn be32(d: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        d.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

impl Packet {
    /// Appends the wire form to `out`. A SACK is cut to [`MAX_SACK_LEN`] and
    /// zero-padded to a multiple of four bytes; an empty one is omitted.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let sack = self.sack.as_deref().filter(|s| !s.is_empty()).map(|s| {
            let s = s.get(..MAX_SACK_LEN).unwrap_or(s);
            (s, s.len().div_ceil(4) * 4)
        });
        out.push(((self.ty as u8) << 4) | VERSION);
        out.push(if sack.is_some() { EXT_SACK } else { 0 });
        out.extend_from_slice(&self.conn_id.to_be_bytes());
        out.extend_from_slice(&self.timestamp_us.to_be_bytes());
        out.extend_from_slice(&self.timestamp_diff_us.to_be_bytes());
        out.extend_from_slice(&self.wnd_size.to_be_bytes());
        out.extend_from_slice(&self.seq_nr.to_be_bytes());
        out.extend_from_slice(&self.ack_nr.to_be_bytes());
        if let Some((s, padded)) = sack {
            out.push(0);
            // padded <= MAX_SACK_LEN, so it fits a byte.
            out.push(padded as u8);
            out.extend_from_slice(s);
            out.resize(out.len() + (padded - s.len()), 0);
        }
        out.extend_from_slice(&self.payload);
    }

    /// Parses one datagram. Never panics; the payload shares `datagram`.
    pub fn decode(datagram: &Bytes) -> Result<Packet, PacketError> {
        let d = datagram.as_ref();
        if d.len() < HEADER_LEN {
            return Err(PacketError::TooShort);
        }
        let first = d.first().copied().ok_or(PacketError::TooShort)?;
        if first & 0x0f != VERSION {
            return Err(PacketError::BadVersion);
        }
        let ty = PacketType::from_nibble(first >> 4).ok_or(PacketError::BadType)?;
        let mut next = d.get(1).copied().ok_or(PacketError::TooShort)?;
        let short = PacketError::TooShort;
        let mut pos = HEADER_LEN;
        let mut links = 0usize;
        let mut sack: Option<Vec<u8>> = None;
        while next != 0 {
            links += 1;
            if links > MAX_EXTENSIONS {
                return Err(PacketError::BadExtension);
            }
            let bad = PacketError::BadExtension;
            let following = *d.get(pos).ok_or(bad)?;
            let len = usize::from(*d.get(pos + 1).ok_or(bad)?);
            let start = pos + 2;
            let end = start + len;
            let body = d.get(start..end).ok_or(bad)?;
            if next == EXT_SACK {
                // BEP 29 asks for a multiple of four bytes, but libtorrent
                // sends only the bytes its bits need (`ceil(span / 8)`), so
                // any non-empty length is taken.
                if len == 0 {
                    return Err(bad);
                }
                if sack.is_none() {
                    sack = Some(body.get(..MAX_SACK_LEN).unwrap_or(body).to_vec());
                }
            }
            pos = end;
            next = following;
        }
        Ok(Packet {
            ty,
            conn_id: be16(d, 2).ok_or(short)?,
            timestamp_us: be32(d, 4).ok_or(short)?,
            timestamp_diff_us: be32(d, 8).ok_or(short)?,
            wnd_size: be32(d, 12).ok_or(short)?,
            seq_nr: be16(d, 16).ok_or(short)?,
            ack_nr: be16(d, 18).ok_or(short)?,
            sack,
            payload: datagram.slice(pos..),
        })
    }
}

/// Sequence numbers acknowledged by a SACK mask. Bit `j` (LSB first) of byte
/// `k` acknowledges `ack_nr + 2 + 8k + j`, wrapping. Callers pass a decoded
/// mask of at most [`MAX_SACK_LEN`] bytes.
pub fn sack_acks(sack: &[u8], ack_nr: u16) -> impl Iterator<Item = u16> + '_ {
    sack.iter().enumerate().flat_map(move |(k, byte)| {
        (0..8usize)
            .filter(move |j| (byte >> j) & 1 == 1)
            .map(move |j| ack_nr.wrapping_add(2).wrapping_add((8 * k + j) as u16))
    })
}

/// `a` is strictly before `b` in wrapping sequence space. Exactly half the
/// window apart (`b - a == 0x8000`) is a tie: neither is before the other.
pub fn seq_lt(a: u16, b: u16) -> bool {
    (1..0x8000).contains(&b.wrapping_sub(a))
}

/// `a` is before or equal to `b` in wrapping sequence space.
pub fn seq_le(a: u16, b: u16) -> bool {
    a == b || seq_lt(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn state(ack: u16) -> Packet {
        Packet {
            ty: PacketType::State,
            conn_id: 7,
            timestamp_us: 1,
            timestamp_diff_us: 2,
            wnd_size: 3,
            seq_nr: 4,
            ack_nr: ack,
            sack: None,
            payload: Bytes::new(),
        }
    }

    fn syn_bytes() -> Vec<u8> {
        let p = Packet {
            ty: PacketType::Syn,
            conn_id: 0x1234,
            timestamp_us: 0x0102_0304,
            timestamp_diff_us: 0,
            wnd_size: 0x0010_0000,
            seq_nr: 1,
            ack_nr: 0,
            sack: None,
            payload: Bytes::new(),
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        out
    }

    #[test]
    fn syn_header_vector() {
        let p = Packet {
            ty: PacketType::Syn,
            conn_id: 0x1234,
            timestamp_us: 0x0102_0304,
            timestamp_diff_us: 0,
            wnd_size: 0x0010_0000,
            seq_nr: 1,
            ack_nr: 0,
            sack: None,
            payload: Bytes::new(),
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        assert_eq!(
            out,
            [
                0x41, 0, 0x12, 0x34, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 1, 0, 0
            ]
        );
        assert_eq!(Packet::decode(&Bytes::from(out)).unwrap(), p);
    }

    #[test]
    fn sack_roundtrip_and_bits() {
        let p = Packet {
            ty: PacketType::State,
            sack: Some(vec![0b0000_0101, 0, 0, 0x80]),
            ..state(10)
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        assert_eq!(out[1], 1);
        assert_eq!(&out[20..22], &[0, 4]);
        let back = Packet::decode(&Bytes::from(out)).unwrap();
        assert_eq!(
            sack_acks(back.sack.as_deref().unwrap(), 10).collect::<Vec<_>>(),
            vec![12, 14, 43]
        );
        assert_eq!(sack_acks(&[1], 0xffff).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn rejects_bad_packets() {
        assert_eq!(
            Packet::decode(&Bytes::from_static(&[0x41; 19])),
            Err(PacketError::TooShort)
        );
        let mut v = syn_bytes();
        v[0] = 0x42;
        assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadVersion));
        let mut v = syn_bytes();
        v[0] = 0x51;
        assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadType));
        let mut v = syn_bytes();
        v[1] = 1;
        v.extend([0, 0]);
        assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadExtension));
        let mut v = syn_bytes();
        v[1] = 9;
        for _ in 0..20 {
            v.extend([9, 0]);
        }
        assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadExtension));
    }

    #[test]
    fn wrapping_sequence_order() {
        assert!(seq_lt(0xfffe, 1));
        assert!(!seq_lt(1, 0xfffe));
        assert!(seq_le(5, 5));
    }

    #[test]
    fn half_window_boundaries() {
        assert!(seq_lt(0, 0x7fff));
        assert!(!seq_lt(0, 0x8000));
        assert!(!seq_lt(0x8000, 0));
        assert!(!seq_le(0, 0x8000));
    }

    #[test]
    fn short_sack_is_padded_on_encode() {
        let p = Packet {
            sack: Some(vec![1, 2, 3]),
            ..state(0)
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        assert_eq!(&out[20..], &[0, 4, 1, 2, 3, 0]);
    }

    #[test]
    fn long_sack_is_cut_on_encode() {
        let p = Packet {
            sack: Some(vec![0xff; 40]),
            ..state(0)
        };
        let mut out = Vec::new();
        p.encode(&mut out);
        assert_eq!(out[21], 32);
        assert_eq!(out.len(), 20 + 2 + 32);
    }

    /// libtorrent sizes its SACK to the bits it needs, `ceil(span / 8)`
    /// bytes, not a multiple of four as BEP 29 asks: seen from qBittorrent
    /// 5.3 as a 1-byte mask. Rejecting it dropped every packet (data and
    /// acks) qBittorrent sent while it held a hole in our stream.
    #[test]
    fn libtorrent_sack_of_any_length_is_accepted() {
        for len in [1u8, 2, 3, 5] {
            let mut v = syn_bytes();
            v[1] = 1;
            v.extend([0, len]);
            v.extend((0..len).map(|i| i + 1));
            v.extend([7, 7]);
            let p = Packet::decode(&Bytes::from(v)).unwrap();
            let want: Vec<u8> = (0..len).map(|i| i + 1).collect();
            assert_eq!(p.sack.as_deref(), Some(&want[..]));
            assert_eq!(&p.payload[..], &[7, 7]);
        }
    }

    #[test]
    fn long_sack_is_cut_not_rejected() {
        let mut v = syn_bytes();
        v[1] = 1;
        v.extend([0, 40]);
        v.extend([0xff; 40]);
        v.extend([9, 9]);
        let p = Packet::decode(&Bytes::from(v)).unwrap();
        assert_eq!(p.sack.unwrap().len(), MAX_SACK_LEN);
        assert_eq!(&p.payload[..], &[9, 9]);
    }

    proptest! {
        #[test]
        fn decode_never_panics_and_roundtrips(
            mut bytes in proptest::collection::vec(any::<u8>(), 0..200),
            biased in any::<bool>(),
            ty in 1u8..=0x41,
            ext in prop::sample::select(vec![0u8, 1, 9]),
        ) {
            if biased && bytes.len() >= 2 {
                bytes[0] = (ty & 0xf0) | 1;
                bytes[0] = bytes[0].clamp(0x01, 0x41);
                bytes[1] = ext;
            }
            if let Ok(p) = Packet::decode(&Bytes::from(bytes)) {
                let mut out = Vec::new();
                p.encode(&mut out);
                prop_assert_eq!(Packet::decode(&Bytes::from(out)).unwrap(), p);
            }
        }

        #[test]
        fn structured_packets_roundtrip(
            ty in prop::sample::select(vec![
                PacketType::Data, PacketType::Fin, PacketType::State,
                PacketType::Reset, PacketType::Syn,
            ]),
            conn_id in any::<u16>(),
            timestamp_us in any::<u32>(),
            timestamp_diff_us in any::<u32>(),
            wnd_size in any::<u32>(),
            seq_nr in any::<u16>(),
            ack_nr in any::<u16>(),
            sack in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=64)),
            payload in proptest::collection::vec(any::<u8>(), 0..=64),
        ) {
            let p = Packet {
                ty, conn_id, timestamp_us, timestamp_diff_us, wnd_size, seq_nr, ack_nr,
                sack: sack.clone(),
                payload: Bytes::from(payload),
            };
            let mut out = Vec::new();
            p.encode(&mut out);
            let expected_sack = sack.filter(|s| !s.is_empty()).map(|mut s| {
                s.truncate(MAX_SACK_LEN);
                while s.len() % 4 != 0 {
                    s.push(0);
                }
                s
            });
            let back = Packet::decode(&Bytes::from(out)).unwrap();
            prop_assert_eq!(back, Packet { sack: expected_sack, ..p });
        }
    }
}
