//! Recursive-descent bencode decoder over `&[u8]`.

use std::collections::HashSet;

use crate::value::{Dict, Value, ValueKind};

/// Decoder limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum container nesting depth.
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_depth: 64 }
    }
}

/// Decoding failures; `pos` is the byte offset in the input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Input ended early.
    #[error("unexpected end of input at {pos}")]
    UnexpectedEof { pos: usize },
    /// A byte that cannot appear here.
    #[error("invalid byte {byte:#04x} at {pos}")]
    InvalidByte { byte: u8, pos: usize },
    /// Integer or length with a leading zero.
    #[error("leading zero at {pos}")]
    LeadingZero { pos: usize },
    /// `i-0e`.
    #[error("negative zero at {pos}")]
    NegativeZero { pos: usize },
    /// Integer does not fit `i64`.
    #[error("integer overflow at {pos}")]
    IntOverflow { pos: usize },
    /// String length does not fit `usize`.
    #[error("length overflow at {pos}")]
    LengthOverflow { pos: usize },
    /// Nesting deeper than `max_depth`.
    #[error("nesting too deep at {pos}")]
    DepthExceeded { pos: usize },
    /// Same dictionary key twice.
    #[error("duplicate dictionary key at {pos}")]
    DuplicateKey { pos: usize },
    /// Bytes left after the value.
    #[error("trailing data at {pos}")]
    TrailingData { pos: usize },
}

/// Decodes one value with default limits; the whole input must be consumed.
pub fn decode(input: &[u8]) -> Result<Value<'_>, DecodeError> {
    decode_with(input, Limits::default())
}

/// Like [`decode`] with explicit limits.
pub fn decode_with(input: &[u8], limits: Limits) -> Result<Value<'_>, DecodeError> {
    let (v, n) = decode_prefix(input, limits)?;
    if n != input.len() {
        return Err(DecodeError::TrailingData { pos: n });
    }
    Ok(v)
}

/// Decodes one value from the start of `input`, returning it and the number
/// of bytes consumed.
pub fn decode_prefix(input: &[u8], limits: Limits) -> Result<(Value<'_>, usize), DecodeError> {
    let mut p = Parser {
        input,
        pos: 0,
        max_depth: limits.max_depth,
    };
    let v = p.value(0)?;
    Ok((v, p.pos))
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    max_depth: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Result<u8, DecodeError> {
        self.input
            .get(self.pos)
            .copied()
            .ok_or(DecodeError::UnexpectedEof { pos: self.pos })
    }

    fn expect(&mut self, want: u8) -> Result<(), DecodeError> {
        let b = self.peek()?;
        if b != want {
            return Err(DecodeError::InvalidByte {
                byte: b,
                pos: self.pos,
            });
        }
        self.pos += 1;
        Ok(())
    }

    fn span(&self, start: usize) -> &'a [u8] {
        self.input.get(start..self.pos).unwrap_or_default()
    }

    fn value(&mut self, depth: usize) -> Result<Value<'a>, DecodeError> {
        let start = self.pos;
        let kind = match self.peek()? {
            b'i' => ValueKind::Int(self.int()?),
            b'0'..=b'9' => ValueKind::Bytes(self.bytes()?),
            b'l' => ValueKind::List(self.list(depth)?),
            b'd' => {
                let (entries, sorted) = self.dict(depth)?;
                let d = Dict {
                    raw: self.span(start),
                    entries,
                    sorted,
                };
                ValueKind::Dict(d)
            }
            byte => {
                return Err(DecodeError::InvalidByte {
                    byte,
                    pos: self.pos,
                });
            }
        };
        Ok(Value {
            raw: self.span(start),
            kind,
        })
    }

    fn int(&mut self) -> Result<i64, DecodeError> {
        let start = self.pos;
        self.expect(b'i')?;
        let neg = self.peek()? == b'-';
        if neg {
            self.pos += 1;
        }
        let digits_at = self.pos;
        let mut acc: i64 = 0;
        loop {
            let b = self.peek()?;
            if b == b'e' {
                break;
            }
            if !b.is_ascii_digit() {
                return Err(DecodeError::InvalidByte {
                    byte: b,
                    pos: self.pos,
                });
            }
            let d = i64::from(b - b'0');
            // Accumulate negatively for negative numbers so i64::MIN fits.
            acc = acc
                .checked_mul(10)
                .and_then(|a| {
                    if neg {
                        a.checked_sub(d)
                    } else {
                        a.checked_add(d)
                    }
                })
                .ok_or(DecodeError::IntOverflow { pos: start })?;
            self.pos += 1;
        }
        let n = self.pos - digits_at;
        if n == 0 {
            return Err(DecodeError::InvalidByte {
                byte: b'e',
                pos: self.pos,
            });
        }
        let first_zero = self.input.get(digits_at) == Some(&b'0');
        if neg && first_zero {
            return Err(DecodeError::NegativeZero { pos: start });
        }
        if first_zero && n > 1 {
            return Err(DecodeError::LeadingZero { pos: start });
        }
        self.pos += 1; // 'e'
        Ok(acc)
    }

    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let start = self.pos;
        let mut len: usize = 0;
        let mut n = 0usize;
        loop {
            let b = self.peek()?;
            if b == b':' {
                break;
            }
            if !b.is_ascii_digit() {
                return Err(DecodeError::InvalidByte {
                    byte: b,
                    pos: self.pos,
                });
            }
            len = len
                .checked_mul(10)
                .and_then(|l| l.checked_add(usize::from(b - b'0')))
                .ok_or(DecodeError::LengthOverflow { pos: start })?;
            n += 1;
            self.pos += 1;
        }
        if n > 1 && self.input.get(start) == Some(&b'0') {
            return Err(DecodeError::LeadingZero { pos: start });
        }
        self.pos += 1; // ':'
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.input.len())
            .ok_or(DecodeError::UnexpectedEof {
                pos: self.input.len(),
            })?;
        let s = self.input.get(self.pos..end).unwrap_or_default();
        self.pos = end;
        Ok(s)
    }

    fn enter(&self, depth: usize) -> Result<usize, DecodeError> {
        if depth >= self.max_depth {
            return Err(DecodeError::DepthExceeded { pos: self.pos });
        }
        Ok(depth + 1)
    }

    fn list(&mut self, depth: usize) -> Result<Vec<Value<'a>>, DecodeError> {
        let inner = self.enter(depth)?;
        self.pos += 1; // 'l'
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.value(inner)?);
        }
        self.pos += 1;
        Ok(items)
    }

    #[allow(clippy::type_complexity)]
    fn dict(&mut self, depth: usize) -> Result<(Vec<(&'a [u8], Value<'a>)>, bool), DecodeError> {
        let inner = self.enter(depth)?;
        self.pos += 1; // 'd'
        let mut entries: Vec<(&'a [u8], Value<'a>)> = Vec::new();
        let mut sorted = true;
        let mut seen: HashSet<&'a [u8]> = HashSet::new();
        while self.peek()? != b'e' {
            let key_pos = self.pos;
            if !self.peek()?.is_ascii_digit() {
                return Err(DecodeError::InvalidByte {
                    byte: self.peek()?,
                    pos: key_pos,
                });
            }
            let key = self.bytes()?;
            if sorted {
                match entries.last().map(|(k, _)| key.cmp(k)) {
                    None | Some(std::cmp::Ordering::Greater) => {}
                    Some(std::cmp::Ordering::Equal) => {
                        return Err(DecodeError::DuplicateKey { pos: key_pos });
                    }
                    Some(std::cmp::Ordering::Less) => {
                        // First out-of-order key: switch to hashing.
                        sorted = false;
                        seen.extend(entries.iter().map(|(k, _)| *k));
                        if !seen.insert(key) {
                            return Err(DecodeError::DuplicateKey { pos: key_pos });
                        }
                    }
                }
            } else if !seen.insert(key) {
                return Err(DecodeError::DuplicateKey { pos: key_pos });
            }
            let v = self.value(inner)?;
            entries.push((key, v));
        }
        self.pos += 1;
        Ok((entries, sorted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_scalars() {
        assert_eq!(decode(b"i42e").unwrap().as_int(), Some(42));
        assert_eq!(decode(b"i-7e").unwrap().as_int(), Some(-7));
        assert_eq!(decode(b"4:spam").unwrap().as_bytes(), Some(&b"spam"[..]));
        assert_eq!(decode(b"0:").unwrap().as_bytes(), Some(&b""[..]));
    }
    #[test]
    fn dict_get_and_raw_span() {
        let input = b"d4:infod6:lengthi3ee3:fooi1ee";
        let v = decode(input).unwrap();
        let info = v.as_dict().unwrap().get(b"info").unwrap();
        assert_eq!(info.raw(), b"d6:lengthi3ee");
    }
    #[test]
    fn rejects_malformed() {
        assert!(matches!(
            decode(b"i03e"),
            Err(DecodeError::LeadingZero { .. })
        ));
        assert!(matches!(
            decode(b"i-0e"),
            Err(DecodeError::NegativeZero { .. })
        ));
        assert!(matches!(
            decode(b"i9223372036854775808e"),
            Err(DecodeError::IntOverflow { .. })
        ));
        assert!(matches!(
            decode(b"5:abc"),
            Err(DecodeError::UnexpectedEof { .. })
        ));
        assert!(matches!(
            decode(b"99999999999999999999:x"),
            Err(DecodeError::LengthOverflow { .. })
        ));
        assert!(matches!(
            decode(b"d1:ai1e1:ai2ee"),
            Err(DecodeError::DuplicateKey { .. })
        ));
        assert!(matches!(
            decode(b"i1ei2e"),
            Err(DecodeError::TrailingData { pos: 3 })
        ));
        assert!(matches!(
            decode(b"di1ei2ee"),
            Err(DecodeError::InvalidByte { .. })
        ));
    }
    #[test]
    fn accepts_unsorted_keys() {
        assert!(decode(b"d1:bi1e1:ai2ee").is_ok());
    }
    #[test]
    fn rejects_unsorted_duplicates_and_edge_ints() {
        assert!(matches!(
            decode(b"d1:bi1e1:ai2e1:bi3ee"),
            Err(DecodeError::DuplicateKey { .. })
        ));
        assert_eq!(
            decode(b"i-9223372036854775808e").unwrap().as_int(),
            Some(i64::MIN)
        );
        assert_eq!(
            decode(b"i9223372036854775807e").unwrap().as_int(),
            Some(i64::MAX)
        );
        assert_eq!(decode(b"i0e").unwrap().as_int(), Some(0));
        assert!(decode(b"ie").is_err());
        assert!(decode(b"i-e").is_err());
        assert!(decode(b"").is_err());
        assert!(matches!(
            decode(b"01:a"),
            Err(DecodeError::LeadingZero { .. })
        ));
        assert!(decode(b"l").is_err());
        assert!(decode(b"18446744073709551615:x").is_err());
    }
    #[test]
    fn depth_limit() {
        let deep = [b"l".repeat(65), b"e".repeat(65)].concat();
        assert!(matches!(
            decode(&deep),
            Err(DecodeError::DepthExceeded { .. })
        ));
        let ok = [b"l".repeat(64), b"e".repeat(64)].concat();
        assert!(decode(&ok).is_ok());
    }
    #[test]
    fn many_unsorted_keys_is_fast() {
        let mut s = b"d".to_vec();
        for i in (0..100_000u32).rev() {
            let k = format!("{i:08}");
            s.extend(format!("8:{k}i0e").as_bytes());
        }
        s.push(b'e');
        let t = std::time::Instant::now();
        assert_eq!(decode(&s).unwrap().as_dict().unwrap().len(), 100_000);
        assert!(t.elapsed() < std::time::Duration::from_secs(2));
    }
    #[test]
    fn decode_prefix_reports_consumed() {
        let (v, n) = decode_prefix(b"d1:ai1ee<binary>", Limits::default()).unwrap();
        assert_eq!(n, 8);
        assert!(v.as_dict().is_some());
    }
    #[test]
    fn arbitrary_garbage_never_panics() {
        for len in 0..4u32 {
            let total = 6u32.pow(len);
            for n in 0..total {
                let (mut x, mut buf) = (n, Vec::new());
                for _ in 0..len {
                    buf.push(b"ild:e1"[(x % 6) as usize]);
                    x /= 6;
                }
                let _ = decode(&buf);
            }
        }
    }
}
