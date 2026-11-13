//! The `Range` request header (RFC 9110 14.2), for the file streams.
//!
//! Only what a media player sends is honoured: one range in `bytes`.
//! Anything else is ignored and the whole file is served, as RFC 9110
//! allows a server to do with a range it does not support.

/// What a `Range` header asks of a file of known length.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum RangeOutcome {
    /// Serve the whole file (`200`): no usable range was asked for.
    Full,
    /// Serve `start..=end_inclusive` (`206`); always
    /// `start <= end_inclusive < len`.
    Partial {
        /// The first byte.
        start: u64,
        /// The last byte, included.
        end_inclusive: u64,
    },
    /// The range starts past the end (`416`).
    Unsatisfiable,
}

/// Parses the value of a `Range` header for a file of `len` bytes.
///
/// Only the unit `bytes` (in any case) with exactly one range-spec
/// (`a-b`, `a-` or `-n`) is a range; whitespace around the spec is
/// tolerated. Anything else (another unit, several ranges, a malformed or
/// overflowing number, `a > b`) is [`RangeOutcome::Full`], and so is every
/// range of an empty file. `a >= len` and `-0` are
/// [`RangeOutcome::Unsatisfiable`]; `b` is clamped to `len - 1`, and a
/// suffix longer than the file is the whole file.
///
/// The header is untrusted: this never panics, whatever the bytes.
#[doc(hidden)]
pub fn parse_range(header: &[u8], len: u64) -> RangeOutcome {
    if len == 0 {
        return RangeOutcome::Full;
    }
    let Some(spec) = spec(header) else {
        return RangeOutcome::Full;
    };
    let Some((first, last)) = split_once(spec, b'-') else {
        return RangeOutcome::Full;
    };
    let last_byte = len - 1;
    if first.is_empty() {
        // `-n`: the last `n` bytes.
        return match number(last) {
            None => RangeOutcome::Full,
            Some(0) => RangeOutcome::Unsatisfiable,
            Some(n) => RangeOutcome::Partial {
                start: len.saturating_sub(n),
                end_inclusive: last_byte,
            },
        };
    }
    let Some(start) = number(first) else {
        return RangeOutcome::Full;
    };
    let end = if last.is_empty() {
        None
    } else {
        match number(last) {
            Some(end) => Some(end),
            None => return RangeOutcome::Full,
        }
    };
    if end.is_some_and(|end| start > end) {
        return RangeOutcome::Full;
    }
    if start >= len {
        return RangeOutcome::Unsatisfiable;
    }
    RangeOutcome::Partial {
        start,
        end_inclusive: end.map_or(last_byte, |end| end.min(last_byte)),
    }
}

/// The single range-spec after `bytes=`, trimmed; `None` for another unit
/// or a list of several.
fn spec(header: &[u8]) -> Option<&[u8]> {
    let (unit, specs) = split_once(header, b'=')?;
    if !unit.eq_ignore_ascii_case(b"bytes") || specs.contains(&b',') {
        return None;
    }
    Some(specs.trim_ascii())
}

/// `s` split around the first `sep`.
fn split_once(s: &[u8], sep: u8) -> Option<(&[u8], &[u8])> {
    let at = s.iter().position(|&b| b == sep)?;
    Some((s.get(..at)?, s.get(at + 1..)?))
}

/// A non-empty run of ASCII digits as a `u64`; `None` otherwise or on
/// overflow.
fn number(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    s.iter().try_fold(0u64, |acc, &b| {
        let digit = char::from(b).to_digit(10)?;
        acc.checked_mul(10)?.checked_add(u64::from(digit))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partial(start: u64, end_inclusive: u64) -> RangeOutcome {
        RangeOutcome::Partial {
            start,
            end_inclusive,
        }
    }

    #[test]
    fn range_parse_table() {
        use RangeOutcome::{Full, Unsatisfiable};
        let cases: Vec<(&str, u64, RangeOutcome)> = vec![
            ("bytes=0-", 1000, partial(0, 999)),
            ("bytes=0-0", 1000, partial(0, 0)),
            ("Bytes=0-0", 1000, partial(0, 0)),
            ("bytes=100-199", 1000, partial(100, 199)),
            ("bytes=900-5000", 1000, partial(900, 999)),
            ("bytes=-500", 1000, partial(500, 999)),
            ("bytes=-5000", 1000, partial(0, 999)),
            ("bytes=1000-", 1000, Unsatisfiable),
            ("bytes=-0", 1000, Unsatisfiable),
            ("bytes=5-2", 1000, Full),
            ("bytes=0-1,5-6", 1000, Full),
            ("items=0-1", 1000, Full),
            ("bytes=", 1000, Full),
            ("bytes=abc", 1000, Full),
            ("bytes=99999999999999999999999-", 1000, Full),
            (" bytes = 1-2 ", 1000, Full),
            ("bytes= 1-2", 1000, partial(1, 2)),
            ("bytes=0-", 0, Full),
        ];
        for (header, len, want) in cases {
            assert_eq!(
                parse_range(header.as_bytes(), len),
                want,
                "{header:?} len {len}"
            );
        }
    }

    #[test]
    fn hostile_headers_never_panic() {
        for header in [
            &b""[..],
            b"=",
            b"-",
            b"bytes=-",
            b"bytes=--1",
            b"bytes=1--2",
            b"bytes=+1-2",
            b"bytes=18446744073709551615-",
            b"bytes=-18446744073709551615",
            b"bytes=0-18446744073709551616",
            b"\xff\xfe=0-1",
            b"bytes=\xff-1",
        ] {
            for len in [0, 1, u64::MAX] {
                if let RangeOutcome::Partial {
                    start,
                    end_inclusive,
                } = parse_range(header, len)
                {
                    assert!(start <= end_inclusive && end_inclusive < len, "{header:?}");
                }
            }
        }
        assert_eq!(
            parse_range(b"bytes=-18446744073709551615", u64::MAX),
            partial(0, u64::MAX - 1)
        );
    }
}
