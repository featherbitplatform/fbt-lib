//! A reader's read-ahead window: the pieces ahead of its position and when
//! each is due.

use std::ops::Range;
use std::time::Duration;

use fbt_core::PieceLayout;
use tokio::time::Instant;

use super::ReadAhead;

/// Most pieces in one reader's window. The torrent's merged deadlines can
/// hold up to this many per reader; the number of readers is capped
/// separately.
pub(crate) const MAX_DEADLINE_PIECES: usize = 512;

/// The latest deadline: a delay beyond it (a tiny bitrate, a huge piece)
/// is clamped to it.
const FAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The pieces covering `[pos, min(pos + W, file.end))`, `W` being the
/// larger of `ra.bytes` and `ra.time` at the stream's rate, with their
/// deadlines, in index order and at most [`MAX_DEADLINE_PIECES`]. `pos` is
/// a torrent-global offset (one before the file counts from the file's
/// start). The piece holding `pos` is due `now`; a later piece when a
/// reader at the rate gets to its first byte. Empty for an empty file or
/// `pos` at or past its end.
///
/// Integer, saturating arithmetic only: no value of `ra` (validated or
/// not) or of `file` makes it panic.
pub(crate) fn window(
    file: Range<u64>,
    pos: u64,
    layout: &PieceLayout,
    ra: &ReadAhead,
    now: Instant,
) -> Vec<(u32, Instant)> {
    let pos = pos.max(file.start);
    let file_end = file.end.min(layout.total_len());
    if pos >= file_end {
        return Vec::new();
    }
    let time_ms = u64::try_from(ra.time.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    // Bytes per second, never zero.
    let rate = ra
        .bitrate
        .unwrap_or_else(|| ra.bytes.saturating_mul(1000) / time_ms)
        .max(1);
    let w = ra.bytes.max(rate.saturating_mul(time_ms) / 1000);
    let end = pos.saturating_add(w).min(file_end);
    let piece_len = u64::from(layout.piece_len());
    let (Ok(first), Ok(last)) = (
        u32::try_from(pos / piece_len),
        u32::try_from((end - 1) / piece_len),
    ) else {
        return Vec::new();
    };
    (first..=last)
        .take(MAX_DEADLINE_PIECES)
        .map(|i| {
            let ahead = layout.piece_offset(i).saturating_sub(pos);
            let delay = Duration::from_millis(ahead.saturating_mul(1000) / rate).min(FAR);
            // `delay <= FAR`: if this overflows, `now + FAR` would too.
            (i, now.checked_add(delay).unwrap_or(now))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    /// Where the test file starts in the torrent: inside piece 3.
    const START: u64 = 3 * MIB + 100;

    /// 1 MiB pieces; a 64 MiB file starting at [`START`].
    fn setup() -> (Range<u64>, PieceLayout) {
        let file = START..START + 64 * MIB;
        let layout = PieceLayout::new(file.end, 1024 * 1024).unwrap();
        (file, layout)
    }

    fn pieces(w: &[(u32, Instant)]) -> Vec<u32> {
        w.iter().map(|&(p, _)| p).collect()
    }

    #[test]
    fn default_window_covers_sixteen_mib() {
        let (file, layout) = setup();
        let now = Instant::now();
        let w = window(file, START, &layout, &ReadAhead::default(), now);
        // [3 MiB + 100, 19 MiB + 100): pieces 3..=19, the first straddles.
        assert_eq!(pieces(&w), (3..=19).collect::<Vec<_>>());
        assert_eq!(w[0].1, now);
        // Piece 4 starts 1 MiB - 100 bytes later, read at 16 MiB / 10 s.
        let rate = 16.0 * MIB as f64 / 10.0;
        let expect = Duration::from_secs_f64((MIB - 100) as f64 / rate);
        let got = w[1].1 - now;
        let diff = got.abs_diff(expect);
        assert!(diff <= Duration::from_millis(1), "{got:?} vs {expect:?}");
        // Deadlines never go backwards.
        assert!(w.windows(2).all(|p| p[0].1 <= p[1].1));
    }

    #[test]
    fn bitrate_extends_the_window() {
        let (file, layout) = setup();
        let now = Instant::now();
        let ra = ReadAhead::new(16 * MIB, Duration::from_secs(10), Some(4 * MIB));
        let w = window(file, START, &layout, &ra, now);
        // 40 MiB from 3 MiB + 100: pieces 3..=43.
        assert_eq!(pieces(&w), (3..=43).collect::<Vec<_>>());
        // At 4 MiB/s, piece 7 (4 MiB - 100 bytes on) is due in about 1 s.
        let got = w[4].1 - now;
        assert!(got.abs_diff(Duration::from_secs(1)) <= Duration::from_millis(1));
    }

    #[test]
    fn window_stops_at_the_file_end() {
        let (file, layout) = setup();
        let now = Instant::now();
        let w = window(
            file.clone(),
            file.end - 2 * MIB,
            &layout,
            &ReadAhead::default(),
            now,
        );
        assert_eq!(pieces(&w), vec![65, 66, 67]);
        let w = window(
            file.clone(),
            file.end - 1,
            &layout,
            &ReadAhead::default(),
            now,
        );
        assert_eq!(w, vec![(67, now)]);
    }

    #[test]
    fn empty_at_or_past_the_end() {
        let (file, layout) = setup();
        let now = Instant::now();
        let ra = ReadAhead::default();
        assert!(window(file.clone(), file.end, &layout, &ra, now).is_empty());
        assert!(window(file.clone(), u64::MAX, &layout, &ra, now).is_empty());
        // An empty file has no window.
        assert!(window(START..START, START, &layout, &ra, now).is_empty());
    }

    #[test]
    fn window_is_capped() {
        let layout = PieceLayout::new(128 * MIB, 16 * 1024).unwrap();
        let ra = ReadAhead::new(64 * MIB, Duration::from_secs(10), None);
        let now = Instant::now();
        let w = window(0..128 * MIB, 0, &layout, &ra, now);
        assert_eq!(w.len(), MAX_DEADLINE_PIECES);
        assert_eq!(
            pieces(&w),
            (0..MAX_DEADLINE_PIECES as u32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn huge_values_saturate() {
        let (file, layout) = setup();
        let now = Instant::now();
        let ra = ReadAhead::new(u64::MAX, Duration::from_secs(10), Some(u64::MAX));
        let w = window(file.clone(), START, &layout, &ra, now);
        // The whole rest of the file (65 pieces), all due now.
        assert_eq!(pieces(&w), (3..=67).collect::<Vec<_>>());
        assert!(w.iter().all(|&(_, t)| t == now));
        let ra = ReadAhead::new(u64::MAX, Duration::MAX, None);
        assert!(window(file, START, &layout, &ra, now).len() <= MAX_DEADLINE_PIECES);

        // 4 GiB pieces read at one byte per second: deadlines stop at FAR.
        let big = u64::from(u32::MAX);
        let layout = PieceLayout::new(3 * big, u32::MAX).unwrap();
        let ra = ReadAhead::new(u64::MAX, Duration::from_secs(10), Some(1));
        let w = window(0..3 * big, 1, &layout, &ra, now);
        assert_eq!(pieces(&w), vec![0, 1, 2]);
        assert_eq!(w[0].1, now);
        assert_eq!(w[1].1, now + FAR);
        assert_eq!(w[2].1, now + FAR);
    }
}
