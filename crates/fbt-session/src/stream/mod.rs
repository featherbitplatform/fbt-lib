//! Streaming while downloading: [`FileReader`] reads one file of a torrent
//! while it downloads, and [`ReadAhead`] sets how far ahead of a reader the
//! torrent fetches.
//!
//! A reader's position sets deadlines on the pieces of its read-ahead
//! window: the piece under the position is due now, each later one when a
//! player at the stream's bitrate would reach it.

pub(crate) mod reader;
pub(crate) mod window;

pub use reader::FileReader;

use std::time::Duration;

/// How far ahead of a reader its torrent fetches: at least
/// [`bytes`](Self::bytes), or [`time`](Self::time) of playback at
/// [`bitrate`](Self::bitrate) when that is more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadAhead {
    /// Least window, in bytes (16 MiB).
    pub bytes: u64,
    /// Time budget at `bitrate` (10 s). Non-zero.
    pub time: Duration,
    /// The stream's bytes per second, when the client knows it; `None` uses `bytes / time`.
    pub bitrate: Option<u64>,
}

impl ReadAhead {
    /// A read-ahead of at least `bytes`, or `time` at `bitrate` bytes per
    /// second when that is more.
    pub fn new(bytes: u64, time: Duration, bitrate: Option<u64>) -> Self {
        ReadAhead {
            bytes,
            time,
            bitrate,
        }
    }

    /// Checks the values the window cannot work with; the error names the
    /// field.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.time.is_zero() {
            return Err("read_ahead.time must be greater than zero".to_owned());
        }
        if self.bitrate == Some(0) {
            return Err("read_ahead.bitrate must be greater than zero".to_owned());
        }
        Ok(())
    }
}

impl Default for ReadAhead {
    fn default() -> Self {
        ReadAhead::new(16 * 1024 * 1024, Duration::from_secs(10), None)
    }
}
