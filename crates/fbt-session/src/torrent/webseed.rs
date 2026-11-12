//! Web seeds (BEP 19, `url-list`): HTTP servers holding the torrent's
//! files, each fetched from as a pseudo-peer.
//!
//! Each URL gets a [`PeerKey`] of its own and counts as a `HaveAll` peer in
//! the picker while it is not retired. An idle seed takes a run of whole,
//! consecutive, unstarted pieces ([`PiecePicker::pick_run`], up to
//! [`WEB_SEED_REQUEST_MAX`] and what the cache budget has room for), so a
//! request is one byte range, split only at file boundaries; the ranges
//! are fetched one after the other through
//! [`TrackerClient::fetch_range`](fbt_tracker::TrackerClient::fetch_range)
//! and the blocks come back as a [`Job::WebSeed`], fed to
//! [`Actor::accept_block`] like a peer's. One request per seed is in
//! flight at a time, only while the torrent is active and its download
//! limits have tokens; the bytes are charged to them afterwards.
//!
//! A failed request releases its blocks and backs the seed off, 30 s
//! doubling to 30 minutes; a 4xx other than 408 and 429 (or a URL that
//! can never work) retires it, as does smart ban when its data fails a
//! hash check. A retired seed stays listed but is never asked again (until
//! added again by hand). Every failure is an
//! [`Event::WebSeedError`](crate::Event::WebSeedError).
//!
//! Parole: a web seed only ever takes pieces nobody started and holds
//! every block of them, so its pieces already have a single contributor
//! (bar end-game duplicates from peers, which arrive first or are not
//! accepted from the seed). Parole would change nothing for it, so the
//! picker never puts a web seed on parole; smart ban still charges and
//! bans it as any sender.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fbt_bencode::{Limits, decode_with};
use fbt_core::{BlockRef, FileLayout, PieceLayout};
use fbt_metainfo::Metainfo;
use fbt_tracker::{RangePace, TrackerClient, TrackerError, url_encode_bytes};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::debug;

use super::actor::{Actor, Phase};
use super::data::Job;
use super::smartban::BlockSource;
use crate::bandwidth::Throttle;
use crate::error::SessionError;
use crate::event::Event;
use crate::picker::{PeerKey, PeerPieces};

/// Most bytes one web seed request covers (whole pieces; a single piece
/// larger than this is still fetched whole).
pub(super) const WEB_SEED_REQUEST_MAX: u64 = 4 << 20;
/// The first backoff after a failed request; it doubles per failure.
const BACKOFF_BASE: Duration = Duration::from_secs(30);
/// The longest backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);
/// With a download limit, a request covers about this many seconds of it
/// (at least one piece, at most [`WEB_SEED_REQUEST_MAX`]).
const LIMITED_REQUEST_SECS: u64 = 4;
/// Longest URL accepted (as `fbt_metainfo` keeps from a torrent).
const MAX_URL_LEN: usize = 2048;

/// What one web seed is doing.
#[derive(Debug)]
pub(super) enum SeedState {
    /// Ready for the next request.
    Idle,
    /// A request is in flight; dropping or sending on `cancel` stops it,
    /// and `None` means it was cancelled (its result is ignored).
    Busy { cancel: Option<oneshot::Sender<()>> },
    /// Waiting after a failure until then.
    Backoff(Instant),
    /// Never asked again.
    Retired,
}

/// One web seed.
#[derive(Debug)]
pub(super) struct WebSeed {
    pub url: String,
    pub key: PeerKey,
    pub state: SeedState,
    /// Failed requests in a row.
    pub failures: u32,
    /// Where its next run starts looking (just past its last one).
    pub cursor: u32,
    /// The id of its latest request: a job carrying another one was
    /// superseded (the seed was retired and revived meanwhile) and is
    /// ignored.
    pub request: u64,
}

/// A torrent's web seeds, at most [`fbt_metainfo::MAX_WEB_SEEDS`].
#[derive(Debug, Default)]
pub(super) struct WebSeeds {
    seeds: Vec<WebSeed>,
    /// The torrent's names for URLs, read once the metadata is known.
    names: Option<Arc<RawNames>>,
}

impl WebSeeds {
    fn get_mut(&mut self, key: PeerKey) -> Option<&mut WebSeed> {
        self.seeds.iter_mut().find(|s| s.key == key)
    }

    /// Every URL, retired ones included, in the order added.
    pub(super) fn urls(&self) -> Vec<String> {
        self.seeds.iter().map(|s| s.url.clone()).collect()
    }

    /// Seeds not retired.
    pub(super) fn num_active(&self) -> u32 {
        let n = self
            .seeds
            .iter()
            .filter(|s| !matches!(s.state, SeedState::Retired))
            .count();
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    /// The keys of the seeds not retired.
    pub(super) fn active_keys(&self) -> impl Iterator<Item = PeerKey> + '_ {
        self.seeds
            .iter()
            .filter(|s| !matches!(s.state, SeedState::Retired))
            .map(|s| s.key)
    }
}

/// Why a request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchError {
    pub message: String,
    /// The seed cannot serve this torrent: retire it.
    pub retire: bool,
    /// The request was cancelled (pause, queue, removal): no failure.
    pub cancelled: bool,
}

impl FetchError {
    fn from_tracker(e: &TrackerError) -> Self {
        let retire = match e {
            TrackerError::HttpStatus(code) => {
                (400..500).contains(code) && *code != 408 && *code != 429
            }
            TrackerError::InvalidUrl(_)
            | TrackerError::UnsupportedScheme(_)
            | TrackerError::TlsDisabled => true,
            _ => false,
        };
        FetchError {
            message: e.to_string(),
            retire,
            cancelled: false,
        }
    }

    fn cancelled() -> Self {
        FetchError {
            message: "cancelled".to_owned(),
            retire: false,
            cancelled: true,
        }
    }
}

/// One part of a run: a byte range of a file on the server, or the zeros
/// of a pad file (BEP 47), which no server holds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Http { url: String, range: Range<u64> },
    Zeros(u64),
}

/// The backoff after `failures` failed requests in a row (at least one):
/// 30 s, doubling, at most 30 minutes.
pub(super) fn backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    BACKOFF_BASE
        .saturating_mul(1u32 << doublings)
        .min(BACKOFF_MAX)
}

/// The torrent's names as its info dictionary spells them, byte for
/// byte: a BEP 19 server holds the files under those, not under the
/// sanitised, collision-renamed paths of [`Metainfo`]'s files.
#[derive(Debug)]
pub(super) struct RawNames {
    /// The `name` entry.
    name: Vec<u8>,
    /// Each file's `path` components, in torrent order; `None` for a
    /// single-file torrent.
    files: Option<Vec<Vec<Vec<u8>>>>,
}

impl RawNames {
    /// Read from `meta`'s raw info dictionary; should it not match the
    /// parsed files (it always does for a parsed torrent), from the parsed
    /// names instead. A file with a raw component that is empty, `.` or
    /// `..` (which would change the URL's path rather than name a file) is
    /// named by its parsed path instead, and so is the torrent's name.
    pub(super) fn of(meta: &Metainfo) -> RawNames {
        let parsed = Self::parsed(meta);
        let Some(mut raw) = Self::parse(meta) else {
            return parsed;
        };
        if !plain_component(&raw.name) {
            raw.name.clone_from(&parsed.name);
        }
        if let (Some(raw_files), Some(parsed_files)) = (raw.files.as_mut(), parsed.files.as_ref()) {
            for (r, p) in raw_files.iter_mut().zip(parsed_files) {
                if r.is_empty() || !r.iter().all(|c| plain_component(c)) {
                    r.clone_from(p);
                }
            }
        }
        raw
    }

    fn parse(meta: &Metainfo) -> Option<RawNames> {
        let root =
            decode_with(meta.raw_info(), Limits::new(RAW_INFO_DEPTH, RAW_INFO_ITEMS)).ok()?;
        let info = root.as_dict()?;
        let name = info.get(b"name")?.as_bytes()?.to_vec();
        let files = match info.get(b"files") {
            None => None,
            Some(list) => {
                let list = list.as_list()?;
                let mut out = Vec::with_capacity(list.len());
                for item in list {
                    let comps = item.as_dict()?.get(b"path")?.as_list()?;
                    let path = comps
                        .iter()
                        .map(|c| c.as_bytes().map(<[u8]>::to_vec))
                        .collect::<Option<Vec<_>>>()?;
                    out.push(path);
                }
                Some(out)
            }
        };
        let count = files.as_ref().map_or(1, Vec::len);
        (count == meta.info.files.len() && files.is_none() == meta.info.single_file)
            .then_some(RawNames { name, files })
    }

    fn parsed(meta: &Metainfo) -> RawNames {
        let files = (!meta.info.single_file).then(|| {
            meta.info
                .files
                .iter()
                .map(|f| {
                    // The first component is the torrent's name.
                    f.path
                        .components()
                        .iter()
                        .skip(1)
                        .map(|c| c.as_bytes().to_vec())
                        .collect()
                })
                .collect()
        });
        RawNames {
            name: meta.info.name.as_bytes().to_vec(),
            files,
        }
    }
}

/// A path component that names something: not empty, `.` or `..`.
fn plain_component(c: &[u8]) -> bool {
    !matches!(c, b"" | b"." | b"..")
}

/// Nesting cap when decoding the raw info dictionary (as `fbt_metainfo`).
const RAW_INFO_DEPTH: usize = 32;
/// Item cap when decoding the raw info dictionary (as `fbt_metainfo`).
const RAW_INFO_ITEMS: usize = 4_194_304;

/// The URL of file `file_index` on the web seed `base` (BEP 19), from the
/// torrent's own names (`names`): a single-file torrent is `base` itself,
/// or `base` + its name when `base` ends in `/`; a multi-file torrent's
/// file is below `base` as a directory, by the torrent's name and the
/// file's path. Each component is percent-encoded byte by byte, so names
/// that are not UTF-8 work too.
pub(super) fn url_for(base: &str, names: &RawNames, file_index: usize) -> String {
    let Some(files) = &names.files else {
        return if base.ends_with('/') {
            format!("{base}{}", url_encode_bytes(&names.name))
        } else {
            base.to_owned()
        };
    };
    let mut out = base.to_owned();
    if !out.ends_with('/') {
        out.push('/');
    }
    out.push_str(&url_encode_bytes(&names.name));
    for c in files.get(file_index).map(Vec::as_slice).unwrap_or_default() {
        out.push('/');
        out.push_str(&url_encode_bytes(c));
    }
    out
}

/// The byte ranges per file that `blocks` (consecutive, in order) cover:
/// `(file index, range within the file)`, in torrent order, zero-length
/// files left out.
pub(super) fn segments(
    pieces: &PieceLayout,
    files: &FileLayout,
    blocks: &[BlockRef],
) -> Vec<(usize, Range<u64>)> {
    let Some(span) = span_of(pieces, blocks) else {
        return Vec::new();
    };
    files
        .map(span.start, span.end - span.start)
        .into_iter()
        .map(|s| (s.file, s.file_offset..s.file_offset + s.len))
        .collect()
}

/// The torrent-global byte range of `blocks`, first to last.
fn span_of(pieces: &PieceLayout, blocks: &[BlockRef]) -> Option<Range<u64>> {
    let first = blocks.first()?;
    let last = blocks.last()?;
    let start = pieces.piece_offset(first.piece) + u64::from(first.offset);
    let end = pieces.piece_offset(last.piece) + u64::from(last.offset) + u64::from(last.len);
    (end > start).then_some(start..end)
}

/// Fetches `parts` one after the other into one buffer of `total` bytes
/// and cuts it at `cuts` (offset and length of each block).
/// The body of each range is read at `pace`'s rate (the torrent's and the
/// session's download limits), so the limits hold while it arrives.
async fn fetch(
    tracker: TrackerClient,
    pace: Throttle,
    parts: Vec<Part>,
    total: usize,
    cuts: Vec<(usize, usize)>,
) -> Result<Vec<Bytes>, FetchError> {
    let mut buf = BytesMut::with_capacity(total);
    for part in parts {
        match part {
            Part::Http { url, range } => {
                let got = tracker
                    .fetch_range_paced(&url, range, &pace)
                    .await
                    .map_err(|e| FetchError::from_tracker(&e))?;
                buf.extend_from_slice(&got);
            }
            Part::Zeros(n) => {
                let n = usize::try_from(n).unwrap_or(usize::MAX);
                buf.resize(buf.len().saturating_add(n), 0);
            }
        }
    }
    let buf = buf.freeze();
    cuts.into_iter()
        .map(|(at, len)| {
            at.checked_add(len)
                .filter(|&end| end <= buf.len())
                .map(|end| buf.slice(at..end))
                .ok_or_else(|| FetchError {
                    message: "short run".to_owned(),
                    retire: false,
                    cancelled: false,
                })
        })
        .collect()
}

impl RangePace for Throttle {
    fn ready(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.wait_ready())
    }

    fn charge(&self, n: u64) {
        Throttle::charge(self, n);
    }
}

/// An `http://` or `https://` URL (scheme in any case) with something
/// after the scheme, within [`MAX_URL_LEN`].
fn is_web_url(url: &str) -> bool {
    url.len() <= MAX_URL_LEN
        && ["http://", "https://"].iter().any(|scheme| {
            url.get(..scheme.len())
                .is_some_and(|h| h.eq_ignore_ascii_case(scheme))
                && url.len() > scheme.len()
        })
}

impl Actor {
    /// Adds the torrent's initial web seeds (the `.torrent`'s `url-list` or
    /// the magnet's `ws`).
    pub(super) fn init_web_seeds(&mut self, urls: Vec<String>) {
        for url in urls {
            let _ = self.add_web_seed(url);
        }
    }

    /// Adds a web seed, or revives a retired one with the same URL; an
    /// active one already there is left alone.
    pub(super) fn add_web_seed(&mut self, url: String) -> Result<(), SessionError> {
        if !is_web_url(&url) {
            return Err(SessionError::InvalidUrl(url));
        }
        if let Some(seed) = self.web_seeds.seeds.iter_mut().find(|s| s.url == url) {
            if matches!(seed.state, SeedState::Retired) {
                seed.state = SeedState::Idle;
                seed.failures = 0;
                let key = seed.key;
                self.count_web_seed(key);
                self.web_seed_tick();
            }
            return Ok(());
        }
        if self.web_seeds.seeds.len() >= fbt_metainfo::MAX_WEB_SEEDS {
            return Err(SessionError::TooManyWebSeeds);
        }
        let key = self.new_key();
        self.web_seeds.seeds.push(WebSeed {
            url,
            key,
            state: SeedState::Idle,
            failures: 0,
            cursor: 0,
            request: 0,
        });
        self.count_web_seed(key);
        self.web_seed_tick();
        Ok(())
    }

    /// Removes the web seed `url` (nothing if there is none); a request in
    /// flight is cancelled.
    pub(super) fn remove_web_seed(&mut self, url: &str) {
        let Some(at) = self.web_seeds.seeds.iter().position(|s| s.url == url) else {
            return;
        };
        let seed = self.web_seeds.seeds.remove(at);
        if !matches!(seed.state, SeedState::Retired) {
            self.uncount_web_seed(seed.key);
        }
        // Dropping a busy seed's sender cancels its request.
    }

    /// The web seed `key` is a `HaveAll` peer of the current picker.
    fn count_web_seed(&mut self, key: PeerKey) {
        if let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) {
            debug!(key = key.0, "web seed counted");
            picker.add_peer_pieces(PeerPieces::All);
        }
    }

    /// The web seed `key` leaves the current picker: its blocks are
    /// released and its availability taken off.
    fn uncount_web_seed(&mut self, key: PeerKey) {
        if let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) {
            picker.release_peer(key);
            picker.remove_peer_pieces(PeerPieces::All);
        }
        self.drop_forfeited();
    }

    /// Counts every active web seed into a new picker (after a check).
    pub(super) fn count_web_seeds(&mut self) {
        let keys: Vec<PeerKey> = self.web_seeds.active_keys().collect();
        for key in keys {
            self.count_web_seed(key);
        }
    }

    /// Cancels every web seed request in flight (the torrent stops); their
    /// blocks are released when the cancelled jobs come back.
    pub(super) fn cancel_web_seeds(&mut self) {
        for seed in &mut self.web_seeds.seeds {
            if let SeedState::Busy { cancel } = &mut seed.state
                && let Some(tx) = cancel.take()
            {
                let _ = tx.send(());
            }
        }
    }

    /// Retires the web seed `key` with `message`, reported with
    /// `retired: true`.
    pub(super) fn retire_web_seed(&mut self, key: PeerKey, message: String) {
        let Some(seed) = self.web_seeds.get_mut(key) else {
            return;
        };
        if matches!(seed.state, SeedState::Retired) {
            return;
        }
        // A request in flight is cancelled as the sender drops.
        seed.state = SeedState::Retired;
        let url = seed.url.clone();
        debug!(torrent = %self.name(), %url, %message, "web seed retired");
        self.uncount_web_seed(key);
        // Its status shows it gone by the time the event is seen.
        self.publish();
        self.emit(Event::WebSeedError {
            torrent: self.ih,
            url,
            message,
            retired: true,
        });
    }

    /// Ends the backoffs that are over and starts a request on every idle
    /// web seed, while the torrent is active, wants pieces and its download
    /// limits have tokens.
    pub(super) fn web_seed_tick(&mut self) {
        if self.web_seeds.seeds.is_empty() {
            return;
        }
        let now = Instant::now();
        for seed in &mut self.web_seeds.seeds {
            if let SeedState::Backoff(until) = seed.state
                && now >= until
            {
                seed.state = SeedState::Idle;
            }
        }
        if !self.active() || self.web_seed_throttle.ready_at(now).is_some() {
            return;
        }
        let idle: Vec<PeerKey> = self
            .web_seeds
            .seeds
            .iter()
            .filter(|s| matches!(s.state, SeedState::Idle))
            .map(|s| s.key)
            .collect();
        for key in idle {
            if !self.start_web_seed(key) {
                break;
            }
        }
    }

    /// Starts one request on the idle web seed `key`; false when there was
    /// nothing to fetch or no room for it.
    fn start_web_seed(&mut self, key: PeerKey) -> bool {
        let generation = self.generation;
        let tracker = self.shared.tracker.clone();
        let pace = self.web_seed_throttle.clone();
        let limit = pace.min_rate();
        let Some(l) = self.loaded.as_mut() else {
            return false;
        };
        let names = Arc::clone(
            self.web_seeds
                .names
                .get_or_insert_with(|| Arc::new(RawNames::of(&l.meta))),
        );
        let Some(picker) = l.picker.as_mut() else {
            return false;
        };
        let Some(seed) = self.web_seeds.seeds.iter_mut().find(|s| s.key == key) else {
            return false;
        };
        if picker.is_finished() {
            return false;
        }
        let piece_len = u64::from(l.layout.piece_len()).max(1);
        let room = self.cache.partial_room(piece_len) as u64;
        if room == 0 {
            return false;
        }
        let mut max = WEB_SEED_REQUEST_MAX.min(room.saturating_mul(piece_len));
        if let Some(rate) = limit {
            // About four seconds at the lowest download limit: the run's
            // pieces are its alone while it trickles in.
            let by_rate = rate
                .saturating_mul(LIMITED_REQUEST_SECS)
                .max(piece_len)
                .min(WEB_SEED_REQUEST_MAX);
            max = max.min(by_rate);
        }
        let mut blocks = picker.pick_run(key, seed.cursor, max);
        if blocks.is_empty() && seed.cursor > 0 {
            blocks = picker.pick_run(key, 0, max);
        }
        // Each piece reserves its buffer from the cache budget; from the
        // first that cannot, the run goes back to the picker.
        let mut refused: Option<u32> = None;
        let mut last = None;
        for b in &blocks {
            if last == Some(b.piece) {
                continue;
            }
            last = Some(b.piece);
            let size = u64::from(l.layout.piece_size(b.piece));
            if !self.cache.has_partial(b.piece) && !self.cache.reserve_partial(b.piece, size) {
                refused = Some(b.piece);
                break;
            }
        }
        if let Some(from) = refused {
            blocks.retain(|b| {
                let keep = b.piece < from;
                if !keep {
                    picker.release(key, *b);
                }
                keep
            });
        }
        let Some(span) = span_of(&l.layout, &blocks) else {
            return false;
        };
        seed.cursor = blocks.last().map_or(0, |b| b.piece.saturating_add(1));
        let file_layout = l.meta.file_layout();
        let parts: Vec<Part> = segments(&l.layout, &file_layout, &blocks)
            .into_iter()
            .map(|(file, range)| {
                if l.meta.info.files.get(file).is_some_and(|f| f.pad) {
                    Part::Zeros(range.end - range.start)
                } else {
                    Part::Http {
                        url: url_for(&seed.url, &names, file),
                        range,
                    }
                }
            })
            .collect();
        let total = usize::try_from(span.end - span.start).unwrap_or(usize::MAX);
        let cuts: Vec<(usize, usize)> = blocks
            .iter()
            .map(|b| {
                let at = l.layout.piece_offset(b.piece) + u64::from(b.offset) - span.start;
                (usize::try_from(at).unwrap_or(usize::MAX), b.len as usize)
            })
            .collect();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        seed.state = SeedState::Busy {
            cancel: Some(cancel_tx),
        };
        seed.request = seed.request.wrapping_add(1);
        let request = seed.request;
        debug!(url = %seed.url, first = blocks.first().map(|b| b.piece), bytes = total, "web seed request");
        self.jobs.spawn(async move {
            let result = tokio::select! {
                r = fetch(tracker, pace, parts, total, cuts) => r,
                _ = cancel_rx => Err(FetchError::cancelled()),
            };
            Job::WebSeed {
                key,
                request,
                generation,
                blocks,
                result,
            }
        });
        true
    }

    /// A web seed request returned: its blocks are accepted, or released
    /// with a backoff or retirement; a request cancelled, stale or for a
    /// torrent no longer active only releases them. A request superseded
    /// by a newer one of the same seed (retired, then revived, meanwhile)
    /// is ignored: its blocks went back with the retirement, and the newer
    /// request may hold them again.
    pub(super) fn on_web_seed_job(
        &mut self,
        key: PeerKey,
        request: u64,
        generation: u64,
        blocks: Vec<BlockRef>,
        result: Result<Vec<Bytes>, FetchError>,
    ) {
        let current = generation == self.generation && matches!(self.phase, Phase::Ready);
        let active = self.active();
        let mut live = false;
        if self
            .web_seeds
            .get_mut(key)
            .is_some_and(|seed| seed.request != request)
        {
            return;
        }
        if let Some(seed) = self.web_seeds.get_mut(key)
            && let SeedState::Busy { cancel } = &seed.state
        {
            live = cancel.is_some();
            seed.state = SeedState::Idle;
        }
        let result = match result {
            Ok(data) if data.len() != blocks.len() => Err(FetchError {
                message: "short run".to_owned(),
                retire: false,
                cancelled: false,
            }),
            r => r,
        };
        match result {
            Ok(data) if live && current && active => {
                if let Some(seed) = self.web_seeds.get_mut(key) {
                    seed.failures = 0;
                }
                for (block, d) in blocks.into_iter().zip(data) {
                    self.accept_block(key, block, d, BlockSource::WebSeed(key), generation);
                }
            }
            result => {
                if current
                    && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
                {
                    for b in &blocks {
                        picker.release(key, *b);
                    }
                }
                if let Err(e) = result
                    && live
                    && current
                    && active
                    && !e.cancelled
                {
                    self.web_seed_failed(key, e);
                }
            }
        }
        self.web_seed_tick();
    }

    /// A request of the web seed `key` failed: retired, or backed off.
    fn web_seed_failed(&mut self, key: PeerKey, e: FetchError) {
        if e.retire {
            self.retire_web_seed(key, e.message);
            return;
        }
        let Some(seed) = self.web_seeds.get_mut(key) else {
            return;
        };
        seed.failures = seed.failures.saturating_add(1);
        let wait = backoff(seed.failures);
        seed.state = SeedState::Backoff(Instant::now() + wait);
        let url = seed.url.clone();
        debug!(torrent = %self.name(), %url, message = %e.message, ?wait, "web seed failed");
        self.emit(Event::WebSeedError {
            torrent: self.ih,
            url,
            message: e.message,
            retired: false,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbt_bencode::{DictBuilder, OwnedValue, encode};

    fn multi(files: &[(&str, i64)], piece_len: i64) -> Metainfo {
        let list = files
            .iter()
            .map(|(name, len)| {
                let mut f = DictBuilder::new();
                f.insert(b"length", *len);
                f.insert(
                    b"path",
                    OwnedValue::List(name.split('/').map(OwnedValue::from).collect::<Vec<_>>()),
                );
                f.build()
            })
            .collect();
        let total: i64 = files.iter().map(|f| f.1).sum();
        let n = (total + piece_len - 1) / piece_len;
        let mut info = DictBuilder::new();
        info.insert(b"files", OwnedValue::List(list));
        info.insert(b"name", "my torrent");
        info.insert(b"piece length", piece_len);
        info.insert(b"pieces", vec![0u8; 20 * n as usize]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        Metainfo::from_bytes(&encode(&root.build())).unwrap()
    }

    fn single(name: &str) -> Metainfo {
        let mut info = DictBuilder::new();
        info.insert(b"length", 10i64);
        info.insert(b"name", name);
        info.insert(b"piece length", 16_384i64);
        info.insert(b"pieces", vec![0u8; 20]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        Metainfo::from_bytes(&encode(&root.build())).unwrap()
    }

    #[test]
    fn urls_follow_bep_19() {
        let s = RawNames::of(&single("a b.iso"));
        assert_eq!(url_for("http://h/x/", &s, 0), "http://h/x/a%20b.iso");
        assert_eq!(url_for("http://h/file.iso", &s, 0), "http://h/file.iso");
        let m = RawNames::of(&multi(&[("d/a#b", 5), ("c", 5)], 16_384));
        assert_eq!(url_for("http://h/", &m, 0), "http://h/my%20torrent/d/a%23b");
        assert_eq!(
            url_for("http://h/root", &m, 1),
            "http://h/root/my%20torrent/c"
        );
    }

    /// Names the parser sanitises or renames are asked for as the torrent
    /// spells them.
    #[test]
    fn urls_use_the_torrents_own_names() {
        let s = single("What?: live.iso");
        assert_eq!(s.info.name, "What__ live.iso", "sanitised by the parser");
        assert_eq!(
            url_for("http://h/", &RawNames::of(&s), 0),
            "http://h/What%3F%3A%20live.iso"
        );
        let files: Vec<(Vec<u8>, i64)> = vec![
            (b"A.txt".to_vec(), 5),
            (b"a.txt".to_vec(), 5),
            (b"\xff.bin".to_vec(), 5),
            (b"con".to_vec(), 5),
        ];
        let list = files
            .iter()
            .map(|(name, len)| {
                let mut f = DictBuilder::new();
                f.insert(b"length", *len);
                f.insert(
                    b"path",
                    OwnedValue::List(vec![OwnedValue::from(name.clone())]),
                );
                f.build()
            })
            .collect();
        let mut info = DictBuilder::new();
        info.insert(b"files", OwnedValue::List(list));
        info.insert(b"name", "t:1");
        info.insert(b"piece length", 16_384i64);
        info.insert(b"pieces", vec![0u8; 20]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        let m = Metainfo::from_bytes(&encode(&root.build())).unwrap();
        // The parser renamed the second file and sanitised the rest.
        assert_ne!(m.info.files[1].path.components()[1], "a.txt");
        let names = RawNames::of(&m);
        let urls: Vec<String> = (0..4).map(|i| url_for("http://h/", &names, i)).collect();
        assert_eq!(
            urls,
            [
                "http://h/t%3A1/A.txt",
                "http://h/t%3A1/a.txt",
                "http://h/t%3A1/%FF.bin",
                "http://h/t%3A1/con",
            ]
        );
    }

    /// Raw components that would move the URL's path (`..`, `.`, empty)
    /// never reach it: such a file is named by its sanitised path.
    #[test]
    fn dot_components_fall_back_to_the_sanitised_path() {
        let paths: [&[&[u8]]; 4] = [&[b"a", b"..", b"b"], &[b".", b"c"], &[b"", b"d"], &[b"ok?"]];
        let list = paths
            .iter()
            .map(|comps| {
                let mut f = DictBuilder::new();
                f.insert(b"length", 5i64);
                f.insert(
                    b"path",
                    OwnedValue::List(comps.iter().map(|c| OwnedValue::from(*c)).collect()),
                );
                f.build()
            })
            .collect();
        let mut info = DictBuilder::new();
        info.insert(b"files", OwnedValue::List(list));
        info.insert(b"name", "t");
        info.insert(b"piece length", 16_384i64);
        info.insert(b"pieces", vec![0u8; 20]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        let m = Metainfo::from_bytes(&encode(&root.build())).unwrap();
        let names = RawNames::of(&m);
        for i in 0..3 {
            let url = url_for("http://h/", &names, i);
            let sanitised: Vec<String> = m.info.files[i].path.components()[1..]
                .iter()
                .map(|c| url_encode_bytes(c.as_bytes()))
                .collect();
            assert_eq!(url, format!("http://h/t/{}", sanitised.join("/")));
            let path = url.trim_start_matches("http://h/");
            assert!(
                path.split('/').all(|c| plain_component(c.as_bytes())),
                "{url}"
            );
        }
        // A plain raw name is still used as the torrent spells it.
        assert_eq!(url_for("http://h/", &names, 3), "http://h/t/ok%3F");
        // A torrent name of `..` is replaced too.
        let mut info = DictBuilder::new();
        info.insert(b"length", 5i64);
        info.insert(b"name", "..");
        info.insert(b"piece length", 16_384i64);
        info.insert(b"pieces", vec![0u8; 20]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        let s = Metainfo::from_bytes(&encode(&root.build())).unwrap();
        let url = url_for("http://h/", &RawNames::of(&s), 0);
        assert!(!url.ends_with("/.."), "{url}");
        assert_eq!(
            url,
            format!("http://h/{}", url_encode_bytes(s.info.name.as_bytes()))
        );
    }

    #[test]
    fn segments_split_at_file_boundaries() {
        let m = multi(&[("a", 20_000), ("b", 0), ("c", 30_000)], 16_384);
        let pieces = m.piece_layout();
        let files = m.file_layout();
        let blocks: Vec<BlockRef> = (0..pieces.num_pieces())
            .flat_map(|p| (0..pieces.blocks_in_piece(p)).map(move |i| (p, i)))
            .map(|(p, i)| pieces.block(p, i))
            .collect();
        assert_eq!(
            segments(&pieces, &files, &blocks),
            vec![(0, 0..20_000), (2, 0..30_000)]
        );
        // Piece 1 alone: the end of `a` and the start of `c`.
        let one: Vec<BlockRef> = blocks.iter().copied().filter(|b| b.piece == 1).collect();
        assert_eq!(
            segments(&pieces, &files, &one),
            vec![(0, 16_384..20_000), (2, 0..12_768)]
        );
        assert!(segments(&pieces, &files, &[]).is_empty());
    }

    #[test]
    fn backoff_doubles_to_thirty_minutes() {
        assert_eq!(backoff(1), Duration::from_secs(30));
        assert_eq!(backoff(2), Duration::from_secs(60));
        assert_eq!(backoff(3), Duration::from_secs(120));
        assert_eq!(backoff(7), Duration::from_secs(1_800));
        assert_eq!(backoff(u32::MAX), Duration::from_secs(1_800));
    }

    #[test]
    fn statuses_that_retire() {
        let retire = |code| FetchError::from_tracker(&TrackerError::HttpStatus(code)).retire;
        assert!(retire(404) && retire(403) && retire(410));
        assert!(!retire(408) && !retire(429) && !retire(500) && !retire(503));
        assert!(!FetchError::from_tracker(&TrackerError::TooManyRedirects).retire);
        assert!(!FetchError::from_tracker(&TrackerError::Timeout).retire);
    }

    #[test]
    fn only_http_urls() {
        assert!(is_web_url("http://a/") && is_web_url("HTTPS://a"));
        assert!(!is_web_url("ftp://a/") && !is_web_url("http://"));
        assert!(!is_web_url(&format!("http://{}", "a".repeat(MAX_URL_LEN))));
    }

    /// A loopback server answering every `Range` request with the bytes of
    /// `payload`, or, while `hold` is set, never answering.
    struct RangeServer {
        addr: std::net::SocketAddr,
        hold: Arc<std::sync::atomic::AtomicBool>,
        requests: Arc<std::sync::atomic::AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for RangeServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl RangeServer {
        async fn start(payload: Vec<u8>) -> RangeServer {
            use std::sync::atomic::Ordering::SeqCst;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let payload = Arc::new(payload);
            let (h, r) = (Arc::clone(&hold), Arc::clone(&requests));
            let task = tokio::spawn(async move {
                let mut conns = tokio::task::JoinSet::new();
                while let Ok((mut s, _)) = listener.accept().await {
                    let (hold, requests, payload) =
                        (Arc::clone(&h), Arc::clone(&r), Arc::clone(&payload));
                    conns.spawn(async move {
                        let mut buf = Vec::new();
                        loop {
                            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                let mut chunk = [0u8; 4096];
                                match s.read(&mut chunk).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                }
                            }
                            let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
                            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                            buf.drain(..end + 4);
                            requests.fetch_add(1, SeqCst);
                            if hold.load(SeqCst) {
                                std::future::pending::<()>().await;
                            }
                            let spec = head
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.eq_ignore_ascii_case("range")
                                        .then(|| v.trim().strip_prefix("bytes="))
                                        .flatten()
                                })
                                .unwrap();
                            let (a, b) = spec.split_once('-').unwrap();
                            let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                            let reply = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {a}-{b}/{}\r\nContent-Length: {}\r\n\r\n",
                                payload.len(),
                                b - a + 1
                            );
                            if s.write_all(reply.as_bytes()).await.is_err()
                                || s.write_all(&payload[a..=b]).await.is_err()
                            {
                                return;
                            }
                        }
                    });
                }
            });
            RangeServer {
                addr,
                hold,
                requests,
                task,
            }
        }

        fn url(&self) -> String {
            format!("http://{}/", self.addr)
        }
    }

    /// A single-file torrent `f.bin` over `len` bytes of `piece_len`
    /// pieces, and its payload.
    fn torrent(len: usize, piece_len: usize) -> (Metainfo, Vec<u8>) {
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut pieces = Vec::new();
        for chunk in payload.chunks(piece_len) {
            pieces.extend_from_slice(&crate::storage::sha1_piece(chunk));
        }
        let mut info = DictBuilder::new();
        info.insert(b"length", len as i64);
        info.insert(b"name", "f.bin");
        info.insert(b"piece length", piece_len as i64);
        info.insert(b"pieces", pieces);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        (
            Metainfo::from_bytes(&encode(&root.build())).unwrap(),
            payload,
        )
    }

    /// A seed retired with a request in flight and revived at once: the
    /// first request, cancelled by the retirement, comes back after the
    /// second started and is ignored; the second completes the torrent.
    /// (Before request ids, the stale result cancelled the new request,
    /// whose own result cancelled the next, without end.)
    #[tokio::test]
    async fn revived_seed_ignores_its_superseded_request() {
        use std::sync::atomic::Ordering::SeqCst;
        let (meta, payload) = torrent(64 * 1024, 16 * 1024);
        let srv = RangeServer::start(payload.clone()).await;
        srv.hold.store(true, SeqCst);
        let storage: Arc<dyn crate::storage::Storage> =
            Arc::new(crate::storage::MemStorage::new(payload.len() as u64));
        let mut actor = crate::torrent::data::tests::test_actor(meta, storage, 64 << 20);
        actor.add_web_seed(srv.url()).unwrap();
        let key = actor.web_seeds.seeds[0].key;
        assert!(matches!(
            actor.web_seeds.seeds[0].state,
            SeedState::Busy { .. }
        ));
        // The held request reaches the server before the seed is retired.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while srv.requests.load(SeqCst) == 0 {
            assert!(tokio::time::Instant::now() < deadline, "no request");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        actor.retire_web_seed(key, "test".to_owned());
        srv.hold.store(false, SeqCst);
        actor.add_web_seed(srv.url()).unwrap();
        assert_eq!(actor.web_seeds.seeds[0].request, 2);
        let finished = |a: &Actor| {
            a.loaded
                .as_ref()
                .and_then(|l| l.picker.as_ref())
                .is_some_and(|p| p.is_finished())
        };
        let run = async {
            while !finished(&actor) {
                let Some(r) = actor.jobs.join_next().await else {
                    break;
                };
                actor.on_job_result(r);
            }
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the revived seed never completed");
        assert!(finished(&actor));
        assert_eq!(actor.web_seeds.seeds[0].request, 2, "no further request");
        assert_eq!(srv.requests.load(SeqCst), 2);
        crate::torrent::actor::EMITTED.take();
    }

    /// A paced fetch reads the body at the download limit, charging each
    /// frame as it goes: the bucket is never more than about one frame in
    /// debt (charging after the fetch would put the whole body there).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn paced_fetch_holds_the_limit() {
        use crate::bandwidth::{Bandwidth, BucketPair, RateLimit};
        const RATE: u64 = 256 * 1024;
        let payload: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        let srv = RangeServer::start(payload.clone()).await;
        let pair = BucketPair::new(RateLimit::new(RATE, 0));
        let throttle = Bandwidth::new(&crate::settings::Settings::default()).web_seed(&pair);
        assert_eq!(throttle.min_rate(), Some(RATE));
        let down = Arc::clone(&pair.down);
        let worst = Arc::new(std::sync::Mutex::new(Duration::ZERO));
        let seen = Arc::clone(&worst);
        let watch = tokio::spawn(async move {
            loop {
                let now = tokio::time::Instant::now();
                let debt = down
                    .lock()
                    .unwrap()
                    .repaid_at(now)
                    .map_or(Duration::ZERO, |t| t - now);
                {
                    let mut w = seen.lock().unwrap();
                    *w = (*w).max(debt);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let tracker = TrackerClient::new(fbt_tracker::TrackerConfig::default()).unwrap();
        let start = tokio::time::Instant::now();
        let got = tracker
            .fetch_range_paced(
                &format!("{}f", srv.url()),
                0..payload.len() as u64,
                &throttle,
            )
            .await
            .unwrap();
        let took = start.elapsed();
        watch.abort();
        assert!(got == payload);
        // 1 MiB at 256 KiB/s from an empty bucket: about 4 s less the
        // last frame. Generous bounds either way.
        assert!(took >= Duration::from_secs(2), "too fast: {took:?}");
        assert!(took < Duration::from_secs(30), "too slow: {took:?}");
        // At most one read buffer (hyper's is at most about 400 KiB)
        // ahead; the whole body charged at once would be 4 s.
        let worst = *worst.lock().unwrap();
        assert!(worst < Duration::from_millis(2_500), "debt {worst:?}");
    }
}
