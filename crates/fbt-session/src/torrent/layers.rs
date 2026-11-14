//! BEP 52 piece layers: building a hybrid's missing layers from the
//! subtree roots of its verified pieces, and answering peers' `hash
//! request`s from the layers we hold.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_core::Bitfield;
use fbt_core::merkle::{MERKLE_BLOCK_LEN, MerkleTree, file_tree_height, verify_hashes};
use fbt_metainfo::{MAX_PIECE_LAYER_HASHES, Metainfo, TorrentVersion, validate_layer};
use fbt_wire::{HashRequest, MAX_HASH_REQUEST_LEN};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::actor::Actor;
use super::data::Job;
use super::readers::Readers;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::{PeerKey, PiecePicker};
use crate::session::Priority;
use crate::storage::check_pieces_ranges;

/// Most `hash request`s of one peer being served at once; further ones are
/// rejected.
pub(super) const MAX_PENDING_HASH_REQUESTS: usize = 64;

/// Most serving jobs of one torrent in flight, all peers together; further
/// requests are rejected. Bounds the tree builds on the blocking pool.
pub(super) const MAX_SERVING_JOBS: usize = 8;

/// Why a `hash request` gets no hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServeError {
    /// The request breaks BEP 52 (`length` not a power of two in
    /// `1..=MAX_HASH_REQUEST_LEN`, `index` not a multiple of it, or
    /// `base layer` at or above the tree height): close the peer.
    Malformed,
    /// Valid, but not ours to answer (unknown root, a layer we lack, below
    /// the piece layer, or a span leaving the padded base layer): send
    /// `hash reject`.
    Reject,
}

/// Answers `req` from `meta`'s piece layers: the `length` base-layer
/// hashes, then the uncles bottom-up (`min(proof_layers, tree_height -
/// base - log2(length))` of them), 32 bytes each. The tree height comes
/// from the file's length, never from the request.
#[cfg(test)]
pub(super) fn serve_hash_request(
    meta: &Metainfo,
    req: &HashRequest,
) -> Result<Vec<u8>, ServeError> {
    serve_from(meta, req, None).0
}

/// [`serve_hash_request`] with the file's tree when it is `cached`;
/// otherwise the tree is built from the piece layer and also returned, for
/// the cache.
fn serve_from(
    meta: &Metainfo,
    req: &HashRequest,
    cached: Option<Arc<MerkleTree>>,
) -> (Result<Vec<u8>, ServeError>, Option<Arc<MerkleTree>>) {
    let mut built = None;
    let result = (|| {
        let length = req.length;
        if !length.is_power_of_two()
            || length > MAX_HASH_REQUEST_LEN
            || !req.index.is_multiple_of(length)
        {
            return Err(ServeError::Malformed);
        }
        let file = meta
            .info
            .files
            .iter()
            .find(|f| !f.pad && f.pieces_root == Some(req.pieces_root))
            .ok_or(ServeError::Reject)?;
        let tree_height = file_tree_height(file.len);
        if req.base_layer >= tree_height {
            return Err(ServeError::Malformed);
        }
        let piece_height = (meta.info.piece_len / MERKLE_BLOCK_LEN).trailing_zeros();
        if req.base_layer < piece_height {
            return Err(ServeError::Reject);
        }
        let (Ok(index), Ok(len)) = (usize::try_from(req.index), usize::try_from(length)) else {
            return Err(ServeError::Reject);
        };
        let tree = match cached {
            Some(t) => t,
            None => {
                let layer = meta
                    .piece_layers
                    .get(&req.pieces_root)
                    .ok_or(ServeError::Reject)?;
                let t = Arc::new(
                    MerkleTree::from_layer(layer, piece_height, tree_height)
                        .ok_or(ServeError::Reject)?,
                );
                built = Some(Arc::clone(&t));
                t
            }
        };
        let (hashes, uncles) = tree
            .hashes(req.base_layer, index, len, req.proof_layers)
            .ok_or(ServeError::Reject)?;
        Ok(hashes.iter().chain(&uncles).flatten().copied().collect())
    })();
    (result, built)
}

/// The torrent's hash serving: the Merkle tree of each file served so far
/// and the serving jobs in flight.
#[derive(Debug, Default)]
pub(super) struct HashServing {
    /// Built on a file's first request; a tree stays valid for its root
    /// whatever later layer swaps add. Bounded by the layers held (each
    /// tree is at most twice its layer).
    trees: HashMap<[u8; 32], Arc<MerkleTree>>,
    /// Serving jobs in flight, at most [`MAX_SERVING_JOBS`].
    jobs: usize,
}

impl HashServing {
    /// Takes a job slot for a peer with `peer_pending` requests in flight:
    /// false (nothing taken) at either cap.
    fn try_start(&mut self, peer_pending: &mut usize) -> bool {
        if self.jobs >= MAX_SERVING_JOBS || *peer_pending >= MAX_PENDING_HASH_REQUESTS {
            return false;
        }
        self.jobs += 1;
        *peer_pending += 1;
        true
    }

    /// Gives back a job slot, and the peer's if it is still there.
    fn finish(&mut self, peer_pending: Option<&mut usize>) {
        self.jobs = self.jobs.saturating_sub(1);
        if let Some(p) = peer_pending {
            *p = p.saturating_sub(1);
        }
    }
}

/// Least time between two swaps of the actor's metainfo for built layers
/// (the last one, at completion, excepted).
const LAYER_SWAP_INTERVAL: Duration = Duration::from_secs(1);

/// One file's layer under construction.
#[derive(Debug)]
struct Building {
    root: [u8; 32],
    file_len: u64,
    /// Global index of the file's first piece.
    first_piece: u32,
    /// The subtree root of each of the file's pieces seen to pass.
    slots: Vec<Option<[u8; 32]>>,
    filled: usize,
    /// Completed (returned, or discarded as wrong): nothing more to do.
    done: bool,
}

/// Builds the piece layers a hybrid torrent lacks from the v2 roots of its
/// verified pieces (v1 governs those pieces). At most
/// [`MAX_PIECE_LAYER_HASHES`] hashes are held in all.
#[derive(Debug)]
pub(super) struct LayerBuilder {
    piece_len: u32,
    /// Sorted by `first_piece`.
    files: Vec<Building>,
}

impl LayerBuilder {
    /// A builder for the layers `meta` lacks; `None` unless `meta` is a
    /// hybrid missing at least one.
    pub(super) fn new(meta: &Metainfo) -> Option<Self> {
        if meta.version() != TorrentVersion::Hybrid {
            return None;
        }
        let piece_len = meta.info.piece_len;
        let pl = u64::from(piece_len);
        let mut files = Vec::new();
        let mut total: usize = 0;
        let mut offset: u64 = 0;
        for f in &meta.info.files {
            let start = offset;
            offset = offset.saturating_add(f.len);
            let Some(root) = f.pieces_root else {
                continue;
            };
            if f.pad || f.len <= pl || meta.piece_layers.contains_key(&root) {
                continue;
            }
            // Hybrid files start on piece boundaries (checked at parse).
            let (Ok(first_piece), Ok(count)) = (
                u32::try_from(start / pl),
                usize::try_from(f.len.div_ceil(pl)),
            ) else {
                continue;
            };
            if total.saturating_add(count) > MAX_PIECE_LAYER_HASHES {
                break;
            }
            total += count;
            files.push(Building {
                root,
                file_len: f.len,
                first_piece,
                slots: vec![None; count],
                filled: 0,
                done: false,
            });
        }
        (!files.is_empty()).then_some(LayerBuilder { piece_len, files })
    }

    fn file_of(&mut self, piece: u32) -> Option<(&mut Building, usize)> {
        let pos = self.files.partition_point(|b| b.first_piece <= piece);
        let b = self.files.get_mut(pos.checked_sub(1)?)?;
        let index = usize::try_from(piece - b.first_piece).ok()?;
        (index < b.slots.len()).then_some((b, index))
    }

    /// Records that piece `piece` passed with subtree root `root`. Returns
    /// the file's layer once it is complete and roots at its `pieces root`;
    /// a complete layer that does not is discarded with a warning.
    pub(super) fn record(
        &mut self,
        piece: u32,
        root: [u8; 32],
    ) -> Option<([u8; 32], Vec<[u8; 32]>)> {
        let piece_len = self.piece_len;
        let (b, index) = self.file_of(piece)?;
        if b.done {
            return None;
        }
        let slot = b.slots.get_mut(index)?;
        if slot.is_none() {
            b.filled += 1;
        }
        *slot = Some(root);
        if b.filled < b.slots.len() {
            return None;
        }
        b.done = true;
        let layer: Vec<[u8; 32]> = std::mem::take(&mut b.slots).into_iter().flatten().collect();
        if validate_layer(&b.root, b.file_len, piece_len, &layer) {
            Some((b.root, layer))
        } else {
            warn!(
                pieces_root = %hex(&b.root),
                "a piece layer built from verified data does not match its pieces root; discarded"
            );
            None
        }
    }

    /// Forgets the root of piece `piece` (lost by a recheck).
    pub(super) fn forget(&mut self, piece: u32) {
        if let Some((b, index)) = self.file_of(piece)
            && !b.done
            && let Some(slot) = b.slots.get_mut(index)
            && slot.take().is_some()
        {
            b.filled -= 1;
        }
    }
}

/// Most hashes one `hash request` of the layer fetch asks for.
pub(super) const LAYER_REQUEST_LEN: u32 = 512;
/// Peers the layer fetch asks at once.
pub(super) const MAX_LAYER_SOURCES: usize = 4;
/// A request unanswered this long retires its source (no ban); it may
/// become a source again after as long.
pub(super) const LAYER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Spans one source has in flight at once.
const SPANS_PER_SOURCE: usize = 4;
/// Most v2 peers waiting to become sources, and most retired ones
/// remembered (each).
const MAX_LAYER_CANDIDATES: usize = 1024;

/// True when `meta` is v2-only and a file of more than one piece lacks its
/// layer, so there is something to fetch: [`LayerFetch::new`] returns a
/// fetch exactly when this holds. The download does not wait for that fetch
/// (see [`TorrentStatus::layers_missing`](crate::TorrentStatus::layers_missing)).
pub(super) fn missing_layers(meta: &Metainfo) -> bool {
    meta.version() == TorrentVersion::V2 && fetch_targets(meta).next().is_some()
}

/// A multi-piece file whose layer is missing, with its fetch geometry.
struct Target {
    root: [u8; 32],
    /// Index into `Info::files`, so priorities can say whether it matters.
    file: usize,
    tree_height: u32,
    /// Log2 of the padded layer width (1 to 31).
    w_log: u32,
    /// Hashes in the layer: the file's piece count.
    count: usize,
    /// Global index of the file's first piece. Every non-empty file starts
    /// on a piece boundary (the parser pads the file before it), so its
    /// pieces are `first_piece .. first_piece + count`.
    first_piece: u32,
}

/// The multi-piece files of `meta` whose layer is missing. Every such file
/// of a parsed torrent is a target: its padded layer is at least 2 wide,
/// and the parser's [`fbt_metainfo::MAX_V2_PIECES`] keeps it below 2^32.
fn fetch_targets(meta: &Metainfo) -> impl Iterator<Item = Target> + '_ {
    let pl = u64::from(meta.info.piece_len);
    let piece_height = (meta.info.piece_len / MERKLE_BLOCK_LEN).trailing_zeros();
    let mut offset: u64 = 0;
    meta.info
        .files
        .iter()
        .enumerate()
        .filter_map(move |(i, f)| {
            // The offset moves on for every file, pads and one-piece files
            // included, or the piece ranges would not match the layout.
            let start = offset;
            offset = offset.saturating_add(f.len);
            if f.pad || f.len <= pl {
                return None;
            }
            let root = f
                .pieces_root
                .filter(|r| !meta.piece_layers.contains_key(r))?;
            let tree_height = file_tree_height(f.len);
            let w_log = tree_height
                .checked_sub(piece_height)
                .filter(|w| (1..32).contains(w))?;
            let count = usize::try_from(f.len.div_ceil(pl)).ok()?;
            let first_piece = u32::try_from(start.checked_div(pl)?).ok()?;
            Some(Target {
                root,
                file: i,
                tree_height,
                w_log,
                count,
                first_piece,
            })
        })
}

/// Whether a file's pieces are wanted: empty `file_prio` means every file is
/// `Normal`, as elsewhere in the actor.
fn file_wanted(file_prio: &[Priority], file: usize) -> bool {
    file_prio.get(file).is_none_or(|p| *p != Priority::Skip)
}

/// Whether some file of `meta` whose pieces are wanted has this `pieces root`.
pub(super) fn root_is_wanted(meta: &Metainfo, file_prio: &[Priority], root: [u8; 32]) -> bool {
    meta.info
        .files
        .iter()
        .enumerate()
        .any(|(i, f)| !f.pad && f.pieces_root == Some(root) && file_wanted(file_prio, i))
}

/// Adds `ranges` to `into`, joining overlapping or adjacent runs so the queue
/// holds one range per stretch of pieces however many layers merged.
fn push_ranges(into: &mut Vec<Range<u32>>, ranges: impl IntoIterator<Item = Range<u32>>) {
    let mut all: Vec<Range<u32>> = std::mem::take(into);
    all.extend(ranges);
    all.sort_by_key(|r| r.start);
    let mut out: Vec<Range<u32>> = Vec::with_capacity(all.len());
    for r in all {
        if r.start >= r.end {
            continue;
        }
        match out.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => out.push(r),
        }
    }
    *into = out;
}

/// Takes the pieces of `done` out of `into`: what no answered pass covered
/// survives, split around the parts that were read.
fn drop_ranges(into: &mut Vec<Range<u32>>, done: &[Range<u32>]) {
    let mut out: Vec<Range<u32>> = Vec::new();
    for r in std::mem::take(into) {
        let mut parts = vec![r];
        for d in done {
            let mut next = Vec::with_capacity(parts.len() + 1);
            for p in parts {
                if d.end <= p.start || d.start >= p.end {
                    next.push(p);
                    continue;
                }
                if p.start < d.start {
                    next.push(p.start..d.start.min(p.end));
                }
                if d.end < p.end {
                    next.push(d.end.max(p.start)..p.end);
                }
            }
            parts = next;
        }
        out.extend(parts.into_iter().filter(|p| p.start < p.end));
    }
    *into = out;
}

/// The BEP 52 piece layers `meta` does not hold: one per multi-piece file
/// whose layer is missing (`0` for a v1 torrent, whose files name no
/// `pieces root`; the same for a hybrid torrent until its layers are built).
pub(super) fn missing_layer_count(meta: &Metainfo) -> u32 {
    u32::try_from(fetch_targets(meta).count()).unwrap_or(u32::MAX)
}

/// The pieces of `meta`'s non-pad files whose `pieces root` is `root`: a
/// layer for `root` makes every piece in these ranges verifiable. Files of
/// identical content share a root, and each starts on a piece boundary, so
/// the ranges never overlap.
pub(super) fn file_piece_ranges(meta: &Metainfo, root: [u8; 32]) -> Vec<Range<u32>> {
    let pl = u64::from(meta.info.piece_len);
    let mut out = Vec::new();
    let mut offset: u64 = 0;
    for f in &meta.info.files {
        let start = offset;
        offset = offset.saturating_add(f.len);
        if f.pad || f.pieces_root != Some(root) {
            continue;
        }
        let Some(blocks) = start.checked_div(pl) else {
            continue;
        };
        let (Ok(first), Ok(pieces)) = (u32::try_from(blocks), u32::try_from(f.len.div_ceil(pl)))
        else {
            continue;
        };
        let (Some(end), true) = (first.checked_add(pieces), pieces > 0) else {
            continue;
        };
        out.push(first..end);
    }
    out
}

/// Holds back the pieces a v2-only torrent cannot verify until their file's
/// layer arrives (see [`PiecePicker::set_pickable`]): asking for them would
/// download bytes no hash can accept. A hybrid torrent verifies its pieces
/// from SHA-1 alone and keeps every piece pickable, layers or not. This
/// runs on a freshly built picker, which holds nothing back yet, so it only
/// ever sets the mask.
pub(super) fn apply_layer_mask(picker: &mut PiecePicker, meta: &Metainfo) {
    if meta.version() != TorrentVersion::V2 {
        return;
    }
    for t in fetch_targets(meta) {
        let Ok(count) = u32::try_from(t.count) else {
            continue;
        };
        let Some(end) = t.first_piece.checked_add(count) else {
            continue;
        };
        picker.set_pickable(t.first_piece..end, false);
    }
}

/// One file whose layer is fetched.
#[derive(Debug)]
struct FetchFile {
    root: [u8; 32],
    tree_height: u32,
    /// Hashes per request: `min(LAYER_REQUEST_LEN, W)`.
    length: u32,
    /// Uncles per answer: `tree_height - piece_height - log2(length)`.
    proof_layers: u32,
    /// The layer, one hash per piece of the file; filled span by span, and
    /// taken away whole once every span is in ([`LayerFetch::take_ready`]).
    layer: Vec<[u8; 32]>,
    /// Spans of this file not received yet; at 0 its layer is ready.
    left: usize,
    /// Whether some of this file's pieces are wanted: their layer is asked
    /// for before a skipped file's (see [`LayerFetch::new`]).
    wanted: bool,
}

/// One request's worth of a file's layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Span {
    file: usize,
    index: u32,
}

/// What an answer does to the layer fetch.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LayerStep {
    /// Requests to send (possibly none).
    Send(Vec<(PeerKey, HashRequest)>),
    /// The peer sent hashes never asked of it, or hashes failing their
    /// proof: close and ban it. Its requests are already given back.
    Bad(PeerKey),
}

/// Fetching the piece layers a v2-only torrent lacks from v2 peers (BEP 52
/// `hash request`). Pure: no I/O, `now` is passed in.
///
/// Each multi-piece file's padded layer (`W = 2^(H - p)` wide for tree
/// height `H` and piece height `p`) is asked for in spans of
/// `min(LAYER_REQUEST_LEN, W)` hashes at the piece layer, with the uncles
/// up to (excluding) the root, so every answer is checked against the
/// file's `pieces root` before it is kept. Spans lying wholly in the
/// padding are not asked for: they hold only pad hashes. At most
/// [`MAX_LAYER_SOURCES`] peers are asked at once, each with at most
/// `SPANS_PER_SOURCE` spans in flight.
///
/// The fetch runs beside the download, not before it:
/// [`take_ready`](Self::take_ready) hands over a file's layer as soon as
/// its last span arrives, and that file's pieces stop being held back then
/// (see [`apply_layer_mask`]).
#[derive(Debug)]
pub(super) struct LayerFetch {
    piece_height: u32,
    files: Vec<FetchFile>,
    /// Spans waiting for a source.
    todo: VecDeque<Span>,
    /// Spans not received yet (waiting or in flight).
    spans_left: usize,
    /// Sources and their spans in flight, each with when it was sent.
    sources: BTreeMap<PeerKey, Vec<(Span, Instant)>>,
    /// v2 peers waiting for a source slot.
    candidates: VecDeque<PeerKey>,
    /// Sources retired by a timeout or a reject, until when: they become
    /// candidates again after.
    retired: BTreeMap<PeerKey, Instant>,
    /// File index of each `pieces root` fetched.
    by_root: HashMap<[u8; 32], usize>,
    /// Every span asked of each connected peer during this fetch (at most
    /// the fetch's span count per peer; dropped with the peer): an answer
    /// to one of them no longer in flight is late, not unrequested.
    asked: HashMap<PeerKey, HashSet<Span>>,
    /// Spans received (at most the fetch's span count).
    done: HashSet<Span>,
    /// Files whose every span arrived, in file order, not yet handed over by
    /// [`take_ready`](Self::take_ready): at most one entry per file.
    ready: Vec<usize>,
}

impl LayerFetch {
    /// A fetch of the layers `meta` lacks; `None` unless `meta` is v2-only
    /// and a multi-piece file lacks its layer ([`missing_layers`]).
    ///
    /// `file_prio` sets the order spans are asked in: the layers of files
    /// whose pieces are wanted come first, since those are what the download
    /// waits for, and skipped files' layers follow. A waiting read pushes its
    /// own file further ahead ([`prioritise`](Self::prioritise)).
    pub(super) fn new(meta: &Metainfo, file_prio: &[Priority]) -> Option<Self> {
        if !missing_layers(meta) {
            return None;
        }
        let piece_height = (meta.info.piece_len / MERKLE_BLOCK_LEN).trailing_zeros();
        let mut files = Vec::new();
        // Each file's spans, in index order; `todo` concatenates them wanted
        // files first, so a skipped file's hashes never delay a wanted one.
        let mut per_file: Vec<Vec<Span>> = Vec::new();
        let mut total: usize = 0;
        for t in fetch_targets(meta) {
            let length = LAYER_REQUEST_LEN.min(1 << t.w_log);
            let Ok(step) = usize::try_from(length) else {
                continue;
            };
            // Unreachable for a parsed torrent: a v2-only torrent has at most
            // `MAX_V2_PIECES` (= `MAX_PIECE_LAYER_HASHES`) pieces, and its
            // layers hold one hash per piece, so no file is ever left out
            // (which would leave the download unable to finish).
            debug_assert!(total.saturating_add(t.count) <= MAX_PIECE_LAYER_HASHES);
            if total.saturating_add(t.count) > MAX_PIECE_LAYER_HASHES {
                break;
            }
            total += t.count;
            let file = files.len();
            let mut spans = Vec::new();
            for index in (0..t.count).step_by(step) {
                if let Ok(index) = u32::try_from(index) {
                    spans.push(Span { file, index });
                }
            }
            per_file.push(spans);
            files.push(FetchFile {
                root: t.root,
                tree_height: t.tree_height,
                length,
                proof_layers: t.w_log - length.trailing_zeros(),
                layer: vec![[0; 32]; t.count],
                left: per_file[file].len(),
                wanted: file_wanted(file_prio, t.file),
            });
        }
        let mut todo: VecDeque<Span> = VecDeque::new();
        for pass in [true, false] {
            for (i, f) in files.iter().enumerate() {
                if f.wanted == pass {
                    todo.extend(per_file[i].iter().copied());
                }
            }
        }
        let by_root = files.iter().enumerate().map(|(i, f)| (f.root, i)).collect();
        (!todo.is_empty()).then(|| LayerFetch {
            piece_height,
            files,
            by_root,
            asked: HashMap::new(),
            done: HashSet::new(),
            ready: Vec::new(),
            spans_left: todo.len(),
            todo,
            sources: BTreeMap::new(),
            candidates: VecDeque::new(),
            retired: BTreeMap::new(),
        })
    }

    fn request(&self, span: Span) -> Option<HashRequest> {
        let f = self.files.get(span.file)?;
        Some(HashRequest {
            pieces_root: f.root,
            base_layer: self.piece_height,
            index: span.index,
            length: f.length,
            proof_layers: f.proof_layers,
        })
    }

    /// A connected v2 peer (v2 bit or truncated v2 hash): a source now, or once a slot
    /// frees.
    pub(super) fn add_source(&mut self, key: PeerKey, now: Instant) -> Vec<(PeerKey, HashRequest)> {
        if !self.sources.contains_key(&key)
            && !self.retired.contains_key(&key)
            && !self.candidates.contains(&key)
            && self.candidates.len() < MAX_LAYER_CANDIDATES
        {
            self.candidates.push_back(key);
        }
        self.fill(now)
    }

    /// `hashes` from `key` answering `req`. An answer to a span asked of
    /// `key` earlier but no longer in flight there (it timed out, say) is
    /// late: kept if the span is still wanted and it verifies, ignored
    /// otherwise. Only an answer to a span never asked of `key`, or an
    /// in-flight answer failing its proof, is `Bad`.
    pub(super) fn on_hashes(
        &mut self,
        key: PeerKey,
        req: &HashRequest,
        hashes: &[u8],
        now: Instant,
    ) -> LayerStep {
        let (span, in_flight) = match self.take_in_flight(key, req) {
            Some(span) => (span, true),
            None => match self.asked_span(key, req) {
                Some(span) => (span, false),
                None => {
                    self.drop_source(key);
                    return LayerStep::Bad(key);
                }
            },
        };
        if self.done.contains(&span) {
            return LayerStep::Send(self.fill(now));
        }
        if !self.accept(span, hashes) {
            if !in_flight {
                return LayerStep::Send(Vec::new());
            }
            self.todo.push_front(span);
            self.drop_source(key);
            return LayerStep::Bad(key);
        }
        self.done.insert(span);
        self.spans_left = self.spans_left.saturating_sub(1);
        // The file's layer is complete once its last span lands: it goes to
        // the actor without waiting for the others.
        let ready = self.files.get_mut(span.file).is_some_and(|f| {
            f.left = f.left.saturating_sub(1);
            f.left == 0
        });
        if ready {
            self.ready.push(span.file);
        }
        // A late answer may beat the span's new request: drop that one.
        self.todo.retain(|s| *s != span);
        for spans in self.sources.values_mut() {
            spans.retain(|(s, _)| *s != span);
        }
        LayerStep::Send(self.fill(now))
    }

    /// The span `req` names, if it was ever asked of `key`.
    fn asked_span(&self, key: PeerKey, req: &HashRequest) -> Option<Span> {
        let span = Span {
            file: *self.by_root.get(&req.pieces_root)?,
            index: req.index,
        };
        (self.asked.get(&key)?.contains(&span) && self.request(span).as_ref() == Some(req))
            .then_some(span)
    }

    /// `hash reject` from `key` for `req`: the source is retired (no ban)
    /// and its spans go to the others. A reject for nothing asked of it is
    /// ignored.
    pub(super) fn on_reject(
        &mut self,
        key: PeerKey,
        req: &HashRequest,
        now: Instant,
    ) -> Vec<(PeerKey, HashRequest)> {
        if let Some(span) = self.take_in_flight(key, req) {
            self.todo.push_front(span);
            self.retire(key, now);
        }
        self.fill(now)
    }

    /// The peer is gone: its spans go to the others.
    pub(super) fn on_disconnect(
        &mut self,
        key: PeerKey,
        now: Instant,
    ) -> Vec<(PeerKey, HashRequest)> {
        self.drop_source(key);
        self.candidates.retain(|k| *k != key);
        self.retired.remove(&key);
        self.asked.remove(&key);
        self.fill(now)
    }

    /// Retires sources with a request older than
    /// [`LAYER_REQUEST_TIMEOUT`] and brings back those retired as long ago.
    pub(super) fn tick(&mut self, now: Instant) -> Vec<(PeerKey, HashRequest)> {
        let late: Vec<PeerKey> = self
            .sources
            .iter()
            .filter(|(_, spans)| {
                spans
                    .iter()
                    .any(|(_, sent)| now.saturating_duration_since(*sent) >= LAYER_REQUEST_TIMEOUT)
            })
            .map(|(k, _)| *k)
            .collect();
        for key in late {
            self.retire(key, now);
        }
        let back: Vec<PeerKey> = self
            .retired
            .iter()
            .filter(|(_, until)| **until <= now)
            .map(|(k, _)| *k)
            .collect();
        for key in back {
            self.retired.remove(&key);
            if self.candidates.len() < MAX_LAYER_CANDIDATES {
                self.candidates.push_back(key);
            }
        }
        self.fill(now)
    }

    /// Every span arrived and passed its proof.
    pub(super) fn is_complete(&self) -> bool {
        self.spans_left == 0
    }

    /// The roots whose spans are not all received yet. Whether those hashes
    /// are wanted is a question for the actor's current priorities (see
    /// [`root_is_wanted`]), so this says what is outstanding, not what matters.
    pub(super) fn owed_roots(&self) -> impl Iterator<Item = [u8; 32]> + '_ {
        self.files.iter().filter(|f| f.left > 0).map(|f| f.root)
    }

    /// The files whose layer arrived whole since the last call, taken once:
    /// each entry is the file's `pieces root` and its layer, one hash per
    /// piece of that file. A fetch is complete (see
    /// [`is_complete`](Self::is_complete)) exactly when this hands over its
    /// last layer.
    pub(super) fn take_ready(&mut self) -> Vec<([u8; 32], Vec<[u8; 32]>)> {
        let mut out = Vec::new();
        for file in std::mem::take(&mut self.ready) {
            if let Some(f) = self.files.get_mut(file)
                && !f.layer.is_empty()
            {
                out.push((f.root, std::mem::take(&mut f.layer)));
            }
        }
        out
    }

    /// Puts the waiting spans of the files with these `pieces root`s ahead
    /// of the rest: a reader sits on those pieces, so they are what the
    /// download is blocked on. Order within each group is unchanged; naming
    /// roots no file has is ignored.
    pub(super) fn prioritise(&mut self, roots: &[[u8; 32]]) {
        if self.todo.is_empty() || roots.is_empty() {
            return;
        }
        let first: HashSet<usize> = roots
            .iter()
            .filter_map(|r| self.by_root.get(r).copied())
            .collect();
        if first.is_empty() {
            return;
        }
        let waiting: Vec<Span> = self.todo.drain(..).collect();
        let (ahead, later): (Vec<Span>, Vec<Span>) =
            waiting.into_iter().partition(|s| first.contains(&s.file));
        self.todo.extend(ahead.into_iter().chain(later));
    }

    /// Removes `req` from `key`'s spans in flight.
    fn take_in_flight(&mut self, key: PeerKey, req: &HashRequest) -> Option<Span> {
        let spans = self.sources.get(&key)?;
        let pos = spans
            .iter()
            .position(|(span, _)| self.request(*span).as_ref() == Some(req))?;
        let (span, _) = self.sources.get_mut(&key)?.remove(pos);
        Some(span)
    }

    /// Checks `bytes` (the span's hashes, then exactly its uncles) against
    /// the file's root and keeps the hashes that fall on its pieces.
    fn accept(&mut self, span: Span, bytes: &[u8]) -> bool {
        let piece_height = self.piece_height;
        let Some(f) = self.files.get_mut(span.file) else {
            return false;
        };
        let (hashes, rest) = bytes.as_chunks::<32>();
        let Ok(length) = usize::try_from(f.length) else {
            return false;
        };
        if !rest.is_empty() || hashes.len() != length.saturating_add(f.proof_layers as usize) {
            return false;
        }
        let (span_hashes, proof) = hashes.split_at(length);
        let Ok(index) = usize::try_from(span.index) else {
            return false;
        };
        if !verify_hashes(
            &f.root,
            f.tree_height,
            piece_height,
            index,
            span_hashes,
            proof,
        ) {
            return false;
        }
        for (slot, h) in f.layer.iter_mut().skip(index).zip(span_hashes) {
            *slot = *h;
        }
        true
    }

    /// Takes `key` off the sources; its spans in flight wait again, first.
    fn drop_source(&mut self, key: PeerKey) {
        if let Some(spans) = self.sources.remove(&key) {
            for (span, _) in spans.into_iter().rev() {
                self.todo.push_front(span);
            }
        }
    }

    /// Drops `key` as a source without blame, until
    /// [`LAYER_REQUEST_TIMEOUT`] from `now`.
    fn retire(&mut self, key: PeerKey, now: Instant) {
        self.drop_source(key);
        if self.retired.len() < MAX_LAYER_CANDIDATES {
            self.retired.insert(key, now + LAYER_REQUEST_TIMEOUT);
        }
    }

    /// Fills free source slots from the candidates, then hands out waiting
    /// spans round-robin.
    fn fill(&mut self, now: Instant) -> Vec<(PeerKey, HashRequest)> {
        while self.sources.len() < MAX_LAYER_SOURCES
            && let Some(key) = self.candidates.pop_front()
        {
            self.sources.insert(key, Vec::new());
        }
        let mut out = Vec::new();
        loop {
            let mut gave = false;
            for (key, spans) in &mut self.sources {
                if spans.len() >= SPANS_PER_SOURCE {
                    continue;
                }
                let Some(span) = self.todo.pop_front() else {
                    break;
                };
                spans.push((span, now));
                out.push((*key, span));
                gave = true;
            }
            if !gave || self.todo.is_empty() {
                break;
            }
        }
        for (key, span) in &out {
            self.asked.entry(*key).or_default().insert(*span);
        }
        out.into_iter()
            .filter_map(|(key, span)| Some((key, self.request(span)?)))
            .collect()
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The parts of `range` whose pieces are not ours yet: those are the ones a
/// pass has to read. A piece already verified passed against the hashes we
/// held then, so reading its bytes again proves nothing.
fn unread_ranges(have: Option<&Bitfield>, range: Range<u32>) -> Vec<Range<u32>> {
    let Some(have) = have else {
        // No picker yet: nothing is known to be ours.
        return vec![range];
    };
    let mut out = Vec::new();
    let mut free = range.start;
    for piece in have.iter_ones() {
        if piece < range.start {
            continue;
        }
        if piece >= range.end {
            break;
        }
        if free < piece {
            out.push(free..piece);
        }
        free = piece.saturating_add(1);
    }
    if free < range.end {
        out.push(free..range.end);
    }
    out
}

impl Actor {
    /// Piece `piece` passed with v2 subtree root `root` (or lost, `None`,
    /// by a recheck): fed to the layer builder; a completed layer waits for
    /// the next swap.
    pub(super) fn on_piece_root(&mut self, piece: u32, root: Option<[u8; 32]>) {
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        let Some(b) = l.layer_builder.as_mut() else {
            return;
        };
        match root {
            Some(root) => {
                if let Some((r, layer)) = b.record(piece, root) {
                    l.built_layers.insert(r, layer);
                }
            }
            None => b.forget(piece),
        }
    }

    /// Merges the layers built or fetched since the last swap into the
    /// actor's metainfo: at most once per [`LAYER_SWAP_INTERVAL`], or now when
    /// `force` (a check finished, or the fetch did). A file stops having its
    /// pieces held back only once they have been read off disk again — but a
    /// second look is owed only where one is due: the pieces of a *fetched*
    /// layer may already be on disk unverified (data can arrive before the
    /// hashes do), while a built layer is made of pieces that passed their
    /// checks, and only a v2-only torrent hides pieces behind a missing layer.
    /// A fetch that just completed goes away with the merge. Only `Loaded.meta`
    /// changes: the handle's `metainfo()` keeps the layers known when it was set.
    pub(super) fn swap_layers(&mut self, force: bool) {
        {
            let Some(l) = self.loaded.as_mut() else {
                return;
            };
            if let Some(fetch) = l.layer_fetch.as_mut() {
                for (root, layer) in fetch.take_ready() {
                    l.fetched_layers.insert(root, layer);
                }
            }
            let finished = l
                .layer_fetch
                .as_ref()
                .is_some_and(|fetch| fetch.is_complete());
            if l.built_layers.is_empty() && l.fetched_layers.is_empty() && !finished {
                return;
            }
            let now = Instant::now();
            if !force
                && !finished
                && l.layers_swapped
                    .is_some_and(|t| now - t < LAYER_SWAP_INTERVAL)
            {
                return;
            }
            // Which roots a re-read is owed for: fetched layers only. A layer
            // `with_piece_layers` would not take (over its cap) is not in
            // `meta`, so its pieces stay held back — they fail every hash, so
            // asking for them wastes bytes and bans honest peers.
            let fetched = std::mem::take(&mut l.fetched_layers);
            let owed: Vec<[u8; 32]> = if l.meta.version() == TorrentVersion::V2 {
                fetched.keys().copied().collect()
            } else {
                // A hybrid's layers are built from pieces that already passed.
                Vec::new()
            };
            let mut layers = std::mem::take(&mut l.built_layers);
            layers.extend(fetched);
            l.meta = Arc::new((*l.meta).clone().with_piece_layers(layers));
            l.layers_swapped = Some(now);
            let meta = Arc::clone(&l.meta);
            let have = l.picker.as_ref().map(|p| p.have());
            let ranges: Vec<Range<u32>> = owed
                .iter()
                .filter(|r| meta.piece_layers.contains_key(*r))
                .flat_map(|r| file_piece_ranges(&meta, *r))
                .flat_map(|range| unread_ranges(have, range))
                .collect();
            push_ranges(&mut l.pending_verify, ranges);
            if finished {
                l.layer_fetch = None;
            }
            let all_in = finished && missing_layer_count(&meta) == 0;
            let pending = std::mem::take(&mut l.pending_verify);
            if let Some(picker) = l.picker.as_mut() {
                if all_in {
                    // Every layer is in: nothing the mask covered stays out,
                    // except the pieces not read off disk yet.
                    picker.set_pickable(0..l.layout.num_pieces(), true);
                }
                for r in &pending {
                    picker.set_pickable(r.clone(), false);
                }
            }
            l.pending_verify = pending;
        }
        self.verify_merged_pieces();
    }

    /// Reads the pieces whose layer just merged back off disk, holding a
    /// `max_active_checks` slot for the pass exactly as [`Actor::start_check`]
    /// does — `spawn_check` runs outside the disk threads only because its
    /// caller holds one (see `disk.rs`). The pieces could not be hashed when
    /// they were downloaded, and this is what keeps complete data from being
    /// fetched again. One pass per torrent at a time; with no slot free it does
    /// not queue for one — the next status tick, merge or result runs this
    /// again, and the ranges stay held back meanwhile (see
    /// [`Actor::on_pieces_verified`]). While the files move the pass waits too.
    pub(super) fn verify_merged_pieces(&mut self) {
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if l.verifying || l.pending_verify.is_empty() || l.picker.is_none() {
            return;
        }
        let Some(storage) = self.storage.clone() else {
            return;
        };
        if self.storage_busy {
            return;
        }
        // A check's slot, held by the job itself until it returns. None free is
        // not a reason to hold one waiting: this runs again on the next tick.
        let Ok(permit) = self.shared.disk.check_permit().try_acquire_owned() else {
            return;
        };
        // The queue keeps its ranges while the pass runs: they stay held back
        // until it answers, so a merge that completes meanwhile cannot release
        // them to a download of bytes that are already on disk.
        let ranges = l.pending_verify.clone();
        l.verifying = true;
        let meta = Arc::clone(&l.meta);
        let layout = l.layout;
        let generation = self.generation;
        self.shared.disk.spawn_check(&mut self.jobs, move || {
            let _slot = permit;
            let result = check_pieces_ranges(&*storage, &layout, &meta, &ranges, |_| {
                ControlFlow::Continue(())
            });
            Job::LayersVerified {
                generation,
                ranges,
                result,
            }
        });
    }

    /// The disk pass over pieces whose layer arrived is back: the ones passing
    /// every hash are ours without being fetched again (`PieceFinished` as for
    /// a download), and the rest of those ranges may be requested now. A pass
    /// that raced a check or a restart is dropped: its ranges go back in the
    /// queue, which runs again at once where a picker stands ready (a torrent
    /// with nothing else going on must not wait for a status tick to notice its
    /// pieces are still held back).
    pub(super) fn on_pieces_verified(
        &mut self,
        generation: u64,
        ranges: &[Range<u32>],
        result: io::Result<Vec<(u32, Option<[u8; 32]>)>>,
    ) {
        let stale = generation != self.generation;
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        l.verifying = false;
        if stale || l.picker.is_none() {
            push_ranges(&mut l.pending_verify, ranges.iter().cloned());
            self.verify_merged_pieces();
            return;
        }
        let passed = match result {
            Ok(passed) => passed,
            Err(e) => {
                debug!(
                    torrent = %self.name(),
                    error = %e,
                    "re-reading pieces after a layer arrived failed; fetching them instead"
                );
                Vec::new()
            }
        };
        for (piece, root) in passed {
            // A pass re-queued behind a check or a storage move can cover a
            // piece that check already found: report each piece once.
            let known = self
                .loaded
                .as_ref()
                .and_then(|l| l.picker.as_ref())
                .is_some_and(|p| p.have().get(piece));
            if known {
                continue;
            }
            self.on_piece_root(piece, root);
            if let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) {
                picker.piece_passed(piece);
            }
            self.deadlines.piece_done(piece);
            self.lost_pieces.remove(&piece);
            self.emit(Event::PieceFinished {
                torrent: self.ih,
                piece,
            });
        }
        // Those ranges have been read: what is left of them may be requested,
        // and only the parts no pass has covered stay in the queue.
        if let Some(l) = self.loaded.as_mut() {
            drop_ranges(&mut l.pending_verify, ranges);
        }
        if let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) {
            for r in ranges {
                picker.set_pickable(r.clone(), true);
            }
        }
        self.need_save_resume = true;
        let finished = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(crate::picker::PiecePicker::is_finished);
        if finished && !self.finished_emitted {
            self.finished_emitted = true;
            self.finish_pending = true;
        }
        // Reads waiting on those pieces are served from disk, and peers learn
        // what we found without downloading it.
        self.sync_all_readers();
        self.serve_waiting_reads();
        self.catch_up_peers();
        self.maybe_finish();
        self.refresh_state();
        self.verify_merged_pieces();
    }

    /// A v1-only torrent has no hash messages: true (and the peer closed)
    /// when this is one. Before the metadata the version is unknown.
    fn close_if_v1_only(&mut self, key: PeerKey) -> bool {
        let v1 = self
            .loaded
            .as_ref()
            .is_some_and(|l| l.meta.version() == TorrentVersion::V1);
        if v1 {
            debug!(key = key.0, "hash message on a v1 torrent; closing");
            self.close_peer(key);
        }
        v1
    }

    /// A peer's `hash request`: served off the actor from `Loaded.meta`
    /// and the file's cached tree, at most [`MAX_PENDING_HASH_REQUESTS`]
    /// at once per peer and [`MAX_SERVING_JOBS`] per torrent; rejected
    /// beyond either, before the metadata, and whenever
    /// [`Settings::serve_hashes`](crate::Settings::serve_hashes) is off.
    pub(super) fn on_hash_request(&mut self, key: PeerKey, req: HashRequest) {
        if !self.peers.contains_key(&key) || self.close_if_v1_only(key) {
            return;
        }
        if !self.shared.settings.serve_hashes {
            debug!(key = key.0, ?req, "hash serving is off; rejected");
            self.send(key, PeerCommand::RejectHashes(req));
            return;
        }
        let (Some(l), Some(p)) = (self.loaded.as_mut(), self.peers.get_mut(&key)) else {
            self.send(key, PeerCommand::RejectHashes(req));
            return;
        };
        if !l.hash_serving.try_start(&mut p.hash_requests) {
            debug!(
                key = key.0,
                ?req,
                "hash request over the serving caps; rejected"
            );
            self.send(key, PeerCommand::RejectHashes(req));
            return;
        }
        let meta = Arc::clone(&l.meta);
        let cached = l.hash_serving.trees.get(&req.pieces_root).cloned();
        self.jobs.spawn_blocking(move || {
            let (result, built) = serve_from(&meta, &req, cached);
            Job::HashesServed {
                key,
                req,
                result,
                built,
            }
        });
    }

    /// A served `hash request` came back: answered, rejected, or the peer
    /// closed for a malformed request. A tree it built joins the cache.
    pub(super) fn on_hashes_served(
        &mut self,
        key: PeerKey,
        req: HashRequest,
        result: Result<Vec<u8>, ServeError>,
        built: Option<Arc<MerkleTree>>,
    ) {
        let peer = self.peers.get_mut(&key).map(|p| &mut p.hash_requests);
        let present = peer.is_some();
        if let Some(l) = self.loaded.as_mut() {
            l.hash_serving.finish(peer);
            if let Some(tree) = built {
                l.hash_serving.trees.entry(req.pieces_root).or_insert(tree);
            }
        }
        if !present {
            return;
        }
        match result {
            Ok(hashes) => {
                let hashes = Bytes::from(hashes);
                self.send(key, PeerCommand::SendHashes { req, hashes });
            }
            Err(ServeError::Reject) => {
                self.send(key, PeerCommand::RejectHashes(req));
            }
            Err(ServeError::Malformed) => {
                debug!(key = key.0, ?req, "malformed hash request; closing");
                self.close_peer(key);
            }
        }
    }

    /// A peer's `hashes` (`Some`) or `hash reject` (`None`): fed to the
    /// layer fetch while one runs, dropped otherwise (closing the peer on a
    /// v1-only torrent). Hashes that are unrequested or fail their proof
    /// ban the peer as bad metadata does; an answer with no fetch running —
    /// the layers came another way in the meantime — is dropped without
    /// blame.
    pub(super) fn on_hash_answer(&mut self, key: PeerKey, req: HashRequest, hashes: Option<Bytes>) {
        if self.close_if_v1_only(key) {
            return;
        }
        debug!(
            key = key.0,
            ?req,
            answered = hashes.is_some(),
            "hash answer"
        );
        let now = Instant::now();
        let Some(step) = self.loaded.as_mut().and_then(|l| {
            let fetch = l.layer_fetch.as_mut()?;
            Some(match hashes {
                None => LayerStep::Send(fetch.on_reject(key, &req, now)),
                Some(h) => fetch.on_hashes(key, &req, &h, now),
            })
        }) else {
            debug!(
                key = key.0,
                ?req,
                "hash answer with no fetch running; dropped"
            );
            return;
        };
        let sends = match step {
            LayerStep::Send(sends) => sends,
            LayerStep::Bad(bad) => {
                debug!(key = bad.0, ?req, "bad or unrequested hashes; banning");
                let sends = self
                    .loaded
                    .as_mut()
                    .and_then(|l| l.layer_fetch.as_mut())
                    .map(|fetch| fetch.on_disconnect(bad, now))
                    .unwrap_or_default();
                self.ban(bad);
                sends
            }
        };
        self.send_hash_requests(sends);
    }

    /// A peer finished its handshake: it is a layer source if it set the v2
    /// bit and a fetch runs.
    pub(super) fn on_peer_v2(&mut self, key: PeerKey, v2: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        debug!(key = key.0, v2, "peer v2 connection");
        p.v2 = v2;
        if v2 && p.connected && p.tx.is_some() {
            self.add_layer_source(key);
        }
    }

    /// A peer that can serve layers joins the fetch's sources, asked for
    /// spans as its slot frees. With no fetch running it is a plain peer:
    /// its hash messages are dropped, not held against it.
    fn add_layer_source(&mut self, key: PeerKey) {
        let sends = self
            .loaded
            .as_mut()
            .and_then(|l| l.layer_fetch.as_mut())
            .map(|fetch| fetch.add_source(key, Instant::now()))
            .unwrap_or_default();
        self.send_hash_requests(sends);
    }

    /// The v2 peers already connected when a `Loaded` with a fetch appears
    /// (a magnet's metadata arrives while they are up) become sources at
    /// once.
    pub(super) fn sync_layer_sources(&mut self) {
        if !self
            .loaded
            .as_ref()
            .is_some_and(|l| l.layer_fetch.is_some())
        {
            return;
        }
        let v2: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.v2 && p.connected && p.tx.is_some())
            .map(|(k, _)| *k)
            .collect();
        for key in v2 {
            self.add_layer_source(key);
        }
    }

    /// A peer is gone: its spans go to the other sources.
    pub(super) fn layer_source_gone(&mut self, key: PeerKey) {
        let sends = self
            .loaded
            .as_mut()
            .and_then(|l| l.layer_fetch.as_mut())
            .map(|fetch| fetch.on_disconnect(key, Instant::now()))
            .unwrap_or_default();
        self.send_hash_requests(sends);
    }

    /// Advances the layer fetch's timers, with what the readers are waiting
    /// for asked before the rest.
    pub(super) fn layer_tick(&mut self) {
        self.layer_prioritise();
        let sends = self
            .loaded
            .as_mut()
            .and_then(|l| l.layer_fetch.as_mut())
            .map(|fetch| fetch.tick(Instant::now()))
            .unwrap_or_default();
        self.send_hash_requests(sends);
    }

    /// Puts the spans of the files a waiting read sits on ahead of the
    /// others: without its layer a file's pieces are not even requested, so
    /// the read is blocked on exactly those hashes.
    pub(super) fn layer_prioritise(&mut self) {
        let Some(roots) = self.loaded.as_ref().and_then(|l| {
            l.layer_fetch
                .as_ref()
                .map(|_| reader_roots(&self.readers, &l.meta))
        }) else {
            return;
        };
        if roots.is_empty() {
            return;
        }
        if let Some(fetch) = self.loaded.as_mut().and_then(|l| l.layer_fetch.as_mut()) {
            fetch.prioritise(&roots);
        }
    }

    fn send_hash_requests(&mut self, sends: Vec<(PeerKey, HashRequest)>) {
        for (key, req) in sends {
            debug!(key = key.0, ?req, "hash request sent");
            self.send(key, PeerCommand::RequestHashes(req));
        }
    }
}

/// The `pieces root`s of the files still being fetched that an open reader
/// sits on, one per file: `pieces` are sorted and deduplicated by the
/// caller, so each target costs two binary searches.
fn reader_roots(readers: &Readers, meta: &Metainfo) -> Vec<[u8; 32]> {
    let mut pieces: Vec<u32> = readers.reading_pieces().collect();
    if pieces.is_empty() {
        return Vec::new();
    }
    pieces.sort_unstable();
    pieces.dedup();
    let mut out: Vec<[u8; 32]> = Vec::new();
    for t in fetch_targets(meta) {
        let Ok(count) = u32::try_from(t.count) else {
            continue;
        };
        let Some(end) = t.first_piece.checked_add(count) else {
            continue;
        };
        let from = pieces.partition_point(|p| *p < t.first_piece);
        if !pieces.get(from).is_some_and(|p| *p < end) || out.contains(&t.root) {
            continue;
        }
        out.push(t.root);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbt_core::merkle::{file_tree_height, pad_hash, piece_root, verify_hashes};
    use fbt_metainfo::CreateTorrent;
    use std::ops::ControlFlow;

    const PIECE: u32 = 32_768;

    /// A hybrid of one 300 000-byte file (ten pieces) without its layer,
    /// and the file's bytes.
    fn hybrid_without_layer() -> (Metainfo, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(root.join("c"), &data).unwrap();
        let bytes = CreateTorrent::new(&root)
            .version(TorrentVersion::Hybrid)
            .piece_len(PIECE)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .unwrap();
        let mut m = Metainfo::from_bytes(&bytes).unwrap();
        assert_eq!(m.piece_layers.len(), 1);
        m.piece_layers.clear();
        (m, data)
    }

    fn roots(data: &[u8]) -> Vec<[u8; 32]> {
        data.chunks(PIECE as usize)
            .map(|c| piece_root(c, (PIECE / 16_384) as usize))
            .collect()
    }

    #[test]
    fn built_layer_matches_pieces_root() {
        let (m, data) = hybrid_without_layer();
        let mut b = LayerBuilder::new(&m).expect("a layer is missing");
        let roots = roots(&data);
        // Out of order, with a piece lost and found again.
        b.forget(3);
        for (i, r) in roots.iter().enumerate().rev().skip(1) {
            assert!(b.record(i as u32, *r).is_none());
        }
        b.forget(5);
        assert!(b.record(9, roots[9]).is_none(), "piece 5 is missing");
        let (root, layer) = b.record(5, roots[5]).expect("complete");
        assert_eq!(Some(root), m.info.files[0].pieces_root);
        assert_eq!(layer, roots);
        assert_eq!(
            m.with_piece_layers([(root, layer)].into())
                .piece_layers
                .len(),
            1
        );
    }

    #[test]
    fn built_layer_that_disagrees_is_discarded() {
        let (m, data) = hybrid_without_layer();
        let mut b = LayerBuilder::new(&m).expect("a layer is missing");
        let mut roots = roots(&data);
        roots[4][0] ^= 1;
        for (i, r) in roots.iter().enumerate() {
            assert!(b.record(i as u32, *r).is_none());
        }
        // Discarded for good: recording again yields nothing either.
        assert!(b.record(4, roots[4]).is_none());
        assert!(b.files.iter().all(|f| f.done && f.slots.is_empty()));
    }

    #[test]
    fn no_builder_without_missing_layers() {
        let (mut m, data) = hybrid_without_layer();
        let root = m.info.files[0].pieces_root.unwrap();
        m = m.with_piece_layers([(root, roots(&data))].into());
        assert!(LayerBuilder::new(&m).is_none());
    }

    /// The hybrid of [`hybrid_without_layer`] with its layer: one file of
    /// ten pieces, tree height 5, piece layer at height 1 (16 wide padded).
    fn hybrid_with_layer() -> (Metainfo, [u8; 32]) {
        let (m, data) = hybrid_without_layer();
        let root = m.info.files[0].pieces_root.unwrap();
        (m.with_piece_layers([(root, roots(&data))].into()), root)
    }

    const PIECE_HEIGHT: u32 = 1;

    fn req(root: [u8; 32], base: u32, index: u32, length: u32, proof: u32) -> HashRequest {
        HashRequest {
            pieces_root: root,
            base_layer: base,
            index,
            length,
            proof_layers: proof,
        }
    }

    #[test]
    fn serve_answers_piece_layer_spans() {
        let (m, root) = hybrid_with_layer();
        let tree_height = file_tree_height(m.info.files[0].len);
        assert_eq!(tree_height, 5);
        for index in [0, 4, 8, 12] {
            let bytes =
                serve_hash_request(&m, &req(root, PIECE_HEIGHT, index, 4, 10)).expect("served");
            let all: Vec<[u8; 32]> = bytes.as_chunks::<32>().0.to_vec();
            assert_eq!(bytes.len(), all.len() * 32);
            // Four hashes, then uncles from height 3 to the root: 2 of them.
            assert_eq!(all.len(), 4 + 2);
            let (hashes, proof) = all.split_at(4);
            assert!(verify_hashes(
                &root,
                tree_height,
                PIECE_HEIGHT,
                index as usize,
                hashes,
                proof
            ));
        }
        // The base layer itself is the piece layer, padded with pad hashes.
        let bytes = serve_hash_request(&m, &req(root, PIECE_HEIGHT, 8, 4, 0)).unwrap();
        let (_, data) = hybrid_without_layer();
        let r = roots(&data);
        assert_eq!(&bytes[..64], [r[8], r[9]].concat().as_slice());
        let pad = pad_hash(PIECE_HEIGHT);
        assert_eq!(&bytes[64..], [pad, pad].concat().as_slice());
        // A higher layer serves too.
        let bytes = serve_hash_request(&m, &req(root, 3, 0, 4, 10)).unwrap();
        let all: Vec<[u8; 32]> = bytes.as_chunks::<32>().0.to_vec();
        assert!(verify_hashes(
            &root,
            tree_height,
            3,
            0,
            &all[..4],
            &all[4..]
        ));
    }

    #[test]
    fn requests_we_cannot_serve_are_rejected() {
        let (m, root) = hybrid_with_layer();
        let serve = |r: HashRequest| serve_hash_request(&m, &r);
        // Below the piece layer: we keep no block hashes.
        assert_eq!(serve(req(root, 0, 0, 4, 0)), Err(ServeError::Reject));
        // Unknown root.
        assert_eq!(
            serve(req([7; 32], PIECE_HEIGHT, 0, 4, 0)),
            Err(ServeError::Reject)
        );
        // A span past the padded layer (16 wide).
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 16, 16, 0)),
            Err(ServeError::Reject)
        );
        // Wider than the layer.
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 0, 32, 0)),
            Err(ServeError::Reject)
        );
        // A layer we lack.
        let (without, _) = hybrid_without_layer();
        assert_eq!(
            serve_hash_request(&without, &req(root, PIECE_HEIGHT, 0, 4, 0)),
            Err(ServeError::Reject)
        );
        // Malformed.
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 0, 3, 0)),
            Err(ServeError::Malformed)
        );
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 0, 0, 0)),
            Err(ServeError::Malformed)
        );
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 0, 16_384, 0)),
            Err(ServeError::Malformed)
        );
        assert_eq!(
            serve(req(root, PIECE_HEIGHT, 5, 4, 0)),
            Err(ServeError::Malformed)
        );
        for base in [5, 9, u32::MAX] {
            assert_eq!(serve(req(root, base, 0, 1, 0)), Err(ServeError::Malformed));
        }
    }

    #[test]
    fn cached_tree_serves_the_same_bytes() {
        let (m, root) = hybrid_with_layer();
        let r = req(root, PIECE_HEIGHT, 4, 4, 10);
        let (fresh, built) = serve_from(&m, &r, None);
        let tree = built.expect("a tree was built");
        let (cached, again) = serve_from(&m, &r, Some(tree));
        assert!(again.is_none(), "a cached tree is not rebuilt");
        assert_eq!(fresh, cached);
        assert!(fresh.is_ok());
        // Nothing is built for requests refused before the layer.
        assert!(serve_from(&m, &req(root, 0, 0, 4, 0), None).1.is_none());
    }

    #[test]
    fn serving_slots_are_capped_and_given_back() {
        let mut s = HashServing::default();
        let mut peers = [0usize; MAX_SERVING_JOBS + 1];
        // One peer reaches the torrent cap alone.
        for _ in 0..MAX_SERVING_JOBS {
            assert!(s.try_start(&mut peers[0]));
        }
        assert!(!s.try_start(&mut peers[1]), "torrent cap");
        assert_eq!(
            (s.jobs, peers[0], peers[1]),
            (MAX_SERVING_JOBS, MAX_SERVING_JOBS, 0)
        );
        for _ in 0..MAX_SERVING_JOBS {
            s.finish(Some(&mut peers[0]));
        }
        assert_eq!((s.jobs, peers[0]), (0, 0));
        // The per-peer cap holds below the torrent's.
        let mut full = MAX_PENDING_HASH_REQUESTS;
        assert!(!s.try_start(&mut full));
        assert_eq!(s.jobs, 0);
        // A job whose peer left gives back the torrent's slot only.
        assert!(s.try_start(&mut peers[2]));
        s.finish(None);
        assert_eq!(s.jobs, 0);
        s.finish(None);
        assert_eq!(s.jobs, 0, "never below zero");
    }

    /// A v2 torrent of `files` (name, length) with 32 KiB pieces, with its
    /// layers, and the same without them.
    fn v2_meta(files: &[(&str, usize)]) -> (Metainfo, Metainfo) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        for (i, (name, len)) in files.iter().enumerate() {
            let data: Vec<u8> = (0..*len).map(|b| ((b * 7 + i) % 251) as u8).collect();
            std::fs::write(root.join(name), data).unwrap();
        }
        let bytes = CreateTorrent::new(&root)
            .version(TorrentVersion::V2)
            .piece_len(PIECE)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .unwrap();
        let full = Metainfo::from_bytes(&bytes).unwrap();
        let mut stripped = full.clone();
        stripped.piece_layers.clear();
        (full, stripped)
    }

    /// Three files: 10 pieces, 4 pieces and one piece (no layer).
    fn three_files() -> (Metainfo, Metainfo) {
        let (full, stripped) = v2_meta(&[("a", 300_000), ("b", 100_000), ("c", 5_000)]);
        assert_eq!(full.piece_layers.len(), 2);
        (full, stripped)
    }

    fn serve(full: &Metainfo, req: &HashRequest) -> Vec<u8> {
        serve_hash_request(full, req).expect("served")
    }

    const A: PeerKey = PeerKey(1);
    const B: PeerKey = PeerKey(2);

    #[test]
    fn fetch_request_geometry() {
        let (full, stripped) = v2_meta(&[("c", 300_000)]);
        let file = &stripped.info.files[0];
        let h = file_tree_height(file.len);
        let p = PIECE_HEIGHT;
        let mut f = LayerFetch::new(&stripped, &[]).expect("a layer is missing");
        let sends = f.add_source(A, Instant::now());
        assert_eq!(
            sends,
            [(A, req(file.pieces_root.unwrap(), p, 0, 16, h - p - 4))]
        );
        // Nothing to fetch with the layer, or for a hybrid.
        assert!(LayerFetch::new(&full, &[]).is_none());
        assert!(!missing_layers(&full));
        let (hybrid, _) = hybrid_without_layer();
        assert!(LayerFetch::new(&hybrid, &[]).is_none());
    }

    /// What a fetch is built for and what `TorrentStatus::layers_missing`
    /// counts come from the same walk of the files.
    #[test]
    fn status_and_fetch_share_one_predicate() {
        let (full, stripped) = v2_meta(&[("a", 300_000), ("b", 100), ("c", 40_000)]);
        let (one_piece_full, one_piece) = v2_meta(&[("a", 100)]);
        let (hybrid, _) = hybrid_without_layer();
        for m in [&full, &stripped, &one_piece_full, &one_piece, &hybrid] {
            assert_eq!(missing_layers(m), LayerFetch::new(m, &[]).is_some());
        }
        // `TorrentStatus::layers_missing` counts what the metadata lacks, hybrid
        // included; a fetch runs only where a v2-only torrent cannot verify.
        for (m, want) in [
            (&full, 0),
            (&stripped, 2),
            (&one_piece_full, 0),
            (&one_piece, 0),
            (&hybrid, 1),
        ] {
            assert_eq!(missing_layer_count(m), want, "{:?}", m.version());
        }
        // Every multi-piece file lacking its layer is fetched: none is left
        // out, so the download can always finish.
        let f = LayerFetch::new(&stripped, &[]).unwrap();
        let want: Vec<_> = stripped
            .info
            .files
            .iter()
            .filter(|e| !e.pad && e.len > u64::from(stripped.info.piece_len))
            .filter_map(|e| e.pieces_root)
            .collect();
        assert_eq!(want.len(), 2);
        assert_eq!(f.files.iter().map(|x| x.root).collect::<Vec<_>>(), want);
    }

    /// A v2 torrent of one 600-piece file (16 KiB pieces: a padded layer
    /// 1024 wide, asked for in two spans of 512), with and without its layer.
    fn wide_without_layer() -> (Metainfo, Metainfo) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        let data: Vec<u8> = (0..600 * 16_384u32).map(|i| (i % 253) as u8).collect();
        std::fs::write(root.join("w"), &data).unwrap();
        let bytes = CreateTorrent::new(&root)
            .version(TorrentVersion::V2)
            .piece_len(16_384)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .unwrap();
        let full = Metainfo::from_bytes(&bytes).unwrap();
        let mut stripped = full.clone();
        stripped.piece_layers.clear();
        (full, stripped)
    }

    #[test]
    fn fetch_splits_wide_layers() {
        // The second span is partly padding.
        let (full, stripped) = wide_without_layer();
        let pr = full.info.files[0].pieces_root.unwrap();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let sends = f.add_source(A, now);
        assert_eq!(
            sends,
            [(A, req(pr, 0, 0, 512, 1)), (A, req(pr, 0, 512, 512, 1))]
        );
        for (k, r) in sends {
            let step = f.on_hashes(k, &r, &serve(&full, &r), now);
            assert_eq!(step, LayerStep::Send(Vec::new()));
        }
        assert!(f.is_complete());
        let ready = f.take_ready();
        assert_eq!(ready.len(), 1, "one file, one layer");
        assert_eq!(full.piece_layers.get(&pr), Some(&ready[0].1));
        assert!(f.take_ready().is_empty(), "a layer is handed over once");
    }

    /// Wanted files' layers are asked for first: a skipped file's hashes must
    /// not delay the layer its download waits for. With every file wanted the
    /// order is the torrent's own.
    #[test]
    fn wanted_layers_are_asked_for_first() {
        let (_, stripped) = three_files();
        // `files` holds pad files too, so the targets name their own indices.
        let targets: Vec<(usize, [u8; 32])> =
            fetch_targets(&stripped).map(|t| (t.file, t.root)).collect();
        assert_eq!(targets.len(), 2, "two multi-piece files");
        let (file_a, pr_a) = targets[0];
        let (_, pr_b) = targets[1];

        // Every file wanted (empty priorities): the torrent's own order.
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let sends = f.add_source(A, Instant::now());
        assert_eq!(
            sends.iter().map(|(_, r)| r.pieces_root).collect::<Vec<_>>(),
            [pr_a, pr_b],
            "every layer is wanted"
        );

        // The first target's file skipped: the other's hashes go out first.
        let mut prio = vec![Priority::Normal; stripped.info.files.len()];
        prio[file_a] = Priority::Skip;
        let mut f = LayerFetch::new(&stripped, &prio).unwrap();
        assert_eq!(
            f.files.iter().map(|x| x.wanted).collect::<Vec<_>>(),
            [false, true],
            "only the second file is wanted"
        );
        let sends = f.add_source(A, Instant::now());
        assert_eq!(
            sends.iter().map(|(_, r)| r.pieces_root).collect::<Vec<_>>(),
            [pr_b, pr_a],
            "the wanted file's layer is asked for first"
        );
    }

    /// Only files with wanted pieces hold `Seeding` back, and only while their
    /// hashes are still outstanding: a skipped file's layer is nobody's
    /// download, and a span received asks for nothing.
    #[test]
    fn only_wanted_files_hold_seeding_back() {
        let (full, stripped) = three_files();
        let targets: Vec<usize> = fetch_targets(&stripped).map(|t| t.file).collect();
        let roots: Vec<[u8; 32]> = fetch_targets(&stripped).map(|t| t.root).collect();
        assert_eq!(targets.len(), 2);

        // Empty priorities mean every file is `Normal`, so both roots matter.
        assert!(
            roots.iter().all(|r| root_is_wanted(&stripped, &[], *r)),
            "every file is wanted"
        );

        // Skip one file: only the other root matters to seeding.
        let mut skip_one = vec![Priority::Normal; stripped.info.files.len()];
        skip_one[targets[0]] = Priority::Skip;
        assert!(!root_is_wanted(&stripped, &skip_one, roots[0]));
        assert!(
            root_is_wanted(&stripped, &skip_one, roots[1]),
            "the other file still wants its layer"
        );

        // Skip both: nothing outstanding is wanted.
        let skip_all = vec![Priority::Skip; stripped.info.files.len()];
        assert!(
            roots
                .iter()
                .all(|r| !root_is_wanted(&stripped, &skip_all, *r)),
            "a skipped file's layer is nobody's download"
        );

        // What the metadata holds asks for nothing, and a fetch that received
        // every span owes no root at all.
        assert!(roots.iter().all(|r| root_is_wanted(&full, &[], *r)));
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        assert_eq!(f.owed_roots().collect::<Vec<_>>(), roots);
        let now = Instant::now();
        let mut sends = f.add_source(A, now);
        while let Some((k, r)) = sends.pop() {
            match f.on_hashes(k, &r, &serve(&full, &r), now) {
                LayerStep::Send(more) => sends.extend(more),
                LayerStep::Bad(k) => panic!("{k:?} judged bad"),
            }
        }
        assert!(f.is_complete());
        assert_eq!(f.owed_roots().next(), None, "nothing is outstanding");
    }

    /// The verify queue joins what merges and keeps what has not been read:
    /// overlapping runs coalesce, and an answered pass leaves behind the parts
    /// of them it did not cover.
    #[test]
    fn verify_ranges_join_and_split() {
        let mut q: Vec<Range<u32>> = Vec::new();
        // Apart runs stay apart, sorted; overlapping and adjacent ones join.
        push_ranges(&mut q, [8..10, 0..4]);
        assert_eq!(q, vec![0..4, 8..10], "apart runs, in order");
        push_ranges(&mut q, [3..6, 10..12]);
        assert_eq!(q, vec![0..6, 8..12], "overlapping and adjacent runs join");
        // A pass that read `2..4` and `11..12` leaves the ends it did not
        // cover held back until a pass reaches them.
        drop_ranges(&mut q, &[2..4, 11..12]);
        assert_eq!(q, vec![0..2, 4..6, 8..11], "the unread parts survive");
        // A pass over everything empties the queue; one more changes nothing.
        drop_ranges(&mut q, &[0..6, 6..32]);
        assert!(q.is_empty(), "everything was read");
        drop_ranges(&mut q, &[0..1, 2..3]);
        assert!(q.is_empty());
    }

    /// Only pieces that are not ours yet need reading: a piece that passed
    /// already proved itself against the hashes we held then.
    #[test]
    fn unread_ranges_skips_verified_pieces() {
        let mut have = Bitfield::new(10);
        have.set(1);
        have.set(4);
        assert_eq!(unread_ranges(Some(&have), 0..8), vec![0..1, 2..4, 5..8]);
        // Bits outside the range neither split it nor extend it: `9` is ours,
        // `0..3` is asked for as two runs around the one verified piece.
        have.set(9);
        assert_eq!(unread_ranges(Some(&have), 0..3), vec![0..1, 2..3]);
        // Everything inside already passed: nothing to read.
        assert!(
            unread_ranges(Some(&Bitfield::full(8)), 2..6).is_empty(),
            "every piece of the range verified"
        );
        // With no picker nothing is known to be ours, so the whole run stands.
        let all = unread_ranges(None, 0..3);
        assert_eq!(all.len(), 1);
        assert_eq!((all[0].start, all[0].end), (0, 3), "no picker: read it all");
        assert!(
            unread_ranges(Some(&have), 4..4).is_empty(),
            "an empty range"
        );
    }

    /// Answers every span `fetch` asks of source `A` with the hashes `full`
    /// holds, until the fetch is complete.
    fn answer_all(fetch: &mut LayerFetch, full: &Metainfo) {
        let now = Instant::now();
        let mut sends = fetch.add_source(A, now);
        while let Some((k, r)) = sends.pop() {
            match fetch.on_hashes(k, &r, &serve(full, &r), now) {
                LayerStep::Send(more) => sends.extend(more),
                LayerStep::Bad(key) => panic!("{key:?} judged bad"),
            }
        }
        assert!(
            fetch.is_complete(),
            "every span answered from the torrent itself"
        );
    }

    /// A fetched layer's pieces may lie on disk unverified — data arrives
    /// before the hashes do — so merging one queues a read of them and starts
    /// it. A hybrid's built layer is made of pieces that already passed their
    /// checks, so merging it asks for no second look and spawns no job:
    /// provenance and version are what tell the two apart.
    #[tokio::test(flavor = "current_thread")]
    async fn fetched_layers_are_reread_and_built_ones_are_not() {
        use super::super::swarm::tests::actor_over;

        // v2-only, its one multi-piece file lacking its layer: a fetch runs.
        let (full, stripped) = v2_meta(&[("only", 300_000)]);
        let mut a = actor_over(stripped, 0);
        {
            let l = a.loaded.as_mut().expect("loaded");
            let mut f = l.layer_fetch.take().expect("a fetch for the missing layer");
            answer_all(&mut f, &full);
            l.layer_fetch = Some(f);
        }
        a.swap_layers(true);
        {
            let l = a.loaded.as_ref().expect("loaded");
            let want = {
                let mut v = Vec::new();
                push_ranges(&mut v, [0..6, 6..10]);
                v
            };
            assert_eq!(l.pending_verify, want, "the fetched pieces await a read");
            assert!(l.verifying, "the pass runs at once");
        }
        assert_eq!(a.jobs.len(), 1, "one check job for the re-read");

        // Hybrid without its layer: the same hashes arrive as a *built* layer.
        let (stripped, data) = hybrid_without_layer();
        let root = stripped.info.files[0].pieces_root.expect("a v2 root");
        let mut a = actor_over(stripped, 0);
        {
            let l = a.loaded.as_mut().expect("loaded");
            assert!(
                l.layer_fetch.is_none(),
                "a hybrid builds, it does not fetch"
            );
            l.built_layers.insert(root, roots(&data));
        }
        a.swap_layers(true);
        {
            let l = a.loaded.as_ref().expect("loaded");
            assert_eq!(l.meta.piece_layers.len(), 1, "the built layer merged");
            assert!(
                l.pending_verify.is_empty(),
                "its pieces passed with the hashes we already held"
            );
            assert!(!l.verifying);
        }
        assert_eq!(a.jobs.len(), 0, "no verify pass for a built layer");
    }

    /// The pass is a check, so it holds one of the `max_active_checks` slots
    /// for its whole run. With every slot held elsewhere it neither runs nor
    /// queues for one: the pieces stay held back, the torrent keeps ticking,
    /// and the next tick, merge or result tries again — which is how
    /// `spawn_check`'s bound to `max_active_checks` (see `disk.rs`) holds.
    #[tokio::test(flavor = "current_thread")]
    async fn no_check_slot_means_no_pass_yet() {
        use super::super::swarm::tests::actor_over;

        let (full, stripped) = v2_meta(&[("only", 300_000)]);
        let mut a = actor_over(stripped.clone(), 0);
        {
            let l = a.loaded.as_mut().expect("loaded");
            let mut f = l.layer_fetch.take().expect("a fetch for the missing layer");
            answer_all(&mut f, &full);
            l.layer_fetch = Some(f);
        }
        // Hold every check slot the pool allows. It is one in these helpers;
        // acquiring until that fails works for any size.
        let checks = a.shared.disk.check_permit();
        let mut slots = Vec::new();
        while let Ok(p) = Arc::clone(&checks).try_acquire_owned() {
            slots.push(p);
        }
        assert!(!slots.is_empty(), "the pool allows at least one check");
        a.swap_layers(true);
        {
            let l = a.loaded.as_ref().expect("loaded");
            assert_eq!(l.pending_verify.len(), 1, "the pieces stay held back");
            assert!(!l.verifying, "no pass without a slot");
            assert!(l.layer_work(), "and the torrent keeps ticking for it");
        }
        assert_eq!(a.jobs.len(), 0, "nothing spawned and nothing queued");
        // A slot coming free lets the next attempt run it.
        drop(slots);
        a.verify_merged_pieces();
        assert!(
            a.loaded.as_ref().expect("loaded").verifying,
            "the pass runs once a slot is free"
        );
        assert_eq!(a.jobs.len(), 1, "one job for it");
    }

    #[test]
    fn fetch_completes_from_one_source() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).expect("layers are missing");
        let now = Instant::now();
        let mut sends = f.add_source(A, now);
        assert_eq!(sends.len(), 2, "one span per file");
        while let Some((k, r)) = sends.pop() {
            assert_eq!(k, A);
            match f.on_hashes(k, &r, &serve(&full, &r), now) {
                LayerStep::Send(more) => sends.extend(more),
                LayerStep::Bad(k) => panic!("{k:?} judged bad"),
            }
        }
        assert!(f.is_complete());
        let layers: BTreeMap<_, _> = f.take_ready().into_iter().collect();
        assert_eq!(layers, full.piece_layers);
        assert_eq!(stripped.with_piece_layers(layers).piece_layers.len(), 2);
    }

    /// One file's layer is complete while the other still owes a span: it
    /// goes to the actor at once, and only once.
    #[test]
    fn a_layer_is_ready_before_the_fetch_is() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).expect("layers are missing");
        let now = Instant::now();
        let mut sends = f.add_source(A, now);
        assert_eq!(sends.len(), 2, "one span per file");
        let (k, r) = sends.pop().unwrap();
        assert_eq!(
            f.on_hashes(k, &r, &serve(&full, &r), now),
            LayerStep::Send(Vec::new())
        );
        assert!(!f.is_complete(), "the other file still owes its span");
        let ready = f.take_ready();
        assert_eq!(ready.len(), 1, "the answered file is done");
        assert_eq!(full.piece_layers.get(&ready[0].0), Some(&ready[0].1));
        assert!(f.take_ready().is_empty(), "a layer is handed over once");
        // The span still in flight lands: so does the other file.
        let (k, r) = sends.pop().unwrap();
        assert_eq!(
            f.on_hashes(k, &r, &serve(&full, &r), now),
            LayerStep::Send(Vec::new())
        );
        assert!(f.is_complete());
        assert_eq!(f.take_ready().len(), 1);
    }

    /// What a source delivered before it died is not asked for again: the
    /// new source answers only what was missing, and the layer that results
    /// holds both parts.
    #[test]
    fn partial_progress_survives_its_source() {
        let (full, stripped) = wide_without_layer();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let mut sends = f.add_source(A, now);
        assert_eq!(sends.len(), 2);
        let (k, r) = sends.pop().unwrap();
        assert_eq!(
            f.on_hashes(k, &r, &serve(&full, &r), now),
            LayerStep::Send(Vec::new())
        );
        assert!(
            f.on_disconnect(A, now).is_empty(),
            "no source left to ask yet"
        );
        let sends = f.add_source(B, now);
        assert_eq!(sends.len(), 1, "only the unanswered span is asked again");
        let (k, r) = sends[0];
        assert_eq!(
            f.on_hashes(k, &r, &serve(&full, &r), now),
            LayerStep::Send(Vec::new())
        );
        assert!(f.is_complete());
        let ready = f.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(
            full.piece_layers.get(&ready[0].0),
            Some(&ready[0].1),
            "the layer holds both spans"
        );
    }

    /// A read waiting on a file gets that file's spans asked for first; a
    /// root no file has changes nothing.
    #[test]
    fn waiting_readers_reorder_the_queue() {
        let (_full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let roots: Vec<[u8; 32]> = f.files.iter().map(|x| x.root).collect();
        assert_eq!(roots.len(), 2);
        let order = |f: &LayerFetch| f.todo.iter().map(|s| s.file).collect::<Vec<_>>();
        let before = order(&f);
        f.prioritise(&[[7; 32]]);
        assert_eq!(order(&f), before, "a root no file has is ignored");
        f.prioritise(&[roots[1]]);
        let sends = f.add_source(A, Instant::now());
        assert_eq!(
            sends.first().map(|(_, r)| r.pieces_root),
            Some(roots[1]),
            "the waiting file is asked for first"
        );
    }

    #[test]
    fn unsolicited_hashes_retire_the_source() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let sends = f.add_source(A, now);
        let (_, asked) = sends[0];
        // The right root at a span never asked for.
        let other = req(
            asked.pieces_root,
            PIECE_HEIGHT,
            0,
            8,
            asked.proof_layers + 1,
        );
        let bytes = serve(&full, &other);
        assert_eq!(f.on_hashes(A, &other, &bytes, now), LayerStep::Bad(A));
        // A peer that is no source at all.
        let bytes = serve(&full, &asked);
        assert_eq!(f.on_hashes(B, &asked, &bytes, now), LayerStep::Bad(B));
        // A's spans went back: the next source gets them.
        assert_eq!(f.add_source(B, now).len(), 2);
    }

    #[test]
    fn bad_proof_retires_the_source() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let sends = f.add_source(A, now);
        let (_, r) = sends[0];
        let mut bytes = serve(&full, &r);
        bytes[40] ^= 1;
        assert_eq!(f.on_hashes(A, &r, &bytes, now), LayerStep::Bad(A));
        // A short answer is as bad.
        let sends = f.add_source(B, now);
        let (_, r) = sends[0];
        let bytes = serve(&full, &r);
        assert_eq!(f.on_hashes(B, &r, &bytes[32..], now), LayerStep::Bad(B));
        assert!(!f.is_complete());
    }

    #[test]
    fn reject_moves_the_span() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let sends = f.add_source(A, now);
        assert!(f.add_source(B, now).is_empty(), "A holds every span");
        let (_, r) = sends[0];
        let moved = f.on_reject(A, &r, now);
        assert!(moved.contains(&(B, r)), "{moved:?}");
        assert!(moved.iter().all(|(k, _)| *k == B));
        // A reject for nothing asked changes nothing.
        assert!(f.on_reject(A, &r, now).is_empty());
        for (k, r) in moved {
            assert_eq!(
                f.on_hashes(k, &r, &serve(&full, &r), now),
                LayerStep::Send(Vec::new())
            );
        }
        assert!(f.is_complete());
    }

    #[test]
    fn timeout_retires_without_ban() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let first = f.add_source(A, now);
        assert!(f.add_source(B, now).is_empty());
        assert!(f.tick(now + LAYER_REQUEST_TIMEOUT / 2).is_empty());
        let later = now + LAYER_REQUEST_TIMEOUT;
        let moved = f.tick(later);
        assert_eq!(moved.len(), first.len());
        assert!(moved.iter().all(|(k, _)| *k == B));
        // A's late answer is ignored, not judged.
        let (_, r) = first[0];
        assert_eq!(
            f.on_hashes(A, &r, &serve(&full, &r), later),
            LayerStep::Send(Vec::new())
        );
        // B times out as well; A, retired as long ago, is asked again.
        let again = f.tick(later + LAYER_REQUEST_TIMEOUT);
        assert!(!again.is_empty());
        assert!(again.iter().all(|(k, _)| *k == A), "{again:?}");
    }

    #[test]
    fn late_answer_after_timeout_is_not_bad() {
        // Five files of two pieces: five spans, one more than a source holds.
        let names = ["f0", "f1", "f2", "f3", "f4"];
        let files: Vec<(&str, usize)> = names.iter().map(|n| (*n, 40_000)).collect();
        let (full, stripped) = v2_meta(&files);
        let mut f = LayerFetch::new(&stripped, &[]).unwrap();
        let now = Instant::now();
        let first = f.add_source(A, now);
        assert_eq!(first.len(), SPANS_PER_SOURCE);
        assert_eq!(f.add_source(B, now).len(), 1);
        // Both time out; back as long after, they share the spans anew.
        f.tick(now + LAYER_REQUEST_TIMEOUT);
        let again = f.tick(now + 2 * LAYER_REQUEST_TIMEOUT);
        let at_b: Vec<HashRequest> = again
            .iter()
            .filter(|(k, _)| *k == B)
            .map(|(_, r)| *r)
            .collect();
        // A's first ask of a span now in flight at B: late, kept.
        let late = *first
            .iter()
            .map(|(_, r)| r)
            .find(|r| at_b.contains(r))
            .expect("a span moved to B");
        let later = now + 2 * LAYER_REQUEST_TIMEOUT;
        assert!(matches!(
            f.on_hashes(A, &late, &serve(&full, &late), later),
            LayerStep::Send(_)
        ));
        assert_eq!(f.spans_left, names.len() - 1);
        // B's answer to that span is now late too: ignored.
        assert!(matches!(
            f.on_hashes(B, &late, &serve(&full, &late), later),
            LayerStep::Send(_)
        ));
        // A late answer failing its proof is ignored, not judged.
        let other = *first
            .iter()
            .map(|(_, r)| r)
            .find(|r| at_b.contains(r) && **r != late)
            .expect("another span moved to B");
        let mut bad = serve(&full, &other);
        bad[0] ^= 1;
        assert_eq!(
            f.on_hashes(A, &other, &bad, later),
            LayerStep::Send(Vec::new())
        );
        assert_eq!(f.spans_left, names.len() - 1);
        // A span never asked of B is still bad.
        let never = first
            .iter()
            .map(|(_, r)| *r)
            .find(|r| !at_b.contains(r) && *r != late)
            .expect("a span B was never asked");
        assert!(!again.contains(&(B, never)));
        assert_eq!(
            f.on_hashes(B, &never, &serve(&full, &never), later),
            LayerStep::Bad(B)
        );
    }
}
