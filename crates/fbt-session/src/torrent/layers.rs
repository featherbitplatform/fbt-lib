//! BEP 52 piece layers: building a hybrid's missing layers from the
//! subtree roots of its verified pieces, and answering peers' `hash
//! request`s from the layers we hold.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_core::merkle::{MERKLE_BLOCK_LEN, MerkleTree, file_tree_height, verify_hashes};
use fbt_metainfo::{FileEntry, MAX_PIECE_LAYER_HASHES, Metainfo, TorrentVersion, validate_layer};
use fbt_wire::{HashRequest, MAX_HASH_REQUEST_LEN};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::actor::Actor;
use super::data::Job;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::session::Priority;
use crate::storage::Storage;

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
/// layer: the torrent must fetch layers before its download starts.
pub(super) fn missing_layers(meta: &Metainfo) -> bool {
    meta.version() == TorrentVersion::V2 && fetch_targets(meta).next().is_some()
}

/// The multi-piece files of `meta` whose layer is missing.
fn fetch_targets(meta: &Metainfo) -> impl Iterator<Item = &FileEntry> {
    let pl = u64::from(meta.info.piece_len);
    meta.info.files.iter().filter(move |f| {
        !f.pad
            && f.len > pl
            && f.pieces_root
                .is_some_and(|r| !meta.piece_layers.contains_key(&r))
    })
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
    /// The layer, one hash per piece of the file; filled span by span.
    layer: Vec<[u8; 32]>,
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

/// Fetching the piece layers a v2-only torrent lacks from peers that set
/// the v2 bit (BEP 52 `hash request`). Pure: no I/O, `now` is passed in.
///
/// Each multi-piece file's padded layer (`W = 2^(H - p)` wide for tree
/// height `H` and piece height `p`) is asked for in spans of
/// `min(LAYER_REQUEST_LEN, W)` hashes at the piece layer, with the uncles
/// up to (excluding) the root, so every answer is checked against the
/// file's `pieces root` before it is kept. Spans lying wholly in the
/// padding are not asked for: they hold only pad hashes. At most
/// [`MAX_LAYER_SOURCES`] peers are asked at once, each with at most
/// `SPANS_PER_SOURCE` spans in flight.
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
}

impl LayerFetch {
    /// A fetch of the layers `meta` lacks; `None` unless `meta` is v2-only
    /// and a multi-piece file lacks its layer. Files past
    /// [`MAX_PIECE_LAYER_HASHES`] hashes in all are left out.
    pub(super) fn new(meta: &Metainfo) -> Option<Self> {
        if !missing_layers(meta) {
            return None;
        }
        let pl = u64::from(meta.info.piece_len);
        let piece_height = (meta.info.piece_len / MERKLE_BLOCK_LEN).trailing_zeros();
        let mut files = Vec::new();
        let mut todo = VecDeque::new();
        let mut total: usize = 0;
        for f in fetch_targets(meta) {
            let Some(root) = f.pieces_root else {
                continue;
            };
            let tree_height = file_tree_height(f.len);
            // A multi-piece file's padded layer is at least 2 wide; its
            // indices are `u32`s.
            let Some(w_log) = tree_height
                .checked_sub(piece_height)
                .filter(|w| (1..32).contains(w))
            else {
                continue;
            };
            let length = LAYER_REQUEST_LEN.min(1 << w_log);
            let (Ok(count), Ok(step)) =
                (usize::try_from(f.len.div_ceil(pl)), usize::try_from(length))
            else {
                continue;
            };
            if total.saturating_add(count) > MAX_PIECE_LAYER_HASHES {
                break;
            }
            total += count;
            let file = files.len();
            for index in (0..count).step_by(step) {
                if let Ok(index) = u32::try_from(index) {
                    todo.push_back(Span { file, index });
                }
            }
            files.push(FetchFile {
                root,
                tree_height,
                length,
                proof_layers: w_log - length.trailing_zeros(),
                layer: vec![[0; 32]; count],
            });
        }
        let by_root = files.iter().enumerate().map(|(i, f)| (f.root, i)).collect();
        (!todo.is_empty()).then(|| LayerFetch {
            piece_height,
            files,
            by_root,
            asked: HashMap::new(),
            done: HashSet::new(),
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

    /// A connected peer that set the v2 bit: a source now, or once a slot
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

    /// The fetched layers by `pieces root` (complete once
    /// [`is_complete`](Self::is_complete)).
    pub(super) fn into_layers(self) -> BTreeMap<[u8; 32], Vec<[u8; 32]>> {
        self.files.into_iter().map(|f| (f.root, f.layer)).collect()
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

    /// Merges the built layers into the actor's metainfo: at most once per
    /// [`LAYER_SWAP_INTERVAL`], or now when `force` (completion). Only
    /// `Loaded.meta` changes: the handle's `metainfo()` keeps the layers
    /// known when it was set.
    pub(super) fn swap_layers(&mut self, force: bool) {
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if l.built_layers.is_empty() {
            return;
        }
        let now = Instant::now();
        if !force
            && l.layers_swapped
                .is_some_and(|t| now - t < LAYER_SWAP_INTERVAL)
        {
            return;
        }
        let layers = std::mem::take(&mut l.built_layers);
        l.meta = Arc::new((*l.meta).clone().with_piece_layers(layers));
        l.layers_swapped = Some(now);
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
    /// beyond either and before the metadata.
    pub(super) fn on_hash_request(&mut self, key: PeerKey, req: HashRequest) {
        if !self.peers.contains_key(&key) || self.close_if_v1_only(key) {
            return;
        }
        let (Some(l), Some(p)) = (self.loaded.as_mut(), self.peers.get_mut(&key)) else {
            self.send(key, PeerCommand::RejectHashes(req));
            return;
        };
        if !l.hash_serving.try_start(&mut p.hash_requests) {
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
    /// ban the peer as bad metadata does.
    pub(super) fn on_hash_answer(&mut self, key: PeerKey, req: HashRequest, hashes: Option<Bytes>) {
        if self.close_if_v1_only(key) {
            return;
        }
        let Some(p) = self.pending_layers.as_mut() else {
            debug!(key = key.0, ?req, "unrequested hash answer dropped");
            return;
        };
        let now = Instant::now();
        let sends = match hashes {
            None => p.fetch.on_reject(key, &req, now),
            Some(h) => match p.fetch.on_hashes(key, &req, &h, now) {
                LayerStep::Send(sends) => sends,
                LayerStep::Bad(bad) => {
                    debug!(key = bad.0, ?req, "bad or unrequested hashes; banning");
                    let sends = p.fetch.on_disconnect(bad, now);
                    self.ban(bad);
                    sends
                }
            },
        };
        self.send_hash_requests(sends);
        self.finish_layer_fetch();
    }

    /// A peer finished its handshake: it is a layer source if it set the v2
    /// bit and a fetch runs.
    pub(super) fn on_peer_v2(&mut self, key: PeerKey, v2: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.v2 = v2;
        if v2 && p.connected && p.tx.is_some() {
            self.add_layer_source(key);
        }
    }

    fn add_layer_source(&mut self, key: PeerKey) {
        let Some(p) = self.pending_layers.as_mut() else {
            return;
        };
        let sends = p.fetch.add_source(key, Instant::now());
        self.send_hash_requests(sends);
    }

    /// A peer is gone: its spans go to the other sources.
    pub(super) fn layer_source_gone(&mut self, key: PeerKey) {
        let Some(p) = self.pending_layers.as_mut() else {
            return;
        };
        let sends = p.fetch.on_disconnect(key, Instant::now());
        self.send_hash_requests(sends);
    }

    /// Advances the layer fetch's timers.
    pub(super) fn layer_tick(&mut self) {
        let Some(p) = self.pending_layers.as_mut() else {
            return;
        };
        let sends = p.fetch.tick(Instant::now());
        self.send_hash_requests(sends);
    }

    fn send_hash_requests(&mut self, sends: Vec<(PeerKey, HashRequest)>) {
        for (key, req) in sends {
            self.send(key, PeerCommand::RequestHashes(req));
        }
    }

    /// The metadata of a v2-only torrent lacking layers: held in
    /// `pending_layers` (in `Phase::Metadata`) while they are fetched from
    /// the connected v2 peers and those still to come.
    pub(super) fn await_layers(&mut self, pending: PendingLayers) {
        self.pending_layers = Some(Box::new(pending));
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

    /// Once every layer is in: the metadata, with them, is installed as a
    /// magnet's is (storage, check, download).
    fn finish_layer_fetch(&mut self) {
        if !self
            .pending_layers
            .as_ref()
            .is_some_and(|p| p.fetch.is_complete())
        {
            return;
        }
        let Some(p) = self.pending_layers.take() else {
            return;
        };
        let p = *p;
        let meta = Arc::unwrap_or_clone(p.meta).with_piece_layers(p.fetch.into_layers());
        self.install_loaded(meta, p.storage, p.file_prio, p.sequential);
    }
}

/// A v2-only torrent whose metadata is known, waiting for the piece layers
/// it lacks; what `install_loaded` needs once they are in.
#[derive(Debug)]
pub(super) struct PendingLayers {
    pub meta: Arc<Metainfo>,
    pub fetch: LayerFetch,
    /// The custom storage from `AddTorrent`; `None` builds an `FsStorage`.
    pub storage: Option<Arc<dyn Storage>>,
    pub file_prio: Vec<Priority>,
    pub sequential: bool,
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
        let mut f = LayerFetch::new(&stripped).expect("a layer is missing");
        let sends = f.add_source(A, Instant::now());
        assert_eq!(
            sends,
            [(A, req(file.pieces_root.unwrap(), p, 0, 16, h - p - 4))]
        );
        // Nothing to fetch with the layer, or for a hybrid.
        assert!(LayerFetch::new(&full).is_none());
        assert!(!missing_layers(&full));
        let (hybrid, _) = hybrid_without_layer();
        assert!(LayerFetch::new(&hybrid).is_none());
    }

    #[test]
    fn fetch_splits_wide_layers() {
        // 600 pieces of 16 KiB: a padded layer 1024 wide, in two spans of
        // 512 (the second one partly padding).
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
        let pr = full.info.files[0].pieces_root.unwrap();
        let mut f = LayerFetch::new(&stripped).unwrap();
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
        assert_eq!(f.into_layers(), full.piece_layers);
    }

    #[test]
    fn fetch_completes_from_one_source() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped).expect("layers are missing");
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
        let layers = f.into_layers();
        assert_eq!(layers, full.piece_layers);
        assert_eq!(stripped.with_piece_layers(layers).piece_layers.len(), 2);
    }

    #[test]
    fn unsolicited_hashes_retire_the_source() {
        let (full, stripped) = three_files();
        let mut f = LayerFetch::new(&stripped).unwrap();
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
        let mut f = LayerFetch::new(&stripped).unwrap();
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
        let mut f = LayerFetch::new(&stripped).unwrap();
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
        let mut f = LayerFetch::new(&stripped).unwrap();
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
        let mut f = LayerFetch::new(&stripped).unwrap();
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
