//! BEP 52 piece layers: building a hybrid's missing layers from the
//! subtree roots of its verified pieces, and answering peers' `hash
//! request`s from the layers we hold.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_core::merkle::{MERKLE_BLOCK_LEN, MerkleTree, file_tree_height};
use fbt_metainfo::{MAX_PIECE_LAYER_HASHES, Metainfo, TorrentVersion, validate_layer};
use fbt_wire::{HashRequest, MAX_HASH_REQUEST_LEN};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::actor::Actor;
use super::data::Job;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;

/// Most `hash request`s of one peer being served at once; further ones are
/// rejected.
pub(super) const MAX_PENDING_HASH_REQUESTS: usize = 64;

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
pub(super) fn serve_hash_request(
    meta: &Metainfo,
    req: &HashRequest,
) -> Result<Vec<u8>, ServeError> {
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
    let layer = meta
        .piece_layers
        .get(&req.pieces_root)
        .ok_or(ServeError::Reject)?;
    let (Ok(index), Ok(len)) = (usize::try_from(req.index), usize::try_from(length)) else {
        return Err(ServeError::Reject);
    };
    let tree =
        MerkleTree::from_layer(layer, piece_height, tree_height).ok_or(ServeError::Reject)?;
    let (hashes, uncles) = tree
        .hashes(req.base_layer, index, len, req.proof_layers)
        .ok_or(ServeError::Reject)?;
    Ok(hashes.iter().chain(&uncles).flatten().copied().collect())
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

    /// A peer's `hash request`: served off the actor from `Loaded.meta`,
    /// at most [`MAX_PENDING_HASH_REQUESTS`] at once per peer; rejected
    /// beyond that and before the metadata.
    pub(super) fn on_hash_request(&mut self, key: PeerKey, req: HashRequest) {
        if !self.peers.contains_key(&key) || self.close_if_v1_only(key) {
            return;
        }
        let meta = self.loaded.as_ref().map(|l| Arc::clone(&l.meta));
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        let Some(meta) = meta.filter(|_| p.hash_requests < MAX_PENDING_HASH_REQUESTS) else {
            self.send(key, PeerCommand::RejectHashes(req));
            return;
        };
        p.hash_requests += 1;
        self.jobs.spawn_blocking(move || Job::HashesServed {
            key,
            req,
            result: serve_hash_request(&meta, &req),
        });
    }

    /// A served `hash request` came back: answered, rejected, or the peer
    /// closed for a malformed request.
    pub(super) fn on_hashes_served(
        &mut self,
        key: PeerKey,
        req: HashRequest,
        result: Result<Vec<u8>, ServeError>,
    ) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.hash_requests = p.hash_requests.saturating_sub(1);
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

    /// A peer's `hashes` or `hash reject`: nothing asks for layers yet, so
    /// it is dropped (closing the peer on a v1-only torrent).
    pub(super) fn on_hash_answer(&mut self, key: PeerKey, req: HashRequest, hashes: usize) {
        if !self.close_if_v1_only(key) {
            debug!(key = key.0, ?req, hashes, "unrequested hash answer dropped");
        }
    }
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
}
