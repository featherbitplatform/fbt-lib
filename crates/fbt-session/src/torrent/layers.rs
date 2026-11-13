//! BEP 52 piece layers: building a hybrid's missing layers from the
//! subtree roots of its verified pieces.

use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::{MAX_PIECE_LAYER_HASHES, Metainfo, TorrentVersion, validate_layer};
use tokio::time::Instant;
use tracing::warn;

use super::actor::Actor;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbt_core::merkle::piece_root;
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
}
