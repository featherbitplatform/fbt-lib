//! Magnet torrents: the metadata fetch and installing what it returns.

use std::path::PathBuf;
use std::sync::Arc;

use fbt_core::{Bitfield, InfoHashes};
use fbt_metainfo::{Magnet, Metainfo, MetainfoError};
use tokio::time::Instant;

use super::actor::{Actor, Loaded, Phase};
use super::commands::selected_priorities;
use super::metadata::MetadataFetch;
use super::swarm::BanTarget;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::session::{Priority, wire_hashes};
use crate::storage::{FsStorage, Storage};

/// A magnet torrent until its metadata arrives.
#[derive(Debug)]
pub(super) struct MagnetState {
    pub fetch: MetadataFetch,
    pub magnet: Magnet,
    pub save_path: PathBuf,
    /// The custom storage from `AddTorrent`, used once the metadata is in.
    pub storage: Option<Arc<dyn Storage>>,
    pub sequential: bool,
}

impl MagnetState {
    /// A fresh fetch; completed buffers are verified in `spawn_blocking`.
    pub(super) fn new_fetch(&self, max_size: usize) -> MetadataFetch {
        MetadataFetch::new(self.magnet.info_hashes, max_size).deferred()
    }
}

impl Actor {
    /// A complete metadata buffer was hashed and parsed off the actor.
    pub(super) fn on_metadata_job(
        &mut self,
        key: PeerKey,
        ban: Option<BanTarget>,
        result: Result<Box<Metainfo>, MetainfoError>,
    ) {
        if self.loaded.is_some() || !matches!(self.phase, Phase::Metadata) {
            return;
        }
        match result {
            Ok(meta) => self.install_metadata(*meta),
            Err(MetainfoError::InfoHashMismatch) => {
                let verdict = self
                    .magnet
                    .as_mut()
                    .map(|m| m.fetch.on_verify_failed(key))
                    .unwrap_or_default();
                // A buffer mixed from several sources bans nobody.
                if verdict.ban {
                    if self.peers.contains_key(&key) {
                        self.ban(key);
                    } else if let Some(t) = ban {
                        self.apply_ban(t);
                    }
                }
                self.send_metadata(verdict.sends);
            }
            Err(e) => self.metadata_failed(format!("invalid metadata: {e}")),
        }
    }

    /// The verified metadata: installed at once, whatever layers it holds. A
    /// v2-only torrent lacking them starts fetching them beside the download
    /// (see [`Loaded::layer_fetch`]).
    pub(super) fn install_metadata(&mut self, meta: Metainfo) {
        let Some(m) = self.magnet.take() else {
            return;
        };
        let m = *m;
        let file_prio = selected_priorities(meta.info.files.len(), m.magnet.select_only.as_deref());
        self.register_revealed_hashes(&meta.info_hashes);
        self.install_loaded(meta, m.storage, file_prio, m.sequential);
    }

    /// The metadata, complete: build `Loaded` and the storage, tell the
    /// handle and every peer, then check the existing data as for a
    /// `.torrent`. `MetadataReceived` is emitted here, once.
    pub(super) fn install_loaded(
        &mut self,
        meta: Metainfo,
        storage: Option<Arc<dyn Storage>>,
        file_prio: Vec<Priority>,
        sequential: bool,
    ) {
        // Resume data of a magnet saved before its metadata names no
        // renamed file (it is refused otherwise), but the storage is built
        // the same way as at add.
        let renamed = self.resume.as_ref().map_or(&[][..], |r| &r.renamed[..]);
        let storage: Arc<dyn Storage> = match storage {
            Some(s) => s,
            None => Arc::new(FsStorage::with_paths(
                self.save_path.clone(),
                &meta,
                renamed,
            )),
        };
        let meta = Arc::new(meta);
        let mut loaded = Loaded::new(Arc::clone(&meta), file_prio, sequential);
        loaded.renamed = renamed.iter().cloned().collect();
        let layout = loaded.layout;
        let total_wanted = loaded.total_wanted;
        self.install(loaded, storage);
        // Peers that connected while the metadata was still arriving can
        // serve its layers: they become sources at once.
        self.sync_layer_sources();
        let _ = self.metainfo_cell.set(meta);
        // Whoever reacts to the event sees `has_metadata` already.
        self.publish();
        self.emit(Event::MetadataReceived { torrent: self.ih });
        // Peers were set up with no pieces; they get the layout now.
        let n = layout.num_pieces();
        self.leave_public_discovery();
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            let Some(addr) = self.peers.get(&key).map(|p| p.addr) else {
                continue;
            };
            let ext = self.ext_config(&addr);
            if let Some(p) = self.peers.get_mut(&key) {
                p.bits = Bitfield::new(n);
                p.fast_has = None;
                if p.have_sent.is_some() {
                    p.have_sent = Some(Bitfield::new(n));
                }
            }
            self.send(key, PeerCommand::MetadataReady { layout, ext });
        }
        // Until the check is done, `stopped` (if any) reports everything
        // as left.
        self.signal(|s| {
            s.left = total_wanted;
            true
        });
        self.metadata_check = true;
        self.start_check();
        self.refresh_state();
    }

    /// A torrent whose metadata says private: the magnet exception ends
    /// here, off the DHT and PEX at once.
    fn leave_public_discovery(&mut self) {
        if self.is_private() {
            self.dht_announcer = None;
            self.dht_rx = None;
            self.drop_pex_peers();
        }
    }

    /// Registers each wire hash of the verified metadata that the magnet's
    /// own hashes lacked (a hybrid's other hash) as an alias of this torrent,
    /// so peers naming it reach us. Not announced.
    fn register_revealed_hashes(&self, revealed: &InfoHashes) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let known = wire_hashes(&self.ih);
        for w in wire_hashes(revealed) {
            if !known.contains(&w) {
                session.add_alias(&self.ih, w);
            }
        }
    }

    /// Advances the metadata fetch's timers.
    pub(super) fn metadata_tick(&mut self) {
        let now = Instant::now();
        let sends = match self.magnet.as_mut() {
            Some(m) if matches!(self.phase, Phase::Metadata) => m.fetch.on_tick(now),
            _ => return,
        };
        self.send_metadata(sends);
        self.close_useless_metadata_peer(now);
    }
}
