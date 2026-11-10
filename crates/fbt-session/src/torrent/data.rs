//! Downloaded data: blocks are buffered per piece in memory, a complete
//! piece is hashed and then written in `spawn_blocking`, and only a written
//! piece counts as ours.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef};
use fbt_metainfo::{Metainfo, MetainfoError};
use tracing::debug;

use super::actor::{Actor, Phase};
use super::swarm::BanTarget;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::sha1_piece;

/// Result of a blocking job, fed back into the actor loop.
#[derive(Debug)]
pub(crate) enum Job {
    /// `check_pieces` finished.
    Checked {
        generation: u64,
        result: io::Result<Bitfield>,
    },
    /// A downloaded piece was hashed; `data` comes back for the write.
    Hashed {
        generation: u64,
        piece: u32,
        ok: bool,
        data: Vec<u8>,
    },
    /// A verified piece was written.
    Written {
        generation: u64,
        piece: u32,
        result: io::Result<()>,
    },
    /// Storage was flushed.
    Flushed(io::Result<()>),
    /// A complete metadata buffer from `key` was verified and parsed.
    Metadata {
        key: PeerKey,
        /// Banned if the buffer fails the hash, even if the peer is gone.
        ban: Option<BanTarget>,
        result: Result<Box<Metainfo>, MetainfoError>,
    },
    /// A block requested by a peer was read.
    Read {
        generation: u64,
        key: PeerKey,
        block: BlockRef,
        result: io::Result<Vec<u8>>,
    },
}

impl Actor {
    pub(super) fn on_job(&mut self, job: Job) {
        match job {
            Job::Checked { generation, result } => self.on_checked(generation, result),
            Job::Hashed {
                generation,
                piece,
                ok,
                data,
            } => self.on_hashed(generation, piece, ok, data),
            Job::Written {
                generation,
                piece,
                result,
            } => self.on_written(generation, piece, result),
            Job::Read {
                generation,
                key,
                block,
                result,
            } => self.on_read(generation, key, block, result),
            Job::Flushed(Err(e)) => {
                tracing::warn!(torrent = %self.name(), error = %e, "flush failed");
            }
            Job::Flushed(Ok(())) => {}
            Job::Metadata { key, ban, result } => self.on_metadata_job(key, ban, result),
        }
    }

    /// A block we requested arrived from `key`.
    pub(super) fn on_block(&mut self, key: PeerKey, block: BlockRef, data: Bytes) {
        let len = data.len() as u64;
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        p.requested.remove(&block);
        let current = p.generation == generation && p.tx.is_some();
        let ready = matches!(self.phase, Phase::Ready);
        let Some(l) = self.loaded.as_mut() else {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        let picker = match l.picker.as_mut() {
            Some(pk) if current && ready => pk,
            _ => {
                self.counters.wasted = self.counters.wasted.saturating_add(len);
                return;
            }
        };
        let res = picker.on_block(key, block);
        if !res.accepted {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        }
        let size = l.layout.piece_size(block.piece) as usize;
        let buf = l.bufs.entry(block.piece).or_insert_with(|| vec![0; size]);
        let start = block.offset as usize;
        let Some(dst) = start
            .checked_add(data.len())
            .and_then(|end| buf.get_mut(start..end))
        else {
            // The picker only accepts exact layout blocks; unreachable.
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        dst.copy_from_slice(&data);
        self.counters.downloaded = self.counters.downloaded.saturating_add(len);
        self.counters.dl.record(len);
        if let Some(p) = self.peers.get_mut(&key) {
            p.from_rate.record(len);
        }
        // End-game: the other holders of this block no longer need it.
        for owner in res.cancel {
            if let Some(o) = self.peers.get_mut(&owner) {
                o.requested.remove(&block);
            }
            self.send(owner, PeerCommand::Cancel(block));
        }
        if res.piece_complete
            && let Some(data) = self
                .loaded
                .as_mut()
                .and_then(|l| l.bufs.remove(&block.piece))
        {
            self.spawn_hash(block.piece, data);
        }
    }

    fn spawn_hash(&mut self, piece: u32, data: Vec<u8>) {
        let Some(l) = self.loaded.as_ref() else {
            return;
        };
        let meta = Arc::clone(&l.meta);
        let generation = self.generation;
        self.jobs.spawn_blocking(move || {
            let ok = meta.info.pieces.get(piece as usize) == Some(&sha1_piece(&data));
            Job::Hashed {
                generation,
                piece,
                ok,
                data,
            }
        });
    }

    fn on_hashed(&mut self, generation: u64, piece: u32, ok: bool, data: Vec<u8>) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if ok {
            let Some(storage) = self.storage.as_ref() else {
                return;
            };
            let storage = Arc::clone(storage);
            let offset = l.layout.piece_offset(piece);
            self.jobs.spawn_blocking(move || Job::Written {
                generation,
                piece,
                result: storage.write(offset, &data),
            });
            return;
        }
        debug!(torrent = %l.meta.info.name, piece, "piece failed its hash check");
        if let Some(picker) = l.picker.as_mut() {
            picker.piece_failed(piece);
        }
        let size = u64::from(l.layout.piece_size(piece));
        self.counters.wasted = self.counters.wasted.saturating_add(size);
        self.emit(Event::HashFailed {
            torrent: self.ih,
            piece,
        });
        self.top_up_all(None);
    }

    fn on_written(&mut self, generation: u64, piece: u32, result: io::Result<()>) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        if let Err(e) = result {
            self.fail(&e, "writing a piece");
            return;
        }
        let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) else {
            return;
        };
        picker.piece_passed(piece);
        let finished = picker.is_finished();
        self.emit(Event::PieceFinished {
            torrent: self.ih,
            piece,
        });
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            let Some(p) = self.peers.get(&key) else {
                continue;
            };
            if !p.connected || p.tx.is_none() {
                continue;
            }
            let recheck_interest = p.am_interested && p.has(piece);
            self.send(key, PeerCommand::Have(piece));
            if recheck_interest {
                self.update_interest(key);
            }
        }
        let just_finished = finished && !self.finished_emitted;
        if just_finished {
            self.finished_emitted = true;
            // Whoever reacts to the event reads the final counters and state.
            self.publish();
            self.emit(Event::TorrentFinished { torrent: self.ih });
            // Durability point; a failure here is logged, the next write
            // would surface a real storage problem.
            if let Some(storage) = self.storage.clone() {
                self.jobs
                    .spawn_blocking(move || Job::Flushed(storage.flush()));
            }
        }
        self.refresh_state();
        if just_finished {
            // After `refresh_state`, so the announce reads `left` = 0.
            self.signal(|s| !std::mem::replace(&mut s.completed, true));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{OnceLock, Weak};

    use fbt_core::PeerId;
    use fbt_tracker::TrackerClient;
    use tokio::sync::{broadcast, mpsc, watch};

    use super::*;
    use crate::session::{AddTorrent, Shared};
    use crate::settings::Settings;
    use crate::status::{TorrentState, TorrentStatus};
    use crate::storage::{MemStorage, Storage};
    use crate::torrent::actor::EMITTED;
    use crate::torrent::swarm::Peer;

    /// Whoever reacts to `TorrentFinished` reads the final status: every
    /// downloaded byte counted and the torrent seeding, not the last tick's
    /// snapshot.
    #[tokio::test]
    async fn torrent_finished_sees_the_final_status() {
        const LEN: usize = 32_768;
        let payload: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
        let mut bytes =
            format!("d4:infod6:lengthi{LEN}e4:name1:a12:piece lengthi{LEN}e6:pieces20:")
                .into_bytes();
        bytes.extend_from_slice(&sha1_piece(&payload));
        bytes.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&bytes).expect("metainfo");
        let settings = Settings {
            dht_enabled: false,
            dht_bootstrap_nodes: Vec::new(),
            ..Settings::default()
        };
        let tracker = TrackerClient::new(settings.tracker.clone()).expect("tracker client");
        let shared = Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            listen_port: 6881,
            listen_port_v4: Some(6881),
            listen_port_v6: None,
            tracker,
            events: broadcast::channel(64).0,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
        });
        let storage: Arc<dyn Storage> = Arc::new(MemStorage::new(LEN as u64));
        let mut add = AddTorrent::new(meta, PathBuf::from("unused"));
        add.storage = Some(Arc::clone(&storage));
        let ih = add.source.info_hashes();
        let (status_tx, _status_rx) = watch::channel(TorrentStatus::initial(ih, "a".into(), true));
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let mut actor = Actor::new(
            add,
            Some(storage),
            Arc::new(OnceLock::new()),
            shared,
            Weak::new(),
            cmd_rx,
            status_tx,
        );
        actor.on_checked(actor.generation, Ok(Bitfield::new(1)));

        let key = PeerKey(0);
        let (tx, _peer_rx) = mpsc::channel(16);
        let mut peer = Peer::new(
            SocketAddr::from(([10, 0, 0, 1], 6881)),
            tx,
            actor.generation,
            1,
            crate::peer::transport::Transport::Tcp,
        );
        peer.connected = true;
        peer.seed = true;
        let blocks = actor
            .loaded
            .as_mut()
            .and_then(|l| l.picker.as_mut())
            .expect("picker")
            .pick(key, peer.pieces(), 16, &mut actor.rng);
        peer.requested.extend(blocks.iter().copied());
        actor.peers.insert(key, peer);
        assert_eq!(blocks.len(), 2);
        for b in blocks {
            let start = b.offset as usize;
            let end = start + b.len as usize;
            actor.on_block(key, b, Bytes::copy_from_slice(&payload[start..end]));
        }
        // Hashed, then Written (which finishes), then Flushed.
        while let Some(r) = actor.jobs.join_next().await {
            actor.on_job_result(r);
        }

        let log = EMITTED.take();
        let (_, seen) = log
            .iter()
            .find(|(e, _)| matches!(e, Event::TorrentFinished { .. }))
            .expect("TorrentFinished");
        assert_eq!(seen.total_downloaded, LEN as u64);
        assert_eq!(seen.total_wasted, 0);
        assert_eq!(seen.state, TorrentState::Seeding);
    }
}
