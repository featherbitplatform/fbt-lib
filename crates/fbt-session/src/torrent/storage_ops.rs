//! Moving the storage and renaming files.
//!
//! An operation first writes every verified piece (`flush_and_wait`), then
//! makes the storage busy: no write is issued (new verified pieces stay
//! dirty in the cache), peers' requests and readers' reads that miss the
//! cache wait (in `PieceReads` and `readers.waiting`), and commands that
//! touch the storage queue up behind it. Once the writes have landed, no
//! read is running and no check reads the data, one disk-pool job moves
//! the files. When it returns the waiting reads start, from the new place.

use std::io;
use std::path::PathBuf;

use fbt_metainfo::{RelPath, collides};
use tokio::sync::oneshot;
use tracing::warn;

use super::Cmd;
use super::actor::{Actor, Phase};
use super::data::Job;
use crate::error::SessionError;
use crate::event::Event;
use crate::storage::cache::FlushReason;
use crate::storage::{MoveMode, Storage};

/// Commands held while the storage is busy; further ones wait in the
/// command channel.
pub(super) const MAX_DEFERRED_CMDS: usize = 64;

/// Where an operation's answer goes.
type Reply = oneshot::Sender<Result<(), SessionError>>;

/// What a storage operation does.
#[derive(Debug, Clone)]
pub(super) enum OpKind {
    Move { path: PathBuf, mode: MoveMode },
    Rename { index: usize, path: RelPath },
}

/// The storage operation under way.
#[derive(Debug)]
pub(super) struct StorageOp {
    kind: OpKind,
    tx: Reply,
    /// Its disk job is running.
    spawned: bool,
}

impl Cmd {
    /// Waits for a storage operation under way: the command touches the
    /// storage (writes, checks, moves, deletes) or needs every verified
    /// piece on disk before it answers.
    pub(super) fn waits_for_storage(&self) -> bool {
        matches!(
            self,
            Cmd::Pause(_)
                | Cmd::Resume(_)
                | Cmd::Remove(..)
                | Cmd::Recheck(_)
                | Cmd::SetPriorities(..)
                | Cmd::Shutdown(_)
                | Cmd::MoveStorage { .. }
                | Cmd::RenameFile { .. }
        )
    }
}

impl Actor {
    pub(super) async fn on_move_storage(&mut self, path: PathBuf, mode: MoveMode, tx: Reply) {
        if self.loaded.is_none() {
            // A magnet without metadata has no files yet: only the place
            // they will be created at changes.
            let custom = self.magnet.as_ref().is_some_and(|m| m.storage.is_some());
            if custom {
                self.move_failed(SessionError::Unsupported, tx);
                return;
            }
            if let Some(m) = self.magnet.as_mut() {
                m.save_path.clone_from(&path);
            }
            self.moved(path, tx);
            return;
        }
        if path == self.save_path {
            self.moved(path, tx);
            return;
        }
        self.begin_storage_op(OpKind::Move { path, mode }, tx).await;
    }

    pub(super) async fn on_rename_file(&mut self, index: usize, path: RelPath, tx: Reply) {
        let Some(l) = self.loaded.as_ref() else {
            let _ = tx.send(Err(SessionError::NoMetadata));
            return;
        };
        let mut files = l.meta.info.files.clone();
        let count = files.len();
        if files.get(index).is_none_or(|f| f.pad) {
            let _ = tx.send(Err(SessionError::InvalidFileIndex {
                index,
                files: count,
            }));
            return;
        }
        for (&i, p) in &l.renamed {
            if let Some(f) = files.get_mut(i) {
                f.path = p.clone();
            }
        }
        if collides(&files, index, &path) {
            let _ = tx.send(Err(SessionError::InvalidPath(path.to_string())));
            return;
        }
        self.begin_storage_op(OpKind::Rename { index, path }, tx)
            .await;
    }

    /// Writes every verified piece, then holds the storage for `kind`; its
    /// job starts once nothing else uses the storage.
    async fn begin_storage_op(&mut self, kind: OpKind, tx: Reply) {
        self.flush_and_wait().await;
        self.storage_busy = true;
        self.storage_op = Some(StorageOp {
            kind,
            tx,
            spawned: false,
        });
        self.try_start_storage_job();
    }

    /// Starts the operation's disk job once no write is in flight, no read
    /// runs, no check reads the data and no flush runs. Called after every
    /// job result.
    pub(super) fn try_start_storage_job(&mut self) {
        let checking = matches!(
            self.phase,
            Phase::Checking { .. } | Phase::QueuedForCheck { .. }
        );
        let idle = self.reads_in_flight == 0
            && !self.cache.writes_in_flight()
            && !self.finish_flush
            && !checking;
        let Some(op) = self.storage_op.as_mut().filter(|op| !op.spawned) else {
            return;
        };
        if !idle {
            return;
        }
        let Some(storage) = self.storage.clone() else {
            // Unreachable: storage comes with the metadata.
            self.on_storage_done(Err(io::ErrorKind::Unsupported.into()));
            return;
        };
        op.spawned = true;
        let kind = op.kind.clone();
        self.shared
            .disk
            .spawn(&mut self.jobs, move || Job::StorageOp {
                result: run_op(&*storage, &kind),
            });
    }

    /// The operation's job returned: the actor records the outcome,
    /// answers, and lets the held reads and writes go.
    pub(super) fn on_storage_done(&mut self, result: io::Result<()>) {
        let Some(op) = self.storage_op.take() else {
            return;
        };
        self.storage_busy = false;
        match (op.kind, result) {
            (OpKind::Move { path, mode }, Ok(())) => {
                if mode == MoveMode::KeepExisting {
                    // The files kept at the target may differ from ours:
                    // checked (after the pieces verified meanwhile are
                    // written) before the answer.
                    self.save_path.clone_from(&path);
                    self.emit(Event::StorageMoved {
                        torrent: self.ih,
                        path,
                    });
                    self.recheck_after_move = Some(op.tx);
                } else {
                    self.moved(path, op.tx);
                }
            }
            (OpKind::Move { .. }, Err(e)) => {
                warn!(torrent = %self.name(), error = %e, "moving the storage failed");
                self.move_failed(storage_error(e), op.tx);
            }
            (OpKind::Rename { index, path }, Ok(())) => {
                let shown = path.to_string();
                if let Some(l) = self.loaded.as_mut() {
                    l.renamed.insert(index, path);
                }
                self.emit(Event::FileRenamed {
                    torrent: self.ih,
                    index,
                    path: shown,
                });
                let _ = op.tx.send(Ok(()));
            }
            (OpKind::Rename { index, .. }, Err(e)) => {
                warn!(torrent = %self.name(), index, error = %e, "renaming a file failed");
                let e = storage_error(e);
                self.emit(Event::FileRenameFailed {
                    torrent: self.ih,
                    index,
                    message: e.to_string(),
                });
                let _ = op.tx.send(Err(e));
            }
        }
        self.resume_storage_io();
    }

    /// The storage is free again: the held piece reads start, the readers'
    /// reads of pieces we have are served, and the pieces verified
    /// meanwhile go to their writes.
    fn resume_storage_io(&mut self) {
        for piece in self.piece_reads.take_unstarted() {
            self.spawn_piece_read(piece);
        }
        self.serve_waiting_reads();
        let finished = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(crate::picker::PiecePicker::is_finished);
        if finished || !self.active() {
            self.flush(FlushReason::All);
        } else {
            self.flush(FlushReason::Pressure);
        }
        self.maybe_finish();
        self.refresh_state();
    }

    /// The move succeeded (or had nothing to do): the save path is `path`.
    fn moved(&mut self, path: PathBuf, tx: Reply) {
        self.save_path.clone_from(&path);
        self.publish();
        self.emit(Event::StorageMoved {
            torrent: self.ih,
            path,
        });
        let _ = tx.send(Ok(()));
    }

    fn move_failed(&mut self, e: SessionError, tx: Reply) {
        self.emit(Event::StorageMoveFailed {
            torrent: self.ih,
            message: e.to_string(),
        });
        let _ = tx.send(Err(e));
    }

    /// After a [`MoveMode::KeepExisting`] move: every verified piece is
    /// written, the data checked, then the caller answered.
    pub(super) async fn finish_keep_existing(&mut self) {
        let Some(tx) = self.recheck_after_move.take() else {
            return;
        };
        self.flush_and_wait().await;
        self.recheck();
        self.publish();
        let _ = tx.send(Ok(()));
    }
}

/// Runs `kind` on `storage`, a panic reported as an I/O error so the
/// operation always ends.
fn run_op(storage: &dyn Storage, kind: &OpKind) -> io::Result<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match kind {
        OpKind::Move { path, mode } => storage.move_to(path, *mode),
        OpKind::Rename { index, path } => storage.rename_file(*index, path),
    }))
    .unwrap_or_else(|_| Err(io::Error::other("the storage panicked")))
}

/// The session error for a failed storage operation.
fn storage_error(e: io::Error) -> SessionError {
    if e.kind() == io::ErrorKind::Unsupported {
        SessionError::Unsupported
    } else {
        SessionError::Storage(e)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Condvar, Mutex};

    use tokio::sync::oneshot::error::TryRecvError;

    use super::super::swarm::tests::actor_with;
    use super::*;
    use crate::storage::MemStorage;

    const PL: u32 = 16_384;

    /// Memory storage whose reads wait until `open` and whose moves are
    /// recorded.
    #[derive(Debug)]
    struct Gated {
        inner: MemStorage,
        gate: (Mutex<bool>, Condvar),
        moves: Mutex<Vec<PathBuf>>,
    }

    impl Gated {
        fn open(&self) {
            *self.gate.0.lock().unwrap() = true;
            self.gate.1.notify_all();
        }

        fn moves(&self) -> Vec<PathBuf> {
            self.moves.lock().unwrap().clone()
        }
    }

    impl Storage for Gated {
        fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
            let open = self.gate.0.lock().unwrap();
            let (_open, timeout) = self
                .gate
                .1
                .wait_timeout_while(open, std::time::Duration::from_secs(30), |o| !*o)
                .unwrap();
            assert!(!timeout.timed_out(), "the gate never opened");
            self.inner.read(offset, buf)
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.inner.write(offset, data)
        }
        fn flush(&self) -> io::Result<()> {
            Ok(())
        }
        fn delete_files(&self) -> io::Result<()> {
            Ok(())
        }
        fn move_to(&self, save_path: &Path, _: MoveMode) -> io::Result<()> {
            self.moves.lock().unwrap().push(save_path.to_path_buf());
            Ok(())
        }
    }

    /// A whole-piece read still running holds the move job back until it
    /// returns; a read asked for meanwhile waits for the move.
    #[tokio::test]
    async fn move_waits_for_running_reads() {
        let mut a = actor_with(&[4 * u64::from(PL)], PL, 0);
        let storage = Arc::new(Gated {
            inner: MemStorage::new(4 * u64::from(PL)),
            gate: (Mutex::new(false), Condvar::new()),
            moves: Mutex::new(Vec::new()),
        });
        a.storage = Some(Arc::clone(&storage) as Arc<dyn Storage>);
        a.start_piece_read(1, 0, Vec::new());
        assert_eq!(a.reads_in_flight, 1);

        let (tx, mut rx) = oneshot::channel();
        let target = PathBuf::from("elsewhere");
        a.on_move_storage(target.clone(), MoveMode::FailIfExists, tx)
            .await;
        assert!(a.storage_busy);
        assert_eq!(a.jobs.len(), 1, "the move started beside a read");
        // A read asked for now is held, not started.
        a.start_piece_read(2, 0, Vec::new());
        assert_eq!(a.reads_in_flight, 1);
        assert_eq!(a.jobs.len(), 1);

        storage.open();
        let r = a.jobs.join_next().await.expect("the read");
        a.on_job_result(r);
        assert_eq!(a.reads_in_flight, 0);
        assert_eq!(a.jobs.len(), 1, "the move job did not start");
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        let r = a.jobs.join_next().await.expect("the move");
        a.on_job_result(r);
        assert_eq!(storage.moves(), vec![target.clone()]);
        assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        assert!(!a.storage_busy);
        assert_eq!(a.save_path, target);
        // The held read started once the move was done.
        assert_eq!(a.reads_in_flight, 1);
        let r = a.jobs.join_next().await.expect("the held read");
        a.on_job_result(r);
        assert_eq!(a.reads_in_flight, 0);
    }
}
