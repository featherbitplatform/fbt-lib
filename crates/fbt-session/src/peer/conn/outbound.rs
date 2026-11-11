//! Torrent commands and everything we write.

use fbt_wire::{Message, WireError};
use futures_util::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout};

use super::state::Conn;
use super::{Flow, wire_len, wire_reason};
use crate::peer::messages::{DisconnectReason, PeerCommand, PeerEvent};

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    /// Asks the torrent for more blocks when we may request and the pipeline
    /// is below half full.
    pub(super) async fn want_blocks(&mut self) -> Flow {
        let may_request = !self.choked || (self.fast && !self.allowed_fast.is_empty());
        let max = self.settings.max_outstanding_requests;
        let n = self.outstanding.len();
        if may_request && n.saturating_mul(2) < max {
            self.emit(PeerEvent::WantBlocks {
                key: self.key,
                count: max - n,
            })
            .await?;
        }
        Ok(())
    }

    pub(super) async fn on_command(&mut self, c: PeerCommand) -> Flow {
        match c {
            PeerCommand::Request(blocks) => {
                let now = Instant::now();
                let mut rejected = Vec::new();
                let mut sent = false;
                for b in blocks {
                    if self.outstanding.contains_key(&b) {
                        continue;
                    }
                    let fast_ok = self.fast && self.allowed_fast.contains(&b.piece);
                    if self.choked && !fast_ok {
                        rejected.push(b);
                        continue;
                    }
                    self.feed(Message::Request(b)).await?;
                    self.outstanding.insert(b, now);
                    sent = true;
                }
                if sent {
                    self.flush().await?;
                }
                for block in rejected {
                    self.emit(PeerEvent::Rejected {
                        key: self.key,
                        block,
                    })
                    .await?;
                }
            }
            PeerCommand::Cancel(b) => {
                if self.outstanding.remove(&b).is_some() {
                    self.send(Message::Cancel(b)).await?;
                }
            }
            PeerCommand::Choke => {
                if !self.choking {
                    self.choking = true;
                    self.feed(Message::Choke).await?;
                    // Queued requests die with the choke, answered or not;
                    // with fast they are rejected explicitly.
                    self.uploads.clear();
                    for b in std::mem::take(&mut self.incoming) {
                        if self.fast {
                            self.feed(Message::RejectRequest(b)).await?;
                        }
                    }
                    self.flush().await?;
                }
            }
            PeerCommand::Unchoke => {
                if self.choking {
                    self.choking = false;
                    self.send(Message::Unchoke).await?;
                }
            }
            PeerCommand::Interested(on) => {
                if on != self.interested {
                    self.interested = on;
                    let m = if on {
                        Message::Interested
                    } else {
                        Message::NotInterested
                    };
                    self.send(m).await?;
                }
            }
            PeerCommand::Have(piece) => self.send(Message::Have(piece)).await?,
            PeerCommand::Haves(pieces) => {
                if !pieces.is_empty() {
                    for piece in pieces {
                        self.feed(Message::Have(piece)).await?;
                    }
                    self.flush().await?;
                }
            }
            PeerCommand::SendBlock { block, data } => {
                // Only answer requests still queued (not cancelled or choked
                // away); the block stays in `incoming` until it is written.
                if self.incoming.contains(&block) {
                    if self.uploads.len() >= self.settings.max_incoming_requests {
                        // Drop entries cancelled since: the live ones are
                        // all in `incoming`, which is bounded.
                        let incoming = &self.incoming;
                        self.uploads.retain(|(b, _)| incoming.contains(b));
                    }
                    self.uploads.push_back((block, data));
                }
            }
            PeerCommand::Reject(b) => {
                if self.incoming.remove(&b) && self.fast {
                    self.send(Message::RejectRequest(b)).await?;
                }
            }
            PeerCommand::MetadataReady { layout, ext } => self.metadata_ready(layout, ext).await?,
            PeerCommand::SendMetadata(msg) => {
                if let Some(m) = self.remote_ids.metadata(&msg) {
                    self.send(m).await?;
                }
            }
            PeerCommand::SendPex(msg) => {
                if let Some(m) = self.remote_ids.pex(&msg) {
                    self.send(m).await?;
                }
            }
            PeerCommand::Close => return Err(DisconnectReason::Shutdown),
        }
        Ok(())
    }

    /// Writes queued blocks while the upload throttle allows, each
    /// charged as it is written, then reports each one sent once flushed.
    pub(super) async fn drain_uploads(&mut self) -> Flow {
        let mut sent = Vec::new();
        while !self.uploads.is_empty() && self.up.ready_at(Instant::now()).is_none() {
            let Some((block, data)) = self.uploads.pop_front() else {
                break;
            };
            // Cancelled, rejected or choked away while queued.
            if !self.incoming.remove(&block) {
                continue;
            }
            let len = data.len() as u64;
            self.feed(Message::Piece {
                piece: block.piece,
                offset: block.offset,
                data,
            })
            .await?;
            sent.push((block, len));
        }
        if sent.is_empty() {
            return Ok(());
        }
        self.flush().await?;
        for (block, len) in sent {
            self.emit(PeerEvent::BlockSent {
                key: self.key,
                block,
                len,
            })
            .await?;
        }
        Ok(())
    }

    pub(super) async fn emit(&mut self, ev: PeerEvent) -> Flow {
        self.events
            .send(ev)
            .await
            .map_err(|_| DisconnectReason::Shutdown)
    }

    /// A write blocked for the idle timeout means the remote stopped reading.
    pub(super) fn write_done(
        &mut self,
        r: Result<Result<(), WireError>, tokio::time::error::Elapsed>,
    ) -> Flow {
        match r {
            Ok(Ok(())) => {
                self.last_send = Instant::now();
                Ok(())
            }
            Ok(Err(e)) => Err(wire_reason(e)),
            Err(_) => Err(DisconnectReason::Io("write stalled".to_owned())),
        }
    }

    /// Queues `m` for writing, charged to the upload throttle (never
    /// delayed by it: only blocks wait, in [`Self::drain_uploads`]).
    pub(super) async fn feed(&mut self, m: Message) -> Flow {
        self.up.charge(wire_len(&m));
        let r = timeout(self.settings.peer_idle_timeout, self.framed.feed(m)).await;
        self.write_done(r)
    }

    pub(super) async fn flush(&mut self) -> Flow {
        let r = timeout(self.settings.peer_idle_timeout, self.framed.flush()).await;
        self.write_done(r)
    }

    pub(super) async fn send(&mut self, m: Message) -> Flow {
        self.feed(m).await?;
        self.flush().await
    }
}
