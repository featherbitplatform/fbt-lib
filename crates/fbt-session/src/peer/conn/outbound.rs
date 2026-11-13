//! Torrent commands and everything we write.

use std::time::Duration;

use fbt_core::BLOCK_SIZE;
use fbt_wire::{Message, WireError};
use futures_util::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout};

use super::state::Conn;
use super::{Flow, wire_len, wire_reason};
use crate::peer::messages::{DisconnectReason, PeerCommand, PeerEvent};

/// Requests to keep in flight to a peer delivering `rate` bytes per second:
/// enough for `queue_time` at that rate, in 16 KiB blocks, at least 2 and
/// at most `max`, never beyond the remote's advertised `reqq` (at least 1),
/// and 1 while it is snubbed.
pub(super) fn pipeline_depth(
    rate: u64,
    queue_time: Duration,
    max: usize,
    reqq: Option<u32>,
    snubbed: bool,
) -> usize {
    if snubbed {
        return max.min(1);
    }
    let blocks =
        u128::from(rate).saturating_mul(queue_time.as_millis()) / (1000 * u128::from(BLOCK_SIZE));
    let depth = usize::try_from(blocks)
        .unwrap_or(usize::MAX)
        .max(2)
        .min(max);
    match reqq {
        Some(r) => depth.min(usize::try_from(r.max(1)).unwrap_or(usize::MAX)),
        None => depth,
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    /// The pipeline depth now: see [`pipeline_depth`].
    pub(super) fn target_depth(&self) -> usize {
        let s = &self.settings;
        pipeline_depth(
            self.block_rate.rate(Instant::now()),
            s.request_queue_time,
            s.max_outstanding_requests,
            self.remote_reqq,
            self.snubbed,
        )
    }

    /// Asks the torrent for more blocks when we may request and at least a
    /// quarter of the target depth (at least one request) is free.
    pub(super) async fn want_blocks(&mut self) -> Flow {
        let may_request = !self.choked || (self.fast && !self.allowed_fast.is_empty());
        let max = self.target_depth();
        let n = self.outstanding.len();
        if may_request && n < max && max - n >= (max / 4).max(1) {
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
                if self.outstanding.is_empty() && self.idle_since_block {
                    // The snub timer starts with the first request in flight.
                    self.snub_since = now;
                }
                // Never past the current depth (a snubbed peer's is 1):
                // the surplus goes back to the torrent.
                let depth = self.target_depth();
                for b in blocks {
                    if self.outstanding.contains_key(&b) {
                        continue;
                    }
                    let fast_ok = self.fast && self.allowed_fast.contains(&b.piece);
                    if (self.choked && !fast_ok) || self.outstanding.len() >= depth {
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
                    // The cancelled request (an end-game duplicate another
                    // peer answered) frees pipeline room; with a shallow
                    // pipeline nothing else may ask for it again.
                    self.want_blocks().await?;
                }
            }
            PeerCommand::Choke => {
                if !self.choking {
                    self.choking = true;
                    self.feed(Message::Choke).await?;
                    // Queued requests die with the choke, answered or not;
                    // with fast they are rejected explicitly.
                    self.uploads.clear();
                    self.queued.clear();
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
                // away), and each block once: one re-requested after a
                // cancel is still queued and goes out from there. The block
                // stays in `incoming` until it is written.
                if !self.incoming.contains(&block) || self.queued.contains(&block) {
                    return Ok(());
                }
                let max = self.settings.max_incoming_requests;
                if self.uploads.len() >= max {
                    // Drop entries cancelled since.
                    let (incoming, queued) = (&self.incoming, &mut self.queued);
                    self.uploads.retain(|(b, _)| {
                        let live = incoming.contains(b);
                        if !live {
                            queued.remove(b);
                        }
                        live
                    });
                }
                if self.uploads.len() >= max {
                    // Unreachable while `incoming` holds at most `max`;
                    // refused rather than queued past the bound.
                    if self.incoming.remove(&block) && self.fast {
                        self.send(Message::RejectRequest(block)).await?;
                    }
                    return Ok(());
                }
                self.queued.insert(block);
                self.uploads.push_back((block, data));
            }
            PeerCommand::Reject(b) => {
                if self.incoming.remove(&b) && self.fast {
                    self.send(Message::RejectRequest(b)).await?;
                }
            }
            PeerCommand::MetadataReady { layout, ext } => self.metadata_ready(layout, ext).await?,
            PeerCommand::SendHashes { req, hashes } => {
                self.send(Message::Hashes { req, hashes }).await?;
            }
            PeerCommand::RejectHashes(req) => self.send(Message::HashReject(req)).await?,
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
            PeerCommand::SendHolepunch(msg) => {
                if let Some(m) = self.remote_ids.holepunch(&msg) {
                    self.send(m).await?;
                }
            }
            PeerCommand::UploadOnly(on) => {
                if let Some(m) = self.remote_ids.upload_only(on) {
                    self.send(m).await?;
                }
            }
            PeerCommand::DontHave(piece) => {
                if let Some(m) = self.remote_ids.donthave(piece) {
                    self.send(m).await?;
                }
            }
            PeerCommand::Close => return Err(DisconnectReason::Shutdown),
        }
        Ok(())
    }

    /// Writes queued blocks while the upload throttle allows, then reports
    /// each one sent once flushed. Each block first reserves its bytes
    /// ([`Throttle::reserve`](crate::bandwidth::Throttle::reserve)) and is
    /// written when that reservation is due, so connections sharing a
    /// bucket take turns instead of racing for it. A reservation whose
    /// block went away (cancelled, choked away) pays for the next block.
    pub(super) async fn drain_uploads(&mut self) -> Flow {
        let mut sent = Vec::new();
        while let Some((block, data)) = self.uploads.front() {
            let len = wire_len(&Message::Piece {
                piece: block.piece,
                offset: block.offset,
                data: data.clone(),
            });
            let turn = match self.upload_turn.take() {
                Some(t) => t,
                None => (self.up.reserve(len), len),
            };
            if turn.0.due_at(Instant::now()).is_some() {
                self.upload_turn = Some(turn);
                break;
            }
            // The first block not cancelled, rejected or choked away while
            // queued.
            let mut next = None;
            while let Some((block, data)) = self.uploads.pop_front() {
                self.queued.remove(&block);
                if self.incoming.remove(&block) {
                    next = Some((block, data));
                    break;
                }
            }
            let Some((block, data)) = next else {
                self.upload_turn = Some(turn);
                break;
            };
            let len = data.len() as u64;
            let m = Message::Piece {
                piece: block.piece,
                offset: block.offset,
                data,
            };
            // A block bigger than the one reserved for pays the rest.
            self.up.charge(wire_len(&m).saturating_sub(turn.1));
            let r = timeout(self.settings.peer_idle_timeout, self.framed.feed(m)).await;
            self.write_done(r)?;
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
