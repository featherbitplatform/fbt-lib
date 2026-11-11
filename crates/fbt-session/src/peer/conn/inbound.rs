//! Handling of what the remote sends.

use bytes::Bytes;
use fbt_core::{BlockRef, PieceLayout};
use fbt_wire::{Message, MessageCodec};
use tokio::io::{AsyncRead, AsyncWrite};

use super::state::{Conn, RawPieces};
use super::{Flow, MAX_ALLOWED_FAST, MAX_PENDING_HAVES, max_frame, protocol};
use crate::peer::ext::{self, ExtConfig, Incoming};
use crate::peer::messages::{DisconnectReason, PeerEvent};

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    pub(super) async fn on_message(&mut self, m: Message) -> Flow {
        let key = self.key;
        match m {
            Message::Choke => {
                if !self.choked {
                    self.choked = true;
                    self.emit(PeerEvent::Choked { key }).await?;
                    // The torrent drops our demand on `Choked`; allowed-fast
                    // pieces may still be requested.
                    if self.fast && !self.allowed_fast.is_empty() {
                        self.want_blocks().await?;
                    }
                }
                // Without fast, a choke silently drops every pending request.
                if !self.fast {
                    if !self.outstanding.is_empty() {
                        self.idle_since_block = false;
                    }
                    for block in std::mem::take(&mut self.outstanding).into_keys() {
                        self.emit(PeerEvent::Rejected { key, block }).await?;
                    }
                }
            }
            Message::Unchoke => {
                if self.choked {
                    self.choked = false;
                    // Time choked never counts towards a snub.
                    self.snub_since = tokio::time::Instant::now();
                    self.emit(PeerEvent::Unchoked { key }).await?;
                    self.want_blocks().await?;
                }
            }
            Message::Interested => {
                if !self.remote_interested {
                    self.remote_interested = true;
                    self.emit(PeerEvent::Interested { key }).await?;
                }
            }
            Message::NotInterested => {
                if self.remote_interested {
                    self.remote_interested = false;
                    self.emit(PeerEvent::NotInterested { key }).await?;
                }
            }
            Message::Have(piece) => {
                if let Some(p) = self.pending.as_mut() {
                    self.pieces_seen = true;
                    if p.haves.len() < MAX_PENDING_HAVES {
                        p.haves.push(piece);
                    }
                    return Ok(());
                }
                if piece >= self.num_pieces {
                    return Err(protocol("have index out of range"));
                }
                self.pieces_seen = true;
                self.emit(PeerEvent::Have { key, piece }).await?;
            }
            Message::Bitfield(bytes) => self.initial_pieces(RawPieces::Bits(bytes)).await?,
            Message::HaveAll => {
                self.require_fast()?;
                self.initial_pieces(RawPieces::All).await?;
            }
            Message::HaveNone => {
                self.require_fast()?;
                self.initial_pieces(RawPieces::None).await?;
            }
            Message::Request(block) => self.on_remote_request(block).await?,
            Message::Piece {
                piece,
                offset,
                data,
            } => {
                let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
                let block = BlockRef { piece, offset, len };
                if self.outstanding.remove(&block).is_some() {
                    self.block_rate.record(data.len() as u64);
                    self.snub_since = tokio::time::Instant::now();
                    self.idle_since_block = true;
                    if self.snubbed {
                        self.snubbed = false;
                        self.emit(PeerEvent::Snubbed {
                            key,
                            snubbed: false,
                        })
                        .await?;
                    }
                    self.emit(PeerEvent::Block { key, block, data }).await?;
                    self.want_blocks().await?;
                } else {
                    let bytes = data.len() as u64;
                    self.emit(PeerEvent::Wasted { key, bytes }).await?;
                }
            }
            Message::Cancel(block) => {
                if self.incoming.remove(&block) {
                    self.emit(PeerEvent::Cancel { key, block }).await?;
                }
            }
            Message::RejectRequest(block) => {
                self.require_fast()?;
                if self.outstanding.remove(&block).is_some() {
                    self.idle_since_block = false;
                    self.emit(PeerEvent::Rejected { key, block }).await?;
                    self.want_blocks().await?;
                }
            }
            Message::AllowedFast(piece) => {
                self.require_fast()?;
                // A remote may send any number of these: only the first
                // [`MAX_ALLOWED_FAST`] distinct pieces count, and only the
                // first one opens the pipeline (the torrent keeps the demand
                // and tops it up as more pieces become requestable).
                if piece < self.num_pieces
                    && self.allowed_fast.len() < MAX_ALLOWED_FAST
                    && self.allowed_fast.insert(piece)
                {
                    self.emit(PeerEvent::AllowedFast { key, piece }).await?;
                    if self.allowed_fast.len() == 1 {
                        self.want_blocks().await?;
                    }
                }
            }
            Message::SuggestPiece(_) => self.require_fast()?,
            Message::Port(port) => {
                if port != 0 && !self.port_seen {
                    self.port_seen = true;
                    self.emit(PeerEvent::DhtPort { key, port }).await?;
                }
            }
            Message::Extended { id, payload } => self.on_extended(id, &payload).await?,
            Message::KeepAlive | Message::Unknown { .. } => {}
        }
        Ok(())
    }

    /// Routes an extended message by the id we assigned.
    pub(super) async fn on_extended(&mut self, id: u8, payload: &Bytes) -> Flow {
        let key = self.key;
        let msg = ext::decode(id, payload, self.ext.pex)
            .map_err(|e| DisconnectReason::Protocol(format!("extended message {id}: {e}")))?;
        match msg {
            Incoming::Handshake(hs) => {
                self.remote_ids.update(&hs);
                // Clamped by the decoder; a repeat without `reqq` keeps the
                // last one.
                if hs.reqq.is_some() {
                    self.remote_reqq = hs.reqq;
                }
                let upload_only = hs.upload_only == Some(true);
                self.emit(PeerEvent::ExtHandshake { key, hs }).await?;
                // Every handshake states it (a repeat without the key
                // turns it off); only a change is reported.
                self.report_upload_only(upload_only).await
            }
            Incoming::UploadOnly(on) => self.report_upload_only(on).await,
            Incoming::DontHave(piece) => {
                // Without metadata there is nothing to retract yet.
                if self.pending.is_some() {
                    return Ok(());
                }
                if piece >= self.num_pieces {
                    return Err(protocol("lt_donthave index out of range"));
                }
                self.emit(PeerEvent::DontHave { key, piece }).await
            }
            Incoming::Metadata(msg) => self.emit(PeerEvent::Metadata { key, msg }).await,
            Incoming::Pex(msg) => self.emit(PeerEvent::Pex { key, msg }).await,
            Incoming::Unknown => Ok(()),
        }
    }

    /// Reports the remote's `upload_only` state when it changed.
    async fn report_upload_only(&mut self, on: bool) -> Flow {
        if on == self.remote_upload_only {
            return Ok(());
        }
        self.remote_upload_only = on;
        self.emit(PeerEvent::UploadOnly { key: self.key, on }).await
    }

    pub(super) fn require_fast(&self) -> Flow {
        if self.fast {
            Ok(())
        } else {
            Err(protocol("fast extension message without fast support"))
        }
    }

    /// A bitfield, `HaveAll` or `HaveNone`: only before any other piece
    /// announcement. Without metadata it is buffered unvalidated.
    pub(super) async fn initial_pieces(&mut self, raw: RawPieces) -> Flow {
        if self.pieces_seen {
            return Err(protocol("piece set after earlier piece announcements"));
        }
        self.pieces_seen = true;
        if let Some(p) = self.pending.as_mut() {
            p.pieces = Some(raw);
            return Ok(());
        }
        let pieces = raw.validate(self.num_pieces)?;
        self.emit(PeerEvent::Pieces {
            key: self.key,
            pieces,
        })
        .await
    }

    /// Leaves no-metadata mode: repeats our extension handshake with `ext`
    /// (now carrying `metadata_size`), validates the buffered piece set
    /// against `layout` and replays it and the in-range `Have`s as if they
    /// had just arrived.
    pub(super) async fn metadata_ready(&mut self, layout: PieceLayout, ext: ExtConfig) -> Flow {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        self.ext = ext;
        if self.remote_reserved.supports_extended() {
            self.send(ext::repeat_handshake_message(&self.ext, self.remote_ip))
                .await?;
        }
        let n = layout.num_pieces();
        self.num_pieces = n;
        *self.framed.codec_mut() = MessageCodec::new(max_frame(Some(n)));
        let key = self.key;
        if let Some(raw) = pending.pieces {
            let pieces = raw.validate(n)?;
            self.emit(PeerEvent::Pieces { key, pieces }).await?;
        }
        for piece in pending.haves.into_iter().filter(|&p| p < n) {
            self.emit(PeerEvent::Have { key, piece }).await?;
        }
        Ok(())
    }

    pub(super) async fn on_remote_request(&mut self, block: BlockRef) -> Flow {
        // Nothing can be served before the metadata.
        if self.pending.is_some()
            || self.choking
            || self.incoming.len() >= self.settings.max_incoming_requests
        {
            if self.fast {
                self.send(Message::RejectRequest(block)).await?;
            }
            return Ok(());
        }
        if self.incoming.insert(block) {
            self.emit(PeerEvent::Request {
                key: self.key,
                block,
            })
            .await?;
        }
        Ok(())
    }
}
