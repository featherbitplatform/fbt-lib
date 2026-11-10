//! BEP 9 metadata exchange for magnet torrents: fetching the info dictionary
//! from peers ([`MetadataFetch`]) and serving ours ([`serve_metadata`]).
//! Pure: no I/O, `now` is passed in.
//!
//! Everything a peer sends here is hostile. A peer becomes a source only if
//! it advertises `ut_metadata` and a `metadata_size` in
//! `1..=max_metadata_size`; its buffer is allocated piece by piece as exact,
//! requested pieces arrive, so nothing beyond that size is ever held. Data
//! that contradicts the advertised size, has the wrong length or was never
//! requested, and a completed buffer that fails the info hash, make the
//! peer [`FetchStep::Bad`].

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use fbt_core::{InfoHashV1, InfoHashV2, InfoHashes};
use fbt_wire::{METADATA_PIECE_LEN, MetadataMsg};
use tokio::time::Instant;

use crate::picker::PeerKey;

/// Peers fetched from at once.
pub(super) const MAX_METADATA_SOURCES: usize = 2;
/// A source that leaves any request unanswered for this long, counted from
/// that request, is dropped (no ban) and becomes a candidate again after as
/// long.
const PIECE_TIMEOUT: Duration = Duration::from_secs(30);
/// A source's whole fetch must finish within this base time plus
/// [`SOURCE_TIME_PER_PIECE`] per metadata piece, capped at
/// [`MAX_SOURCE_DEADLINE`]; past it the source is retired like a timeout, so
/// a peer trickling pieces just inside [`PIECE_TIMEOUT`] cannot hold its slot.
const SOURCE_BASE_DEADLINE: Duration = Duration::from_secs(30);
/// See [`SOURCE_BASE_DEADLINE`].
const SOURCE_TIME_PER_PIECE: Duration = Duration::from_secs(5);
/// See [`SOURCE_BASE_DEADLINE`].
const MAX_SOURCE_DEADLINE: Duration = Duration::from_secs(10 * 60);
/// `ut_metadata` requests served per connection; later ones get `Reject`.
pub(super) const MAX_METADATA_REQUESTS_PER_PEER: u32 = 256;
/// Requests in flight to one source.
const MAX_IN_FLIGHT: usize = 2;
/// Peers waiting to become a source; further ones are ignored.
const MAX_CANDIDATES: usize = 1024;

/// What the actor does after a `ut_metadata` message.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FetchStep {
    /// Nothing to send.
    Nothing,
    /// Send these requests.
    Send(Vec<(PeerKey, MetadataMsg)>),
    /// The peer sent bad metadata: close it with `BadMetadata` and never
    /// dial it again. The next source starts when it is gone.
    Bad(PeerKey),
    /// The verified info dictionary.
    Done(Vec<u8>),
    /// With [`MetadataFetch::deferred`]: a complete buffer from this peer,
    /// not hashed yet. The actor verifies it off the actor task and reports
    /// a mismatch with [`MetadataFetch::on_verify_failed`].
    Verify(PeerKey, Vec<u8>),
}

/// One peer we fetch the whole metadata from.
#[derive(Debug)]
struct Source {
    /// The `metadata_size` it advertised (in range).
    size: u32,
    num_pieces: u32,
    /// Received pieces, by index.
    pieces: Vec<Option<Bytes>>,
    received: u32,
    /// Next piece to request.
    next: u32,
    /// Requested pieces and when each was asked for; `None` until the next
    /// tick sets it.
    in_flight: BTreeMap<u32, Option<Instant>>,
    /// When it became a source; `None` until the next tick sets it.
    started: Option<Instant>,
}

/// Fetches the info dictionary from up to [`MAX_METADATA_SOURCES`] peers.
#[derive(Debug)]
pub(super) struct MetadataFetch {
    expected: InfoHashes,
    max_size: usize,
    /// Completed buffers are returned as [`FetchStep::Verify`].
    deferred: bool,
    /// Eligible peers waiting for a source slot, with their size.
    candidates: VecDeque<(PeerKey, u32)>,
    sources: HashMap<PeerKey, Source>,
    /// Dropped sources (reject, timeout): their late answers are ignored,
    /// and they become candidates again at the instant given.
    retired: HashMap<PeerKey, (u32, Instant)>,
    /// Complete buffers being verified (deferred mode).
    verifying: HashSet<PeerKey>,
    /// Peers that sent bad metadata: never used again.
    banned: HashSet<PeerKey>,
    done: bool,
    /// The latest `now` seen, for calls that get none.
    now: Option<Instant>,
}

impl MetadataFetch {
    pub(super) fn new(expected: InfoHashes, max_size: usize) -> Self {
        MetadataFetch {
            expected,
            max_size,
            deferred: false,
            candidates: VecDeque::new(),
            sources: HashMap::new(),
            retired: HashMap::new(),
            verifying: HashSet::new(),
            banned: HashSet::new(),
            done: false,
            now: None,
        }
    }

    /// Completed buffers come back unverified as [`FetchStep::Verify`], so
    /// the actor can hash them in `spawn_blocking`.
    pub(super) fn deferred(mut self) -> Self {
        self.deferred = true;
        self
    }

    /// A `metadata_size` we would fetch: `1..=max_size`.
    pub(super) fn acceptable_size(&self, size: u32) -> bool {
        size >= 1 && usize::try_from(size).is_ok_and(|s| s <= self.max_size)
    }

    /// A peer's (possibly repeated) extension handshake. It becomes a
    /// candidate if it speaks `ut_metadata` with an acceptable size; a
    /// source keeps the size it first advertised, and one that turns
    /// `ut_metadata` off is dropped.
    pub(super) fn on_ext_handshake(
        &mut self,
        key: PeerKey,
        metadata_size: Option<u32>,
        supports: bool,
        now: Instant,
    ) -> Vec<(PeerKey, MetadataMsg)> {
        self.now = Some(now);
        if self.done || self.banned.contains(&key) || self.verifying.contains(&key) {
            return Vec::new();
        }
        if self.sources.contains_key(&key) || self.retired.contains_key(&key) {
            if !supports {
                self.forget(key);
            }
            return self.start_sources();
        }
        let eligible = metadata_size.filter(|&s| supports && self.acceptable_size(s));
        let at = self.candidates.iter().position(|(k, _)| *k == key);
        match (at, eligible) {
            (Some(i), Some(size)) => {
                if let Some(c) = self.candidates.get_mut(i) {
                    c.1 = size;
                }
            }
            (Some(i), None) => {
                self.candidates.remove(i);
            }
            (None, Some(size)) if self.candidates.len() < MAX_CANDIDATES => {
                self.candidates.push_back((key, size));
            }
            (None, _) => {}
        }
        self.start_sources()
    }

    /// A `ut_metadata` message other than a request (requests are ours to
    /// serve, not part of the fetch: [`FetchStep::Nothing`]).
    pub(super) fn on_message(&mut self, key: PeerKey, msg: MetadataMsg, now: Instant) -> FetchStep {
        self.now = Some(now);
        if self.done
            || self.banned.contains(&key)
            || self.verifying.contains(&key)
            || self.retired.contains_key(&key)
        {
            // Late answers to requests we gave up on are not held against
            // the peer.
            return FetchStep::Nothing;
        }
        match msg {
            MetadataMsg::Request { .. } => FetchStep::Nothing,
            MetadataMsg::Reject { piece } => {
                if !self
                    .sources
                    .get(&key)
                    .is_some_and(|s| s.in_flight.contains_key(&piece))
                {
                    return FetchStep::Nothing;
                }
                self.retire(key, now);
                send_or_nothing(self.start_sources())
            }
            MetadataMsg::Data {
                piece,
                total_size,
                data,
            } => self.on_data(key, piece, total_size, data, now),
        }
    }

    fn on_data(
        &mut self,
        key: PeerKey,
        piece: u32,
        total_size: u32,
        data: Bytes,
        now: Instant,
    ) -> FetchStep {
        let Some(src) = self.sources.get_mut(&key) else {
            // Never asked: a peer that is no source sends no data.
            return self.bad(key);
        };
        if total_size != src.size
            || src.in_flight.remove(&piece).is_none()
            || piece_len(src.size, piece) != Some(data.len())
        {
            return self.bad(key);
        }
        let Some(slot) = src.pieces.get_mut(piece as usize) else {
            return self.bad(key);
        };
        *slot = Some(data);
        src.received = src.received.saturating_add(1);
        if src.received < src.num_pieces {
            return send_or_nothing(src.request_more(key, Some(now)));
        }
        let Some(src) = self.sources.remove(&key) else {
            return FetchStep::Nothing;
        };
        let mut bytes = Vec::with_capacity(src.size as usize);
        for p in src.pieces.into_iter().flatten() {
            bytes.extend_from_slice(&p);
        }
        if self.deferred {
            self.verifying.insert(key);
            return FetchStep::Verify(key, bytes);
        }
        if hashes_match(&self.expected, &bytes) {
            self.finish();
            FetchStep::Done(bytes)
        } else {
            self.bad(key)
        }
    }

    /// The peer disconnected: it is forgotten and the next candidate
    /// starts.
    pub(super) fn on_peer_gone(&mut self, key: PeerKey) -> Vec<(PeerKey, MetadataMsg)> {
        self.forget(key);
        self.banned.remove(&key);
        self.start_sources()
    }

    /// A deferred buffer from `key` failed the info hash: `key` is banned.
    /// A peer that disconnected meanwhile is not remembered (its key is
    /// never reused).
    pub(super) fn on_verify_failed(&mut self, key: PeerKey) -> Vec<(PeerKey, MetadataMsg)> {
        if self.verifying.remove(&key) {
            self.banned.insert(key);
        }
        self.forget(key);
        self.start_sources()
    }

    /// Drops sources (no ban) with a request unanswered for
    /// [`PIECE_TIMEOUT`] or past their whole-fetch deadline, and re-admits
    /// retired peers whose cool-down is over.
    pub(super) fn on_tick(&mut self, now: Instant) -> Vec<(PeerKey, MetadataMsg)> {
        self.now = Some(now);
        if self.done {
            return Vec::new();
        }
        let mut expired = Vec::new();
        for (key, src) in &mut self.sources {
            if src.expired(now) {
                expired.push(*key);
            }
        }
        for key in expired {
            self.retire(key, now);
        }
        let ready: Vec<(PeerKey, u32)> = self
            .retired
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(k, (size, _))| (*k, *size))
            .collect();
        for (key, size) in ready {
            self.retired.remove(&key);
            if self.candidates.len() < MAX_CANDIDATES {
                self.candidates.push_back((key, size));
            }
        }
        self.start_sources()
    }

    /// Starts candidates while source slots are free. A buffer being
    /// verified keeps its slot, so at most [`MAX_METADATA_SOURCES`] buffers
    /// exist at once.
    fn start_sources(&mut self) -> Vec<(PeerKey, MetadataMsg)> {
        let mut out = Vec::new();
        if self.done {
            return out;
        }
        while self.sources.len() + self.verifying.len() < MAX_METADATA_SOURCES {
            let Some((key, size)) = self.candidates.pop_front() else {
                break;
            };
            let mut src = Source::new(size, self.now);
            out.extend(src.request_more(key, self.now));
            self.sources.insert(key, src);
        }
        out
    }

    /// Drops source `key` without a ban; it may be used again after the
    /// cool-down.
    fn retire(&mut self, key: PeerKey, now: Instant) {
        if let Some(src) = self.sources.remove(&key) {
            self.retired.insert(key, (src.size, now + PIECE_TIMEOUT));
        }
    }

    /// `key` sent bad metadata: dropped and never used again.
    fn bad(&mut self, key: PeerKey) -> FetchStep {
        self.forget(key);
        self.banned.insert(key);
        FetchStep::Bad(key)
    }

    fn forget(&mut self, key: PeerKey) {
        self.sources.remove(&key);
        self.retired.remove(&key);
        self.verifying.remove(&key);
        self.candidates.retain(|(k, _)| *k != key);
    }

    fn finish(&mut self) {
        self.done = true;
        self.candidates.clear();
        self.sources.clear();
        self.retired.clear();
        self.verifying.clear();
    }
}

impl Source {
    /// A source for `size` bytes (already checked against the cap, so the
    /// piece table is at most `MAX_METADATA_SIZE / 16 KiB` entries).
    fn new(size: u32, now: Option<Instant>) -> Self {
        let num_pieces = size.div_ceil(METADATA_PIECE_LEN as u32);
        Source {
            size,
            num_pieces,
            pieces: vec![None; num_pieces as usize],
            received: 0,
            next: 0,
            in_flight: BTreeMap::new(),
            started: now,
        }
    }

    /// How long the whole fetch from this source may take.
    fn deadline(&self) -> Duration {
        SOURCE_TIME_PER_PIECE
            .saturating_mul(self.num_pieces)
            .saturating_add(SOURCE_BASE_DEADLINE)
            .min(MAX_SOURCE_DEADLINE)
    }

    /// A request has waited [`PIECE_TIMEOUT`] or the whole fetch is past
    /// its [`deadline`](Self::deadline). Times still unset are set to `now`.
    fn expired(&mut self, now: Instant) -> bool {
        let started = *self.started.get_or_insert(now);
        let mut late = now >= started + self.deadline();
        for at in self.in_flight.values_mut() {
            let at = *at.get_or_insert(now);
            late |= now >= at + PIECE_TIMEOUT;
        }
        late
    }

    /// Requests the next pieces, up to [`MAX_IN_FLIGHT`] outstanding, each
    /// timed from `now`.
    fn request_more(&mut self, key: PeerKey, now: Option<Instant>) -> Vec<(PeerKey, MetadataMsg)> {
        let mut out = Vec::new();
        while self.in_flight.len() < MAX_IN_FLIGHT && self.next < self.num_pieces {
            self.in_flight.insert(self.next, now);
            out.push((key, MetadataMsg::Request { piece: self.next }));
            self.next += 1;
        }
        out
    }
}

/// The exact length of metadata piece `piece` of a `size`-byte dictionary;
/// `None` for a piece out of range.
fn piece_len(size: u32, piece: u32) -> Option<usize> {
    let size = usize::try_from(size).ok()?;
    let start = usize::try_from(piece)
        .ok()?
        .checked_mul(METADATA_PIECE_LEN)?;
    (start < size).then(|| (size - start).min(METADATA_PIECE_LEN))
}

/// `bytes` hash to every expected info hash (SHA-1 for v1, SHA-256 for v2).
fn hashes_match(expected: &InfoHashes, bytes: &[u8]) -> bool {
    (expected.v1.is_some() || expected.v2.is_some())
        && expected.v1.is_none_or(|h| h == InfoHashV1::of(bytes))
        && expected.v2.is_none_or(|h| h == InfoHashV2::of(bytes))
}

fn send_or_nothing(sends: Vec<(PeerKey, MetadataMsg)>) -> FetchStep {
    if sends.is_empty() {
        FetchStep::Nothing
    } else {
        FetchStep::Send(sends)
    }
}

/// Our answer to a request for metadata piece `piece`: `Reject` without
/// metadata or for a piece out of range.
pub(super) fn serve_metadata(raw_info: Option<&[u8]>, piece: u32) -> MetadataMsg {
    let reject = MetadataMsg::Reject { piece };
    let Some(raw) = raw_info else {
        return reject;
    };
    let Ok(total_size) = u32::try_from(raw.len()) else {
        return reject;
    };
    let Some(len) = piece_len(total_size, piece) else {
        return reject;
    };
    // In range, so this cannot overflow.
    let start = piece as usize * METADATA_PIECE_LEN;
    match raw.get(start..start + len) {
        Some(data) => MetadataMsg::Data {
            piece,
            total_size,
            data: Bytes::copy_from_slice(data),
        },
        None => reject,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 16 * 1024 * 1024;

    fn info(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7 + i / 251) as u8).collect()
    }

    fn fetch_for(info: &[u8]) -> MetadataFetch {
        MetadataFetch::new(InfoHashes::from_v1(InfoHashV1::of(info)), MAX)
    }

    fn data(info: &[u8], piece: u32) -> MetadataMsg {
        let start = piece as usize * METADATA_PIECE_LEN;
        let end = (start + METADATA_PIECE_LEN).min(info.len());
        MetadataMsg::Data {
            piece,
            total_size: info.len() as u32,
            data: Bytes::copy_from_slice(&info[start..end]),
        }
    }

    fn req(key: u64, piece: u32) -> (PeerKey, MetadataMsg) {
        (PeerKey(key), MetadataMsg::Request { piece })
    }

    fn sent(step: FetchStep) -> Vec<(PeerKey, MetadataMsg)> {
        match step {
            FetchStep::Send(v) => v,
            other => panic!("expected Send, got {other:?}"),
        }
    }

    #[test]
    fn fetches_all_pieces_and_verifies() {
        let info = info(40_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let a = PeerKey(1);
        assert_eq!(
            f.on_ext_handshake(a, Some(40_000), true, now),
            vec![req(1, 0), req(1, 1)]
        );
        assert_eq!(sent(f.on_message(a, data(&info, 1), now)), vec![req(1, 2)]);
        assert_eq!(f.on_message(a, data(&info, 0), now), FetchStep::Nothing);
        assert_eq!(f.on_message(a, data(&info, 2), now), FetchStep::Done(info));
    }

    #[test]
    fn deferred_mode_returns_the_buffer_unverified() {
        let info = info(20_000);
        let mut f =
            MetadataFetch::new(InfoHashes::from_v1(InfoHashV1::of(b"other")), MAX).deferred();
        let now = Instant::now();
        let (a, b) = (PeerKey(1), PeerKey(2));
        f.on_ext_handshake(a, Some(20_000), true, now);
        f.on_ext_handshake(b, Some(20_000), true, now);
        f.on_message(a, data(&info, 0), now);
        assert_eq!(
            f.on_message(a, data(&info, 1), now),
            FetchStep::Verify(a, info.clone())
        );
        // A mismatch bans it; its late handshake is ignored.
        assert!(f.on_verify_failed(a).is_empty());
        assert!(f.on_ext_handshake(a, Some(20_000), true, now).is_empty());
        // B goes on.
        f.on_message(b, data(&info, 0), now);
        assert!(matches!(f.on_message(b, data(&info, 1), now), FetchStep::Verify(k, _) if k == b));
    }

    #[test]
    fn buffers_being_verified_keep_their_slots() {
        let info = info(1000);
        let mut f = MetadataFetch::new(InfoHashes::from_v1(InfoHashV1::of(&info)), MAX).deferred();
        let now = Instant::now();
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        for k in [a, b, c] {
            f.on_ext_handshake(k, Some(1000), true, now);
        }
        assert!(matches!(
            f.on_message(a, data(&info, 0), now),
            FetchStep::Verify(..)
        ));
        assert!(matches!(
            f.on_message(b, data(&info, 0), now),
            FetchStep::Verify(..)
        ));
        // Two buffers are out for verification: C waits.
        assert!(f.on_tick(now).is_empty());
        assert!(f.on_ext_handshake(c, Some(1000), true, now).is_empty());
        assert_eq!(f.on_verify_failed(a), vec![req(3, 0)]);
        // A peer gone while verifying frees its slot too, and is not
        // remembered as banned.
        f.on_peer_gone(b);
        assert!(!f.banned.contains(&b));
        assert!(f.on_verify_failed(b).is_empty());
        assert!(!f.banned.contains(&b));
    }

    #[test]
    fn v2_hash_is_checked_too() {
        let info = info(1000);
        let ih = InfoHashes::hybrid(InfoHashV1::of(&info), InfoHashV2::of(b"not it"));
        let mut f = MetadataFetch::new(ih, MAX);
        let now = Instant::now();
        f.on_ext_handshake(PeerKey(1), Some(1000), true, now);
        assert_eq!(
            f.on_message(PeerKey(1), data(&info, 0), now),
            FetchStep::Bad(PeerKey(1))
        );
        let ih = InfoHashes::hybrid(InfoHashV1::of(&info), InfoHashV2::of(&info));
        let mut f = MetadataFetch::new(ih, MAX);
        f.on_ext_handshake(PeerKey(1), Some(1000), true, now);
        assert_eq!(
            f.on_message(PeerKey(1), data(&info, 0), now),
            FetchStep::Done(info)
        );
    }

    #[test]
    fn wrong_hash_bans_source() {
        let info = info(20_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let (a, b) = (PeerKey(1), PeerKey(2));
        f.on_ext_handshake(a, Some(20_000), true, now);
        f.on_ext_handshake(b, Some(20_000), true, now);
        let mut flipped = info.clone();
        flipped[100] ^= 0xff;
        f.on_message(a, data(&flipped, 0), now);
        assert_eq!(f.on_message(a, data(&flipped, 1), now), FetchStep::Bad(a));
        // Banned: a repeated handshake does not bring it back.
        assert!(f.on_ext_handshake(a, Some(20_000), true, now).is_empty());
        f.on_message(b, data(&info, 0), now);
        assert_eq!(f.on_message(b, data(&info, 1), now), FetchStep::Done(info));
    }

    #[test]
    fn inconsistent_total_size_is_bad() {
        let info = info(20_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let a = PeerKey(1);
        f.on_ext_handshake(a, Some(20_000), true, now);
        let msg = MetadataMsg::Data {
            piece: 0,
            total_size: 20_001,
            data: Bytes::copy_from_slice(&info[..METADATA_PIECE_LEN]),
        };
        assert_eq!(f.on_message(a, msg, now), FetchStep::Bad(a));
    }

    #[test]
    fn wrong_piece_length_is_bad() {
        let info = info(20_000);
        let now = Instant::now();
        let a = PeerKey(1);
        // A short first piece, and a last piece one byte too long.
        for (piece, len) in [(0u32, 16_000usize), (1, 20_000 - METADATA_PIECE_LEN + 1)] {
            let mut f = fetch_for(&info);
            f.on_ext_handshake(a, Some(20_000), true, now);
            let msg = MetadataMsg::Data {
                piece,
                total_size: 20_000,
                data: Bytes::from(vec![0u8; len]),
            };
            assert_eq!(
                f.on_message(a, msg, now),
                FetchStep::Bad(a),
                "piece {piece}"
            );
        }
    }

    #[test]
    fn unrequested_data_is_bad() {
        let info = info(60_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let (a, c) = (PeerKey(1), PeerKey(3));
        f.on_ext_handshake(a, Some(60_000), true, now);
        // Piece 3 was not requested yet (only 0 and 1 are in flight).
        assert_eq!(f.on_message(a, data(&info, 3), now), FetchStep::Bad(a));
        // Data from a peer that is no source at all.
        assert_eq!(f.on_message(c, data(&info, 0), now), FetchStep::Bad(c));
        // A piece index whose offset overflows.
        let mut f = fetch_for(&info);
        f.on_ext_handshake(a, Some(60_000), true, now);
        let msg = MetadataMsg::Data {
            piece: u32::MAX,
            total_size: 60_000,
            data: Bytes::new(),
        };
        assert_eq!(f.on_message(a, msg, now), FetchStep::Bad(a));
    }

    #[test]
    fn size_over_cap_is_not_requested() {
        let info = info(20_000);
        let mut f = MetadataFetch::new(InfoHashes::from_v1(InfoHashV1::of(&info)), 16_384);
        let now = Instant::now();
        assert!(!f.acceptable_size(1 << 30));
        assert!(!f.acceptable_size(0));
        assert!(f.acceptable_size(16_384));
        assert!(
            f.on_ext_handshake(PeerKey(1), Some(1 << 30), true, now)
                .is_empty()
        );
        assert!(
            f.on_ext_handshake(PeerKey(2), Some(0), true, now)
                .is_empty()
        );
        assert!(f.on_ext_handshake(PeerKey(3), None, true, now).is_empty());
        // Without ut_metadata a peer is no source whatever its size.
        assert!(
            f.on_ext_handshake(PeerKey(4), Some(100), false, now)
                .is_empty()
        );
        assert_eq!(
            f.on_ext_handshake(PeerKey(5), Some(16_384), true, now),
            vec![req(5, 0)]
        );
    }

    #[test]
    fn reject_moves_to_next_source_without_ban() {
        let info = info(60_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        for k in [a, b, c] {
            f.on_ext_handshake(k, Some(60_000), true, now);
        }
        assert_eq!(
            sent(f.on_message(a, MetadataMsg::Reject { piece: 0 }, now)),
            vec![req(3, 0), req(3, 1)]
        );
        // A's late answers are ignored, not held against it.
        assert_eq!(f.on_message(a, data(&info, 1), now), FetchStep::Nothing);
        // B and C answer both requests, so they do not time out below.
        let later = now + Duration::from_secs(10);
        for k in [b, c] {
            f.on_message(k, data(&info, 0), later);
            f.on_message(k, data(&info, 1), later);
        }
        // After the cool-down A is a candidate again: when B goes, A starts.
        assert!(f.on_tick(now + PIECE_TIMEOUT).is_empty());
        assert_eq!(f.on_peer_gone(b), vec![req(1, 0), req(1, 1)]);
    }

    #[test]
    fn timeout_moves_to_next_source() {
        let info = info(20_000);
        let mut f = fetch_for(&info);
        let t0 = Instant::now();
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        for k in [a, b, c] {
            f.on_ext_handshake(k, Some(20_000), true, t0);
        }
        // B answers in time; A never does.
        let t1 = t0 + Duration::from_secs(20);
        f.on_message(b, data(&info, 0), t1);
        assert!(
            f.on_tick(t0 + PIECE_TIMEOUT - Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(f.on_tick(t0 + PIECE_TIMEOUT), vec![req(3, 0), req(3, 1)]);
        assert_eq!(f.on_message(a, data(&info, 0), t1), FetchStep::Nothing);
    }

    /// Each request is timed on its own: answering one piece does not buy
    /// time for another left unanswered.
    #[test]
    fn each_request_has_its_own_timeout() {
        let info = info(60_000);
        let mut f = fetch_for(&info);
        let t0 = Instant::now();
        let (a, c) = (PeerKey(1), PeerKey(3));
        f.on_ext_handshake(a, Some(60_000), true, t0);
        f.on_ext_handshake(PeerKey(2), Some(60_000), true, t0);
        f.on_ext_handshake(c, Some(60_000), true, t0);
        // Piece 0 arrives (piece 2 is asked for); piece 1 never does.
        let t1 = t0 + Duration::from_secs(20);
        assert_eq!(sent(f.on_message(a, data(&info, 0), t1)), vec![req(1, 2)]);
        f.on_message(PeerKey(2), data(&info, 0), t1);
        f.on_message(PeerKey(2), data(&info, 1), t1);
        let sends = f.on_tick(t0 + PIECE_TIMEOUT);
        assert_eq!(sends, vec![req(3, 0), req(3, 1)], "A retired at 30 s");
        assert_eq!(f.on_message(a, data(&info, 1), t1), FetchStep::Nothing);
    }

    /// A source answering every request just inside [`PIECE_TIMEOUT`]
    /// would hold its slot for ever; its whole-fetch deadline retires it
    /// (no ban) and an honest candidate completes.
    #[test]
    fn trickling_sources_are_retired_by_their_deadline() {
        let len = 20 * METADATA_PIECE_LEN;
        let info = info(len);
        let mut f = fetch_for(&info);
        let t0 = Instant::now();
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        for k in [a, b, c] {
            f.on_ext_handshake(k, Some(len as u32), true, t0);
        }
        // 30 s + 20 pieces x 5 s.
        let deadline = t0 + Duration::from_secs(130);
        let step = Duration::from_secs(29);
        let mut next = [0u32, 0u32];
        let mut t = t0;
        let mut started_c = Vec::new();
        while started_c.is_empty() {
            t += step;
            assert!(t < deadline + step, "never retired");
            // Both slow sources answer their two requests 29 s after them.
            for (i, k) in [a, b].into_iter().enumerate() {
                for _ in 0..MAX_IN_FLIGHT {
                    f.on_message(k, data(&info, next[i]), t);
                    next[i] += 1;
                }
            }
            started_c = f.on_tick(t);
        }
        assert!(t >= deadline, "not before the deadline");
        assert!(started_c.contains(&req(3, 0)), "{started_c:?}");
        assert!(!f.banned.contains(&a) && !f.banned.contains(&b));
        assert!(f.retired.contains_key(&a) && f.retired.contains_key(&b));
        // C answers promptly and completes.
        let mut piece = 0;
        loop {
            match f.on_message(c, data(&info, piece), t) {
                FetchStep::Done(bytes) => {
                    assert_eq!(bytes, info);
                    break;
                }
                FetchStep::Send(_) | FetchStep::Nothing => piece += 1,
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn source_deadline_is_capped() {
        let src = Source::new(MAX as u32, None);
        assert_eq!(src.deadline(), MAX_SOURCE_DEADLINE);
        assert_eq!(Source::new(1, None).deadline(), Duration::from_secs(35));
    }

    #[test]
    fn at_most_two_sources() {
        let info = info(20_000);
        let mut f = fetch_for(&info);
        let now = Instant::now();
        let mut asked = Vec::new();
        for k in 1..=4 {
            asked.extend(f.on_ext_handshake(PeerKey(k), Some(20_000), true, now));
        }
        let keys: HashSet<PeerKey> = asked.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, HashSet::from([PeerKey(1), PeerKey(2)]));
        assert_eq!(asked.len(), 2 * MAX_IN_FLIGHT);
        // A source leaving makes room for the next candidate.
        assert_eq!(f.on_peer_gone(PeerKey(1)), vec![req(3, 0), req(3, 1)]);
    }

    #[test]
    fn serve_metadata_pieces_and_reject() {
        let info = info(40_000);
        assert_eq!(serve_metadata(Some(&info), 0), data(&info, 0));
        match serve_metadata(Some(&info), 2) {
            MetadataMsg::Data {
                piece: 2,
                total_size: 40_000,
                data,
            } => assert_eq!(data.len(), 40_000 - 2 * METADATA_PIECE_LEN),
            other => panic!("{other:?}"),
        }
        for piece in [3, u32::MAX] {
            assert_eq!(
                serve_metadata(Some(&info), piece),
                MetadataMsg::Reject { piece }
            );
        }
        assert_eq!(serve_metadata(None, 0), MetadataMsg::Reject { piece: 0 });
    }
}
