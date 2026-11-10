# Milestone 2: Must-haves Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.2.0`: magnet links download through the DHT (BEP 5/32/42/43, persisted routing table) and BEP 9 metadata exchange. Connections speak MSE/PE under a forced, preferred or disabled policy, and peers are exchanged over BEP 11 PEX. Private torrents (BEP 27) never touch the DHT or PEX. Everything is proven against qBittorrent and Transmission on an isolated Docker network.

**Architecture:** Three protocol layers land first, each usable on its own:
- `fbt-wire` gains the BEP 10 extension messages and an `MseStream<S>` wrapper (DH and RC4 handshake, then a transparent encrypted or plaintext stream).
- `fbt-metainfo` gains magnet parsing and `Metainfo::from_info_bytes`.
- A new `fbt-dht` crate runs one actor task that owns one UDP socket per address family, one routing table per family, a token secret and a bounded peer store.

`fbt-session` then wires them in:
- the torrent actor gets a metadata phase: piece-level state moves into an `Option<Loaded>`, and connections survive the arrival of metadata;
- the session owns at most one `Dht`; each public torrent runs a DHT announcer next to its tracker announcer;
- per-peer PEX state lives in `torrent/pex.rs`.

**Tech Stack:** as milestone 1, plus `crypto-bigint` (768-bit modular exponentiation, constant time), `rc4` (RustCrypto), `socket2` (an IPv6-only UDP socket next to the IPv4 one).

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 2 is its section 8 item 2. The milestone 1 plan (`2026-11-10-milestone-1-core-download.md`) and PR #1's "Deferred to later milestones" list are the other inputs. This plan folds in the milestone 2 items from that list.

## Global Constraints

- Every BitTorrent component is written from scratch: KRPC, routing table, MSE, extension messages, magnet parsing. No BitTorrent crates (`mainline`, librqbit, `serde_bencode`, `bendy`, ...). General-purpose crates only, declared once in the root `[workspace.dependencies]`.
- `#![forbid(unsafe_code)]` in every crate, including the new `fbt-dht`.
- No panic, hang or unbounded allocation from remote input: peers, trackers, DHT datagrams, magnet URIs, MSE bytes. `fbt-dht` joins the four parser crates in denying `clippy::unwrap_used`, `expect_used`, `indexing_slicing` and `panic` in non-test code. Every remote list, map and buffer has a named cap constant.
- Bencode that comes from a peer or a DHT node is decoded with explicit `Limits` (depth and item count), never with `decode()`'s defaults.
- `cargo tree -e normal,build --no-default-features --features pure-rust -p fbt` (normal and build edges) must contain neither `ring` nor `cc` (nor `cmake`, `aws-lc*` or `*-sys` crates; `windows-sys` is bindings only). `bash dev/check-pure-rust.sh` passes after every task that adds a dependency. No `asm` features on the crypto crates.
- **No commit's test suite and no CI job contacts a public DHT node.** The defaults `router.bittorrent.com:6881`, `dht.transmissionbt.com:6881` and `router.utorrent.com:6881` live only in `fbt-session`'s `Settings::default()`. `fbt_dht::DhtConfig::default()` bootstraps from nothing. Every test either disables the DHT or bootstraps from a loopback node. The interop network is `internal: true`.
- The DHT never hammers anyone:
  - announces at most every `dht_announce_interval` (default 15 min);
  - bootstrap retries back off from 30 s, doubling to 15 min;
  - inbound queries are rate-limited per IP.
- Private torrents (BEP 27): no DHT lookups or announces, no PEX in either direction, no `Port` message. Trackers and user-given peers (including magnet `x.pe`) still work.
- Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0.
- `tracing` for logs (no subscriber installed by the library), `thiserror` for errors.
- Public enums and config structs of `fbt-session`/`fbt` are `#[non_exhaustive]`; config structs implement `Default`. Re-exported types from other crates that clients see (`Magnet`, `DhtStats`) are `#[non_exhaustive]` too.
- Builds and tests pass on macOS, Linux and Windows.
- Gitflow: work on `feature/m2-must-haves` off `develop`. Conventional Commits with crate scope (`feat(dht): ...`). **No `Co-Authored-By` trailer, no session id or link, no "Generated with" footer in any commit or PR.**
- Breaking changes to the `fbt` API are allowed (0.x) and each one goes in `CHANGELOG.md` under `### Changed`.

## Review Focus

1. **A peer advertises a huge `metadata_size`, or sends metadata pieces with an inconsistent `total_size` or a wrong length.** Expected: nothing is allocated beyond `max_metadata_size`. The peer is disconnected with `DisconnectReason::BadMetadata` and never asked again, and the fetch continues from others. Test: Task 13 `oversized_or_inconsistent_metadata_is_refused`.
2. **The DHT state blob a client restores is corrupt, truncated or from a future version.** Expected: `Session::new` still succeeds and the DHT starts fresh with a warning; it never fails or panics. Test: Task 14 `corrupt_dht_state_is_ignored`.
3. **The UDP port the DHT wants is taken, or the host has no IPv6.** Expected: the DHT falls back to an ephemeral IPv4 port, or runs IPv4-only, and the session starts. Tests: Task 14 `dht_port_taken_falls_back`, Task 7 `ipv6_bind_failure_keeps_ipv4_running`.
4. **A magnet torrent is paused, removed or the session shut down while metadata is still being fetched.** Expected: the command completes promptly, peers are closed, no file or directory is created, and `stopped` goes to trackers that answered. Test: Task 13 `remove_before_metadata_creates_no_files`.
5. **Forced encryption meets a plaintext-only peer, or a peer stalls halfway through the MSE handshake.** Expected: the connection ends within `handshake_timeout` with `DisconnectReason::Encryption(..)`, and the half-open slot is released. It counts as a failed attempt (no instant retry loop under `Forced`). Tests: Task 12 `forced_policy_against_plaintext_peer_fails_cleanly`, `stalled_mse_handshake_times_out`.

---

## Starting point

`develop` holds milestone 1 (`v0.1.0`, merged back from `release/0.1.0`). Execution starts with `git -C fbt-lib switch -c feature/m2-must-haves develop`. Check first that the workspace version on `develop` is `0.1.0`.

## File Structure

```
Cargo.toml                                    + crypto-bigint, rc4, socket2 in [workspace.dependencies]
crates/fbt-bencode/src/decode.rs              Limits::max_items, MAX_DEPTH_CAP
crates/fbt-wire/src/ext.rs                    BEP 10 handshake, ut_metadata (BEP 9), ut_pex (BEP 11)
crates/fbt-wire/src/mse/{mod.rs,dh.rs,stream.rs,handshake.rs}   MSE/PE
crates/fbt-metainfo/src/magnet.rs             magnet URIs; metainfo.rs gains from_info_bytes
crates/fbt-dht/                               new crate
  src/{lib.rs,id.rs,krpc.rs,table.rs,token.rs,store.rs,rate.rs,node.rs,server.rs,lookup.rs,state.rs}
  tests/swarm.rs                              multi-node loopback swarms
crates/fbt-session/src/
  settings.rs, event.rs, status.rs, error.rs, session.rs, listener.rs   (extended)
  peer/{conn.rs,ext.rs,messages.rs}           extension layer, no-metadata mode, MSE streams
  torrent/swarm.rs                            split out of peers.rs: Known, Retries, Peer, connect, limits
  torrent/peers.rs                            what stays: interest, requests, choking, availability
  torrent/metadata.rs                         metadata fetch and serve (pure)
  torrent/pex.rs                              PEX bookkeeping (pure)
  torrent/dht.rs                              per-torrent DHT announcer task
crates/fbt-session/tests/{magnet.rs,dht.rs,pex.rs,encryption.rs}
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-example/src/main.rs, tests/cli.rs
fuzz/fuzz_targets/{krpc.rs,extension.rs,mse.rs,magnet.rs}, fuzz/seeds/<new targets>/
.github/workflows/fuzz.yml                    corpus cache, workflow_dispatch, fbt-dht path
dev/interop/{compose.yaml,run.sh,README.md,qbittorrent/qBittorrent.conf,transmission/{settings.json,dht.bootstrap}}
```

---

### Task 1: bencode hard depth cap and item limit

**Files:**
- Modify: `crates/fbt-bencode/src/decode.rs`, `crates/fbt-bencode/src/lib.rs`.

**Interfaces:**
- Produces:
  - `pub struct Limits { pub max_depth: usize, pub max_items: usize }`, with `pub const fn new(max_depth: usize, max_items: usize) -> Limits`. `Default` is `max_depth: 64, max_items: 4_194_304`.
  - `pub const MAX_DEPTH_CAP: usize = 256;`. Every decode uses `limits.max_depth.min(MAX_DEPTH_CAP)`, so no caller can ask for unbounded recursion.
  - `DecodeError::TooManyItems { pos }`. Every value (int, string, list, dict, and each dict key) counts as one item; the error comes as soon as the count passes `max_items`.

- [ ] **Step 1: Write failing tests** in `decode.rs`

```rust
#[test] fn depth_is_capped_whatever_the_limit() {
    let deep = [b"l".repeat(300), b"e".repeat(300)].concat();
    let r = decode_with(&deep, Limits::new(usize::MAX, usize::MAX));
    assert!(matches!(r, Err(DecodeError::DepthExceeded { .. })));
    let ok = [b"l".repeat(256), b"e".repeat(256)].concat();
    assert!(decode_with(&ok, Limits::new(usize::MAX, usize::MAX)).is_ok());
}
#[test] fn item_limit_counts_every_node() {
    assert!(decode_with(b"li1ei2ei3ee", Limits::new(8, 4)).is_ok());   // list + 3 ints
    assert!(matches!(decode_with(b"li1ei2ei3ee", Limits::new(8, 3)), Err(DecodeError::TooManyItems { .. })));
    assert!(matches!(decode_with(b"d1:ai1ee", Limits::new(8, 2)), Err(DecodeError::TooManyItems { .. }))); // dict, key, value
}
#[test] fn prefix_respects_item_limit() {
    assert!(matches!(decode_prefix(b"li1ei2eeXX", Limits::new(8, 2)), Err(DecodeError::TooManyItems { .. })));
}
```

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-bencode`
- [ ] **Step 3: Implement.** The parser keeps an `items` counter next to `pos`. Update the one `Limits { max_depth }` literal in `decode_prefix`.
- [ ] **Step 4: Run.** `cargo test -p fbt-bencode && cargo test --workspace`: PASS, including the M1 `many_unsorted_keys_is_fast` test.
- [ ] **Step 5: Commit.** `feat(bencode): hard depth cap and item-count limit`

---

### Task 2: extension protocol messages (BEP 10, 9, 11)

**Files:**
- Create: `crates/fbt-wire/src/ext.rs`.
- Modify: `crates/fbt-wire/src/handshake.rs` (reserved setters), `crates/fbt-wire/src/lib.rs`, `crates/fbt-wire/Cargo.toml` (deps `fbt-bencode`, `bitflags`).

**Interfaces:**
- Consumes: `fbt_bencode::{decode_with, decode_prefix, Limits, DictBuilder, encode}`.
- Produces:
  - `Reserved::set_extended()` (byte 5 `|= 0x10`) and `Reserved::set_dht()` (byte 7 `|= 0x01`).
  - Constants:
    - `pub const EXT_HANDSHAKE_ID: u8 = 0;`
    - `pub const UT_METADATA: &str = "ut_metadata";` and `pub const UT_PEX: &str = "ut_pex";`
    - `pub const OUR_UT_PEX_ID: u8 = 1;` and `pub const OUR_UT_METADATA_ID: u8 = 2;` (libtorrent's numbering)
    - `pub const METADATA_PIECE_LEN: usize = 16_384;`
    - `pub const MAX_EXT_HANDSHAKE_LEN: usize = 65_536;`
    - `pub const MAX_PEX_ENTRIES: usize = 200;`
  - `#[derive(Debug, Clone, PartialEq, Eq, Default)] pub struct ExtHandshake`. Fields and their bencode keys:
    - `pub m: BTreeMap<String, u8>` (`m`)
    - `pub client: Option<String>` (`v`, cut to 64 bytes)
    - `pub listen_port: Option<u16>` (`p`, 0 dropped)
    - `pub reqq: Option<u32>` (`reqq`)
    - `pub metadata_size: Option<u32>` (`metadata_size`, negative or over `u32` dropped)
    - `pub your_ip: Option<IpAddr>` (`yourip`, 4 or 16 bytes)

    Methods: `encode(&self) -> Bytes`, `decode(payload: &[u8]) -> Result<ExtHandshake, ExtError>` and `id_of(&self, name: &str) -> Option<u8>` (`None` when absent or 0, the disable value). Decoding uses `Limits::new(4, 512)`. The `m` map keeps at most 64 entries and drops non-UTF-8 names and names over 64 bytes. A payload over `MAX_EXT_HANDSHAKE_LEN` gives `ExtError::TooLarge`. Unknown keys are ignored.
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub enum MetadataMsg { Request { piece: u32 }, Data { piece: u32, total_size: u32, data: Bytes }, Reject { piece: u32 } }`, with `encode(&self) -> Bytes` and `decode(payload: &Bytes) -> Result<MetadataMsg, ExtError>`.
    - The header dict is read with `decode_prefix(_, Limits::new(2, 16))`. For `Data`, the bytes after the dict are the piece (a zero-copy `Bytes::slice`).
    - Data longer than `METADATA_PIECE_LEN` gives `InvalidField("data")`.
    - An unknown `msg_type` gives `InvalidField("msg_type")`.
  - `bitflags! pub struct PexFlags: u8 { ENCRYPTION = 0x01; SEED = 0x02; UTP = 0x04; HOLEPUNCH = 0x08; CONNECTABLE = 0x10 }`.
  - `#[derive(Debug, Clone, PartialEq, Eq, Default)] pub struct PexMsg { pub added: Vec<(SocketAddr, PexFlags)>, pub dropped: Vec<SocketAddr> }`, with `encode(&self) -> Bytes` and `decode(payload: &[u8]) -> Result<PexMsg, ExtError>`.
    - Keys: `added`/`added.f` (6-byte compact), `added6`/`added6.f` (18-byte), `dropped`, `dropped6`.
    - A compact field whose length is not a multiple of the entry size gives `InvalidField`.
    - A missing or short `.f` means `PexFlags::empty()` for the uncovered entries. Unknown flag bits are dropped.
    - Each list is cut to `MAX_PEX_ENTRIES` while decoding, before allocation. Uses `Limits::new(2, 32)`.
  - `#[derive(Debug, thiserror::Error)] pub enum ExtError { Bencode(#[from] DecodeError), NotADict, MissingField(&'static str), InvalidField(&'static str), TooLarge }`.

- [ ] **Step 1: Write failing tests** in `crates/fbt-wire/tests/ext.rs`

```rust
#[test] fn reserved_bits() {
    let mut r = Reserved::default(); r.set_extended(); r.set_dht(); r.set_fast();
    assert_eq!(r.0, [0, 0, 0, 0, 0, 0x10, 0, 0x05]);
    assert!(r.supports_extended() && r.supports_dht() && r.supports_fast());
}
#[test] fn ext_handshake_roundtrip_and_disable() {
    let mut h = ExtHandshake { listen_port: Some(6881), reqq: Some(250), metadata_size: Some(31_235), client: Some("fbt/0.2.0".into()), your_ip: Some("10.1.2.3".parse().unwrap()), ..Default::default() };
    h.m.insert(UT_METADATA.into(), 2); h.m.insert(UT_PEX.into(), 0);
    let back = ExtHandshake::decode(&h.encode()).unwrap();
    assert_eq!(back, h);
    assert_eq!(back.id_of(UT_METADATA), Some(2));
    assert_eq!(back.id_of(UT_PEX), None);
}
#[test] fn ext_handshake_hostile_fields() {
    // negative metadata_size, port 0, 3-byte yourip, 100 m entries
    let mut m = DictBuilder::new();
    for i in 0..100 { m.insert(format!("x{i}").as_bytes(), 1i64); }
    let mut d = DictBuilder::new();
    d.insert(b"m", m.build()).insert(b"metadata_size", -5i64).insert(b"p", 0i64).insert(b"yourip", &b"abc"[..]);
    let h = ExtHandshake::decode(&encode(&d.build())).unwrap();
    assert_eq!((h.metadata_size, h.listen_port, h.your_ip), (None, None, None));
    assert_eq!(h.m.len(), 64);
    assert!(matches!(ExtHandshake::decode(&vec![b'x'; MAX_EXT_HANDSHAKE_LEN + 1]), Err(ExtError::TooLarge)));
}
#[test] fn ut_metadata_bep9_vectors() {
    assert_eq!(MetadataMsg::Request { piece: 0 }.encode(), &b"d8:msg_typei0e5:piecei0ee"[..]);
    assert_eq!(MetadataMsg::Reject { piece: 0 }.encode(), &b"d8:msg_typei2e5:piecei0ee"[..]);
    let wire = Bytes::from_static(b"d8:msg_typei1e5:piecei0e10:total_sizei34256eeDATA");
    assert_eq!(MetadataMsg::decode(&wire).unwrap(), MetadataMsg::Data { piece: 0, total_size: 34_256, data: Bytes::from_static(b"DATA") });
}
#[test] fn ut_metadata_rejects_oversized_data_and_bad_type() {
    let mut big = b"d8:msg_typei1e5:piecei0e10:total_sizei99999ee".to_vec(); big.extend(vec![0; 16_385]);
    assert!(matches!(MetadataMsg::decode(&Bytes::from(big)), Err(ExtError::InvalidField("data"))));
    assert!(matches!(MetadataMsg::decode(&Bytes::from_static(b"d8:msg_typei7e5:piecei0ee")), Err(ExtError::InvalidField("msg_type"))));
}
#[test] fn pex_roundtrip_flags_and_caps() {
    let m = PexMsg { added: vec![("10.0.0.1:6881".parse().unwrap(), PexFlags::SEED | PexFlags::CONNECTABLE), ("[::1]:51413".parse().unwrap(), PexFlags::ENCRYPTION)], dropped: vec!["10.0.0.2:1".parse().unwrap()] };
    assert_eq!(PexMsg::decode(&m.encode()).unwrap(), m);
    let mut d = DictBuilder::new(); d.insert(b"added", vec![1u8; 6 * 1000]);
    assert_eq!(PexMsg::decode(&encode(&d.build())).unwrap().added.len(), MAX_PEX_ENTRIES);
    let mut d = DictBuilder::new(); d.insert(b"added", vec![1u8; 7]);
    assert!(matches!(PexMsg::decode(&encode(&d.build())), Err(ExtError::InvalidField(_))));
}
```

Plus a proptest `ext_decoders_never_panic`: arbitrary bytes into all three decoders.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-wire --test ext`
- [ ] **Step 3: Implement** `ext.rs` as specified.
- [ ] **Step 4: Run.** `cargo test -p fbt-wire`: PASS.
- [ ] **Step 5: Commit.** `feat(wire): extension handshake, ut_metadata and ut_pex messages`

---

### Task 3: Message Stream Encryption (MSE/PE)

**Files:**
- Create: `crates/fbt-wire/src/mse/{mod.rs,dh.rs,stream.rs,handshake.rs}`, `crates/fbt-wire/tests/mse.rs`.
- Modify: root `Cargo.toml` (`crypto-bigint`, `rc4`, `rand` already present), `crates/fbt-wire/Cargo.toml`.

**Crate choice:** use `crypto-bigint` (0.7, `U768` with `modular::{MontyForm, MontyParams}`) rather than `num-bigint`, for three reasons. It is a RustCrypto crate, as the spec asks. Its fixed-width, allocation-free arithmetic suits a 768-bit modulus. And its exponentiation is constant time, which matters because the exponent is our secret key. Use the newest `rc4` release whose `cipher` version builds with the workspace's `sha1 0.11`, and note the version pair in the commit body. Neither crate enables `asm`.

**Interfaces:**
- Produces (re-exported from `fbt_wire`):
  - `bitflags! pub struct CryptoMethods: u32 { PLAINTEXT = 0x01; RC4 = 0x02 }`.
  - `pub const MSE_MAX_PAD: usize = 512;` and `pub const MSE_MAX_IA: usize = 1024;`. Our cap on `len(IA)`, which is bounded and generous for libtorrent's handshake-in-IA.
  - `pub struct MseStream<S>`, implementing `AsyncRead + AsyncWrite + Unpin` when `S: AsyncRead + AsyncWrite + Unpin`. Methods:
    - `MseStream::plaintext(stream: S, prefix: Vec<u8>) -> Self`: `prefix` is served before the stream's own bytes;
    - `is_encrypted(&self) -> bool`;
    - `get_ref(&self) -> &S`.
  - `pub async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(stream: S, info_hash: &[u8; 20], provide: CryptoMethods, initial_payload: &[u8]) -> Result<MseStream<S>, MseError>`.
  - `pub async fn respond<S: AsyncRead + AsyncWrite + Unpin>(stream: S, prefix: Vec<u8>, lookup: &(dyn Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync), allowed: CryptoMethods) -> Result<(MseStream<S>, [u8; 20]), MseError>`.
    - `prefix` holds the bytes already read for detection.
    - `lookup` maps `HASH('req2', SKEY)` to SKEY (the info hash).
    - It returns the stream and the SKEY.
    - IA bytes become the stream's first readable bytes.
  - `pub fn req2_hash(info_hash: &[u8; 20]) -> [u8; 20]`, which is `SHA1("req2" ‖ info_hash)`.
  - `pub fn is_plaintext_handshake_prefix(first: &[u8]) -> bool`: true if `first` is a prefix of `\x13BitTorrent protocol` (all 20 bytes are compared when present).
  - `#[derive(Debug, thiserror::Error)] pub enum MseError { Io(#[from] io::Error), NoSync, UnknownTorrent, NoCommonMethod, BadPublicKey, BadLength(&'static str) }`.

Protocol (the "Message Stream Encryption" spec), with values pinned:

- **Key exchange.**
  - `P` = the 768-bit prime `0xFFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563`, and `G = 2`.
  - Private keys are 160 random bits. Public keys and `S` travel as 96-byte big-endian values.
  - A received `Y` of 0, 1, `P-1` or ≥ `P` gives `BadPublicKey`.
- **Steps.** `HASH` is SHA-1. The 5 steps are:
  1. A→B: `Ya`, `PadA`.
  2. B→A: `Yb`, `PadB`.
  3. A→B: `HASH('req1', S)`, `HASH('req2', SKEY) xor HASH('req3', S)`, then `ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA))`, then `ENCRYPT(IA)`.
  4. B→A: `ENCRYPT(VC, crypto_select, len(PadD), PadD)`, then the payload stream.
  5. A→B: the payload stream.
- **Field values.**
  - VC is 8 zero bytes.
  - `crypto_provide`/`crypto_select` are 4 bytes big-endian.
  - Length fields are 2 bytes big-endian.
  - Pads are 0 to 512 random bytes (we send random lengths; PadC/PadD are zero bytes).
- **RC4 keys.** `keyA = HASH('keyA', S, SKEY)` and `keyB = HASH('keyB', S, SKEY)`. A encrypts with `keyA` and B with `keyB`. Each RC4 discards its first 1024 keystream bytes. The step 3/4 headers are always RC4-encrypted; the payload is RC4 or plaintext per `crypto_select`.
- **Synchronisation bounds.** B looks for `HASH('req1', S)` within the first 628 bytes after `Ya` (512 pad + 20 bytes + slack). A looks for the encrypted VC within 616 bytes after `Yb`. Past either bound the result is `NoSync`. No read buffers more than these bounds plus 96.
- **Selection.**
  - B selects RC4 when `provide & allowed` contains it, else PLAINTEXT, else `NoCommonMethod`.
  - A rejects a `crypto_select` that isn't exactly one of its provided bits (`NoCommonMethod`).
  - `len(PadC)`/`len(PadD)` over 512, or `len(IA)` over `MSE_MAX_IA`, gives `BadLength`.
- **Write path.** `MseStream` encrypts each byte exactly once. `poll_write` encrypts only the bytes it accepts into an internal buffer, and accepts no new bytes until that buffer drains.

- [ ] **Step 1: Write failing tests** in `crates/fbt-wire/tests/mse.rs`

```rust
fn lookup_for(ih: [u8; 20]) -> impl Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync { move |h| (*h == req2_hash(&ih)).then_some(ih) }
#[tokio::test] async fn rc4_roundtrip_both_directions() {
    let (a, b) = tokio::io::duplex(8192); let ih = [0xaa; 20]; let l = lookup_for(ih);
    let (i, r) = tokio::join!(initiate(a, &ih, CryptoMethods::RC4, b""), respond(b, Vec::new(), &l, CryptoMethods::all()));
    let (mut i, (mut r, skey)) = (i.unwrap(), r.unwrap());
    assert_eq!(skey, ih); assert!(i.is_encrypted() && r.is_encrypted());
    i.write_all(b"hello").await.unwrap(); i.flush().await.unwrap();
    let mut buf = [0; 5]; r.read_exact(&mut buf).await.unwrap(); assert_eq!(&buf, b"hello");
    r.write_all(b"world").await.unwrap(); r.flush().await.unwrap();
    i.read_exact(&mut buf).await.unwrap(); assert_eq!(&buf, b"world");
}
#[tokio::test] async fn plaintext_selected_when_responder_allows_only_plaintext() { /* provide all, allowed PLAINTEXT → !is_encrypted() on both, bytes pass */ }
#[tokio::test] async fn forced_initiator_vs_plaintext_only_responder() {
    // initiate(.., RC4) vs respond(.., PLAINTEXT): both sides Err(MseError::NoCommonMethod)
}
#[tokio::test] async fn initial_payload_is_first_read_on_responder() {
    // initiate(.., b"\x13BitTorrent protocol..68 bytes") → responder's first read_exact(68) yields exactly those bytes
}
#[tokio::test] async fn unknown_torrent_rejected() { /* lookup always None → respond Err(UnknownTorrent) */ }
#[tokio::test] async fn respond_gives_up_without_sync() {
    let junk: Vec<u8> = (0..2000u32).map(|i| (i * 7 + 3) as u8).collect();
    let r = respond(tokio::io::join(&junk[..], tokio::io::sink()), Vec::new(), &|_: &[u8; 20]| None, CryptoMethods::all()).await;
    assert!(matches!(r, Err(MseError::NoSync)));
}
#[test] fn plaintext_prefix_detection() {
    assert!(is_plaintext_handshake_prefix(b"\x13BitTorrent protocol"));
    assert!(is_plaintext_handshake_prefix(b"\x13BitT"));
    assert!(!is_plaintext_handshake_prefix(&[0x13; 20]));
}
#[test] fn req2_hash_vector() {
    assert_eq!(hex(&req2_hash(&[0xaa; 20])), "c9b0599dfa950cfbde978efc6aa0588f510fa315");
}
```

Unit tests in `dh.rs`: `shared_secret_agrees` (two key pairs, `a.shared(&b.public) == b.shared(&a.public)`) and `rejects_degenerate_public_keys` (0, 1, `P-1`, `P` and `0xff..ff` give `BadPublicKey`). Unit test in `stream.rs`: `rc4_discards_first_1024_bytes`, where our cipher over 16 zero bytes equals bytes `1024..1040` of a raw `rc4` keystream with the same key. Also `partial_writes_never_double_encrypt`: a writer that accepts 1 byte per poll still delivers the right plaintext.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-wire --test mse`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-wire && bash dev/check-pure-rust.sh && cargo deny --all-features check`: PASS.
- [ ] **Step 5: Commit.** `feat(wire): MSE/PE handshake and encrypted stream`

---

### Task 4: magnet URIs and metainfo from info bytes

**Files:**
- Create: `crates/fbt-metainfo/src/magnet.rs`, `crates/fbt-metainfo/tests/magnet.rs`.
- Modify: `crates/fbt-metainfo/src/metainfo.rs`, `crates/fbt-metainfo/src/lib.rs`.

**Interfaces:**
- Consumes: `fbt_core::{InfoHashes, InfoHashV1, InfoHashV2}`.
- Produces:
  - `#[derive(Debug, Clone, PartialEq, Eq)] #[non_exhaustive] pub struct Magnet`. Fields:
    - `pub info_hashes: InfoHashes`
    - `pub display_name: Option<String>`
    - `pub trackers: Vec<String>`
    - `pub peers: Vec<SocketAddr>`
    - `pub select_only: Option<Vec<RangeInclusive<usize>>>`

    Methods:
    - `Magnet::parse(uri: &str) -> Result<Magnet, MagnetError>` (also `FromStr`);
    - `Magnet::from_info_hashes(ih: InfoHashes) -> Magnet`;
    - `to_uri(&self) -> String`;
    - `is_selected(&self, file: usize) -> bool`, which is true when `select_only` is `None`.
  - `#[derive(Debug, thiserror::Error)] pub enum MagnetError { NotMagnet, TooLong, BadEncoding, MissingInfoHash, InvalidInfoHash(String), ConflictingInfoHash, InvalidSelection }`.
  - `impl Metainfo { pub fn magnet(&self) -> Magnet }`: info hashes, name and the first tracker of each tier.
  - `impl Metainfo { pub fn from_info_bytes(info: &[u8], expected: InfoHashes, trackers: Vec<Vec<String>>) -> Result<Metainfo, MetainfoError> }`. It parses `info` with the same rules as `from_bytes` by sharing one `parse_info` helper. It checks SHA-1 against `expected.v1` and SHA-256 against `expected.v2` (when the info dict has `meta version 2`). A mismatch gives `MetainfoError::InfoHashMismatch`. Trackers go through the same dedup and 64-URL cap. `comment`, `created_by` and `creation_date` are `None`.

Parsing rules:

- The scheme `magnet:?` is matched case-insensitively. The whole URI is at most 65_536 bytes (`TooLong`). Parameters are split on `&`, values are percent-decoded (`+` means space in `dn`), and invalid escapes give `BadEncoding`.
- **`xt`.**
  - `urn:btih:` takes 40 hex digits or 32 base32 characters (RFC 4648, either case).
  - `urn:btmh:1220` takes 64 hex digits, a SHA-256 multihash; other multihash codes give `InvalidInfoHash`.
  - Two different btih (or two btmh) values give `ConflictingInfoHash`.
  - btih plus btmh gives a hybrid identity.
  - With no `xt`, the result is `MissingInfoHash`.
- **`dn`, `tr`, `x.pe`.**
  - `dn` is sanitized with `sanitize_component`.
  - `tr` and `tr.N` are taken in order, deduplicated, at most 64.
  - `x.pe` and `x.pe.N` accept only IP-literal socket addresses (hostnames and port 0 are dropped), at most 200.
  - Unknown keys are ignored.
- **`so` (BEP 53).** Comma-separated indices and `a-b` ranges, at most 1000 entries, indices up to 1_000_000. `b < a` or junk gives `InvalidSelection`.

- [ ] **Step 1: Write failing tests** in `crates/fbt-metainfo/tests/magnet.rs`

```rust
const HEX: &str = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
#[test] fn parses_full_magnet() {
    let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&dn=My+Name&tr=udp%3A%2F%2Ft.example%3A80&tr=http%3A%2F%2Fa.example%2Fannounce&tr=udp%3A%2F%2Ft.example%3A80&x.pe=10.0.0.1:6881&x.pe=[::1]:51413&x.pe=host.example:1&so=0,2,4-6")).unwrap();
    assert_eq!(m.info_hashes.v1.unwrap().to_string(), HEX);
    assert_eq!(m.display_name.as_deref(), Some("My Name"));
    assert_eq!(m.trackers, ["udp://t.example:80", "http://a.example/announce"]);
    assert_eq!(m.peers.len(), 2);
    assert!(m.is_selected(5) && !m.is_selected(3) && !m.is_selected(7));
}
#[test] fn base32_equals_hex() {
    let a = Magnet::parse("magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK").unwrap();
    assert_eq!(a.info_hashes.v1.unwrap().to_string(), HEX);
}
#[test] fn btmh_and_hybrid() {
    let v2 = "a".repeat(64);
    let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btmh:1220{v2}")).unwrap();
    assert!(m.info_hashes.v1.is_some() && m.info_hashes.v2.is_some());
    assert!(matches!(Magnet::parse(&format!("magnet:?xt=urn:btmh:1114{v2}")), Err(MagnetError::InvalidInfoHash(_))));
}
#[test] fn rejects_bad_magnets() {
    assert!(matches!(Magnet::parse("http://x"), Err(MagnetError::NotMagnet)));
    assert!(matches!(Magnet::parse("magnet:?dn=x"), Err(MagnetError::MissingInfoHash)));
    assert!(matches!(Magnet::parse("magnet:?xt=urn:btih:zz"), Err(MagnetError::InvalidInfoHash(_))));
    assert!(matches!(Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btih:{}", "b".repeat(40))), Err(MagnetError::ConflictingInfoHash)));
    assert!(matches!(Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&dn=%zz")), Err(MagnetError::BadEncoding)));
    assert!(matches!(Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&so=5-2")), Err(MagnetError::InvalidSelection)));
    assert!(matches!(Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&tr={}", "a".repeat(70_000))), Err(MagnetError::TooLong)));
}
#[test] fn to_uri_roundtrips() { /* parse(m.to_uri()) == m for the full magnet above */ }
#[test] fn from_info_bytes_verifies_hash() {
    let (meta, _) = support::single_file_torrent();
    let raw = meta.raw_info().to_vec();
    let again = Metainfo::from_info_bytes(&raw, meta.info_hashes, vec![vec!["http://t/a".into()]]).unwrap();
    assert_eq!(again.info, meta.info);
    assert_eq!(again.trackers, vec![vec!["http://t/a".to_string()]]);
    let wrong = InfoHashes::from_v1(InfoHashV1([0; 20]));
    assert!(matches!(Metainfo::from_info_bytes(&raw, wrong, vec![]), Err(MetainfoError::InfoHashMismatch)));
}
#[test] fn metainfo_magnet_link() { /* fixture single.torrent: meta.magnet().to_uri() parses back to the same info hash and dn */ }
```

Plus a proptest `magnet_parse_never_panics` over arbitrary strings prefixed with `magnet:?`.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-metainfo`
- [ ] **Step 3: Implement.** Write base32 and percent-decoding by hand in `magnet.rs`; no crate for either.
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(metainfo): magnet URIs (btih, btmh, so) and metainfo from info bytes`

---

### Task 5: fbt-dht crate, node ids (BEP 42) and KRPC messages

**Files:**
- Create: `crates/fbt-dht/{Cargo.toml,src/lib.rs,src/id.rs,src/krpc.rs}`. The crate uses the parser-crate clippy deny block; deps are `fbt-core`, `fbt-bencode`, `sha1`, `rand`, `thiserror`, `tracing`, and `tokio` (later tasks).
- Modify: `CLAUDE.md` (crate list; `fbt-dht` joins the clippy-deny list).

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)] pub struct NodeId(pub [u8; 20])`, with:
    - `random(rng: &mut impl Rng) -> NodeId`;
    - `distance(&self, other: &NodeId) -> [u8; 20]` (XOR);
    - `bucket_index(&self, other: &NodeId) -> Option<usize>`: the number of leading zero bits of the distance, `0..=159`, and `None` when equal;
    - `Display`/`Debug` as lowercase hex.
  - BEP 42:
    - `pub fn bep42_id(ip: IpAddr, r: u8, rng: &mut impl Rng) -> NodeId`;
    - `NodeId::is_valid_for(&self, ip: IpAddr) -> bool`;
    - `pub fn is_local(ip: IpAddr) -> bool`: loopback, RFC 1918, link-local, CGNAT `100.64/10`, IPv6 unique-local `fc00::/7` and link-local. Local addresses are exempt from BEP 42.
    - The masks are `0x030f3fff` (IPv4, 4 bytes) and `0x0103070f1f3f7fff` (IPv6, first 8 bytes), with `r & 7` placed in the top 3 bits. The first 21 bits of the id equal the first 21 bits of `crc32c`, and the last id byte is `r`.
    - `pub(crate) fn crc32c(data: &[u8]) -> u32`: table-driven Castagnoli (polynomial `0x82F63B78` reflected), written here.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct NodeInfo { pub id: NodeId, pub addr: SocketAddr }`.
  - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] pub struct Want { pub v4: bool, pub v6: bool }` (BEP 32 `want`).
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub enum Query { Ping, FindNode { target: NodeId }, GetPeers { info_hash: [u8; 20] }, AnnouncePeer { info_hash: [u8; 20], port: u16, implied_port: bool, token: Vec<u8> }, Unknown(String) }`.
  - `pub struct QueryMsg { pub id: NodeId, pub query: Query, pub want: Want, pub read_only: bool }`.
  - `pub struct Response { pub id: NodeId, pub nodes: Vec<NodeInfo>, pub nodes6: Vec<NodeInfo>, pub values: Vec<SocketAddr>, pub token: Option<Vec<u8>> }`.
  - `pub enum Body { Query(QueryMsg), Response(Response), Error { code: i64, message: String } }`.
  - `pub struct Message { pub tid: Vec<u8>, pub body: Body, pub ip: Option<SocketAddr>, pub version: Option<Vec<u8>> }`, with `encode(&self) -> Vec<u8>` (canonical, sorted keys) and `decode(buf: &[u8]) -> Result<Message, KrpcError>`. All of these are `Debug + Clone + PartialEq + Eq`.
  - Constants:
    - `pub const KRPC_LIMITS: Limits = Limits::new(6, 600);`
    - `MAX_TID_LEN = 16`, `MAX_TOKEN_LEN = 64`, `MAX_VALUES = 200`, `MAX_NODES = 64` (compact entries per field; extra is cut)
    - `MAX_ERROR_MESSAGE = 256`
    - `MAX_DATAGRAM = 1500`
    - `pub const ERR_GENERIC: i64 = 201`, `ERR_SERVER = 202`, `ERR_PROTOCOL = 203`, `ERR_METHOD_UNKNOWN = 204`
  - `#[derive(Debug, thiserror::Error)] pub enum KrpcError { Bencode(#[from] DecodeError), Malformed(&'static str), TooLarge }`.

Wire rules:

- **Common keys.** `y` is `q`, `r` or `e`. The `ip` key is BEP 42's compact requester address (6 or 18 bytes). `ro: 1` (BEP 43) maps to `read_only`. `v` is kept up to 8 bytes.
- **Compact entries.** Nodes are 26 bytes (IPv4) or 38 bytes (IPv6). `values` entries are 6 or 18 bytes; others are skipped.
- **Unknown methods.** An unknown method name decodes to `Query::Unknown` (name cut to 32 bytes), so the server can answer 204 with the right `tid`.
- **Rejected input.**
  - A missing `t`, `y`, `id` (or `id` not 20 bytes), `info_hash` or `target` gives `Malformed`.
  - A `port` outside `1..=65535` gives `Malformed("port")`, unless `implied_port` is set.
  - Input over `MAX_DATAGRAM` gives `TooLarge`.

- [ ] **Step 1: Write failing tests** in `id.rs` and `krpc.rs`

```rust
#[test] fn bep42_vectors() {
    for (ip, r, id) in [
        ("124.31.75.21", 1u8, "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401"),
        ("21.75.31.124", 86, "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256"),
        ("65.23.51.170", 22, "a5d43220bc8f112a3d426c84764f8c2a1150e616"),
        ("84.124.73.14", 65, "1b0321dd1bb1fe518101ceef99462b947a01ff41"),
        ("43.213.53.83", 90, "e56f6cbf5b7c4be0237986d5243b87aa6d51305a"),
    ] {
        let ip: IpAddr = ip.parse().unwrap(); let want = id_from_hex(id);
        assert!(want.is_valid_for(ip));
        let ours = bep42_id(ip, r, &mut rng());
        assert_eq!(&ours.0[..2], &want.0[..2]); assert_eq!(ours.0[2] & 0xf8, want.0[2] & 0xf8); assert_eq!(ours.0[19], r);
    }
    assert!(!NodeId([0; 20]).is_valid_for("124.31.75.21".parse().unwrap()));
}
#[test] fn crc32c_check_value() { assert_eq!(crc32c(b"123456789"), 0xE306_9283); }
#[test] fn local_addresses_exempt() { for ip in ["127.0.0.1", "10.1.2.3", "192.168.0.1", "172.20.0.5", "::1", "fd00::1"] { assert!(is_local(ip.parse().unwrap())); } assert!(!is_local("8.8.8.8".parse().unwrap())); }
#[test] fn bucket_index_by_shared_prefix() {
    let a = NodeId([0; 20]); let mut b = [0; 20]; b[0] = 0x80; assert_eq!(a.bucket_index(&NodeId(b)), Some(0));
    let mut c = [0; 20]; c[19] = 1; assert_eq!(a.bucket_index(&NodeId(c)), Some(159)); assert_eq!(a.bucket_index(&a), None);
}
const BEP5: &[&[u8]] = &[
    b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
    b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
    b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee",
    b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
    b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
    b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
    b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re",
];
#[test] fn bep5_examples_roundtrip_byte_exact() { for m in BEP5 { assert_eq!(Message::decode(m).unwrap().encode(), *m); } }
#[test] fn bep32_43_42_fields() {
    // find_node with want [n4,n6] and ro=1 decodes Want{v4:true,v6:true}, read_only; response with nodes6 (38-byte) and ip (6-byte) roundtrips
}
#[test] fn unknown_method_keeps_tid() {
    let m = Message::decode(b"d1:ad2:id20:abcdefghij0123456789e1:q4:vote1:t2:xy1:y1:qe").unwrap();
    assert_eq!(m.tid, b"xy"); assert!(matches!(m.body, Body::Query(QueryMsg { query: Query::Unknown(ref n), .. }) if n == "vote"));
}
#[test] fn rejects_malformed() {
    assert!(matches!(Message::decode(b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe"), Err(KrpcError::Malformed("id"))));
    assert!(matches!(Message::decode(&vec![b'x'; 1501]), Err(KrpcError::TooLarge)));
    assert!(Message::decode(b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe").is_err());
}
#[test] fn caps_values_and_nodes() { /* response with 1000 values → MAX_VALUES kept; 100 compact nodes → MAX_NODES */ }
```

Plus a proptest `krpc_decode_never_panics`.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-dht && cargo clippy -p fbt-dht --all-targets -- -D warnings`: PASS.
- [ ] **Step 5: Commit.** `feat(dht): node ids with BEP 42 and KRPC messages`

---

### Task 6: routing table, tokens, peer store

**Files:**
- Create: `crates/fbt-dht/src/{table.rs,token.rs,store.rs,rate.rs}`.

**Interfaces:**
- Consumes: `NodeId`, `NodeInfo`, `is_local` (Task 5).
- Produces (all `pub(crate)`; the clock is passed in as `now: Instant` so tests need no timers):
  - `pub(crate) const K: usize = 8;`, `const REPLACEMENTS: usize = 8;`, `const QUESTIONABLE_AFTER: Duration = 15 min;`, `const BAD_AFTER_FAILURES: u32 = 2;`.
  - `pub(crate) struct RoutingTable`, with:
    - `new(own: NodeId, family: Family) -> Self`, where `pub enum Family { V4, V6 }`;
    - `own_id(&self) -> NodeId`;
    - `insert(&mut self, n: NodeInfo, now: Instant) -> Insert`;
    - `heard_from(&mut self, id: &NodeId, addr: SocketAddr, now: Instant)`;
    - `failed(&mut self, id: &NodeId)`;
    - `closest(&self, target: &NodeId, n: usize) -> Vec<NodeInfo>`;
    - `len(&self) -> usize`;
    - `stale_buckets(&self, now: Instant) -> Vec<usize>`;
    - `random_id_in_bucket(&self, i: usize, rng: &mut impl Rng) -> NodeId`;
    - `nodes(&self) -> impl Iterator<Item = &NodeInfo>`;
    - `reset_own(&mut self, own: NodeId)`.
  - `pub(crate) enum Insert { Added, Updated, Pending { ping: NodeInfo }, Rejected }`.
  - `pub(crate) struct Tokens`, with `new(rng: &mut impl Rng) -> Self`, `issue(&self, ip: IpAddr) -> Vec<u8>`, `verify(&self, ip: IpAddr, token: &[u8]) -> bool` and `rotate(&mut self, rng: &mut impl Rng)`. A token is the first 8 bytes of `SHA1(secret ‖ ip octets)`. The current and previous secrets verify. The node rotates every 5 min, so a token lives 5 to 10 min.
  - `pub(crate) struct PeerStore`, with `new(max_torrents: usize, max_peers: usize) -> Self`, `announce(&mut self, ih: [u8; 20], peer: SocketAddr, now: Instant)`, `peers(&self, ih: &[u8; 20], family: Family, max: usize, rng: &mut impl Rng) -> Vec<SocketAddr>` (random sample), `expire(&mut self, now: Instant)` (entries older than 30 min) and `torrents(&self) -> usize`. At capacity, a new info hash evicts the one with the fewest peers, and a new peer replaces a random one.
  - `pub(crate) struct RateLimiter`, with `new(per_sec: u32, max_ips: usize) -> Self` and `allow(&mut self, ip: IpAddr, now: Instant) -> bool`. Token bucket per IP with burst `2 × per_sec`. At most `max_ips` buckets (default 4096); the least recently used bucket is dropped beyond that.

Table rules:

- **Layout.** 160 fixed buckets indexed by `bucket_index`, each with `K` entries plus a replacement cache of `REPLACEMENTS`.
- **Node state.** A node is good if heard from within 15 min, questionable otherwise, and bad after 2 consecutive failures.
- **Inserting into a full bucket.**
  1. If the bucket holds a bad node, replace it.
  2. Otherwise, if it holds a questionable node, return `Pending { ping: oldest questionable }`. The newcomer waits in the replacement cache; a failed ping then replaces the questionable node with the newest cache entry.
  3. Otherwise drop the newcomer.
- **Rejected.** `Rejected` covers:
  - our own id;
  - a family mismatch;
  - an undialable address (port 0, unspecified, multicast);
  - a second node on the same non-local IP;
  - a known id reappearing on another address while the old entry isn't bad (anti-hijack).
- **Ordering.** `closest` orders by XOR distance and skips bad nodes.

- [ ] **Step 1: Write failing tests** (one module per file)
  - `insert_until_full_then_pending_ping_then_replace_on_failure`: 8 good nodes in bucket 0, a 9th gives `Pending`. Once time moves 16 min ahead and the pinged node gets `failed` twice, the 9th is in the table.
  - `bad_node_replaced_immediately`
  - `closest_sorted_by_xor_distance`: 50 random nodes; `closest(t, 8)` equals a brute-force sort.
  - `one_node_per_public_ip_but_local_exempt`: two ids on `8.8.8.8` keep one; two on `127.0.0.1` keep both.
  - `known_id_on_new_address_is_rejected`
  - `stale_buckets_after_15_minutes`
  - `reset_own_rebuckets_every_node`: `len()` is unchanged, and every node sits in `bucket_index(new_own, id)`.
  - `table_size_is_bounded`: 100_000 random inserts leave `len() <= 160 * K`.
  - `token_valid_across_one_rotation_only`, `token_bound_to_ip`
  - `store_caps_torrents_and_peers`, `store_expires_after_30_minutes`, `store_peers_filtered_by_family`
  - `rate_limiter_burst_then_refill`, `rate_limiter_memory_bounded`: 10_000 distinct IPs leave at most `max_ips` entries.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(dht): routing table, tokens, peer store and rate limiter`

---

### Task 7: DHT node runtime and query server

**Files:**
- Create: `crates/fbt-dht/src/{node.rs,server.rs}`. Deps `tokio` (`net`, `time`, `sync`, `rt`, `macros`) and `socket2`.
- Modify: `crates/fbt-dht/src/lib.rs`.

**Interfaces:**
- Consumes: Tasks 5 and 6.
- Produces (public):
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct DhtConfig`. Fields and defaults:
    - `bind_v4: Option<SocketAddr> = Some(0.0.0.0:6881)`
    - `bind_v6: Option<SocketAddr> = Some([::]:6881)`
    - `bootstrap: Vec<String> = []`
    - `read_only: bool = false`
    - `state: Option<DhtState> = None`
    - `query_timeout: Duration = 3 s`
    - `query_rate_per_ip: u32 = 5`
    - `enforce_node_id: bool = false`
    - `max_stored_torrents: usize = 2000`
    - `max_peers_per_torrent: usize = 500`
    - `max_pending_queries: usize = 1024`
  - `#[derive(Debug, Clone)] pub struct Dht`, with:
    - `pub async fn spawn(cfg: DhtConfig) -> Result<Dht, DhtError>`;
    - `pub fn local_addrs(&self) -> Vec<SocketAddr>`;
    - `pub fn add_node(&self, addr: SocketAddr)`, which pings and inserts on reply;
    - `pub fn stats(&self) -> DhtStats`, the latest `watch` value, published at most once a second;
    - `pub fn subscribe_stats(&self) -> watch::Receiver<DhtStats>`;
    - `pub async fn shutdown(&self)`, which is idempotent.
  - `#[derive(Debug, Clone, Default, PartialEq, Eq)] #[non_exhaustive] pub struct DhtStats { pub nodes_v4: usize, pub nodes_v6: usize, pub stored_info_hashes: usize, pub bootstrapped: bool, pub local_addrs: Vec<SocketAddr> }`.
  - `#[derive(Debug, thiserror::Error)] pub enum DhtError { Bind(io::Error), NoSocket, Shutdown, InvalidState(&'static str) }`.
  - Crate-internal: `pub(crate) async fn query(&self, addr: SocketAddr, q: Query) -> Result<Response, QueryError>`, with `QueryError::{Timeout, Remote { code: i64 }, Busy, Shutdown}`. `Busy` is returned when `max_pending_queries` are in flight. Task 8 builds lookups on it.

Behavior:

- **Sockets and task layout.** One actor task owns both sockets. The IPv6 socket is created with `socket2` and `set_only_v6(true)`, so both families can share the port number. A failed IPv6 bind logs a warning and runs IPv4-only; with no socket at all, `spawn` returns `NoSocket`. Datagrams over `MAX_DATAGRAM` or undecodable ones are dropped silently.
- **Transactions.** 2-byte transaction ids from a counter. A response is accepted only from the address the query went to, with a matching tid. Pending queries time out after `query_timeout`, and every timeout calls `table.failed`.
- **Answering queries** (`server.rs`):
  - `ping` returns `id`.
  - `find_node` returns the `K` closest nodes. They come from the asking family's table by default; `want` selects `nodes`/`nodes6` across both tables (BEP 32).
  - `get_peers` returns a `token` and up to 100 IPv4 or 50 IPv6 `values` when stored, plus `nodes` (or `nodes6`).
  - `announce_peer` with a token that fails `verify` gets error 203. Otherwise the peer is stored at `(source ip, implied_port ? source port : port)`.
  - An unknown method gets error 204.
  - Every response carries `ip` = the requester's address (BEP 42) and `v = b"FB" + [major, minor]`.
- **Rate limiting.** Queries beyond `RateLimiter::allow` (per source IP, `query_rate_per_ip`) are dropped without an answer.
- **Read-only mode (BEP 43).** A read-only node sends `ro: 1` in every query and drops every incoming query. Nodes whose queries carry `ro: 1` are answered but never inserted into the table.
- **Learning nodes.** Any node that answers our query is inserted (`Insert::Pending` triggers the ping).
- **Node ids.**
  - When `enforce_node_id` is set, a non-local node whose id fails `is_valid_for` is not inserted.
  - Every response's `ip` field is a vote for our external address. Three distinct reporter IPs agreeing on a non-local address make it our external IP. If our id isn't valid for that IP, it is regenerated with `bep42_id` and the table is rebuilt with `reset_own`. This logic lives in `pub(crate) struct ExternalIp { fn vote(&mut self, reporter: IpAddr, ip: IpAddr) -> Option<IpAddr> }`, which keeps at most 64 votes.
- **Housekeeping.** Every 60 s: expire the peer store, prune timed-out transactions, publish stats. Every 5 min: rotate the token secret.

- [ ] **Step 1: Write failing tests** in `node.rs`. They drive a spawned `Dht` on `127.0.0.1:0` (`bind_v6: None` unless stated) from a raw `tokio::net::UdpSocket` using `Message::encode/decode`.
  - `answers_ping_with_id_and_requester_ip`: the response `ip == Some(raw socket's local addr)`.
  - `unknown_method_gets_error_204`
  - `get_peers_then_announce_then_values_returned`: token from `get_peers`, `announce_peer` port 4000, then a second `get_peers` gives `values == [127.0.0.1:4000]`.
  - `implied_port_uses_source_port`
  - `announce_with_bad_token_gets_error_203`
  - `read_only_node_ignores_queries_and_flags_its_own`: with `read_only: true`, a ping gets no reply within 500 ms. `add_node(raw addr)` makes the raw socket receive a ping carrying `ro == 1`.
  - `queries_from_read_only_nodes_do_not_enter_table`: after 5 `ro=1` pings from distinct ids, `stats().nodes_v4 == 0`.
  - `query_flood_is_rate_limited`: with `query_rate_per_ip: 5`, 100 pings in one burst get between 1 and 10 answers.
  - `garbage_and_oversized_datagrams_are_ignored`: after 2000 bytes of junk and an empty datagram, the next ping is still answered.
  - `response_from_wrong_address_is_ignored`: our query to socket A, answered by socket B with the right tid, returns `QueryError::Timeout`.
  - `pending_queries_are_bounded`: `max_pending_queries: 4`, 10 queries to a silent socket, and the 5th onwards return `QueryError::Busy`.
  - `ipv6_bind_failure_keeps_ipv4_running`: `bind_v6` is an address that cannot bind (`[::1]:<port already bound by a std socket>`); `spawn` succeeds and `local_addrs()` holds only the IPv4 address.
  - `external_ip_votes_regenerate_id`: unit test on `ExternalIp`. Two votes give `None`, a third distinct reporter gives `Some(ip)`, and local IPs never win.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS on all three OSes.
- [ ] **Step 5: Commit.** `feat(dht): node runtime with query server, rate limiting and read-only mode`

---

### Task 8: lookups, bootstrap, announce and persistence

**Files:**
- Create: `crates/fbt-dht/src/{lookup.rs,state.rs}`, `crates/fbt-dht/tests/swarm.rs`.
- Modify: `crates/fbt-dht/src/{node.rs,lib.rs}`.

**Interfaces:**
- Consumes: `Dht::query`, `RoutingTable`, `Tokens` (Task 7).
- Produces:
  - `pub fn get_peers(&self, info_hash: [u8; 20]) -> PeerLookup` and `pub fn announce(&self, info_hash: [u8; 20], port: Option<u16>) -> PeerLookup`. `port: None` means `implied_port`. Each runs one lookup per bound family and yields peers as they arrive; `announce` then sends `announce_peer` to the `K` closest responders that gave a token.
  - `#[derive(Debug)] pub struct PeerLookup`, with `pub async fn next(&mut self) -> Option<Vec<SocketAddr>>`. It gives `None` once every family's lookup has finished. Dropping it cancels the lookup. Each batch is deduplicated and holds only dialable addresses.
  - `pub async fn state(&self) -> Result<DhtState, DhtError>`.
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct DhtState { pub id_v4: Option<NodeId>, pub id_v6: Option<NodeId>, pub nodes_v4: Vec<SocketAddr>, pub nodes_v6: Vec<SocketAddr> }`, with `to_bytes(&self) -> Vec<u8>` and `from_bytes(b: &[u8]) -> Result<DhtState, DhtError>`.
    - The format is a bencoded dict: `version` = 1, `id`, `id6`, `nodes` (6-byte compact addresses), `nodes6` (18-byte).
    - `from_bytes` decodes with `Limits::new(2, 16)`. A missing or different `version`, a wrong id length or a misaligned node string gives `InvalidState`.
    - At most 1000 nodes per family on both save and load.
  - `pub(crate) struct Lookup` (pure state machine), with:
    - `new(target: NodeId, seeds: Vec<NodeInfo>) -> Self`;
    - `next_queries(&mut self) -> Vec<NodeInfo>` (at most `ALPHA = 3` in flight);
    - `on_response(&mut self, from: &NodeId, nodes: &[NodeInfo], token: Option<Vec<u8>>)`;
    - `on_failure(&mut self, from: &NodeId)`;
    - `is_done(&self) -> bool`;
    - `responders(&self) -> Vec<(NodeInfo, Vec<u8>)>` (the `K` closest with tokens).

    It keeps at most 64 candidates sorted by distance and sends at most 200 queries in all (`MAX_LOOKUP_QUERIES`). It is done when the `K` closest known candidates have all answered or failed and nothing is in flight.

Behavior:

- **Bootstrap.**
  - Seeds come from restored `state` nodes plus the `bootstrap` entries. Those are resolved with `tokio::net::lookup_host` under a 5 s timeout each; entries that fail to resolve or are malformed are skipped with a debug log.
  - The bootstrap itself is a `find_node(own id)` lookup per family. `stats().bootstrapped` becomes true once a table has at least `K` nodes.
  - While the tables are empty, bootstrap is retried after 30 s, doubling up to 15 min.
  - A restored `id_v4`/`id_v6` replaces the random id.
- **Refresh.** Every 60 s, each stale bucket (up to the deepest non-empty one plus one) gets a `find_node` for `random_id_in_bucket`.
- **Lookup seeds.** Lookups start from `table.closest(target, K)`.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `lookup.rs`:
    - `queries_alpha_closest_first`
    - `closer_nodes_from_responses_are_queried_next`
    - `done_when_k_closest_answered`
    - `failures_do_not_stall`
    - `bounded_queries`: a generator answering with 8 fresh, ever-closer nodes each time stops at `MAX_LOOKUP_QUERIES`.
  - Unit tests in `state.rs`:
    - `state_roundtrip`
    - `rejects_future_version_and_garbage`: `version` 2, `b"garbage"` and a 25-byte `nodes` each give `Err(DhtError::InvalidState(_))`.
    - `caps_nodes_on_load`
  - Integration tests in `tests/swarm.rs`. Every node binds `127.0.0.1:0` with `query_rate_per_ip: 10_000`, `query_timeout: 500ms` and `bind_v6: None` unless stated.
    - `twenty_nodes_bootstrap_and_fill_tables`: node 0 has no bootstrap; nodes 1..20 bootstrap from node 0. Within 10 s every node's `stats().nodes_v4 >= 8` and `bootstrapped`.
    - `announce_then_get_peers_finds_the_peer`: node 19 runs `announce(ih, Some(4242))` to completion; node 3's `get_peers(ih)` yields `127.0.0.1:4242` within 5 s.
    - `ipv6_swarm_converges_when_available`: skipped (early return) if `std::net::UdpSocket::bind("[::1]:0")` fails. Ten nodes on `[::1]:0` with `bind_v4: None`; get_peers finds the announced peer `[::1]:4242`.
    - `restored_state_rejoins_without_bootstrap`: `state()` of node 5, then a new node with `state: Some(..)` and an empty `bootstrap` reaches `nodes_v4 >= 8` and keeps node 5's `id_v4`.
    - `silent_bootstrap_is_retried_with_backoff`: bootstrap points at a raw socket that never answers. Under `tokio::time::pause()`, 10 virtual minutes bring at most 6 `find_node` datagrams to it.
    - `malformed_bootstrap_entries_are_skipped`: `["no-port", "127.0.0.1:notaport"]` gives a running node with `bootstrapped == false` and no panic.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-dht`: PASS on all three OSes, with swarm tests deterministic over 20 local reruns (`for i in $(seq 20); do cargo test -p fbt-dht --test swarm -q || break; done`).
- [ ] **Step 5: Commit.** `feat(dht): iterative lookups, bootstrap with backoff, announce and state persistence`

---

### Task 9: session refactor: split peers.rs, `Loaded`, simultaneous-dial tie-break

**Files:**
- Create: `crates/fbt-session/src/torrent/swarm.rs`.
- Modify: `crates/fbt-session/src/torrent/{peers.rs,actor.rs,data.rs,serve.rs,mod.rs}`, `crates/fbt-session/tests/swarm.rs`.

**Interfaces:**
- Produces (crate-internal; no behavior change except the tie-break):
  - `torrent/swarm.rs` takes from `peers.rs`: `Known`, `MAX_KNOWN`, `MAX_NEVER`, `MAX_ATTEMPTS`, `dialable`, `AfterFailure`, `Retries`, `Peer` (with `Peer::new(addr, tx, generation, num_pieces)` unchanged), `new_key`, `queue_addr`, `remember`, `queue_retries`, `try_connect`, `on_incoming`, `send`, `close_peer`, `close_all`, `on_connected` and `on_disconnected`'s bookkeeping half. It also gets their unit tests.
  - `torrent/peers.rs` keeps the piece side: `on_peer_event` dispatch, `on_pieces`, `on_have`, `on_interested`, `set_choke`, `rechoke`, `on_rejected`, `wants_piece`, `update_interest`, `fill`, `top_up_all`, `partial_cap`, `pick_capped`, `evict_orphans`, plus their tests. Neither file exceeds about 700 lines.
  - `pub(super) struct Loaded { pub meta: Arc<Metainfo>, pub layout: PieceLayout, pub picker: Option<PiecePicker>, pub piece_prio: Vec<u8>, pub total_wanted: u64, pub file_prio: Vec<Priority>, pub bufs: HashMap<u32, Vec<u8>> }`. `Actor` replaces those fields with `pub(super) loaded: Option<Loaded>`, always `Some` in this task (Task 13 adds the `None` phase). Piece-level handlers in `data.rs`, `serve.rs` and `peers.rs` take `&mut Loaded` or return early on `None`.
  - `pub(super) fn keep_new_connection(our_id: &PeerId, their_id: &PeerId, new_outgoing: bool, old_outgoing: bool) -> bool`. When two connections share a peer id and differ in direction, keep the one initiated by the side with the lower peer id (bytewise). The same direction keeps the older connection (M1 behavior). The losing side closes with `DisconnectReason::Duplicate`. Both ends compute the same answer, so exactly one connection survives a simultaneous dial.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn tie_break_keeps_lower_id_initiator() {
    let lo = PeerId([1; 20]); let hi = PeerId([2; 20]);
    // we are lo: keep our outgoing, drop their incoming duplicate
    assert!(keep_new_connection(&lo, &hi, true, false));
    assert!(!keep_new_connection(&lo, &hi, false, true));
    // we are hi: keep their (incoming) connection
    assert!(keep_new_connection(&hi, &lo, false, true));
    assert!(!keep_new_connection(&hi, &lo, true, false));
    // same direction: older wins
    assert!(!keep_new_connection(&lo, &hi, true, true));
}
```

  Plus in `tests/swarm.rs`, `simultaneous_dial_leaves_one_connection`: sessions A and B (A seeding, B leeching) call `add_peer` on each other inside one `tokio::join!`. After 3 s each has `num_peers == 1`, and B finishes.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.** Move code without changing it first, and commit nothing until `cargo test -p fbt-session` passes again. Then add `Loaded` and the tie-break.
- [ ] **Step 4: Run.** `cargo test -p fbt-session && cargo clippy -p fbt-session --all-targets -- -D warnings`: PASS, with every M1 test unchanged.
- [ ] **Step 5: Commit.** `refactor(session): split swarm bookkeeping from piece logic; peer-id tie-break on simultaneous dial`

---

### Task 10: session milestone-1 debt: announcer plan, completed, left, choke and reject paths

**Files:**
- Modify: `crates/fbt-session/src/torrent/announcer.rs`, `crates/fbt-session/tests/{trackers.rs,swarm.rs,support/mod.rs}`.

**Interfaces:**
- Produces:
  - `fn plan(run: &Run, sig: &Signals, completed_sent: bool, now: Instant) -> (AnnounceEvent, Instant)`, a free function, pure, with `now` passed in. Also `fn clamp_intervals(resp: &AnnounceResponse) -> (Duration, Option<Duration>)` (both capped at `MAX_INTERVAL`), and `fn request_for(url: &str, base: &AnnounceRequest, tracker_ids: &HashMap<String, Vec<u8>>) -> AnnounceRequest` (echoes only that URL's `tracker id`). The task loop calls these.
  - Fix, duplicate `completed`: a `Completed` walk sets `completed_sent = true` as soon as the first request of the walk is sent, so an interrupted walk never sends a second `completed` in the same session. This is at-most-once semantics, as libtorrent has.
  - Fix, wrong `left` on a recheck `stopped`: the task keeps `last_left: u64` from the latest status whose state isn't `CheckingFiles`. `stopped` (and any announce while checking) reports `last_left`, not the transient checking figure.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `announcer.rs`:
    - `first_announce_is_started_now_or_after_backoff`
    - `regular_announce_after_interval`
    - `forced_reannounce_waits_min_interval`
    - `completed_once_even_after_restart`: `plan` with `completed_sent = true` never returns `Completed`.
    - `interval_above_two_hours_is_clamped`: `interval` 10 h gives `MAX_INTERVAL`, and `min_interval` 5 h gives `MAX_INTERVAL`.
    - `tracker_id_echoed_to_same_url_only`
  - Integration tests:
    - `interrupted_completed_walk_is_not_repeated` (`tests/trackers.rs`): the fake HTTP tracker delays its `completed` reply 2 s while the test pauses and resumes the torrent. The recorded events hold `completed` exactly once.
    - `recheck_stopped_reports_previous_left` (`tests/trackers.rs`): a leecher at 50 % calls `force_recheck`. The `stopped` announce's `left` equals the pre-recheck `total_wanted - wanted_done`.
    - `rechoke_chokes_peer_that_lost_its_slot` (`tests/swarm.rs`): `unchoke_slots = 2` and `rechoke_interval` 200 ms. Three scripted interested peers, where peer A uploads to us fastest. After two rounds a peer that was unchoked receives `Choke`.
    - `request_while_choked_gets_reject_with_fast` (`tests/swarm.rs`): a scripted fast peer requests a valid block while choked and receives `RejectRequest` for that block.
    - `request_while_choked_without_fast_is_dropped` (`tests/swarm.rs`): no `Piece` arrives within 1 s, and the connection stays up.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `fix(session): at-most-once completed, last known left on stopped, pure announce planning`

---

### Task 11: extension layer in peer connections

**Files:**
- Create: `crates/fbt-session/src/peer/ext.rs`.
- Modify: `crates/fbt-session/src/peer/{conn.rs,messages.rs}`, `crates/fbt-session/src/torrent/{swarm.rs,peers.rs}`, `crates/fbt-session/tests/support/mod.rs`.

**Interfaces:**
- Consumes: `fbt_wire::{ExtHandshake, MetadataMsg, PexMsg, Reserved, OUR_UT_METADATA_ID, OUR_UT_PEX_ID, UT_METADATA, UT_PEX}`.
- Produces:
  - `PeerCtx` changes: `layout: Option<PieceLayout>`, where `None` means metadata is still pending. It also gains `ext: ExtConfig`, defined as `pub(crate) struct ExtConfig { pub listen_port: u16, pub metadata_size: Option<u32>, pub pex: bool, pub dht_port: Option<u16>, pub reqq: u32 }`.
  - `PeerEvent` gains:
    - `ExtHandshake { key, hs: Box<ExtHandshake> }`
    - `Metadata { key, msg: MetadataMsg }`
    - `Pex { key, msg: PexMsg }`
    - `DhtPort { key, port: u16 }`
  - `PeerCommand` gains `MetadataReady { layout: PieceLayout }`, `SendMetadata(MetadataMsg)` and `SendPex(PexMsg)`.
  - `Peer` (swarm) gains `ext: Option<Box<ExtHandshake>>` and `listen_port: Option<u16>`.
  - `tests/support`: `ScriptedPeer::connect_with(addr, info_hash, reserved: Reserved) -> ScriptedPeer`, `send_ext_handshake(&mut self, hs: &ExtHandshake)` and `next_ext(&mut self, id: u8) -> Option<Bytes>`.

Behavior:

- **Handshake bits.** Our handshake sets `fast` and `extended`, plus `dht` when `ext.dht_port` is `Some`.
- **Our extension handshake.** Sent right after the BT handshake when the remote set the extended bit, before our bitfield. It carries:
  - `m = {ut_metadata: OUR_UT_METADATA_ID}`, plus `ut_pex: OUR_UT_PEX_ID` when `ext.pex`;
  - `p`, `reqq`, `v = "fbt/<version>"`, `yourip` (the remote's IP) and `metadata_size` when known.
- **Remote extension handshake.** Reported once as `ExtHandshake`; a repeat updates the remote ids and is reported again. A decode error is `DisconnectReason::Protocol`.
- **Incoming extended messages.** Routed by our ids: `OUR_UT_METADATA_ID` decodes to `Metadata`, `OUR_UT_PEX_ID` to `Pex`. A decode error in either is a protocol error. Unknown ids are ignored.
- **Outgoing extended messages.** `SendMetadata`/`SendPex` go out with the remote's ids. They are dropped silently when the remote hasn't enabled that extension.
- **`Port`.** We send `Port(dht_port)` after the handshake when both sides set the DHT bit. A received `Port(p)` with `p != 0` is reported once per connection; later ones are ignored.
- **No-metadata mode** (`layout: None`):
  - We send `HaveNone` (with fast) or nothing.
  - `Bitfield`, `HaveAll`, `HaveNone` and `Have` are buffered unvalidated: at most one bitfield, and at most 4096 `Have`s (`MAX_PENDING_HAVES`, extra ignored).
  - The codec limit is `1 << 20`.
  - Requests from the remote are rejected (fast) or dropped.
  - On `MetadataReady { layout }`, the conn validates the buffer against `layout`: a bad bitfield is `Protocol`, and out-of-range `Have`s are dropped. It then emits `Pieces` and `Have` exactly as if they had just arrived, and switches to normal mode with the codec limit from M1.

- [ ] **Step 1: Write failing tests** in `peer/conn.rs`, on the duplex harness extended with `reserved(fast, ext, dht)`:
  - `extended_handshake_exchanged`: the first message from us after the handshake is `Extended { id: 0 }`. Its decoded `m[ut_metadata] == 2`, `listen_port` matches, and the remote's handshake comes back as `PeerEvent::ExtHandshake`.
  - `no_extension_messages_without_the_bit`
  - `pex_not_advertised_when_disabled`
  - `port_sent_and_received_once`
  - `ext_messages_routed_by_our_ids`: id 2 gives `Metadata`, id 1 gives `Pex`, and id 9 is ignored while the connection stays up.
  - `malformed_ut_metadata_is_protocol_error`
  - `send_uses_remote_ids_and_drops_unsupported`: the remote maps `ut_metadata: 7` and has no `ut_pex`. `SendMetadata` goes out as `Extended { id: 7 }`, and `SendPex` sends nothing.
  - `availability_before_metadata_is_replayed`: `layout: None`; the remote sends `Bitfield([0b1010_0000])` and `Have(3)`, and no `Pieces` event appears. `MetadataReady` with 4 pieces gives `Pieces(Bits {0, 2})`, then `Have { piece: 3 }`.
  - `invalid_buffered_bitfield_fails_at_metadata`
  - `pending_haves_are_bounded`: 100_000 `Have`s before metadata, then `MetadataReady` with 200_000 pieces: at most 4096 `Have` events.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session peer`
- [ ] **Step 3: Implement.** Keep `ext.rs` as the remote-id map, our handshake builder and the decode dispatch, so `conn.rs` only routes. Torrent side: store `ExtHandshake` on `Peer`; ignore `Metadata`/`Pex`/`DhtPort` until Tasks 13 to 15.
- [ ] **Step 4: Run.** `cargo test -p fbt-session`: PASS.
- [ ] **Step 5: Commit.** `feat(session): extension handshake, Port and no-metadata mode in peer connections`

---

### Task 12: MSE in connections and the listener

**Files:**
- Modify: `crates/fbt-session/src/{settings.rs,listener.rs,session.rs}`, `crates/fbt-session/src/peer/{conn.rs,messages.rs}`, `crates/fbt-session/src/torrent/{mod.rs,swarm.rs}`, `crates/fbt-session/src/lib.rs`.
- Create: `crates/fbt-session/tests/encryption.rs`.

**Interfaces:**
- Consumes: `fbt_wire::{initiate, respond, MseStream, CryptoMethods, req2_hash, is_plaintext_handshake_prefix}`.
- Produces:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)] #[non_exhaustive] pub enum EncryptionPolicy { Forced, #[default] Preferred, Disabled }`, re-exported from the crate root. `Settings` gains `pub encryption: EncryptionPolicy`.
  - `pub(crate) type PeerStream = MseStream<TcpStream>`. `Cmd::Incoming.stream` and `spawn_incoming` take it.
  - `PeerEvent::Connected` gains `encrypted: bool`, and `Peer` stores it (for PEX flags).
  - `DisconnectReason::Encryption(String)`.
  - `Registry` gains `by_req2: HashMap<[u8; 20], [u8; 20]>`, kept with `by_wire`.
  - `Retries` gains `plaintext_only: HashSet<SocketAddr>` (bounded by `MAX_NEVER`, oldest forgotten) and `fn encryption_failed(&mut self, addr: SocketAddr) -> bool` (true when a plaintext retry is due).

Behavior:

- **Outgoing.** The whole exchange runs inside `handshake_timeout`, and the half-open slot is released on every exit.

  | Policy | What we do |
  |---|---|
  | `Forced` | `initiate(provide = RC4)` |
  | `Preferred` | `initiate(provide = RC4 \| PLAINTEXT)`, unless the address is `plaintext_only` |
  | `Disabled` | plaintext |

  IA is empty, and the BT handshake follows over the `MseStream`. An MSE failure is `Encryption(msg)`.
  - Under `Preferred`, the first `Encryption` failure from an address marks it `plaintext_only` and re-queues it at once without counting an attempt.
  - Under `Forced`, it counts as a normal failed attempt.
- **Incoming** (listener).
  1. Read up to 20 bytes within `handshake_timeout`. If `is_plaintext_handshake_prefix`, the connection is plaintext: refused under `Forced`, otherwise `MseStream::plaintext(stream, first)` and the M1 `read_handshake` path.
  2. Otherwise it is MSE: refused under `Disabled`, otherwise `respond(stream, first, &by_req2 lookup, allowed)`. `allowed` is `RC4` under `Forced` and `RC4 | PLAINTEXT` under `Preferred`.
  3. The BT handshake read from the stream must name the SKEY's torrent; a different info hash is dropped.

  Refused or failed incoming connections never reach a torrent and are logged at debug. Every step shares one `handshake_timeout` deadline.

- [ ] **Step 1: Write failing tests** in `tests/encryption.rs`

```rust
async fn pair(seed: EncryptionPolicy, leech: EncryptionPolicy) -> (Session, Session, TorrentHandle) { /* seeder with data, leecher add_peer(seeder) */ }
#[tokio::test] async fn forced_both_sides_downloads_encrypted() { /* TorrentFinished within 30 s; data identical */ }
#[tokio::test] async fn preferred_vs_disabled_falls_back_to_plaintext() { /* leecher Preferred, seeder Disabled → finishes; one PeerDisconnected{reason: Encryption(_)} then a plaintext connection */ }
#[tokio::test] async fn forced_policy_against_plaintext_peer_fails_cleanly() {
    // leecher Forced dials a seeder with Disabled: PeerDisconnected{reason: Encryption(_)} per attempt,
    // at most MAX_ATTEMPTS (3) attempts with peer_retry_delay 200 ms, never finishes, num_peers stays 0
}
#[tokio::test] async fn forced_listener_refuses_plaintext_handshake() { /* ScriptedPeer::connect (plaintext) to a Forced session → wait_closed within 2 s, torrent saw no PeerConnected */ }
#[tokio::test] async fn disabled_listener_refuses_mse() { /* raw fbt_wire::initiate against a Disabled session → Err */ }
#[tokio::test] async fn stalled_mse_handshake_times_out() { /* raw TCP sends 96 random bytes then nothing; handshake_timeout 500 ms → server closes within 1 s; a second torrent connection still works */ }
#[tokio::test] async fn incoming_mse_routes_by_skey_among_many_torrents() { /* 3 torrents in the seeder; MSE initiator for torrent #2 gets served torrent #2 */ }
```

  Plus incoming-handshake tests in `tests/lifecycle.rs`, covering M1 debt:
  - `incoming_unknown_info_hash_is_dropped`
  - `incoming_garbage_is_dropped`
  - `incoming_slow_handshake_times_out`
  - `incoming_valid_plaintext_handshake_is_routed`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test encryption --test lifecycle`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-session`: PASS, with every M1 swarm test passing under the default `Preferred`, so sessions talk to each other encrypted.
- [ ] **Step 5: Commit.** `feat(session): MSE/PE for outgoing and incoming connections with forced, preferred and disabled policies`

---

### Task 13: magnet torrents and metadata exchange

**Files:**
- Create: `crates/fbt-session/src/torrent/metadata.rs`, `crates/fbt-session/tests/magnet.rs`.
- Modify: `crates/fbt-session/src/{session.rs,settings.rs,event.rs,status.rs,error.rs,lib.rs}`, `crates/fbt-session/src/torrent/{mod.rs,actor.rs,swarm.rs,peers.rs,announcer.rs}`, every test that builds `AddTorrent` by field.

**Interfaces:**
- Consumes: `Magnet`, `Metainfo::from_info_bytes` (Task 4); `PeerEvent::{ExtHandshake, Metadata}`, `PeerCommand::{SendMetadata, MetadataReady}` (Task 11); `Loaded` (Task 9).
- Produces (public):
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub enum TorrentSource { Metainfo(Box<Metainfo>), Magnet(Magnet) }`.
  - `AddTorrent.metainfo` is replaced by `pub source: TorrentSource` (breaking, goes in the changelog). `AddTorrent::new(metainfo, save_path)` keeps its signature, and `AddTorrent::magnet(magnet: Magnet, save_path: PathBuf) -> AddTorrent` is added.
  - `Settings` gains `pub max_metadata_size: usize`, default 16 MiB, validated `16_384..=MAX_METADATA_SIZE` with `pub const MAX_METADATA_SIZE: usize = 64 * 1024 * 1024`.
  - `TorrentState::DownloadingMetadata`, `TorrentStatus.has_metadata: bool`, and `Event::MetadataReceived { torrent }` (category `STATUS`).
  - `DisconnectReason::BadMetadata`.
  - `SessionError::NoMetadata` and `SessionError::V2OnlyUnsupported`.
  - `TorrentHandle::metainfo(&self) -> Option<Arc<Metainfo>>`, sync, read from a `OnceLock` in `HandleInner`. `num_files` becomes that `OnceLock`'s file count.
- Produces (internal, `metadata.rs`, pure, `now` passed in):
  - `pub(super) struct MetadataFetch`, with:
    - `new(expected: InfoHashes, max_size: usize) -> Self`;
    - `on_ext_handshake(&mut self, key: PeerKey, metadata_size: Option<u32>, supports: bool, now: Instant) -> Vec<(PeerKey, MetadataMsg)>`;
    - `on_message(&mut self, key: PeerKey, msg: MetadataMsg, now: Instant) -> FetchStep`;
    - `on_peer_gone(&mut self, key: PeerKey) -> Vec<(PeerKey, MetadataMsg)>`;
    - `on_tick(&mut self, now: Instant) -> Vec<(PeerKey, MetadataMsg)>`.
  - `pub(super) enum FetchStep { Nothing, Send(Vec<(PeerKey, MetadataMsg)>), Bad(PeerKey), Done(Vec<u8>) }`.
  - `pub(super) fn serve_metadata(raw_info: Option<&[u8]>, piece: u32) -> MetadataMsg`.
  - `pub(super) const MAX_METADATA_SOURCES: usize = 2; const PIECE_TIMEOUT: Duration = 30 s; const MAX_METADATA_REQUESTS_PER_PEER: u32 = 256;`.

Fetch rules:

- **Sources.** A peer becomes a source when its handshake has `ut_metadata` and `metadata_size` is in `1..=max_size`. Peers with a size outside that range are not used and are disconnected with `BadMetadata`.
- **Requests.** At most `MAX_METADATA_SOURCES` peers are fetched from at once. Each fetches the whole metadata into its own buffer, allocated piece by piece as data arrives, with at most 2 requests in flight.
- **Bad data.** A `Data` whose `total_size` differs from the source's `metadata_size`, whose length isn't the expected piece length, or that wasn't requested is `Bad(key)`.
- **Rejects and timeouts.** A `Reject` or a 30 s timeout drops the source without a ban, and the next candidate starts.
- **Completion.** When a buffer completes, its SHA-1 (and SHA-256 when the expected hashes include v2) must match, else `Bad(key)`. A match is `Done(bytes)`.
- **Bans.** A `Bad` peer is closed with `BadMetadata`, and its address goes into `Retries::never`.

Actor behavior:

- **Adding a magnet.**
  - `btmh` only (no v1) gives `V2OnlyUnsupported`.
  - Non-empty `file_priorities` gives `InvalidPriorities { expected: 0, got }`.
  - The registry key is the magnet's `InfoHashes`, which stays the torrent's identity after metadata.
- **Start.** The actor starts in `DownloadingMetadata` with `loaded: None`. Magnet `tr`s feed the announcer, as one tier each. `x.pe` peers are queued like `add_peer` (Task 14 adds the `PeersFound { source: Magnet }` event for them).
- **Announcing without metadata.** Announces report `left = 16_384`, so trackers treat us as a leecher.
- **On `Done`.**
  1. `Metainfo::from_info_bytes(bytes, ih, magnet trackers)`. A parse failure puts the torrent in `TorrentState::Error` with the message.
  2. Build `Loaded`, applying `so=` as `Priority::Skip` for unselected files.
  3. Fill the handle's `OnceLock`.
  4. Emit `MetadataReceived`.
  5. Send `MetadataReady` to every peer.
  6. Run `start_check`, then the M1 flow.
- **Serving metadata.** `ut_metadata` requests are answered from `meta.raw_info()`, using `serve_metadata` per request. More than `MAX_METADATA_REQUESTS_PER_PEER` from one connection get `Reject`.
- **Commands before metadata.**
  - `pause`, `resume`, `remove`, `force_reannounce` and `add_peer` work.
  - `set_file_priorities` and `force_recheck` give `NoMetadata`.
  - `pieces()` gives an empty bitfield.
  - `remove(DeleteData)` deletes nothing, since no storage exists yet.
- **Storage.** For a magnet, storage is created only after metadata, as `FsStorage::new(save_path, &meta)` or the custom `storage`.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `metadata.rs`:
    - `fetches_all_pieces_and_verifies`: a 40_000-byte info dict over 3 pieces gives `Done(bytes)`.
    - `wrong_hash_bans_source`
    - `inconsistent_total_size_is_bad`
    - `wrong_piece_length_is_bad`
    - `unrequested_data_is_bad`
    - `size_over_cap_is_not_requested`
    - `reject_moves_to_next_source_without_ban`
    - `timeout_moves_to_next_source`
    - `at_most_two_sources`
    - `serve_metadata_pieces_and_reject`: the last piece is short, and out-of-range or `None` gives `Reject`.
  - Integration tests in `tests/magnet.rs` (the session has no DHT yet; these tests find peers through `x.pe` hints):
    - `magnet_with_peer_hint_downloads`: STATUS events in order are `TorrentAdded`, `StateChanged { DownloadingMetadata }`, `MetadataReceived`, …, `TorrentFinished`. Files are identical, `handle.metainfo().unwrap().info.name == "torrent"`, and `status().has_metadata`.
    - `metadata_from_lying_peer_is_rejected_then_fetched_from_honest_one`: a scripted peer advertises the right size but serves flipped bytes. Expect `PeerDisconnected { reason: BadMetadata }` for it, then `MetadataReceived` from the honest seeder.
    - `oversized_or_inconsistent_metadata_is_refused` (Review Focus 1): with `max_metadata_size` 16 KiB, a scripted peer claiming 1 GiB receives no `ut_metadata` request within 2 s and is disconnected with `BadMetadata`. A second one, serving `total_size` different from its advertised size, also gets `BadMetadata`.
    - `so_selection_skips_unselected_files`: three files with `so=0,2`; the middle file's non-boundary bytes are absent.
    - `remove_before_metadata_creates_no_files` (Review Focus 4): a magnet pointing at a silent scripted peer; `remove(DeleteData)` returns within 1 s and the save dir is still empty. The same goes for `pause` then `resume` and for `shutdown`.
    - `seeder_serves_metadata_and_rejects_out_of_range`
    - `magnet_v2_only_rejected`
    - `magnet_priorities_rejected_until_metadata`: `InvalidPriorities { expected: 0, .. }` at add, `NoMetadata` from `set_file_priorities` before metadata.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): magnet torrents with BEP 9 metadata exchange`

---

### Task 14: DHT in the session

**Files:**
- Create: `crates/fbt-session/src/torrent/dht.rs`, `crates/fbt-session/tests/dht.rs`.
- Modify: `crates/fbt-session/src/{session.rs,settings.rs,event.rs,lib.rs}`, `crates/fbt-session/src/torrent/{actor.rs,swarm.rs,mod.rs}`, `crates/fbt-session/Cargo.toml` (dep `fbt-dht`), `crates/fbt-session/tests/support/mod.rs` (`test_settings()` sets `dht_enabled = false`), `crates/fbt-example/src/main.rs` and `tests/cli.rs`. The example gains `--no-dht` on `download` and `seed`, and the CLI tests pass it, so no commit's tests reach the public routers.

**Interfaces:**
- Consumes: `fbt_dht::{Dht, DhtConfig, DhtState, DhtStats, PeerLookup}`.
- Produces (public):
  - `Settings` gains these fields (defaults shown):
    - `dht_enabled: bool = true`
    - `dht_bootstrap_nodes: Vec<String> = DEFAULT_DHT_BOOTSTRAP` mapped to strings. `pub const DEFAULT_DHT_BOOTSTRAP: [&str; 3] = ["router.bittorrent.com:6881", "dht.transmissionbt.com:6881", "router.utorrent.com:6881"]`. Each entry must be `host:port` with a valid `u16`, or `InvalidSettings`.
    - `dht_ipv6: bool = true`
    - `dht_read_only: bool = false`
    - `dht_state: Option<Vec<u8>> = None`
    - `dht_announce_interval: Duration = 15 min` (non-zero)
    - hidden: `dht_query_timeout: Duration = 3 s` and `dht_query_rate_per_ip: u32 = 5`
  - `Session::dht_stats(&self) -> Option<DhtStats>` (sync) and `async fn dht_state(&self) -> Result<Option<Vec<u8>>, SessionError>`, which returns `DhtState::to_bytes()`.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] #[non_exhaustive] pub enum PeerSource { Tracker, Dht, Pex, Magnet }`.
  - Two new events:
    - `Event::PeersFound { torrent, source: PeerSource, count: usize }` (category `PEER`). It is emitted per batch for tracker, DHT and magnet peers (PEX in Task 15).
    - `Event::DhtBootstrapped { nodes: usize }` (category `DHT`, emitted once per session). `EventMask::DHT = 1 << 5`, and `ALL` includes it.
  - `pub use fbt_dht::DhtStats;` from `fbt-session`.
- Internal: `Shared` gains `dht: Option<Dht>` and `dht_port: Option<u16>`. `torrent/dht.rs` holds `pub(super) fn spawn(p: DhtParams) -> (DhtAnnouncer, mpsc::Receiver<Vec<SocketAddr>>)`, driven by the same `Signals` watch as the tracker announcer.

Behavior:

- **Starting the DHT.** `Session::new` with `dht_enabled` binds the DHT on the TCP listener's IP and port (`[::]` with the same port when `dht_ipv6`). If that UDP port is taken, it falls back to port 0. A `dht_state` that fails `DhtState::from_bytes` is ignored with a `warn!`, and the DHT starts fresh.
- **Per-torrent announcer.** For a torrent that is not private and running:
  - `dht.announce(wire_hash, Some(listen_port))` immediately, then every `dht_announce_interval`;
  - every 60 s instead while `DownloadingMetadata` (never more often);
  - found peers go to the candidate queue, as `PeersFound { source: Dht }`.
- **Private torrents.**
  - No DHT announcer runs.
  - The `dht` reserved bit and the `Port` message are suppressed (`ExtConfig.dht_port: None`), and incoming `DhtPort` is ignored.
  - A magnet whose metadata turns out private stops its DHT announcer at `MetadataReceived`.
- **`Port` messages.** `PeerEvent::DhtPort` on a public torrent calls `dht.add_node((peer ip, port))`.
- **Shutdown.** `Session::shutdown` shuts the DHT down after the torrents.

- [ ] **Step 1: Write failing tests** in `tests/dht.rs`. All use a loopback `fbt_dht::Dht` as bootstrap (`bind_v6: None`, `query_rate_per_ip: 10_000`) and settings with `dht_bootstrap_nodes = [bootstrap addr]`, `dht_ipv6 = false`, `dht_query_timeout = 500ms`, `dht_query_rate_per_ip = 10_000`.

```rust
#[tokio::test] async fn magnet_download_via_dht_only() {
    // seeder: torrent with no trackers, DHT on; leecher: AddTorrent::magnet(Magnet::from_info_hashes(ih)) (no tr, no x.pe)
    // → PeersFound{source: PeerSource::Dht, ..}, MetadataReceived, TorrentFinished within 60 s; data identical
}
#[tokio::test] async fn private_torrent_never_touches_dht() {
    // private seeder, dht_announce_interval 1 s; after 5 s bootstrap.stats().stored_info_hashes == 0;
    // a ScriptedPeer with the dht bit sees no Port message and our reserved byte 7 & 0x01 == 0 for that torrent
}
#[tokio::test] async fn port_message_adds_node() { /* ScriptedPeer (dht bit) sends Port(raw Dht's port) → session.dht_stats().nodes_v4 >= 1 within 3 s */ }
#[tokio::test] async fn dht_state_roundtrip_through_session() { /* s1.dht_state() → s2 with dht_state Some and empty bootstrap list → s2.dht_stats().nodes_v4 >= 1 within 3 s */ }
#[tokio::test] async fn corrupt_dht_state_is_ignored() { /* dht_state Some(b"garbage") → Session::new Ok, dht_stats() Some */ }
#[tokio::test] async fn dht_port_taken_falls_back() { /* hold UdpSocket on 127.0.0.1:P, TcpListener free on P; Session listen P → Ok, dht local addr port != P */ }
#[tokio::test] async fn dht_disabled_means_none() { /* dht_enabled false → dht_stats() None, dht_state() Ok(None) */ }
#[tokio::test] async fn dht_bootstrapped_event_once() {}
#[test] fn bootstrap_entries_validated() { /* "nohost" → InvalidSettings naming dht_bootstrap_nodes */ }
```

  Plus `defaults_ship_public_bootstrap_nodes`: `Settings::default().dht_bootstrap_nodes` equals the three names, and `fbt_dht::DhtConfig::default().bootstrap.is_empty()`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS. Then confirm no test touches a public node: `grep -rn "router.bittorrent\|transmissionbt\|utorrent" crates/*/tests crates/*/src` shows only `settings.rs` and the `defaults_ship_public_bootstrap_nodes` test.
- [ ] **Step 5: Commit.** `feat(session): DHT peer discovery, Port messages and DHT state save/restore`

---

### Task 15: PEX

**Files:**
- Create: `crates/fbt-session/src/torrent/pex.rs`, `crates/fbt-session/tests/pex.rs`.
- Modify: `crates/fbt-session/src/{settings.rs}`, `crates/fbt-session/src/torrent/{actor.rs,swarm.rs,peers.rs}`.

**Interfaces:**
- Consumes: `PexMsg`, `PexFlags` (Task 2); `PeerEvent::Pex`, `PeerCommand::SendPex` (Task 11); `PeerSource::Pex`, `Event::PeersFound` (Task 14).
- Produces:
  - `Settings` gains `pub pex_enabled: bool = true` and a hidden `pub pex_interval: Duration = 60 s` (non-zero).
  - `pub(super) const MAX_PEX_ADDED: usize = 50; const MAX_PEX_DROPPED: usize = 50;`. The receive gap is `pex_interval * 3 / 4` (45 s by default).
  - `pub(super) struct PexPeer { sent: HashSet<SocketAddr>, last_sent: Option<Instant>, last_received: Option<Instant> }`, with:
    - `fn build(&mut self, live: &[(SocketAddr, PexFlags)], exclude: SocketAddr, now: Instant, interval: Duration) -> Option<PexMsg>`. It gives `None` before `interval` has passed since the last send or when nothing changed. `added` holds at most 50 new addresses and `dropped` at most 50 addresses gone since the last message. The recipient itself is excluded.
    - `fn accept(&mut self, now: Instant, gap: Duration) -> bool`. It is the receive rate gate: one message per `gap`.
  - `pub(super) fn connectable_addr(p: &Peer) -> Option<SocketAddr>`: the address for outgoing peers, `(ip, listen_port)` for incoming peers whose extension handshake carried `p`, `None` otherwise.

Behavior:

- **When PEX runs.** Only when `pex_enabled`, the torrent isn't private, and the peer advertised `ut_pex`.
- **Sending.** Every `pex_interval` tick, each such peer gets `build(...)` over the connected peers' connectable addresses. Flags: `ENCRYPTION` if that connection is encrypted, `SEED` if it is a seed, `CONNECTABLE` always (only connectable addresses are listed).
- **Receiving.** A message failing `accept` is dropped. Otherwise the first `MAX_PEX_ADDED` dialable added addresses go to the candidate queue through `Known`, and `PeersFound { source: Pex, count }` is emitted. `dropped` is ignored, since peers we know stay known until connections fail.
- **Private torrents.** A private torrent ignores incoming `ut_pex`, and its extension handshake omits `ut_pex` (`ExtConfig.pex = false`).

- [ ] **Step 1: Write failing tests**
  - Unit tests in `pex.rs`:
    - `first_message_lists_connected_peers`
    - `diff_lists_added_and_dropped`
    - `never_sends_before_interval`
    - `caps_added_and_dropped_at_50`
    - `excludes_recipient`
    - `receive_gate_drops_bursts`
    - `incoming_peer_without_listen_port_is_not_shared`
  - Integration tests in `tests/pex.rs`, with `pex_interval` 500 ms (so the receive gap is 375 ms).
    - `third_peer_learned_through_pex`: A seeds; B leeches with `add_peer(A)`; C leeches with only `add_peer(B)`. C gets `PeersFound { source: Pex }` containing A's listen address, then `num_peers == 2`, and finishes.
    - `private_torrent_sends_and_accepts_no_pex`: same topology with a private torrent. Within 5 s, C sees no `PeersFound { source: Pex }`, and a scripted peer advertising `ut_pex` sees no `ut_pex` in our handshake and gets no PEX message.
    - `pex_disabled_setting_disables_both_directions`
    - `pex_flood_is_bounded`: a scripted peer sends 20 `ut_pex` messages of 200 added addresses each, back to back. At most `MAX_PEX_ADDED` addresses are taken per accepted message, and only one message is accepted per gap.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session pex`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): peer exchange (BEP 11), off for private torrents`

---

### Task 16: fbt facade and example CLI

**Files:**
- Modify: `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`, `crates/fbt-example/src/main.rs`, `crates/fbt-example/tests/cli.rs`.

**Interfaces:**
- Consumes: everything new and public from Tasks 4 and 12 to 15.
- Produces:
  - New `fbt` re-exports: `Magnet`, `MagnetError`, `TorrentSource`, `EncryptionPolicy`, `PeerSource`, `DhtStats`, `DEFAULT_DHT_BOOTSTRAP`, `MAX_METADATA_SIZE`. `lib_surface.rs` names each of them.
  - Crate docs: a second `no_run` doctest adds a magnet and waits for `MetadataReceived`. There is also a section on encryption policy, the DHT defaults (public routers, overridable through `dht_bootstrap_nodes`) and `dht_state()` persistence.
  - `fbt-example`:
    - `download <TORRENT_OR_MAGNET>`: a value starting with `magnet:` is parsed as a magnet, anything else read as a file. New flags on `download` and `seed`:
      - `--encryption <forced|preferred|disabled>`
      - `--dht-bootstrap <HOST:PORT>`, repeatable; it replaces the defaults
      - `--no-dht` (from Task 14)
      - `--no-pex`
    - `info <TORRENT>` also prints `magnet: <uri>`.
    - New `dht-node [--port <N>] [--dht-bootstrap <HOST:PORT>]... [--run-for <SECS>]`: a session with the DHT on, no torrents and no default bootstrap. It prints `dht node listening on <addr>` and then every 10 s `dht nodes: v4=<n> v6=<n> torrents=<n>`.
    - Extra output lines from `download`/`seed`, used by the interop greps:
      - `metadata received: <name>`
      - `found <n> peers via <tracker|dht|pex|magnet>`
      - `peer connected <addr>`
    - The progress line shows state `metadata` while `DownloadingMetadata`.

- [ ] **Step 1: Write failing tests** in `tests/cli.rs`:
  - `info_prints_magnet_link`
  - `download_magnet_from_seed_process`: `seed --no-dht --port 0` (port read from a new first output line `listening on <addr>`), then `download "magnet:?xt=urn:btih:<hash>&x.pe=127.0.0.1:<port>" --no-dht --out <tmp>`. Exit 0 within 60 s, files identical, stdout contains `metadata received:`.
  - `encryption_forced_download_from_seed_process`
  - `dht_node_starts_and_stops`: `dht-node --port 0 --run-for 1` exits 0 and printed `dht node listening on`.
  - `bad_magnet_is_a_clean_error`: exit 1 and stderr starting with `error:`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-example -p fbt && RUSTDOCFLAGS="-D warnings" cargo doc -p fbt --no-deps`: PASS.
- [ ] **Step 5: Commit.** `feat(fbt): magnet, encryption and DHT in the facade and example CLI`

---

### Task 17: fuzz targets and fuzz workflow

**Files:**
- Create: `fuzz/fuzz_targets/{krpc.rs,extension.rs,mse.rs,magnet.rs}`, `fuzz/seeds/{krpc,extension,mse,magnet}/`.
- Modify: `fuzz/Cargo.toml` (deps `fbt-dht`, `tokio` with `rt` and `io-util`; four `[[bin]]`s), `.github/workflows/fuzz.yml`, `CLAUDE.md` (fuzz target list).

**Interfaces:**
- Consumes:
  - `fbt_dht::krpc::Message::decode`: `pub mod krpc` is exposed `#[doc(hidden)]` for this.
  - `ExtHandshake::decode`, `MetadataMsg::decode`, `PexMsg::decode`.
  - `fbt_wire::respond` and `initiate`.
  - `Magnet::parse`.

- [ ] **Step 1: Write the targets.**
  - `krpc`: decode, and on `Ok` re-encode and decode again, asserting equality.
  - `extension`: the first byte selects the decoder (handshake, ut_metadata, ut_pex), the rest is the payload. Re-encode on `Ok`.
  - `mse`: in a current-thread runtime, the first byte selects the role.
    - Responder: `respond(tokio::io::join(&data[1..], tokio::io::sink()), Vec::new(), &|_| Some([0; 20]), CryptoMethods::all())`.
    - Initiator: `initiate(join(&data[1..], sink()), &[0; 20], CryptoMethods::all(), b"")`.
    - Both must return within the input, with no panic and no hang.
  - `magnet`: `Magnet::parse(&String::from_utf8_lossy(data))`, and on `Ok` `parse(to_uri())` must succeed.

  Seeds: the BEP 5 examples, the BEP 9 vectors, a PEX message, a captured MSE step 1 (96 bytes plus a pad), and the magnets from Task 4's tests.
- [ ] **Step 2: Run each new target locally for 60 s.** `cargo +nightly fuzz run <target> fuzz/corpus/<target> fuzz/seeds/<target> -- -max_total_time=60` should report no crashes. Fix any finding in the owning crate, with a regression test, before continuing.
- [ ] **Step 3: Update `fuzz.yml`.**
  - Add the four targets to the matrix, and `crates/fbt-dht/**` to the PR paths.
  - Add `workflow_dispatch` with input `seconds` (default `600`); the run time is the input, else 1200 on schedule, else 60.
  - Persist the corpus with `actions/cache@v4`: path `fuzz/corpus/${{ matrix.target }}`, key `fuzz-corpus-${{ matrix.target }}-${{ github.run_id }}`, `restore-keys: fuzz-corpus-${{ matrix.target }}-`. Add a `cargo fuzz cmin` step on schedule runs before the cache saves.
- [ ] **Step 4: Commit.** `test(fuzz): KRPC, extension, MSE and magnet targets; persisted corpus and manual runs`

---

### Task 18: interop: magnet via DHT, forced encryption, PEX

**Files:**
- Modify: `dev/interop/{compose.yaml,run.sh,README.md}`, `dev/interop/qbittorrent/qBittorrent.conf`, `dev/interop/transmission/settings.json`.
- Create: `dev/interop/transmission/dht.bootstrap`.

**Interfaces:**
- Consumes: the `fbt-example` subcommands and flags from Task 16.

Setup changes:

- **Isolated network.** The `swarm` network becomes `internal: true`, so no container can reach the internet and no public DHT node is ever contacted. Images are pulled or built before `up`. The README states this guarantee.
- **Bootstrap node.** A new service `dht-node`, from the `fbt-interop-fbt:local` image, runs `fbt-example dht-node --port 6881`. The clients `depends_on` it.
- **qBittorrent.** `Session\DHTEnabled=true`, `Session\PeXEnabled=true`, and the DHT bootstrap set to `dht-node:6881` only. qBittorrent 5.x has `Session\DHTBootstrapNodes`; check the key against the pinned image (it is shown in its WebUI "Advanced" preferences) and record it in the README.
- **Transmission.** `"dht-enabled": true` and `"pex-enabled": true`. `dht.bootstrap` holds `dht-node 6881` and is copied into `tr-config/` by `run.sh`, as `settings.json` is.
- **Our CLI.** Every `fbt-example` invocation passes `--dht-bootstrap dht-node:6881`. The M1 download scenario also passes `--encryption disabled`, so plaintext keeps its coverage (spec section 7 asks for both forced and disabled).
- **Recorded caveat.** libtorrent by default keeps at most one routing-table node per /24, and every container shares one bridge subnet. The scenario depends only on each client reaching `dht-node`, which stores the announces; the README records this.

New `run.sh` scenarios, after the M1 ones:

1. **Forced encryption.**
   - Switch both clients to encryption required: qBittorrent with `app/setPreferences` `json={"encryption":1}`, Transmission with `transmission-remote --encryption-required`.
   - Record qBittorrent's `uploaded` for fixture 1.
   - `fbt-example download /interop/fixture.torrent --out /interop/fbt-enc --port 6893 --encryption forced` must exit 0 within 300 s with matching hashes. qBittorrent's `uploaded` must have grown, and Transmission's `Uploaded` must have changed.
   - Then run the existing seed check again with fixture 2 into a fresh Transmission download dir, with `--encryption forced` on our seed.
   - Restore both clients to preferred afterwards.
   - Prints `interop: encryption OK (qbittorrent, transmission)`.
2. **Magnet through DHT.**
   - `make_payload` fixture 3, and `transmission-create` *without* `-t` (no tracker).
   - Both clients seed it, then wait 30 s so they announce to `dht-node`.
   - `fbt-example download "magnet:?xt=urn:btih:$hash3&dn=fixture3" --out /interop/fbt-magnet --port 6892 --dht-bootstrap dht-node:6881` must exit 0 within 300 s with matching hashes.
   - The log must contain `found .* peers via dht` and `metadata received: fixture3`.
   - Prints `interop: magnet via DHT OK`.
3. **PEX.**
   - Fixture 4 with no tracker: Transmission seeds it.
   - qBittorrent adds it and is pointed at Transmission with `torrents/addPeers` (`hashes=$hash4&peers=<transmission ip>:51413`, where the IP comes from `getent hosts transmission` in the `fbt` container). Wait until qBittorrent reports progress > 0.
   - `fbt-example download fixture4.torrent --no-dht --peer <qbittorrent ip>:6882 --out /interop/fbt-pex --port 6894` must exit 0 within 300 s with matching hashes.
   - The log must contain `found .* peers via pex` and `peer connected <transmission ip>:`.
   - Prints `interop: PEX OK`.

- [ ] **Step 1: Write the files.** Run `bash dev/interop/run.sh` locally (Docker Desktop on macOS works with the bridge network, as in M1).
- [ ] **Step 2: Verify.** Output ends with the M1 lines plus `interop: encryption OK (qbittorrent, transmission)`, `interop: magnet via DHT OK` and `interop: PEX OK`, and the script exits 0. Run `docker network inspect fbt-interop_swarm --format '{{.Internal}}'` while it runs; it should print `true`.
- [ ] **Step 3: Raise the CI `interop` timeout** to 40 minutes in `.github/workflows/ci.yml`, and add `target/interop/logs/` artifacts for the new logs.
- [ ] **Step 4: Commit.** `ci: interop for magnet via DHT, forced encryption and PEX on an isolated network`

---

### Task 19: docs, changelog and full gate

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `deny.toml` (only if a new license or advisory appears, with its one-line reason).

- [ ] **Step 1: Update the docs.**
  - `README.md`: feature list (DHT, magnets, MSE, PEX), a magnet usage example matching the `fbt` doctest, and the DHT defaults with how to override them.
  - `CHANGELOG.md` `## [Unreleased]`:
    - `### Added` per crate (`fbt-dht` new; wire extension messages and MSE; magnets; session magnet, DHT, PEX, encryption; example `dht-node`; fuzz targets; interop scenarios).
    - `### Changed`: `AddTorrent.metainfo` becomes `source: TorrentSource`, `Limits` gains `max_items`, new `TorrentState` and `Event` variants, `PeerEvent::Connected` gains `encrypted` (internal), and simultaneous dials keep the lower-peer-id initiator's connection.
    - `### Fixed`: duplicate `completed`, and `left` on a recheck `stopped`.
  - `CLAUDE.md`:
    - crate list with `fbt-dht`;
    - the clippy deny list naming `fbt-dht`;
    - the rule "tests never contact public DHT nodes; `test_settings()` disables the DHT";
    - the fuzz targets list;
    - the interop scenarios line.
- [ ] **Step 2: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo deny --all-features check && bash dev/check-pure-rust.sh && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps && bash dev/sast.sh`: all pass.
- [ ] **Step 3: Commit.** `docs: milestone 2 changelog, README and CLAUDE.md`

---

## After the plan: shipping v0.2.0

PR `feature/m2-must-haves` → `develop`. Every CI job must be green, including `interop` and the fuzz runs the PR triggers. The PR body lists the decisions made on the owner's behalf (the "Decision" list of this plan's review) and a "Deferred to later milestones" section. Then follow the Gitflow release:

1. `release/0.2.0` off `develop`: bump the workspace version to `0.2.0` (the peer-id prefix becomes `-FB0200-` and the KRPC `v` becomes `FB\x00\x02` automatically). Move `Unreleased` to `## [0.2.0] - <date>`, and check `bash dev/test-release-version.sh`.
2. PR into `main`, then the annotated tag `v0.2.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 3:**
  - DHT, uTP and UDP trackers on one shared UDP socket (the DHT has its own sockets until then);
  - uTP and the PEX `UTP` flag;
  - BEP 55 holepunch (the PEX `HOLEPUNCH` flag is parsed, never acted on);
  - LSD;
  - multiple listen addresses (IPv6 TCP listener);
  - `yourip` and DHT external-IP votes feeding NAT detection;
  - MSE-aware rate accounting.
- **Milestone 5:** BEP 44 put/get, BEP 33 scrape and BEP 51 `sample_infohashes`; saving DHT state inside session state and resume data (until then clients persist `Session::dht_state()` themselves); smart ban beyond the per-torrent metadata ban.
- **Milestone 6:** v2-only magnets (`btmh` without `btih`) and v2 metadata verification beyond the SHA-256 identity check.
- **Not in any milestone of the spec's list, proposed for milestone 3:** BEP 21 upload-only and BEP 54 `lt_donthave` (spec section 4 lists them under core protocol).
