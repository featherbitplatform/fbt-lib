//! The BEP 10 extension layer of a peer connection: what we advertise in our
//! extension handshake, the message ids the remote assigned, and decoding of
//! incoming extended messages. `conn.rs` only routes; nothing here does I/O.
//!
//! Everything decoded here is hostile input. The wire crate bounds sizes and
//! list lengths; the remote's `reqq` is clamped here, and its
//! `metadata_size` stays an untrusted hint (the metadata fetch caps it).

use std::net::IpAddr;

use bytes::Bytes;
use fbt_wire::{
    EXT_HANDSHAKE_ID, ExtError, ExtHandshake, HolepunchMsg, LT_DONTHAVE, Message, MetadataMsg,
    OUR_LT_DONTHAVE_ID, OUR_UPLOAD_ONLY_ID, OUR_UT_HOLEPUNCH_ID, OUR_UT_METADATA_ID, OUR_UT_PEX_ID,
    PexMsg, UPLOAD_ONLY, UT_HOLEPUNCH, UT_METADATA, UT_PEX, decode_donthave, decode_upload_only,
    encode_donthave, encode_upload_only,
};

use crate::settings::MAX_INCOMING_REQUESTS;

/// Largest `reqq` kept from a remote extension handshake; anything that
/// sizes a queue from it stays within the bound of our own request queue.
pub(crate) const MAX_REMOTE_REQQ: u32 = MAX_INCOMING_REQUESTS as u32;

/// What a peer connection advertises in its handshakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExtConfig {
    /// Our peer listener port (`p`).
    pub listen_port: u16,
    /// Size of the info dictionary, once known (`metadata_size`).
    pub metadata_size: Option<u32>,
    /// Advertise `ut_pex`.
    pub pex: bool,
    /// Our DHT port: sets the DHT reserved bit and sends `Port`.
    pub dht_port: Option<u16>,
    /// Requests we queue from one peer (`reqq`).
    pub reqq: u32,
    /// We are finished (BEP 21): the handshake carries `upload_only: 1`.
    pub upload_only: bool,
    /// Advertise `ut_holepunch` (BEP 55).
    pub holepunch: bool,
}

/// Our extension handshake to a remote at `remote_ip`.
pub(crate) fn our_handshake(cfg: &ExtConfig, remote_ip: IpAddr) -> ExtHandshake {
    let mut hs = ExtHandshake::default();
    hs.m.insert(UT_METADATA.to_owned(), OUR_UT_METADATA_ID);
    if cfg.pex {
        hs.m.insert(UT_PEX.to_owned(), OUR_UT_PEX_ID);
    }
    // Both are always accepted: `upload_only` can flip at any time and
    // `lt_donthave` is harmless when unused.
    hs.m.insert(UPLOAD_ONLY.to_owned(), OUR_UPLOAD_ONLY_ID);
    hs.m.insert(LT_DONTHAVE.to_owned(), OUR_LT_DONTHAVE_ID);
    if cfg.holepunch {
        hs.m.insert(UT_HOLEPUNCH.to_owned(), OUR_UT_HOLEPUNCH_ID);
    }
    hs.upload_only = cfg.upload_only.then_some(true);
    hs.client = Some(concat!("fbt/", env!("CARGO_PKG_VERSION")).to_owned());
    hs.listen_port = Some(cfg.listen_port).filter(|p| *p != 0);
    hs.reqq = Some(cfg.reqq);
    hs.metadata_size = cfg.metadata_size;
    hs.your_ip = Some(remote_ip);
    hs
}

/// Our handshake as a wire message.
pub(crate) fn handshake_message(cfg: &ExtConfig, remote_ip: IpAddr) -> Message {
    Message::Extended {
        id: EXT_HANDSHAKE_ID,
        payload: our_handshake(cfg, remote_ip).encode(),
    }
}

/// A repeated handshake after `cfg` changed (the metadata arrived): like
/// [`handshake_message`], and an extension we no longer speak is turned off
/// explicitly with id 0, since a repeat that omits it changes nothing.
pub(crate) fn repeat_handshake_message(cfg: &ExtConfig, remote_ip: IpAddr) -> Message {
    let mut hs = our_handshake(cfg, remote_ip);
    if !cfg.pex {
        hs.m.insert(UT_PEX.to_owned(), 0);
    }
    if !cfg.holepunch {
        hs.m.insert(UT_HOLEPUNCH.to_owned(), 0);
    }
    Message::Extended {
        id: EXT_HANDSHAKE_ID,
        payload: hs.encode(),
    }
}

/// The ids the remote assigned to the extensions we speak; `None` while it
/// has not enabled one (or has not sent its handshake yet).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RemoteIds {
    ut_metadata: Option<u8>,
    ut_pex: Option<u8>,
    upload_only: Option<u8>,
    lt_donthave: Option<u8>,
    ut_holepunch: Option<u8>,
}

impl RemoteIds {
    /// Takes the ids of a (possibly repeated) remote handshake. An absent
    /// or zero id disables that extension.
    pub(crate) fn update(&mut self, hs: &ExtHandshake) {
        self.ut_metadata = hs.id_of(UT_METADATA);
        self.ut_pex = hs.id_of(UT_PEX);
        self.upload_only = hs.id_of(UPLOAD_ONLY);
        self.lt_donthave = hs.id_of(LT_DONTHAVE);
        self.ut_holepunch = hs.id_of(UT_HOLEPUNCH);
    }

    /// A BEP 21 `upload_only` message, if the remote speaks it.
    pub(crate) fn upload_only(&self, on: bool) -> Option<Message> {
        self.upload_only.map(|id| Message::Extended {
            id,
            payload: encode_upload_only(on),
        })
    }

    /// A BEP 54 `lt_donthave` message, if the remote speaks it.
    pub(crate) fn donthave(&self, piece: u32) -> Option<Message> {
        self.lt_donthave.map(|id| Message::Extended {
            id,
            payload: encode_donthave(piece),
        })
    }

    /// `msg` addressed with the remote's `ut_metadata` id.
    pub(crate) fn metadata(&self, msg: &MetadataMsg) -> Option<Message> {
        self.ut_metadata.map(|id| Message::Extended {
            id,
            payload: msg.encode(),
        })
    }

    /// `msg` addressed with the remote's `ut_holepunch` id.
    pub(crate) fn holepunch(&self, msg: &HolepunchMsg) -> Option<Message> {
        self.ut_holepunch.map(|id| Message::Extended {
            id,
            payload: msg.encode(),
        })
    }

    /// `msg` addressed with the remote's `ut_pex` id.
    pub(crate) fn pex(&self, msg: &PexMsg) -> Option<Message> {
        self.ut_pex.map(|id| Message::Extended {
            id,
            payload: msg.encode(),
        })
    }
}

/// An incoming extended message, by the id we assigned.
#[derive(Debug)]
pub(crate) enum Incoming {
    Handshake(Box<ExtHandshake>),
    Metadata(MetadataMsg),
    Pex(PexMsg),
    /// A BEP 21 `upload_only` message.
    UploadOnly(bool),
    /// A BEP 54 `lt_donthave` message (index not range-checked here).
    DontHave(u32),
    /// A BEP 55 `ut_holepunch` message (addresses not checked here).
    Holepunch(HolepunchMsg),
    /// An id we never assigned: ignored.
    Unknown,
}

/// Decodes the extended message `id` (one of ours) with `payload`. With
/// `pex` (or `holepunch`) off we did not advertise `ut_pex` (or
/// `ut_holepunch`): its id is one we never assigned, and the message is
/// [`Incoming::Unknown`], not even decoded.
pub(crate) fn decode(
    id: u8,
    payload: &Bytes,
    pex: bool,
    holepunch: bool,
) -> Result<Incoming, ExtError> {
    Ok(match id {
        EXT_HANDSHAKE_ID => {
            let mut hs = ExtHandshake::decode(payload)?;
            hs.reqq = hs.reqq.map(|r| r.min(MAX_REMOTE_REQQ));
            Incoming::Handshake(Box::new(hs))
        }
        OUR_UT_METADATA_ID => Incoming::Metadata(MetadataMsg::decode(payload)?),
        OUR_UT_PEX_ID if pex => Incoming::Pex(PexMsg::decode(payload)?),
        OUR_UPLOAD_ONLY_ID => Incoming::UploadOnly(decode_upload_only(payload)?),
        OUR_LT_DONTHAVE_ID => Incoming::DontHave(decode_donthave(payload)?),
        OUR_UT_HOLEPUNCH_ID if holepunch => Incoming::Holepunch(HolepunchMsg::decode(payload)?),
        _ => Incoming::Unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ExtConfig {
        ExtConfig {
            listen_port: 6881,
            metadata_size: Some(1234),
            pex: true,
            dht_port: None,
            reqq: 250,
            upload_only: false,
            holepunch: false,
        }
    }

    #[test]
    fn our_handshake_carries_config() {
        let ip: IpAddr = "10.1.2.3".parse().expect("ip");
        let hs = our_handshake(&cfg(), ip);
        assert_eq!(hs.id_of(UT_METADATA), Some(OUR_UT_METADATA_ID));
        assert_eq!(hs.id_of(UT_PEX), Some(OUR_UT_PEX_ID));
        assert_eq!(hs.listen_port, Some(6881));
        assert_eq!(hs.reqq, Some(250));
        assert_eq!(hs.metadata_size, Some(1234));
        assert_eq!(hs.your_ip, Some(ip));
        assert!(hs.client.as_deref().is_some_and(|v| v.starts_with("fbt/")));
    }

    #[test]
    fn upload_only_and_donthave_are_advertised() {
        let ip: IpAddr = "10.1.2.3".parse().expect("ip");
        let hs = our_handshake(&cfg(), ip);
        assert_eq!(hs.id_of(UPLOAD_ONLY), Some(OUR_UPLOAD_ONLY_ID));
        assert_eq!(hs.id_of(LT_DONTHAVE), Some(OUR_LT_DONTHAVE_ID));
        assert_eq!(hs.upload_only, None);
        assert_eq!(hs.id_of(UT_HOLEPUNCH), None);
        let on = ExtConfig {
            upload_only: true,
            holepunch: true,
            ..cfg()
        };
        let hs = our_handshake(&on, ip);
        assert_eq!(hs.upload_only, Some(true));
        assert_eq!(hs.id_of(UT_HOLEPUNCH), Some(OUR_UT_HOLEPUNCH_ID));
    }

    #[test]
    fn remote_reqq_is_clamped() {
        let hs = ExtHandshake {
            reqq: Some(u32::MAX),
            ..ExtHandshake::default()
        };
        let Ok(Incoming::Handshake(hs)) = decode(EXT_HANDSHAKE_ID, &hs.encode(), true, false)
        else {
            panic!("decode");
        };
        assert_eq!(hs.reqq, Some(MAX_REMOTE_REQQ));
    }

    #[test]
    fn pex_id_is_unknown_unless_advertised() {
        let junk = Bytes::from_static(b"not bencode");
        assert!(matches!(
            decode(OUR_UT_PEX_ID, &junk, false, false),
            Ok(Incoming::Unknown)
        ));
        assert!(decode(OUR_UT_PEX_ID, &junk, true, false).is_err());
        let msg = PexMsg::default().encode();
        assert!(matches!(
            decode(OUR_UT_PEX_ID, &msg, true, false),
            Ok(Incoming::Pex(_))
        ));
    }

    #[test]
    fn holepunch_id_is_unknown_unless_advertised() {
        let addr = "10.0.0.1:6881".parse().expect("addr");
        let msg = HolepunchMsg::Rendezvous(addr).encode();
        assert!(matches!(
            decode(OUR_UT_HOLEPUNCH_ID, &msg, true, false),
            Ok(Incoming::Unknown)
        ));
        assert!(matches!(
            decode(OUR_UT_HOLEPUNCH_ID, &msg, true, true),
            Ok(Incoming::Holepunch(HolepunchMsg::Rendezvous(a))) if a == addr
        ));
        // Repeated without holepunch: turned off explicitly.
        let ip: IpAddr = "10.1.2.3".parse().expect("ip");
        let Message::Extended { payload, .. } = repeat_handshake_message(&cfg(), ip) else {
            panic!("extended");
        };
        let hs = ExtHandshake::decode(&payload).expect("handshake");
        assert_eq!(hs.m.get(UT_HOLEPUNCH), Some(&0));
        // The remote's id addresses our messages.
        let mut theirs = ExtHandshake::default();
        theirs.m.insert(UT_HOLEPUNCH.to_owned(), 9);
        let mut ids = RemoteIds::default();
        assert!(ids.holepunch(&HolepunchMsg::Connect(addr)).is_none());
        ids.update(&theirs);
        assert!(matches!(
            ids.holepunch(&HolepunchMsg::Connect(addr)),
            Some(Message::Extended { id: 9, .. })
        ));
    }

    #[test]
    fn zero_id_disables_an_extension() {
        let mut hs = ExtHandshake::default();
        hs.m.insert(UT_METADATA.to_owned(), 3);
        hs.m.insert(UT_PEX.to_owned(), 4);
        let mut ids = RemoteIds::default();
        ids.update(&hs);
        assert!(ids.pex(&PexMsg::default()).is_some());
        hs.m.insert(UT_PEX.to_owned(), 0);
        ids.update(&hs);
        assert!(ids.pex(&PexMsg::default()).is_none());
        assert!(matches!(
            ids.metadata(&MetadataMsg::Request { piece: 0 }),
            Some(Message::Extended { id: 3, .. })
        ));
    }
}
