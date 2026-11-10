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
    EXT_HANDSHAKE_ID, ExtError, ExtHandshake, Message, MetadataMsg, OUR_UT_METADATA_ID,
    OUR_UT_PEX_ID, PexMsg, UT_METADATA, UT_PEX,
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
}

/// Our extension handshake to a remote at `remote_ip`.
pub(crate) fn our_handshake(cfg: &ExtConfig, remote_ip: IpAddr) -> ExtHandshake {
    let mut hs = ExtHandshake::default();
    hs.m.insert(UT_METADATA.to_owned(), OUR_UT_METADATA_ID);
    if cfg.pex {
        hs.m.insert(UT_PEX.to_owned(), OUR_UT_PEX_ID);
    }
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
}

impl RemoteIds {
    /// Takes the ids of a (possibly repeated) remote handshake. An absent
    /// or zero id disables that extension.
    pub(crate) fn update(&mut self, hs: &ExtHandshake) {
        self.ut_metadata = hs.id_of(UT_METADATA);
        self.ut_pex = hs.id_of(UT_PEX);
    }

    /// `msg` addressed with the remote's `ut_metadata` id.
    pub(crate) fn metadata(&self, msg: &MetadataMsg) -> Option<Message> {
        self.ut_metadata.map(|id| Message::Extended {
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
    /// An id we never assigned: ignored.
    Unknown,
}

/// Decodes the extended message `id` (one of ours) with `payload`.
pub(crate) fn decode(id: u8, payload: &Bytes) -> Result<Incoming, ExtError> {
    Ok(match id {
        EXT_HANDSHAKE_ID => {
            let mut hs = ExtHandshake::decode(payload)?;
            hs.reqq = hs.reqq.map(|r| r.min(MAX_REMOTE_REQQ));
            Incoming::Handshake(Box::new(hs))
        }
        OUR_UT_METADATA_ID => Incoming::Metadata(MetadataMsg::decode(payload)?),
        OUR_UT_PEX_ID => Incoming::Pex(PexMsg::decode(payload)?),
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
    fn remote_reqq_is_clamped() {
        let hs = ExtHandshake {
            reqq: Some(u32::MAX),
            ..ExtHandshake::default()
        };
        let Ok(Incoming::Handshake(hs)) = decode(EXT_HANDSHAKE_ID, &hs.encode()) else {
            panic!("decode");
        };
        assert_eq!(hs.reqq, Some(MAX_REMOTE_REQQ));
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
