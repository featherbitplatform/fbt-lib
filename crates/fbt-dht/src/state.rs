//! The persisted node state: our ids and the addresses of known nodes.
//!
//! A bencoded dict `{version: 1, id, id6, nodes, nodes6}`; node lists use the
//! shared compact codec ([`fbt_core::compact`]). The file is untrusted on
//! load: it decodes with tight limits and every list is capped.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use fbt_bencode::{Dict, Limits, OwnedValue, Value, decode_with, encode};
use fbt_core::compact::{self, decode_v4_list, decode_v6_list};

use crate::id::{NODE_ID_LEN, NodeId};
use crate::node::DhtError;

/// Most nodes per address family saved or loaded.
pub const MAX_STATE_NODES: usize = 1000;
/// The format version written and accepted.
const STATE_VERSION: i64 = 1;
/// A flat dict of five keys: depth 2 leaves room for nothing nested deeper.
const STATE_LIMITS: Limits = Limits::new(2, 16);

/// A DHT node's persistent state, for a fast rejoin after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtState {
    /// The IPv4 node id.
    pub id_v4: Option<NodeId>,
    /// The IPv6 node id.
    pub id_v6: Option<NodeId>,
    /// Known IPv4 nodes.
    pub nodes_v4: Vec<SocketAddr>,
    /// Known IPv6 nodes.
    pub nodes_v6: Vec<SocketAddr>,
}

fn compact_list(addrs: &[SocketAddr], v6: bool) -> OwnedValue {
    let mut out = Vec::new();
    for a in addrs
        .iter()
        .filter(|a| a.is_ipv6() == v6)
        .take(MAX_STATE_NODES)
    {
        compact::encode(*a, &mut out);
    }
    OwnedValue::Bytes(out)
}

fn id(d: &Dict<'_>, key: &'static str) -> Result<Option<NodeId>, DhtError> {
    match d.get(key.as_bytes()) {
        None => Ok(None),
        Some(v) => {
            let b: [u8; NODE_ID_LEN] = v
                .as_bytes()
                .and_then(|b| b.try_into().ok())
                .ok_or(DhtError::InvalidState(key))?;
            Ok(Some(NodeId(b)))
        }
    }
}

fn nodes(
    d: &Dict<'_>,
    key: &'static str,
    decode: fn(&[u8], usize) -> Result<Vec<SocketAddr>, compact::CompactError>,
) -> Result<Vec<SocketAddr>, DhtError> {
    match d.get(key.as_bytes()) {
        None => Ok(Vec::new()),
        Some(v) => {
            let b = v.as_bytes().ok_or(DhtError::InvalidState(key))?;
            decode(b, MAX_STATE_NODES).map_err(|_| DhtError::InvalidState(key))
        }
    }
}

impl DhtState {
    /// Encodes the state; at most [`MAX_STATE_NODES`] nodes per family, and
    /// entries of the wrong family are left out.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"version".to_vec(), OwnedValue::Int(STATE_VERSION));
        if let Some(id) = self.id_v4 {
            d.insert(b"id".to_vec(), OwnedValue::Bytes(id.0.to_vec()));
        }
        if let Some(id) = self.id_v6 {
            d.insert(b"id6".to_vec(), OwnedValue::Bytes(id.0.to_vec()));
        }
        d.insert(b"nodes".to_vec(), compact_list(&self.nodes_v4, false));
        d.insert(b"nodes6".to_vec(), compact_list(&self.nodes_v6, true));
        encode(&OwnedValue::Dict(d))
    }

    /// Decodes a state written by [`to_bytes`](Self::to_bytes). A missing or
    /// different version, a wrong id length or a misaligned node list gives
    /// [`DhtError::InvalidState`]; node lists are cut to
    /// [`MAX_STATE_NODES`].
    pub fn from_bytes(b: &[u8]) -> Result<DhtState, DhtError> {
        let value = decode_with(b, STATE_LIMITS).map_err(|_| DhtError::InvalidState("bencode"))?;
        let d = value
            .as_dict()
            .ok_or(DhtError::InvalidState("not a dict"))?;
        if d.get(b"version").and_then(Value::as_int) != Some(STATE_VERSION) {
            return Err(DhtError::InvalidState("version"));
        }
        Ok(DhtState {
            id_v4: id(d, "id")?,
            id_v6: id(d, "id6")?,
            nodes_v4: nodes(d, "nodes", decode_v4_list)?,
            nodes_v6: nodes(d, "nodes6", decode_v6_list)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DhtState {
        DhtState {
            id_v4: Some(NodeId([1; 20])),
            id_v6: Some(NodeId([2; 20])),
            nodes_v4: vec![
                "1.2.3.4:6881".parse().unwrap(),
                "5.6.7.8:1".parse().unwrap(),
            ],
            nodes_v6: vec!["[2001:db8::1]:6881".parse().unwrap()],
        }
    }

    #[test]
    fn state_roundtrip() {
        let s = sample();
        assert_eq!(DhtState::from_bytes(&s.to_bytes()).unwrap(), s);
        let empty = DhtState {
            id_v4: None,
            id_v6: None,
            nodes_v4: vec![],
            nodes_v6: vec![],
        };
        assert_eq!(DhtState::from_bytes(&empty.to_bytes()).unwrap(), empty);
        // wrong-family entries are not written
        let mut mixed = sample();
        mixed.nodes_v4.push("[2001:db8::9]:1".parse().unwrap());
        assert_eq!(DhtState::from_bytes(&mixed.to_bytes()).unwrap(), sample());
    }

    #[test]
    fn rejects_future_version_and_garbage() {
        let bad: [&[u8]; 6] = [
            b"d7:versioni2ee",
            b"garbage",
            b"d5:nodes25:aaaaaaaaaaaaaaaaaaaaaaaaa7:versioni1ee",
            b"de",
            b"d2:id3:abc7:versioni1ee",
            b"d6:nodes65:aaaaa7:versioni1ee",
        ];
        for b in bad {
            assert!(
                matches!(DhtState::from_bytes(b), Err(DhtError::InvalidState(_))),
                "{}",
                String::from_utf8_lossy(b)
            );
        }
        // too deep or too many items for Limits::new(2, 16)
        assert!(DhtState::from_bytes(b"d7:versioni1e1:xllleeee").is_err());
    }

    #[test]
    fn caps_nodes_on_load() {
        let many: Vec<SocketAddr> = (0..MAX_STATE_NODES as u32 + 50)
            .map(|k| SocketAddr::from((u32::to_be_bytes(0x0100_0000 + k), 6881)))
            .collect();
        let s = DhtState {
            id_v4: None,
            id_v6: None,
            nodes_v4: many.clone(),
            nodes_v6: vec![],
        };
        // capped on save
        let saved = DhtState::from_bytes(&s.to_bytes()).unwrap();
        assert_eq!(saved.nodes_v4, many[..MAX_STATE_NODES]);
        // and on load, for a file written by someone else
        let mut raw = Vec::new();
        for a in &many {
            fbt_core::compact::encode(*a, &mut raw);
        }
        let mut file = format!("d5:nodes{}:", raw.len()).into_bytes();
        file.extend_from_slice(&raw);
        file.extend_from_slice(b"7:versioni1ee");
        assert_eq!(
            DhtState::from_bytes(&file).unwrap().nodes_v4.len(),
            MAX_STATE_NODES
        );
    }
}
