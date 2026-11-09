//! Canonical bencode encoding and owned values.

use std::collections::BTreeMap;

use crate::value::{Value, ValueKind};

/// An owned bencode value; dictionaries are key-sorted, so encoding is canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedValue {
    /// An integer.
    Int(i64),
    /// A byte string.
    Bytes(Vec<u8>),
    /// A list.
    List(Vec<OwnedValue>),
    /// A dictionary keyed by raw bytes.
    Dict(BTreeMap<Vec<u8>, OwnedValue>),
}

impl From<i64> for OwnedValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<&[u8]> for OwnedValue {
    fn from(v: &[u8]) -> Self {
        Self::Bytes(v.to_vec())
    }
}
impl From<Vec<u8>> for OwnedValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}
impl From<&str> for OwnedValue {
    fn from(v: &str) -> Self {
        Self::Bytes(v.as_bytes().to_vec())
    }
}
impl From<String> for OwnedValue {
    fn from(v: String) -> Self {
        Self::Bytes(v.into_bytes())
    }
}

impl Value<'_> {
    /// Copies this value into an [`OwnedValue`] (dictionary order becomes sorted).
    pub fn to_owned_value(&self) -> OwnedValue {
        match self.kind() {
            ValueKind::Int(i) => OwnedValue::Int(*i),
            ValueKind::Bytes(b) => OwnedValue::Bytes(b.to_vec()),
            ValueKind::List(l) => OwnedValue::List(l.iter().map(Value::to_owned_value).collect()),
            ValueKind::Dict(d) => OwnedValue::Dict(
                d.iter()
                    .map(|(k, v)| (k.to_vec(), v.to_owned_value()))
                    .collect(),
            ),
        }
    }
}

/// Encodes a value canonically.
pub fn encode(v: &OwnedValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(v, &mut out);
    out
}

fn encode_bytes(b: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(b.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(b);
}

/// Appends the canonical encoding of `v` to `out`.
pub fn encode_into(v: &OwnedValue, out: &mut Vec<u8>) {
    match v {
        OwnedValue::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        OwnedValue::Bytes(b) => encode_bytes(b, out),
        OwnedValue::List(l) => {
            out.push(b'l');
            for item in l {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        OwnedValue::Dict(d) => {
            out.push(b'd');
            for (k, item) in d {
                encode_bytes(k, out);
                encode_into(item, out);
            }
            out.push(b'e');
        }
    }
}

/// Builds a dictionary with sorted keys.
#[derive(Debug, Default)]
pub struct DictBuilder {
    map: BTreeMap<Vec<u8>, OwnedValue>,
}

impl DictBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts (or replaces) a key.
    pub fn insert(&mut self, key: &[u8], v: impl Into<OwnedValue>) -> &mut Self {
        self.map.insert(key.to_vec(), v.into());
        self
    }

    /// Finishes into an [`OwnedValue::Dict`].
    pub fn build(&self) -> OwnedValue {
        OwnedValue::Dict(self.map.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;
    use proptest::prelude::*;

    fn arb_value() -> impl Strategy<Value = OwnedValue> {
        let leaf = prop_oneof![
            any::<i64>().prop_map(OwnedValue::Int),
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(OwnedValue::Bytes),
        ];
        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8).prop_map(OwnedValue::List),
                proptest::collection::btree_map(
                    proptest::collection::vec(any::<u8>(), 0..32),
                    inner,
                    0..8
                )
                .prop_map(OwnedValue::Dict),
            ]
        })
    }

    #[test]
    fn encodes_canonical() {
        let mut b = DictBuilder::new();
        b.insert(b"b", 1i64).insert(b"a", 2i64);
        assert_eq!(encode(&b.build()), b"d1:ai2e1:bi1ee");
    }

    proptest! {
        #[test]
        fn roundtrip(v in arb_value()) {
            let bytes = encode(&v);
            prop_assert_eq!(decode(&bytes).unwrap().to_owned_value(), v);
        }

        #[test]
        fn canonical_reencode(v in arb_value()) {
            let bytes = encode(&v);
            let again = encode(&decode(&bytes).unwrap().to_owned_value());
            prop_assert_eq!(again, bytes);
        }
    }
}
