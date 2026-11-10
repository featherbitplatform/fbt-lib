//! Zero-copy bencode values.

/// The shape of a decoded value. Obtained from [`Value::kind`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueKind<'a> {
    /// An integer.
    Int(i64),
    /// A byte string.
    Bytes(&'a [u8]),
    /// A list.
    List(Vec<Value<'a>>),
    /// A dictionary.
    Dict(Dict<'a>),
}

/// A decoded bencode value borrowing from the input, remembering its exact
/// input span (see [`Value::raw`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) kind: ValueKind<'a>,
}

impl<'a> Value<'a> {
    /// The value's shape.
    pub fn kind(&self) -> &ValueKind<'a> {
        &self.kind
    }

    /// The exact input bytes this value was decoded from.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// The integer, if this is one.
    pub fn as_int(&self) -> Option<i64> {
        match self.kind {
            ValueKind::Int(i) => Some(i),
            _ => None,
        }
    }

    /// The byte string, if this is one.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self.kind {
            ValueKind::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// The byte string as UTF-8, if this is one and it is valid.
    pub fn as_str(&self) -> Option<&'a str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// The list items, if this is a list.
    pub fn as_list(&self) -> Option<&[Value<'a>]> {
        match &self.kind {
            ValueKind::List(l) => Some(l),
            _ => None,
        }
    }

    /// The dictionary, if this is one.
    pub fn as_dict(&self) -> Option<&Dict<'a>> {
        match &self.kind {
            ValueKind::Dict(d) => Some(d),
            _ => None,
        }
    }
}

/// A decoded dictionary; entries stay in input order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dict<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) entries: Vec<(&'a [u8], Value<'a>)>,
    /// True when keys arrived strictly ascending (enables binary search).
    pub(crate) sorted: bool,
}

impl<'a> Dict<'a> {
    /// Looks up a key.
    pub fn get(&self, key: &[u8]) -> Option<&Value<'a>> {
        if self.sorted {
            let i = self.entries.binary_search_by(|(k, _)| (*k).cmp(key)).ok()?;
            self.entries.get(i).map(|(_, v)| v)
        } else {
            self.entries.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
        }
    }

    /// Entries in input order.
    pub fn iter(&self) -> impl Iterator<Item = (&'a [u8], &Value<'a>)> + '_ {
        self.entries.iter().map(|(k, v)| (*k, v))
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when there are no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The exact input bytes of this dictionary.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }
}
