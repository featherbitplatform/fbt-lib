//! Portable sanitization of untrusted torrent path components.
//!
//! The rules are identical on every OS so a torrent saves to the same
//! relative layout everywhere, and no component can ever escape the save path.

use std::fmt;
use std::path::PathBuf;

/// Longest component, in bytes, that is kept as is.
const MAX_COMPONENT_BYTES: usize = 255;
/// Longest extension (including the dot) preserved when truncating.
const MAX_EXT_BYTES: usize = 16;

/// A sanitized relative path: one or more safe components.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelPath(Vec<String>);

impl RelPath {
    /// The path components, never empty.
    pub fn components(&self) -> &[String] {
        &self.0
    }

    /// A single-component path from an already-sanitized name.
    pub(crate) fn single(name: String) -> Self {
        Self(vec![name])
    }

    /// Prepends an already-sanitized root component.
    pub(crate) fn with_root(mut self, root: String) -> Self {
        self.0.insert(0, root);
        self
    }

    /// The path as a platform `PathBuf` (relative).
    pub fn to_path_buf(&self) -> PathBuf {
        self.0.iter().collect()
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, c) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            f.write_str(c)?;
        }
        Ok(())
    }
}

/// Errors from [`sanitize_path`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// Nothing usable is left after sanitization.
    #[error("path is empty after sanitization")]
    Empty,
}

fn is_reserved_stem(stem: &str) -> bool {
    let s = stem.trim_end_matches([' ', '.']).to_ascii_uppercase();
    matches!(s.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((s.starts_with("COM") || s.starts_with("LPT"))
            && s.len() == 4
            && s.as_bytes()
                .last()
                .is_some_and(|c| (b'1'..=b'9').contains(c)))
}

fn truncate_at_boundary(s: &str, max: usize) -> &str {
    let mut end = max.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.get(..end).unwrap_or("")
}

/// Cleans one already-split piece. `None` when nothing usable remains.
fn clean_piece(piece: &str) -> Option<String> {
    if piece.is_empty() || piece == "." || piece == ".." {
        return None;
    }
    let replaced: String = piece
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let mut s = replaced.trim_end_matches(['.', ' ']).to_string();
    if s.is_empty() {
        return None;
    }
    // Reserved device names: compare the stem before the first dot.
    let stem_end = s.find('.').unwrap_or(s.len());
    if s.get(..stem_end).is_some_and(is_reserved_stem) {
        s.insert(stem_end, '_');
    }
    if s.len() > MAX_COMPONENT_BYTES {
        s = truncate_component(&s);
    }
    if s.is_empty() { None } else { Some(s) }
}

fn truncate_component(s: &str) -> String {
    let ext_start = s.rfind('.').filter(|&i| i > 0);
    let (stem, ext) = match ext_start.and_then(|i| s.get(i..).map(|e| (i, e))) {
        Some((i, e)) if e.len() <= MAX_EXT_BYTES => (s.get(..i).unwrap_or(""), e),
        _ => (s, ""),
    };
    let stem = truncate_at_boundary(stem, MAX_COMPONENT_BYTES - ext.len());
    let stem = stem.trim_end_matches(['.', ' ']);
    format!("{stem}{ext}")
}

/// Splits `raw` (lossy UTF-8) on `/` and `\`, dropping a leading drive prefix
/// from the first piece when `first` is set.
fn pieces(raw: &[u8], first: bool) -> Vec<String> {
    let text = String::from_utf8_lossy(raw);
    let mut out = Vec::new();
    for (i, piece) in text.split(['/', '\\']).enumerate() {
        let mut piece = piece;
        if first && i == 0 {
            let b = piece.as_bytes();
            if b.len() >= 2
                && b.first().is_some_and(u8::is_ascii_alphabetic)
                && b.get(1) == Some(&b':')
            {
                piece = piece.get(2..).unwrap_or("");
            }
        }
        if let Some(c) = clean_piece(piece) {
            out.push(c);
        }
    }
    out
}

/// Sanitizes a single name (such as a torrent `name`) into one safe component.
///
/// Separators inside `raw` split it; surviving pieces are joined with `_`.
/// `None` when nothing usable is left.
pub fn sanitize_component(raw: &[u8]) -> Option<String> {
    let parts = pieces(raw, true);
    if parts.is_empty() {
        return None;
    }
    let joined = parts.join("_");
    if joined.len() > MAX_COMPONENT_BYTES {
        let t = truncate_component(&joined);
        return if t.is_empty() { None } else { Some(t) };
    }
    Some(joined)
}

/// Sanitizes a list of raw path components into a safe relative path.
pub fn sanitize_path<'a>(raw: impl IntoIterator<Item = &'a [u8]>) -> Result<RelPath, PathError> {
    let mut out = Vec::new();
    for (i, r) in raw.into_iter().enumerate() {
        out.extend(pieces(r, i == 0));
    }
    if out.is_empty() {
        Err(PathError::Empty)
    } else {
        Ok(RelPath(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn p(parts: &[&str]) -> String {
        sanitize_path(parts.iter().map(|s| s.as_bytes()))
            .unwrap()
            .to_string()
    }

    #[test]
    fn traversal_and_roots_dropped() {
        assert_eq!(p(&["..", "etc", "passwd"]), "etc/passwd");
        assert_eq!(p(&["/abs", "x"]), "abs/x");
        assert_eq!(p(&["a/../../b"]), "a/b");
        assert_eq!(p(&["C:\\Windows", "x"]), "Windows/x");
    }

    #[test]
    fn reserved_and_illegal() {
        assert_eq!(p(&["con.txt"]), "con_.txt");
        assert_eq!(p(&["LPT9"]), "LPT9_");
        assert_eq!(p(&["a<b>:c?.d "]), "a_b__c_.d");
        assert_eq!(p(&["x\u{0}y"]), "x_y");
    }

    #[test]
    fn long_component_keeps_extension() {
        let s = format!("{}.mkv", "é".repeat(200));
        let out = p(&[&s]);
        assert!(out.len() <= 255 && out.ends_with(".mkv"));
    }

    #[test]
    fn nothing_left_is_error() {
        assert!(matches!(
            sanitize_path([&b".."[..], b"."]),
            Err(PathError::Empty)
        ));
    }

    #[test]
    fn component_and_lossy() {
        assert_eq!(sanitize_component(b"../x").as_deref(), Some("x"));
        assert_eq!(sanitize_component(b"..."), None);
        assert_eq!(p(&["a\u{FFFD}"]), "a\u{FFFD}");
        assert_eq!(
            sanitize_path([&b"a\xff"[..]]).unwrap().to_string(),
            "a\u{FFFD}"
        );
    }

    proptest! {
        #[test]
        fn never_escapes(parts in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 0..6)) {
            if let Ok(path) = sanitize_path(parts.iter().map(Vec::as_slice)) {
                prop_assert!(!path.components().is_empty());
                for c in path.components() {
                    prop_assert!(!c.is_empty());
                    prop_assert!(c != "." && c != "..");
                    prop_assert!(c.len() <= 255);
                    prop_assert!(!c.contains(['/', '\\', ':', '\0']));
                }
            }
        }
    }
}
