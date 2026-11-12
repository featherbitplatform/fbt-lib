//! Renaming of colliding file paths at parse time.
//!
//! Two paths collide when, on a case-insensitive or normalising filesystem,
//! they would name the same file, or one would be a file and the other a
//! directory of the same name. Their keys (see [`RelPath::key`]) are compared:
//! each component NFC-normalised and lower-cased. The first file in torrent
//! order keeps its name; a later one is renamed to `stem.N.ext` (libtorrent's
//! `resolve_duplicate_filenames`), `N` the smallest free number from 1. A
//! renamed directory carries every later file under it. Padding files are never
//! written to disk and are exempt. The result depends only on the torrent, so
//! resume data stays valid.

use std::collections::{HashMap, HashSet};

use crate::metainfo::FileEntry;
use crate::path::{
    MAX_COMPONENT_BYTES, RelPath, guard_reserved, push_component_key, truncate_at_boundary,
};

/// Longest extension (with its dot) that survives in a renamed component.
const MAX_EXT_BYTES: usize = 16;

/// The key of every proper prefix of `parts` and of the whole path: entry `i`
/// is the key of `parts[..=i]`, as a slice of one shared buffer.
fn prefix_keys(parts: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(parts.len());
    let mut key = String::new();
    for (i, c) in parts.iter().enumerate() {
        if i > 0 {
            key.push('/');
        }
        push_component_key(&mut key, c);
        out.push(key.clone());
    }
    out
}

/// `stem.n.ext` for `component`, within [`MAX_COMPONENT_BYTES`] (the stem
/// gives way), never a reserved Windows device name.
fn numbered(component: &str, n: usize) -> String {
    let dot = component.rfind('.').filter(|&i| i > 0);
    let (stem, ext) = match dot.and_then(|i| Some((i, component.get(i..)?))) {
        Some((i, e)) if e.len() <= MAX_EXT_BYTES => (component.get(..i).unwrap_or(""), e),
        _ => (component, ""),
    };
    let suffix = format!(".{n}{ext}");
    let room = MAX_COMPONENT_BYTES.saturating_sub(suffix.len());
    let stem = truncate_at_boundary(stem, room);
    guard_reserved(format!("{stem}{suffix}"))
}

/// Renames colliding files in place, in torrent order. One pass: every key is
/// looked up in a hash set, and a directory rename is remembered so later
/// files under it follow.
pub(crate) fn resolve_collisions(files: &mut [FileEntry]) {
    // Keys of the files kept so far, and of their directory prefixes.
    let mut file_keys: HashSet<String> = HashSet::new();
    let mut dir_keys: HashSet<String> = HashSet::new();
    // Original directory-prefix key -> the prefix it became.
    let mut renamed_dirs: HashMap<String, Vec<String>> = HashMap::new();
    // Where the search for a free number resumes, per colliding name: the
    // sets only grow, so every smaller number stays taken.
    let mut resume: HashMap<String, usize> = HashMap::new();
    let max_n = files.len().saturating_add(1);

    for file in files.iter_mut().filter(|f| !f.pad) {
        let original = file.path.components();
        let original_keys = prefix_keys(original);
        let last = original.len().saturating_sub(1);

        // Follow the longest directory prefix renamed earlier.
        let mut parts: Vec<String> = original.to_vec();
        for k in (0..last).rev() {
            let hit = original_keys.get(k).and_then(|key| renamed_dirs.get(key));
            if let Some(prefix) = hit {
                let mut moved = prefix.clone();
                moved.extend(original.iter().skip(k + 1).cloned());
                parts = moved;
                break;
            }
        }

        // The shallowest component whose key is taken the wrong way.
        let keys = prefix_keys(&parts);
        let clash = keys
            .iter()
            .enumerate()
            .position(|(i, key)| file_keys.contains(key) || (i == last && dir_keys.contains(key)));
        if let Some(i) = clash
            && let Some(component) = parts.get(i).cloned()
        {
            let name = parts.get(..=i).unwrap_or(&[]).join("/");
            let mut n = resume.get(&name).copied().unwrap_or(0);
            while n < max_n {
                n += 1;
                let mut candidate: Vec<String> = parts.get(..i).unwrap_or(&[]).to_vec();
                candidate.push(numbered(&component, n));
                let mut key = String::new();
                for (j, c) in candidate.iter().enumerate() {
                    if j > 0 {
                        key.push('/');
                    }
                    push_component_key(&mut key, c);
                }
                if file_keys.contains(&key) || dir_keys.contains(&key) {
                    continue;
                }
                resume.insert(name, n);
                if i < last {
                    if let Some(original_key) = original_keys.get(i) {
                        let mut renamed = candidate.clone();
                        renamed.extend(parts.iter().skip(i + 1).cloned());
                        // The directory's later files rewrite through its
                        // renamed prefix only.
                        renamed_dirs.insert(original_key.clone(), candidate);
                        parts = renamed;
                    }
                } else {
                    parts = candidate;
                }
                break;
            }
        }

        let keys = prefix_keys(&parts);
        for (i, key) in keys.into_iter().enumerate() {
            if i == last {
                file_keys.insert(key);
            } else {
                dir_keys.insert(key);
            }
        }
        file.path = RelPath::from_components(parts);
    }
}

/// Whether `candidate` as the path of `files[index]` would collide with
/// another file: its key equals another file's key, is a directory prefix of
/// it, or has it as a directory prefix. Padding files are exempt, as in
/// the parse-time renaming.
pub fn collides(files: &[FileEntry], index: usize, candidate: &RelPath) -> bool {
    let key = candidate.key();
    files
        .iter()
        .enumerate()
        .filter(|(i, f)| *i != index && !f.pad)
        .any(|(_, f)| {
            let other = f.path.key();
            other == key || is_dir_prefix(&key, &other) || is_dir_prefix(&other, &key)
        })
}

/// Whether `prefix` is a proper directory prefix of `path` (both keys).
fn is_dir_prefix(prefix: &str, path: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}
