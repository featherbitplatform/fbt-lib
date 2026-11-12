//! Renaming of colliding file paths at parse time.
//!
//! Two paths collide when, on a case-insensitive or normalising filesystem,
//! they would name the same file, or one would be a file and the other a
//! directory of the same name. Their keys (see [`RelPath::key`]) are compared:
//! each component NFC-normalised and lower-cased. The first file in torrent
//! order keeps its name; a later one is renamed to `stem.N.ext` (libtorrent's
//! `resolve_duplicate_filenames`), `N` the smallest free number from 1. In
//! `stem.N.ext`, `ext` is whatever follows the last `.` of a name that has a
//! stem (so none for dotfiles), kept only up to 16 bytes with its dot; a longer
//! tail counts as part of the stem, as when over-long names are cut. A renamed
//! directory carries every later file under it. Padding files are never written
//! to disk and are exempt. The result depends only on the torrent, so resume
//! data stays valid.
//!
//! The pass is linear in the total bytes of the paths. Prefixes are interned in
//! a trie keyed by `(parent, component key)`, so a prefix costs one small entry
//! however deep it sits, and every lookup is one hash probe.

use std::collections::HashMap;

use crate::metainfo::FileEntry;
use crate::path::{
    MAX_COMPONENT_BYTES, RelPath, guard_reserved, push_component_key, truncate_at_boundary,
};

/// Longest extension (with its dot) that survives in a renamed component.
const MAX_EXT_BYTES: usize = 16;
/// The trie's root: the empty prefix.
const ROOT: u32 = 0;
/// A node is the key of a file kept so far.
const IS_FILE: u8 = 1;
/// A node is a directory prefix of a file kept so far.
const IS_DIR: u8 = 2;

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

fn component_key(component: &str) -> String {
    let mut key = String::with_capacity(component.len());
    push_component_key(&mut key, component);
    key
}

/// Interned path prefixes. A node is identified by its parent and the key of
/// its last component; the flags say what the files kept so far made of it.
struct Trie {
    children: HashMap<(u32, String), u32>,
    flags: Vec<u8>,
}

impl Trie {
    fn new() -> Self {
        Trie {
            children: HashMap::new(),
            flags: vec![0],
        }
    }

    /// The node of `key` under `parent`, created when new.
    fn intern(&mut self, parent: u32, key: &str) -> u32 {
        let next = u32::try_from(self.flags.len()).unwrap_or(u32::MAX);
        let id = *self
            .children
            .entry((parent, key.to_owned()))
            .or_insert(next);
        if id == next {
            self.flags.push(0);
        }
        id
    }

    /// What the files kept so far made of `node`.
    fn flags_of(&self, node: u32) -> u8 {
        usize::try_from(node)
            .ok()
            .and_then(|i| self.flags.get(i))
            .copied()
            .unwrap_or(0)
    }

    fn mark(&mut self, node: u32, flag: u8) {
        if let Some(f) = usize::try_from(node)
            .ok()
            .and_then(|i| self.flags.get_mut(i))
        {
            *f |= flag;
        }
    }

    /// Nodes held, the root included.
    fn len(&self) -> usize {
        self.flags.len()
    }
}

/// A directory prefix that was renamed: the path it became and its nodes.
struct Renamed {
    parts: Vec<String>,
    nodes: Vec<u32>,
}

/// Renames colliding files in place, in torrent order.
pub(crate) fn resolve_collisions(files: &mut [FileEntry]) {
    resolve(files);
}

/// [`resolve_collisions`], returning how many trie nodes it needed.
fn resolve(files: &mut [FileEntry]) -> usize {
    let mut trie = Trie::new();
    // Original prefix node -> the prefix it became.
    let mut renamed_dirs: HashMap<u32, Renamed> = HashMap::new();
    // Where the search for a free number resumes, per colliding key: the
    // flags only grow, so every smaller number stays taken.
    let mut resume: HashMap<u32, usize> = HashMap::new();
    let max_n = files.len().saturating_add(1);

    for file in files.iter_mut().filter(|f| !f.pad) {
        let original = file.path.components();
        let last = original.len().saturating_sub(1);
        let comp_keys: Vec<String> = original.iter().map(|c| component_key(c)).collect();
        let mut original_nodes = Vec::with_capacity(original.len());
        let mut node = ROOT;
        for key in &comp_keys {
            node = trie.intern(node, key);
            original_nodes.push(node);
        }

        // Follow the longest directory prefix renamed earlier.
        let mut parts: Vec<String> = original.to_vec();
        let mut nodes = original_nodes.clone();
        for k in (0..last).rev() {
            let hit = original_nodes.get(k).and_then(|n| renamed_dirs.get(n));
            if let Some(r) = hit {
                parts = r.parts.clone();
                nodes = r.nodes.clone();
                parts.extend(original.iter().skip(k + 1).cloned());
                let mut parent = nodes.last().copied().unwrap_or(ROOT);
                for key in comp_keys.iter().skip(k + 1) {
                    parent = trie.intern(parent, key);
                    nodes.push(parent);
                }
                break;
            }
        }

        // The shallowest component whose key is taken the wrong way.
        let clash = nodes.iter().enumerate().position(|(i, &n)| {
            let f = trie.flags_of(n);
            f & IS_FILE != 0 || (i == last && f & IS_DIR != 0)
        });
        if let Some(i) = clash
            && let Some(component) = parts.get(i).cloned()
            && let Some(&taken) = nodes.get(i)
        {
            let parent = i
                .checked_sub(1)
                .and_then(|p| nodes.get(p))
                .copied()
                .unwrap_or(ROOT);
            let mut n = resume.get(&taken).copied().unwrap_or(0);
            while n < max_n {
                n += 1;
                let name = numbered(&component, n);
                let candidate = trie.intern(parent, &component_key(&name));
                if trie.flags_of(candidate) != 0 {
                    continue;
                }
                resume.insert(taken, n);
                nodes.truncate(i);
                nodes.push(candidate);
                parts.truncate(i);
                parts.push(name);
                if i < last {
                    if let Some(&original_node) = original_nodes.get(i) {
                        renamed_dirs.insert(
                            original_node,
                            Renamed {
                                parts: parts.clone(),
                                nodes: nodes.clone(),
                            },
                        );
                    }
                    let mut parent = candidate;
                    for (comp, key) in original.iter().zip(&comp_keys).skip(i + 1) {
                        parent = trie.intern(parent, key);
                        nodes.push(parent);
                        parts.push(comp.clone());
                    }
                }
                break;
            }
        }

        for (i, &n) in nodes.iter().enumerate() {
            trie.mark(n, if i == last { IS_FILE } else { IS_DIR });
        }
        file.path = RelPath::from_components(parts);
    }
    trie.len()
}

/// Whether `candidate` as the path of `files[index]` would collide with
/// another file: its key equals another file's key, is a directory prefix of
/// it, or has it as a directory prefix. Padding files are exempt, as in the
/// parse-time renaming.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(parts: Vec<String>) -> FileEntry {
        FileEntry {
            path: RelPath::from_components(parts),
            len: 1,
            pad: false,
        }
    }

    /// Deep directories that are all distinct: the trie holds one node per
    /// component, not one per (file, prefix) pair with its own key string.
    #[test]
    fn deep_distinct_directories_cost_one_node_per_component() {
        let (files_n, depth) = (200usize, 200usize);
        let mut files: Vec<FileEntry> = (0..files_n)
            .map(|i| {
                let mut p = vec!["root".to_owned()];
                p.extend((0..depth).map(|d| format!("{i}-{d}-{}", "x".repeat(200))));
                p.push("f".to_owned());
                entry(p)
            })
            .collect();
        let start = std::time::Instant::now();
        let nodes = resolve(&mut files);
        // The root of the trie, "root", then each file's own components.
        assert_eq!(nodes, 1 + 1 + files_n * (depth + 1));
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }
}
