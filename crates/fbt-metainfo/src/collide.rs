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
    let (stem, ext) = split_ext(component);
    let suffix = format!(".{n}{ext}");
    let room = MAX_COMPONENT_BYTES.saturating_sub(suffix.len());
    let stem = truncate_at_boundary(stem, room);
    guard_reserved(format!("{stem}{suffix}"))
}

/// A component as `(stem, extension with its dot)`.
fn split_ext(component: &str) -> (&str, &str) {
    let dot = component.rfind('.').filter(|&i| i > 0);
    match dot.and_then(|i| Some((i, component.get(i..)?))) {
        Some((i, e)) if e.len() <= MAX_EXT_BYTES => (component.get(..i).unwrap_or(""), e),
        _ => (component, ""),
    }
}

/// What the `digits`-digit numbered names of `component` have in common: the
/// stem cut to the room those names leave, and the extension, as keys. Equal
/// families give the same candidate for every such number, so they can share
/// one search position.
fn family(component: &str, digits: u32) -> String {
    let (stem, ext) = split_ext(component);
    let suffix = ext.len() + 1 + usize::try_from(digits).unwrap_or(0);
    let room = MAX_COMPONENT_BYTES.saturating_sub(suffix);
    let mut out = component_key(truncate_at_boundary(stem, room));
    out.push('\0');
    push_component_key(&mut out, ext);
    out
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

/// What one renaming pass cost; only the tests read it.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, Default)]
struct Stats {
    /// Trie nodes held, the root included.
    nodes: usize,
    /// Candidate names tried in all.
    probes: usize,
}

/// [`resolve_collisions`], returning its cost.
fn resolve(files: &mut [FileEntry]) -> Stats {
    let mut probes = 0usize;
    let mut trie = Trie::new();
    // Original prefix node -> the prefix it became.
    let mut renamed_dirs: HashMap<u32, Renamed> = HashMap::new();
    // Where the search for a free number resumes, per parent, digit count and
    // family of candidate names (see `family`): the flags only grow, so every
    // smaller number of the group stays taken. A candidate name belongs to
    // exactly one group, so each taken name is skipped once in all.
    let mut resume: HashMap<(u32, u32, String), usize> = HashMap::new();
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
        {
            let parent = i
                .checked_sub(1)
                .and_then(|p| nodes.get(p))
                .copied()
                .unwrap_or(ROOT);
            let mut found = None;
            'digits: for digits in 1..=max_n.ilog10() + 1 {
                let low = 10usize.saturating_pow(digits - 1);
                let high = 10usize.saturating_pow(digits).saturating_sub(1).min(max_n);
                let group = (parent, digits, family(&component, digits));
                let mut n = resume.get(&group).copied().unwrap_or(low);
                while n <= high {
                    probes += 1;
                    let name = numbered(&component, n);
                    let candidate = trie.intern(parent, &component_key(&name));
                    n += 1;
                    if trie.flags_of(candidate) == 0 {
                        resume.insert(group, n);
                        found = Some((candidate, name));
                        break 'digits;
                    }
                }
                resume.insert(group, n);
            }
            if let Some((candidate, name)) = found {
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
            }
        }

        for (i, &n) in nodes.iter().enumerate() {
            trie.mark(n, if i == last { IS_FILE } else { IS_DIR });
        }
        file.path = RelPath::from_components(parts);
    }
    Stats {
        nodes: trie.len(),
        probes,
    }
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
            pieces_root: None,
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
        let stats = resolve(&mut files);
        // The root of the trie, "root", then each file's own components.
        assert_eq!(stats.nodes, 1 + 1 + files_n * (depth + 1));
        assert_eq!(stats.probes, 0);
    }

    /// Pairs of a long name and its upper-case twin, different pairs sharing
    /// the truncated stem of their renames: one probe per rename, in all.
    #[test]
    fn truncated_candidates_share_one_hint() {
        let pairs = 4000usize;
        let mut files = Vec::new();
        for i in 0..pairs {
            let base = format!("{}{i:08}", "a".repeat(247));
            files.push(entry(vec!["root".to_owned(), base.to_uppercase()]));
            files.push(entry(vec!["root".to_owned(), base]));
        }
        let n = files.len();
        let stats = resolve(&mut files);
        assert!(
            stats.probes <= 2 * n,
            "{} probes for {n} files",
            stats.probes
        );
        let mut keys: Vec<String> = files.iter().map(|f| f.path.key()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), n);
        assert!(files.iter().all(|f| {
            f.path
                .components()
                .iter()
                .all(|c| c.len() <= MAX_COMPONENT_BYTES)
        }));
    }

    /// Every case variant of one name: one key, one hint.
    #[test]
    fn case_variant_flood_takes_one_probe_per_rename() {
        let n = 20_000usize;
        let mut files: Vec<FileEntry> = (0..n)
            .map(|i| {
                let name: String = (0..20)
                    .map(|b| {
                        let c = char::from(b'a' + u8::try_from(b).unwrap_or(0));
                        if (i >> b) & 1 == 1 {
                            c.to_ascii_uppercase()
                        } else {
                            c
                        }
                    })
                    .collect();
                entry(vec!["root".to_owned(), name])
            })
            .collect();
        let stats = resolve(&mut files);
        assert!(
            stats.probes <= 2 * n,
            "{} probes for {n} files",
            stats.probes
        );
    }

    /// Literal `a.N` names already hold numbers 1..=3000; later `A`s skip them
    /// once, not once each.
    #[test]
    fn pre_taken_numbers_are_probed_once() {
        let mut files = vec![entry(vec!["root".to_owned(), "a".to_owned()])];
        files.extend((1..=3000).map(|i| entry(vec!["root".to_owned(), format!("a.{i}")])));
        files.extend((0..3000).map(|_| entry(vec!["root".to_owned(), "A".to_owned()])));
        let n = files.len();
        let stats = resolve(&mut files);
        assert!(
            stats.probes <= 2 * n,
            "{} probes for {n} files",
            stats.probes
        );
        assert_eq!(
            files.last().map(|f| f.path.to_string()).as_deref(),
            Some("root/A.6000")
        );
    }
}
