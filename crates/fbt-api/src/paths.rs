//! The [`ApiConfig::allowed_roots`](crate::ApiConfig::allowed_roots)
//! check on paths a request names: save paths, move targets, `create`
//! sources and the save path of uploaded resume data.

use std::io;
use std::path::{Component, Path, PathBuf};

use axum::http::StatusCode;

use crate::error::ApiErrorResponse;
use crate::routes::AppState;

/// `403 path_not_allowed`.
fn not_allowed(path: &Path) -> ApiErrorResponse {
    ApiErrorResponse::new(
        StatusCode::FORBIDDEN,
        "path_not_allowed",
        format!("{} is outside the allowed roots", path.display()),
    )
}

/// Checks `path` against [`AppState::roots`] and returns the path to use.
///
/// Without roots every path is allowed and returned as given. With roots
/// the path must be absolute and free of `..`; its longest existing
/// ancestor is canonicalised (resolving every symlink in it, so a link
/// out of a root is followed and refused, and a dangling link is refused)
/// and the remaining components, which must be plain names, are appended.
/// That canonical path is returned when it lies inside a canonical root,
/// compared component-wise; otherwise `403 path_not_allowed`.
///
/// The returned path is the one to hand to the session, so a symlink
/// swapped in after the check does not redirect it.
pub(crate) fn check_path(state: &AppState, path: &Path) -> Result<PathBuf, ApiErrorResponse> {
    let Some(roots) = &state.roots else {
        return Ok(path.to_path_buf());
    };
    let canonical = canonicalise(path).ok_or_else(|| not_allowed(path))?;
    if roots.iter().any(|root| canonical.starts_with(root)) {
        Ok(canonical)
    } else {
        Err(not_allowed(path))
    }
}

/// `path` with its longest existing ancestor canonicalised and the rest
/// appended; `None` for a relative path, a `..` anywhere, or an ancestor
/// that exists (as a link, say) but cannot be canonicalised.
fn canonicalise(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return None;
    }
    // Plain names past the existing ancestor, innermost first.
    let mut rest = Vec::new();
    let mut existing = path;
    loop {
        // `symlink_metadata` does not follow links: a dangling link exists
        // here and then fails to canonicalise.
        match existing.symlink_metadata() {
            Ok(_) => break,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
        match existing.components().next_back() {
            Some(Component::Normal(name)) => rest.push(name),
            Some(Component::CurDir) => {}
            // The root or prefix itself is missing.
            _ => return None,
        }
        existing = existing.parent()?;
    }
    let mut canonical = existing.canonicalize().ok()?;
    for name in rest.iter().rev() {
        canonical.push(name);
    }
    Some(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalises_missing_tails_and_refuses_escapes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        assert_eq!(
            canonicalise(&dir.path().join("a").join("b")),
            Some(base.join("a").join("b"))
        );
        assert_eq!(canonicalise(dir.path()), Some(base.clone()));
        assert_eq!(
            canonicalise(&dir.path().join(".").join("a")),
            Some(base.join("a"))
        );
        assert_eq!(canonicalise(&dir.path().join("..")), None);
        assert_eq!(
            canonicalise(&dir.path().join("a").join("..").join("b")),
            None
        );
        assert_eq!(canonicalise(Path::new("relative")), None);
        assert_eq!(canonicalise(Path::new("")), None);
    }

    #[cfg(unix)]
    #[test]
    fn follows_links_and_refuses_dangling_ones() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(other.path(), &link).unwrap();
        assert_eq!(
            canonicalise(&link.join("x")),
            Some(other.path().canonicalize().unwrap().join("x"))
        );
        let dangling = dir.path().join("dangling");
        std::os::unix::fs::symlink(other.path().join("missing"), &dangling).unwrap();
        assert_eq!(canonicalise(&dangling.join("x")), None);
    }
}
