//! The [`ApiConfig::allowed_roots`](crate::ApiConfig::allowed_roots)
//! check on paths a request names: save paths, move targets, `create`
//! sources and the save path of uploaded resume data.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

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

/// An allowed root: as configured, and canonicalised once at startup.
#[derive(Debug, Clone)]
pub(crate) struct Root {
    /// The path as [`ApiConfig::allowed_roots`](crate::ApiConfig::allowed_roots)
    /// gave it.
    pub(crate) given: PathBuf,
    /// Its canonical form.
    pub(crate) canonical: PathBuf,
}

/// Checks `path` against [`AppState::roots`] and returns the path to use.
///
/// Without roots every path is allowed and returned as given. With roots,
/// before any filesystem call, the path must be absolute, free of `..`
/// and lexically below a root (as configured or canonical, compared
/// component-wise): a foreign drive, UNC share or tree is refused without
/// being touched, so a client cannot make the server stat (or, on Windows,
/// authenticate to) an arbitrary host. Then, on the blocking pool, its
/// longest existing ancestor is canonicalised (resolving every symlink in
/// it, so a link out of a root is followed and refused, and a dangling
/// link is refused) and the remaining components, which must be plain
/// names, are appended. That canonical path is returned when it lies
/// inside a canonical root, compared component-wise; otherwise
/// `403 path_not_allowed`. A path that reaches a root only through a
/// symlink alias outside the roots (say `/tmp` for a `/private/tmp` root)
/// must be given in a root's own form.
///
/// The returned path is the one to hand to the session, so a symlink
/// present at check time cannot redirect it; the residual check-to-use
/// race needs local write access inside a root.
pub(crate) async fn check_path(state: &AppState, path: &Path) -> Result<PathBuf, ApiErrorResponse> {
    match &state.roots {
        None => Ok(path.to_path_buf()),
        Some(roots) => check_in(roots, path).await,
    }
}

/// [`check_path`] against `roots`.
async fn check_in(roots: &Arc<[Root]>, path: &Path) -> Result<PathBuf, ApiErrorResponse> {
    if !lexically_inside(roots, path) {
        return Err(not_allowed(path));
    }
    let owned = path.to_path_buf();
    let roots = Arc::clone(roots);
    let canonical = tokio::task::spawn_blocking(move || {
        canonicalise(&owned).filter(|c| roots.iter().any(|root| c.starts_with(&root.canonical)))
    })
    .await
    .map_err(|e| ApiErrorResponse::internal(format!("path check failed: {e}")))?;
    canonical.ok_or_else(|| not_allowed(path))
}

/// `path` is absolute, has no `..` and lies below a root as configured or
/// canonical. Purely lexical: no filesystem call.
fn lexically_inside(roots: &[Root], path: &Path) -> bool {
    path.is_absolute()
        && !path.components().any(|c| matches!(c, Component::ParentDir))
        && roots
            .iter()
            .any(|root| path.starts_with(&root.given) || path.starts_with(&root.canonical))
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

    fn roots(dir: &Path) -> Arc<[Root]> {
        Arc::from(vec![Root {
            given: dir.to_path_buf(),
            canonical: dir.canonicalize().unwrap(),
        }])
    }

    #[tokio::test]
    async fn foreign_trees_are_refused_without_a_stat() {
        let dir = tempfile::tempdir().unwrap();
        let roots = roots(dir.path());
        // A sibling of the root that does not exist: resolving it would
        // need a stat of its parent, the lexical check refuses it first.
        let sibling = dir
            .path()
            .parent()
            .unwrap()
            .join("fbt-api-no-such-sibling")
            .join("x");
        assert!(!lexically_inside(&roots, &sibling));
        let e = check_in(&roots, &sibling).await.unwrap_err();
        assert_eq!(
            (e.status, e.code),
            (StatusCode::FORBIDDEN, "path_not_allowed")
        );
        // The root itself, in either form, and a missing path below it.
        assert!(lexically_inside(&roots, dir.path()));
        assert!(lexically_inside(&roots, &roots[0].canonical.join("x")));
        let below = check_in(&roots, &dir.path().join("a")).await.unwrap();
        assert_eq!(below, roots[0].canonical.join("a"));
        assert!(!lexically_inside(&roots, &dir.path().join("..").join("x")));
        assert!(!lexically_inside(&roots, Path::new("relative")));
    }

    #[cfg(windows)]
    #[test]
    fn unc_and_foreign_drives_are_refused_lexically() {
        let dir = tempfile::tempdir().unwrap();
        let roots = roots(dir.path());
        for p in [r"\\host\share\x", r"\\?\UNC\host\share\x", r"\\.\pipe\x"] {
            assert!(!lexically_inside(&roots, Path::new(p)), "{p}");
        }
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
