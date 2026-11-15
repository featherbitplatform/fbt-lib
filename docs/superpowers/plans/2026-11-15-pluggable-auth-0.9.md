# fbt-api pluggable auth (0.9.0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The implementer step runs through the `local-implementer` skill.

**Goal:** an embedder of `fbt-api` can authenticate clients its own way and serve its own extra routes from the same `ApiServer`, keeping the connection cap, body limit, stall guard and CORS that `ApiServer` gives.

**Architecture:** a new `Auth::Custom(Arc<dyn Authenticator>)` variant hands each request's method, matched path, headers, query token and peer address to the embedder's synchronous check; `ApiServer::start_with(session, config, extra: axum::Router)` merges the embedder's routes into the API router before the auth layer, so they get the same auth and limits. Nothing changes for `Auth::Token` and `Auth::Disabled`.

**Tech Stack:** Rust 2024, axum, hyper (as today).

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (fbt-api section); the consumer's need is featherbittorrent's design, section 6.3 (`../featherbittorrent/docs/superpowers/specs/2026-11-14-featherbittorrent-design.md`).

## Global Constraints

- Gitflow: `feature/pluggable-auth` off `develop`; release `0.9.0` via `release/0.9.0`, annotated tag `v0.9.0` on `main`, merged back to `develop`.
- Conventional Commits; no attribution of any kind in commits or PRs.
- Breaking changes are allowed under 0.x but each is listed in `CHANGELOG.md` under Changed with who is affected.
- Every public item documented (`#![deny(missing_docs)]`); `cargo doc` with `-D warnings`.
- Tests never contact the public internet.
- The core crates never gain server features (`dev/check-pure-rust.sh` stays green).

## Review Focus

1. A custom authenticator that panics: the connection gets a 500 and the server keeps serving others. Test in Task 1 (`custom_panic_is_500`).
2. Extra routes are authenticated like built-in ones: a request to an extra route without credentials reaches the authenticator, and its rejection is a 401 with the same JSON error body as built-in routes. Test in Task 2 (`extra_routes_go_through_auth`).
3. The body limit and connection cap apply to extra routes. Test in Task 2 (`extra_routes_respect_body_limit`).
4. `?token=` is offered to the authenticator only on the routes that take it today (streams, events), never on others. Test in Task 1 (`query_token_only_on_stream_and_events`).

---

### Task 1: `Auth::Custom` and the `Authenticator` trait

**Files:** Modify `crates/fbt-api/src/config.rs`, `crates/fbt-api/src/auth.rs`, `crates/fbt-api/src/lib.rs` (re-exports, crate docs), `crates/fbt-api/src/error.rs` (if a new response is needed); Test: unit tests in `auth.rs`, `crates/fbt-api/tests/` as the crate already organises them.

**Interfaces:**
- Produces:
  - `pub trait Authenticator: Send + Sync + std::fmt::Debug + 'static { fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection>; }`
  - `#[non_exhaustive] pub struct AuthRequest<'a> { pub method: &'a Method, pub matched_path: Option<&'a str>, pub headers: &'a HeaderMap, pub query_token: Option<&'a str>, pub peer: Option<SocketAddr> }` (`query_token` is the decoded `?token=` value, present only on the `QUERY_TOKEN` routes, capped at `MAX_QUERY_TOKEN`; `peer` from `ConnectInfo` when present).
  - `#[non_exhaustive] pub enum AuthRejection { Unauthorized, Forbidden, TooManyRequests { retry_after_secs: u64 } }` → 401 (same body and `WWW-Authenticate: Bearer` as today), 403, 429 with `Retry-After`.
  - `Auth::Custom(Arc<dyn Authenticator>)`; `Debug` prints `Custom(<authenticator>)` using the authenticator's own `Debug`. Validation accepts it with any `bind`.
  - `EXEMPT` (openapi.json) stays exempt for `Token`; for `Custom` every request, `openapi.json` included, goes to the authenticator (the embedder decides).
  - CORS preflights keep being answered by the CORS layer before auth, as today.
- The check is synchronous on purpose: embedders keep session tokens in memory; anything slower belongs in their own login route.

- [ ] **Step 1: Write the failing tests:** `custom_allows_and_rejects` (an authenticator allowing only `x-test: ok` → 200 with it, 401 without), `custom_sees_peer_and_matched_path` (records what it saw: method, `/api/v1/torrents` as matched path, peer `127.0.0.1`), `query_token_only_on_stream_and_events`, `custom_forbidden_and_429` (status codes and `Retry-After`), `custom_panic_is_500` (`std::panic::catch_unwind` around the call, `AssertUnwindSafe`; the server answers the next request), `custom_debug_redacts_nothing_it_does_not_own` (Debug output is `Custom(<the authenticator's Debug>)`).
- [ ] **Step 2: Run** `cargo test -p fbt-api custom`. Expected: FAIL.
- [ ] **Step 3: Implement** in `auth.rs`: `check` gains a `Custom` arm that builds `AuthRequest` and maps `AuthRejection` to `ApiErrorResponse`.
- [ ] **Step 4: Run** `cargo test -p fbt-api`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(api): pluggable authenticator`.

### Task 2: Extra routes on `ApiServer`

**Files:** Modify `crates/fbt-api/src/server.rs`, `crates/fbt-api/src/lib.rs`; Test: the crate's server tests.

**Interfaces:**
- Consumes: Task 1.
- Produces: `ApiServer::start_with(session: Session, config: ApiConfig, extra: axum::Router) -> Result<ApiServer, ApiError>`; `start` becomes `start_with(session, config, Router::new())`. Also `pub fn router_with(session, config: &ApiConfig, extra: axum::Router) -> Result<axum::Router, ApiError>` for symmetry with `router`.
- `extra` is merged into the API routes before the auth, body-limit and CORS layers, so it gets every one of them, and it is served by the same connection cap and stall guard. `extra` must not repeat a path fbt-api serves (axum panics on overlap at merge time; document it and list the reserved prefix: everything fbt-api serves is under `/api/v1/` with the paths in `openapi()`). featherbittorrent will use `/api/v1/auth/...` and `/api/v1/fs/...`.

- [ ] **Step 1: Write the failing tests:** `extra_routes_are_served` (an extra `GET /api/v1/x/hello` answers with Token auth and the right bearer), `extra_routes_go_through_auth` (no bearer → 401 with the standard body; with `Custom`, the authenticator sees the extra matched path), `extra_routes_respect_body_limit` (`max_body_bytes` 1024, a 2 KiB POST to an extra route → 413).
- [ ] **Step 2: Run** `cargo test -p fbt-api extra_routes`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full gate (`cargo fmt --all --check`, clippy `-D warnings`, `cargo test --workspace`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `bash dev/check-pure-rust.sh`). Expected: PASS.
- [ ] **Step 5: Commit** `feat(api): serve extra routes from ApiServer`.

### Task 3: Changelog, docs, release 0.9.0

- [ ] **Step 1:** `CHANGELOG.md` under `[Unreleased]`: Added (`Auth::Custom`, `Authenticator`, `AuthRequest`, `AuthRejection`, `ApiServer::start_with`, `router_with`), Changed (a new `Auth` variant breaks exhaustive matches outside the workspace). README/crate docs: one example of a custom authenticator.
- [ ] **Step 2:** PR into `develop`, merge on green CI, then `release/0.9.0` (bump workspace version, changelog section `## [0.9.0] - <date>`), PR into `main`, merge on green, annotated tag `v0.9.0`, merge `main` back into `develop` (controller, standard flow).
