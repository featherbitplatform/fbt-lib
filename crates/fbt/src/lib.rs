//! fbt: pure-Rust BitTorrent library, public facade.
//!
//! # Features
//!
//! - `tls` (default): HTTPS trackers over rustls with the `ring` provider,
//!   which needs a C toolchain to build.
//! - `pure-rust`: HTTPS trackers over rustls with the pure-Rust RustCrypto
//!   provider. Depend on `fbt` with
//!   `default-features = false, features = ["pure-rust"]` for a build with no
//!   C or assembly.

#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
