//! fbt-tracker: Tracker clients (HTTP and UDP).

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )
)]

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
