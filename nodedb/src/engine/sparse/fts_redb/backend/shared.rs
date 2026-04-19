//! Shared helpers for the FTS backend: error construction and the
//! inclusive upper bound for `(tid, collection, subkey)` range scans.

/// Upper-bound sentinel for the third tuple component of a
/// `(tid, collection, subkey)` range scan. `"\u{10ffff}"` is the highest
/// scalar value representable in UTF-8, so every valid subkey sorts
/// below it.
pub(super) const MAX_SUBKEY: &str = "\u{10ffff}";

/// Upper-bound sentinel for the second tuple component of a
/// `(tid, collection, …)` range scan. Same reasoning as [`MAX_SUBKEY`].
pub(super) const MAX_COLLECTION: &str = "\u{10ffff}";

pub(super) fn redb_err(ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("{ctx}: {e}"),
    }
}
