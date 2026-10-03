// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the FTS backend: error construction.

pub(super) fn redb_err(ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("{ctx}: {e}"),
    }
}
