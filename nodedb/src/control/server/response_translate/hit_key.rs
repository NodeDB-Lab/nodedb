// SPDX-License-Identifier: BUSL-1.1

//! Shared decode for the surrogate-or-headless key the vector, full-text,
//! and hybrid response translators all read off a Data-Plane hit.
//!
//! The Data Plane's `HybridFusionKey` (`nodedb/src/data/executor/handlers/
//! hybrid_key.rs`) renders a bound row as its hex storage key and a
//! headless vector hit (no surrogate binding) as the `__local_<id>`
//! sentinel. [`parse_surrogate_hex`] is the one place that string gets
//! reinterpreted back into a [`Surrogate`] on the Control Plane.

use nodedb_types::{StorageKey, Surrogate};

/// Decode a hit's identity-key text into a surrogate. `None` means the row
/// carries no surrogate binding — the `__local_<id>` headless sentinel, or a
/// value that is not a storage key — and callers leave the row's identifier
/// untouched rather than fabricate one.
pub(crate) fn parse_surrogate_hex(candidate: &str) -> Option<Surrogate> {
    StorageKey::parse(candidate).map(|key| key.surrogate())
}
