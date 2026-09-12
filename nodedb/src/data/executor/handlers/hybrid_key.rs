// SPDX-License-Identifier: BUSL-1.1

//! The key the vector, text, and graph legs of a hybrid search fuse on.
//!
//! RRF fuses on key equality, so every leg must rank under one key type. The
//! text and graph legs always carry a surrogate. A vector hit whose HNSW node
//! has no surrogate binding carries none, and must never be misread as one.

use std::fmt;

use nodedb_types::{StorageKey, Surrogate};

/// Prefix of the rendered key for a vector hit with no surrogate binding.
/// The Control Plane hybrid translator passes such a `doc_id` through untouched.
pub(in crate::data::executor) const HEADLESS_SENTINEL_PREFIX: &str = "__local_";

/// One hybrid-search leg hit, in the key space RRF fuses on.
///
/// `Bound` is a row with a surrogate: it fuses across legs and renders as the
/// storage key the response envelope carries. `Headless` is a vector hit with
/// no surrogate binding: it ranks in the vector leg only, fuses with nothing,
/// and renders as the `__local_<id>` sentinel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::data::executor) enum HybridFusionKey {
    Bound(StorageKey),
    Headless(u32),
}

impl HybridFusionKey {
    /// The key of a row that carries `surrogate`.
    pub fn for_surrogate(surrogate: Surrogate) -> Self {
        Self::Bound(StorageKey::for_surrogate(surrogate))
    }

    /// The storage key of a bound row. `None` for a headless hit.
    pub fn storage_key(&self) -> Option<StorageKey> {
        match self {
            Self::Bound(key) => Some(*key),
            Self::Headless(_) => None,
        }
    }
}

impl fmt::Display for HybridFusionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(key) => write!(f, "{key}"),
            Self::Headless(local_id) => write!(f, "{HEADLESS_SENTINEL_PREFIX}{local_id}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_renders_storage_key_and_headless_renders_sentinel() {
        let bound = HybridFusionKey::for_surrogate(Surrogate::new(42));
        assert_eq!(bound.to_string(), "0000002a");
        assert_eq!(
            bound.storage_key(),
            Some(StorageKey::for_surrogate(Surrogate::new(42)))
        );

        let headless = HybridFusionKey::Headless(7);
        assert_eq!(headless.to_string(), "__local_7");
        assert_eq!(headless.storage_key(), None);
    }

    #[test]
    fn headless_never_equals_a_bound_key_with_the_same_number() {
        assert_ne!(
            HybridFusionKey::Headless(7),
            HybridFusionKey::for_surrogate(Surrogate::new(7))
        );
    }
}
