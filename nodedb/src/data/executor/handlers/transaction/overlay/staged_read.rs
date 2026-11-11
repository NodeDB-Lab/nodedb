// SPDX-License-Identifier: BUSL-1.1

//! Read-side views over a [`TxnOverlay`]: the staged entries of one
//! collection, and the overlay's size.

use nodedb_types::RowIdentity;

use super::staged::{Staged, TxnOverlay};
use crate::types::{DatabaseId, TenantId};

impl TxnOverlay {
    /// Iterate all staged `(surrogate, Staged)` pairs for a collection.
    /// Yields nothing if the collection has no overlay entries.
    pub fn iter_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (u32, &'a Staged)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| overlay.by_surrogate.iter().map(|(k, v)| (*k, v)))
    }

    /// Iterate all staged `(identity, Staged)` pairs for a collection.
    ///
    /// Unlike [`iter_for_collection`](Self::iter_for_collection) (keyed by
    /// surrogate, the Document scan's row identity), this is keyed by the
    /// row's client identity -- the identity a KV scan merge needs, since a
    /// KV row's scan identity is its raw key bytes, not a surrogate.
    pub fn iter_doc_entries_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (&'a RowIdentity, &'a Staged)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay
                    .doc_id_to_surrogate
                    .iter()
                    .filter_map(move |(doc_id, surrogate)| {
                        overlay
                            .by_surrogate
                            .get(surrogate)
                            .map(|staged| (doc_id, staged))
                    })
            })
    }

    /// True if no collection has any staged mutation or truncate marker.
    pub fn is_empty(&self) -> bool {
        self.truncated.is_empty()
            && self
                .collections
                .values()
                .all(|overlay| overlay.by_surrogate.is_empty())
    }

    /// Total number of staged mutations across all collections.
    pub fn len(&self) -> usize {
        self.collections
            .values()
            .map(|overlay| overlay.by_surrogate.len())
            .sum()
    }

    /// Sum of staged `Put` body byte lengths across all collections. The
    /// staging handlers cap it at
    /// `[tuning.query] max_txn_overlay_bytes`.
    pub fn memory_size_estimate(&self) -> usize {
        self.collections
            .values()
            .flat_map(|overlay| overlay.by_surrogate.values())
            .map(|staged| match staged {
                Staged::Put(body) => body.len(),
                Staged::Tombstone => 0,
            })
            .sum()
    }
}
