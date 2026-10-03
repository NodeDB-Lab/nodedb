// SPDX-License-Identifier: BUSL-1.1

//! LSM compaction support on the Origin inverted index.
//!
//! Exposes the atomic `compact_commit` (write merged + remove sources in one
//! redb transaction) and a backend-driven enumeration of FTS-bearing
//! `(tenant, collection)` pairs used by the maintenance cycle.

use nodedb_types::TenantId;

use super::core::InvertedIndex;
use crate::engine::sparse::fts_redb::CompactCommit;

impl InvertedIndex {
    /// Atomically write a new merged FTS segment and remove the source segments
    /// that were merged into it, in one redb write transaction.
    ///
    /// Crash-safe: if the process terminates before the commit, the original
    /// segments remain intact for the next maintenance pass. The single
    /// transaction also guarantees that no reader observes both the new and
    /// the old segments simultaneously.
    pub fn compact_commit(&self, params: CompactCommit<'_>) -> crate::Result<()> {
        self.inner.backend().compact_commit(params)
    }

    /// Enumerate every `(database_id, TenantId, collection, field)` index
    /// that has at least one FTS segment in the backing store. `field` is
    /// empty for a whole-document index.
    ///
    /// Used by the maintenance cycle to discover compaction candidates
    /// without requiring a separate in-memory registry of FTS-indexed
    /// collections.
    pub fn list_all_fts_indexes(&self) -> crate::Result<Vec<(u64, TenantId, String, String)>> {
        self.inner.backend().list_all_fts_indexes().map(|indexes| {
            indexes
                .into_iter()
                .map(|(d, t, c, f)| (d, TenantId::new(t), c, f))
                .collect()
        })
    }
}
