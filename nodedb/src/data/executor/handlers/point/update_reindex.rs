// SPDX-License-Identifier: BUSL-1.1

//! Atomic bitemporal UPDATE: write the new document version and reconcile the
//! versioned secondary index in a single write transaction.
//!
//! Bitemporal collections never populate the plain `INDEXES` table — every
//! secondary-index entry lives in the versioned index. The `PointUpdate`
//! fast-path therefore has to teach the versioned index about the change:
//! values the update removed must be tombstoned (so a later
//! `versioned_index_lookup_as_of` skips this doc for the old value) and the
//! current values must be asserted at the new system time (so a lookup on the
//! new value finds it). This mirrors, on the versioned index, the
//! insert/delete-time maintenance in `apply_point_put` / `apply_point_delete`.

use std::collections::BTreeSet;

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::{IndexPath, extract_index_values};
use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};

/// Inputs for [`CoreLoop::bitemporal_update_reindex`].
pub(in crate::data::executor) struct BitemporalUpdateReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub new_body: &'a [u8],
    pub index_paths: &'a [IndexPath],
    /// Decoded pre-update document, if the prior row could be decoded. `None`
    /// means no old index values to reconcile (nothing to tombstone).
    pub old_doc: Option<&'a serde_json::Value>,
    /// Decoded post-update document, used to compute the current index values.
    pub new_doc: &'a serde_json::Value,
}

/// Inputs for [`CoreLoop::nonbitemporal_update_reindex`].
pub(in crate::data::executor) struct NonbitemporalUpdateReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
    /// The same storage key, typed — `put_in_txn` below takes it directly.
    pub storage_key: &'a crate::engine::document::store::StorageKey,
    /// New stored bytes for the primary document row.
    pub new_body: &'a [u8],
    pub index_paths: &'a [IndexPath],
    /// Pre-update document image, for the secondary-index SET diff.
    pub old_doc: &'a serde_json::Value,
    /// Post-update document image, for the secondary-index SET diff.
    pub new_doc: &'a serde_json::Value,
}

impl CoreLoop {
    /// Extract the indexed values a document contributes for one path, honoring
    /// the path's partial predicate and case-folding — matching the put-time
    /// semantics in `apply_point_put`.
    fn indexed_values_for_path(doc: &serde_json::Value, path: &IndexPath) -> BTreeSet<String> {
        if let Some(ref pred) = path.predicate
            && !pred.evaluate_json(doc)
        {
            return BTreeSet::new();
        }
        extract_index_values(doc, &path.path, path.is_array)
            .into_iter()
            .map(|v| {
                if path.case_insensitive {
                    v.to_lowercase()
                } else {
                    v
                }
            })
            .collect()
    }

    /// Write the new bitemporal body and reconcile the versioned secondary
    /// index within an externally-owned WriteTransaction. Removed values are
    /// tombstoned; current values are asserted live at `sys_from_ms`. Does NOT
    /// commit.
    ///
    /// The transaction belongs to the caller because the body and its index
    /// diff are only part of what an UPDATE lands: the collection's declared
    /// constraints derive further writes from the same change, and those must
    /// commit or roll back with it rather than in a transaction of their own.
    ///
    /// On `Err` the caller MUST drop `txn` without committing.
    ///
    /// Returns the `(field, value)` tuples the diff touched (removed ∪
    /// current). The caller records them into the per-index write-value
    /// substrate AFTER its commit succeeds — those versions describe writes
    /// that are durable, so they must not be published for a transaction that
    /// never lands.
    pub(in crate::data::executor) fn bitemporal_update_reindex(
        &mut self,
        txn: &WriteTransaction,
        p: BitemporalUpdateReindex<'_>,
    ) -> crate::Result<Vec<(String, String)>> {
        self.sparse.versioned_put_in_txn(
            txn,
            VersionedPut {
                database_id: p.database_id,
                tenant: p.tid,
                coll: p.collection,
                doc_id: p.doc_id,
                sys_from_ms: p.sys_from_ms,
                valid_from_ms: p.valid_from_ms,
                valid_until_ms: p.valid_until_ms,
                body: p.new_body,
            },
        )?;

        // Collected up front (owned, no borrow of `self`) so it can be handed
        // to `note_index_write_values` after the caller's commit without a
        // borrow conflict.
        let mut touched_values: Vec<(String, String)> = Vec::new();

        for path in p.index_paths {
            let new_values = Self::indexed_values_for_path(p.new_doc, path);
            let old_values = p
                .old_doc
                .map(|d| Self::indexed_values_for_path(d, path))
                .unwrap_or_default();

            // Tombstone values the update dropped so lookups on them skip this
            // doc from `sys_from_ms` onward.
            for value in old_values.difference(&new_values) {
                self.sparse.versioned_index_tombstone_in_txn(
                    txn,
                    VersionedIndexEntry {
                        database_id: p.database_id,
                        tenant: p.tid,
                        coll: p.collection,
                        field: &path.path,
                        value,
                        doc_id: p.doc_id,
                        sys_from_ms: p.sys_from_ms,
                    },
                )?;
            }

            // Assert every current value live at the new system time.
            for value in &new_values {
                self.sparse.versioned_index_put_in_txn(
                    txn,
                    VersionedIndexEntry {
                        database_id: p.database_id,
                        tenant: p.tid,
                        coll: p.collection,
                        field: &path.path,
                        value,
                        doc_id: p.doc_id,
                        sys_from_ms: p.sys_from_ms,
                    },
                )?;
            }

            for value in old_values.union(&new_values) {
                touched_values.push((path.path.clone(), value.clone()));
            }
        }

        Ok(touched_values)
    }

    /// Write the new (non-bitemporal) document body and reconcile the plain
    /// `INDEXES` secondary index within an externally-owned WriteTransaction.
    /// Does NOT commit.
    ///
    /// The autocommit bulk-UPDATE path uses this instead of the
    /// self-committing [`SparseEngine::put`](crate::engine::sparse::SparseEngine::put):
    /// the primary row and the secondary-index SET diff land in ONE redb
    /// transaction, closing the crash window in which the index would still
    /// point at the pre-update value while the document already holds the new
    /// one — a desync that makes a later lookup on the new value miss the row
    /// and a lookup on the old value wrongly return it.
    ///
    /// On `Err` the caller MUST drop `txn` without committing.
    ///
    /// Returns the `(field, value)` tuples the index diff touched (added ∪
    /// removed). The caller records them into the per-index write-value
    /// substrate via `note_index_write_values` AFTER its commit succeeds —
    /// those versions describe writes that are durable, so they must not be
    /// published for a transaction that never lands.
    pub(in crate::data::executor) fn nonbitemporal_update_reindex(
        &mut self,
        txn: &WriteTransaction,
        p: NonbitemporalUpdateReindex<'_>,
    ) -> crate::Result<Vec<(String, String)>> {
        self.sparse.put_in_txn(
            txn,
            p.database_id,
            p.tid,
            p.collection,
            p.storage_key,
            p.new_body,
        )?;

        let (added, removed) = if !p.index_paths.is_empty() {
            self.apply_secondary_indexes_in_txn(
                txn,
                crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                    database_id: p.database_id,
                    tid: p.tid,
                    collection: p.collection,
                    old_doc: Some(p.old_doc),
                    new_doc: p.new_doc,
                    doc_id: p.doc_id,
                    index_paths: p.index_paths,
                },
            )?
        } else {
            (Vec::new(), Vec::new())
        };

        let mut touched = added;
        touched.extend(removed);
        Ok(touched)
    }
}
