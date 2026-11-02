// SPDX-License-Identifier: BUSL-1.1

//! Removing one row's current version inside a caller-owned transaction.
//!
//! No row is erased. A delete appends a tombstone version at a system time
//! and tombstones every value the closed version held in the versioned
//! index. History before that system time stays readable at any earlier cut
//! until the purge removes it.
//!
//! The point delete and the bulk delete both land each row here, so the two
//! write the same versions and index tombstones for the same row.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::StorageKey;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;
use crate::types::{DatabaseId, TenantId};

/// Inputs to [`CoreLoop::tombstone_version_in_txn`].
pub(in crate::data::executor) struct VersionTombstone<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a StorageKey,
    /// The stored body of the version the tombstone closes. Its indexed
    /// values receive the index tombstones.
    pub current_body: &'a [u8],
    /// The system time of the tombstone version.
    pub sys_from: i64,
}

/// Inputs to [`CoreLoop::remove_current_row_in_txn`].
pub(in crate::data::executor) struct CurrentRowRemoval<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a StorageKey,
    /// The tombstone's system time.
    pub sys_from: i64,
}

/// What [`CoreLoop::remove_current_row_in_txn`] removed.
pub(in crate::data::executor) struct RemovedRow {
    /// The removed row's stored body. `None` when no row was there. A
    /// removed row's tombstone lands at the removal's `sys_from`.
    pub prior: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Close a row's current version within `txn`. Writes the
    /// tombstone version at `sys_from` and an index tombstone for each
    /// value the closed version held. Does NOT commit.
    ///
    /// A registered collection's body must decode. An undecodable body fails
    /// the delete, because its index entries left live keep the row findable
    /// by its old values.
    ///
    /// Returns the `(field, value)` tuples tombstoned. On `Err` the caller
    /// MUST drop `txn` without committing.
    pub(in crate::data::executor) fn tombstone_version_in_txn(
        &self,
        txn: &WriteTransaction,
        p: VersionTombstone<'_>,
    ) -> crate::Result<Vec<(String, String)>> {
        let mut tombstoned: Vec<(String, String)> = Vec::new();
        self.write_version_tombstone(txn, p, |field, value| {
            tombstoned.push((field.to_string(), value.to_string()));
        })?;
        Ok(tombstoned)
    }

    /// [`Self::tombstone_version_in_txn`] for a caller that records the
    /// indexed values from the row's pre-image itself.
    pub(in crate::data::executor) fn close_version_in_txn(
        &self,
        txn: &WriteTransaction,
        p: VersionTombstone<'_>,
    ) -> crate::Result<()> {
        self.write_version_tombstone(txn, p, |_, _| {})
    }

    /// Write the tombstone version and the index tombstones, handing each
    /// tombstoned `(field, value)` to `tombstoned`.
    fn write_version_tombstone(
        &self,
        txn: &WriteTransaction,
        p: VersionTombstone<'_>,
        mut tombstoned: impl FnMut(&str, &str),
    ) -> crate::Result<()> {
        self.sparse.versioned_tombstone_in_txn(
            txn,
            p.database_id,
            p.tid,
            p.collection,
            p.storage_key,
            p.sys_from,
        )?;
        let config_key = (
            DatabaseId::new(p.database_id),
            TenantId::new(p.tid),
            p.collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            // No registered config names the index paths here (a replay that
            // runs before the collection registers): tombstone every entry
            // of the row that is live now, found by walking the index.
            for (field, value) in self.sparse.versioned_index_tombstone_document_in_txn(
                txn,
                p.database_id,
                p.tid,
                p.collection,
                p.storage_key,
                p.sys_from,
            )? {
                tombstoned(&field, &value);
            }
            return Ok(());
        };
        if config.index_paths.is_empty() {
            return Ok(());
        }
        let doc = self.decode_stored_document(config, p.current_body)?;
        for (field, value) in self.index_tuples_for_doc(&doc, &config.index_paths) {
            self.sparse.versioned_index_tombstone_in_txn(
                txn,
                VersionedIndexEntry {
                    database_id: p.database_id,
                    tenant: p.tid,
                    coll: p.collection,
                    field: &field,
                    value: &value,
                    doc_id: p.storage_key,
                    sys_from: p.sys_from,
                },
            )?;
            tombstoned(&field, &value);
        }
        Ok(())
    }

    /// Remove one row's current version within `txn`: read it through
    /// `txn` and close it by [`Self::close_version_in_txn`] at `sys_from`.
    /// A row that is absent removes nothing. Does NOT commit.
    ///
    /// On `Err` the caller MUST drop `txn` without committing.
    pub(in crate::data::executor) fn remove_current_row_in_txn(
        &self,
        txn: &WriteTransaction,
        r: CurrentRowRemoval<'_>,
    ) -> crate::Result<RemovedRow> {
        let Some(prior) =
            self.sparse
                .get_in_txn(txn, r.database_id, r.tid, r.collection, r.storage_key)?
        else {
            return Ok(RemovedRow { prior: None });
        };
        // The caller records the indexed values from the row's pre-image.
        self.close_version_in_txn(
            txn,
            VersionTombstone {
                database_id: r.database_id,
                tid: r.tid,
                collection: r.collection,
                storage_key: r.storage_key,
                current_body: &prior,
                sys_from: r.sys_from,
            },
        )?;
        Ok(RemovedRow { prior: Some(prior) })
    }
}
