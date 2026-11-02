// SPDX-License-Identifier: BUSL-1.1

//! Atomic UPDATE: append the new document version and reconcile the
//! versioned secondary index in a single write transaction.
//!
//! Every document collection keeps its rows and index entries versioned.
//! Values the update removed are tombstoned at the new version's system time,
//! so a lookup at or after it skips this doc for the old value, and values it
//! gained get a live entry. A lookup below that system time still sees the
//! old values. This mirrors the insert/delete-time maintenance in
//! `apply_point_put` / `apply_point_delete`.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::IndexPath;
use crate::engine::sparse::btree_versioned::VersionedPut;

/// Inputs for [`CoreLoop::update_reindex`].
pub(in crate::data::executor) struct UpdateReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a crate::engine::document::store::StorageKey,
    /// The system-time ordinal of the new version.
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub new_body: &'a [u8],
    pub index_paths: &'a [IndexPath],
    /// Decoded pre-update document. `None` means no old index values to
    /// reconcile (nothing to tombstone).
    pub old_doc: Option<&'a serde_json::Value>,
    /// Decoded post-update document, used to compute the current index values.
    pub new_doc: &'a serde_json::Value,
}

/// Inputs for [`CoreLoop::update_body_in_txn`].
pub(in crate::data::executor) struct UpdateBody<'a> {
    pub config_key: &'a (crate::types::DatabaseId, crate::types::TenantId, String),
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a crate::engine::document::store::StorageKey,
    /// The row as stored before the update.
    pub current_bytes: &'a [u8],
    /// The row as it will be stored.
    pub updated_bytes: &'a [u8],
    /// The system-time ordinal of the new version.
    pub sys_from: i64,
}

impl CoreLoop {
    /// Write a row's post-update body within `txn`, with the secondary-index
    /// diff between its two images. Every update of a stored row lands
    /// through here, so no path writes a body its secondary index still
    /// describes by the old value. Does NOT commit.
    ///
    /// The row gets a version valid for all time at `sys_from`. A collection
    /// with index paths decodes both images by its storage mode, and an
    /// image that does not decode fails the write: the body never lands
    /// without its diff.
    pub(in crate::data::executor) fn update_body_in_txn(
        &mut self,
        txn: &WriteTransaction,
        p: UpdateBody<'_>,
    ) -> crate::Result<()> {
        let index_paths: Vec<IndexPath> = self
            .doc_configs
            .get(p.config_key)
            .map(|c| c.index_paths.clone())
            .unwrap_or_default();
        let images = match self.doc_configs.get(p.config_key) {
            Some(cfg) if !index_paths.is_empty() => self
                .decode_stored_document(cfg, p.current_bytes)
                .and_then(|old| {
                    self.decode_stored_document(cfg, p.updated_bytes)
                        .map(|new| Some((old, new)))
                })
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!(
                        "update: document failed to decode for secondary-index diff \
                         (collection {}, id {}): {e}",
                        p.collection, p.storage_key
                    ),
                })?,
            // An unregistered collection, or one with no index paths, has no
            // index entries to maintain.
            Some(_) | None => None,
        };
        let empty = serde_json::Value::Null;
        let (old_doc, new_doc) = match &images {
            Some((old, new)) => (Some(old), new),
            None => (None, &empty),
        };
        self.update_reindex(
            txn,
            UpdateReindex {
                database_id: p.database_id,
                tid: p.tid,
                collection: p.collection,
                doc_id: p.storage_key,
                sys_from: p.sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                new_body: p.updated_bytes,
                index_paths: if images.is_some() { &index_paths } else { &[] },
                old_doc,
                new_doc,
            },
        )
    }

    /// Append the new version and reconcile the versioned secondary index
    /// within an externally-owned WriteTransaction. Removed values are
    /// tombstoned and gained values asserted live at `sys_from`. Does NOT
    /// commit.
    ///
    /// The transaction belongs to the caller because the body and its index
    /// diff are only part of what an UPDATE lands: the collection's declared
    /// constraints derive further writes from the same change, and those must
    /// commit or roll back with it rather than in a transaction of their own.
    ///
    /// On `Err` the caller MUST drop `txn` without committing.
    pub(in crate::data::executor) fn update_reindex(
        &mut self,
        txn: &WriteTransaction,
        p: UpdateReindex<'_>,
    ) -> crate::Result<()> {
        self.sparse.versioned_put_in_txn(
            txn,
            VersionedPut {
                database_id: p.database_id,
                tenant: p.tid,
                coll: p.collection,
                doc_id: p.doc_id,
                sys_from: p.sys_from,
                valid_from_ms: p.valid_from_ms,
                valid_until_ms: p.valid_until_ms,
                body: p.new_body,
            },
        )?;
        if p.index_paths.is_empty() {
            return Ok(());
        }
        self.apply_secondary_indexes_in_txn(
            txn,
            crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                database_id: p.database_id,
                tid: p.tid,
                collection: p.collection,
                old_doc: p.old_doc,
                new_doc: p.new_doc,
                doc_id: p.doc_id,
                index_paths: p.index_paths,
                sys_from: p.sys_from,
            },
        )?;
        Ok(())
    }
}
