// SPDX-License-Identifier: BUSL-1.1

//! Shared index, history, stats, cache, and undo work after row materialization.

use super::{
    core::MemoryIndexPut,
    enforce::PutEnforcement,
    types::{InstalledDocumentRow, PointPutOutcome},
};
use crate::{
    data::executor::{
        core_loop::CoreLoop,
        enforcement::unique::{PostImage, UniqueJudge, UniqueScope, check_unique_post_state},
        handlers::transaction::undo::{UndoEntry, memory::abort_error},
    },
    types::SharedInstalledDocument,
};
use nodedb_physical::physical_plan::ResolvedSumTarget;
use redb::WriteTransaction;
use tracing::warn;

pub(super) struct MaterializedDocumentPut<'a> {
    pub row: InstalledDocumentRow<'a>,
    pub installed: SharedInstalledDocument,
    pub index_text: bool,
    pub user_roles: &'a [String],
    pub enforce: bool,
    pub unique: UniqueJudge,
    pub resolved_targets: &'a [ResolvedSumTarget],
    pub check_history: bool,
}
impl CoreLoop {
    pub(super) fn apply_materialized_document(
        &mut self,
        txn: &WriteTransaction,
        put: MaterializedDocumentPut<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let MaterializedDocumentPut {
            row,
            installed,
            index_text,
            user_roles,
            enforce,
            unique,
            resolved_targets,
            check_history,
        } = put;
        let InstalledDocumentRow {
            database_id,
            tid,
            collection,
            storage_key,
            surrogate,
            wal_lsn,
        } = row;
        // Runs before any store, index or history slot is touched, so a
        // refusal leaves the prior row as it stood.
        self.check_installed_image_size(database_id, tid, collection, storage_key, &installed)?;
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let crate::bridge::envelope::RowVersion {
            sys_from,
            valid_from_ms,
            valid_until_ms,
        } = installed.version;
        let value: &[u8] = &installed.canonical;
        let stored: &[u8] = &installed.stored;
        let projection = self.installed_read_projection(&config_key, &installed)?;
        let doc = projection.document;
        // A carried stamp below the row's newest version installs history
        // only. A fresh ordinal is raised above the newest version, so only
        // a carried one can land below it.
        if check_history
            && let Some(slot) =
                self.history_slot_in_txn(txn, database_id, tid, collection, &storage_key, sys_from)?
        {
            self.check_stateless_put_enforcement(
                enforce,
                PutEnforcement {
                    config_key: &config_key,
                    database_id,
                    tid,
                    collection,
                    value,
                    old_value: &slot.predecessor,
                    user_roles,
                    resolved_targets,
                },
            )?;
            return self.apply_history_put(
                txn,
                super::history::HistoryPut {
                    database_id,
                    tid,
                    collection,
                    storage_key: &storage_key,
                    installed,
                    read_projection: doc,
                    slot,
                },
            );
        }

        // The row's current body: the prior value the event reports, the
        // enforcement pre-image, and the old index values the write moves
        // off.
        let old_value = self
            .sparse
            .get_in_txn(txn, database_id, tid, collection, &storage_key)?;
        // Pre-write document for the secondary-index SET diff, routed
        // through the storage-mode-aware helper so a strict row's Binary
        // Tuple decodes too. `None` means "no prior row" or "no index
        // paths". A prior row that exists but fails to decode fails the
        // write instead, or its old index entries stay live forever.
        let old_doc_for_index: Option<serde_json::Value> =
            match (old_value.as_ref(), self.doc_configs.get(&config_key)) {
                (Some(b), Some(config)) if !config.index_paths.is_empty() => {
                    Some(self.decode_stored_document(config, b)?)
                }
                _ => None,
            };

        // Runs before any store or index is touched, so a refusal leaves
        // nothing behind.
        self.check_stateless_put_enforcement(
            enforce,
            PutEnforcement {
                config_key: &config_key,
                database_id,
                tid,
                collection,
                value,
                old_value: &old_value,
                user_roles,
                resolved_targets,
            },
        )?;

        self.sparse.versioned_put_in_txn(
            txn,
            crate::engine::sparse::btree_versioned::VersionedPut {
                database_id,
                tenant: tid,
                coll: collection,
                doc_id: &storage_key,
                sys_from,
                valid_from_ms,
                valid_until_ms,
                body: stored,
            },
        )?;
        let prior = old_value;

        // Derived state follows the current read schema, including later ADD defaults.
        // Accepted bytes and their captured canonical image remain unchanged.
        // Shared with the DELETE-rollback re-index path.
        let text_fields = self.declared_text_fields_of(&config_key);
        // A collection with no declared text field holds no inverted-index
        // state, so the write skips the index entirely.
        let index_text = index_text && !text_fields.is_empty();
        let text_content = crate::data::executor::fts_text::extract_fts_fields(&doc, text_fields);
        // Empty text is NOT skipped — stripping every declared word
        // must still remove the document from the index.
        if index_text {
            // Lands in the caller's transaction, so propagating an error
            // rolls row + index back together as one unit. Swallowing it
            // would be permanent: no WAL record to replay, so the gap
            // stays invisible to full-text search until a manual reindex.
            if let Err(e) = self.inverted.index_document_in_txn(
                txn,
                crate::engine::sparse::inverted::IndexDocScope {
                    database_id,
                    tid: crate::types::TenantId::new(tid),
                    collection,
                    surrogate,
                    ordinal: sys_from,
                },
                &text_content,
            ) {
                // Recorded here, at the detection site — an fsync'd
                // report survives a restart, unlike a log line.
                crate::diag::fts_index_update_failed(&e, collection, surrogate.as_u32());
                warn!(core = self.core_id, %collection, %storage_key, error = %e, "inverted index update failed; rejecting the write");
                return Err(e);
            }
        }

        let stats_prior =
            self.stats_store
                .observe_document_in_txn(txn, database_id, tid, collection, &doc)?;

        self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);

        self.doc_cache
            .put(database_id, tid, collection, &storage_key, sys_from, stored);

        // Secondary index extraction into the caller's write txn — the
        // non-_in_txn variant would deadlock since `execute_point_put`
        // already owns the only writer. A `Row` UNIQUE probe runs first,
        // through a separate MVCC read txn that sees the committed index only.
        let mut index_tuples: Vec<(String, String)> = Vec::new();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // A collection with index paths indexes the body, and judges its
        // UNIQUE claims. A body that does not decode fails the write: skipping
        // it stores a row its indexes do not describe and whose claims
        // no one judged.
        let indexed = match self.doc_configs.get(&config_key) {
            Some(config) if !config.index_paths.is_empty() => {
                Some((config.index_paths.clone(), doc.clone()))
            }
            _ => None,
        };
        if let Some((paths, doc)) = indexed {
            if unique == UniqueJudge::Row {
                check_unique_post_state(
                    &UniqueScope {
                        sparse: &self.sparse,
                        database_id,
                        tid,
                        collection,
                        paths: &paths,
                        base_visible: true,
                        crs: &self.crs_context(),
                    },
                    &[PostImage {
                        surrogate: storage_key.surrogate().as_u32(),
                        doc: Some(&doc),
                        judged: true,
                    }],
                )?;
            }
            // Keyed at the same `sys_from` as the row version, so one
            // `version_sys_from` in the undo entry reverses both. Values the
            // write drops are tombstoned, values it gains are added.
            index_tuples = self.apply_secondary_indexes_in_txn(
                txn,
                crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                    database_id,
                    tid,
                    collection,
                    old_doc: old_doc_for_index.as_ref(),
                    new_doc: &doc,
                    doc_id: &storage_key,
                    index_paths: &paths,
                    sys_from,
                },
            )?;
        }

        // Every write below is in memory, where the caller's transaction
        // cannot reach it. A failure part-way reverses what already ran, so
        // an `Err` from this function leaves no in-memory index entry behind.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        if let Err(e) = self.apply_point_put_memory_indexes(
            MemoryIndexPut {
                database_id,
                tid,
                collection,
                storage_key,
                value: &projection.native,
                wal_lsn: wal_lsn.map(|l| l.as_u64()).unwrap_or(0),
                sys_from,
                replaces_row: prior.is_some(),
            },
            &mut memory_undo,
        ) {
            let undone = self.undo_memory_effects(database_id, tid, memory_undo);
            return Err(abort_error(e, undone));
        }

        Ok(PointPutOutcome {
            prior_value: prior,
            installed,
            index_tuples,
            memory_undo,
            stats_prior,
            history: None,
        })
    }

    /// Refuse `installed` when its accepted or stored body is over the row
    /// image limit, or its stored body has a map key that is not a string.
    /// The refusal names the row by the identity INSERT mints.
    pub(in crate::data::executor) fn check_installed_image_size(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: nodedb_types::StorageKey,
        installed: &SharedInstalledDocument,
    ) -> crate::Result<()> {
        let largest = installed.canonical.len().max(installed.stored.len());
        let key = nodedb_types::find_body_non_string_key(&installed.stored);
        if largest <= nodedb_types::MAX_ROW_IMAGE_BYTES && key.is_none() {
            return Ok(());
        }
        let column = self.identity_column(database_id, tid, collection);
        let identity = nodedb_types::RowIdentity::of_stored_row(
            &installed.canonical,
            Some(column.as_str()),
            storage_key,
        );
        let refusal = match key {
            Some(key) if largest <= nodedb_types::MAX_ROW_IMAGE_BYTES => {
                crate::data::executor::row_field_names::non_string_key_refusal(
                    collection,
                    identity.as_str(),
                    &key,
                )
            }
            Some(_) | None => crate::data::executor::row_image_limit::row_image_too_large(
                collection,
                identity.as_str(),
                largest,
            ),
        };
        Err(crate::Error::DataPlane(refusal))
    }
}
