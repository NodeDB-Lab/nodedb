// SPDX-License-Identifier: BUSL-1.1

//! Undo of a document version installed below the row's newest version.
//!
//! Such an install wrote the version at its ordinal, its index entries at
//! that ordinal, and the entries that close them at its successor's
//! ordinal. It ran no current-state side effect, so its undo removes those
//! entries and touches nothing else: no text index, no document cache, no
//! cascade.

use nodedb_types::StorageKey;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::history_install::SuccessorIndexWrites;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

use super::UndoError;
use super::document::UndoDocumentContext;

/// What reverses one history install.
pub(in crate::data::executor) struct HistoryVersionUndo {
    pub collection: String,
    pub document_id: StorageKey,
    /// The installed version's ordinal. Undo removes the version there.
    pub version_sys_from: i64,
    /// `(field, value)` pairs whose entries the install wrote at
    /// `version_sys_from`.
    pub index_tuples: Vec<(String, String)>,
    /// The entries the install wrote at its successor's ordinal.
    pub successor: SuccessorIndexWrites,
    /// Pre-image of the collection's hash-chain head (see
    /// `UndoEntry::PutDocument`).
    pub chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
}

impl CoreLoop {
    /// Reverse one history install. An error is fatal to the rollback.
    pub(super) fn apply_undo_history_version(
        &mut self,
        database_id: u64,
        tid: u64,
        entry_index: usize,
        undo: HistoryVersionUndo,
    ) -> Result<(), UndoError> {
        let HistoryVersionUndo {
            collection,
            document_id,
            version_sys_from,
            index_tuples,
            successor,
            chain_hash_prior,
        } = undo;
        let ctx = UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection: &collection,
            document_id: &document_id,
        };
        self.undo_version_write(ctx, version_sys_from, &index_tuples)?;
        self.undo_successor_index(ctx, &successor)?;
        self.undo_chain_hash(database_id, tid, &collection, entry_index, chain_hash_prior)
    }

    /// Remove the entries a history install wrote at its successor's
    /// ordinal, in one redb write transaction.
    fn undo_successor_index(
        &self,
        ctx: UndoDocumentContext<'_>,
        successor: &SuccessorIndexWrites,
    ) -> Result<(), UndoError> {
        if successor.tuples.is_empty() {
            return Ok(());
        }
        let UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection,
            document_id,
        } = ctx;
        let map_err = |stage: &str, e: crate::Error| {
            let err = UndoError::failed(
                entry_index,
                format!("successor index {stage} on {collection}/{document_id}"),
                e,
            );
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %err,
                "transaction undo: successor index removal failed; shard state unknown"
            );
            err
        };
        let txn = self
            .sparse
            .begin_write()
            .map_err(|e| map_err("begin_write", e))?;
        for (field, value) in &successor.tuples {
            self.sparse
                .versioned_index_remove_in_txn(
                    &txn,
                    VersionedIndexEntry {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        field,
                        value,
                        doc_id: document_id,
                        sys_from: successor.sys_from,
                    },
                )
                .map_err(|e| map_err("remove", e))?;
        }
        txn.commit().map_err(|e| {
            map_err(
                "commit",
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: e.to_string(),
                },
            )
        })
    }
}
