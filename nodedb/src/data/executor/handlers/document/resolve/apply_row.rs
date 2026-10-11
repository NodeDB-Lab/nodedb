// SPDX-License-Identifier: BUSL-1.1

//! Landing one already-decided document mutation.
//!
//! Each arm runs the same write the live handler runs for the same row, then
//! the event and index write-versions once it commits. Nothing here
//! recomputes an image or re-decides a policy — both were settled by resolve.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::{removed_row_image, submitted_row_image};
use crate::data::executor::enforcement::chain_guard::{
    AbandonedWrite, ChainGuard, abandon_write, abort_after_apply,
};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::point::apply_put::{PointPutParams, VectorIndexDelta};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// One already-decided row write, as the apply loop hands it over.
pub(super) struct ApplyResolvedPut<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// The row's client identity, as `RETURNING` and CDC name it.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// Pre-encode MessagePack body — the write path encodes the strict Binary
    /// Tuple from it.
    pub value: &'a [u8],
    /// The stored pre-image the drift check just confirmed, `None` when the row
    /// was absent and still is.
    pub precondition: Option<&'a [u8]>,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// One already-decided row removal.
pub(super) struct ApplyResolvedDelete<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Store one resolved row and report the redo entries it owes.
    pub(super) fn apply_resolved_document_put(
        &mut self,
        task: &ExecutionTask,
        put: ApplyResolvedPut<'_>,
    ) -> Result<Vec<WriteSetEntry>, ErrorCode> {
        let ApplyResolvedPut {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            precondition,
            resolved_sum_targets,
        } = put;
        let database_id = task.request.database_id.as_u64();
        let storage_key = StorageKey::for_surrogate(surrogate);
        // The resolve pass carried the identity INSERT minted for this row;
        // the event and the redo entry both name the row by it.
        let row_identity = RowIdentity::from_user_key(document_id);
        let has_vectors = self.collection_has_vectors(database_id, tid, collection);

        // A row absent before this write is a link of a HASH_CHAIN target.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        if precondition.is_none() {
            chain
                .chain_insert(self, surrogate, value)
                .map_err(ErrorCode::from)?;
        }
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                chain.restore(self);
                return Err(ErrorCode::from(e));
            }
        };

        // HNSW insert appends rather than replaces, so the prior embedding
        // must come out first or KNN keeps scoring both. The removal is in
        // memory, so an abort below puts the prior nodes back.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        if has_vectors && precondition.is_some() {
            memory_undo.extend(
                self.remove_document_vector_indexes(database_id, tid, collection, storage_key)
                    .into_iter()
                    .map(VectorIndexDelta::into_delete_undo),
            );
        }
        let mut outcome = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                wal_lsn: task.wal_lsn(),
                resolved_targets: resolved_sum_targets,
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => {
                // Dropping `txn` reverses the write but not the cache entry
                // or the prior vector removal.
                let e = abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo),
                    e,
                );
                return Err(ErrorCode::from(e));
            }
        };
        memory_undo.append(&mut outcome.memory_undo);
        if let Err(e) = chain
            .settle(self, surrogate, &outcome.stored_value)
            .and_then(|()| chain.persist_head(self, &txn))
        {
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key).undo(memory_undo),
                e,
            );
            return Err(ErrorCode::from(e));
        }

        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };
        let images = match precondition {
            Some(old) => WriteImages::Update {
                old: ImageBody::Stored(old),
                new: ImageBody::Submitted(value),
            },
            None => WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        };
        let enforcement = match write_hook::run(self, &txn, &hook_ctx, images) {
            Ok(enforcement) => enforcement,
            Err(e) => {
                let e = abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo),
                    e,
                );
                return Err(ErrorCode::from(e));
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                e,
            );
            return Err(ErrorCode::from(e));
        }

        if let Err(e) = txn.commit() {
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                },
            );
            return Err(ErrorCode::from(e));
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Same post-commit index bookkeeping `execute_point_update` runs.
        if let Some(stamp) = self.task_write_stamp(task) {
            let mut tuples = std::mem::take(&mut outcome.secondary_index_added);
            tuples.append(&mut outcome.secondary_index_removed);
            tuples.append(&mut outcome.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                stamp,
            );
        }

        let stored_bytes = outcome.stored_value;
        self.emit_put_event(
            task,
            tid,
            collection,
            row_identity.clone(),
            &stored_bytes,
            precondition,
        );
        self.note_surrogate_write(task, tid, collection, surrogate.as_u32());

        // The row, journalled after apply from the body `apply_point_put`
        // took. A resolved write's mutations can name several collections,
        // so the entry names its own.
        let mut write_set = vec![
            submitted_row_image(
                surrogate.as_u32(),
                row_identity,
                value.to_vec(),
                outcome.bitemporal_sys_from_ms,
            )
            .in_collection(collection.to_string()),
        ];
        // Derived target rows live in a DIFFERENT collection, so each carries
        // its own `Some(collection)` and homes to that collection's vShard.
        write_set.extend(target_write_set);
        Ok(write_set)
    }

    /// Remove one resolved row and report the redo entries it owes.
    pub(super) fn apply_resolved_document_delete(
        &mut self,
        task: &ExecutionTask,
        del: ApplyResolvedDelete<'_>,
    ) -> Result<Vec<WriteSetEntry>, ErrorCode> {
        let ApplyResolvedDelete {
            tid,
            collection,
            document_id,
            surrogate,
            resolved_sum_targets,
        } = del;
        let database_id = task.request.database_id.as_u64();

        let txn = self.sparse.begin_write().map_err(ErrorCode::from)?;
        let mut outcome = self
            .apply_point_delete(
                &txn,
                PointDeleteParams {
                    database_id,
                    tid,
                    collection,
                    document_id,
                    surrogate,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    resolved_targets: resolved_sum_targets,
                },
            )
            .map_err(ErrorCode::from)?;
        // Every abort below drops `txn` uncommitted, which reverses the
        // durable writes only. `abandon_write` reverses the in-memory
        // cascades and the target rows' cache and index entries.
        let storage_key = StorageKey::for_surrogate(surrogate);
        let memory_undo = std::mem::take(&mut outcome.memory_undo);

        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };
        // The pre-image is the ONLY image a delete has, and it is what tells the
        // fold to take the removed row's contribution off the total.
        let enforcement = match outcome.prior_value {
            Some(ref old) => match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Delete {
                    old: ImageBody::Stored(old),
                },
            ) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    let e = abandon_write(
                        self,
                        AbandonedWrite::row(database_id, tid, collection, &storage_key)
                            .undo(memory_undo),
                        e,
                    );
                    return Err(ErrorCode::from(e));
                }
            },
            None => Default::default(),
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                e,
            );
            return Err(ErrorCode::from(e));
        }

        if let Err(e) = txn.commit() {
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                crate::Error::DataPlane(ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                }),
            );
            return Err(ErrorCode::from(e));
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        if let Some(prior_bytes) = outcome.prior_value.as_deref() {
            self.note_surrogate_write(task, tid, collection, surrogate.as_u32());
            if let Some(stamp) = self.task_write_stamp(task) {
                let mut tuples = outcome.secondary_index_tuples;
                tuples.extend(outcome.bitemporal_index_tuples);
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &tuples,
                    stamp,
                );
            }
            self.emit_document_delete_event(
                task,
                tid,
                collection,
                RowIdentity::from_user_key(document_id),
                Some(prior_bytes),
            );
        }
        // The removal, journalled after apply, naming its own collection,
        // then the target rows its debit rewrote.
        let mut write_set = Vec::with_capacity(target_write_set.len() + 1);
        if outcome.prior_value.is_some() {
            write_set.push(
                removed_row_image(
                    surrogate.as_u32(),
                    RowIdentity::from_user_key(document_id),
                    outcome.bitemporal_sys_from_ms,
                )
                .in_collection(collection.to_string()),
            );
        }
        write_set.extend(target_write_set);
        Ok(write_set)
    }
}
