// SPDX-License-Identifier: BUSL-1.1

//! Document writes of a committed redo record while its apply scope is open.
//!
//! The document redo arm hands each decoded row here instead of its restart
//! path. A replica applying a committed record re-executes the write the way
//! the transaction batch did: it links the hash chain on an insert (a row
//! already installed keeps its durable link) and folds the row into its
//! materialized-sum targets inside the row's own write transaction. Restart replay runs this path only for a Calvin record whose
//! stamp names sum targets: the fold subtracts the row's prior image, so a
//! row that already holds its post-image folds nothing.
//!
//! Constraint checks ran before the first write (see `validate`), so the
//! writes run with `enforce = false`. In the install pass every written row
//! records the undo entries that reverse it and the target rows it folded
//! into.

use nodedb_types::Surrogate;
use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{
    AbandonedWrite, ChainGuard, abandon_write, abort_after_apply,
};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::document_outcome::{
    DocumentRow, push_delete_undo, push_put_undo, push_target_undo,
};
use crate::engine::document::store::{RowIdentity, StorageKey};
use crate::event::WriteOp;
use crate::types::Lsn;

use super::state::{AppliedDocWrite, target_doc_write};

/// One document row a committed redo record writes.
pub(in crate::data::executor) struct CommittedDocWrite<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// The row's client identity text.
    pub document_id: &'a str,
    pub surrogate: u32,
    pub record_lsn: u64,
}

impl CoreLoop {
    /// Write one document put of a committed record. Returns whether the row
    /// was written; an error is kept on the open scope for the apply to report.
    pub(in crate::data::executor) fn apply_committed_document_put(
        &mut self,
        row: CommittedDocWrite<'_>,
        value: &[u8],
    ) -> bool {
        let result = self.committed_document_put(&row, value);
        self.settle_committed_write(row.record_lsn, result.map(|()| true))
    }

    /// Remove one document row of a committed record. Returns whether a row
    /// was removed.
    pub(in crate::data::executor) fn apply_committed_document_delete(
        &mut self,
        row: CommittedDocWrite<'_>,
    ) -> bool {
        let result = self.committed_document_delete(&row);
        self.settle_committed_write(row.record_lsn, result)
    }

    /// Keep a failed write's error on the open scope. Restart replay logs it
    /// with the record's LSN and skips the row, as its plain path does.
    fn settle_committed_write(&mut self, record_lsn: u64, result: crate::Result<bool>) -> bool {
        match result {
            Ok(applied) => applied,
            Err(error) => {
                match self.redo_apply.scope.as_mut() {
                    Some(scope) => scope.record_error(error),
                    None => self.replay_record_rejected(
                        "document",
                        record_lsn,
                        None,
                        &format!("folding a Calvin redo document write failed: {error}"),
                    ),
                }
                false
            }
        }
    }

    fn committed_document_put(
        &mut self,
        row: &CommittedDocWrite<'_>,
        value: &[u8],
    ) -> crate::Result<()> {
        let resolved = self.committed_sum_targets(row.collection);
        let surrogate = Surrogate::new(row.surrogate);
        let storage_key = StorageKey::for_surrogate(surrogate);
        let wal_lsn = (row.record_lsn != 0).then(|| Lsn::new(row.record_lsn));
        let hook_ctx = HookCtx {
            database_id: row.database_id,
            tid: row.tenant_id,
            collection: row.collection,
            resolved_targets: &resolved,
            // A committed record defers a target by omission from `resolved`.
            deferred_sum_targets: &[],
            wal_lsn,
        };

        // The pre-image decides whether the hash chain links the row or reuses
        // its durable link, and is the image the materialized-sum fold
        // subtracts. Read only when one of the two needs it.
        let mut chain = ChainGuard::begin(self, row.database_id, row.tenant_id, row.collection);
        let prior_bytes = if chain.enabled() || write_hook::folds_images(self, &hook_ctx) {
            self.current_row(row.database_id, row.tenant_id, row.collection, &storage_key)?
        } else {
            None
        };
        // A restore's rows carry their source links to relink. A commit's rows
        // never carry one: the validate pass refused it. Restart replay applies
        // records their origin already judged.
        let relink = self.redo_apply.scope.as_ref().is_none_or(|scope| {
            scope.origin == nodedb_physical::physical_plan::RedoOrigin::Restore
        });
        chain.chain_redo_put(self, surrogate, value, prior_bytes.as_deref(), relink)?;

        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(error) => {
                chain.restore(self);
                return Err(error);
            }
        };
        let mut outcome = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id: row.database_id,
                tid: row.tenant_id,
                collection: row.collection,
                storage_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &[],
                enforce: false,
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                resolved_targets: &resolved,
                wal_lsn,
            },
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(abort_after_apply(
                    self,
                    &mut chain,
                    abandoned(row, &storage_key),
                    error,
                ));
            }
        };
        if let Err(error) = chain
            .settle(self, surrogate, &outcome.stored_value)
            .and_then(|()| chain.persist_head(self, &txn))
        {
            return Err(abort_after_apply(
                self,
                &mut chain,
                abandoned(row, &storage_key).undo(std::mem::take(&mut outcome.memory_undo)),
                error,
            ));
        }

        // The fold reads the SUBMITTED body: `_chain_hash` wraps the row and no
        // binding is declared over it.
        let images = match prior_bytes {
            Some(ref old) => WriteImages::Update {
                old: ImageBody::Stored(old),
                new: ImageBody::Submitted(value),
            },
            None => WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        };
        let mut target_writes = match write_hook::run(self, &txn, &hook_ctx, images) {
            Ok(enforcement) => enforcement.target_writes,
            Err(error) => {
                return Err(abort_after_apply(
                    self,
                    &mut chain,
                    abandoned(row, &storage_key).undo(std::mem::take(&mut outcome.memory_undo)),
                    error,
                ));
            }
        };
        if let Err(error) = commit_row(txn) {
            return Err(abort_after_apply(
                self,
                &mut chain,
                abandoned(row, &storage_key)
                    .undo(std::mem::take(&mut outcome.memory_undo))
                    .targets(target_writes),
                error,
            ));
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        let mut index_tuples = outcome.secondary_index_added.clone();
        index_tuples.extend(outcome.secondary_index_removed.iter().cloned());
        index_tuples.extend(outcome.bitemporal_index_tuples.iter().cloned());
        self.record_committed_doc_write(AppliedDocWrite {
            collection: row.collection.to_string(),
            identity: RowIdentity::from_user_key(row.document_id),
            op: if outcome.prior_value.is_some() {
                WriteOp::Update
            } else {
                WriteOp::Insert
            },
            old_value: outcome.prior_value.clone(),
            // The submitted body, before any hash-chain wrapping: the image a
            // client wrote, as the materialized-sum fold reads it too.
            new_body: Some(value.to_vec()),
            index_tuples,
        });
        if self.recording_redo_undo() {
            let mut undo = Vec::new();
            push_target_undo(&mut undo, &mut target_writes);
            push_put_undo(
                &mut undo,
                DocumentRow {
                    collection: row.collection,
                    storage_key,
                },
                outcome,
                chain.prior(),
            );
            self.record_redo_undo(undo);
        }
        self.record_committed_targets(target_writes);
        Ok(())
    }

    fn committed_document_delete(&mut self, row: &CommittedDocWrite<'_>) -> crate::Result<bool> {
        let resolved = self.committed_sum_targets(row.collection);
        let surrogate = Surrogate::new(row.surrogate);
        let storage_key = StorageKey::for_surrogate(surrogate);
        let hook_ctx = HookCtx {
            database_id: row.database_id,
            tid: row.tenant_id,
            collection: row.collection,
            resolved_targets: &resolved,
            // A committed record defers a target by omission from `resolved`.
            deferred_sum_targets: &[],
            wal_lsn: (row.record_lsn != 0).then(|| Lsn::new(row.record_lsn)),
        };

        let txn = self.sparse.begin_write()?;
        let mut outcome = self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id: row.database_id,
                tid: row.tenant_id,
                collection: row.collection,
                document_id: row.document_id,
                surrogate,
                user_roles: &[],
                enforce: false,
                resolved_targets: &resolved,
            },
        )?;
        // An abort below drops `txn` uncommitted, which reverses the durable
        // writes only. `abandon_write` reverses the in-memory cascades.
        let mut target_writes = match outcome.prior_value {
            Some(ref old) => match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Delete {
                    old: ImageBody::Stored(old),
                },
            ) {
                Ok(enforcement) => enforcement.target_writes,
                Err(error) => {
                    let undo = std::mem::take(&mut outcome.memory_undo);
                    return Err(abandon_write(
                        self,
                        abandoned(row, &storage_key).undo(undo),
                        error,
                    ));
                }
            },
            None => Vec::new(),
        };
        if let Err(error) = commit_row(txn) {
            let undo = std::mem::take(&mut outcome.memory_undo);
            return Err(abandon_write(
                self,
                abandoned(row, &storage_key)
                    .undo(undo)
                    .targets(target_writes),
                error,
            ));
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        let removed = outcome.prior_value.clone();
        let mut index_tuples = outcome.secondary_index_tuples.clone();
        index_tuples.extend(outcome.bitemporal_index_tuples.iter().cloned());
        // A delete that removed no row can still have cascaded side effects to
        // reverse, so its undo is recorded either way.
        if self.recording_redo_undo() {
            let mut undo = Vec::new();
            push_target_undo(&mut undo, &mut target_writes);
            push_delete_undo(
                &mut undo,
                DocumentRow {
                    collection: row.collection,
                    storage_key,
                },
                outcome,
            );
            self.record_redo_undo(undo);
        }
        let Some(old_value) = removed else {
            return Ok(false);
        };
        self.record_committed_doc_write(AppliedDocWrite {
            collection: row.collection.to_string(),
            identity: RowIdentity::from_user_key(row.document_id),
            op: WriteOp::Delete,
            old_value: Some(old_value),
            new_body: None,
            index_tuples,
        });
        self.record_committed_targets(target_writes);
        Ok(true)
    }

    /// The sum targets a write to `collection` folds into: the open
    /// committed-redo scope's. With no scope open nothing folds.
    fn committed_sum_targets(
        &self,
        collection: &str,
    ) -> Vec<nodedb_physical::physical_plan::ResolvedSumTarget> {
        self.redo_apply
            .scope
            .as_ref()
            .map(|scope| scope.sum_targets_for(collection))
            .unwrap_or_default()
    }

    fn record_committed_doc_write(&mut self, write: AppliedDocWrite) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.doc_writes.push(write);
        }
    }

    fn record_committed_targets(
        &mut self,
        targets: Vec<crate::data::executor::enforcement::materialized_sum::apply::TargetWrite>,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            for target in targets {
                scope.doc_writes.push(target_doc_write(&target));
                scope.target_writes.push(target);
            }
        }
    }
}

/// A put of `row` abandoned after `apply_point_put` ran. The caller drops its
/// transaction uncommitted.
fn abandoned<'a>(row: &CommittedDocWrite<'a>, storage_key: &'a StorageKey) -> AbandonedWrite<'a> {
    AbandonedWrite::row(row.database_id, row.tenant_id, row.collection, storage_key)
}

fn commit_row(txn: WriteTransaction) -> crate::Result<()> {
    txn.commit().map_err(|e| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("committed redo document commit: {e}"),
    })
}
