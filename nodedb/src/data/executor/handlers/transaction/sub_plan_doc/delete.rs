// SPDX-License-Identifier: BUSL-1.1

//! Document PointDelete helper for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::funnel::WriteEnforcementOutcome;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::document_outcome::{
    DocumentRow, push_delete_undo, push_target_undo,
};
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::tx_point_delete`].
pub(in crate::data::executor::handlers::transaction) struct TxPointDelete<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub user_roles: &'a [String],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this delete must debit, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
}

impl CoreLoop {
    /// Execute a PointDelete within a transaction.
    pub(in crate::data::executor::handlers::transaction) fn tx_point_delete(
        &mut self,
        p: TxPointDelete<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxPointDelete {
            task: dummy_task,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
            resolved_sum_targets,
        } = p;
        let database_id = dummy_task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            // A delete carries no deferral list, and needs none: `PointDelete`
            // has no such field because its balance is settled from the stored
            // row's image and deferred by OMISSION from the resolution above,
            // which this helper already forwards. Empty here is complete, not a
            // dropped field.
            deferred_sum_targets: &[],
            wal_lsn: dummy_task.wal_lsn(),
        };

        // Core delete path shared with the autocommit caller: bitemporal-vs-plain
        // primary tombstone/delete (including versioned index tombstones),
        // FTS/inverted removal, secondary-index cascade, graph-edge cascade,
        // spatial R-tree removal, `mark_node_deleted` bookkeeping, doc_cache
        // invalidation, and stateless DELETE enforcement. Every side-effect is
        // captured in the outcome and reversed via the undo log below, so the
        // transactional delete is identical to autocommit and fully
        // rollback-safe.
        //
        // Each transaction sub-plan owns its own per-row redb write txn; the
        // batch is stitched together by the undo log, not one big txn. A
        // failure inside `apply_point_delete` returns before the commit, so the
        // txn is dropped and every sparse-database write it staged is rolled
        // back.
        let txn = self.sparse.begin_write().map_err(|e| ErrorCode::Internal {
            detail: e.to_string(),
        })?;
        let outcome = self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                user_roles,
                enforce: true,
                resolved_targets: resolved_sum_targets,
            },
        )?;

        // Whether a row was actually removed, captured before `prior_value` is
        // moved into the undo entry below. A delete against an absent key is the
        // same plan as one that removed a row, so the count is only knowable
        // here.
        let removed = outcome.prior_value.is_some();

        // Image-folding enforcement, inside the SAME transaction the removal was
        // staged in, so a materialized-sum debit and the row's removal land or
        // roll back together. The pre-image is the only image a delete has, and
        // it is what a running total has to subtract; a delete that matched
        // nothing folds nothing.
        let enforcement = match outcome.prior_value {
            Some(ref old) => write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Delete {
                    old: ImageBody::Stored(old),
                },
            )?,
            None => WriteEnforcementOutcome::default(),
        };
        let WriteEnforcementOutcome {
            target_writes,
            balanced_entries,
        } = enforcement;

        // A removal SUBTRACTS the row's amount from its group, so it is
        // accumulated onto the open batch like any other write: a transaction
        // that deletes one leg of a balanced journal leaves the group
        // unbalanced and is refused at the batch's commit boundary.
        self.settle_balanced_entries(database_id, tid, collection, balanced_entries)?;

        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("commit: {e}"),
        })?;
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // A target write is a full document write with side effects of its
        // own. The delete's own entries reverse the row, its index entries,
        // vectors, R-tree entries, the node tombstone, and every edge the
        // cascade removed.
        push_target_undo(undo_log, &target_writes);
        push_delete_undo(
            undo_log,
            DocumentRow {
                database_id,
                tid,
                collection,
                storage_key: nodedb_types::StorageKey::for_surrogate(surrogate),
                // The plan's `document_id` is the row's client identity.
                identity: nodedb_types::RowIdentity::from_user_key(document_id),
            },
            outcome,
        );

        // `PointDelete` renders a `DELETE <n>` command tag, so its response
        // carries the count — 0 when the key was absent — exactly as the
        // autocommit handler (`handlers/point/delete.rs`) reports it. A bare
        // `Ok` here leaves a Calvin-flushed delete with no count for the
        // coordinator to render, since `execute_transaction_batch` hands the
        // last sub-plan's payload back as the participant's applied response.
        Ok(self.response_affected(dummy_task, u64::from(removed)))
    }
}
