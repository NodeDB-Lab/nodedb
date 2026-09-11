// SPDX-License-Identifier: BUSL-1.1

//! `ApplyBalanceDelta`: move one materialized-sum balance on a TARGET row whose
//! collection does not share the source write's vShard.
//!
//! A collection homes to one vShard, so a binding's source and target are
//! generally served by different cores. The co-resident path applies the balance
//! inside the source write's own transaction, which is only possible because one
//! core owns both rows. When it does not, the Control Plane settles the delta at
//! plan time and appends this op as a task of its own, homed on the target — the
//! same shape an implicit graph edge takes, and for the same reason. The pair
//! then classifies as multi-shard and commits atomically through Calvin.
//!
//! # This is a full document write
//!
//! The read-modify-write is [`CoreLoop::apply_balance_delta`], shared with the
//! co-resident path: same arithmetic, same encoding decisions, same refusals. A
//! balance must not depend on where two collections happened to hash.
//!
//! # Durability names the TARGET
//!
//! The response's write-set entry carries `collection: Some(target)` and the
//! row's ABSOLUTE post-image. The post-apply redo re-derives the vShard from
//! that name, so the durable record homes with the row it describes rather than
//! with the statement that caused it. An absolute image, never the delta: a redo
//! record replays through `apply_point_put`, and replaying a delta would add it a
//! second time.

use std::str::FromStr;

use rust_decimal::Decimal;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::rmw::BalanceRmw;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::Surrogate;

/// Dispatch-side arguments for [`CoreLoop::execute_apply_balance_delta`].
pub(in crate::data::executor) struct ApplyBalanceDeltaParams<'a> {
    pub tid: u64,
    /// TARGET collection — this task is homed on it.
    pub collection: &'a str,
    /// The target row's user-visible document id, for logging only. Storage
    /// addressing goes through the surrogate, like every other document write.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// The balance column being moved.
    pub column: &'a str,
    /// Signed amount to add, as an exact decimal string.
    pub delta: &'a str,
    pub join_column: &'a str,
    pub join_value: &'a str,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_apply_balance_delta(
        &mut self,
        task: &ExecutionTask,
        params: ApplyBalanceDeltaParams<'_>,
    ) -> Response {
        let ApplyBalanceDeltaParams {
            tid,
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
        } = params;
        debug!(
            core = self.core_id,
            %collection, %document_id, %column, %delta,
            "apply balance delta"
        );

        // A delta that will not parse is a malformed plan, not a zero: applying
        // zero would report success for a balance that never moved, and the
        // stored total would then disagree with the `SUM(...)` over the source
        // rows for good.
        let delta = match Decimal::from_str(delta) {
            Ok(delta) => delta,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "materialized-sum delta '{delta}' for {collection}.{column} is not a \
                             decimal: {e}"
                        ),
                    },
                );
            }
        };

        let database_id = task.request.database_id.as_u64();
        let row_key = nodedb_types::StorageKey::for_surrogate(surrogate);

        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => return self.response_error(task, e),
        };

        let write = match self.apply_balance_delta(
            &txn,
            &BalanceRmw {
                database_id,
                tid,
                target_collection: collection,
                target_column: column,
                surrogate,
                delta,
                join_column,
                join_value,
                wal_lsn: task.wal_lsn(),
            },
        ) {
            Ok(write) => write,
            Err(e) => {
                // `apply_point_put` populates the document cache before this
                // transaction is dropped, so the entry it left would outlive a
                // balance that never committed.
                self.doc_cache
                    .invalidate(database_id, tid, collection, &row_key);
                return self.response_error(task, e);
            }
        };

        if let Err(e) = txn.commit() {
            self.doc_cache
                .invalidate(database_id, tid, collection, &row_key);
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        let mut response = self.response_affected(task, 1);
        response.write_set = vec![crate::bridge::envelope::WriteSetEntry {
            surrogate: write.surrogate.as_u32(),
            is_delete: false,
            value: write.body,
            // Always `Some`: the row lives in the TARGET collection, and the
            // redo has to name it so the record homes to the target's vShard
            // rather than to whichever collection the statement started from.
            collection: Some(write.collection),
        }];
        response
    }
}
