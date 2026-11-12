// SPDX-License-Identifier: BUSL-1.1

//! The whole-record answer of a resolve, and the guard check a session
//! commit runs before it resolves.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;
use crate::wal::RedoRecord;

use super::entry::ResolvedLists;

impl CoreLoop {
    /// Check a committing session transaction's guards against the state it
    /// commits on. `false` asks the commit to retry.
    ///
    /// A session transaction staged its guards at statement time and holds
    /// no lock since, so a node's edges or a document's presence can change
    /// before COMMIT. A changed state answers `false`, and the transaction
    /// writes nothing.
    pub(super) fn session_guards_hold(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) -> crate::Result<bool> {
        match self.node_edge_guards_hold(task, tid, plans) {
            Ok(true) => self.node_presence_guards_hold(task, plans),
            other => other,
        }
    }

    /// Resolve a committing transaction's staged writes into a
    /// [`RedoRecord`] and return its encoded bytes in the response payload.
    /// A Calvin slice resolves here, and so does every resolve test. A
    /// session commit reads the same resolve in parts
    /// (`execute_resolve_txn_part`).
    pub(in crate::data::executor) fn execute_resolve_txn(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> Response {
        let ResolvedLists {
            ops,
            row_changes,
            row_sources,
        } = match self.resolve_txn_lists(task, tid, txn_id, plans) {
            Ok(resolved) => resolved,
            Err(e) => return self.response_error(task, e),
        };
        let record = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources,
            // The commit attaches the transaction's publishes to the
            // record it resolves here.
            publishes: Vec::new(),
            row_changes,
            // The commit stamps the committing identity's roles on the
            // record it resolves here.
            user_roles: Vec::new(),
        };
        match record.to_bytes() {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, e),
        }
    }
}
