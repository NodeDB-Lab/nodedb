// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging of an in-transaction `TRUNCATE`.
//!
//! A `TRUNCATE` inside `BEGIN..COMMIT` sets a truncate marker on the
//! transaction's overlay (`TxnOverlay::mark_truncated`). Every row staged
//! earlier in the same transaction is tombstoned through the undo journal,
//! and every base row with no newer overlay entry is hidden from the
//! transaction's own reads and existence probes. Nothing touches base: the
//! buffered plan replays through the live truncate inside the COMMIT
//! `TransactionBatch`, in statement order, and `ROLLBACK` / `ROLLBACK TO
//! SAVEPOINT` drop the marker.
//!
//! The response carries no payload: the statement's tag is the bare
//! `TRUNCATE` autocommit answers with, so there is no row count to report.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Stage a `DocumentOp::Truncate` of `collection` into `txn_id`'s overlay.
    pub(in crate::data::executor) fn stage_document_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
    ) -> Response {
        self.stage_truncate_marker(task, tid, txn_id, collection)
    }

    /// Stage a `KvOp::Truncate` of `collection` into `txn_id`'s overlay. KV
    /// rows share the document overlay (keyed by `kv_row_identity`), so the
    /// same marker hides them.
    pub(in crate::data::executor) fn stage_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
    ) -> Response {
        self.stage_truncate_marker(task, tid, txn_id, collection)
    }

    fn stage_truncate_marker(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
    ) -> Response {
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        self.txn_overlay_mut(txn_id).mark_truncated(coll_key);
        self.response_with_payload(task, Vec::new())
    }
}
