// SPDX-License-Identifier: BUSL-1.1

//! Test driver for a session commit on one core: stage each plan, resolve
//! the transaction into its redo record, and install the record.
//!
//! [`CoreLoop::commit_plans_then_refuse_for_test`] appends a sub-record that
//! passes validation and fails while it installs, so the install rolls back
//! every write the transaction's own sub-records made.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::{Response, Status};
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, TxnId};
use crate::wal::{RedoRecord, RedoSubRecord};
use nodedb_wal::record::RecordType;

use super::entry::CommittedRedo;

impl CoreLoop {
    /// Commit `plans` as one session transaction and install its redo record
    /// at `lsn`, the path every committed transaction takes on a core.
    ///
    /// Returns the first refusal: a staging error, a resolve error, or the
    /// install's error. The transaction overlay is released on every path.
    pub(in crate::data::executor) fn commit_plans_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        lsn: u64,
    ) -> Response {
        self.commit_plans_around_for_test(task, tid, plans, lsn, |_| {})
    }

    /// Commit `plans` like [`Self::commit_plans_for_test`], running `between`
    /// after the resolve and before the install: a write another session
    /// commits while the transaction's record is in flight.
    pub(in crate::data::executor) fn commit_plans_around_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        lsn: u64,
        between: impl FnOnce(&mut Self),
    ) -> Response {
        let response = match self.stage_and_resolve_for_test(task, tid, plans, lsn) {
            Ok(redo) => {
                between(self);
                self.install_redo_for_test(task, tid, plans, &redo, lsn)
            }
            Err(refusal) => refusal,
        };
        // A session COMMIT releases the overlay once the install answered.
        self.drop_overlay_entry(TxnId::new(lsn));
        response
    }

    /// Commit `plans` like [`Self::commit_plans_for_test`], with one more
    /// sub-record after theirs that fails while it installs.
    pub(in crate::data::executor) fn commit_plans_then_refuse_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        lsn: u64,
    ) -> Response {
        let response = match self.stage_and_resolve_for_test(task, tid, plans, lsn) {
            Ok(redo) => {
                let mut record = RedoRecord::from_bytes(&redo).expect("decode resolved redo");
                record.ops.push(failing_install_sub_record());
                let redo = record.to_bytes().expect("encode redo");
                self.install_redo_for_test(task, tid, plans, &redo, lsn)
            }
            Err(refusal) => refusal,
        };
        self.drop_overlay_entry(TxnId::new(lsn));
        response
    }

    /// Stage `plans` under the transaction `lsn` names and resolve them into
    /// the encoded redo record. The overlay stays for the caller to release.
    fn stage_and_resolve_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        lsn: u64,
    ) -> Result<Vec<u8>, Response> {
        let txn_id = TxnId::new(lsn);
        let mut request = task.request.clone();
        request.txn_id = Some(txn_id);
        request.wal_lsn = None;
        let stage_task = ExecutionTask::new(request);
        for plan in plans {
            let staged = self.execute_stage_write(&stage_task, tid, plan);
            if staged.status != Status::Ok {
                return Err(staged);
            }
        }
        let resolved = self.execute_resolve_txn(&stage_task, tid, txn_id, plans);
        if resolved.status != Status::Ok {
            return Err(resolved);
        }
        Ok(resolved.payload.as_bytes().to_vec())
    }

    fn install_redo_for_test(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        redo: &[u8],
        lsn: u64,
    ) -> Response {
        let mut install_request = task.request.clone();
        install_request.wal_lsn = Some(Lsn::new(lsn));
        let install_task = ExecutionTask::new(install_request);
        let collections = written_collections(plans);
        let sum_targets = redo_sum_targets(plans);
        self.install_committed_redo(
            &install_task,
            tid,
            CommittedRedo {
                redo,
                collections: &collections,
                sum_targets: &sum_targets,
            },
        )
    }
}

/// A timeseries batch whose line names another measurement than its
/// collection: it passes validation and fails while it installs.
pub(in crate::data::executor) fn failing_install_sub_record() -> RedoSubRecord {
    let lines = zerompk::to_msgpack_vec(&vec!["other_probe,host=a value=1 1".to_string()])
        .expect("encode lines");
    RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload: crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
            "refusal_probe",
            &lines,
            None,
            "ilp-msgpack",
        )
        .expect("encode ingest"),
    }
}

impl CoreLoop {
    /// Run the install pass of a committed redo record carrying `ops` at
    /// `lsn`, and return the undo entries that reverse its writes.
    ///
    /// The forward write lands through the same arms a committed install
    /// drives. The caller reverses it with `rollback_undo_log`, as a failed
    /// install does.
    pub(in crate::data::executor) fn install_with_undo_for_test(
        &mut self,
        tid: u64,
        lsn: u64,
        ops: Vec<RedoSubRecord>,
    ) -> Vec<crate::data::executor::handlers::transaction::undo::UndoEntry> {
        let payload = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let record = nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: tid,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        self.redo_apply.scope = Some(super::state::RedoApplyScope::new(
            super::state::RedoApplyPass::Install,
            Vec::new(),
        ));
        let applied = self.replay_engines_in_lsn_order(
            std::slice::from_ref(&record),
            self.redo_apply.num_cores,
            &nodedb_wal::TombstoneSet::new(),
        );
        let scope = self.redo_apply.scope.take().expect("install scope");
        applied.expect("install arms");
        assert!(scope.error.is_none(), "install error: {:?}", scope.error);
        scope.undo
    }
}

/// The redo sub-record of a document put: `(collection, document_id, body,
/// provenance, surrogate)`.
pub(in crate::data::executor) fn doc_put_sub_record(
    collection: &str,
    document_id: &str,
    body: &[u8],
    surrogate: u32,
) -> RedoSubRecord {
    RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: zerompk::to_msgpack_vec(&(
            collection,
            document_id,
            body.to_vec(),
            None::<nodedb_types::sync::wire::SyncProvenance>,
            surrogate,
        ))
        .expect("encode document put"),
    }
}

/// The redo sub-record of a document delete: `(collection, document_id,
/// provenance, surrogate)`.
pub(in crate::data::executor) fn doc_delete_sub_record(
    collection: &str,
    document_id: &str,
    surrogate: u32,
) -> RedoSubRecord {
    RedoSubRecord {
        record_type: RecordType::Delete as u32,
        payload: zerompk::to_msgpack_vec(&(
            collection,
            document_id,
            None::<nodedb_types::sync::wire::SyncProvenance>,
            surrogate,
        ))
        .expect("encode document delete"),
    }
}
