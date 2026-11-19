// SPDX-License-Identifier: BUSL-1.1

//! The validate of a whole committed redo record: every check its install
//! runs before the first write.
//!
//! The live install runs it, then installs. Restart replay runs it on a
//! record whose install has no outcome, at the record's place in the
//! stream, and applies the record only when it passes. Both reach the same
//! verdict on the same state.

use nodedb_physical::physical_plan::{RedoOrigin, RedoSumTargets};
use nodedb_wal::WalRecord;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::RedoRecord;

use super::clone_copy::clone_probe_unreadable;
use super::entry::RedoSnapshot;
use super::passes::{RedoTarget, redo_wal_record};
use super::sub_ops::{RedoDocOp, document_ops, kv_ops};
use super::validate::CommitBoundary;

/// What the validate of a whole record reads.
pub(super) struct WholeValidateInput<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    /// The LSN of the record's `TransactionRedo` WAL record.
    pub lsn: Lsn,
    pub redo: &'a RedoRecord,
    /// The record's encoded bytes, which the replay arms decode.
    pub redo_bytes: &'a [u8],
    pub sum_targets: &'a [RedoSumTargets],
    pub origin: RedoOrigin,
    pub snapshot: Option<RedoSnapshot<'a>>,
    /// The instant retention is judged at, in ms since the Unix epoch.
    pub judged_at_ms: i64,
}

/// What a passed validate hands the install.
pub(super) struct WholeValidated {
    /// The document writes, without the clone copies the install skips.
    pub doc_ops: Vec<RedoDocOp>,
    /// The record at its LSN, as the replay arms take it.
    pub record: WalRecord,
}

impl CoreLoop {
    /// Validate the whole record `input` names. Writes nothing. `Err` is
    /// the refusal the install answers with.
    pub(super) fn validate_whole_redo(
        &mut self,
        input: &WholeValidateInput<'_>,
    ) -> Result<WholeValidated, ErrorCode> {
        let WholeValidateInput {
            task,
            tid,
            lsn,
            redo,
            redo_bytes,
            sum_targets,
            origin,
            snapshot,
            judged_at_ms,
        } = *input;
        let database_id = task.request.database_id.as_u64();
        // A clone copy over a row the target holds writes nothing, so no
        // check judges it.
        let doc_ops = self
            .without_skipped_clone_copies(database_id, tid, document_ops(&redo.ops))
            .map_err(clone_probe_unreadable)?;
        // Nothing is written when a check refuses the record, so the refusal
        // is final on every replica. A role-guarded transition is judged
        // against the roles the record carries.
        let kv_writes = kv_ops(&redo.ops);
        self.check_commit_boundary(&CommitBoundary {
            database_id,
            tid,
            documents: &doc_ops,
            kv: &kv_writes,
            sum_targets,
            origin,
            user_roles: &redo.user_roles,
            judged_at_ms,
        })
        .map_err(ErrorCode::from)?;
        let record = redo_wal_record(task, lsn, redo_bytes.to_vec())?;
        let target = RedoTarget {
            record: &record,
            sub_records: redo.ops.len(),
            database_id,
            tid,
            origin,
        };
        self.validate_redo_pass(&target, sum_targets)
            .map_err(|refusal| refusal.into_code())?;
        // A snapshot transaction commits only while nothing it read or
        // replaces moved past its snapshot. Nothing is written yet.
        if let Some(snapshot) = snapshot {
            self.check_redo_snapshot(task, tid, snapshot.reads, &redo.ops, snapshot.ordinal)?;
        }
        Ok(WholeValidated { doc_ops, record })
    }
}
