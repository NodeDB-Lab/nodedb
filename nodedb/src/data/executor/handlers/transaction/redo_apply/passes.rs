// SPDX-License-Identifier: BUSL-1.1

//! The two passes of a committed redo record's apply over the replay arms.
//!
//! * The validate pass decodes, routes and checks every sub-record and writes
//!   nothing. Each arm claims every sub-record it owns. A failure, or a
//!   sub-record no arm claimed, refuses the record. The pass reads only state
//!   every replica holds at the same log position, so the refusal is final.
//!   It runs over the whole record as one batch, or batch by batch through
//!   [`ValidateBatches`], which refuses with the error a whole-record pass
//!   refuses with.
//! * The install pass writes every sub-record and records the undo entries
//!   that reverse each write. A failure rolls every write back, so the core
//!   holds none of the record, which is what restart replay reproduces once
//!   the funnel cancels the record in the WAL. The refusal is retryable: the
//!   failure did not come from the record.
//! * A panic in either pass rolls back what the pass wrote and refuses the
//!   record as retryable.

use std::panic::{AssertUnwindSafe, catch_unwind};

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::data::panic_payload::panic_payload_to_string;
use crate::wal::redo::replay_arm::RedoArm;

use super::batched::ActiveArms;
use super::state::{ArmError, RedoApplyPass, RedoApplyScope};

/// Why a pass refused the record.
pub(super) enum PassRefusal {
    /// The validate pass refused it. Nothing was written.
    Invalid(ErrorCode),
    /// The install pass failed. Every write was rolled back.
    RolledBack(ErrorCode),
    /// The install pass failed and its rollback failed too. The core's state
    /// is unknown, and the `RollbackFailed` code fail-stops the core.
    RollbackFailed(ErrorCode),
    /// A batched install could not store its durable progress. The core
    /// fail-stopped, nothing was rolled back, and restart replay completes
    /// the record.
    Unpersisted(ErrorCode),
}

impl PassRefusal {
    /// The code the apply responds with.
    pub(super) fn into_code(self) -> ErrorCode {
        match self {
            Self::Invalid(code) | Self::RollbackFailed(code) | Self::Unpersisted(code) => code,
            Self::RolledBack(cause) => ErrorCode::RetryableRefusal {
                reason: format!(
                    "the committed redo record failed part way through its install and every \
                     write of it was rolled back: {cause:?}"
                ),
            },
        }
    }
}

/// Where the record applies.
pub(super) struct RedoTarget<'a> {
    pub record: &'a WalRecord,
    /// Sub-records in the record. Every one belongs to exactly one arm.
    pub sub_records: usize,
    pub database_id: u64,
    pub tid: u64,
    /// Who committed the record. The document arm reads it from the scope.
    pub origin: nodedb_physical::physical_plan::RedoOrigin,
}

impl CoreLoop {
    /// Run the validate pass: every arm checks and claims its sub-records and
    /// writes nothing.
    pub(super) fn validate_redo_pass(
        &mut self,
        target: &RedoTarget<'_>,
        sum_targets: &[RedoSumTargets],
    ) -> Result<(), PassRefusal> {
        let mut batches = ValidateBatches::open(sum_targets, target.origin);
        self.validate_redo_batch(&mut batches, target)?;
        batches.finish(target.sub_records).map(drop)
    }

    /// Run the validate pass over one batch of a record's sub-records,
    /// with the scope `batches` keeps across its batches. `batch.record`
    /// holds the batch.
    ///
    /// `Err` only when the pass cannot go on: a panic, or a lost scope. The
    /// batch's first error goes to `batches`, which ranks it.
    pub(super) fn validate_redo_batch(
        &mut self,
        batches: &mut ValidateBatches,
        batch: &RedoTarget<'_>,
    ) -> Result<(), PassRefusal> {
        let Some(scope) = batches.scope.take() else {
            return Err(PassRefusal::Invalid(ErrorCode::Internal {
                detail: "a batched validate pass ran on after it lost its apply scope".into(),
            }));
        };
        let batch_index = scope.batch;
        let (applied, mut scope) = self.run_redo_arms(batch, scope)?;
        // The reconstitution precedes every arm, so its error ranks first.
        let error = match applied {
            Err(code) => Some(ArmError {
                arm: RedoArm::Reconstitute,
                batch: batch_index,
                code,
            }),
            Ok(()) => scope.take_arm_error(),
        };
        batches.scope = Some(scope);
        if let Some(error) = error {
            batches.rank(error);
        }
        Ok(())
    }

    /// Run the install pass. On success the scope carries what the arms
    /// wrote. On a failure every write is rolled back first.
    pub(super) fn install_redo_pass(
        &mut self,
        target: &RedoTarget<'_>,
        sum_targets: &[RedoSumTargets],
    ) -> Result<RedoApplyScope, PassRefusal> {
        let (applied, mut scope) = self.run_redo_arms(
            target,
            RedoApplyScope::new(RedoApplyPass::Install, sum_targets.to_vec())
                .with_origin(target.origin),
        )?;
        let Some(cause) = applied.err().or(scope.error.take()) else {
            return Ok(scope);
        };
        let undo = std::mem::take(&mut scope.undo);
        Err(self.roll_back_redo(
            target.database_id,
            target.tid,
            undo,
            cause,
            scope.label_mark_before,
        ))
    }

    /// Reverse `undo`, the log of a whole-record install, after a pass
    /// failed with `cause`. `label_mark_before` is the node-label LSN mark
    /// the install's version commit found, which the rollback restores.
    ///
    /// Under a root journal the reversal persists with the task's write set
    /// commit. With none, every reverse write commits durably.
    pub(super) fn roll_back_redo(
        &mut self,
        database_id: u64,
        tid: u64,
        undo: Vec<UndoEntry>,
        cause: ErrorCode,
        label_mark_before: Option<u64>,
    ) -> PassRefusal {
        if let Err(mut undo_error) = self.rollback_undo_log(database_id, tid, undo) {
            undo_error.action = format!(
                "rolling back a committed redo install that failed with {cause:?}: {}",
                undo_error.action
            );
            return PassRefusal::RollbackFailed(ErrorCode::from(undo_error));
        }
        if let Err(error) = self.restore_label_mark(label_mark_before) {
            return rollback_end_failed(&cause, "restoring the node-label LSN mark", &error);
        }
        PassRefusal::RolledBack(cause)
    }

    /// Lower the node-label LSN mark to `label_mark_before`, the mark a
    /// rolled-back install's first version commit found. Restart replay of
    /// the record, when it applies, then writes the versions again.
    pub(super) fn restore_label_mark(&self, label_mark_before: Option<u64>) -> crate::Result<()> {
        match label_mark_before {
            Some(mark) => self.edge_store.lower_label_lsn_mark(mark),
            None => Ok(()),
        }
    }

    /// Drive every replay arm over the record with `scope` open. Returns the
    /// arms' own result and the scope they filled.
    ///
    /// A panic in an arm rolls back what the pass wrote and refuses the
    /// record as retryable: it is no verdict on the record's bytes.
    fn run_redo_arms(
        &mut self,
        target: &RedoTarget<'_>,
        scope: RedoApplyScope,
    ) -> Result<(Result<(), ErrorCode>, RedoApplyScope), PassRefusal> {
        // The arms route a record to `vshard_id % num_cores`. A committed
        // record carries no collection tombstone of its own: a collection
        // dropped before this entry committed refused the commit instead.
        self.redo_apply.scope = Some(scope);
        let num_cores = self.redo_apply.num_cores;
        let applied = catch_unwind(AssertUnwindSafe(|| {
            self.replay_engines_in_lsn_order(
                std::slice::from_ref(target.record),
                num_cores,
                &nodedb_wal::TombstoneSet::new(),
            )
        }));
        let scope = self.redo_apply.scope.take();
        match (applied, scope) {
            (Ok(applied), Some(scope)) => Ok((applied.map_err(ErrorCode::from), scope)),
            (Err(payload), Some(mut scope)) => {
                let cause = arm_panic(payload.as_ref());
                let undo = std::mem::take(&mut scope.undo);
                Err(self.roll_back_redo(
                    target.database_id,
                    target.tid,
                    undo,
                    cause,
                    scope.label_mark_before,
                ))
            }
            (_, None) => Err(lost_scope()),
        }
    }
}

/// The cause a panic in a replay arm refuses the record with: it is no
/// verdict on the record's bytes.
pub(super) fn arm_panic(payload: &(dyn std::any::Any + Send)) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!(
            "panic while applying a committed redo record: {}",
            panic_payload_to_string(payload)
        ),
    }
}

/// The refusal of a rollback whose undo entries all reversed and whose end
/// `step` failed with `error`.
pub(super) fn rollback_end_failed(
    cause: &ErrorCode,
    step: &str,
    error: &crate::Error,
) -> PassRefusal {
    PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
        entry_index: 0,
        detail: format!(
            "rolling back a committed redo install that failed with {cause:?}: {step} failed: \
             {error}"
        ),
        cause: None,
    })
}

/// The refusal of a pass that lost its scope. The scope held the undo log.
/// Without it nothing can be rolled back, so the core's state is unknown.
pub(super) fn lost_scope() -> PassRefusal {
    PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
        entry_index: 0,
        detail: "committed transaction redo lost its apply scope and its undo log".into(),
        cause: None,
    })
}

/// A validate pass driven over a record batch by batch: one Validate scope
/// kept across the batches, the sub-records its arms claimed, and the
/// first error by the order a whole-record pass records them.
///
/// A whole-record pass runs every arm over every sub-record, arm by arm. Its
/// first error is the one of the earliest arm that hit one, and among that
/// arm's errors, the one of the earliest sub-record. So the batches rank
/// their errors by `(arm, batch)`.
pub(super) struct ValidateBatches {
    scope: Option<RedoApplyScope>,
    first: Option<ArmError>,
}

impl ValidateBatches {
    /// Open the validate pass of a record `origin` committed, which folds
    /// into `sum_targets`.
    pub(super) fn open(
        sum_targets: &[RedoSumTargets],
        origin: nodedb_physical::physical_plan::RedoOrigin,
    ) -> Self {
        Self {
            scope: Some(
                RedoApplyScope::new(RedoApplyPass::Validate, sum_targets.to_vec())
                    .with_origin(origin),
            ),
            first: None,
        }
    }

    /// Name the batch the next [`CoreLoop::validate_redo_batch`] runs
    /// over. Its errors rank by it.
    pub(super) fn enter_batch(&mut self, batch: u32) {
        if let Some(scope) = self.scope.as_mut() {
            scope.batch = batch;
        }
    }

    /// Keep `error` when it ranks before the kept one. A tie keeps the kept
    /// error: the earlier sub-record of one batch recorded it.
    fn rank(&mut self, error: ArmError) {
        match &self.first {
            Some(kept) if (kept.arm, kept.batch) <= (error.arm, error.batch) => {}
            _ => self.first = Some(error),
        }
    }

    /// End the pass over a record of `sub_records` sub-records: its first
    /// error refuses it, and so does a sub-record no arm claimed. `Ok`
    /// holds the sub-records each arm claimed.
    pub(super) fn finish(self, sub_records: usize) -> Result<ActiveArms, PassRefusal> {
        if let Some(error) = self.first {
            return Err(PassRefusal::Invalid(final_refusal(error.code)));
        }
        let (claimed, arms) = self.scope.map_or((0, ActiveArms::default()), |scope| {
            (scope.claimed, scope.arm_claims)
        });
        if claimed != sub_records {
            return Err(PassRefusal::Invalid(ErrorCode::RejectedPrevalidation {
                reason: format!(
                    "the replay arms claimed {claimed} of the {sub_records} sub-records of the \
                     committed redo record: a sub-record matches no engine's shape"
                ),
            }));
        }
        Ok(arms)
    }
}

/// The `TransactionRedo` WAL record the arms read `payload` from, at `lsn`
/// on the vShard `task` names. A record the WAL refuses, such as one past
/// its size limit, refuses alike on every replica, so the refusal is final.
pub(super) fn redo_wal_record(
    task: &ExecutionTask,
    lsn: crate::types::Lsn,
    payload: Vec<u8>,
) -> Result<WalRecord, ErrorCode> {
    WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: lsn.as_u64(),
        tenant_id: task.request.tenant_id.as_u64(),
        vshard_id: task.request.vshard_id.as_u32(),
        database_id: task.request.database_id.as_u64(),
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .map_err(|error| final_refusal(crate::Error::from(error).into()))
}

/// A validate-pass failure as a final refusal. The pass wrote nothing and
/// read only state every replica holds at the same log position, so a
/// failure the arms report as internal is a verdict on the record's bytes.
pub(super) fn final_refusal(code: ErrorCode) -> ErrorCode {
    match code {
        ErrorCode::Internal { detail } => ErrorCode::RejectedPrevalidation { reason: detail },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::RedoOrigin;

    use super::*;

    fn refusal(arm: RedoArm, batch: u32, reason: &str) -> ArmError {
        ArmError {
            arm,
            batch,
            code: ErrorCode::RejectedPrevalidation {
                reason: reason.into(),
            },
        }
    }

    fn refused_reason(batches: ValidateBatches) -> String {
        match batches.finish(0) {
            Err(PassRefusal::Invalid(ErrorCode::RejectedPrevalidation { reason })) => reason,
            Err(other) => panic!("expected a validate refusal, got {:?}", other.into_code()),
            Ok(_) => panic!("expected a validate refusal"),
        }
    }

    /// A later batch's error of an earlier arm ranks first: a whole-record
    /// pass runs that arm over every sub-record before the next arm runs.
    #[test]
    fn an_earlier_arm_ranks_before_an_earlier_batch() {
        let mut batches = ValidateBatches::open(&[], RedoOrigin::Commit);
        batches.rank(refusal(RedoArm::Kv, 0, "kv in batch one"));
        batches.rank(refusal(RedoArm::Document, 1, "document in batch two"));
        batches.rank(refusal(RedoArm::Timeseries, 2, "timeseries in batch three"));
        assert_eq!(refused_reason(batches), "document in batch two");
    }

    /// Within one arm, the earlier batch holds the earlier sub-record.
    #[test]
    fn a_tie_keeps_the_earlier_batch() {
        let mut batches = ValidateBatches::open(&[], RedoOrigin::Commit);
        batches.rank(refusal(RedoArm::Document, 0, "batch one"));
        batches.rank(refusal(RedoArm::Document, 1, "batch two"));
        assert_eq!(refused_reason(batches), "batch one");
    }

    /// The batch index decides between two errors of one arm, whatever
    /// order they reach the ranking in.
    #[test]
    fn an_earlier_batch_ranks_first_in_one_arm() {
        let mut batches = ValidateBatches::open(&[], RedoOrigin::Commit);
        batches.rank(refusal(RedoArm::Document, 3, "batch four"));
        batches.rank(refusal(RedoArm::Document, 1, "batch two"));
        assert_eq!(refused_reason(batches), "batch two");
    }

    /// A pass with no error refuses a record its arms did not claim whole.
    #[test]
    fn an_unclaimed_sub_record_refuses_the_record() {
        let batches = ValidateBatches::open(&[], RedoOrigin::Commit);
        assert!(batches.finish(0).is_ok());
        let batches = ValidateBatches::open(&[], RedoOrigin::Commit);
        assert!(matches!(
            batches.finish(1),
            Err(PassRefusal::Invalid(
                ErrorCode::RejectedPrevalidation { .. }
            ))
        ));
    }
}
