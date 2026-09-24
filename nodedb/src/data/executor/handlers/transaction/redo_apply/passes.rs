// SPDX-License-Identifier: BUSL-1.1

//! The two passes of a committed redo record's apply over the replay arms.
//!
//! * The validate pass decodes, routes and checks every sub-record and writes
//!   nothing. Each arm claims every sub-record it owns. A failure, or a
//!   sub-record no arm claimed, refuses the record. The pass reads only state
//!   every replica holds at the same log position, so the refusal is final.
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

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::panic_payload::panic_payload_to_string;

use super::state::{RedoApplyPass, RedoApplyScope};

/// Why a pass refused the record.
pub(super) enum PassRefusal {
    /// The validate pass refused it. Nothing was written.
    Invalid(ErrorCode),
    /// The install pass failed. Every write was rolled back.
    RolledBack(ErrorCode),
    /// The install pass failed and its rollback failed too. The core's state
    /// is unknown, and the `RollbackFailed` code fail-stops the core.
    RollbackFailed(ErrorCode),
}

impl PassRefusal {
    /// The code the apply responds with.
    pub(super) fn into_code(self) -> ErrorCode {
        match self {
            Self::Invalid(code) | Self::RollbackFailed(code) => code,
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
}

impl CoreLoop {
    /// Run the validate pass: every arm checks and claims its sub-records and
    /// writes nothing.
    pub(super) fn validate_redo_pass(
        &mut self,
        target: &RedoTarget<'_>,
        sum_targets: &[RedoSumTargets],
    ) -> Result<(), PassRefusal> {
        let (applied, scope) = self.run_redo_arms(
            target,
            RedoApplyScope::new(RedoApplyPass::Validate, sum_targets.to_vec()),
        )?;
        if let Some(code) = applied.err().or(scope.error) {
            return Err(PassRefusal::Invalid(final_refusal(code)));
        }
        if scope.claimed != target.sub_records {
            return Err(PassRefusal::Invalid(ErrorCode::RejectedPrevalidation {
                reason: format!(
                    "the replay arms claimed {} of the {} sub-records of the committed redo \
                     record: a sub-record matches no engine's shape",
                    scope.claimed, target.sub_records
                ),
            }));
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
            RedoApplyScope::new(RedoApplyPass::Install, sum_targets.to_vec()),
        )?;
        let Some(cause) = applied.err().or(scope.error.take()) else {
            return Ok(scope);
        };
        let undo = std::mem::take(&mut scope.undo);
        Err(self.roll_back_redo(target, undo, cause))
    }

    /// Reverse `undo` after a pass failed with `cause`.
    fn roll_back_redo(
        &mut self,
        target: &RedoTarget<'_>,
        undo: Vec<UndoEntry>,
        cause: ErrorCode,
    ) -> PassRefusal {
        match self.rollback_undo_log(target.database_id, target.tid, undo) {
            Ok(()) => PassRefusal::RolledBack(cause),
            Err((entry_index, detail)) => PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
                entry_index,
                detail: format!(
                    "rolling back a committed redo install that failed with {cause:?}: {detail}"
                ),
            }),
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
                let cause = ErrorCode::Internal {
                    detail: format!(
                        "panic while applying a committed redo record: {}",
                        panic_payload_to_string(payload.as_ref())
                    ),
                };
                let undo = std::mem::take(&mut scope.undo);
                Err(self.roll_back_redo(target, undo, cause))
            }
            // The scope held the undo log. Without it nothing can be rolled
            // back, so the core's state is unknown.
            (_, None) => Err(PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
                entry_index: 0,
                detail: "committed transaction redo lost its apply scope and its undo log".into(),
            })),
        }
    }
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
