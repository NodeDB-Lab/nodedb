// SPDX-License-Identifier: BUSL-1.1

//! The stages of a stream install job, and what a step and a park report.

use crate::bridge::envelope::Response;
use crate::wal::redo::replay_arm::RedoArm;

use super::super::super::passes::PassRefusal;
use super::super::super::state::RedoApplyScope;
use super::super::rollback::run::RollbackRun;
use super::super::rollback::undo::{BatchAt, SpillableUndo};
use super::arm_pass::ArmPass;

/// Where a job stands.
pub(super) enum JobStage {
    /// The arms run. `next_arm` indexes the job's active arms. `pass` is
    /// the open pass of that arm, `None` before its first batch.
    Arms {
        next_arm: usize,
        pass: Option<Box<ArmPass>>,
    },
    /// Every arm wrote all of the record: the finish runs next.
    Finish(Box<FinishInput>),
    /// The arms failed, and their rollback's last durable checkpoint kept
    /// `kept`.
    RollingBack {
        run: Box<RollbackRun>,
        kept: BatchAt,
    },
    /// The job answered.
    Done,
}

/// Where a job stands between slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor::handlers::transaction::redo_apply::batched) enum ParkPoint {
    /// The validate walked batch `batch`, among every batch of the stream,
    /// and wrote nothing.
    Validating { batch: u64 },
    /// The last slice ran batch `batch`, among every batch of the stream,
    /// of `arm`'s pass.
    Batch { arm: RedoArm, batch: u64 },
    /// The arms ended.
    Finish,
    /// The rollback's last durable checkpoint kept this batch.
    RollingBack(BatchAt),
}

/// How the arms failed to end with their scope.
pub(super) enum ArmsFailed {
    /// The install answers with this refusal. Nothing rolls back.
    Refused(PassRefusal),
    /// The install rolls back.
    RollBack(Box<RollbackRun>),
}

/// What the finish reads: the apply scope the arms filled, and the undo log
/// out of it.
pub(super) struct FinishInput {
    pub(super) scope: RedoApplyScope,
    pub(super) undo: SpillableUndo,
}

/// What one step did.
#[derive(Debug)]
pub(in crate::data::executor::handlers::transaction::redo_apply::batched) enum StepOutcome {
    /// A slice ran, and the job has more to run.
    Ran,
    /// The job answered with this response.
    Done(Response),
}
