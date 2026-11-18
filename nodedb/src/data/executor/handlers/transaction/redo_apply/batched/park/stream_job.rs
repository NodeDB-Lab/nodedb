// SPDX-License-Identifier: BUSL-1.1

//! A parking stream install from its validate to its answer, run one slice
//! per step.
//!
//! The job opens validating, under the fence its task armed. Each validate
//! slice walks one batch and writes nothing, so a yield point there needs
//! no cursor: a crash leaves the record as it stood, and boot replays it
//! from op 0. The content header is the stream's first record, so the
//! slice that walks it fences the header's sum targets before any other
//! task runs. A header whose names the fence cannot key runs the rest of
//! the job to its end in that slice. Each slice also fences the collection
//! of every read it checked.
//!
//! The slice that finds the stream ended takes the verdict. A refusal
//! answers, and the caller releases the fence. Content that passed opens
//! the install job, whose first slice runs in the same step.

use std::collections::BTreeSet;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::super::super::state::{RedoApplyScope, TsInstalled};
use super::super::super::stream_validate::{StreamValidation, ValidateSlice};
use super::super::install::job::StreamInstallJob;
use super::super::install::job_stage::{ParkPoint, StepOutcome};
use super::owned::OwnedStream;
use super::start::sum_target_keys;

/// A parking stream install.
pub(in crate::data::executor::handlers::transaction::redo_apply::batched) enum StreamJob {
    /// The validate walks the stream.
    Validating(Box<ValidatingJob>),
    /// The validate passed, and the install runs.
    Installing(Box<StreamInstallJob>),
    /// The validate refused, and the job answered.
    Refused,
}

/// A stream install whose validate runs.
pub(in crate::data::executor::handlers::transaction::redo_apply::batched) struct ValidatingJob {
    stream: OwnedStream,
    validation: StreamValidation,
    /// The fence tried to key the header's sum targets.
    targets_fenced: bool,
    /// The job gives the core back between slices. `false` once a header
    /// sum target cannot be keyed.
    parks: bool,
}

impl StreamJob {
    /// Open the validate of `stream`, the install of `task`, before its
    /// first batch. `Err` when the stream cannot be read.
    pub(super) fn open(
        core: &CoreLoop,
        task: &ExecutionTask,
        stream: OwnedStream,
    ) -> Result<Self, ErrorCode> {
        let validation = core.open_stream_validate(&stream.validate_input(task))?;
        Ok(Self::Validating(Box::new(ValidatingJob {
            stream,
            validation,
            targets_fenced: false,
            parks: true,
        })))
    }

    /// Run one slice for `task`, the task that opened the job. `Err` when
    /// the job answered already.
    pub(super) fn step(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
    ) -> Result<StepOutcome, ErrorCode> {
        match std::mem::replace(self, Self::Refused) {
            Self::Validating(validating) => Ok(self.step_validating(core, task, validating)),
            Self::Installing(mut job) => {
                let outcome = job.step(core, task);
                *self = Self::Installing(job);
                outcome
            }
            Self::Refused => Err(ErrorCode::Internal {
                detail: "a stream install whose validate refused ran a step after it answered"
                    .into(),
            }),
        }
    }

    /// Hand over what each resolved timeseries batch the install stored.
    pub(super) fn take_ts_installs(&mut self) -> Vec<TsInstalled> {
        match self {
            Self::Installing(job) => job.take_ts_installs(),
            Self::Validating(_) | Self::Refused => Vec::new(),
        }
    }

    /// Where the job stands between slices.
    pub(super) fn park_point(&self) -> ParkPoint {
        match self {
            Self::Validating(validating) => ParkPoint::Validating {
                batch: validating.validation.last_batch(),
            },
            Self::Installing(job) => job.park_point(),
            Self::Refused => ParkPoint::Finish,
        }
    }

    /// The RAM the job holds between slices. While the arms run, the apply
    /// scope is `core_scope`, the one in the core.
    pub(super) fn resident_bytes(&self, core_scope: Option<&RedoApplyScope>) -> usize {
        match self {
            Self::Validating(validating) => validating
                .validation
                .resident_bytes()
                .saturating_add(validating.stream.held_bytes()),
            Self::Installing(job) => job.resident_bytes(core_scope),
            Self::Refused => 0,
        }
    }

    /// Walk the validate's next batch. A job that parks stops after one
    /// batch. The step that finds the stream ended takes the verdict.
    fn step_validating(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        mut validating: Box<ValidatingJob>,
    ) -> StepOutcome {
        loop {
            let input = validating.stream.validate_input(task);
            match core.step_stream_validate(&mut validating.validation, &input) {
                Ok(ValidateSlice::Ran) => {
                    validating.fence_header_targets(core, task);
                    validating.fence_checked_reads(core);
                    if validating.parks {
                        *self = Self::Validating(validating);
                        return StepOutcome::Ran;
                    }
                }
                Ok(ValidateSlice::Ended) => break,
                Err(code) => return StepOutcome::Done(core.response_error(task, code)),
            }
        }
        let ValidatingJob {
            mut stream,
            validation,
            parks,
            ..
        } = *validating;
        let validated = match core.finish_stream_validate(validation, &stream.validate_input(task))
        {
            Ok(validated) => validated,
            Err(code) => return StepOutcome::Done(core.response_error(task, code)),
        };
        let mut job = match StreamInstallJob::open(core, stream.install_input(task), validated) {
            Ok(job) => Box::new(job),
            Err(refusal) => return StepOutcome::Done(core.response_error(task, refusal)),
        };
        let outcome = loop {
            match job.step(core, task) {
                Ok(StepOutcome::Ran) if !parks => {}
                Ok(outcome) => break outcome,
                Err(code) => break StepOutcome::Done(core.response_error(task, code)),
            }
        };
        *self = Self::Installing(job);
        outcome
    }
}

impl ValidatingJob {
    /// Fence the collection of every read the slice checked. Each read is
    /// checked once, so a write to its row after the check would go unseen.
    /// Every slice checks the reads it walked, so the end of the stream
    /// leaves none to check.
    fn fence_checked_reads(&mut self, core: &mut CoreLoop) {
        let keys = self.validation.take_read_keys();
        if !keys.is_empty() {
            core.arm_install_fence(keys);
        }
    }

    /// Fence the sum targets the content header names, once a batch held
    /// it. A name the fence cannot key stops the job from parking: its
    /// report is filed.
    fn fence_header_targets(&mut self, core: &mut CoreLoop, task: &ExecutionTask) {
        if self.targets_fenced {
            return;
        }
        let Some(header) = self.validation.header() else {
            return;
        };
        self.targets_fenced = true;
        let mut targets = BTreeSet::new();
        if sum_target_keys(task.request.database_id, &header.sum_targets, &mut targets) {
            core.arm_install_fence(targets);
        } else {
            self.parks = false;
        }
    }
}
