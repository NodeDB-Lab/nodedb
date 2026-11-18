// SPDX-License-Identifier: BUSL-1.1

//! A stream install parked between its slices, and the task that waits for
//! it.
//!
//! An install parks inside its task's execution: its ambient core state
//! leaves the core, and its task answers with a placeholder that never
//! leaves the core. `run_task` then takes the task out of the run and
//! parks it beside the job, and `tick()` drives the job one slice at a
//! time. The task completes in the tick that ends the job.

use nodedb_mem::{EngineId, ReservationToken, ScopedMemory};
use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::install_context::InstallContext;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

use super::super::super::calvin_install::CalvinTail;
use super::stream_job::StreamJob;

/// The detail of the placeholder response a parked install's task answers
/// with inside its run.
const PARK_PLACEHOLDER: &str = "the batched install parked; its task answers when the install ends";

/// What a parked install's task owes once its install answered.
pub(in crate::data::executor::handlers::transaction::redo_apply) enum InstallTail {
    /// A plain committed redo: the install's response is the task's.
    Plain,
    /// A committed Calvin slice: its staged state ends, and its reply
    /// answers.
    Calvin(Box<CalvinTail>),
}

/// An install that parked in the task running now.
pub(in crate::data::executor) struct PendingPark {
    pub(super) job: StreamJob,
    /// The install's ambient core state, out of the core.
    pub(super) context: InstallContext,
    pub(in crate::data::executor::handlers::transaction::redo_apply) tail: InstallTail,
    /// The marker's LSN.
    pub(super) lsn: Lsn,
    /// The governor charge of the RAM the job holds between slices.
    pub(super) reservation: ReservationToken,
    /// The span the install started in. Every later slice runs in it.
    pub(super) span: tracing::Span,
}

/// A parked install and its task.
pub(in crate::data::executor) struct ParkedInstall {
    pub(super) park: PendingPark,
    pub(super) task: ExecutionTask,
    /// The task journals its write set.
    pub(super) journalled: bool,
}

/// Whether `response` is a parked install's placeholder.
pub(in crate::data::executor) fn is_park_placeholder(response: &Response) -> bool {
    matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::Internal { detail }) if detail == PARK_PLACEHOLDER
    )
}

impl CoreLoop {
    /// Let `task` park its stream install: `run_task` runs it, it applies
    /// a committed redo, and no other install is parked.
    pub(in crate::data::executor) fn grant_yield(&mut self, task: &ExecutionTask) {
        self.redo_apply.yield_grant = self.redo_apply.parked.is_none()
            && matches!(
                task.plan(),
                PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { .. })
            );
    }

    /// Take the grant [`Self::grant_yield`] gave the running task. Only the
    /// first install the task starts may park.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn take_yield_grant(
        &mut self,
    ) -> bool {
        std::mem::take(&mut self.redo_apply.yield_grant)
    }

    /// Park `task` beside the install that parked while it ran. Returns
    /// `task` when no install parked.
    pub(in crate::data::executor) fn park_yielded(
        &mut self,
        task: ExecutionTask,
        journalled: bool,
    ) -> Option<ExecutionTask> {
        self.redo_apply.yield_grant = false;
        let Some(park) = self.redo_apply.parking.take() else {
            return Some(task);
        };
        self.redo_apply.parked = Some(Box::new(ParkedInstall {
            park: *park,
            task,
            journalled,
        }));
        None
    }

    /// Whether an install is parked between its slices.
    pub(crate) fn has_parked_install(&self) -> bool {
        self.redo_apply.parked.is_some()
    }

    /// The response `task` answers with inside its run once its install
    /// parked. It never leaves the core.
    pub(super) fn park_placeholder(&self, task: &ExecutionTask) -> Response {
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: PARK_PLACEHOLDER.to_owned(),
            },
        )
    }

    /// Charge the governor for the `bytes` of RAM a parked job of `task`
    /// holds between slices.
    pub(super) fn charge_parked(&self, task: &ExecutionTask, bytes: usize) -> ReservationToken {
        ScopedMemory::new(
            self.governor.clone(),
            task.request.database_id,
            task.request.tenant_id,
            EngineId::Query,
        )
        .charge(bytes)
    }
}

#[cfg(test)]
impl CoreLoop {
    /// Where the parked install stands, `None` when none is parked.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn parked_point(
        &self,
    ) -> Option<super::super::install::job_stage::ParkPoint> {
        self.redo_apply
            .parked
            .as_ref()
            .map(|parked| parked.park.job.park_point())
    }

    /// The RAM the parked job holds by its own measure, `None` when none is
    /// parked. The arms' apply scope is out of the core while parked.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn parked_resident(
        &self,
    ) -> Option<usize> {
        self.redo_apply
            .parked
            .as_ref()
            .map(|parked| parked.park.job.resident_bytes(None))
    }
}
