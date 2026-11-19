// SPDX-License-Identifier: BUSL-1.1

//! One slice of the parked stream install per tick.
//!
//! The slice runs with the install's ambient state back in the core, in the
//! span the install started in. A slice that leaves more to run takes the
//! state out again, and the job stays parked. The slice that answers ends
//! the task: its Calvin tail runs, and the task finishes as `run_task`
//! finishes every task. The answer and the fence's release, which runs
//! owed maintenance and puts the waiters back in the queue, follow.
//!
//! The slice, from the state's return through the task's finish, runs
//! inside one unwind boundary. A panic there stops the core and drops the
//! job, and the task answers that its install stopped part way. A core
//! that stops while the install is parked answers the same way. The
//! durable batches and the install cursor stay, and the next boot completes
//! the record forward. A job that stopped in its validate wrote nothing,
//! and the next boot replays the record from op 0.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

use super::super::install::job_stage::StepOutcome;
use super::parked::{InstallTail, ParkedInstall, PendingPark};
use super::start::unfinished;

/// How one slice of a parked install ended.
enum SliceEnd {
    /// The job has more to run.
    Parked(Box<PendingPark>),
    /// The task finished with this response.
    Answered(Response),
    /// The core fail-stopped part way, for this reason.
    Stopped(String),
}

impl CoreLoop {
    /// Run one slice of the parked install. Returns 1 when a slice ran or
    /// the install's task answered, else 0.
    pub(in crate::data::executor) fn drive_parked_install(&mut self) -> usize {
        self.drop_stale_parking();
        let Some(parked) = self.redo_apply.parked.take() else {
            return 0;
        };
        let ParkedInstall {
            park,
            mut task,
            journalled,
        } = *parked;
        let lsn = park.lsn;
        if let Some((_, detail)) = self.fail_stop.cause() {
            let detail = format!("the core fail-stopped while it was parked: {detail}");
            self.end_unfinished(&task, lsn, &detail);
            return 1;
        }
        #[cfg(feature = "failpoints")]
        if self.parked_slice_held(&park) {
            self.redo_apply.parked = Some(Box::new(ParkedInstall {
                park,
                task,
                journalled,
            }));
            return 0;
        }
        let span = park.span.clone();
        let ran = {
            let _entered = span.enter();
            catch_unwind(AssertUnwindSafe(|| {
                self.run_parked_slice(park, &mut task, journalled)
            }))
        };
        match ran {
            Ok(SliceEnd::Parked(park)) => {
                self.redo_apply.parked = Some(Box::new(ParkedInstall {
                    park: *park,
                    task,
                    journalled,
                }));
            }
            Ok(SliceEnd::Answered(response)) => {
                self.answer_task(&task, response);
                self.release_install_fence();
            }
            Ok(SliceEnd::Stopped(detail)) => self.end_unfinished(&task, lsn, &detail),
            Err(payload) => {
                let detail = self.stop_panicked_install(lsn, payload.as_ref());
                self.end_unfinished(&task, lsn, &detail);
            }
        }
        1
    }

    /// Run one slice of `park`, the install of `task`. The slice that
    /// answers finishes the task.
    fn run_parked_slice(
        &mut self,
        park: PendingPark,
        task: &mut ExecutionTask,
        journalled: bool,
    ) -> SliceEnd {
        let PendingPark {
            mut job,
            context,
            tail,
            lsn,
            reservation,
            span,
        } = park;
        // A test panics here: the install's state is still out of the core.
        crate::fail_point!(self.fail_scope, "redo_apply::batched::before_enter");
        if let Err(refused) = self.enter_install(context) {
            // The core fail-stopped. The state the refusal hands back goes
            // with the job.
            drop(refused.context);
            return SliceEnd::Stopped(refused.error.to_string());
        }
        let response = match job.step(self, task) {
            Ok(StepOutcome::Ran) => {
                // The judged view leaves the core with the apply scope.
                let resident = job
                    .resident_bytes(self.redo_apply.scope.as_ref())
                    .saturating_add(self.apply_scope.judged_resident_bytes());
                let context = match self.leave_install() {
                    Ok(context) => context,
                    Err(error) => return SliceEnd::Stopped(error.to_string()),
                };
                self.note_parked(&job, lsn);
                // The new charge replaces the slice before's.
                drop(reservation);
                let reservation = self.charge_parked(task, resident);
                return SliceEnd::Parked(Box::new(PendingPark {
                    job,
                    context,
                    tail,
                    lsn,
                    reservation,
                    span,
                }));
            }
            Ok(StepOutcome::Done(response)) => response,
            Err(code) => self.response_error(task, code),
        };
        let ts_installs = job.take_ts_installs();
        drop(job);
        drop(reservation);
        let response = match tail {
            InstallTail::Plain => response,
            InstallTail::Calvin(tail) => self.calvin_install_tail(response, &ts_installs, *tail),
        };
        self.end_parked_execution(task);
        SliceEnd::Answered(self.finish_run(task, journalled, response))
    }

    /// Stop the core when an install parked in a task that then panicked
    /// before `run_task` took it. The task is gone, and what the install
    /// changed is unknown.
    fn drop_stale_parking(&mut self) {
        let Some(stale) = self.redo_apply.parking.take() else {
            return;
        };
        let detail = format!(
            "the task of the batched install at lsn {} panicked after the install parked",
            stale.lsn.as_u64()
        );
        drop(stale);
        self.fail_stop_core(FailStopCause::InstallPanicked, &detail);
        self.release_install_fence();
    }

    /// Whether a test holds `park` before its next slice: the failpoint
    /// `redo_apply::batched::hold_parked::{point}`, or
    /// `redo_apply::batched::hold_rollback_parked::{point}` while it rolls
    /// back, is armed to wait for a file that does not exist yet. Other
    /// tasks run meanwhile.
    #[cfg(feature = "failpoints")]
    fn parked_slice_held(&self, park: &PendingPark) -> bool {
        use super::start::{ParkSite, park_fail_point};
        use crate::fail_point::{FailAction, lookup};

        let point = park_fail_point(park.job.park_point(), ParkSite::Held);
        matches!(
            lookup(self.fail_scope, &point),
            Some(FailAction::WaitForFile(path)) if !path.exists()
        )
    }

    /// Answer `task`, whose install at `lsn` stopped part way for `detail`
    /// on a core that fail-stopped, and release the fence. Its waiters
    /// return to the queue, which the stopped core refuses.
    fn end_unfinished(&mut self, task: &ExecutionTask, lsn: Lsn, detail: &str) {
        let response = self.response_error(task, unfinished(lsn, detail));
        self.answer_task(task, response);
        self.release_install_fence();
    }
}
