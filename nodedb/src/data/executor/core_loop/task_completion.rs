// SPDX-License-Identifier: BUSL-1.1

//! The start and the end of one task's run on the core.
//!
//! [`CoreLoop::begin_run`] opens the task's journal before it executes.
//! [`CoreLoop::complete_run`] does what the task owes once it answered:
//! the applied note, the journal's end, the rollback fail-stop
//! ([`CoreLoop::finish_run`]), then the idempotency record and the response
//! ([`CoreLoop::answer_task`]). A task whose install parked finishes in the
//! tick that ends the install, inside that tick's unwind boundary, and
//! answers outside it.

use tracing::warn;

use crate::bridge::dispatch::{BridgeResponse, JournalGroup};
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::handlers::transaction::redo_apply::is_park_placeholder;

use super::super::task::{ExecutionTask, TaskState};
use super::CoreLoop;
use super::write_set_journal::RootJournalPermit;

/// What a task's run opened before it executes.
pub(in crate::data::executor) struct RunStart {
    /// The task journals its write set.
    pub(in crate::data::executor) journalled: bool,
    /// The root redo installer's journal permit.
    pub(in crate::data::executor) permit: Option<RootJournalPermit>,
}

impl CoreLoop {
    /// Mark `task` running and open the journal of its record group
    /// `journal`. A committed redo install may give the core back between
    /// its batches.
    pub(in crate::data::executor) fn begin_run(
        &mut self,
        task: &mut ExecutionTask,
        journal: Option<JournalGroup>,
    ) -> RunStart {
        task.state = TaskState::Running;
        self.grant_yield(task);
        let journalled = journal.is_some();
        let permit = journal.and_then(|group| self.begin_task_journal(task, group));
        RunStart { journalled, permit }
    }

    /// End `task`, which answered `resp`, and send the response.
    /// `journalled` is the task's [`RunStart::journalled`].
    pub(in crate::data::executor) fn complete_run(
        &mut self,
        mut task: ExecutionTask,
        journalled: bool,
        resp: Response,
    ) {
        let resp = self.finish_run(&mut task, journalled, resp);
        self.answer_task(&task, resp);
    }

    /// Everything [`Self::complete_run`] does but the answer: the applied
    /// note, the journal's end and the rollback fail-stop. Returns the
    /// response the task answers with.
    pub(in crate::data::executor) fn finish_run(
        &mut self,
        task: &mut ExecutionTask,
        journalled: bool,
        mut resp: Response,
    ) -> Response {
        debug_assert!(
            !is_park_placeholder(&resp),
            "a parked install's placeholder response reached its task's end"
        );
        // Every engine's next checkpoint, flush or manifest stamps what
        // this core applied, so a record applied here is noted whichever
        // engine applied it. A refused record carries a durable abort
        // marker instead, and a stamp never names it.
        if resp.status == Status::Ok
            && let Some(lsn) = task.wal_lsn()
        {
            self.floors.applied_prefix.note_applied(lsn);
        }
        // A crash test kills the process here: one collection's logged
        // write applied, and its response never leaves the core. A
        // committed redo matches each collection it writes. A task with
        // no WAL record never matches. A journalled write's sparse and
        // edge store effects are not durable yet here.
        #[cfg(feature = "failpoints")]
        if task.wal_lsn().is_some() {
            for collection in task.plan().named_collections() {
                crate::fail_point!(self.fail_scope, &format!("core::after_apply::{collection}"));
            }
        }
        if journalled {
            if let Err(error) = self.finish_journalled_task(task, &mut resp) {
                resp.status = Status::Error;
                resp.error_code = Some(Box::new(ErrorCode::from(error)));
            }
            // A crash test kills the process here: the write's write set
            // is stored and its parts are not journalled yet.
            #[cfg(feature = "failpoints")]
            for collection in task.plan().named_collections() {
                crate::fail_point!(
                    self.fail_scope,
                    &format!("core::after_capture::{collection}")
                );
            }
        }
        task.state = TaskState::Completed;
        // A failed rollback leaves this core's state unknown.
        self.fail_stop_on_rollback_failure(&resp);
        resp
    }

    /// Record `task`'s outcome for its idempotency key and send `response`.
    pub(in crate::data::executor) fn answer_task(
        &mut self,
        task: &ExecutionTask,
        response: Response,
    ) {
        if let Some(key) = task.request.idempotency_key {
            self.idempotency.record(key, response.status == Status::Ok);
        }

        // Bound the dangling-edge tracker across all tenants. Count
        // all entries; when the aggregate exceeds the cap, drop the
        // whole map — callers are tolerant of false negatives (an
        // `EdgePut` to a recently-deleted node races the tracker, so
        // the semantics are advisory regardless).
        let total: usize = self
            .deleted_nodes
            .values()
            .flat_map(|collections| collections.values())
            .map(|nodes| nodes.len())
            .sum();
        if total > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core = self.core_id, error = %e, "failed to send response — response queue full");
        }
    }
}
