// SPDX-License-Identifier: BUSL-1.1

//! Fail-stop of a core whose state is unknown.
//!
//! A core's state is unknown when a rollback fails part way, or when a
//! committed record installed and the work after its install failed: a
//! memtable flush that drained rows, a vector seal, or a truncate's removal.
//! A core also stops when a logged record's resolved rows cannot all land
//! on it: storing part of them diverges it from its peers.
//! Restart replay rebuilds the state from the WAL. Replay itself stops a core
//! that meets a committed record it cannot apply, and boot then refuses to
//! start.
//! Until then the core must not serve the state it holds.
//!
//! The first cause wins. It logs one ERROR, files one recorder report,
//! records the node-wide marker that
//! `/healthz`, the native `STATUS` and the `nodedb_data_plane_core_fail_stopped`
//! gauge read, and latches the core. A latched core refuses every request it
//! dequeues with `CoreFailStopped`: it applied nothing, so the funnel
//! cancels the request's record and another replica or a restart serves it.
//! A Calvin scheduler reads the code and halts instead of voting.
//! The latch never clears.

use tracing::{error, warn};

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::metrics::CoreFailStopReport;

use super::CoreLoop;

/// Why a core fail-stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum FailStopCause {
    /// A rollback of an undo log failed part way.
    RollbackFailed,
    /// A committed record installed, and the settle owed after its install
    /// failed. It cannot be rolled back.
    PostInstallFailed,
    /// A logged record's resolved rows cannot all land on this node. Every
    /// other node stores them, so skipping one diverges this node.
    CommittedRowUnfit,
    /// Restart replay met a committed record it cannot apply. The state past
    /// it has a hole, so replay stops there.
    ReplayRecordUnapplied,
    /// A journalled write's write set cannot be stored beside its
    /// effects, so a crash cannot settle its record group.
    WriteSetUnpersisted,
}

impl FailStopCause {
    fn label(self) -> &'static str {
        match self {
            Self::RollbackFailed => "rollback_failed",
            Self::PostInstallFailed => "post_install_failed",
            Self::CommittedRowUnfit => "committed_row_unfit",
            Self::ReplayRecordUnapplied => "replay_record_unapplied",
            Self::WriteSetUnpersisted => "write_set_unpersisted",
        }
    }
}

/// The fail-stop latch of one core. Never clears.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct CoreFailStop {
    cause: Option<(FailStopCause, String)>,
}

impl CoreFailStop {
    pub(in crate::data::executor) fn is_stopped(&self) -> bool {
        self.cause.is_some()
    }

    /// The cause and detail of the stop, if the core stopped.
    pub(in crate::data::executor) fn cause(&self) -> Option<(FailStopCause, &str)> {
        self.cause
            .as_ref()
            .map(|(cause, detail)| (*cause, detail.as_str()))
    }
}

impl CoreLoop {
    /// Whether this core fail-stopped. A stopped core also publishes no
    /// checkpoint and runs no maintenance: an artifact of its state would
    /// carry that state past the restart that must rebuild it.
    pub(crate) fn is_fail_stopped(&self) -> bool {
        self.fail_stop.is_stopped()
    }

    /// Stop this core from serving: its state is unknown. Only the first
    /// cause is kept and reported.
    pub(in crate::data::executor) fn fail_stop_core(&mut self, cause: FailStopCause, detail: &str) {
        if self.fail_stop.is_stopped() {
            return;
        }
        error!(
            core = self.core_id,
            cause = cause.label(),
            detail,
            "data plane core fail-stopped: its state is unknown, and it refuses every \
             request until a restart rebuilds it from the WAL"
        );
        crate::diag::data_plane_core_fail_stopped(self.core_id, cause.label(), detail);
        if let Some(metrics) = &self.metrics {
            metrics.core_fail_stops.record(CoreFailStopReport {
                core_id: self.core_id,
                cause: cause.label(),
                detail: detail.to_string(),
            });
        }
        self.fail_stop.cause = Some((cause, detail.to_string()));
    }

    /// Fail-stop the core when `response` reports a failed rollback.
    pub(in crate::data::executor) fn fail_stop_on_rollback_failure(&mut self, response: &Response) {
        if let Some(code) = response.error_code.as_deref() {
            self.fail_stop_on_rollback_code(code);
        }
    }

    /// Fail-stop the core when `code` is `RollbackFailed`. The logged detail
    /// names the undo entry, the reverse write, and its typed cause.
    pub(in crate::data::executor) fn fail_stop_on_rollback_code(&mut self, code: &ErrorCode) {
        if let ErrorCode::RollbackFailed {
            entry_index,
            detail,
            cause,
        } = code
        {
            let detail = match cause {
                Some(cause) => format!("undo entry {entry_index}: {detail}; cause: {cause:?}"),
                None => format!("undo entry {entry_index}: {detail}"),
            };
            self.fail_stop_core(FailStopCause::RollbackFailed, &detail);
        }
    }

    /// Refuse every queued request of a fail-stopped core. Returns how many
    /// were refused.
    pub(in crate::data::executor) fn refuse_queued_while_stopped(&mut self) -> usize {
        let Some((cause, _)) = self.fail_stop.cause() else {
            return 0;
        };
        let detail = format!(
            "core {} is fail-stopped ({}): its state is unknown until restart",
            self.core_id,
            cause.label()
        );
        let mut refused = 0;
        while let Some(task) = self.task_queue.pop_front() {
            self.drop_journal_group(task.request_id().as_u64());
            let response = self.response_error(
                &task,
                ErrorCode::CoreFailStopped {
                    core_id: self.core_id,
                    detail: detail.clone(),
                },
            );
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                warn!(core = self.core_id, error = %e, "failed to send a fail-stop refusal");
            }
            refused += 1;
        }
        refused
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    #[test]
    fn the_first_cause_wins() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert!(!core.fail_stop.is_stopped());

        core.fail_stop_core(FailStopCause::PostInstallFailed, "first");
        core.fail_stop_core(FailStopCause::RollbackFailed, "second");

        assert_eq!(
            core.fail_stop.cause(),
            Some((FailStopCause::PostInstallFailed, "first"))
        );
    }

    #[test]
    fn a_rollback_failure_response_stops_the_core_and_records_the_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.set_metrics(std::sync::Arc::clone(&metrics));
        let task = crate::data::executor::core_loop::tests::make_default_task();
        let response = core.response_error(
            &task,
            ErrorCode::RollbackFailed {
                entry_index: 2,
                detail: "restore failed".into(),
                cause: Some(Box::new(ErrorCode::Internal {
                    detail: "storage error (sparse): commit".into(),
                })),
            },
        );

        core.fail_stop_on_rollback_failure(&response);

        assert!(core.fail_stop.is_stopped());
        let (cause, detail) = core.fail_stop.cause().expect("stopped");
        assert_eq!(cause, FailStopCause::RollbackFailed);
        assert!(detail.starts_with("undo entry 2: restore failed; cause: Internal"));
        assert_eq!(metrics.core_fail_stops.stopped_cores(), 1);
        assert_eq!(
            metrics.core_fail_stops.report().map(|r| r.cause),
            Some("rollback_failed")
        );
    }

    #[test]
    fn a_response_without_a_rollback_failure_leaves_the_core_serving() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = crate::data::executor::core_loop::tests::make_default_task();
        let response = core.response_error(&task, ErrorCode::ConflictRetry);

        core.fail_stop_on_rollback_failure(&response);

        assert!(!core.fail_stop.is_stopped());
    }
}
