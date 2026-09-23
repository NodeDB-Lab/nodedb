// SPDX-License-Identifier: BUSL-1.1

//! Apply halt for one Calvin scheduler.
//!
//! Every replica applies every sequenced txn. A position is marked applied
//! only after it applied on this replica, or after an abort that every
//! replica reaches identically. A replica-local infrastructure error is
//! neither: the txn's effect on this replica is unknown or missing. So the
//! scheduler halts and never marks that position applied.
//!
//! A halted scheduler:
//! - keeps the stuck txn unapplied, with its locks and its `pending` entry;
//! - closes intake with [`IntakeClosure::ApplyHalted`]. The fan-out then drops
//!   new input and arms catch-up, which keeps the sequencer log retained;
//! - stops the deferred re-send and the catch-up drain;
//! - keeps routing responses, verdicts, and promotions for other in-flight
//!   txns. Each one holds locks disjoint from the stuck txn, so its outcome
//!   does not depend on it. Peer vShards wait on its votes, and the per-position
//!   applied gate keeps restart replay exact. A later infrastructure error on
//!   another txn holds that txn the same way.
//!
//! The first cause wins. It logs one `error!` line, sets the
//! `nodedb_calvin_apply_halted` gauge, and records the node-wide
//! [`CalvinApplyHaltMarker`] that `/healthz` and the native status report.
//! While the node shuts down (the Data Plane drains), the scheduler holds the
//! same way, logs at info, and sets no node marker: the process is exiting.
//!
//! [`IntakeClosure::ApplyHalted`]: super::intake::IntakeClosure::ApplyHalted
//! [`CalvinApplyHaltMarker`]: crate::control::cluster::CalvinApplyHaltMarker

use tracing::{debug, error, info, warn};

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use crate::bridge::envelope::Response;
use crate::control::cluster::CalvinApplyHalt;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::apply_halt_reason;

/// Why a scheduler halted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum HaltReason {
    /// The node shuts down and its Data Plane drains.
    Draining,
    /// The dispatcher refused a request for a reason other than capacity.
    DispatchRefused,
    /// The executor response channel closed before a response arrived.
    ResponseDisconnected,
    /// The resolve of a committed txn failed or returned an undecodable record.
    ResolveFailed,
    /// The flush of a committed txn returned an error.
    FlushFailed,
    /// This replica failed to stage a txn whose verdict is COMMIT.
    LocalStageFailed,
    /// The surrogate catalog refused a coordinator-assigned identity.
    IdentityBindFailed,
    /// A redo or `CalvinApplied` WAL append failed.
    WalAppendFailed,
}

impl HaltReason {
    /// The `nodedb_calvin_apply_halted` reason index.
    fn metric_reason(self) -> usize {
        match self {
            Self::Draining => apply_halt_reason::DRAINING,
            Self::DispatchRefused => apply_halt_reason::DISPATCH_REFUSED,
            Self::ResponseDisconnected => apply_halt_reason::RESPONSE_DISCONNECTED,
            Self::ResolveFailed => apply_halt_reason::RESOLVE_FAILED,
            Self::FlushFailed => apply_halt_reason::FLUSH_FAILED,
            Self::LocalStageFailed => apply_halt_reason::LOCAL_STAGE_FAILED,
            Self::IdentityBindFailed => apply_halt_reason::IDENTITY_BIND_FAILED,
            Self::WalAppendFailed => apply_halt_reason::WAL_APPEND_FAILED,
        }
    }

    /// The reason label on the gauge and the node marker.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn label(
        self,
    ) -> &'static str {
        apply_halt_reason::LABELS[self.metric_reason()]
    }
}

/// The sub-operation of the stuck txn that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum HaltStep {
    /// The stage request, or the stage result under a COMMIT verdict.
    Stage,
    /// The resolve request of a committed txn.
    Resolve,
    /// The flush request of a committed txn.
    Flush,
    /// The drop request of an aborted txn.
    Drop,
    /// The direct apply of a txn with no commit state.
    Apply,
    /// The `TransactionRedo` WAL append.
    RedoAppend,
    /// The `CalvinApplied` WAL append.
    AppliedMarker,
    /// The surrogate identity binding before the stage dispatch.
    IdentityBind,
}

impl HaltStep {
    /// The step label on the node marker and the log line.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn label(
        self,
    ) -> &'static str {
        match self {
            Self::Stage => "stage",
            Self::Resolve => "resolve",
            Self::Flush => "flush",
            Self::Drop => "drop",
            Self::Apply => "apply",
            Self::RedoAppend => "redo_append",
            Self::AppliedMarker => "applied_marker",
            Self::IdentityBind => "identity_bind",
        }
    }

    /// The request a txn in `state` awaits a response for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn awaited_by(
        state: Option<CommitState>,
    ) -> Self {
        match state {
            Some(CommitState::Staged | CommitState::AwaitingVerdict) => Self::Stage,
            Some(CommitState::AwaitingRedoResolve) => Self::Resolve,
            Some(CommitState::AwaitingResolve {
                committed: true, ..
            }) => Self::Flush,
            Some(CommitState::AwaitingResolve {
                committed: false, ..
            }) => Self::Drop,
            None => Self::Apply,
        }
    }
}

/// The first halt of one scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct ApplyHalt {
    pub reason: HaltReason,
    /// The stuck txn, step, and error text, in the node marker's shape.
    pub report: CalvinApplyHalt,
}

/// First-cause-wins halt latch of one scheduler. Never clears.
#[derive(Debug, Default)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct HaltLatch {
    first: Option<ApplyHalt>,
}

/// Error text for an executor response that was not `Ok`.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn error_response_text(
    request: &str,
    response: &Response,
) -> String {
    format!(
        "{request} returned {:?} with error code {:?}",
        response.status,
        response.error_code.as_deref()
    )
}

impl Scheduler {
    /// Whether this scheduler halted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_apply_halted(
        &self,
    ) -> bool {
        self.halt.first.is_some()
    }

    /// The first halt, if this scheduler halted.
    #[cfg(test)]
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn apply_halt(
        &self,
    ) -> Option<&ApplyHalt> {
        self.halt.first.as_ref()
    }

    /// Hold `txn_id` unapplied and halt this scheduler.
    ///
    /// The caller leaves the txn's `pending` entry and locks in place and
    /// never calls `on_txn_complete` for it. A halt while the node shuts down
    /// records [`HaltReason::Draining`] whatever `reason` says. A second halt
    /// only logs: the first cause stays.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn halt_apply(
        &mut self,
        txn_id: TxnId,
        reason: HaltReason,
        step: HaltStep,
        error: String,
    ) {
        let reason = if self.node_shutting_down() {
            HaltReason::Draining
        } else {
            reason
        };
        if let Some(first) = &self.halt.first {
            if first.reason == HaltReason::Draining || reason == HaltReason::Draining {
                debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    step = step.label(),
                    %error,
                    "calvin scheduler: shutting down; holding txn unapplied"
                );
            } else {
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    reason = reason.label(),
                    step = step.label(),
                    %error,
                    halted_epoch = first.report.epoch,
                    halted_position = first.report.position,
                    "calvin scheduler: apply halted; holding another txn unapplied"
                );
            }
            return;
        }

        let report = CalvinApplyHalt {
            vshard_id: self.vshard_id,
            epoch: txn_id.epoch,
            position: txn_id.position,
            reason: reason.label(),
            step: step.label(),
            error,
        };
        self.metrics.set_apply_halted(reason.metric_reason());
        if reason == HaltReason::Draining {
            info!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                step = report.step,
                error = %report.error,
                "calvin scheduler: shutting down; holding txn unapplied and closing intake"
            );
        } else {
            error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                reason = report.reason,
                step = report.step,
                error = %report.error,
                "calvin scheduler: apply halted; txn held unapplied with its locks, \
                 intake closed until restart"
            );
            crate::diag::calvin_apply_halted(
                report.vshard_id,
                report.epoch,
                report.position,
                report.reason,
                report.step,
                &report.error,
            );
            self.shared
                .sequencer_halt
                .apply_halt()
                .record(report.clone());
        }
        self.halt.first = Some(ApplyHalt { reason, report });
    }

    /// Whether the node shuts down: the shutdown watch fired, or the
    /// dispatcher closed its Data Plane enqueue gate.
    fn node_shutting_down(&self) -> bool {
        if self.shared.shutdown.is_shutdown() {
            return true;
        }
        self.shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_data_plane_draining()
    }
}
