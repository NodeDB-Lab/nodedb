// SPDX-License-Identifier: BUSL-1.1

//! Capacity-safe Data-Plane dispatch for sequenced Calvin work.
//!
//! A sequenced txn has no refusal outcome: every replica must apply it. So
//! every scheduler dispatch goes through [`Scheduler::dispatch_sequenced`].
//! A capacity refusal parks the request in a FIFO. The txn keeps its locks
//! and its `pending` entry, and never reaches `on_txn_complete`. The run loop
//! re-sends parked requests once a routed response frees capacity. A terminal
//! refusal halts the scheduler (see [`super::halt`]).

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::bridge::dispatch::DispatchRefusal;
use crate::bridge::envelope::Request;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::RequestId;

/// The Calvin sub-operation one scheduler dispatch carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum DispatchStep {
    /// `CalvinExecuteStatic` stage of a static txn.
    StageStatic,
    /// `CalvinExecuteActive` stage of a dependent-read txn.
    StageActive,
    /// `CalvinResolve` of a committed staged txn.
    Resolve,
    /// `CalvinFlush` of a committed staged txn.
    Flush,
    /// `CalvinDrop` of an aborted staged txn.
    Drop,
    /// One-way `RecordCalvinWriteVersions` of a committed txn.
    WriteVersionRecord,
}

/// Result of [`Scheduler::dispatch_sequenced`].
#[derive(Debug)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum DispatchOutcome {
    /// The dispatcher accepted the request.
    Sent,
    /// The dispatcher refused at capacity. The request waits in the deferred
    /// FIFO, and the txn stays in flight.
    Deferred,
    /// The dispatcher refused terminally. Nothing is parked.
    Failed(crate::Error),
}

/// A request refused at capacity, waiting to be re-sent.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct DeferredDispatch {
    txn_id: TxnId,
    step: DispatchStep,
    request: Request,
}

/// FIFO of requests refused at capacity, in refusal order.
pub(in crate::control::cluster::calvin::scheduler::driver::core) type DeferredQueue =
    VecDeque<DeferredDispatch>;

/// Result of one send attempt, before the caller decides where a refused
/// request goes in the FIFO.
enum Attempt {
    Sent,
    Capacity(Box<DeferredDispatch>),
    Failed(crate::Error),
}

impl Scheduler {
    /// Register `request` in the tracker, dispatch it, and cancel the
    /// registration on refusal.
    ///
    /// A capacity refusal appends the request to the deferred FIFO and
    /// returns [`DispatchOutcome::Deferred`]. The caller keeps the txn in
    /// flight. Any other refusal returns [`DispatchOutcome::Failed`], and the
    /// caller runs its terminal handling.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_sequenced(
        &mut self,
        txn_id: TxnId,
        step: DispatchStep,
        request: Request,
    ) -> DispatchOutcome {
        match self.send_once(txn_id, step, request) {
            Attempt::Sent => DispatchOutcome::Sent,
            Attempt::Capacity(parked) => {
                self.deferred.push_back(*parked);
                self.metrics
                    .set_dispatch_deferred_depth(self.deferred.len());
                DispatchOutcome::Deferred
            }
            Attempt::Failed(error) => DispatchOutcome::Failed(error),
        }
    }

    /// Whether any refused request waits for capacity.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn has_deferred_dispatch(
        &self,
    ) -> bool {
        !self.deferred.is_empty()
    }

    /// Whether the run loop re-sends parked requests: some wait, and the
    /// scheduler has not halted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resends_deferred(
        &self,
    ) -> bool {
        self.has_deferred_dispatch() && !self.is_apply_halted()
    }

    /// Number of refused requests waiting for capacity.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn deferred_dispatch_len(
        &self,
    ) -> usize {
        self.deferred.len()
    }

    /// Re-send parked requests in FIFO order.
    ///
    /// Stops at the first capacity refusal, which goes back to the FIFO
    /// front. A terminal refusal runs the step's terminal handling, and stops
    /// the pass once the scheduler halted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn redispatch_deferred(
        &mut self,
    ) {
        while let Some(parked) = self.deferred.pop_front() {
            let DeferredDispatch {
                txn_id,
                step,
                mut request,
            } = parked;
            if step != DispatchStep::WriteVersionRecord && !self.pending.contains_key(&txn_id) {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?step,
                    "calvin: parked dispatch for a txn no longer pending; one step per txn is broken, discarding"
                );
                continue;
            }
            self.refresh_deferred_request(step, &mut request);
            match self.send_once(txn_id, step, request) {
                Attempt::Sent => {}
                Attempt::Capacity(parked) => {
                    self.deferred.push_front(*parked);
                    break;
                }
                Attempt::Failed(error) => {
                    self.fail_dispatch_step(txn_id, step, error);
                    if self.is_apply_halted() {
                        break;
                    }
                }
            }
        }
        self.metrics
            .set_dispatch_deferred_depth(self.deferred.len());
    }

    /// Run the terminal handling of a dispatch the dispatcher refused for a
    /// reason other than capacity.
    ///
    /// A stage, resolve, flush, or drop refusal halts the scheduler: the txn
    /// keeps its `pending` entry and locks, and its position stays unapplied.
    /// A refusal during shutdown holds the txn the same way. A write-version
    /// record is dropped with a warning, because the commit does not depend on
    /// it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn fail_dispatch_step(
        &mut self,
        txn_id: TxnId,
        step: DispatchStep,
        error: crate::Error,
    ) {
        let halt_step = match step {
            DispatchStep::StageStatic | DispatchStep::StageActive => HaltStep::Stage,
            DispatchStep::Resolve => HaltStep::Resolve,
            DispatchStep::Flush => HaltStep::Flush,
            DispatchStep::Drop => HaltStep::Drop,
            DispatchStep::WriteVersionRecord => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: write-version record dispatch failed"
                );
                return;
            }
        };
        self.halt_apply(
            txn_id,
            HaltReason::DispatchRefused,
            halt_step,
            error.to_string(),
        );
    }

    /// One send attempt: register, dispatch, and cancel on refusal.
    fn send_once(&mut self, txn_id: TxnId, step: DispatchStep, request: Request) -> Attempt {
        let request_id = request.request_id;
        let resp_rx = self.shared.tracker.register(request_id);
        let result = match self.shared.dispatcher.lock() {
            Ok(mut dispatcher) => dispatcher.try_dispatch(request),
            Err(poisoned) => poisoned.into_inner().try_dispatch(request),
        };
        let refusal = match result {
            Ok(()) => {
                self.on_dispatch_sent(txn_id, step, request_id, resp_rx);
                return Attempt::Sent;
            }
            Err(refusal) => refusal,
        };
        self.shared.tracker.cancel(&request_id);
        let DispatchRefusal { error, request } = *refusal;
        match error {
            crate::Error::DispatchCapacity { scope } => {
                self.metrics.record_dispatch_deferred();
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?step,
                    %scope,
                    "calvin: dispatch refused at capacity; deferred until capacity frees"
                );
                Attempt::Capacity(Box::new(DeferredDispatch {
                    txn_id,
                    step,
                    request,
                }))
            }
            other => Attempt::Failed(other),
        }
    }

    /// Per-step bookkeeping once the dispatcher accepts a request.
    fn on_dispatch_sent(
        &mut self,
        txn_id: TxnId,
        step: DispatchStep,
        request_id: RequestId,
        resp_rx: crate::control::ResponseReceiver,
    ) {
        match step {
            DispatchStep::StageStatic | DispatchStep::StageActive => {
                self.metrics.record_dispatch();
                if let Some(pending) = self.pending.get_mut(&txn_id) {
                    // no-determinism: dispatch_time is executor-latency observability, off-WAL
                    pending.dispatch_time = Instant::now();
                }
                self.spawn_response_bridge(txn_id, request_id, resp_rx);
            }
            DispatchStep::Resolve | DispatchStep::Flush | DispatchStep::Drop => {
                self.spawn_response_bridge(txn_id, request_id, resp_rx);
            }
            DispatchStep::WriteVersionRecord => {
                // One-way record: drain the response so it routes to a live
                // receiver, then discard it.
                tokio::spawn(async move {
                    let mut rx = resp_rx;
                    let _ = rx.recv().await;
                });
                self.shared
                    .calvin_counters
                    .write_versions_recorded
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Refresh the dispatch-time fields of a parked request before a re-send.
    ///
    /// The deadline restarts from now. A stage request re-reads group
    /// leadership, which the Data Plane uses to gate OLLP verification.
    fn refresh_deferred_request(&self, step: DispatchStep, request: &mut Request) {
        request.deadline = self.request_deadline();
        if !matches!(step, DispatchStep::StageStatic | DispatchStep::StageActive) {
            return;
        }
        if let PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic {
                is_group_leader, ..
            }
            | MetaOp::CalvinExecuteActive {
                is_group_leader, ..
            },
        ) = &mut request.plan
        {
            *is_group_leader = self.is_group_leader();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_cluster::calvin::types::SchedulerInput;
    use nodedb_types::TenantId;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::halt::HaltReason;
    use crate::control::cluster::calvin::scheduler::driver::core::intake::IntakeClosure;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        begin_data_plane_drain, build_test_scheduler_with_data_side, fill_tenant_inflight,
        make_validate_only_txn, test_coll_vshard,
    };

    /// A stage refused because the Data Plane drains stays pending and
    /// unapplied, keeps its locks, and closes intake. Shutdown sets no node
    /// marker.
    #[tokio::test]
    async fn stage_refused_while_draining_is_held_unapplied_without_node_marker() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        begin_data_plane_drain(&scheduler.shared);
        let txn_id = TxnId::new(3, 0);

        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));
        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(4, 0)));

        assert!(
            !scheduler.applied.is_applied(3, 0),
            "a terminal refusal must not mark the position applied"
        );
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "the refused txn keeps its pending entry"
        );
        assert!(
            scheduler.blocked.contains_key(&TxnId::new(4, 0)),
            "the refused txn keeps its key locks"
        );
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::Draining)
        );
        assert_eq!(scheduler.intake_closure(), Some(IntakeClosure::ApplyHalted));
        assert!(
            !scheduler.shared.sequencer_halt.apply_halt().is_halted(),
            "a shutdown halt sets no node marker"
        );
    }

    /// A parked stage whose re-send is refused terminally stays pending and
    /// unapplied, and the scheduler stops re-sending.
    #[tokio::test]
    async fn parked_stage_refused_terminally_on_resend_is_held_unapplied() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let txn_id = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));
        assert!(scheduler.has_deferred_dispatch(), "the stage parks");

        begin_data_plane_drain(&shared);
        scheduler.redispatch_deferred();

        assert!(!scheduler.applied.is_applied(3, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.is_apply_halted());
        assert!(!scheduler.resends_deferred());
    }
}
