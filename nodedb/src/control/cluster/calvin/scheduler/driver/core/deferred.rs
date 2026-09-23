// SPDX-License-Identifier: BUSL-1.1

//! Capacity-safe Data-Plane dispatch for sequenced Calvin work.
//!
//! A sequenced txn has no refusal outcome: every replica must apply it. So
//! every scheduler dispatch goes through [`Scheduler::dispatch_sequenced`].
//! A capacity refusal parks the request in a FIFO. The txn keeps its locks
//! and its `pending` entry, and never reaches `on_txn_complete`. The run loop
//! re-sends parked requests once a routed response frees capacity.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;
use tokio::sync::mpsc;

use super::scheduler::Scheduler;
use crate::bridge::dispatch::DispatchRefusal;
use crate::bridge::envelope::{Request, Response};
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

    /// Number of refused requests waiting for capacity.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn deferred_dispatch_len(
        &self,
    ) -> usize {
        self.deferred.len()
    }

    /// Re-send parked requests in FIFO order.
    ///
    /// Stops at the first capacity refusal, which goes back to the FIFO
    /// front. A terminal refusal runs the step's terminal handling.
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
                Attempt::Failed(error) => self.fail_dispatch_step(txn_id, step, error),
            }
        }
        self.metrics
            .set_dispatch_deferred_depth(self.deferred.len());
    }

    /// Run the terminal handling of a dispatch the dispatcher refused for a
    /// reason other than capacity.
    ///
    /// Stage steps release the txn's locks. Resolve, flush, and drop steps
    /// complete the txn as an infra abort. A write-version record is dropped
    /// with a warning, because the commit does not depend on it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn fail_dispatch_step(
        &mut self,
        txn_id: TxnId,
        step: DispatchStep,
        error: crate::Error,
    ) {
        match step {
            DispatchStep::StageStatic | DispatchStep::StageActive => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?step,
                    %error,
                    "calvin scheduler: dispatch failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
            }
            DispatchStep::Resolve | DispatchStep::Flush | DispatchStep::Drop => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?step,
                    %error,
                    "calvin: commit resolution dispatch failed"
                );
                self.complete_infra_abort(txn_id);
            }
            DispatchStep::WriteVersionRecord => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: write-version record dispatch failed"
                );
            }
        }
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
        resp_rx: mpsc::Receiver<Response>,
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
