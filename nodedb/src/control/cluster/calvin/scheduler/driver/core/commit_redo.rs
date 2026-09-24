// SPDX-License-Identifier: BUSL-1.1

//! Redo-record resolution for a committed staged static Calvin transaction.
//!
//! A committed static Calvin dispatch stages its transaction on the Data
//! Plane (validate the read-set + buffer the plans, no base mutation). Once
//! the local commit vote is known (`resolve_staged_commit`), this module
//! drives the resolve step: dispatch `MetaOp::CalvinResolve` to reconstitute
//! the staged post-images as one replayable `RedoRecord`, WAL-append that
//! record (restoring restart durability for this vShard's slice of the
//! commit), then hand off to `dispatch_commit_resolution` for the flush that
//! `finish_resolved_commit` / `commit_apply_tail` complete.

use super::super::types::CommitState;
use super::commit_resolution_dispatch::CommitResolution;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::halt::{HaltReason, HaltStep, error_response_text};
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::server::dispatch_utils::MintedRecords;
use crate::types::VShardId;
use crate::wal::{CalvinStamp, RedoRecord};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Handle the `MetaOp::CalvinResolve` response: decode the resolved
    /// `RedoRecord`, WAL-append it (unless its op set is empty), then dispatch
    /// the flush that installs it, stamped with that record's LSN.
    ///
    /// The verdict is already COMMIT, so a skipped resolve would tear the
    /// committed txn on this replica. A non-`Ok` response, a decode failure,
    /// or a WAL-append failure halts the scheduler: the txn keeps its
    /// `pending` entry and locks, and its position stays unapplied.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_redo_resolve(
        &mut self,
        txn_id: TxnId,
        response: Response,
    ) {
        if response.status != Status::Ok {
            self.halt_apply(
                txn_id,
                HaltReason::ResolveFailed,
                HaltStep::Resolve,
                error_response_text("CalvinResolve", &response),
            );
            return;
        }

        let mut redo = match RedoRecord::from_bytes(response.payload.as_bytes()) {
            Ok(r) => r,
            Err(e) => {
                self.halt_apply(
                    txn_id,
                    HaltReason::ResolveFailed,
                    HaltStep::Resolve,
                    format!("CalvinResolve redo record decode failed: {e}"),
                );
                return;
            }
        };
        let Some(pending) = self.pending.get(&txn_id) else {
            // Txn state was reclaimed out from under us (should not happen —
            // locks are held until `on_txn_complete`); complete defensively.
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        // The stamp carries what the slice folds, so the live install and
        // restart replay fold at this record's LSN.
        redo.calvin_stamp = Some(CalvinStamp {
            epoch: txn_id.epoch,
            position: txn_id.position,
            vshard_id: self.vshard_id,
            collections: pending.flush_scope.collections.clone(),
            sum_targets: pending.flush_scope.sum_targets.clone(),
        });

        // The flush installs these exact bytes, the payload of the record
        // appended below.
        let redo_bytes = if redo.ops.is_empty() {
            Vec::new()
        } else {
            match redo.to_bytes() {
                Ok(bytes) => bytes,
                Err(e) => {
                    self.halt_apply(
                        txn_id,
                        HaltReason::ResolveFailed,
                        HaltStep::Resolve,
                        format!("CalvinResolve redo record encode failed: {e}"),
                    );
                    return;
                }
            }
        };

        // The record's outcome-floor window opens before the append. It stays
        // with the pending txn until the flush completes.
        let (redo_lsn, redo_records) = if redo.ops.is_empty() {
            (None, None)
        } else {
            let records = MintedRecords::open(&self.shared.outcome_floor);
            let appended = records
                .appender(&self.shared.wal, crate::wal::manager::NO_APPLY_KEY)
                .append_transaction_redo(
                    tenant_id,
                    VShardId::new(self.vshard_id),
                    database_id,
                    &redo,
                );
            match appended {
                Ok(lsn) => {
                    // The txn committed, so its redo record is never
                    // cancelled. The flush closes it from its outcome.
                    records.mark_sent();
                    (Some(lsn), Some(records))
                }
                Err(e) => {
                    // The txn stays pending and unapplied, and a failed append
                    // leaves no record for restart replay to reach.
                    records.settle();
                    self.halt_apply(
                        txn_id,
                        HaltReason::WalAppendFailed,
                        HaltStep::RedoAppend,
                        format!("TransactionRedo WAL append failed: {e}"),
                    );
                    return;
                }
            }
        };
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.redo_records = redo_records;
            pending.flush_scope.redo = redo_bytes;
        }

        // A flush refused at capacity is parked for re-send. The txn awaits its
        // flush response either way, so the state below is the same.
        if let DispatchOutcome::Failed(error) =
            self.dispatch_commit_resolution(txn_id, CommitResolution::Flush { redo_lsn })
        {
            self.fail_dispatch_step(txn_id, DispatchStep::Flush, error);
            return;
        }

        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = Some(CommitState::AwaitingResolve {
                committed: true,
                redo_lsn,
            });
        }
    }

    /// Dispatch `MetaOp::CalvinResolve` to this vShard's core, registering a
    /// response bridge so the resolve response re-enters the completion loop
    /// under `CommitState::AwaitingRedoResolve`.
    ///
    /// Mirrors `dispatch_commit_resolution`'s exempt, no-WAL-LSN dispatch
    /// shape — a resolve reads the staged overlay and writes nothing.
    ///
    /// A capacity refusal returns [`DispatchOutcome::Deferred`]: the resolve
    /// is parked for re-send and the txn stays in flight. A txn with no
    /// `pending` entry returns [`DispatchOutcome::Failed`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_calvin_resolve(
        &mut self,
        txn_id: TxnId,
    ) -> DispatchOutcome {
        let Some(pending) = self.pending.get(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let request_id = self.next_request_id();
        let plan = PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position });
        // A resolve reads the staged overlay only; it writes no WAL record
        // itself, so no committed LSN rides on this envelope.
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan, None);

        // The resolve response re-enters the completion loop under the SAME
        // txn_id, now in `AwaitingRedoResolve`, where `finish_redo_resolve` runs.
        self.dispatch_sequenced(txn_id, DispatchStep::Resolve, request)
    }
}

/// The terminal error for a commit-resolution dispatch whose txn has no
/// `pending` entry to build the request from.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn missing_pending_error(
    txn_id: TxnId,
) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "calvin txn {}/{} has no pending entry to dispatch from",
            txn_id.epoch, txn_id.position
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, scheduler_with_pending, staged_response,
    };

    /// A resolve that returns an error under a COMMIT verdict holds the txn
    /// unapplied and halts: skipping it would tear the committed txn.
    #[tokio::test]
    async fn resolve_error_response_holds_committed_txn_unapplied() {
        let txn_id = TxnId::new(8, 0);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoResolve);

        scheduler.finish_redo_resolve(
            txn_id,
            error_response(ErrorCode::Internal {
                detail: "resolve failed".to_string(),
            }),
        );

        assert!(!scheduler.applied.is_applied(8, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::ResolveFailed)
        );
        assert!(scheduler.shared.sequencer_halt.apply_halt().is_halted());
    }

    /// A resolve whose redo record does not decode holds the txn unapplied
    /// and halts.
    #[tokio::test]
    async fn undecodable_resolve_payload_holds_committed_txn_unapplied() {
        let txn_id = TxnId::new(8, 0);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoResolve);
        let mut response = staged_response(Status::Ok, None);
        response.payload = Payload::from_vec(vec![0xff, 0x00, 0x13]);

        scheduler.finish_redo_resolve(txn_id, response);

        assert!(!scheduler.applied.is_applied(8, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::ResolveFailed)
        );
        assert_eq!(
            scheduler.pending.get(&txn_id).and_then(|p| p.commit_state),
            Some(CommitState::AwaitingRedoResolve),
            "no flush is dispatched"
        );
    }
}
