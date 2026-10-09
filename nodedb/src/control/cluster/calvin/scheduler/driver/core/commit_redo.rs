// SPDX-License-Identifier: BUSL-1.1

//! Redo-record resolution for a committed staged static Calvin transaction.
//!
//! A committed static Calvin dispatch stages its transaction on the Data
//! Plane (validate the read-set + buffer the plans, no base mutation). Once
//! the local commit vote is known (`resolve_staged_commit`), this module
//! drives the resolve step: dispatch `MetaOp::CalvinResolve` to reconstitute
//! the staged post-images as one replayable `RedoRecord`, WAL-append that
//! record (restoring restart durability for this vShard's slice of the
//! commit), then queue the flush for its sequencer-order turn
//! ([`super::flush_turn`]); `finish_resolved_commit` / `commit_apply_tail`
//! complete it.

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
    /// `RedoRecord`, attach the transaction's messages on the participant that
    /// carries them, WAL-append it (unless it holds no op and no message),
    /// then dispatch the flush that installs it, stamped with that record's
    /// LSN.
    ///
    /// The verdict is already COMMIT, so a skipped resolve will tear the
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

        // The answer carries the slice's staged reply beside the record. The
        // flush renders the reply from the core's staged entry.
        let resolved = zerompk::from_msgpack::<nodedb_physical::physical_plan::CalvinResolved>(
            response.payload.as_bytes(),
        );
        let decoded = match resolved {
            Ok(resolved) => RedoRecord::from_bytes(&resolved.redo),
            Err(e) => Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("CalvinResolve answer: {e}"),
            }),
        };
        let mut redo = match decoded {
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
            // Txn state was reclaimed out from under us (must not happen —
            // locks are held until `on_txn_complete`); complete defensively.
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let event_source =
            super::request::slice_event_source(&pending.txn.tx_class, &pending.flush_scope);
        let commit_hlc = self
            .cut_floors
            .commit_hlc(pending.txn.epoch, pending.txn.epoch_system_ms);
        // The stamp carries what the slice folds, so the live install and
        // restart replay fold at this record's LSN.
        redo.calvin_stamp = Some(CalvinStamp {
            epoch: txn_id.epoch,
            position: txn_id.position,
            vshard_id: self.vshard_id,
            collections: pending.flush_scope.collections.clone(),
            sum_targets: pending.flush_scope.sum_targets.clone(),
        });
        // The participants whose records carry the messages and the applied
        // key. A class whose write vShards cannot be derived halts the resolve.
        let tx_class = &pending.txn.tx_class;
        let homes = tx_class
            .publish_vshard()
            .and_then(|publish| Ok((publish, tx_class.applied_key_home()?)));
        let (publish_home, applied_key_home) = match homes {
            Ok(homes) => homes,
            Err(e) => {
                self.halt_apply(
                    txn_id,
                    HaltReason::ResolveFailed,
                    HaltStep::Resolve,
                    format!("Calvin transaction write vShards underivable: {e}"),
                );
                return;
            }
        };
        // One participant's record carries the messages the transaction's
        // trigger bodies published, so they commit once, with its writes.
        if publish_home == Some(self.vshard_id) {
            match crate::wal::RedoPublish::decode_all(&pending.txn.tx_class.publishes) {
                Ok(mut publishes) => {
                    // Named by the transaction's sequencer position on its
                    // vShard's Calvin partition, the same on every replica.
                    crate::wal::RedoPublish::stamp_all(
                        &mut publishes,
                        crate::wal::PublishPosition {
                            partition: crate::event::cdc::position::calvin_partition(
                                self.vshard_id,
                            ),
                            epoch: 0,
                            index: txn_id.epoch,
                            base: u64::from(txn_id.position),
                        },
                    );
                    redo.publishes = publishes;
                }
                Err(e) => {
                    self.halt_apply(
                        txn_id,
                        HaltReason::ResolveFailed,
                        HaltStep::Resolve,
                        format!("Calvin transaction publishes decode failed: {e}"),
                    );
                    return;
                }
            }
        }
        // One participant's record carries the dedup key of the cross-shard
        // request the transaction applies, so the key is durable exactly when
        // the request's writes are.
        if applied_key_home == Some(self.vshard_id) {
            match zerompk::from_msgpack::<crate::wal::CrossShardAppliedKey>(
                &pending.txn.tx_class.applied_key,
            ) {
                Ok(key) => redo.cross_shard_applied = Some(key),
                Err(e) => {
                    self.halt_apply(
                        txn_id,
                        HaltReason::ResolveFailed,
                        HaltStep::Resolve,
                        format!("Calvin transaction applied key decode failed: {e}"),
                    );
                    return;
                }
            }
        }
        let installs_nothing =
            redo.ops.is_empty() && redo.publishes.is_empty() && redo.cross_shard_applied.is_none();

        // The flush installs these exact bytes, the payload of the record
        // appended below.
        let redo_bytes = if installs_nothing {
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
        let (redo_lsn, redo_records) = if installs_nothing {
            (None, None)
        } else {
            let records = MintedRecords::open(&self.shared.outcome_floor);
            // The flush reports no rows beyond the record's own: restart
            // replay folds the sum targets from the record's stamp. The record
            // is therefore whole at append, and no part follows it.
            let appended = records
                .appender(&self.shared.wal, crate::wal::manager::NO_APPLY_KEY)
                .with_event_source(event_source)
                .with_commit_hlc(commit_hlc)
                .append_whole_transaction_redo(
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
                    // Before the flush, so the position exists before any of
                    // the txn's change events reach the Event Plane.
                    self.shared.cdc_router.positions().record_calvin(
                        lsn.as_u64(),
                        crate::event::cdc::position::CalvinPosition {
                            sequencer_epoch: txn_id.epoch,
                            position: txn_id.position,
                        },
                    );
                    self.shared
                        .cdc_router
                        .positions()
                        .record_commit_hlc(lsn.as_u64(), commit_hlc);
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
            // The commit publishes the rows its redo installs, as a
            // data-group `TransactionRedo` apply does. A rolled-back
            // transaction never resolves, so it publishes nothing.
            pending.change_sets = if redo_bytes.is_empty() {
                Vec::new()
            } else {
                vec![crate::control::server::dispatch_utils::redo_change_set(
                    &redo_bytes,
                )]
            };
            pending.flush_scope.redo = redo_bytes;
        }

        // The flush runs in sequencer order, once every lower txn finished.
        self.queue_flush(txn_id, redo_lsn);
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
    use super::super::super::types::CommitState;
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, scheduler_with_pending, staged_response,
    };

    /// A resolve that returns an error under a COMMIT verdict holds the txn
    /// unapplied and halts: skipping it will tear the committed txn.
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
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRedoResolve),
            "no flush is dispatched"
        );
    }
}
