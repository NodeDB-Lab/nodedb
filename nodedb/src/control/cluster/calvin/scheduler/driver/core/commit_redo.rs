// SPDX-License-Identifier: BUSL-1.1

//! Redo resolution for a committed Calvin slice on the data-group leader.
//!
//! Once the global verdict is COMMIT, the leader dispatches
//! `MetaOp::CalvinResolve` to reconstitute the staged post-images as one
//! replayable `RedoRecord`. It stamps the record with the slice's sequencer
//! position, attaches the transaction's messages and dedup key on the
//! participant that carries them, and proposes the record to the vShard's
//! data group (see [`super::redo_propose`]). Every replica, this one
//! included, installs it from the log.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::super::types::CommitState;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::halt::{HaltReason, HaltStep, error_response_text};
use super::redo_propose::OwedRedo;
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::CollectionIncarnation;
use crate::control::wal_replication::encode::{CalvinEntryStamps, RedoEntryTarget};
use crate::control::wal_replication::transaction_redo::{CalvinSlice, TransactionRedoPayload};
use crate::control::wal_replication::types::CalvinRedoMeta;
use crate::types::VShardId;
use crate::wal::{CalvinStamp, RedoRecord};

impl Scheduler {
    /// Handle the `MetaOp::CalvinResolve` response: decode the resolved
    /// `RedoRecord`, stamp it, attach the transaction's messages and dedup
    /// key on the participant that carries them, and propose it.
    ///
    /// The verdict is already COMMIT, so a skipped resolve tears the
    /// committed txn. A non-`Ok` response or a record that cannot be read
    /// halts the scheduler: the txn keeps its `pending` entry and locks, and
    /// its position stays unapplied.
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
        match self.owed_redo(txn_id, &response) {
            Ok(owed) => {
                if let Some(pending) = self.pending.get_mut(&txn_id) {
                    pending.redo = Some(owed);
                    pending.commit_state = CommitState::AwaitingRedoApply { proposed: None };
                }
                self.propose_calvin_redo(txn_id);
            }
            Err(error) => self.halt_apply(
                txn_id,
                HaltReason::ResolveFailed,
                HaltStep::Resolve,
                error.to_string(),
            ),
        }
    }

    /// The stamped redo the committed slice `txn_id` proposes, from its
    /// resolve answer `response`.
    fn owed_redo(&self, txn_id: TxnId, response: &Response) -> crate::Result<OwedRedo> {
        // The answer carries the slice's staged reply beside the record. Every
        // replica renders the reply after its install.
        let resolved = zerompk::from_msgpack::<nodedb_physical::physical_plan::CalvinResolved>(
            response.payload.as_bytes(),
        )
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("CalvinResolve answer: {e}"),
        })?;
        let mut redo = RedoRecord::from_bytes(&resolved.redo)?;
        let pending = self
            .pending
            .get(&txn_id)
            .ok_or_else(|| missing_pending_error(txn_id))?;
        let tx_class = &pending.txn.tx_class;
        // The stamp names the position every replica claims at install.
        redo.calvin_stamp = Some(CalvinStamp {
            epoch: txn_id.epoch,
            position: txn_id.position,
            vshard_id: self.vshard_id,
        });
        // The participants whose records carry the messages and the applied
        // key. A class whose write vShards cannot be derived fails.
        let publish_home = tx_class
            .publish_vshard()
            .map_err(|e| crate::Error::Internal {
                detail: format!("Calvin transaction write vShards underivable: {e}"),
            })?;
        let applied_key_home = tx_class
            .applied_key_home()
            .map_err(|e| crate::Error::Internal {
                detail: format!("Calvin transaction write vShards underivable: {e}"),
            })?;
        // One participant's record carries the messages the transaction's
        // trigger bodies published, so they commit once, with its writes.
        // Every replica stamps their position from the entry's log place.
        if publish_home == Some(self.vshard_id) {
            redo.publishes = crate::wal::RedoPublish::decode_all(&tx_class.publishes)?;
        }
        // One participant's record carries the dedup key of the cross-shard
        // request the transaction applies, so the key is durable exactly when
        // the request's writes are.
        if applied_key_home == Some(self.vshard_id) {
            let key =
                zerompk::from_msgpack::<crate::wal::CrossShardAppliedKey>(&tx_class.applied_key)
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("Calvin transaction applied key: {e}"),
                    })?;
            redo.cross_shard_applied = Some(key);
        }
        let scope = &pending.scope;
        let payload = TransactionRedoPayload::from_calvin(
            redo,
            CalvinSlice {
                collections: scope.collections.clone(),
                sum_targets: scope.sum_targets.clone(),
                identities: scope.identities.clone(),
                event_source: super::request::slice_event_source(tx_class, scope),
                origin: scope.origin,
                meta: CalvinRedoMeta {
                    epoch_system_ms: pending.txn.epoch_system_ms,
                    reply: resolved.reply,
                    primary_write: pending.has_primary_write,
                    user_write: pending.raises_write_mark,
                    returning: pending.has_returning,
                },
            },
        );
        let incarnations = scope
            .collections
            .iter()
            .map(|collection| CollectionIncarnation {
                collection: collection.clone(),
                incarnation: tx_class
                    .incarnations
                    .iter()
                    .find(|named| named.collection == *collection)
                    .map_or(nodedb_types::Hlc::ZERO, |named| named.incarnation),
            })
            .collect();
        Ok(OwedRedo {
            payload,
            stamps: CalvinEntryStamps {
                write_hlc: self
                    .cut_floors
                    .commit_hlc(pending.txn.epoch, pending.txn.epoch_system_ms),
                restore_id: tx_class.restore_id,
                metadata_floor: tx_class.metadata_floor,
                incarnations,
            },
            target: RedoEntryTarget {
                tenant_id: tx_class.tenant_id,
                database_id: tx_class.database_id,
                vshard_id: VShardId::new(self.vshard_id),
            },
            attempts: 0,
        })
    }

    /// Dispatch `MetaOp::CalvinResolve` to this vShard's core, registering a
    /// response bridge so the resolve response re-enters the completion loop
    /// under `CommitState::AwaitingRedoResolve`.
    ///
    /// A resolve reads the staged overlay and writes nothing, so no WAL LSN
    /// rides on it. A capacity refusal returns [`DispatchOutcome::Deferred`]:
    /// the resolve is parked for re-send and the txn stays in flight. A txn
    /// with no `pending` entry returns [`DispatchOutcome::Failed`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_calvin_resolve(
        &mut self,
        txn_id: TxnId,
    ) -> DispatchOutcome {
        let Some(pending) = self.pending.get(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let request_id = self.next_request_id();
        let plan = PhysicalPlan::Meta(MetaOp::CalvinResolve {
            epoch: txn_id.epoch,
            position: txn_id.position,
        });
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan);
        self.dispatch_sequenced(txn_id, DispatchStep::Resolve, request)
    }
}

/// The terminal error for a dispatch whose txn has no `pending` entry to
/// build the request from.
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
            "no redo is proposed"
        );
    }
}
