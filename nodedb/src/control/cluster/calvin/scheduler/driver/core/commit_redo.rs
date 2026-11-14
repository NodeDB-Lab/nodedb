// SPDX-License-Identifier: BUSL-1.1

//! Redo resolution for a committed Calvin slice on the data-group leader.
//!
//! Once the global verdict is COMMIT, the leader dispatches
//! `MetaOp::CalvinResolve` to reconstitute the staged post-images as the
//! parts of one replayable `RedoRecord`. It stamps the record's header with
//! the slice's sequencer position, attaches the transaction's messages and
//! dedup key on the participant that carries them, and proposes the record
//! to the vShard's data group as its parts arrive (see
//! [`super::redo_propose`] and `redo_stream`). Every replica, this one
//! included, installs it from the log.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::deferred::{DispatchOutcome, DispatchStep};
use super::halt::{HaltReason, HaltStep, error_response_text};
use super::redo_propose::OwedRedo;
use super::redo_stream::OwedBody;
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::CollectionIncarnation;
use crate::control::wal_replication::encode::{CalvinEntryStamps, RedoEntryTarget};
use crate::control::wal_replication::transaction_redo::{CalvinSlice, TransactionRedoPayload};
use crate::control::wal_replication::types::CalvinRedoMeta;
use crate::types::VShardId;
use crate::wal::{CalvinStamp, RedoRecord};
use nodedb_physical::physical_plan::CalvinResolvedPart;

impl Scheduler {
    /// Handle a `MetaOp::CalvinResolve` response: one part of the resolved
    /// redo. Part `0` builds the stamped redo the slice owes, with the
    /// transaction's messages and dedup key on the participant that carries
    /// them, and starts its proposal. A later part advances the stream.
    ///
    /// The verdict is already COMMIT, so a skipped resolve tears the
    /// committed txn. A non-`Ok` response or a record that cannot be read
    /// halts the scheduler: the txn keeps its `pending` entry and locks, and
    /// its position stays unapplied.
    ///
    /// No client input reaches either halt. A plan the resolve refuses never
    /// commits: it is refused before it is sequenced, or its stage votes
    /// abort. The messages and applied key `owed_redo` decodes were decoded
    /// before the class was sequenced.
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
        let taken = decode_part(&response).and_then(|answer| {
            if answer.part.index == 0 {
                self.open_owed_redo(txn_id, answer)
            } else {
                self.continue_owed_redo(txn_id, answer)
            }
        });
        if let Err(error) = taken {
            self.halt_apply(
                txn_id,
                HaltReason::ResolveFailed,
                HaltStep::Resolve,
                error.to_string(),
            );
        }
    }

    /// The stamped redo the committed slice `txn_id` owes, with the header
    /// fields alone and the staged `reply` part `0` answered. Every replica
    /// renders the reply after its install. A later attempt keeps the
    /// attempt count of the earlier one.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn owed_redo(
        &self,
        txn_id: TxnId,
        staged_reply: nodedb_physical::physical_plan::CalvinReplySpec,
    ) -> crate::Result<OwedRedo> {
        let mut redo = RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        };
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
            mirror_database: None,
        });
        // Every replica judges a role-guarded state transition against the
        // roles of the identity whose writes the transaction commits.
        redo.user_roles = tx_class.user_roles.clone();
        // The participants whose records carry the messages and the applied key.
        let publish_home = tx_class.publish_vshard();
        let applied_key_home = tx_class.applied_key_home();
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
        let reply = frozen_or_staged_reply(pending, staged_reply);
        let attempts = pending.redo.as_ref().map_or(0, |owed| owed.attempts);
        let payload = TransactionRedoPayload::from_calvin(
            redo,
            CalvinSlice {
                collections: scope.collections.clone(),
                incarnation_names: scope.incarnation_names.clone(),
                sum_targets: scope.sum_targets.clone(),
                identities: scope.identities.clone(),
                event_source: super::request::slice_event_source(tx_class, scope),
                origin: scope.origin,
                meta: CalvinRedoMeta {
                    epoch_system_ms: pending.txn.epoch_system_ms,
                    reply,
                    primary_write: pending.has_primary_write,
                    user_write: pending.raises_write_mark,
                    returning: pending.has_returning,
                },
            },
        );
        let incarnations = scope
            .incarnation_names
            .iter()
            .map(|(collection, kind)| CollectionIncarnation {
                collection: collection.clone(),
                incarnation: tx_class
                    .incarnations
                    .iter()
                    .find(|named| named.collection == *collection && named.kind == *kind)
                    .map_or(nodedb_types::Hlc::ZERO, |named| named.incarnation),
                kind: *kind,
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
            attempts,
            body: OwedBody::Streamed(None),
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
        // The open attempt's next part, or part 0 for a new attempt. A part
        // holds up to one entry's bytes, so a redo that fits one entry
        // arrives in part 0.
        let part = match pending.redo.as_ref().map(|owed| &owed.body) {
            Some(OwedBody::Streamed(Some(progress))) => progress.next_part(),
            Some(OwedBody::Streamed(None) | OwedBody::Inline) | None => 0,
        };
        let max_part_bytes = self.shared.redo_chunks.limits().max_entry_bytes as u64;
        let request_id = self.next_request_id();
        let plan = PhysicalPlan::Meta(MetaOp::CalvinResolve {
            epoch: txn_id.epoch,
            position: txn_id.position,
            part,
            max_part_bytes,
        });
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan);
        self.dispatch_sequenced(txn_id, DispatchStep::Resolve, request)
    }
}

/// The resolve part a `MetaOp::CalvinResolve` response carries.
fn decode_part(response: &Response) -> crate::Result<CalvinResolvedPart> {
    zerompk::from_msgpack::<CalvinResolvedPart>(response.payload.as_bytes()).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("CalvinResolve answer: {e}"),
        }
    })
}

/// The reply `pending`'s redo carries. A slice that holds a plan of the
/// txn's `RETURNING` contract voted only once its frozen reply was stored, so
/// it answers that stored reply, whatever its stage decided. Any other slice
/// answers as staged.
fn frozen_or_staged_reply(
    pending: &super::super::types::PendingTxn,
    staged: nodedb_physical::physical_plan::CalvinReplySpec,
) -> nodedb_physical::physical_plan::CalvinReplySpec {
    let holds_contract = pending
        .txn
        .tx_class
        .returning
        .as_ref()
        .is_some_and(|contract| {
            contract
                .plans
                .iter()
                .any(|plan| pending.returning.plan_indexes.contains(&plan.plan_index))
        });
    if holds_contract {
        nodedb_physical::physical_plan::CalvinReplySpec::Frozen
    } else {
        staged
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
