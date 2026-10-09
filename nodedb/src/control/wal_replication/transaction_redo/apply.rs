// SPDX-License-Identifier: BUSL-1.1

//! Apply one committed transaction redo on this node.
//!
//! The one apply path for a committed redo record. The data-group apply loop
//! runs it for every committed `TransactionRedo` entry on every replica, the
//! proposer included. The write funnel appends the record to this node's WAL
//! and dispatches it to the owning core, and the fsync completes before the
//! result returns.

use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, PendingWrite, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering,
    enqueue_write,
};
use crate::control::state::SharedState;
use crate::control::surrogate::bind_carried_identities;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use crate::wal::{PublishPosition, RedoPublish};

use super::payload::TransactionRedoPayload;

/// Where a committed redo applies.
#[derive(Debug, Clone, Copy)]
pub struct RedoTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
}

/// Record the cross-shard key a redo carries once the redo installed. The
/// same record in the WAL restores it after a crash between the two, and a
/// failed store write still answers for the key in memory until then.
pub(crate) fn record_cross_shard_key(
    state: &SharedState,
    payload: &TransactionRedoPayload,
    outcome: &SubmitOutcome,
) {
    if outcome.response.status != crate::bridge::envelope::Status::Ok {
        return;
    }
    if let (Some(key), Some(dedup)) = (
        payload.redo.cross_shard_applied.as_ref(),
        state.cross_shard_dedup.get(),
    ) && let Err(error) = dedup.record_applied(key)
    {
        tracing::error!(
            source_vshard = key.source_vshard,
            source_lsn = key.source_lsn,
            origin = %key.origin,
            error = %error,
            "cross-shard dedup key held in memory only; the WAL restores it on restart"
        );
    }
}

/// Bind the redo's identities, then append it and enqueue it on its core.
/// [`PendingWrite::finish`] collects the outcome.
///
/// `apply_key` is the idempotency key of the Raft entry the redo comes from.
/// The redo record's header carries it. `commit_hlc` is the entry's commit
/// stamp. `change_position` is the Raft log position of the entry. The redo's
/// `PUBLISH TO` messages are stamped with it before the record is appended,
/// so each message's event names its position on every replica and after a
/// WAL replay. `change_feed` names the feed that carries the redo's row
/// changes: the entry's group.
pub(crate) async fn enqueue_transaction_redo(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
    apply_key: u64,
    commit_hlc: Option<u64>,
    change_position: Option<crate::event::cdc::position::ReplicatedPosition>,
    change_feed: ChangeFeedOwner,
) -> crate::Result<PendingWrite> {
    bind_carried_identities(
        &state.surrogate_assigner,
        target.database_id,
        target.tenant_id,
        &payload.identities,
    )?;
    let plan = match change_position {
        Some(position) if !payload.redo.publishes.is_empty() => {
            let mut stamped = payload.clone();
            RedoPublish::stamp_all(
                &mut stamped.redo.publishes,
                PublishPosition {
                    partition: target.vshard_id.as_u32(),
                    epoch: position.epoch,
                    index: position.log_index,
                },
            );
            stamped.apply_plan()?
        }
        _ => payload.apply_plan()?,
    };
    enqueue_write(
        state,
        SubmitWrite {
            tenant_id: target.tenant_id,
            database_id: target.database_id,
            vshard_id: target.vshard_id,
            plan,
            trace_id: TraceId::generate(),
            event_source: payload.event_source,
            txn_id: None,
            // The proposer authorized every write before it resolved them.
            user_id: None,
            // Every replica appends the record to its own WAL, in apply order.
            durability: WalDurability::AppendHere {
                now_override: None,
                apply_key,
                commit_hlc,
                change_position,
            },
            // Raft fixed the order.
            ordering: WriteOrdering::AlreadyOrdered,
            // A committed transaction publishes its rows like autocommit
            // writes do, at the commit's position. A rolled-back transaction
            // never reaches this apply, so it publishes nothing.
            change_feed,
        },
    )
    .await
}
