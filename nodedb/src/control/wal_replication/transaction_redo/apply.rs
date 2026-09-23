// SPDX-License-Identifier: BUSL-1.1

//! Apply one committed transaction redo on this node.
//!
//! The one apply path for a committed redo record. The data-group apply loop
//! runs it for every committed `TransactionRedo` entry on every replica, the
//! proposer included; a node with no Raft runs it for its own commit. Either
//! way the write funnel appends the record to this node's WAL and dispatches
//! it to the owning core, and the fsync completes before the result returns.

use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::control::state::SharedState;
use crate::control::surrogate::bind_carried_identities;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::payload::TransactionRedoPayload;

/// Where a committed redo applies.
#[derive(Debug, Clone, Copy)]
pub struct RedoTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
}

/// Bind the redo's identities, then append and apply it on this node.
///
/// `apply_key` is the idempotency key of the Raft entry the redo comes from,
/// `0` on a node with no Raft. The redo record's header carries it. The
/// outcome carries the Data Plane's response verbatim, including an error
/// status.
pub(crate) async fn apply_transaction_redo(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
    apply_key: u64,
) -> crate::Result<SubmitOutcome> {
    bind_carried_identities(
        &state.surrogate_assigner,
        target.database_id,
        target.tenant_id,
        &payload.identities,
    )?;
    let plan = payload.apply_plan()?;
    submit_write(
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
            },
            // Raft fixed the order; a node with no Raft already validated the
            // commit and holds no gate for it.
            ordering: WriteOrdering::AlreadyOrdered,
            // A committed transaction publishes no Control-Plane change event;
            // its rows reach subscribers through the Data Plane's events.
            change_feed: ChangeFeedOwner::Unowned,
        },
    )
    .await
}
