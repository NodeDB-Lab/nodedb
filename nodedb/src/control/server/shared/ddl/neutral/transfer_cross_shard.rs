// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TRANSFER_ITEM(...)` between collections on two vShards.
//!
//! The move runs as one read-dependent Calvin transaction (see
//! `control::planner::calvin::read_dependent`): the destination write takes
//! the bytes the source vShard holds under the transaction's locks, and both
//! halves commit or neither does. The handler applies the gates a
//! same-vShard move passes: both collections' grants, the row-level
//! security injection, the clone-write check, and the descriptor write lease
//! held until the move finishes.
//!
//! Inside a transaction block the move is refused: its two halves cannot
//! stage into one vShard's overlay.

use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::planner::calvin::read_dependent::{
    CrossShardItemMove, move_item_across_shards,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

use super::super::result::{DdlError, DdlResult};
use super::kv_atomic::{error_to_ddl, single_text_col};
use super::read_gate::CollectionReadGate;

/// Run `plan`, a `KvOp::TransferItem` between `collections` (source, then
/// destination) whose vShards differ, as one Calvin transaction.
/// `source_vshard` is the source collection's vShard.
pub(super) async fn transfer_item_across_shards(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    txn_ctx: &DmlTxnCtx<'_>,
    mut plan: PhysicalPlan,
    collections: [&str; 2],
    source_vshard: VShardId,
) -> Result<Vec<DdlResult>, DdlError> {
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) != TransactionState::Idle {
        return Err(error_to_ddl(&crate::Error::CrossShardInExplicitTransaction));
    }
    let database_id = DatabaseId::DEFAULT;
    let gate = CollectionReadGate::for_request(state, identity, database_id);
    for collection in collections {
        gate.authorize(collection)?;
        gate.authorize_permission(collection, Permission::Write)?;
    }
    gate.inject_rls(&mut plan)?;

    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    let checked = intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task: PhysicalTask {
            tenant_id: identity.tenant_id,
            vshard_id: source_vshard,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        },
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await
    .map_err(|e| error_to_ddl(&e))?;
    let checked = match checked {
        CloneCheckedOutcome::Handled(response) => {
            let text = crate::data::executor::response_codec::decode_payload_to_json(
                response.payload.as_ref(),
            );
            return Ok(vec![single_text_col("transfer_item", text)]);
        }
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    // The lease holds every collection's descriptor until the move finished,
    // so a drain of either waits for it.
    let (authorized, _lease) = checked.into_parts();
    let PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection,
        dest_collection,
        item_key,
        dest_key,
        surrogate,
        source_rls_write_check,
        dest_rls_write_check: _,
    }) = authorized.into_physical_task().plan
    else {
        return Err(DdlError::internal(
            "TRANSFER_ITEM: the authorized task is not the item move",
        ));
    };
    let reply = serde_json::json!({
        "item_key": String::from_utf8_lossy(&item_key),
        "dest_key": String::from_utf8_lossy(&dest_key),
        "source_collection": source_collection.as_str(),
        "dest_collection": dest_collection.as_str(),
    });

    move_item_across_shards(
        state,
        identity,
        CrossShardItemMove {
            tenant_id: identity.tenant_id,
            database_id,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check,
        },
    )
    .await
    .map_err(|e| error_to_ddl(&e))?;

    let payload = nodedb_types::json_to_msgpack(&reply)
        .map_err(|e| DdlError::internal(format!("TRANSFER_ITEM reply: {e}")))?;
    let text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    Ok(vec![single_text_col("transfer_item", text)])
}
