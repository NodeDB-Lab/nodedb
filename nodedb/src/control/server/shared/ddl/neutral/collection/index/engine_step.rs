// SPDX-License-Identifier: BUSL-1.1

//! Propose one index engine step through the collection's data group.
//!
//! An engine step builds or removes the entries of a secondary index: a
//! document index build or purge, or a key-value index register or drop.
//! The step goes through the durable autocommit route, which proposes it
//! through Raft. Every replica of the collection's vShard applies it in log
//! order, and each replica's write funnel appends its WAL record. Restart
//! replay runs the step again at that record.
//!
//! The proposal carries this node's metadata floor. A replica applies the
//! step only once its own catalog reached the catalog this node planned the
//! step against, so the index's registration on that replica comes first.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{AutocommitWrite, dispatch_durable_autocommit_write};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::super::result::DdlError;

/// Propose `plan` on the vShard of `collection` and wait until this node
/// applied it. A refusal keeps its SQLSTATE and code, so a UNIQUE duplicate
/// is `23505`. `context` names the step in every error message.
pub(super) async fn propose_index_step(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    context: &str,
) -> Result<Vec<u8>, DdlError> {
    dispatch_durable_autocommit_write(
        state,
        AutocommitWrite {
            tenant_id,
            database_id,
            vshard_id: nodedb_types::CollectionKey::from_bare(database_id, collection).vshard(),
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
            // Index maintenance is a system write.
            user_roles: Vec::new(),
        },
    )
    .await
    .and_then(crate::control::server::shared::response_payload::payload_or_typed_error)
    .map_err(|e| DdlError::from_error_in_context(context, &e))
}
