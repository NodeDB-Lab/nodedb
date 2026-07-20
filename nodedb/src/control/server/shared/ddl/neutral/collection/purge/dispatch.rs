// SPDX-License-Identifier: BUSL-1.1

//! Dispatch `MetaOp::UnregisterCollection` to the local Data Plane.
//!
//! Called from `catalog_entry::post_apply::async_dispatch::collection::purge_async`
//! on **every node** (leader and followers) so each node's Data
//! Plane reclaims its own local L1/L2 storage for the purged
//! collection symmetrically with the metadata row removal.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

/// Dispatch `MetaOp::UnregisterCollection { tenant_id, name, purge_lsn }`
/// to this node's Data Plane so `clear_collection_all_engines` reclaims
/// every engine's redb + versioned storage for the purged collection.
///
/// **Result-checked.** This is the correctness-critical half of a DROP:
/// the catalog row has already been removed at apply, so if the engine
/// purge does not actually succeed, engine rows survive behind a gone
/// catalog row (the resurrection hole on re-CREATE). The Data-Plane
/// handler is fail-closed and returns a `Status::Error` response on a
/// failed engine purge, but the shared `dispatch_to_data_plane` hands
/// that back as `Ok(response)` regardless of `response.status` (its
/// contract is used broadly and must not change). We therefore inspect
/// `response.status` HERE and turn an error response — or a transport
/// error, or a deadline — into a propagated `Err`. The caller records a
/// durable pending-reclaim entry so the purge is retried at-least-once
/// rather than lost to a warn log.
///
/// Idempotent: safe to re-dispatch after a partial or failed attempt.
pub async fn dispatch_unregister_collection(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
) -> crate::Result<()> {
    let tenant = TenantId::new(tenant_id);
    let database = DatabaseId::new(database_id);
    let vshard = VShardId::from_collection_in_database(database, name);
    let plan = PhysicalPlan::Meta(MetaOp::UnregisterCollection {
        tenant_id,
        name: name.to_string(),
        purge_lsn,
    });

    let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant,
        database,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await?;

    if response.status == Status::Error {
        let detail = match response.error_code {
            Some(code) => format!("{code:?}"),
            None => "engine purge returned Status::Error with no error_code".to_string(),
        };
        return Err(crate::Error::Storage {
            engine: "collection-purge".into(),
            detail: format!(
                "UnregisterCollection for tenant {tenant_id} collection '{name}' \
                 failed on the local Data Plane: {detail}"
            ),
        });
    }

    Ok(())
}
