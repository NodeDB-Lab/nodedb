// SPDX-License-Identifier: BUSL-1.1

//! A backup's consistent cut on a remote source node.
//!
//! The coordinator of a backup picks the envelope watermark `W` and takes the
//! cut on its own replicas. A snapshot it sends to another node carries `W`.
//! That node takes the same cut on its replicas before it snapshots, so every
//! write committed below `W` has its final outcome in the snapshot, and every
//! write at or above `W` refuses a restore of the envelope.

use nodedb_cluster::rpc_codec::TypedClusterError;
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::control::state::SharedState;

use super::support::execution_error_to_typed;

/// Take the cut a tenant snapshot plan asks for, then return the plan with
/// the request cleared. Every other plan passes through unchanged.
pub(super) async fn take_backup_cut(
    state: &std::sync::Arc<SharedState>,
    plan: PhysicalPlan,
) -> Result<PhysicalPlan, TypedClusterError> {
    let PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: Some(watermark),
    }) = plan
    else {
        return Ok(plan);
    };
    crate::control::backup::cut::cut_at(state, tenant_id, watermark)
        .await
        .map_err(execution_error_to_typed)?;
    Ok(PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: None,
    }))
}
