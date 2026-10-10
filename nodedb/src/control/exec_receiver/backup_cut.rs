// SPDX-License-Identifier: BUSL-1.1

//! A backup's consistent cut on a remote source node.
//!
//! The coordinator of a backup picks the envelope watermark `W` and takes the
//! cut on its own replicas. A snapshot it sends to another node carries `W`.
//! That node takes the same cut on its replicas before it snapshots, so every
//! write committed below `W` has its final outcome in the snapshot, and every
//! write at or above `W` refuses a restore of the envelope.
//!
//! A database backup's request also carries a capture request. The node then
//! takes the cut with capturing barriers and answers with the captures it
//! parked, never with a snapshot of its live state.

use nodedb_cluster::rpc_codec::{ExecuteResponse, TypedClusterError};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::control::state::SharedState;

use super::support::{PLAN_DECODE_FAILED, execution_error_to_typed};

/// Take the cut a tenant snapshot plan asks for, then return the plan with
/// the request cleared. A capture request passes through unchanged for
/// [`answer_capture_plan`]. Every other plan passes through unchanged.
pub(super) async fn take_backup_cut(
    state: &std::sync::Arc<SharedState>,
    plan: PhysicalPlan,
) -> Result<PhysicalPlan, TypedClusterError> {
    let PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: Some(watermark),
        cut_capture: None,
        arrays,
    }) = plan
    else {
        return Ok(plan);
    };
    crate::control::backup::cut::cut_at(state, watermark)
        .await
        .map_err(execution_error_to_typed)?;
    Ok(PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: None,
        cut_capture: None,
        arrays,
    }))
}

/// Whether `plan` asks this node for a database backup's cut captures.
pub(super) fn is_capture_plan(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
            cut_capture: Some(_),
            ..
        })
    )
}

/// The answer to `plan` when it asks for a database backup's cut captures,
/// else `None`: this node takes the cut with capturing barriers and answers
/// with every capture it parked for the request, encoded for the wire.
pub(super) async fn answer_capture_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
) -> Option<ExecuteResponse> {
    let PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        cut_watermark,
        cut_capture: Some(request),
        ..
    }) = plan
    else {
        return None;
    };
    let Some(watermark) = *cut_watermark else {
        return Some(ExecuteResponse::err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "a cut capture request carries no cut watermark".into(),
        }));
    };
    let answer =
        crate::control::backup::cut_capture::collect::cut_and_reply(state, watermark, request)
            .await;
    Some(match answer {
        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, Vec::new()),
        Err(error) => ExecuteResponse::err(execution_error_to_typed(error)),
    })
}
