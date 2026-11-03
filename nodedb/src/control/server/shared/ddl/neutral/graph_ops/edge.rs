// SPDX-License-Identifier: BUSL-1.1

//! Node label handlers (GRAPH LABEL / GRAPH UNLABEL) and the response and
//! home helpers the edge writes share. `GRAPH INSERT EDGE` is in
//! `edge_insert`, `GRAPH DELETE EDGE` in `edge_delete`.
//!
//! Each function receives already-parsed typed fields; handlers never touch
//! `&str` parse paths.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::GraphOp;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{data_plane_verdict, ddl_err};

/// Read the affected count off a Data-Plane response. A missing count is an
/// error, never a default.
pub(super) fn response_affected(
    response: &crate::bridge::envelope::Response,
) -> Result<u64, DdlError> {
    require_affected_count(response.payload.as_bytes()).map_err(|e| {
        DdlError::from_error_in_context("edge write response is missing its affected count", &e)
    })
}

/// The home vShard(s) an edge resolves to: `vsrc` holds the forward row, `vdst`
/// the reverse row. `single_home` is true when both share one vShard.
/// Bundled for [`stage_edge_dual_home`](super::edge_stage::stage_edge_dual_home).
pub struct EdgeHomes {
    pub vsrc: VShardId,
    pub vdst: VShardId,
    pub single_home: bool,
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    node_id: String,
    labels: Vec<String>,
    remove: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    if node_id.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH LABEL/UNLABEL requires a quoted node id",
        ));
    }
    if labels.is_empty() {
        return Err(ddl_err("42601", "missing AS '<label>' [, '<label2>']"));
    }

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(node_id.as_bytes());

    let plan = if remove {
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels })
    } else {
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels })
    };

    // Single-keyed on `node_id`, so single-home: route to `from_key(node_id)`.
    // The propose seam runs the write as a Calvin transaction, so every
    // replica writes the label version at the transaction's ordinal.
    let response =
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
            state,
            tenant_id,
            database_id,
            vshard_id,
            plan,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    data_plane_verdict(&response)?;

    let tag = if remove { "UNLABEL" } else { "LABEL" };
    Ok(vec![DdlResult::Status {
        command: tag.to_string(),
        rows_affected: Some(response_affected(&response)?),
    }])
}
