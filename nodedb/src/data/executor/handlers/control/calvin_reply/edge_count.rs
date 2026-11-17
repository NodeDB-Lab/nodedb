// SPDX-License-Identifier: BUSL-1.1

//! The count an edge write answers when each endpoint home stages it.

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

use super::reply::CalvinStaging;
use crate::bridge::envelope::ErrorCode;

/// The count an edge write's staged reply answers on `vshard`.
///
/// An edge lives on both endpoint homes, and each home stages it. It counts
/// once, at its owner: its source endpoint's key home. The other home counts
/// none. The owned counts of a slice's edge writes add up, so the reply is
/// every edge this home owns. The coordinator of an edge-only statement sums
/// every home's reply (`ReplyFold::SumOwnedEdges`). Every edge of a batch
/// has one owner: the coordinator splits batches by home pair.
///
/// Any other plan's reply is returned as staged.
pub(super) fn owned_edge_count(
    vshard: crate::types::VShardId,
    plan: &PhysicalPlan,
    staged: Vec<u8>,
    staging: &mut CalvinStaging,
) -> Result<Vec<u8>, ErrorCode> {
    let Some(src) = edge_source(plan) else {
        return Ok(staged);
    };
    if staged.is_empty() {
        return Ok(staged);
    }
    let count = if crate::types::RecordHomes::edge_owner(src) == vshard {
        crate::control::server::shared::sql::staging_predicates::extract_affected_count(&staged)
            .ok_or_else(|| ErrorCode::Internal {
                detail: "a staged edge write answered no affected count".into(),
            })?
    } else {
        0
    };
    staging.owned_edges = staging.owned_edges.saturating_add(count);
    let total = usize::try_from(staging.owned_edges).map_err(|_| ErrorCode::Internal {
        detail: format!("{} owned edges do not fit a count", staging.owned_edges),
    })?;
    crate::data::executor::response_codec::encode_count("affected", total).map_err(|e| {
        ErrorCode::Internal {
            detail: format!("calvin edge count reply: {e}"),
        }
    })
}

/// The source endpoint of an edge write, which names its owner home. A
/// batch names its first edge's: every edge of a batch slice shares it.
fn edge_source(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Graph(
            GraphOp::EdgePut { src_id, .. } | GraphOp::EdgeDelete { src_id, .. },
        ) => Some(src_id.as_str()),
        PhysicalPlan::Graph(
            GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges },
        ) => edges.first().map(|edge| edge.src_id.as_str()),
        _ => None,
    }
}
