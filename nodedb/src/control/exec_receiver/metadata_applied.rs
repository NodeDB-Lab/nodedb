// SPDX-License-Identifier: BUSL-1.1

//! Answer a restoring node once this node applied the metadata log through
//! the index it names.

use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::rpc_codec::{ExecuteResponse, TypedClusterError};
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan};

use crate::control::state::SharedState;

use super::support::PLAN_DECODE_FAILED;

/// The answer to `plan` when it is a metadata-applied request, else `None`:
/// one empty payload once this node applied the metadata log through the
/// requested index, or an error when `budget` runs out first.
pub(super) async fn answer_applied_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
    budget: Duration,
) -> Option<ExecuteResponse> {
    let PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index }) = plan else {
        return None;
    };
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    let reached = match crate::control::metadata_proposer::wait::wait_applied(
        watcher, *index, budget,
    )
    .await
    {
        Ok(outcome) => outcome.is_reached(),
        Err(error) => {
            tracing::warn!(index = *index, %error, "metadata-applied wait did not finish");
            false
        }
    };
    Some(if reached {
        ExecuteResponse::ok(vec![Vec::new()], 0, Vec::new())
    } else {
        ExecuteResponse::err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: format!(
                "node {} did not apply the metadata log through index {index} in time",
                state.node_id
            ),
        })
    })
}
