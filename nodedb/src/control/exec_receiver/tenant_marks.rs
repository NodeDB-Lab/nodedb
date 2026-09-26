// SPDX-License-Identifier: BUSL-1.1

//! Answer a restoring node's request for this node's tenant write marks.
//!
//! The restoring node asks a replica of every data group it does not
//! replicate itself. This node answers only for groups it replicates, and only
//! once it applied every entry the groups committed before the request.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteResponse, TypedClusterError};

use crate::control::backup::restore::guard::{encode_marks, local_tenant_marks};
use crate::control::security::auth_fence::cluster::hosts_group;
use crate::control::state::SharedState;

use super::support::execution_error_to_typed;

/// This node's marks of `tenant_id` in `group_ids`, encoded for the wire.
///
/// A group this node does not replicate is refused with
/// [`TypedClusterError::NotLeader`] naming the group. The asking node then
/// picks another replica. The refusal carries the replica this node's routing
/// table names for the group, when it names one other than this node.
/// `budget` is what remains of the asking statement's deadline.
pub(super) async fn answer_tenant_marks(
    state: &Arc<SharedState>,
    tenant_id: u64,
    group_ids: &[u64],
    budget: Duration,
) -> ExecuteResponse {
    if let Some(&group_id) = group_ids.iter().find(|group| !hosts_group(state, **group)) {
        return ExecuteResponse::err(TypedClusterError::NotLeader {
            group_id,
            leader_node_id: replica_hint(state, group_id),
            leader_addr: None,
            term: 0,
        });
    }
    let deadline = tokio::time::Instant::now() + budget;
    let marks = match local_tenant_marks(state, tenant_id, group_ids, deadline).await {
        Ok(marks) => marks,
        Err(error) => return ExecuteResponse::err(execution_error_to_typed(error)),
    };
    match encode_marks(&marks) {
        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, 0),
        Err(error) => ExecuteResponse::err(execution_error_to_typed(error)),
    }
}

/// A replica of `group_id` other than this node, from this node's routing
/// table: the leader when it knows one, else the first voter, else the first
/// learner.
fn replica_hint(state: &SharedState, group_id: u64) -> Option<u64> {
    let routing = state.cluster_routing.as_ref()?;
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let info = routing.group_info(group_id)?;
    std::iter::once(info.leader)
        .chain(info.members.iter().copied())
        .chain(info.learners.iter().copied())
        .find(|&node| node != 0 && node != state.node_id)
}
