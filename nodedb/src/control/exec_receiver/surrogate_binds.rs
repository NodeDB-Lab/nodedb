// SPDX-License-Identifier: BUSL-1.1

//! Answer a backup or MOVE TENANT coordinator's request for this node's
//! PK→surrogate binds with a home among the vShards this node is the source
//! for.

use std::collections::HashSet;

use nodedb_cluster::rpc_codec::ExecuteResponse;
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan};

use crate::control::backup::bind_capture::{encode_binds, local_binds};
use crate::control::backup::restore::bind_conflicts::{encode_holders, local_holders};
use crate::control::state::SharedState;

use super::support::execution_error_to_typed;

/// The answer to `plan` when it is a surrogate bind request, else `None`:
/// this node's binds of the tenant's collections with a home among the
/// requested vShards, encoded for the wire.
pub(super) fn answer_binds_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
) -> Option<ExecuteResponse> {
    let PhysicalPlan::ClusterEvent(ClusterEventOp::SurrogateBinds {
        tenant_id,
        database_id,
        vshards,
        collections,
    }) = plan
    else {
        return None;
    };
    let vshards: HashSet<u32> = vshards.iter().copied().collect();
    let encoded =
        local_binds(state, *tenant_id, *database_id, &vshards, collections).and_then(encode_binds);
    Some(match encoded {
        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, Vec::new()),
        Err(error) => ExecuteResponse::err(execution_error_to_typed(error)),
    })
}

/// The answer to `plan` when it is a surrogate holders request, else `None`:
/// the key this node binds each requested `(collection, surrogate)` to,
/// encoded for the wire.
pub(super) fn answer_holders_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
) -> Option<ExecuteResponse> {
    let PhysicalPlan::ClusterEvent(ClusterEventOp::SurrogateHolders {
        tenant_id,
        database_id,
        entries,
    }) = plan
    else {
        return None;
    };
    let encoded = local_holders(state, *tenant_id, *database_id, entries).and_then(encode_holders);
    Some(match encoded {
        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, Vec::new()),
        Err(error) => ExecuteResponse::err(execution_error_to_typed(error)),
    })
}
