// SPDX-License-Identifier: BUSL-1.1

//! Transaction staging-overlay teardown on the current vShard leader.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::gateway::RouteDecision;
use crate::control::gateway::retry::retry_not_leader;
use crate::control::state::SharedState;

use super::leader_forward::{forward_to_leader, resolve_leader};
use super::outcome::TxnDataPlane;
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;

/// Release a transaction's staging overlay on the vShard's current leader.
pub(super) async fn drop_txn_overlay(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    txn_id: crate::types::TxnId,
) -> crate::Result<()> {
    let drop_plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id });
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: drop_plan.clone(),
        post_set_op: PostSetOp::None,
        txn_id: Some(txn_id),
    };
    match resolve_leader(&task, state) {
        RouteDecision::Local => {
            dp.dispatch_no_wal(task).await?;
            Ok(())
        }
        _ => {
            let shared = gateway_shared(state)?;
            let routing_ref = shared.cluster_routing.as_deref();
            retry_not_leader(routing_ref, |_attempt| {
                let task = task.clone();
                let drop_plan = drop_plan.clone();
                async move {
                    match resolve_leader(&task, state) {
                        RouteDecision::Local => dp.dispatch_no_wal(task).await,
                        remote => forward_to_leader(state, remote, task, &drop_plan).await,
                    }
                }
            })
            .await
            .map(|_| ())
        }
    }
}
