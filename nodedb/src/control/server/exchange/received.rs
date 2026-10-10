// SPDX-License-Identifier: BUSL-1.1

//! Run a plan that an `ExecuteRequest` carried to this node.
//!
//! A transaction meta-op (`StageWrite`, `ResolveTxn`, `DropTxnOverlay`, ...)
//! runs on the one core owning its vShard, because only that core holds the
//! transaction's staging overlay. Fanning it to every core stages the write on
//! each of them and returns a non-owning core's answer. Every other plan fans
//! across all local cores.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::router::is_task_vshard_scoped;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId, VShardId};

use super::all_cores::{NodeLevelResult, execute_plan_all_local_cores};
use super::owning_core::dispatch_single_owning_core;

/// Where a received plan runs on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceivedRoute {
    /// The one core owning this vShard.
    OwningCore(VShardId),
    /// Every local core, merged.
    AllCores,
}

/// Decide where a received plan runs.
///
/// The request's vShard must match the plan's scope. A scoped plan without a
/// vShard will fan to every core. A vShard on any other plan means the
/// sender and receiver disagree on the plan's scope.
fn route_received_plan(
    plan: &PhysicalPlan,
    scoped_vshard: Option<VShardId>,
) -> crate::Result<ReceivedRoute> {
    match (is_task_vshard_scoped(plan), scoped_vshard) {
        (true, Some(vshard_id)) => Ok(ReceivedRoute::OwningCore(vshard_id)),
        (false, None) => Ok(ReceivedRoute::AllCores),
        (true, None) => Err(crate::Error::Internal {
            detail: "execute request: a vShard-scoped plan arrived without its vShard".into(),
        }),
        (false, Some(vshard_id)) => Err(crate::Error::Internal {
            detail: format!(
                "execute request: vShard {} arrived with a plan that is not vShard-scoped",
                vshard_id.as_u32()
            ),
        }),
    }
}

/// Run a received plan on the core or cores [`route_received_plan`] picks.
pub(crate) async fn execute_received_plan(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    scoped_vshard: Option<VShardId>,
) -> crate::Result<NodeLevelResult> {
    match route_received_plan(&plan, scoped_vshard)? {
        ReceivedRoute::OwningCore(vshard_id) => {
            let resp = dispatch_single_owning_core(
                state,
                tenant_id,
                database_id,
                plan,
                vshard_id,
                trace_id,
                txn_id,
            )
            .await?;
            Ok(NodeLevelResult {
                not_found: crate::control::local_dispatch::is_not_found(&resp),
                payload: resp.payload.to_vec(),
                watermark_lsn: resp.watermark_lsn,
                read_versions: resp.read_versions,
            })
        }
        ReceivedRoute::AllCores => {
            execute_plan_all_local_cores(state, tenant_id, database_id, plan, trace_id, txn_id)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{KvOp, MetaOp};
    use nodedb_types::QualifiedCollection;

    fn kv_put() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    #[test]
    fn a_scoped_request_reaches_exactly_its_owning_core() {
        let stage = PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(kv_put()),
        });
        assert_eq!(
            route_received_plan(&stage, Some(VShardId::new(41))).unwrap(),
            ReceivedRoute::OwningCore(VShardId::new(41))
        );
        let resolve = PhysicalPlan::Meta(MetaOp::ResolveTxn {
            txn_id: TxnId::new(3),
            plans: vec![kv_put()],
        });
        assert_eq!(
            route_received_plan(&resolve, Some(VShardId::new(7))).unwrap(),
            ReceivedRoute::OwningCore(VShardId::new(7))
        );
    }

    #[test]
    fn an_unscoped_request_fans_across_every_core() {
        assert_eq!(
            route_received_plan(&kv_put(), None).unwrap(),
            ReceivedRoute::AllCores
        );
    }

    #[test]
    fn a_scoped_plan_without_its_vshard_is_refused() {
        let drop = PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
            txn_id: TxnId::new(3),
        });
        assert!(matches!(
            route_received_plan(&drop, None),
            Err(crate::Error::Internal { .. })
        ));
    }

    #[test]
    fn a_vshard_on_an_unscoped_plan_is_refused() {
        assert!(matches!(
            route_received_plan(&kv_put(), Some(VShardId::new(1))),
            Err(crate::Error::Internal { .. })
        ));
    }
}
