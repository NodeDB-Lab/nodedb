// SPDX-License-Identifier: BUSL-1.1

//! Every edge write runs as a Calvin transaction.
//!
//! The edge versions of a collection live in one ordering domain: the
//! Calvin sequence. An edge version is applied at its transaction's ordinal.
//! A TRUNCATE's cut is its own transaction's ordinal, and it hides every
//! version applied below it. So on every replica, and under any clock skew,
//! a TRUNCATE hides exactly the versions sequenced before it.
//!
//! A plain data-group Raft entry cannot give that. It carries no Calvin
//! ordinal, so its edge versions order against no TRUNCATE cut. It also
//! takes no Calvin lock, so a node delete's guard does not order against it.
//!
//! The Raft propose seams (`propose_replicated_entry`, `propose_sync_write`)
//! hand every edge write here. A session COMMIT that buffered an edge write
//! commits through Calvin. A RESTORE re-issues its edge versions as Calvin
//! transactions of its own, which carry the restore's mark (see
//! `backup::restore::redo_reissue`).

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::submit::submit_calvin_routed_write;
use super::tx_class::build_single_vshard_tx_class;
use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Whether `plan` writes an edge. Exhaustive over `GraphOp`: a new graph op
/// is a compile error here.
pub fn is_edge_write(plan: &PhysicalPlan) -> bool {
    let PhysicalPlan::Graph(op) = plan else {
        return false;
    };
    match op {
        GraphOp::EdgePut { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDeleteBatch { .. } => true,
        // Guards and a TRUNCATE's edge share run only inside a Calvin
        // transaction already. Label writes version no edge.
        GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. }
        | GraphOp::NodePresenceRead { .. } => false,
    }
}

/// Whether any task among `tasks` writes an edge.
pub fn writes_edges(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| is_edge_write(&task.plan))
}

/// One edge write and the scope it runs in.
pub struct EdgeWrite {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The vShard the write was addressed to. The transaction's participants
    /// are the edges' endpoint homes.
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    /// The source every participant stamps on the write's events.
    pub event_source: EventSource,
}

/// Run `write` as a single-write Calvin transaction and return its applied
/// response. A Data-Plane refusal comes back as `Err(Error::DataPlane)`, the
/// shape a proposed write returns.
///
/// Returns a boxed future: the Calvin submit can reach a Raft propose seam
/// that calls back here, and the box gives that cycle a finite size.
pub fn sequence_edge_write<'a>(
    state: &'a SharedState,
    write: EdgeWrite,
) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
    Box::pin(async move {
        let EdgeWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            event_source,
        } = write;
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let mut tx_class =
            build_single_vshard_tx_class(std::slice::from_ref(&task), tenant_id, &[])?;
        tx_class.set_event_source(event_source.wal_code());
        // Every participant of a graph-only transaction reports its answer
        // in its completion ack, so the count arrives on any node.
        let response = submit_calvin_routed_write(state, tx_class).await?;
        crate::control::local_dispatch::reject_data_plane_error(&response)?;
        Ok(response)
    })
}

/// Run the edge write `entry` carries as a Calvin transaction. `None` when
/// `entry` writes no edge: it keeps its Raft entry.
pub async fn sequence_replicated_edge_write(
    state: &SharedState,
    entry: &ReplicatedEntry,
) -> crate::Result<Option<Response>> {
    let Some(plan) = crate::control::wal_replication::decode::edge_write_plan(&entry.write)? else {
        return Ok(None);
    };
    let response = sequence_edge_write(
        state,
        EdgeWrite {
            tenant_id: TenantId::new(entry.tenant_id),
            database_id: DatabaseId::new(entry.database_id),
            vshard_id: VShardId::new(entry.vshard_id),
            plan,
            event_source: entry.event_source.into(),
        },
    )
    .await?;
    Ok(Some(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{QualifiedCollection, Surrogate};

    fn batch_edge() -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: "a".into(),
            label: "L".into(),
            dst_id: "b".into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    /// Single and batched edge writes take the Calvin route. A guard, a
    /// TRUNCATE share and a label write do not: they version no edge
    /// outside a Calvin transaction.
    #[test]
    fn every_edge_write_and_only_an_edge_write_is_sequenced() {
        let put = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: vec![batch_edge()],
        });
        let delete = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
            edges: vec![batch_edge()],
        });
        assert!(is_edge_write(&put));
        assert!(is_edge_write(&delete));
        let share = PhysicalPlan::Graph(GraphOp::TruncateEdges {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            vshard: 0,
        });
        let labels = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "a".into(),
            labels: vec!["L".into()],
        });
        assert!(!is_edge_write(&share));
        assert!(!is_edge_write(&labels));
    }
}
