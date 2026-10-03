// SPDX-License-Identifier: BUSL-1.1

//! Native graph reads served where the graph's partitions live.
//!
//! A graph algorithm reads every partition of the collection's graph, a
//! one-hop neighbors read reads one node's edges, a hop, path or subgraph
//! walk crosses partitions hop by hop, and a RAG fusion reads the collection's
//! indexes on its owner and its edges everywhere. Routed by collection, each
//! will read a single node's share. Each runs through the same coordinators
//! as the SQL surface (`graph_dispatch::whole_graph`,
//! `graph_dispatch::keyed_read`, `graph_dispatch::walk_reads`,
//! `graph_dispatch::rag_fusion`). Every running server has cluster routing,
//! the synthesized one-node cluster included, so a walk and a fusion run
//! through these coordinators on every server. A walk answers the payload
//! shape the Data Plane gives, and `response_shape::walk` turns its node
//! names into rows. The native protocol has no read-consistency setting, so
//! every one of these reads is strong.

use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::ok_payload_response;
use crate::types::VShardId;

use super::response::data_plane_response_to_native;
use super::{DispatchCtx, error_to_native};

/// A graph read this module serves.
enum OwnerRead {
    /// An algorithm, over the edges live at `Option<i64>` system time when set.
    /// The parameters are boxed: they dwarf the other variants.
    Algo(
        nodedb_graph::GraphAlgorithm,
        Box<nodedb_graph::AlgoParams>,
        Option<i64>,
    ),
    Neighbors(String),
    /// A `Hop`, `Path` or `Subgraph` in a cluster.
    Walk,
    /// A RAG fusion in a cluster.
    Rag,
}

/// Serve `plan` when it is a graph algorithm, a one-hop neighbors read, or a
/// walk or RAG fusion in a cluster. Returns `None` for every other plan.
pub(super) async fn dispatch_graph_owner_read(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    vshard_id: VShardId,
) -> Option<NativeResponse> {
    let read = match plan {
        PhysicalPlan::Graph(GraphOp::Algo {
            algorithm, params, ..
        }) => OwnerRead::Algo(*algorithm, Box::new(params.clone()), None),
        PhysicalPlan::Graph(GraphOp::TemporalAlgorithm {
            algorithm,
            params,
            system_time,
        }) => match system_time {
            nodedb_types::SystemTimeScope::Current => {
                OwnerRead::Algo(*algorithm, Box::new(params.clone()), None)
            }
            nodedb_types::SystemTimeScope::AsOf(ms) => {
                OwnerRead::Algo(*algorithm, Box::new(params.clone()), Some(*ms))
            }
            nodedb_types::SystemTimeScope::AllVersions => {
                return Some(error_to_native(
                    seq,
                    &crate::Error::BadRequest {
                        detail: "a graph algorithm runs over one system time;                                  all-versions is not supported"
                            .into(),
                    },
                ));
            }
        },
        PhysicalPlan::Graph(GraphOp::Neighbors { node_id, .. }) => {
            OwnerRead::Neighbors(node_id.clone())
        }
        PhysicalPlan::Graph(
            GraphOp::Hop { .. } | GraphOp::Path { .. } | GraphOp::Subgraph { .. },
        ) if ctx.state.cluster_routing.is_some() => OwnerRead::Walk,
        PhysicalPlan::Graph(GraphOp::RagFusion { .. }) if ctx.state.cluster_routing.is_some() => {
            OwnerRead::Rag
        }
        _ => return None,
    };
    let tenant_id = ctx.tenant_id();
    let database_id = ctx.database_id();
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: plan.clone(),
        post_set_op: PostSetOp::None,
        txn_id,
    };
    if let Err(error) = super::sql_gateway::authorize_native_task(ctx, &task) {
        return Some(error_to_native(seq, &error));
    }
    let _request = ctx.state.tenant_request_guard(tenant_id);
    let served = match read {
        OwnerRead::Algo(algorithm, params, system_as_of_ms) => {
            crate::control::server::graph_dispatch::run_graph_algo(
                ctx.state,
                tenant_id,
                database_id,
                algorithm,
                *params,
                system_as_of_ms,
                true,
            )
            .await
            .map(ok_payload_response)
        }
        OwnerRead::Neighbors(node_id) => crate::control::server::graph_dispatch::read_on_key_owner(
            ctx.state,
            tenant_id,
            database_id,
            &node_id,
            plan.clone(),
            txn_id,
            true,
        )
        .await
        .map(ok_payload_response),
        OwnerRead::Walk => {
            crate::control::server::graph_dispatch::serve_walk_plan(
                ctx.state,
                tenant_id,
                database_id,
                plan,
                txn_id,
                true,
            )
            .await?
        }
        OwnerRead::Rag => {
            crate::control::server::graph_dispatch::serve_rag_plan(
                ctx.state,
                tenant_id,
                database_id,
                plan,
                true,
            )
            .await?
        }
    };
    Some(match served {
        Ok(response) => data_plane_response_to_native(ctx, seq, plan, &response),
        Err(error) => error_to_native(seq, &error),
    })
}
