//! Read handlers: GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH;

use super::response::payload_to_query_response;

fn to_engine_direction(d: GraphDirection) -> Direction {
    match d {
        GraphDirection::In => Direction::In,
        GraphDirection::Out => Direction::Out,
        GraphDirection::Both => Direction::Both,
    }
}

fn clamp_depth(value: usize, field: &'static str) -> PgWireResult<usize> {
    if value > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(sqlstate_error(
            "22023",
            &format!("{field} {value} exceeds maximum allowed value {MAX_GRAPH_TRAVERSAL_DEPTH}"),
        ));
    }
    Ok(value)
}

/// `GRAPH TRAVERSE FROM '<node_id>' [DEPTH <n>] [LABEL '<label>'] [DIRECTION in|out|both]`
pub async fn traverse(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    start: String,
    depth: usize,
    edge_label: Option<String>,
    direction: GraphDirection,
) -> PgWireResult<Vec<Response>> {
    if start.is_empty() {
        return Err(sqlstate_error("42601", "missing FROM '<node_id>'"));
    }
    let depth = clamp_depth(depth, "DEPTH")?;
    let dir = to_engine_direction(direction);
    let tenant_id = identity.tenant_id;

    match dispatch_utils::cross_core_bfs(state, tenant_id, vec![start], edge_label, dir, depth)
        .await
    {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH NEIGHBORS OF '<node_id>' [LABEL '<label>'] [DIRECTION in|out|both]`
pub async fn neighbors(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    node: String,
    edge_label: Option<String>,
    direction: GraphDirection,
) -> PgWireResult<Vec<Response>> {
    if node.is_empty() {
        return Err(sqlstate_error("42601", "missing OF '<node_id>'"));
    }
    let dir = to_engine_direction(direction);
    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: node,
        edge_label,
        direction: dir,
        rls_filters: Vec::new(),
    });

    match dispatch_utils::broadcast_to_all_cores(state, tenant_id, plan, 0).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH PATH FROM '<src>' TO '<dst>' [MAX_DEPTH <n>] [LABEL '<label>']`
///
/// Returns the actual shortest path `[src, hop_1, ..., dst]`. An
/// unreachable destination yields an empty array. Orchestrated by
/// `cross_core_shortest_path`, which records parent pointers per
/// hop so the path can be reconstructed across every topology —
/// single core, single-node multi-core, and clustered.
pub async fn shortest_path(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    src: String,
    dst: String,
    max_depth: usize,
    edge_label: Option<String>,
) -> PgWireResult<Vec<Response>> {
    if src.is_empty() || dst.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH PATH requires FROM '<src>' TO '<dst>'",
        ));
    }
    let max_depth = clamp_depth(max_depth, "MAX_DEPTH")?;
    let tenant_id = identity.tenant_id;
    match dispatch_utils::cross_core_shortest_path(
        state, tenant_id, src, dst, edge_label, max_depth,
    )
    .await
    {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}
