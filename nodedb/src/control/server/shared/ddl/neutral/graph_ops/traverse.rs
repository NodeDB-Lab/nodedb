// SPDX-License-Identifier: BUSL-1.1

//! Read handlers: GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH.

use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::DatabaseId;
use nodedb_types::filter::MetadataFilter;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::response::payload_to_rows;
use super::support::ddl_err;

/// Names the traversal family in the refusal a read policy raises: what the
/// result carries instead of rows is graph topology.
const TRAVERSAL_WHAT: &str = "graph traversal, which returns graph topology";

fn to_engine_direction(d: GraphDirection) -> Direction {
    match d {
        GraphDirection::In => Direction::In,
        GraphDirection::Out => Direction::Out,
        GraphDirection::Both => Direction::Both,
    }
}

fn clamp_depth(value: usize, field: &'static str) -> Result<usize, DdlError> {
    if value > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(ddl_err(
            "22023",
            format!("{field} {value} exceeds maximum allowed value {MAX_GRAPH_TRAVERSAL_DEPTH}"),
        ));
    }
    Ok(value)
}

/// Check a requested traversal depth against a tenant depth limit.
///
/// `limit = 0` means unlimited — the same convention as `max_connections`.
/// Returns a [`DdlError`] if the depth exceeds a finite limit.
pub(crate) fn check_graph_depth_against_limit(
    depth: usize,
    limit: u32,
    field: &'static str,
) -> Result<(), DdlError> {
    if limit > 0 && depth as u32 > limit {
        return Err(ddl_err(
            "42P17",
            format!("{field} {depth} exceeds tenant quota max_graph_depth={limit}"),
        ));
    }
    Ok(())
}

/// Look up the tenant's `max_graph_depth` quota and check against it.
fn check_tenant_graph_depth(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    depth: usize,
    field: &'static str,
) -> Result<(), DdlError> {
    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };
    let limit = tenants.quota(tenant_id).max_graph_depth;
    check_graph_depth_against_limit(depth, limit, field)
}

/// Fail closed unless `identity` can read the traversal's collection.
///
/// A traversal discloses which nodes exist in a collection and how they are
/// connected, so it carries the same read grant the collection's rows do.
fn authorize_traversal(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    // The traversal reaches the Data Plane through `broadcast_to_all_cores`,
    // which never runs `inject_rls`, so the RBAC check and the policy are both
    // resolved here. A traversal returns topology rather than row bodies —
    // there is nothing for a row filter to evaluate — and disclosing the shape
    // of rows whose contents are protected is the leak, so a read policy
    // refuses outright.
    let gate = RefusingReadGate::open(state, identity, database_id, collection, TRAVERSAL_WHAT)?;

    // Column redaction is refused here for the same reason and on the same
    // seam: the traversal returns topology, so there are no stored columns in
    // its result for the redaction hook to mask.
    // `collection` is passed bare: the callee qualifies it against
    // `database_id` itself, so qualifying it here too will double-qualify.
    crate::control::planner::redaction_refusal::refuse_unredactable_graph_collection(
        collection,
        database_id,
        gate.tenant_id(),
        gate.auth(),
        &state.redaction,
    )
    .map_err(|error| ddl_err("0A000", error.to_string()))?;

    Ok(())
}

/// `GRAPH TRAVERSE` request fields.
pub struct TraverseRequest {
    pub collection: String,
    pub start: String,
    pub depth: usize,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: Vec<String>,
    pub direction: GraphDirection,
    /// `EDGE WHERE` filters, AND-ed. Empty admits every edge.
    pub edge_predicate: Vec<MetadataFilter>,
    /// The session's active transaction, for read-your-own-writes overlay merge.
    pub txn_id: Option<crate::types::TxnId>,
    /// The session reads linearizably.
    pub linearizable: bool,
}

/// `GRAPH TRAVERSE IN '<collection>' FROM '<node_id>' [DEPTH <n>]
/// [LABEL '<label>'[, '<label>' ...]] [DIRECTION in|out|both]
/// [EDGE WHERE <predicate>]`
///
/// Each result edge carries its current property object.
///
/// `txn_id` reaches every hop of the walk (`cross_core_traverse_subgraph`).
/// The walk, its `EDGE WHERE` predicate and the returned properties see the
/// transaction's staged edge puts, tombstones and property changes.
pub async fn traverse(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    req: TraverseRequest,
) -> Result<Vec<DdlResult>, DdlError> {
    let TraverseRequest {
        collection,
        start,
        depth,
        edge_labels,
        direction,
        edge_predicate,
        txn_id,
        linearizable,
    } = req;
    if start.is_empty() {
        return Err(ddl_err("42601", "missing FROM '<node_id>'"));
    }
    authorize_traversal(state, identity, database_id, &collection)?;
    let depth = clamp_depth(depth, "DEPTH")?;
    let tenant_id = identity.tenant_id;
    check_tenant_graph_depth(state, tenant_id, depth, "DEPTH")?;
    let dir = to_engine_direction(direction);

    // Subgraph-shaped dispatcher: emits the `{nodes,edges}` JSON both clients
    // decode. Tree DDL aggregates that only need a flat reachable set call
    // `cross_core_bfs_with_options` directly.
    match crate::control::server::graph_dispatch::cross_core_traverse_subgraph(
        state,
        crate::control::server::graph_dispatch::CrossCoreTraverseSubgraphParams {
            tenant_id,
            database_id,
            // The walk plans read edges by the stored, database-qualified name.
            collection: Some(
                nodedb_types::QualifiedCollection::new(database_id, &collection)
                    .as_str()
                    .to_owned(),
            ),
            start,
            edge_labels: &edge_labels,
            direction: dir,
            max_depth: depth,
            options: &GraphTraversalOptions::default(),
            linearizable,
            edge_predicate: &edge_predicate,
            with_properties: true,
            txn_id,
        },
    )
    .await
    {
        Ok(resp) => Ok(payload_to_rows(&resp.payload)),
        Err(e) => Err(DdlError::from_error(&e)),
    }
}

/// `GRAPH NEIGHBORS` request fields.
pub struct NeighborsRequest {
    pub collection: String,
    pub node: String,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: Vec<String>,
    pub direction: GraphDirection,
    /// The session's active transaction, for read-your-own-writes overlay merge.
    pub txn_id: Option<crate::types::TxnId>,
    /// The session reads linearizably.
    pub linearizable: bool,
}

/// `GRAPH NEIGHBORS IN '<collection>' OF '<node_id>'
/// [LABEL '<label>'[, '<label>' ...]] [DIRECTION in|out|both]`
///
/// `txn_id` (the caller's active session transaction, if any) is stamped
/// onto the request so this read observes the transaction's own staged
/// edge writes (read-your-own-writes) via `GraphTxnOverlay`.
pub async fn neighbors(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    req: NeighborsRequest,
) -> Result<Vec<DdlResult>, DdlError> {
    let NeighborsRequest {
        collection,
        node,
        edge_labels,
        direction,
        txn_id,
        linearizable,
    } = req;
    if node.is_empty() {
        return Err(ddl_err("42601", "missing OF '<node_id>'"));
    }
    authorize_traversal(state, identity, database_id, &collection)?;
    let dir = to_engine_direction(direction);
    let tenant_id = identity.tenant_id;

    let node_key = node.clone();
    let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
        collection: Some(nodedb_types::QualifiedCollection::new(
            database_id,
            &collection,
        )),
        node_id: node,
        edge_labels,
        direction: dir,
        rls_filters: Vec::new(),
    });

    // The node's edges live on its key vShard: the read runs on that vShard's
    // leader, confirmed there when linearizable.
    match crate::control::server::graph_dispatch::read_on_key_owner(
        state,
        tenant_id,
        database_id,
        &node_key,
        plan,
        txn_id,
        linearizable,
    )
    .await
    {
        Ok(payload) => Ok(payload_to_rows(&payload)),
        Err(e) => Err(DdlError::from_error(&e)),
    }
}

/// `GRAPH PATH` request fields.
pub struct ShortestPathRequest {
    pub collection: String,
    pub src: String,
    pub dst: String,
    pub max_depth: usize,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: Vec<String>,
    /// `EDGE WHERE` filters, AND-ed. Empty admits every edge.
    pub edge_predicate: Vec<MetadataFilter>,
    /// The session's active transaction, for read-your-own-writes overlay merge.
    pub txn_id: Option<crate::types::TxnId>,
    /// The session reads linearizably.
    pub linearizable: bool,
}

/// `GRAPH PATH IN '<collection>' FROM '<src>' TO '<dst>' [MAX_DEPTH <n>]
/// [LABEL '<label>'[, '<label>' ...]] [EDGE WHERE <predicate>]`
///
/// Returns the actual shortest path `[src, hop_1, ..., dst]`. An
/// unreachable destination yields an empty array. Orchestrated by
/// `cross_core_shortest_path`, which records parent pointers per
/// hop so the path can be reconstructed across every topology —
/// single core, single-node multi-core, and clustered. The path sees the
/// staged edge writes of `txn_id`.
pub async fn shortest_path(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    req: ShortestPathRequest,
) -> Result<Vec<DdlResult>, DdlError> {
    let ShortestPathRequest {
        collection,
        src,
        dst,
        max_depth,
        edge_labels,
        edge_predicate,
        txn_id,
        linearizable,
    } = req;
    if src.is_empty() || dst.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH PATH requires FROM '<src>' TO '<dst>'",
        ));
    }
    authorize_traversal(state, identity, database_id, &collection)?;
    let max_depth = clamp_depth(max_depth, "MAX_DEPTH")?;
    let tenant_id = identity.tenant_id;
    check_tenant_graph_depth(state, tenant_id, max_depth, "MAX_DEPTH")?;
    match crate::control::server::graph_dispatch::cross_core_shortest_path(
        state,
        crate::control::server::graph_dispatch::CrossCoreShortestPathParams {
            tenant_id,
            database_id,
            // The walk plans read edges by the stored, database-qualified name.
            collection: Some(
                nodedb_types::QualifiedCollection::new(database_id, &collection)
                    .as_str()
                    .to_owned(),
            ),
            src,
            dst,
            edge_labels,
            max_depth,
            options: GraphTraversalOptions::default(),
            linearizable,
            edge_predicate,
            txn_id,
        },
    )
    .await
    {
        Ok(resp) => Ok(payload_to_rows(&resp.payload)),
        Err(e) => Err(DdlError::from_error(&e)),
    }
}

#[cfg(test)]
mod tests {
    use super::check_graph_depth_against_limit;

    #[test]
    fn tenant_graph_depth_under_bound_succeeds() {
        assert!(check_graph_depth_against_limit(5, 10, "DEPTH").is_ok());
        assert!(check_graph_depth_against_limit(10, 10, "DEPTH").is_ok());
    }

    #[test]
    fn tenant_graph_depth_exceeded_rejected() {
        let err = check_graph_depth_against_limit(11, 10, "DEPTH")
            .expect_err("depth > limit must be rejected");
        assert!(
            err.message.contains("11"),
            "error must include the requested depth"
        );
        assert!(err.message.contains("10"), "error must include the limit");
    }

    #[test]
    fn tenant_graph_depth_zero_means_unlimited() {
        assert!(check_graph_depth_against_limit(usize::MAX, 0, "DEPTH").is_ok());
        assert!(check_graph_depth_against_limit(99999, 0, "MAX_DEPTH").is_ok());
    }
}
