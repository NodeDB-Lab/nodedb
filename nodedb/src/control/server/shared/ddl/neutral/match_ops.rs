// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral MATCH pattern query handler — parses Cypher-style MATCH
//! syntax, compiles to PhysicalPlan::GraphMatch, and dispatches to Data Plane.
//!
//! The handler builds [`DdlResult`](super::super::result::DdlResult) directly
//! and carries no pgwire types. It is dispatched from the neutral router's typed
//! `GraphStmt::MatchQuery` arm, but re-parses the raw `sql` with the graph
//! pattern compiler (as the pgwire handler did) to build the physical plan.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::graph_dispatch;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::{DatabaseId, TraceId, TxnId};
use nodedb_physical::physical_plan::GraphOp;

use super::super::result::{DdlError, DdlResult};
use super::refuse_gate::RefusingReadGate;

/// Names the MATCH shape in the refusal a read policy raises: a pattern match
/// returns variable bindings over topology, with no row for a filter to apply
/// to — and its own `WHERE` can probe a hidden row's fields one predicate at a
/// time.
const MATCH_WHAT: &str = "a pattern match, which returns bindings over graph topology";

/// Returned when a MATCH could not be fully resolved within its expansion
/// budget — either the cross-shard hop rounds or the variable-length paging
/// rounds were exhausted with work still pending, or a single-node
/// variable-length expansion hit its hard cap with no coordinator to drain it.
/// The result set would be INCOMPLETE, so it is surfaced as a fail-closed error
/// (SQLSTATE 54001, `program_limit_exceeded`) rather than silently returning a
/// truncated result the client cannot distinguish from a complete one.
const MATCH_INCOMPLETE_MESSAGE: &str = "MATCH result incomplete: the pattern exceeded the expansion budget; \
     narrow the pattern or its variable-length `*min..max` bound";

/// Handle a MATCH query.
///
/// Parses the Cypher-style MATCH syntax, serializes the MatchQuery AST,
/// constructs PhysicalPlan::GraphMatch, and broadcasts to all Data Plane cores.
pub async fn match_query(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_id: Option<TxnId>,
) -> Result<Vec<DdlResult>, DdlError> {
    // Parse the MATCH query.
    let query = crate::engine::graph::pattern::compiler::parse(sql)
        .map_err(|e| DdlError::new("42601", format!("MATCH parse error: {e}")))?;

    // Both dispatch shapes below reach the Data Plane without a single plan for
    // the planner's authorization and RLS passes to inspect, so both are
    // resolved here, on the pattern's own scope.
    //
    // A pattern scoped with `IN '<collection>'` asks the narrow question about
    // that collection. An unscoped pattern may walk any collection the tenant
    // holds, so the set it could touch is the set it must be granted: every
    // active collection of the database, failing closed on the first denial.
    // Requiring an explicit `IN` instead would refuse the unscoped form for
    // every caller, including one already granted everything the pattern can
    // reach; this keeps that caller's behavior exactly as it was and refuses
    // only the caller who would otherwise walk a collection it cannot read.
    // The RLS half mirrors it: the narrow question when the pattern names a
    // collection, the tenant-wide one when it names none.
    let gate = RefusingReadGate::for_request(state, identity, database_id);
    match query.collection.as_deref() {
        Some(collection) => gate.gate_collection(collection, MATCH_WHAT)?,
        None => {
            gate.authorize_every_collection()?;
            gate.refuse_if_any_read_policy(MATCH_WHAT)?;
        }
    }

    // If the query targets a named collection via `IN '<collection>'`, gate
    // on catalog `is_active` (see `graph_ops::support::ensure_collection_active`,
    // shared with `SHOW GRAPH STATS` and `GRAPH RAG FUSION`): a plain
    // `DROP COLLECTION` (no PURGE) only flips `is_active=false` in the
    // catalog and does not reclaim edges/CSR, so reads must independently
    // hide it until UNDROP or a hard purge. This mirrors base-engine
    // `SELECT ... FROM c` behavior on a soft-dropped collection
    // (not-found/deactivated).
    if let Some(ref name) = query.collection {
        super::graph_ops::support::ensure_collection_active(
            state,
            database_id,
            identity.tenant_id.as_u64(),
            name,
        )?;
    }

    // Enforce the tenant's max_graph_depth quota.  Reject if any edge in the
    // pattern exceeds the cap.  Unbounded [*] (max_hops == usize::MAX) is
    // rejected when the tenant has any finite cap.
    {
        let tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        let limit = tenants.quota(identity.tenant_id).max_graph_depth;
        if limit > 0 {
            for clause in &query.clauses {
                for chain in &clause.patterns {
                    for triple in &chain.triples {
                        let hops = triple.edge.max_hops;
                        if hops > limit as usize {
                            return Err(DdlError::new(
                                "42P17",
                                format!(
                                    "MATCH traversal depth {hops} exceeds tenant quota \
                                     max_graph_depth={limit}"
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    // Collect column names for response schema.
    let column_names: Vec<String> = if query.return_columns.is_empty() {
        // Return all bound node variables.
        query.bound_node_names()
    } else {
        query
            .return_columns
            .iter()
            .map(|c| c.alias.clone().unwrap_or_else(|| c.expr.clone()))
            .collect()
    };

    // Serialize the MatchQuery for SPSC transport.
    let query_bytes = zerompk::to_msgpack_vec(&query)
        .map_err(|e| DdlError::new("XX000", format!("serialize match query: {e}")))?;

    let tenant_id = identity.tenant_id;

    // A MATCH returns graph topology — node bindings and edge labels — which
    // the result-path column-redaction hook has no stored columns to rewrite.
    // Both dispatch shapes below send the same query bytes, so the refusal is
    // applied here, once, before either runs. `query.collection` is already
    // parsed, so the scoped check is used directly rather than re-decoding
    // `query_bytes` back into a `MatchQuery` just to read it again. The gate's
    // own scope is reused so the redaction refusal, the RBAC check, and the RLS
    // refusal all resolve against the same principal.
    crate::control::planner::redaction_refusal::refuse_unredactable_graph_match_scoped(
        query.collection.as_deref(),
        tenant_id,
        database_id,
        gate.auth(),
        &state.redaction,
    )
    .map_err(|e| DdlError::new("0A000", e.to_string()))?;

    // Single-node mode: keep the direct path byte-identical — broadcast the `Match`
    // plan with `cluster_mode = false` to all local cores. The Data Plane emits
    // no frontier, so the unwrapped rows payload is exactly the prior bare-array
    // gather. No cross-shard orchestration is needed (and there is no routing
    // table to consult).
    if state.cluster_routing.is_none() {
        let plan = crate::bridge::envelope::PhysicalPlan::Graph(GraphOp::Match {
            query: query_bytes,
            frontier_bitmap: None,
            cluster_mode: false,
        });
        return match graph_dispatch::broadcast_match_to_all_cores(
            state,
            tenant_id,
            database_id,
            plan,
            TraceId::ZERO,
            txn_id,
        )
        .await
        {
            Ok(outcome) => {
                // Single-node frontier is always empty (cluster_mode=false). A
                // variable-length expansion can still hit its hard cap on a
                // single node (no coordinator to drive resume), so a partial
                // result is surfaced fail-closed rather than silently truncated.
                let _frontier = outcome.frontier;
                if outcome.partial {
                    Err(DdlError::new("54001", MATCH_INCOMPLETE_MESSAGE))
                } else {
                    match_payload_to_rows(&outcome.rows_payload, &column_names)
                }
            }
            Err(e) => Err(DdlError::new("XX000", e.to_string())),
        };
    }

    // Cluster mode: scatter-all to local + every remote owner, then drive the
    // continuation round loop across shard boundaries. `scatter_match` returns
    // the deduped rows in the same bare-array shape and a `partial` flag set on
    // truncation / round exhaustion. The active `txn_id` is threaded onto every
    // LOCAL scatter/resume leg so this node's cores merge the transaction's
    // staged edge overlay (read-your-own-writes); remote legs read committed CSR
    // (multi-node overlay forwarding is a separate unit).
    let deadline_ms = crate::control::gateway::dispatcher::statement_deadline_ms(state);
    match graph_dispatch::scatter_match(
        state,
        tenant_id,
        database_id,
        query_bytes,
        deadline_ms,
        txn_id,
    )
    .await
    {
        Ok(outcome) => {
            // A `partial` result means the cross-shard hop rounds or the
            // variable-length resume paging budget were exhausted with work
            // still pending: the result set is INCOMPLETE. Surface it
            // fail-closed so a client never mistakes it for a complete result.
            if outcome.partial {
                Err(DdlError::new("54001", MATCH_INCOMPLETE_MESSAGE))
            } else {
                match_payload_to_rows(&outcome.rows_payload, &column_names)
            }
        }
        Err(e) => Err(DdlError::new("XX000", e.to_string())),
    }
}

/// Convert MATCH result payload to a protocol-neutral multi-row result.
fn match_payload_to_rows(
    payload: &crate::bridge::envelope::Payload,
    column_names: &[String],
) -> Result<Vec<DdlResult>, DdlError> {
    let columns = column_names.to_vec();

    if payload.is_empty() {
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
            columns,
            Vec::new(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let rows: Vec<serde_json::Value> = sonic_rs::from_str(&json_text)
        .map_err(|e| DdlError::new("XX000", format!("invalid match result JSON: {e}")))?;

    let mut out_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut map = Map::new();
        for col_name in column_names {
            let val = row.get(col_name).and_then(|v| v.as_str()).unwrap_or("NULL");
            map.insert(col_name.clone(), JsonValue::String(val.to_string()));
        }
        out_rows.push(map);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns, out_rows,
    ))])
}

// Tenant-prefix stripping lives in the Data Plane, in
// `engine::graph::pattern::executor::rows_to_msgpack`, so every
// `GraphOp::Match` consumer (pgwire, native, HTTP) receives
// already-unscoped node ids on the wire.
