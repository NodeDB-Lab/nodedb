//! Rebalance DDL command: REBALANCE.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// REBALANCE — compute and display a rebalance plan.
///
/// Shows the planned vShard moves to achieve uniform distribution.
/// Superuser only.
pub fn rebalance(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can rebalance",
        ));
    }

    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };
    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => {
            return Err(sqlstate_error("55000", "cluster topology not available"));
        }
    };

    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let topo = topo.read().unwrap_or_else(|p| p.into_inner());

    let plan = nodedb_cluster::compute_plan(&routing, &topo)
        .map_err(|e| sqlstate_error("XX000", &format!("rebalance planning failed: {e}")))?;

    if plan.is_empty() {
        let schema = Arc::new(vec![text_field("status")]);
        let mut enc = DataRowEncoder::new(schema.clone());
        enc.encode_field(&"cluster is balanced — no moves needed")?;
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![Ok(enc.take_row())]),
        ))]);
    }

    let schema = Arc::new(vec![
        int8_field("vshard_id"),
        int8_field("source_node"),
        int8_field("target_node"),
        int8_field("source_group"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for m in &plan.moves {
        encoder.encode_field(&(m.vshard_id as i64))?;
        encoder.encode_field(&(m.source_node as i64))?;
        encoder.encode_field(&(m.target_node as i64))?;
        encoder.encode_field(&(m.source_group as i64))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
