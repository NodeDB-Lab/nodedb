// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral rebalance DDL command: REBALANCE.
//!
//! Ported from the pgwire `ddl::cluster::rebalance_cmd` handler. The
//! routing / topology reads and the `compute_plan` call are preserved
//! verbatim; only the result construction changed from pgwire `Response` /
//! `QueryResponse` to the protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// REBALANCE — compute and display a rebalance plan.
///
/// Shows the planned vShard moves to achieve uniform distribution.
/// Superuser only.
pub fn rebalance(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can rebalance",
        ));
    }

    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };
    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => {
            return Err(ddl_err("55000", "cluster topology not available"));
        }
    };

    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let topo = topo.read().unwrap_or_else(|p| p.into_inner());

    let plan = nodedb_cluster::compute_plan(&routing, &topo)
        .map_err(|e| ddl_err("XX000", format!("rebalance planning failed: {e}")))?;

    if plan.is_empty() {
        let mut row = Map::new();
        row.insert(
            "status".to_string(),
            JsonValue::String("cluster is balanced — no moves needed".to_string()),
        );
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
            vec!["status".to_string()],
            vec![row],
        ))]);
    }

    let columns = vec![
        "vshard_id".to_string(),
        "source_node".to_string(),
        "target_node".to_string(),
        "source_group".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();
    for m in &plan.moves {
        let mut row = Map::new();
        row.insert(
            "vshard_id".to_string(),
            JsonValue::String((m.vshard_id as i64).to_string()),
        );
        row.insert(
            "source_node".to_string(),
            JsonValue::String((m.source_node as i64).to_string()),
        );
        row.insert(
            "target_node".to_string(),
            JsonValue::String((m.target_node as i64).to_string()),
        );
        row.insert(
            "source_group".to_string(),
            JsonValue::String((m.source_group as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
