// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW ROUTING` — expose the vshard → leaseholder →
//! node address mapping so smart clients can cache it and route writes
//! directly to the leaseholder, skipping the gateway hop.
//!
//! Ported from the pgwire `ddl::cluster::routing_hint` handler. The
//! routing / topology reads are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.
//!
//! Result columns: `vshard_id`, `group_id`, `leaseholder_node_id`,
//! `leaseholder_addr`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW ROUTING — full vshard → leaseholder → address table.
///
/// Any authenticated user may call this (smart-client libs need it).
pub fn show_routing(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let columns = vec![
        "vshard_id".to_string(),
        "group_id".to_string(),
        "leaseholder_node_id".to_string(),
        "leaseholder_addr".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();

    let rt = routing.read().unwrap_or_else(|p| p.into_inner());
    let topo_guard = state
        .cluster_topology
        .as_ref()
        .map(|t| t.read().unwrap_or_else(|p| p.into_inner()));

    for vshard_id in 0..nodedb_cluster::routing::VSHARD_COUNT {
        let group_id = rt.group_for_vshard(vshard_id).unwrap_or(0);
        let leader = rt.group_info(group_id).map(|info| info.leader).unwrap_or(0);
        let addr = topo_guard
            .as_ref()
            .and_then(|topo| topo.get_node(leader))
            .map(|n| n.addr.clone())
            .unwrap_or_default();

        let mut row = Map::new();
        row.insert(
            "vshard_id".to_string(),
            JsonValue::String((vshard_id as i64).to_string()),
        );
        row.insert(
            "group_id".to_string(),
            JsonValue::String((group_id as i64).to_string()),
        );
        row.insert(
            "leaseholder_node_id".to_string(),
            JsonValue::String((leader as i64).to_string()),
        );
        row.insert("leaseholder_addr".to_string(), JsonValue::String(addr));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
