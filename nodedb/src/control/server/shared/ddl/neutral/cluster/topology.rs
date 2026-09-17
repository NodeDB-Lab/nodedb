// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral cluster topology DDL commands: SHOW NODES, SHOW NODE,
//! REMOVE NODE, SHOW CLUSTER.
//!
//! Ported from the pgwire `ddl::cluster::topology` handlers. The topology /
//! routing / raft-status reads and the `REMOVE NODE` `set_state` side-effect
//! are preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral `DdlResult` over
//! `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{ddl_err, node_state_str};

/// SHOW NODES — list all cluster members with state.
///
/// Superuser only.
pub fn show_nodes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can list nodes",
        ));
    }

    let columns = vec![
        "node_id".to_string(),
        "address".to_string(),
        "state".to_string(),
        "raft_groups".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();

    match &state.cluster_topology {
        Some(t) => {
            let topo = t.read().unwrap_or_else(|p| p.into_inner());
            let mut nodes: Vec<_> = topo.all_nodes().collect();
            nodes.sort_by_key(|n| n.node_id);

            for node in nodes {
                let groups_str: String = node
                    .raft_groups
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let mut row = Map::new();
                row.insert(
                    "node_id".to_string(),
                    JsonValue::String((node.node_id as i64).to_string()),
                );
                row.insert("address".to_string(), JsonValue::String(node.addr.clone()));
                row.insert(
                    "state".to_string(),
                    JsonValue::String(node_state_str(node.state).to_string()),
                );
                row.insert("raft_groups".to_string(), JsonValue::String(groups_str));
                rows.push(row);
            }
        }
        None => {
            // Single-node mode: show this node as the only member.
            let mut row = Map::new();
            row.insert(
                "node_id".to_string(),
                JsonValue::String((state.node_id as i64).to_string()),
            );
            row.insert(
                "address".to_string(),
                JsonValue::String("local".to_string()),
            );
            row.insert("state".to_string(), JsonValue::String("active".to_string()));
            row.insert("raft_groups".to_string(), JsonValue::String(String::new()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW NODE <node_id> — detailed info for a specific node.
///
/// Superuser only.
pub fn show_node(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can inspect nodes",
        ));
    }

    if parts.len() < 3 {
        return Err(ddl_err("42601", "syntax: SHOW NODE <node_id>"));
    }

    let node_id: u64 = parts[2]
        .parse()
        .map_err(|_| ddl_err("42601", format!("invalid node_id: '{}'", parts[2])))?;

    let columns = vec!["property".to_string(), "value".to_string()];

    let props = match &state.cluster_topology {
        Some(t) => {
            let topo = t.read().unwrap_or_else(|p| p.into_inner());
            let node = match topo.get_node(node_id) {
                Some(n) => n,
                None => {
                    return Err(ddl_err(
                        "42704",
                        format!("node {node_id} not found in cluster topology"),
                    ));
                }
            };
            vec![
                ("node_id".to_string(), node.node_id.to_string()),
                ("address".to_string(), node.addr.clone()),
                ("state".to_string(), format!("{:?}", node.state)),
                (
                    "raft_groups".to_string(),
                    node.raft_groups
                        .iter()
                        .map(|g| g.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            ]
        }
        None => {
            // Single-node mode: show self info if node_id matches.
            if node_id != state.node_id {
                return Err(ddl_err(
                    "42704",
                    format!(
                        "node {node_id} not found (single-node instance, this node is {})",
                        state.node_id
                    ),
                ));
            }
            let wal_lsn = state.wal.next_lsn().as_u64().saturating_sub(1);
            vec![
                ("node_id".to_string(), state.node_id.to_string()),
                ("address".to_string(), "local".to_string()),
                ("state".to_string(), "active".to_string()),
                ("mode".to_string(), "single-node".to_string()),
                ("wal_lsn".to_string(), wal_lsn.to_string()),
            ]
        }
    };

    let mut rows = Vec::new();
    for (key, value) in &props {
        let mut row = Map::new();
        row.insert("property".to_string(), JsonValue::String(key.clone()));
        row.insert("value".to_string(), JsonValue::String(value.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// REMOVE NODE <node_id> — mark a node as decommissioned.
///
/// Superuser only.
pub fn remove_node(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can remove nodes",
        ));
    }

    if parts.len() < 3 {
        return Err(ddl_err("42601", "syntax: REMOVE NODE <node_id>"));
    }

    let node_id: u64 = parts[2]
        .parse()
        .map_err(|_| ddl_err("42601", format!("invalid node_id: '{}'", parts[2])))?;

    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let mut topo = topo.write().unwrap_or_else(|p| p.into_inner());

    if !topo.contains(node_id) {
        return Err(ddl_err(
            "42704",
            format!("node {node_id} not found in cluster topology"),
        ));
    }

    topo.set_state(node_id, nodedb_cluster::NodeState::Decommissioned);

    Ok(vec![DdlResult::Status {
        command: "REMOVE NODE".to_string(),
        rows_affected: None,
    }])
}

/// SHOW CLUSTER — cluster overview.
///
/// Superuser only.
pub fn show_cluster(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view cluster status",
        ));
    }

    let columns = vec!["property".to_string(), "value".to_string()];

    let mut props = vec![("node_id", state.node_id.to_string())];

    if let Some(topo) = &state.cluster_topology {
        let topo = topo.read().unwrap_or_else(|p| p.into_inner());
        props.push(("nodes_total", topo.node_count().to_string()));
        props.push(("nodes_active", topo.active_nodes().len().to_string()));
        props.push(("topology_version", topo.version().to_string()));
    } else {
        props.push(("mode", "single-node".to_string()));
    }

    if let Some(routing) = &state.cluster_routing {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        props.push(("raft_groups", routing.num_groups().to_string()));
        props.push(("vshards", "1024".to_string()));
    }

    if let Some(status_fn) = state.raft_status_fn.get() {
        let statuses = status_fn();
        let leaders = statuses.iter().filter(|s| s.role == "Leader").count();
        props.push(("groups_leading", leaders.to_string()));
        props.push(("groups_following", (statuses.len() - leaders).to_string()));
    }

    let mut rows = Vec::new();
    for (key, value) in &props {
        let mut row = Map::new();
        row.insert(
            "property".to_string(),
            JsonValue::String((*key).to_string()),
        );
        row.insert("value".to_string(), JsonValue::String(value.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
