// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral raft group DDL commands: SHOW RAFT GROUPS, SHOW RAFT
//! GROUP, ALTER RAFT GROUP.
//!
//! Ported from the pgwire `ddl::cluster::raft` handlers. The raft-status /
//! routing reads and the `ALTER RAFT GROUP` `ConfChange` propose are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral `DdlResult` over
//! `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW RAFT GROUPS — list all Raft groups with leader, term, and status.
///
/// Superuser only.
pub fn show_raft_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view raft groups",
        ));
    }

    let status_fn = match state.raft_status_fn.get() {
        Some(f) => f,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let statuses = status_fn();

    let columns = vec![
        "group_id".to_string(),
        "role".to_string(),
        "leader_id".to_string(),
        "term".to_string(),
        "commit_index".to_string(),
        "last_applied".to_string(),
        "members".to_string(),
        "vshards".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();
    for s in &statuses {
        let mut row = Map::new();
        row.insert(
            "group_id".to_string(),
            JsonValue::String((s.group_id as i64).to_string()),
        );
        row.insert("role".to_string(), JsonValue::String(s.role.clone()));
        row.insert(
            "leader_id".to_string(),
            JsonValue::String((s.leader_id as i64).to_string()),
        );
        row.insert(
            "term".to_string(),
            JsonValue::String((s.term as i64).to_string()),
        );
        row.insert(
            "commit_index".to_string(),
            JsonValue::String((s.commit_index as i64).to_string()),
        );
        row.insert(
            "last_applied".to_string(),
            JsonValue::String((s.last_applied as i64).to_string()),
        );
        row.insert(
            "members".to_string(),
            JsonValue::String((s.member_count as i64).to_string()),
        );
        row.insert(
            "vshards".to_string(),
            JsonValue::String((s.vshard_count as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW RAFT GROUP <id> — detailed info for a specific Raft group.
///
/// Superuser only.
pub fn show_raft_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can inspect raft groups",
        ));
    }

    if parts.len() < 4 {
        return Err(ddl_err("42601", "syntax: SHOW RAFT GROUP <group_id>"));
    }

    let group_id: u64 = parts[3]
        .parse()
        .map_err(|_| ddl_err("42601", format!("invalid group_id: '{}'", parts[3])))?;

    let status_fn = match state.raft_status_fn.get() {
        Some(f) => f,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let statuses = status_fn();
    let group = match statuses.iter().find(|s| s.group_id == group_id) {
        Some(g) => g,
        None => {
            return Err(ddl_err(
                "42704",
                format!("raft group {group_id} not found on this node"),
            ));
        }
    };

    let columns = vec!["property".to_string(), "value".to_string()];

    let props = [
        ("group_id", group.group_id.to_string()),
        ("role", group.role.clone()),
        ("leader_id", group.leader_id.to_string()),
        ("term", group.term.to_string()),
        ("commit_index", group.commit_index.to_string()),
        ("last_applied", group.last_applied.to_string()),
        ("member_count", group.member_count.to_string()),
        ("vshard_count", group.vshard_count.to_string()),
    ];

    let mut extra_props = Vec::new();
    if let Some(routing) = &state.cluster_routing {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        if let Some(info) = routing.group_info(group_id) {
            extra_props.push((
                "members".to_string(),
                info.members
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        let vshards = routing.vshards_for_group(group_id);
        if let (Some(first), Some(last)) = (vshards.first(), vshards.last()) {
            let range = format!("{first}..{last} ({} total)", vshards.len());
            extra_props.push(("vshards".to_string(), range));
        }
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
    for (key, value) in &extra_props {
        let mut row = Map::new();
        row.insert("property".to_string(), JsonValue::String(key.clone()));
        row.insert("value".to_string(), JsonValue::String(value.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// ALTER RAFT GROUP <id> ADD|REMOVE NODE <node_id>
///
/// Proposes a membership change to the Raft group via a ConfChange entry.
/// The change takes effect when the entry is committed by quorum.
///
/// Superuser only.
pub fn alter_raft_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    group_id_str: &str,
    action: &str,
    node_id_str: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can alter raft groups",
        ));
    }

    let group_id: u64 = group_id_str
        .parse()
        .map_err(|_| ddl_err("42601", format!("invalid group_id: '{group_id_str}'")))?;

    let action = action.to_uppercase();
    let node_id: u64 = node_id_str
        .parse()
        .map_err(|_| ddl_err("42601", format!("invalid node_id: '{node_id_str}'")))?;

    let change_type = match action.as_str() {
        "ADD" => nodedb_cluster::ConfChangeType::AddNode,
        "REMOVE" => nodedb_cluster::ConfChangeType::RemoveNode,
        _ => {
            return Err(ddl_err(
                "42601",
                format!("expected ADD or REMOVE, got '{action}'"),
            ));
        }
    };

    let proposer = match state.raft_proposer.get() {
        Some(p) => p,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let change = nodedb_cluster::ConfChange {
        change_type,
        node_id,
    };
    let data = change
        .to_entry_data()
        .map_err(|e| ddl_err("XX000", format!("conf_change encode: {e}")))?;

    // Find a vShard that maps to this group to propose through Raft.
    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(ddl_err("55000", "cluster routing not available"));
        }
    };

    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let vshards = routing.vshards_for_group(group_id);
    if vshards.is_empty() {
        return Err(ddl_err(
            "42704",
            format!("raft group {group_id} has no vShards"),
        ));
    }
    let vshard_id = vshards[0];
    drop(routing);

    match proposer(vshard_id, data) {
        Ok((_gid, _idx)) => Ok(vec![DdlResult::Status {
            command: "ALTER RAFT GROUP".to_string(),
            rows_affected: None,
        }]),
        Err(e) => Err(ddl_err("XX000", format!("propose failed: {e}"))),
    }
}
