//! Raft group DDL commands: SHOW RAFT GROUPS, SHOW RAFT GROUP, ALTER RAFT GROUP.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW RAFT GROUPS — list all Raft groups with leader, term, and status.
///
/// Superuser only.
pub fn show_raft_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view raft groups",
        ));
    }

    let status_fn = match &state.raft_status_fn {
        Some(f) => f,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let statuses = status_fn();

    let schema = Arc::new(vec![
        int8_field("group_id"),
        text_field("role"),
        int8_field("leader_id"),
        int8_field("term"),
        int8_field("commit_index"),
        int8_field("last_applied"),
        int8_field("members"),
        int8_field("vshards"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for s in &statuses {
        encoder.encode_field(&(s.group_id as i64))?;
        encoder.encode_field(&s.role)?;
        encoder.encode_field(&(s.leader_id as i64))?;
        encoder.encode_field(&(s.term as i64))?;
        encoder.encode_field(&(s.commit_index as i64))?;
        encoder.encode_field(&(s.last_applied as i64))?;
        encoder.encode_field(&(s.member_count as i64))?;
        encoder.encode_field(&(s.vshard_count as i64))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW RAFT GROUP <id> — detailed info for a specific Raft group.
///
/// Superuser only.
pub fn show_raft_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can inspect raft groups",
        ));
    }

    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SHOW RAFT GROUP <group_id>",
        ));
    }

    let group_id: u64 = parts[3]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid group_id: '{}'", parts[3])))?;

    let status_fn = match &state.raft_status_fn {
        Some(f) => f,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let statuses = status_fn();
    let group = match statuses.iter().find(|s| s.group_id == group_id) {
        Some(g) => g,
        None => {
            return Err(sqlstate_error(
                "42704",
                &format!("raft group {group_id} not found on this node"),
            ));
        }
    };

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

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

    for (key, value) in &props {
        encoder.encode_field(key)?;
        encoder.encode_field(value)?;
        rows.push(Ok(encoder.take_row()));
    }
    for (key, value) in &extra_props {
        encoder.encode_field(key)?;
        encoder.encode_field(value)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can alter raft groups",
        ));
    }

    // ALTER RAFT GROUP <id> ADD|REMOVE NODE <node_id>
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER RAFT GROUP <group_id> ADD|REMOVE NODE <node_id>",
        ));
    }

    let group_id: u64 = parts[3]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid group_id: '{}'", parts[3])))?;

    let action = parts[4].to_uppercase();
    let node_id: u64 = parts[6]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid node_id: '{}'", parts[6])))?;

    let change_type = match action.as_str() {
        "ADD" => nodedb_cluster::ConfChangeType::AddNode,
        "REMOVE" => nodedb_cluster::ConfChangeType::RemoveNode,
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("expected ADD or REMOVE, got '{action}'"),
            ));
        }
    };

    let proposer = match state.raft_proposer.get() {
        Some(p) => p,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let change = nodedb_cluster::ConfChange {
        change_type,
        node_id,
    };
    let data = change.to_entry_data();

    // Find a vShard that maps to this group to propose through Raft.
    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(sqlstate_error("55000", "cluster routing not available"));
        }
    };

    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let vshards = routing.vshards_for_group(group_id);
    if vshards.is_empty() {
        return Err(sqlstate_error(
            "42704",
            &format!("raft group {group_id} has no vShards"),
        ));
    }
    let vshard_id = vshards[0];
    drop(routing);

    match proposer(vshard_id, data) {
        Ok((_gid, _idx)) => Ok(vec![Response::Execution(Tag::new("ALTER RAFT GROUP"))]),
        Err(e) => Err(sqlstate_error("XX000", &format!("propose failed: {e}"))),
    }
}
