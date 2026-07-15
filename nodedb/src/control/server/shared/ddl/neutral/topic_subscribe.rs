// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SUBSCRIBE TO` (legacy pub/sub) DDL handler.
//!
//! ```sql
//! SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]
//! ```
//!
//! Ported from the pgwire `ddl::pubsub::subscribe_to` handler. Unlike the
//! `CREATE/DROP/SHOW TOPIC` + `PUBLISH TO` family (migrated separately to
//! `neutral::topic` on the newer `state.ep_topic_registry`), `SUBSCRIBE TO`
//! is the sole survivor still reading the legacy `state.topic_registry` — no
//! caller migrated it, so it is ported here verbatim on the same registry it
//! has always used. It carries no per-connection state: the subscription
//! receiver (`_rx`) is dropped exactly as the pgwire handler dropped it, and
//! only metadata rows are returned. Only the result construction changed
//! from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`]; the SQLSTATE codes are unchanged.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]
pub fn subscribe_to(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    sql: &str,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let topic_name = parts.get(2).unwrap_or(&"").to_lowercase();
    let since_seq: u64 = find_ascii_case_insensitive(sql, " SINCE ")
        .and_then(|pos| sql[pos + 7..].split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Check for GROUP clause: SUBSCRIBE TO topic GROUP group_name [SINCE seq]
    let group_name = find_ascii_case_insensitive(sql, " GROUP ")
        .map(|pos| sql[pos + 7..].split_whitespace().next().unwrap_or(""))
        .filter(|g| !g.is_empty())
        .map(|g| g.to_lowercase());

    let (sub_id, _rx, backlog) = if let Some(ref group) = group_name {
        state
            .topic_registry
            .subscribe_group(&topic_name, group, since_seq)
            .map_err(|e| err("42P01", e.to_string()))?
    } else {
        state
            .topic_registry
            .subscribe(&topic_name, since_seq)
            .map_err(|e| err("42P01", e.to_string()))?
    };

    let columns = vec![
        "subscription_id".to_string(),
        "topic".to_string(),
        "group".to_string(),
        "backlog".to_string(),
    ];
    let mut row = Map::new();
    row.insert(
        "subscription_id".to_string(),
        JsonValue::String(sub_id.to_string()),
    );
    row.insert("topic".to_string(), JsonValue::String(topic_name));
    row.insert(
        "group".to_string(),
        JsonValue::String(group_name.as_deref().unwrap_or("-").to_string()),
    );
    row.insert(
        "backlog".to_string(),
        JsonValue::String(backlog.len().to_string()),
    );

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows: vec![row],
        notice: None,
    })])
}
