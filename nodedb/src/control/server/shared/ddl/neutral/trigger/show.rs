// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW TRIGGERS` DDL handler.
//!
//! Ported from the pgwire `ddl::trigger::show` handler. The registry read,
//! `sort_key` ordering, optional `ON <collection>` filter, and the exact
//! column set / per-column text encoding are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral [`DdlResult::Rows`] over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW TRIGGERS [ON <collection>]`
pub fn show_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // Optional collection filter: SHOW TRIGGERS ON <collection>
    let collection_filter = if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("ON") {
        Some(parse_ident_token(parts[3])?)
    } else {
        None
    };

    let columns = vec![
        "name".to_string(),
        "collection".to_string(),
        "timing".to_string(),
        "events".to_string(),
        "granularity".to_string(),
        "execution".to_string(),
        "enabled".to_string(),
        "priority".to_string(),
        "owner".to_string(),
    ];

    let triggers = state
        .trigger_registry
        .list_for_tenant(database_id, tenant_id);
    let mut sorted = triggers;
    sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    let mut rows = Vec::new();
    for t in &sorted {
        if let Some(ref filter) = collection_filter
            && &t.collection != filter
        {
            continue;
        }
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(t.name.clone()));
        row.insert(
            "collection".to_string(),
            JsonValue::String(t.collection.clone()),
        );
        row.insert(
            "timing".to_string(),
            JsonValue::String(t.timing.as_str().to_string()),
        );
        row.insert("events".to_string(), JsonValue::String(t.events.display()));
        row.insert(
            "granularity".to_string(),
            JsonValue::String(t.granularity.as_str().to_string()),
        );
        row.insert(
            "execution".to_string(),
            JsonValue::String(t.execution_mode.as_str().to_string()),
        );
        row.insert(
            "enabled".to_string(),
            JsonValue::String(t.enabled.to_string()),
        );
        row.insert(
            "priority".to_string(),
            JsonValue::String(t.priority.to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(t.owner.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
