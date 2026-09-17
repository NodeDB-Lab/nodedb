// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW MATERIALIZED VIEWS [FOR <source>]` handler.
//!
//! Ported from the pgwire `ddl::materialized_view::show` handler. The catalog
//! read, the optional `FOR <source>` filter, and the exact column set (all five
//! columns `text`) are preserved verbatim; only the result construction changed
//! from pgwire `Response` / `QueryResponse` to the protocol-neutral
//! [`DdlResult::Rows`] over [`ShapedRows`]. All columns are `text`, so
//! `ShapedRows::text_types(5)` reproduces the RowDescription byte-identically.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub fn show_materialized_views(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parse_ident_token(parts[4])?)
    } else {
        None
    };

    let columns = vec![
        "name".to_string(),
        "source".to_string(),
        "refresh_mode".to_string(),
        "owner".to_string(),
        "query".to_string(),
    ];

    let views = state
        .credentials
        .catalog()
        .list_materialized_views(database_id.as_u64(), tenant_id.as_u64())
        .map_err(|e| err("XX000", format!("catalog read failed: {e}")))?;

    let mut rows = Vec::new();
    for view in &views {
        if let Some(ref filter) = source_filter
            && view.source != *filter
        {
            continue;
        }

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(view.name.clone()));
        row.insert("source".to_string(), JsonValue::String(view.source.clone()));
        row.insert(
            "refresh_mode".to_string(),
            JsonValue::String(view.refresh_mode.clone()),
        );
        row.insert("owner".to_string(), JsonValue::String(view.owner.clone()));
        row.insert(
            "query".to_string(),
            JsonValue::String(view.query_sql.clone()),
        );
        rows.push(row);
    }
    for view in state
        .mv_registry
        .list_for_tenant(database_id, tenant_id.as_u64())
    {
        if let Some(ref filter) = source_filter
            && view.source_stream != *filter
        {
            continue;
        }
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(view.name));
        row.insert("source".to_string(), JsonValue::String(view.source_stream));
        row.insert(
            "refresh_mode".to_string(),
            JsonValue::String("STREAMING".to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(view.owner));
        row.insert("query".to_string(), JsonValue::Null);
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
