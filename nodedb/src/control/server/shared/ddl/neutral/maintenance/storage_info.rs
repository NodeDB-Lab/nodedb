// SPDX-License-Identifier: BUSL-1.1

//! `SHOW STORAGE FOR collection` and `SHOW COMPACTION STATUS` handlers.
//!
//! Ported from the pgwire maintenance handlers. Both result sets are all-text
//! columns (`text_field`), so the protocol-neutral [`ShapedRows`] carries
//! `DdlColType::Text` per column and each cell as its `String` form — the same
//! bytes `DataRowEncoder::encode_field(&str)` produced, keeping the
//! RowDescription and DataRow output byte-identical.

use nodedb_types::DatabaseId;

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Handle `SHOW STORAGE FOR collection`.
pub fn handle_show_storage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    // SHOW STORAGE FOR collection
    let collection = parse_ident_token(
        parts
            .get(3)
            .ok_or_else(|| ddl_err("42601", "syntax: SHOW STORAGE FOR <collection>"))?,
    )?;

    let tenant_id = identity.tenant_id.as_u64();

    // Verify collection exists.
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, &collection)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(ddl_err(
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ));
    }

    // Load column stats if available (from last ANALYZE).
    let stats = state
        .credentials
        .catalog()
        .load_column_stats(database_id.as_u64(), tenant_id, &collection)
        .ok()
        .unwrap_or_default();

    let columns = vec![
        "collection".to_string(),
        "columns".to_string(),
        "row_count".to_string(),
        "last_analyzed".to_string(),
    ];

    let row_count = stats.first().map(|s| s.row_count).unwrap_or(0);
    let last_analyzed = stats
        .first()
        .map(|s| {
            if s.analyzed_at > 0 {
                format!("{}ms ago", now_ms().saturating_sub(s.analyzed_at))
            } else {
                "never".to_string()
            }
        })
        .unwrap_or_else(|| "never".to_string());

    let mut row = Map::new();
    row.insert(
        "collection".to_string(),
        JsonValue::String(collection.clone()),
    );
    row.insert(
        "columns".to_string(),
        JsonValue::String(stats.len().to_string()),
    );
    row.insert(
        "row_count".to_string(),
        JsonValue::String(row_count.to_string()),
    );
    row.insert(
        "last_analyzed".to_string(),
        JsonValue::String(last_analyzed),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}

/// Handle `SHOW COMPACTION STATUS`.
pub fn handle_show_compaction_status(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let columns = vec![
        "status".to_string(),
        "pending_jobs".to_string(),
        "compaction_debt".to_string(),
    ];

    // Compaction runs automatically in the Data Plane. We report the current
    // state as "idle" — detailed stats require Data Plane query support.
    let mut row = Map::new();
    row.insert("status".to_string(), JsonValue::String("idle".to_string()));
    row.insert(
        "pending_jobs".to_string(),
        JsonValue::String("0".to_string()),
    );
    row.insert(
        "compaction_debt".to_string(),
        JsonValue::String("0".to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
