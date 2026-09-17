// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral audit-log SHOW commands: `SHOW AUDIT LOG`,
//! `SHOW AUDIT WHERE`, `SHOW AUDIT IN DATABASE`, and `EXPORT AUDIT`.
//!
//! Ported from the pgwire `ddl::inspect_audit` handlers. The catalog /
//! in-memory audit-log reads, ordering (most-recent-first), and the
//! catalog fall-through scan are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire audit handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical
/// after the migration off the pgwire router.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Shared column names + types for all audit SHOW commands.
fn audit_columns() -> (Vec<String>, Vec<DdlColType>) {
    (
        vec![
            "seq".to_string(),
            "timestamp_us".to_string(),
            "event".to_string(),
            "tenant_id".to_string(),
            "database_id".to_string(),
            "source".to_string(),
            "detail".to_string(),
        ],
        vec![
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Text,
            DdlColType::Text,
        ],
    )
}

/// SHOW AUDIT LOG [LIMIT <n>]
///
/// Shows recent persisted audit entries. Superuser only.
pub fn show_audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view audit log",
        ));
    }

    let limit = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("LIMIT") {
        parts[4].parse::<usize>().unwrap_or(100)
    } else {
        100
    };

    let catalog = state.credentials.catalog();

    let entries = catalog
        .load_recent_audit_entries(limit)
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

    let (columns, column_types) = audit_columns();
    let mut rows = Vec::with_capacity(entries.len());

    for entry in entries.iter().rev() {
        // Most recent first.
        let mut row = Map::new();
        row.insert(
            "seq".to_string(),
            JsonValue::String((entry.seq as i64).to_string()),
        );
        row.insert(
            "timestamp_us".to_string(),
            JsonValue::String((entry.timestamp_us as i64).to_string()),
        );
        row.insert("event".to_string(), JsonValue::String(entry.event.clone()));
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((entry.tenant_id.unwrap_or(0) as i64).to_string()),
        );
        row.insert(
            "database_id".to_string(),
            JsonValue::String((entry.database_id.unwrap_or(0) as i64).to_string()),
        );
        row.insert(
            "source".to_string(),
            JsonValue::String(entry.source.clone()),
        );
        row.insert(
            "detail".to_string(),
            JsonValue::String(entry.detail.clone()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW AUDIT WHERE event_type = '<snake_name>'
///
/// Filters in-memory audit entries by event type.
/// The filter value must be the snake_case event name, e.g.
/// `'permission_denied'`, `'rls_rejected'`, `'lockout_triggered'`.
pub fn show_audit_where(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view audit log",
        ));
    }

    // Parse: SHOW AUDIT WHERE event_type = '<value>' [LIMIT <n>]
    // parts: ["SHOW", "AUDIT", "WHERE", "event_type", "=", "'permission_denied'", ...]
    let event_filter = if parts.len() >= 6 && parts[3].eq_ignore_ascii_case("event_type") {
        parts[5].trim_matches('\'').to_ascii_lowercase()
    } else {
        return Err(ddl_err(
            "42601",
            "syntax: SHOW AUDIT WHERE event_type = '<event_name>' [LIMIT <n>]",
        ));
    };

    let limit = if parts.len() >= 8 && parts[6].eq_ignore_ascii_case("LIMIT") {
        parts[7].parse::<usize>().map_err(|_| {
            ddl_err(
                "42601",
                "syntax: SHOW AUDIT WHERE event_type = '<event_name>' [LIMIT <n>] (LIMIT must be a non-negative integer)",
            )
        })?
    } else {
        100
    };

    let log = match state.audit.lock() {
        Ok(l) => l,
        Err(p) => p.into_inner(),
    };

    let (columns, column_types) = audit_columns();
    let all = log.all();
    let mut rows = Vec::new();

    for entry in all.iter().rev() {
        if rows.len() >= limit {
            break;
        }
        if entry.event.snake_name() != event_filter {
            continue;
        }
        let mut row = Map::new();
        row.insert(
            "seq".to_string(),
            JsonValue::String((entry.seq as i64).to_string()),
        );
        row.insert(
            "timestamp_us".to_string(),
            JsonValue::String((entry.timestamp_us as i64).to_string()),
        );
        row.insert(
            "event".to_string(),
            JsonValue::String(entry.event.snake_name().to_string()),
        );
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((entry.tenant_id.map_or(0i64, |t| t.as_u64() as i64)).to_string()),
        );
        row.insert(
            "database_id".to_string(),
            JsonValue::String((entry.database_id.map_or(0i64, |d| d.as_u64() as i64)).to_string()),
        );
        row.insert(
            "source".to_string(),
            JsonValue::String(entry.source.clone()),
        );
        row.insert(
            "detail".to_string(),
            JsonValue::String(entry.detail.clone()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// `SHOW AUDIT IN DATABASE <name> [LIMIT <n>]`
///
/// Returns all in-memory audit entries whose `database_id` matches the
/// named database. Falls back to a full-scan of the catalog when the
/// in-memory window is exhausted and a persistent catalog is available.
///
/// Superuser only.
pub fn show_audit_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_name: &str,
    limit: usize,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view audit log",
        ));
    }

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(db_name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{db_name}' does not exist")))?;

    let (columns, column_types) = audit_columns();
    let mut rows = Vec::new();

    // Scan the in-memory log first.
    let log = match state.audit.lock() {
        Ok(l) => l,
        Err(p) => p.into_inner(),
    };
    for entry in log.query_by_database(db_id).into_iter().rev() {
        if rows.len() >= limit {
            break;
        }
        let mut row = Map::new();
        row.insert(
            "seq".to_string(),
            JsonValue::String((entry.seq as i64).to_string()),
        );
        row.insert(
            "timestamp_us".to_string(),
            JsonValue::String((entry.timestamp_us as i64).to_string()),
        );
        row.insert(
            "event".to_string(),
            JsonValue::String(entry.event.snake_name().to_string()),
        );
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((entry.tenant_id.map_or(0i64, |t| t.as_u64() as i64)).to_string()),
        );
        row.insert(
            "database_id".to_string(),
            JsonValue::String((entry.database_id.map_or(0i64, |d| d.as_u64() as i64)).to_string()),
        );
        row.insert(
            "source".to_string(),
            JsonValue::String(entry.source.clone()),
        );
        row.insert(
            "detail".to_string(),
            JsonValue::String(entry.detail.clone()),
        );
        rows.push(row);
    }
    drop(log);

    // If the in-memory log didn't fill the limit, scan the catalog.
    if rows.len() < limit {
        let remaining = limit - rows.len();
        let all_entries = catalog
            .load_recent_audit_entries(remaining * 10)
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        for entry in all_entries.iter().rev() {
            if rows.len() >= limit {
                break;
            }
            if entry.database_id != Some(db_id.as_u64()) {
                continue;
            }
            let mut row = Map::new();
            row.insert(
                "seq".to_string(),
                JsonValue::String((entry.seq as i64).to_string()),
            );
            row.insert(
                "timestamp_us".to_string(),
                JsonValue::String((entry.timestamp_us as i64).to_string()),
            );
            row.insert("event".to_string(), JsonValue::String(entry.event.clone()));
            row.insert(
                "tenant_id".to_string(),
                JsonValue::String((entry.tenant_id.unwrap_or(0) as i64).to_string()),
            );
            row.insert(
                "database_id".to_string(),
                JsonValue::String((entry.database_id.unwrap_or(0) as i64).to_string()),
            );
            row.insert(
                "source".to_string(),
                JsonValue::String(entry.source.clone()),
            );
            row.insert(
                "detail".to_string(),
                JsonValue::String(entry.detail.clone()),
            );
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// Audit entries are read with a regular `SELECT` query against
/// `system.audit_log`; the client redirects the result.
pub fn export_audit_log(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can export audit log",
        ));
    }
    Err(ddl_err(
        "0A000",
        "use `SELECT ... FROM system.audit_log` and redirect the query \
         result on the client",
    ))
}
