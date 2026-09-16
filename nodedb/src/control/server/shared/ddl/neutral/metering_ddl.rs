// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral usage metering DDL commands.
//!
//! ```sql
//! DEFINE METERING DIMENSION 'api_calls' UNIT 'calls'
//! SHOW USAGE FOR AUTH USER 'user_42'
//! SHOW USAGE FOR ORG 'acme'
//! SHOW QUOTA FOR AUTH USER 'user_42'
//! ```
//!
//! Ported from the pgwire `ddl::metering_ddl` handlers. The usage-store /
//! quota-manager / tenant-usage reads, ordering, and the superuser gates are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral `DdlResult` over
//! `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire metering handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical
/// after the migration off the pgwire router.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// DEFINE METERING DIMENSION '<name>' UNIT '<unit>'
pub fn define_dimension(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 5 {
        return Err(ddl_err(
            "42601",
            "syntax: DEFINE METERING DIMENSION '<name>' UNIT '<unit>'",
        ));
    }
    let _name = parts[3].trim_matches('\'');
    let _unit = parts
        .iter()
        .position(|p| p.to_uppercase() == "UNIT")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''))
        .unwrap_or("tokens");

    // Custom dimensions are stored in config, not in a catalog table.
    // For now, acknowledge the command.
    Ok(vec![DdlResult::Status {
        command: "DEFINE METERING DIMENSION".to_string(),
        rows_affected: None,
    }])
}

/// SHOW USAGE FOR AUTH USER '<id>' / SHOW USAGE FOR ORG '<id>'
pub fn show_usage(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let (user_filter, org_filter) = parse_for_clause(parts);

    let events = state.usage_store.query(
        user_filter.as_deref(),
        org_filter.as_deref(),
        0, // All time.
    );

    let columns = vec![
        "auth_user_id".to_string(),
        "org_id".to_string(),
        "collection".to_string(),
        "operation".to_string(),
        "tokens".to_string(),
        "timestamp".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = events
        .iter()
        .map(|e| {
            let mut row = Map::new();
            row.insert(
                "auth_user_id".to_string(),
                JsonValue::String(e.auth_user_id.clone()),
            );
            row.insert("org_id".to_string(), JsonValue::String(e.org_id.clone()));
            row.insert(
                "collection".to_string(),
                JsonValue::String(e.collection.clone()),
            );
            row.insert(
                "operation".to_string(),
                JsonValue::String(e.operation.clone()),
            );
            row.insert(
                "tokens".to_string(),
                JsonValue::String(e.tokens.to_string()),
            );
            row.insert(
                "timestamp".to_string(),
                JsonValue::String(e.timestamp_secs.to_string()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// SHOW QUOTA FOR AUTH USER '<id>' / SHOW QUOTA FOR ORG '<id>'
pub fn show_quota(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let (user_filter, _org_filter) = parse_for_clause(parts);
    let grantee_id = user_filter.as_deref().unwrap_or("");

    let quotas = state.quota_manager.list_quotas();
    let now_secs = crate::control::security::time::now_secs();

    let columns = vec![
        "scope".to_string(),
        "max_tokens".to_string(),
        "used_tokens".to_string(),
        "remaining".to_string(),
        "pct_used".to_string(),
        "enforcement".to_string(),
        "exceeded".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = quotas
        .iter()
        .filter_map(|q| {
            state
                .quota_manager
                .get_status(&q.scope_name, grantee_id, now_secs)
        })
        .map(|s| {
            let mut row = Map::new();
            row.insert("scope".to_string(), JsonValue::String(s.scope_name.clone()));
            row.insert(
                "max_tokens".to_string(),
                JsonValue::String(s.max_tokens.to_string()),
            );
            row.insert(
                "used_tokens".to_string(),
                JsonValue::String(s.used_tokens.to_string()),
            );
            row.insert(
                "remaining".to_string(),
                JsonValue::String(s.remaining.to_string()),
            );
            row.insert(
                "pct_used".to_string(),
                JsonValue::String(format!("{:.1}%", s.pct_used * 100.0)),
            );
            row.insert(
                "enforcement".to_string(),
                JsonValue::String(format!("{:?}", s.enforcement)),
            );
            row.insert(
                "exceeded".to_string(),
                JsonValue::String(s.exceeded.to_string()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// SHOW USAGE FOR TENANT <id>
///
/// Returns real-time usage snapshot for a tenant from TenantUsage counters.
pub fn show_usage_for_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err("42501", "permission denied: requires superuser"));
    }

    // SHOW USAGE FOR TENANT <id>
    let tid: u64 = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TENANT"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ddl_err("42601", "syntax: SHOW USAGE FOR TENANT <id>"))?;

    let columns = vec!["metric".to_string(), "value".to_string()];

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let mut rows = Vec::new();
    if let Some(usage) = tenants.usage(crate::types::TenantId::new(tid)) {
        let metrics: &[(&str, u64)] = &[
            ("memory_bytes", usage.memory_bytes),
            ("storage_bytes", usage.storage_bytes),
            ("active_requests", usage.active_requests as u64),
            ("qps_current", usage.requests_this_second as u64),
            ("total_requests", usage.total_requests),
            ("rejected_requests", usage.rejected_requests),
            ("active_connections", usage.active_connections as u64),
        ];
        for &(name, val) in metrics {
            let mut row = Map::new();
            row.insert("metric".to_string(), JsonValue::String(name.to_string()));
            row.insert("value".to_string(), JsonValue::String(val.to_string()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// EXPORT USAGE FOR TENANT <id> [PERIOD '<month>'] FORMAT 'json'
///
/// Returns a billing-friendly JSON export of tenant usage from the UsageStore.
pub fn export_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err("42501", "permission denied: requires superuser"));
    }

    // Parse tenant_id.
    let tid: u64 = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TENANT"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            ddl_err(
                "42601",
                "syntax: EXPORT USAGE FOR TENANT <id> [PERIOD '<month>'] FORMAT 'json'",
            )
        })?;

    // Parse optional PERIOD '<month>' (e.g., '2026-03').
    let since_secs = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("PERIOD"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            let s = s.trim_matches('\'');
            // Parse YYYY-MM to epoch seconds.
            let mut iter = s.split('-');
            let year: i32 = iter.next()?.parse().ok()?;
            let month: u32 = iter.next()?.parse().ok()?;
            // Approximate: first day of month at midnight UTC.
            let days_since_epoch =
                (year as i64 - 1970) * 365 + (year as i64 - 1969) / 4 + (month as i64 - 1) * 30;
            Some(days_since_epoch.max(0) as u64 * 86400)
        })
        .unwrap_or(0);

    let json = state.usage_store.export_tenant_json(tid, since_secs);

    let mut row = Map::new();
    row.insert("usage_json".to_string(), JsonValue::String(json));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["usage_json".to_string()],
        vec![row],
    ))])
}

/// Parse FOR AUTH USER '<id>' or FOR ORG '<id>' from parts.
fn parse_for_clause(parts: &[&str]) -> (Option<String>, Option<String>) {
    let for_idx = parts.iter().position(|p| p.to_uppercase() == "FOR");
    let Some(idx) = for_idx else {
        return (None, None);
    };

    let grantee_type = parts
        .get(idx + 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_default();
    match grantee_type.as_str() {
        "AUTH" => {
            // FOR AUTH USER '<id>'
            let id = parts.get(idx + 3).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        "ORG" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (None, id)
        }
        "USER" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        _ => (None, None),
    }
}
