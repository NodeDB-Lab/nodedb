// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW ALERTS` and `SHOW ALERT STATUS ON <name>` DDL handlers.
//!
//! Ported from the pgwire `ddl::alert::show` handlers. The registry read, the
//! hysteresis-state read, the condition/window/status formatting, and the exact
//! column set are preserved verbatim; only the result construction changed from
//! pgwire `Response` / `QueryResponse` to the protocol-neutral
//! [`DdlResult::Rows`] over [`ShapedRows`]. The mixed text/`int8` column OIDs are
//! reproduced by building `column_types` manually so the RowDescription stays
//! byte-identical (the `int8` cells are emitted as their decimal text form, the
//! same bytes the pgwire `DataRowEncoder::encode_field(&i64)` produced).

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// SHOW ALERTS — list all alert rules for the tenant.
pub fn show_alerts(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let alerts = state
        .alert_registry
        .list_for_tenant_in_database(database_id.as_u64(), tenant_id);

    let columns = vec![
        "name".to_string(),
        "collection".to_string(),
        "condition".to_string(),
        "group_by".to_string(),
        "window".to_string(),
        "fire_after".to_string(),
        "recover_after".to_string(),
        "severity".to_string(),
        "enabled".to_string(),
        "notify_count".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();
    for alert in &alerts {
        let condition_str = format!(
            "{}({}) {} {}",
            alert.condition.agg_func,
            alert.condition.column,
            alert.condition.op.as_sql(),
            alert.condition.threshold,
        );
        let group_by_str = if alert.group_by.is_empty() {
            "-".to_string()
        } else {
            alert.group_by.join(", ")
        };
        let window_str = format_duration_ms(alert.window_ms);

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(alert.name.clone()));
        row.insert(
            "collection".to_string(),
            JsonValue::String(alert.collection.clone()),
        );
        row.insert("condition".to_string(), JsonValue::String(condition_str));
        row.insert("group_by".to_string(), JsonValue::String(group_by_str));
        row.insert("window".to_string(), JsonValue::String(window_str));
        row.insert(
            "fire_after".to_string(),
            JsonValue::String((alert.fire_after as i64).to_string()),
        );
        row.insert(
            "recover_after".to_string(),
            JsonValue::String((alert.recover_after as i64).to_string()),
        );
        row.insert(
            "severity".to_string(),
            JsonValue::String(alert.severity.clone()),
        );
        row.insert(
            "enabled".to_string(),
            JsonValue::String(alert.enabled.to_string()),
        );
        row.insert(
            "notify_count".to_string(),
            JsonValue::String((alert.notify_targets.len() as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW ALERT STATUS ON <name> — per-group active/cleared state.
pub fn show_alert_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = name.to_lowercase();

    // Verify alert exists.
    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .is_none()
    {
        return Err(err("42704", format!("alert '{name}' does not exist")));
    }

    let states = state
        .alert_hysteresis
        .list_states(database_id.as_u64(), tenant_id, &name);

    let columns = vec![
        "group_key".to_string(),
        "status".to_string(),
        "consecutive_fire".to_string(),
        "consecutive_recover".to_string(),
        "last_value".to_string(),
        "fired_at".to_string(),
        "cleared_at".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();
    for (group_key, group_state) in &states {
        let status_str = match group_state.status {
            crate::event::alert::AlertStatus::Active => "ACTIVE",
            crate::event::alert::AlertStatus::Cleared => "CLEARED",
        };
        let last_value = group_state
            .last_value
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "-".to_string());

        let mut row = Map::new();
        row.insert(
            "group_key".to_string(),
            JsonValue::String(group_key.clone()),
        );
        row.insert(
            "status".to_string(),
            JsonValue::String(status_str.to_string()),
        );
        row.insert(
            "consecutive_fire".to_string(),
            JsonValue::String((group_state.consecutive_fire as i64).to_string()),
        );
        row.insert(
            "consecutive_recover".to_string(),
            JsonValue::String((group_state.consecutive_recover as i64).to_string()),
        );
        row.insert("last_value".to_string(), JsonValue::String(last_value));
        row.insert(
            "fired_at".to_string(),
            JsonValue::String(
                group_state
                    .fired_at
                    .map(|t| t as i64)
                    .unwrap_or(0)
                    .to_string(),
            ),
        );
        row.insert(
            "cleared_at".to_string(),
            JsonValue::String(
                group_state
                    .cleared_at
                    .map(|t| t as i64)
                    .unwrap_or(0)
                    .to_string(),
            ),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

fn format_duration_ms(ms: u64) -> String {
    const MINUTE: u64 = 60_000;
    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 86_400_000;

    if ms.is_multiple_of(DAY) {
        format!("{}d", ms / DAY)
    } else if ms.is_multiple_of(HOUR) {
        format!("{}h", ms / HOUR)
    } else if ms.is_multiple_of(MINUTE) {
        format!("{}m", ms / MINUTE)
    } else if ms.is_multiple_of(1_000) {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}
