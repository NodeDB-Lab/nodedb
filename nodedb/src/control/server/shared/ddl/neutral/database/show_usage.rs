// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE USAGE FOR <name>`.
//!
//! The tenant-admin gate, catalog lookup, live-gauge reads from
//! `DatabaseMetricsRegistry`, and per-dimension row rendering (`unlimited` limit +
//! `percent_used`) run here. The result is the protocol-neutral
//! [`DdlResult`] over `ShapedRows`. Every column is `Text`.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::QuotaRecord;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_tenant_admin;
use super::support::{ddl_err, text_rows};

/// Rendered where a dimension has no counter, so no number is measurable.
/// Distinct from `0`, which asserts a measured zero.
pub(crate) const UNMEASURED: &str = "n/a";

/// Handle `SHOW DATABASE USAGE FOR <name>`.
pub fn show_database_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database usage")?;

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    let record = catalog
        .get_database_quota(db_id)
        .map_err(|e| DdlError::from_error_in_context("quota read failed", &e))?
        .unwrap_or(QuotaRecord::DEFAULT);

    // Pull live gauges from the database metrics registry. Dimensions without a
    // per-database accounting source land as `0`, which is the gauge's actual
    // value, not a fabricated placeholder.
    let metrics = &state.database_metrics;
    let (cur_memory, cur_storage, cur_queries) = (
        metrics.memory_bytes(db_id),
        metrics.storage_bytes(db_id),
        metrics.qps_total(db_id),
    );
    // Live connections come from the admission registry, which holds one
    // permit per connection admitted to this database. `None` means the
    // database has no `max_connections` cap, so no per-database counter
    // exists — reported as `n/a`, never as a `0`, which reads as
    // "no connections are open".
    let cur_connections = state
        .admission_registry
        .database_live_connections(db_id)
        .map(u64::from);

    let columns = vec![
        "database".to_string(),
        "quota_name".to_string(),
        "limit".to_string(),
        "current".to_string(),
        "percent_used".to_string(),
    ];

    let dims: &[(&str, u64, Option<u64>)] = &[
        (
            "max_memory_bytes",
            record.max_memory_bytes,
            Some(cur_memory),
        ),
        (
            "max_storage_bytes",
            record.max_storage_bytes,
            Some(cur_storage),
        ),
        ("max_qps", record.max_qps as u64, Some(cur_queries)),
        (
            "max_connections",
            record.max_connections as u64,
            cur_connections,
        ),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    for &(quota_name, limit, current) in dims {
        let limit_str = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        let (current_str, pct_str) = match current {
            Some(current) => (current.to_string(), format_percent(limit, current)),
            None => (UNMEASURED.to_string(), UNMEASURED.to_string()),
        };
        let mut row = Map::new();
        row.insert("database".to_string(), JsonValue::String(name.to_string()));
        row.insert(
            "quota_name".to_string(),
            JsonValue::String(quota_name.to_string()),
        );
        row.insert("limit".to_string(), JsonValue::String(limit_str));
        row.insert("current".to_string(), JsonValue::String(current_str));
        row.insert("percent_used".to_string(), JsonValue::String(pct_str));
        rows.push(row);
    }

    Ok(text_rows(columns, rows))
}

/// Render `current / limit` as a `"<n>%"` string.
///
/// `limit == 0` means "unlimited" and renders as `"n/a"` (percentage of
/// infinity is undefined). Otherwise the result is `(current * 100 / limit)`
/// floored, with the divisor pre-promoted to `u128` so the multiply can never
/// overflow even when both inputs are near `u64::MAX`.
pub(crate) fn format_percent(limit: u64, current: u64) -> String {
    if limit == 0 {
        return UNMEASURED.to_string();
    }
    let pct = (u128::from(current) * 100) / u128::from(limit);
    format!("{pct}%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_unlimited_renders_na() {
        assert_eq!(format_percent(0, 100), "n/a");
        assert_eq!(format_percent(0, 0), "n/a");
    }

    #[test]
    fn percent_basic_arithmetic() {
        assert_eq!(format_percent(100, 25), "25%");
        assert_eq!(format_percent(100, 100), "100%");
        assert_eq!(format_percent(100, 200), "200%"); // over-budget surfaces, never silently clamped
        assert_eq!(format_percent(1000, 0), "0%");
    }

    #[test]
    fn percent_does_not_overflow_on_max() {
        // Both at u64::MAX: 100 * MAX / MAX = 100. Pre-u64 arithmetic would overflow.
        assert_eq!(format_percent(u64::MAX, u64::MAX), "100%");
    }
}
