// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW SCHEDULES` and `SHOW SCHEDULE HISTORY` DDL handlers.
//!
//! Ported from the pgwire `ddl::schedule::show` handlers. The registry read,
//! `job_history` lookups, next-fire computation, and the exact column set /
//! per-column text encoding are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral [`DdlResult::Rows`] over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW SCHEDULES`
pub fn show_schedules(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "name".to_string(),
        "cron".to_string(),
        "scope".to_string(),
        "target".to_string(),
        "overlap".to_string(),
        "missed_policy".to_string(),
        "enabled".to_string(),
        "last_status".to_string(),
        "next_run".to_string(),
        "owner".to_string(),
    ];

    let schedules = state
        .schedule_registry
        .list_for_tenant(database_id, tenant_id);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut rows = Vec::new();
    for s in &schedules {
        let last_run = state
            .job_history
            .last_run(database_id.as_u64(), tenant_id, &s.name);
        let last_status = match last_run {
            Some(ref r) if r.success => "ok".to_string(),
            Some(ref r) => format!("error: {}", r.error.as_deref().unwrap_or("unknown")),
            None => "never".to_string(),
        };

        let next_run = if s.enabled {
            CronExpr::parse(&s.cron_expr)
                .ok()
                .and_then(|cron| cron.next_fire_after(now_secs))
                .map(format_epoch)
                .unwrap_or_else(|| "-".to_string())
        } else {
            "disabled".to_string()
        };

        let target = s.target_collection.as_deref().unwrap_or("*");

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(s.name.clone()));
        row.insert("cron".to_string(), JsonValue::String(s.cron_expr.clone()));
        row.insert(
            "scope".to_string(),
            JsonValue::String(s.scope.as_str().to_string()),
        );
        row.insert("target".to_string(), JsonValue::String(target.to_string()));
        row.insert(
            "overlap".to_string(),
            JsonValue::String(s.allow_overlap.to_string()),
        );
        row.insert(
            "missed_policy".to_string(),
            JsonValue::String(s.missed_policy.as_str().to_string()),
        );
        row.insert(
            "enabled".to_string(),
            JsonValue::String(s.enabled.to_string()),
        );
        row.insert("last_status".to_string(), JsonValue::String(last_status));
        row.insert("next_run".to_string(), JsonValue::String(next_run));
        row.insert("owner".to_string(), JsonValue::String(s.owner.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Handle `SHOW SCHEDULE HISTORY name`
pub fn show_schedule_history(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = name.to_lowercase();

    // Verify the schedule exists.
    if state
        .schedule_registry
        .get(database_id, tenant_id, &name)
        .is_none()
    {
        return Err(DdlError::new(
            "42704",
            format!("schedule \"{name}\" does not exist"),
        ));
    }

    let columns = vec![
        "schedule".to_string(),
        "started_at".to_string(),
        "duration_ms".to_string(),
        "success".to_string(),
        "error".to_string(),
    ];

    let runs = state
        .job_history
        .last_runs(database_id.as_u64(), tenant_id, &name, 50);

    let mut rows = Vec::with_capacity(runs.len());
    for r in &runs {
        let mut row = Map::new();
        row.insert(
            "schedule".to_string(),
            JsonValue::String(r.schedule_name.clone()),
        );
        row.insert(
            "started_at".to_string(),
            JsonValue::String(format_epoch_ms(r.started_at)),
        );
        row.insert(
            "duration_ms".to_string(),
            JsonValue::String(r.duration_ms.to_string()),
        );
        row.insert(
            "success".to_string(),
            JsonValue::String(r.success.to_string()),
        );
        row.insert(
            "error".to_string(),
            JsonValue::String(r.error.as_deref().unwrap_or("").to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Format epoch seconds as ISO 8601 UTC string.
///
/// Uses Howard Hinnant's civil-from-days algorithm for date conversion.
/// See: <https://howardhinnant.github.io/date_algorithms.html>
fn format_epoch(epoch_secs: u64) -> String {
    let secs = epoch_secs as i64;
    let days = secs / 86_400;
    let day_secs = secs % 86_400;
    let hour = day_secs / 3600;
    let minute = (day_secs % 3600) / 60;
    let second = day_secs % 60;

    // Civil date from day count.
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Format epoch milliseconds as ISO 8601 UTC string.
fn format_epoch_ms(epoch_ms: u64) -> String {
    format_epoch(epoch_ms / 1000)
}
