//! `SHOW SCHEDULES` and `SHOW SCHEDULE HISTORY` DDL handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;

use super::super::super::types::text_field;

/// Handle `SHOW SCHEDULES`
pub fn show_schedules(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("cron"),
        text_field("scope"),
        text_field("target"),
        text_field("overlap"),
        text_field("missed_policy"),
        text_field("enabled"),
        text_field("last_status"),
        text_field("next_run"),
        text_field("owner"),
    ]);

    let schedules = state.schedule_registry.list_for_tenant(tenant_id);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut rows = Vec::new();
    for s in &schedules {
        let last_run = state.job_history.last_run(tenant_id, &s.name);
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

        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&s.name);
        let _ = encoder.encode_field(&s.cron_expr);
        let _ = encoder.encode_field(&s.scope.as_str().to_string());
        let target = s.target_collection.as_deref().unwrap_or("*");
        let _ = encoder.encode_field(&target.to_string());
        let _ = encoder.encode_field(&s.allow_overlap.to_string());
        let _ = encoder.encode_field(&s.missed_policy.as_str().to_string());
        let _ = encoder.encode_field(&s.enabled.to_string());
        let _ = encoder.encode_field(&last_status);
        let _ = encoder.encode_field(&next_run);
        let _ = encoder.encode_field(&s.owner);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Handle `SHOW SCHEDULE HISTORY name`
pub fn show_schedule_history(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let name = name.to_lowercase();

    // Verify the schedule exists.
    if state.schedule_registry.get(tenant_id, &name).is_none() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42704".to_owned(),
            format!("schedule \"{name}\" does not exist"),
        ))));
    }

    let schema = Arc::new(vec![
        text_field("schedule"),
        text_field("started_at"),
        text_field("duration_ms"),
        text_field("success"),
        text_field("error"),
    ]);

    let runs = state.job_history.last_runs(tenant_id, &name, 50);

    let mut rows = Vec::with_capacity(runs.len());
    for r in &runs {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&r.schedule_name);
        let _ = encoder.encode_field(&format_epoch_ms(r.started_at));
        let _ = encoder.encode_field(&r.duration_ms.to_string());
        let _ = encoder.encode_field(&r.success.to_string());
        let _ = encoder.encode_field(&r.error.as_deref().unwrap_or("").to_string());
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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
