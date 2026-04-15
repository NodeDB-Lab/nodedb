//! `ANALYZE collection [(col1, col2)]` — collect column statistics.
//!
//! Dispatches a scan query to the Data Plane, collects all rows as JSON,
//! then passes them to `stats_collector` which uses SIMD kernels for
//! min/max computation. Results stored in the system catalog for
//! DataFusion cost-based optimization.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `ANALYZE collection [(col1, col2)]`.
///
/// Scans the collection via the Data Plane, computes per-column statistics
/// using SIMD-accelerated kernels, and stores them in the system catalog.
pub async fn handle_analyze(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let collection = parts
        .get(1)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "ANALYZE requires a collection name".to_owned(),
            )))
        })?
        .to_lowercase();

    let specific_columns = parse_column_list(sql);

    let catalog = state.credentials.catalog().as_ref().ok_or_else(|| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            "catalog not available".to_owned(),
        )))
    })?;

    let coll = catalog
        .get_collection(tenant_id, &collection)
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("catalog error: {e}"),
            )))
        })?
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("collection \"{collection}\" does not exist"),
            )))
        })?;

    let columns_to_analyze: Vec<String> = if specific_columns.is_empty() {
        coll.fields.iter().map(|(name, _)| name.clone()).collect()
    } else {
        specific_columns
    };

    // Dispatch a scan to the Data Plane to collect all rows.
    let scan_sql = format!("SELECT * FROM {collection}");
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let rows = match query_ctx.plan_sql(&scan_sql, identity.tenant_id).await {
        Ok(tasks) => {
            let mut json_rows = Vec::new();
            for task in tasks {
                let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    state,
                    task.tenant_id,
                    task.vshard_id,
                    task.plan,
                    0,
                )
                .await;
                if let Ok(resp) = resp
                    && !resp.payload.is_empty()
                {
                    let json = crate::data::executor::response_codec::decode_payload_to_json(
                        &resp.payload,
                    );
                    json_rows.push(json);
                }
            }
            json_rows
        }
        Err(_) => Vec::new(), // Scan failed — store zero stats.
    };

    let now = now_ms();

    if !columns_to_analyze.is_empty() && !rows.is_empty() {
        // Use stats_collector to compute real statistics via SIMD kernels.
        let computed = super::stats_collector::collect_stats_from_json_rows(
            tenant_id,
            &collection,
            &columns_to_analyze,
            &rows,
            now,
        );
        for stats in &computed {
            catalog.put_column_stats(stats).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("failed to store column stats: {e}"),
                )))
            })?;
        }
    } else {
        // No rows or no fields — store metadata-only stats.
        for col_name in &columns_to_analyze {
            let stats = crate::control::security::catalog::column_stats::StoredColumnStats {
                tenant_id,
                collection: collection.clone(),
                column: col_name.clone(),
                row_count: rows.len() as u64,
                null_count: 0,
                distinct_count: 0,
                min_value: None,
                max_value: None,
                avg_value_len: None,
                analyzed_at: now,
            };
            let _ = catalog.put_column_stats(&stats);
        }
        if columns_to_analyze.is_empty() {
            let stats = crate::control::security::catalog::column_stats::StoredColumnStats {
                tenant_id,
                collection: collection.clone(),
                column: "*".to_string(),
                row_count: rows.len() as u64,
                null_count: 0,
                distinct_count: 0,
                min_value: None,
                max_value: None,
                avg_value_len: None,
                analyzed_at: now,
            };
            let _ = catalog.put_column_stats(&stats);
        }
    }

    state.dml_counter.reset(tenant_id, &collection);

    tracing::info!(
        %collection,
        columns = columns_to_analyze.len(),
        rows_scanned = rows.len(),
        "ANALYZE completed"
    );
    Ok(vec![Response::Execution(Tag::new("ANALYZE"))])
}

/// Parse optional `(col1, col2)` column list from ANALYZE statement.
fn parse_column_list(sql: &str) -> Vec<String> {
    if let Some(paren_start) = sql.find('(')
        && let Some(paren_end) = sql.rfind(')')
    {
        let inner = &sql[paren_start + 1..paren_end];
        return inner
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
    }
    Vec::new()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
