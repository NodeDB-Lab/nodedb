// SPDX-License-Identifier: BUSL-1.1

//! `ANALYZE collection [(col1, col2)]` — collect column statistics.
//!
//! Dispatches a scan query to the Data Plane, collects all rows as JSON,
//! then passes them to `stats_collector` which uses SIMD kernels for
//! min/max computation. Results stored in the system catalog for
//! DataFusion cost-based optimization.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::column_stats::StoredColumnStats;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Handle `ANALYZE collection [(col1, col2)]`.
///
/// Scans the collection via the Data Plane, computes per-column statistics
/// using SIMD-accelerated kernels, and replicates them as one catalog entry.
///
/// The scan reaches every vShard leader, so the numbers describe the whole
/// collection. The planner runs on whichever node received the query. The
/// rows go through the metadata raft group, so every node costs alike.
pub async fn handle_analyze(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let collection = parse_ident_token(
        parts
            .get(1)
            .ok_or_else(|| ddl_err("42601", "ANALYZE requires a collection name"))?,
    )?;

    let specific_columns = parse_column_list(sql)?;

    let catalog = state.credentials.catalog();

    let coll = catalog
        .get_collection(database_id, tenant_id, &collection)
        .map_err(|e| DdlError::from_error_in_context("catalog error", &e))?
        .ok_or_else(|| {
            ddl_err(
                "42P01",
                format!("collection \"{collection}\" does not exist"),
            )
        })?;

    let columns_to_analyze: Vec<String> = if specific_columns.is_empty() {
        coll.fields.iter().map(|(name, _)| name.clone()).collect()
    } else {
        specific_columns
    };

    // Dispatch a scan to the Data Plane to collect all rows.
    let scan_sql = format!("SELECT * FROM {}", ::nodedb_types::quote_ident(&collection));
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            &scan_sql,
            database_id,
        )
        .await?;
    let mut rows: Vec<ShapedRow> = Vec::new();
    for task in tasks {
        let row_context = crate::bridge::envelope::describe_row_context(&task.plan);
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        let resp = crate::control::server::shared::clone_write::intercept_authorize_and_dispatch(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state,
                task,
                identity,
                tenant_id: identity.tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &emitter,
            },
            TraceId::ZERO,
        )
        .await
        .map_err(|error| DdlError::from_error_in_context("ANALYZE scan failed", &error))?;
        if !resp.payload.is_empty() {
            push_scan_rows(&resp.payload, row_context, &mut rows)?;
        }
    }

    let now = now_ms();

    // One vector carries every column, so a planner reads the whole set or
    // none of it.
    let stats_rows: Vec<StoredColumnStats> = if !columns_to_analyze.is_empty() && !rows.is_empty() {
        // Use stats_collector to compute real statistics via SIMD kernels.
        super::stats_collector::collect_stats_from_rows(
            database_id.as_u64(),
            tenant_id,
            &collection,
            &columns_to_analyze,
            &rows,
            now,
        )
    } else {
        // No rows or no fields — store metadata-only stats. An empty column
        // list still records the collection's row count under `*`.
        let metadata_columns: Vec<String> = if columns_to_analyze.is_empty() {
            vec!["*".to_string()]
        } else {
            columns_to_analyze.clone()
        };
        metadata_columns
            .into_iter()
            .map(|column| StoredColumnStats {
                database_id: database_id.as_u64(),
                tenant_id,
                collection: collection.clone(),
                column,
                row_count: rows.len() as u64,
                null_count: 0,
                distinct_count: 0,
                min_value: None,
                max_value: None,
                avg_value_len: None,
                analyzed_at: now,
            })
            .collect()
    };

    let entry = CatalogEntry::PutColumnStats(Box::new(stats_rows));
    super::super::replicate::propose_and_apply_async(state, &entry).await?;

    state
        .dml_counter
        .reset(database_id.as_u64(), tenant_id, &collection);

    tracing::info!(
        %collection,
        columns = columns_to_analyze.len(),
        rows_scanned = rows.len(),
        "ANALYZE completed"
    );
    Ok(vec![DdlResult::Status {
        command: "ANALYZE".to_string(),
        rows_affected: None,
    }])
}

/// Decode one task's scan payload into typed rows, unwrapping scan
/// envelopes under the row shape `context` its plan produces. A payload that
/// does not decode fails the ANALYZE.
fn push_scan_rows(
    payload: &[u8],
    context: crate::data::executor::response_codec::RowContext,
    rows: &mut Vec<ShapedRow>,
) -> Result<(), DdlError> {
    let decoded = crate::data::executor::response_codec::decode_row_payload_value(payload, context)
        .map_err(|e| DdlError::from_error_in_context("ANALYZE scan rows do not decode", &e))?;
    crate::control::server::response_shape::project::push_row_bodies(decoded, context, rows);
    Ok(())
}

/// Parse optional `(col1, col2)` column list from ANALYZE statement.
///
/// An empty entry carries no column, so it drops before the identifier check.
fn parse_column_list(sql: &str) -> Result<Vec<String>, DdlError> {
    if let Some(paren_start) = sql.find('(')
        && let Some(paren_end) = sql.rfind(')')
    {
        let inner = &sql[paren_start + 1..paren_end];
        return inner
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(parse_ident_token)
            .collect();
    }
    Ok(Vec::new())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::parse_column_list;

    #[test]
    fn column_list_lowercases_bare_names() {
        assert_eq!(
            parse_column_list("ANALYZE metrics (Ts, VALUE)").expect("bare columns"),
            vec!["ts".to_string(), "value".to_string()]
        );
    }

    #[test]
    fn column_list_preserves_quoted_case() {
        assert_eq!(
            parse_column_list("ANALYZE metrics (\"Ts\", \"MiXeD\")").expect("quoted columns"),
            vec!["Ts".to_string(), "MiXeD".to_string()]
        );
    }

    #[test]
    fn no_column_list_yields_no_columns() {
        assert!(
            parse_column_list("ANALYZE metrics")
                .expect("no column list")
                .is_empty()
        );
    }

    #[test]
    fn malformed_column_name_is_rejected() {
        assert!(parse_column_list("ANALYZE metrics (\"unterminated)").is_err());
    }
}
