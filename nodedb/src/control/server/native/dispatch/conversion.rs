// SPDX-License-Identifier: BUSL-1.1

//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::conversion::json_to_value_display;
use nodedb_types::protocol::NativeResponse;

use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::{DdlError, DdlResult};

/// Convert a crate-level error into a NativeResponse.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::RateExceeded { .. } => (
            nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
            format!("{e}"),
        ),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        // A cross-shard Calvin OCC abort is a serialization failure (40001) —
        // the client should retry the whole transaction.
        crate::Error::CalvinSerializationConflict => (
            nodedb_types::error::sqlstate::SERIALIZATION_FAILURE,
            format!("{e}"),
        ),
        other => ("XX000", format!("{other}")),
    };
    NativeResponse::error(seq, code, message)
}

/// Convert a `NodeDbError` produced while shaping a response into a
/// NativeResponse error frame.
pub(crate) fn shape_error_to_native(seq: u64, e: &nodedb_types::NodeDbError) -> NativeResponse {
    NativeResponse::error(seq, "XX000", e.message().to_string())
}

/// Encode a protocol-neutral DDL dispatch result into a single
/// `NativeResponse`.
///
/// Reduction mirrors the previous pgwire→native bridge: on error, an error
/// frame carrying the neutral SQLSTATE + message; otherwise the first
/// row-returning / status / empty result determines the response (a status tag
/// becomes a single-column status row, a row result becomes a columns+rows
/// frame, an empty result or an empty vec becomes a bare OK).
pub(crate) fn ddl_result_to_native(
    seq: u64,
    result: Result<Vec<DdlResult>, DdlError>,
) -> NativeResponse {
    match result {
        Err(DdlError { sqlstate, message }) => NativeResponse::error(seq, sqlstate, message),
        // Unknown pgwire response variants are dropped during translation, so
        // the first element is the first meaningful result — mirroring the
        // previous bridge, which returned on the first known variant.
        Ok(results) => match results.into_iter().next() {
            Some(DdlResult::Status { command, .. }) => NativeResponse::status_row(seq, command),
            Some(DdlResult::Rows(shaped)) => {
                let (columns, rows) = to_native_columns_rows(&shaped);
                NativeResponse {
                    seq,
                    status: nodedb_types::protocol::ResponseStatus::Ok,
                    columns: Some(columns),
                    rows: Some(rows),
                    rows_affected: None,
                    watermark_lsn: 0,
                    error: None,
                    auth: None,
                    warnings: Vec::new(),
                }
            }
            Some(DdlResult::Empty) | None => NativeResponse::ok(seq),
        },
    }
}

/// Build the native response for a completed Calvin transaction, surfacing
/// RETURNING rows when the write carried them.
///
/// `apply_result` is the applied Data-Plane response drained from the sidecar
/// (`None` for a plain write) and `returning_plan` is the RETURNING doc task's
/// plan (used to derive the shaping kind). When both are present and the plan is
/// a RETURNING write, the payload is shaped into native columns/rows; otherwise
/// the response falls back to `fallback_affected` rows-affected — matching the
/// non-Calvin native DML path.
pub(crate) fn calvin_native_response(
    seq: u64,
    apply_result: Option<crate::bridge::envelope::Response>,
    returning_plan: Option<&crate::bridge::envelope::PhysicalPlan>,
    fallback_affected: u64,
    state: &crate::control::state::SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
) -> NativeResponse {
    use crate::control::server::response_shape::compose::{
        ShapeOutcome, shape_response_materialized,
    };
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};

    if let (Some(resp), Some(plan)) = (apply_result.as_ref(), returning_plan)
        && matches!(describe_plan(plan), PlanKind::ReturningRows)
        && let Ok(ShapeOutcome::Rows(shaped)) = shape_response_materialized(
            resp.payload.as_bytes(),
            plan,
            PlanKind::ReturningRows,
            None,
            state,
            database_id,
            tenant_id,
        )
    {
        let (cols, rows) = to_native_columns_rows(&shaped);
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        if !cols.is_empty() {
            r.columns = Some(cols);
        }
        r.rows = Some(rows);
        return r;
    }

    // Plain write with a deposited applied Response: surface its ACTUAL affected
    // count + watermark from the payload rather than the caller's fallback
    // estimate. `None` (multishard, undeposited) keeps the fallback.
    let mut r = NativeResponse::ok(seq);
    if let Some(resp) = &apply_result {
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        r.rows_affected = Some(
            crate::control::server::shared::sql::staging_predicates::extract_affected_count(
                resp.payload.as_bytes(),
            )
            .unwrap_or(fallback_affected),
        );
    } else {
        r.rows_affected = Some(fallback_affected);
    }
    r
}

/// Convert protocol-neutral `ShapedRows` (produced by
/// `response_shape::compose::shape_response_materialized`) into native wire
/// columns/rows: each JSON scalar cell becomes a typed `Value` via
/// `json_to_value_display`; a column absent from a given row's map becomes
/// `Value::Null`.
pub(crate) fn to_native_columns_rows(shaped: &ShapedRows) -> (Vec<String>, Vec<Vec<Value>>) {
    let rows = shaped
        .rows
        .iter()
        .map(|row| {
            shaped
                .columns
                .iter()
                .map(|col| {
                    row.get(col.as_str())
                        .map(json_to_value_display)
                        .unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect();
    (shaped.columns.clone(), rows)
}
