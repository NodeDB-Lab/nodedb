// SPDX-License-Identifier: BUSL-1.1

//! Response builders for the NDJSON query route.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::mirror::source::sync::await_mirror_sync;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::http::routes::ndjson_error::{error_line, error_value};
use crate::control::server::shared::metering::{DetachedMeterGuard, PlanMeteringInfo};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// One line per row.
pub(super) fn rows_to_ndjson(rows: Vec<serde_json::Value>) -> String {
    let mut ndjson = String::new();
    for row in rows {
        ndjson.push_str(&row.to_string());
        ndjson.push('\n');
    }
    ndjson
}

/// The guard a streaming body owns, so only rows actually sent are billed. A
/// stream holds a single task.
pub(super) fn stream_meter_guard(
    state: &Arc<SharedState>,
    scope: &RequestAuthScope<'_>,
    tasks: &[PhysicalTask],
) -> Option<DetachedMeterGuard> {
    if !state.metering_config.enabled {
        return None;
    }
    let [task] = tasks else {
        return None;
    };
    DetachedMeterGuard::new(state, scope, &PlanMeteringInfo::extract(&task.plan))
}

/// The error line of a statement whose sync mirror did not confirm it.
pub(super) async fn sync_timeout_line(
    state: &SharedState,
    database: Option<DatabaseId>,
) -> Option<String> {
    await_mirror_sync(state, database?)
        .await
        .err()
        .map(|error| error_line(&error))
}

/// A materialized NDJSON body with the request's rate-limit headers.
pub(super) fn ndjson_response(
    ndjson: String,
    rate_limit_headers: axum::http::HeaderMap,
) -> Response {
    let mut response = Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response());
    response.headers_mut().extend(rate_limit_headers);
    response
}

/// The `429` JSON body for a tenant quota refusal.
pub(super) fn quota_rejection(error: &crate::Error) -> Response {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("Retry-After", "1")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(error_value(error).to_string()))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response())
}
