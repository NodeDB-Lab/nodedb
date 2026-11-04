// SPDX-License-Identifier: BUSL-1.1

//! Response builders for the NDJSON query route.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::control::server::http::routes::ndjson_error::error_value;

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
