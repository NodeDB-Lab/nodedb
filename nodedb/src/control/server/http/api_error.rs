// SPDX-License-Identifier: BUSL-1.1

//! The HTTP API error type and its JSON body.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use nodedb_types::error::sqlstate;

use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;

/// HTTP API error type.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    Forbidden(String),
    BadRequest(String),
    Internal(String),
    /// 429 Too Many Requests with Retry-After header.
    RateLimited {
        message: String,
        retry_after_secs: u64,
    },
    /// 503 Service Unavailable for a transient refusal of SQLSTATE `57P03`,
    /// such as a dispatch capacity refusal. Nothing ran, so the client
    /// retries after `Retry-After`.
    Overloaded {
        message: String,
        code: nodedb_types::error::ErrorCode,
    },
    /// Arbitrary HTTP status from gateway error mapping.
    HttpStatus(u16, String),
    /// An error with its NodeDB numeric code preserved (e.g. a DDL refusal),
    /// so the client's typed reconstruction gets more than a bare message.
    Coded {
        status: StatusCode,
        message: String,
        code: nodedb_types::error::ErrorCode,
        /// The typed error that caused this one, such as the Data-Plane
        /// refusal behind a DDL phase failure. `None` when there is none.
        cause: Option<Box<nodedb_types::NodeDbError>>,
    },
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use super::types::HttpError;

        let plain = |status: StatusCode, message: String| {
            (status, axum::Json(HttpError::new(message))).into_response()
        };

        match self {
            ApiError::RateLimited {
                message,
                retry_after_secs,
            } => {
                let body = HttpError::new(message);
                let resp = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
                with_retry_after(resp, retry_after_secs)
            }
            ApiError::Overloaded { message, code } => {
                let body = HttpError::with_code(message, code.to_string());
                let resp = (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
                overloaded(resp)
            }
            ApiError::Coded {
                status,
                message,
                code,
                cause,
            } => {
                let is_overload = numeric_code_to_sqlstate(code) == sqlstate::SERVER_OVERLOAD;
                let body = HttpError::with_code(message, code.to_string());
                let body = match cause {
                    Some(cause) => body.caused_by(&cause),
                    None => body,
                };
                let resp = (status, axum::Json(body)).into_response();
                if is_overload { overloaded(resp) } else { resp }
            }
            ApiError::Unauthorized(msg) => plain(StatusCode::UNAUTHORIZED, msg),
            ApiError::Forbidden(msg) => plain(StatusCode::FORBIDDEN, msg),
            ApiError::BadRequest(msg) => plain(StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => plain(StatusCode::INTERNAL_SERVER_ERROR, msg),
            ApiError::HttpStatus(code, msg) => plain(
                StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                msg,
            ),
        }
    }
}

/// Seconds a client waits before it retries a `57P03` capacity refusal.
const OVERLOAD_RETRY_AFTER_SECS: u64 = 1;

/// Finish a `57P03` answer: add `Retry-After` and count it under `http`.
/// Every overload answer HTTP sends passes through here.
fn overloaded(resp: Response) -> Response {
    crate::control::metrics::overload_responses()
        .record(crate::control::metrics::OverloadProtocol::Http);
    with_retry_after(resp, OVERLOAD_RETRY_AFTER_SECS)
}

/// Set the `Retry-After` header of `resp` to `secs`.
fn with_retry_after(mut resp: Response, secs: u64) -> Response {
    resp.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from(secs),
    );
    resp
}

/// The status and message come from the one gateway mapping,
/// [`GatewayErrorMap::to_http`](crate::control::gateway::GatewayErrorMap::to_http).
/// A rate refusal and a `57P03` capacity refusal also carry a `Retry-After`
/// hint.
impl From<crate::Error> for ApiError {
    fn from(e: crate::Error) -> Self {
        let (status, message) = crate::control::gateway::GatewayErrorMap::to_http(&e);
        if let crate::Error::RateExceeded { retry_after_ms, .. } = &e {
            return Self::RateLimited {
                message,
                retry_after_secs: retry_after_ms.div_ceil(1000).max(1),
            };
        }
        let (_severity, state, _message) =
            crate::control::server::pgwire::types::error_to_sqlstate(&e);
        if state == sqlstate::SERVER_OVERLOAD {
            return Self::Overloaded {
                message,
                code: crate::error_classify::classify(&e).code(),
            };
        }
        Self::HttpStatus(status, message)
    }
}
