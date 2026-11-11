// SPDX-License-Identifier: BUSL-1.1

//! The HTTP API error type and its JSON body.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use nodedb_types::error::sqlstate;

use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;
use crate::control::server::shared::statement_exec::{RequestError, StatementAt};

use super::types::HttpError;

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
    /// `error` stopped statement `statement` of a request of several
    /// statements. The answer is the answer of `error`, with the body keys
    /// `statement` and `position` added.
    AtStatement {
        error: Box<ApiError>,
        /// The 1-based statement number.
        statement: usize,
        /// The 1-based character offset in the request text where the
        /// statement starts. `None` when the text gives no offset.
        position: Option<usize>,
    },
}

impl ApiError {
    /// The HTTP error for `error`. `sql` is the request text the statement
    /// index counts in. An error with no statement converts as
    /// `From<crate::Error>` does.
    pub(crate) fn from_request(error: RequestError, sql: &str) -> Self {
        let RequestError { error, at } = error;
        Self::from(*error).at_statement(at, sql)
    }

    /// This error, raised by statement `at` of the request text `sql`.
    /// `None` answers this error as it is.
    pub(crate) fn at_statement(self, at: Option<StatementAt>, sql: &str) -> Self {
        match at {
            Some(at) => Self::AtStatement {
                error: Box::new(self),
                statement: at.number(),
                position: at.position(sql),
            },
            None => self,
        }
    }
}

/// What an answer adds after its body.
enum Finish {
    Plain,
    RetryAfter(u64),
    /// A `57P03` answer: `Retry-After` and the overload metric.
    Overloaded,
}

impl ApiError {
    /// The status, the JSON body and the finish of this error's answer.
    fn into_parts(self) -> (StatusCode, HttpError, Finish) {
        match self {
            ApiError::RateLimited {
                message,
                retry_after_secs,
            } => (
                StatusCode::TOO_MANY_REQUESTS,
                HttpError::new(message),
                Finish::RetryAfter(retry_after_secs),
            ),
            ApiError::Overloaded { message, code } => (
                StatusCode::SERVICE_UNAVAILABLE,
                HttpError::with_code(message, code.to_string()),
                Finish::Overloaded,
            ),
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
                let finish = if is_overload {
                    Finish::Overloaded
                } else {
                    Finish::Plain
                };
                (status, body, finish)
            }
            ApiError::Unauthorized(msg) => {
                (StatusCode::UNAUTHORIZED, HttpError::new(msg), Finish::Plain)
            }
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, HttpError::new(msg), Finish::Plain),
            ApiError::BadRequest(msg) => {
                (StatusCode::BAD_REQUEST, HttpError::new(msg), Finish::Plain)
            }
            ApiError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                HttpError::new(msg),
                Finish::Plain,
            ),
            ApiError::HttpStatus(code, msg) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                HttpError::new(msg),
                Finish::Plain,
            ),
            ApiError::AtStatement {
                error,
                statement,
                position,
            } => {
                let (status, body, finish) = error.into_parts();
                (status, body.at_statement(statement, position), finish)
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body, finish) = self.into_parts();
        let resp = (status, axum::Json(body)).into_response();
        match finish {
            Finish::Plain => resp,
            Finish::RetryAfter(secs) => with_retry_after(resp, secs),
            Finish::Overloaded => overloaded(resp),
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(error: ApiError) -> (StatusCode, serde_json::Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read the error body");
        let body = serde_json::from_slice(&bytes).expect("a JSON error body");
        (status, body)
    }

    fn internal() -> crate::Error {
        crate::Error::Internal { detail: "x".into() }
    }

    #[tokio::test]
    async fn an_error_of_one_statement_answers_no_statement_keys() {
        let error = RequestError::in_statement(internal(), 0, 1);
        let (_, body) = body_of(ApiError::from_request(error, "SELECT 1")).await;
        assert!(body.get("statement").is_none(), "{body}");
        assert!(body.get("position").is_none(), "{body}");
    }

    #[tokio::test]
    async fn an_error_of_the_second_statement_names_it_and_keeps_its_status() {
        let sql = "SELECT 1; SELECT 2";
        let (bare_status, bare) = body_of(ApiError::from(internal())).await;
        let error = RequestError::in_statement(internal(), 1, 2);
        let (status, body) = body_of(ApiError::from_request(error, sql)).await;
        assert_eq!(status, bare_status);
        assert_eq!(body["error"], bare["error"], "{body}");
        assert_eq!(body["statement"], 2, "{body}");
        assert_eq!(body["position"], 11, "{body}");
    }
}
