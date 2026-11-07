// SPDX-License-Identifier: BUSL-1.1

//! Typed request and response structs for the HTTP API.
//!
//! All structs derive `Serialize` + `Deserialize` with `snake_case` renaming.
//! `&'static str` fields that need serde are expressed as `String` or
//! `std::borrow::Cow<'static, str>` so callers can use both owned and
//! borrowed values without extra allocation in the common (literal) case.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

// ── Request types ───────────────────────────────────────────────────────────

/// `POST /v1/query` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpQueryRequest {
    pub sql: String,
    /// `strong` (the default), `eventual` or `bounded_staleness:<secs>`.
    #[serde(default)]
    pub read_consistency: Option<String>,
}

/// `POST /v1/query/stream` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpQueryStreamRequest {
    pub sql: String,
    /// `strong` (the default), `eventual` or `bounded_staleness:<secs>`.
    #[serde(default)]
    pub read_consistency: Option<String>,
}

/// `POST /v1/auth/exchange-key` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpExchangeKeyRequest {
    #[serde(default)]
    pub scopes: Vec<String>,
    pub rate_limit_qps: Option<u64>,
    pub rate_limit_burst: Option<u64>,
    pub expires_days: Option<u64>,
}

/// `POST /v1/collections/{name}/crdt/apply` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpCrdtApplyRequest {
    pub doc_id: String,
    /// Hex-encoded CRDT delta bytes.
    pub delta: String,
}

// ── Response types ──────────────────────────────────────────────────────────

/// `POST /v1/query` success response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpQueryResponse {
    pub status: Cow<'static, str>,
    /// Row payloads decoded to JSON. Intentionally `serde_json::Value` — row
    /// shapes are engine-specific and must not be re-encoded at this layer.
    pub rows: Vec<serde_json::Value>,
}

impl HttpQueryResponse {
    pub fn ok(rows: Vec<serde_json::Value>) -> Self {
        Self {
            status: Cow::Borrowed("ok"),
            rows,
        }
    }
}

/// `POST /v1/auth/exchange-key` success response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpExchangeKeyResponse {
    pub api_key: String,
    pub auth_user_id: String,
    pub expires_in: u64,
}

/// `POST /v1/auth/session` success response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpSessionResponse {
    pub session_id: String,
    pub expires_in: u64,
}

/// `POST /v1/collections/{name}/crdt/apply` success response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpCrdtApplyResponse {
    pub status: Cow<'static, str>,
    pub collection: String,
    pub doc_id: String,
}

impl HttpCrdtApplyResponse {
    pub fn ok(collection: String, doc_id: String) -> Self {
        Self {
            status: Cow::Borrowed("ok"),
            collection,
            doc_id,
        }
    }
}

/// Generic `{ "status": "ok" }` response (e.g. `DELETE /v1/auth/session`).
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpStatusOk {
    pub status: Cow<'static, str>,
}

impl HttpStatusOk {
    pub fn ok() -> Self {
        Self {
            status: Cow::Borrowed("ok"),
        }
    }
}

/// Generic error response — produced by `ApiError::into_response`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpError {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// The typed error that caused this one, as its message and code. Absent
    /// when there is none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<HttpError>>,
}

impl HttpError {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            cause: None,
        }
    }

    pub fn with_code(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: Some(code.into()),
            cause: None,
        }
    }

    /// Carry `cause` as this error's cause, with its own message and code.
    pub fn caused_by(mut self, cause: &nodedb_types::NodeDbError) -> Self {
        self.cause = Some(Box::new(Self::with_code(
            cause.message(),
            cause.code().to_string(),
        )));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_query_request_deserializes() {
        let json = r#"{"sql":"SELECT 1"}"#;
        let req: HttpQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.sql, "SELECT 1");
    }

    #[test]
    fn http_query_response_shape() {
        let resp = HttpQueryResponse::ok(vec![serde_json::json!({"id": 1})]);
        let json = serde_json::to_string(&resp).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["status"], "ok");
        assert!(v["rows"].is_array());
        assert_eq!(v["rows"][0]["id"], 1);
    }

    #[test]
    fn http_error_shape_no_code() {
        let e = HttpError::new("something went wrong");
        let json = serde_json::to_string(&e).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["error"], "something went wrong");
        // `code` must be absent when None (skip_serializing_if).
        assert!(v.get("code").is_none());
    }

    #[test]
    fn http_error_shape_with_code() {
        let e = HttpError::with_code("not found", "COLLECTION_NOT_FOUND");
        let json = serde_json::to_string(&e).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["error"], "not found");
        assert_eq!(v["code"], "COLLECTION_NOT_FOUND");
    }

    #[test]
    fn http_exchange_key_request_defaults() {
        // All optional fields absent.
        let json = r#"{"scopes":[]}"#;
        let req: HttpExchangeKeyRequest = serde_json::from_str(json).unwrap();
        assert!(req.scopes.is_empty());
        assert!(req.rate_limit_qps.is_none());
        assert!(req.expires_days.is_none());
    }

    #[test]
    fn http_exchange_key_response_shape() {
        let resp = HttpExchangeKeyResponse {
            api_key: "nda_abc".into(),
            auth_user_id: "u1".into(),
            expires_in: 2_592_000,
        };
        let v: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["api_key"], "nda_abc");
        assert_eq!(v["auth_user_id"], "u1");
        assert_eq!(v["expires_in"], 2_592_000u64);
    }

    #[test]
    fn http_session_response_shape() {
        let resp = HttpSessionResponse {
            session_id: "nds_xyz".into(),
            expires_in: 3600,
        };
        let v: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["session_id"], "nds_xyz");
        assert_eq!(v["expires_in"], 3600u64);
    }

    #[test]
    fn http_crdt_apply_request_deserializes() {
        let json = r#"{"doc_id":"d1","delta":"deadbeef"}"#;
        let req: HttpCrdtApplyRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.doc_id, "d1");
        assert_eq!(req.delta, "deadbeef");
    }

    #[test]
    fn http_crdt_apply_response_shape() {
        let resp = HttpCrdtApplyResponse::ok("coll".into(), "doc1".into());
        let v: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["collection"], "coll");
        assert_eq!(v["doc_id"], "doc1");
    }

    #[test]
    fn http_status_ok_shape() {
        let resp = HttpStatusOk::ok();
        let v: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["status"], "ok");
    }
}
