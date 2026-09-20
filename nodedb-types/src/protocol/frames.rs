// SPDX-License-Identifier: Apache-2.0

//! Native protocol request and response frame types.

use serde::{Deserialize, Serialize};

use crate::value::Value;

use super::auth::AuthResponse;
use super::opcodes::ResponseStatus;
use super::request_fields::RequestFields;

// ─── Request Frame ──────────────────────────────────────────────────

/// A request sent from client to server over the native protocol.
///
/// Serialized as MessagePack. The `op` field selects the handler,
/// `seq` correlates request to response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeRequest {
    /// Operation code.
    pub op: super::opcodes::OpCode,
    /// Client-assigned sequence number for request/response correlation.
    pub seq: u64,
    /// Operation-specific fields (flattened into the same map).
    #[serde(flatten)]
    pub fields: RequestFields,
}

impl zerompk::ToMessagePack for NativeRequest {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        self.op.write(writer)?;
        writer.write_u64(self.seq)?;
        self.fields.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for NativeRequest {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let op = super::opcodes::OpCode::read(reader)?;
        let seq = reader.read_u64()?;
        let fields = RequestFields::read(reader)?;
        Ok(Self { op, seq, fields })
    }
}

// ─── Response Frame ─────────────────────────────────────────────────

/// A response sent from server to client over the native protocol.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct NativeResponse {
    /// Echoed from the request for correlation.
    pub seq: u64,
    /// Execution outcome.
    pub status: ResponseStatus,
    /// Column names (for query results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Row data (for query results). Each row is a Vec of Values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
    /// Number of rows affected (for writes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    /// The statement's command verb (`INSERT`, `UPDATE`, `DELETE`, `UPSERT`,
    /// `MERGE`, `TRUNCATE`, ...). `None` when the statement produced no DML
    /// outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub command: Option<String>,
    /// WAL LSN watermark at time of computation.
    pub watermark_lsn: u64,
    /// Error details (if status == Error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorPayload>,
    /// Auth response (if op == Auth and status == Ok).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthResponse>,
    /// Advisory warnings (e.g. password expiry grace period, must_change_password).
    /// Empty in the common case; `#[serde(default)]` and `#[msgpack(default)]`
    /// keep this additive and backward-compatible with older clients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub warnings: Vec<String>,
}

/// Error details in a response.
///
/// Encoded as a MessagePack map rather than an array so that fields can be
/// added without invalidating the frames older peers already understand —
/// the same reason [`NativeResponse`] is a map. An array representation has
/// no field names, so a decoder cannot tell a missing optional field from a
/// truncated frame and every addition is a breaking change.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ErrorPayload {
    /// SQLSTATE-style error code (e.g., "42P01" for undefined table).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Stable numeric NodeDB error code (`nodedb_types::ErrorCode`'s inner
    /// value, e.g. 1000 for a constraint violation).
    ///
    /// SQLSTATE is many-to-one — a unique violation and a duplicate
    /// idempotency key both render as `23505`, and everything unclassified
    /// renders as `XX000` — so it cannot carry the classification a client
    /// needs to reconstruct a typed error. This field is the authoritative
    /// one for that. `0` means the sending server predates this field, and a
    /// client must fall back to a generic error rather than guess.
    /// `#[serde(default)]` + `#[msgpack(default)]` keep it additive and
    /// backward-compatible with peers that never send it.
    #[serde(default, skip_serializing_if = "is_zero")]
    #[msgpack(default)]
    pub ndb_code: u16,
}

/// `skip_serializing_if` predicate for [`ErrorPayload::ndb_code`]: zero is the
/// "not carried" sentinel, so it is omitted rather than sent as a real code.
fn is_zero(code: &u16) -> bool {
    *code == 0
}

impl NativeResponse {
    /// Create a successful response with no data.
    pub fn ok(seq: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            command: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create a successful response from a `QueryResult`.
    pub fn from_query_result(seq: u64, qr: crate::result::QueryResult, lsn: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(qr.columns),
            rows: Some(qr.rows),
            rows_affected: Some(qr.rows_affected),
            command: qr.command,
            watermark_lsn: lsn,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an error response that carries only a SQLSTATE.
    ///
    /// Use [`Self::error_with_code`] wherever the numeric NodeDB code is
    /// known: a frame built here reaches the client with `ndb_code == 0` and
    /// collapses to a generic internal error on the far side.
    pub fn error(seq: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::error_with_code(seq, code, message, 0)
    }

    /// Create an error response carrying both the SQLSTATE and the stable
    /// numeric NodeDB code, so the client can rebuild the typed error rather
    /// than inferring one from a many-to-one SQLSTATE.
    pub fn error_with_code(
        seq: u64,
        code: impl Into<String>,
        message: impl Into<String>,
        ndb_code: u16,
    ) -> Self {
        Self {
            seq,
            status: ResponseStatus::Error,
            columns: None,
            rows: None,
            rows_affected: None,
            command: None,
            watermark_lsn: 0,
            error: Some(ErrorPayload {
                code: code.into(),
                message: message.into(),
                ndb_code,
            }),
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an auth success response.
    pub fn auth_ok(seq: u64, username: String, tenant_id: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            command: None,
            watermark_lsn: 0,
            error: None,
            auth: Some(AuthResponse {
                username,
                tenant_id,
            }),
            warnings: Vec::new(),
        }
    }

    /// Create a response with a single "status" column and one row.
    pub fn status_row(seq: u64, message: impl Into<String>) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(vec!["status".into()]),
            rows: Some(vec![vec![Value::String(message.into())]]),
            rows_affected: Some(1),
            command: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::opcodes::OpCode;
    use super::super::text_fields::TextFields;
    use super::*;

    #[test]
    fn native_response_ok() {
        let r = NativeResponse::ok(42);
        assert_eq!(r.seq, 42);
        assert_eq!(r.status, ResponseStatus::Ok);
        assert!(r.error.is_none());
    }

    #[test]
    fn native_response_error() {
        let r = NativeResponse::error(1, "42P01", "collection not found");
        assert_eq!(r.status, ResponseStatus::Error);
        let e = r.error.unwrap();
        assert_eq!(e.code, "42P01");
        assert_eq!(e.message, "collection not found");
    }

    #[test]
    fn native_response_from_query_result() {
        let qr = crate::result::QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![
                Value::String("u1".into()),
                Value::String("Alice".into()),
            ]],
            rows_affected: 0,
            command: None,
        };
        let r = NativeResponse::from_query_result(5, qr, 100);
        assert_eq!(r.seq, 5);
        assert_eq!(r.watermark_lsn, 100);
        assert_eq!(r.columns.as_ref().unwrap().len(), 2);
        assert_eq!(r.rows.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn native_response_status_row() {
        let r = NativeResponse::status_row(3, "OK");
        assert_eq!(r.columns.as_ref().unwrap(), &["status"]);
        assert_eq!(r.rows.as_ref().unwrap()[0][0].as_str(), Some("OK"));
    }

    #[test]
    fn msgpack_roundtrip_request() {
        let req = NativeRequest {
            op: OpCode::Sql,
            seq: 1,
            fields: RequestFields::Text(TextFields {
                sql: Some("SELECT 1".into()),
                ..Default::default()
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: NativeRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.op, OpCode::Sql);
        assert_eq!(decoded.seq, 1);
    }

    #[test]
    fn msgpack_roundtrip_response() {
        let resp = NativeResponse::from_query_result(
            7,
            crate::result::QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![Value::Integer(42)]],
                rows_affected: 0,
                command: None,
            },
            99,
        );
        let bytes = zerompk::to_msgpack_vec(&resp).unwrap();
        let decoded: NativeResponse = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.seq, 7);
        assert_eq!(decoded.watermark_lsn, 99);
        assert_eq!(decoded.rows.unwrap()[0][0].as_i64(), Some(42));
    }

    #[test]
    fn error_payload_round_trips_the_numeric_code() {
        let frame = NativeResponse::error_with_code(7, "23505", "duplicate key", 1000);
        let bytes = zerompk::to_msgpack_vec(&frame).expect("encode");
        let decoded: NativeResponse = zerompk::from_msgpack(&bytes).expect("decode");
        let payload = decoded.error.expect("error payload survives the wire");
        assert_eq!(payload.code, "23505");
        assert_eq!(payload.ndb_code, 1000);
    }

    #[test]
    fn error_payload_without_numeric_code_still_decodes() {
        // Hand-rolled 2-key map: exactly what a peer built before the numeric
        // code was added sends. It must decode with `ndb_code == 0` rather
        // than fail, which is what makes the field additive.
        let mut legacy = vec![0x82u8];
        for (key, value) in [("code", "42P01"), ("message", "boom")] {
            legacy.push(0xA0 | key.len() as u8);
            legacy.extend_from_slice(key.as_bytes());
            legacy.push(0xA0 | value.len() as u8);
            legacy.extend_from_slice(value.as_bytes());
        }
        let decoded: ErrorPayload = zerompk::from_msgpack(&legacy).expect("legacy frame decodes");
        assert_eq!(decoded.code, "42P01");
        assert_eq!(decoded.message, "boom");
        assert_eq!(
            decoded.ndb_code, 0,
            "an absent numeric code must read as the 'not carried' sentinel"
        );
    }
}
