// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral query-function handlers.
//!
//! Fallible helpers yield a protocol-neutral [`DdlError`] (SQLSTATE +
//! message), and the single-column `result` output builds a
//! [`DdlResult::Rows`] directly.

use nodedb_types::Value;
use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::project::push_flat_rows;
use crate::control::server::response_shape::types::{ShapedRow, ShapedRows};
use crate::data::executor::response_codec::{RowContext, decode_row_payload_value};

use super::super::super::result::{DdlError, DdlResult};

/// Construct a protocol-neutral DDL error (SQLSTATE + message).
pub fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub fn parse_timestamp_secs(s: &str) -> Result<u64, DdlError> {
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    if let Some(dt) = nodedb_types::NdbDateTime::parse(s) {
        return Ok((dt.micros / 1_000_000) as u64);
    }
    Err(err("22007", &format!("cannot parse '{s}' as timestamp")))
}

/// Read a typed cell as a Decimal: a Decimal exactly, an integer, a float, or
/// text that parses. Returns `None` for any other value.
pub fn value_to_decimal(v: &Value) -> Option<rust_decimal::Decimal> {
    match v {
        Value::Decimal(d) => Some(*d),
        Value::Integer(i) => Some(rust_decimal::Decimal::from(*i)),
        Value::Float(f) => rust_decimal::Decimal::try_from(*f).ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// The text a typed cell matches as: the text a plain row stores it as.
///
/// A string, UUID or ULID is itself, a Decimal or duration its text, and an
/// instant its ISO 8601 text. Any other value has no text form.
pub fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => Some(s.clone()),
        Value::Decimal(d) => Some(d.to_string()),
        Value::Duration(d) => Some(d.to_string()),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => Some(dt.to_string()),
        _ => None,
    }
}

/// Build the single-row `result` output carrying `value`.
///
/// The output has one text column named `result` with one row holding
/// `value`.
pub fn single_result(value: &str) -> Vec<DdlResult> {
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(value.to_string()));
    vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        vec![row],
    ))]
}

/// Decode a `DocumentOp::Scan` payload into typed rows, with each row's
/// `{id, data}` scan envelope unwrapped so callers read stored fields
/// directly. Plain and native rows decode alike, and each field keeps its
/// type.
///
/// Reuses `response_shape::project::push_flat_rows` — the same unwrap the
/// pgwire/HTTP row shaper applies — so there is exactly one definition of
/// "unwrap a scan envelope" in the tree.
///
/// The callers read a row's identity under `id`, so a row that lacks `id`
/// gains it here. These rows feed the query functions only and never reach
/// a client as a result set. A payload that does not decode is an error.
pub fn scan_rows(payload: &[u8]) -> Result<Vec<ShapedRow>, DdlError> {
    let decoded = decode_row_payload_value(payload, RowContext::ScanEnvelope)
        .map_err(|e| DdlError::from_error_in_context("scan response does not decode", &e))?;
    let mut rows = Vec::new();
    push_flat_rows(
        decoded,
        RowContext::ScanEnvelope,
        nodedb_types::DEFAULT_IDENTITY_COLUMN,
        &mut rows,
    )
    .map_err(|e| DdlError::from_error(&e))?;
    Ok(rows)
}

/// Build a zero-row `result` output.
///
/// The output has one text column named `result` and no rows.
pub fn empty_result() -> Vec<DdlResult> {
    vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        Vec::new(),
    ))]
}
