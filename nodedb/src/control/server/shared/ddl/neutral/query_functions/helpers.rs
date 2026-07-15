// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral query-function handlers.
//!
//! Ported verbatim from the pgwire `ddl::query_functions::helpers` module; the
//! only change is that fallible helpers now yield a protocol-neutral
//! [`DdlError`] (SQLSTATE + message) instead of a pgwire `PgWireError`, and the
//! single-column `result` output builds a [`DdlResult::Rows`] directly instead
//! of a pgwire `QueryResponse`. SQLSTATE codes and messages are unchanged.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::types::ShapedRows;

use super::super::super::result::{DdlError, DdlResult};

/// Construct a protocol-neutral DDL error (SQLSTATE + message).
pub fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.to_string(),
    }
}

pub fn extract_function_args<'a>(sql: &'a str, func_name: &str) -> Result<Vec<&'a str>, DdlError> {
    let pos = find_ascii_case_insensitive(sql, func_name)
        .ok_or_else(|| err("42601", &format!("missing {func_name}")))?;
    let after = &sql[pos + func_name.len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| err("42601", &format!("{func_name} requires (...) arguments")))?;
    let paren_end = after
        .rfind(')')
        .ok_or_else(|| err("42601", "missing closing ')'"))?;
    let inner = &after[paren_start + 1..paren_end];
    Ok(inner.split(',').collect())
}

pub fn clean_arg(s: &str) -> String {
    s.trim()
        .trim_matches('\'')
        .trim_matches('"')
        .trim()
        .to_string()
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

/// Convert a JSON value to Decimal. Returns `None` for non-numeric values.
pub fn json_to_decimal(v: &serde_json::Value) -> Option<rust_decimal::Decimal> {
    if let Some(i) = v.as_i64() {
        Some(rust_decimal::Decimal::from(i))
    } else if let Some(f) = v.as_f64() {
        rust_decimal::Decimal::try_from(f).ok()
    } else if let Some(s) = v.as_str() {
        s.parse().ok()
    } else {
        None
    }
}

/// Build the single-row `result` output carrying `value`.
///
/// Mirrors the pgwire `return_single_value` helper: one text column named
/// `result` with one row holding `value`.
pub fn single_result(value: &str) -> Vec<DdlResult> {
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(value.to_string()));
    vec![DdlResult::Rows(ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![row],
        notice: None,
    })]
}

/// Build a zero-row `result` output.
///
/// Mirrors the pgwire empty-`QueryResponse` case (one text column named
/// `result`, no rows).
pub fn empty_result() -> Vec<DdlResult> {
    vec![DdlResult::Rows(ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: Vec::new(),
        notice: None,
    })]
}
