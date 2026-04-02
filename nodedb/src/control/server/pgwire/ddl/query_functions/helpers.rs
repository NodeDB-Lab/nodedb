//! Shared helpers for DDL-routed query functions.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use super::super::super::types::{sqlstate_error, text_field};

pub fn extract_function_args<'a>(sql: &'a str, func_name: &str) -> PgWireResult<Vec<&'a str>> {
    let upper = sql.to_uppercase();
    let pos = upper
        .find(func_name)
        .ok_or_else(|| sqlstate_error("42601", &format!("missing {func_name}")))?;
    let after = &sql[pos + func_name.len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", &format!("{func_name} requires (...) arguments")))?;
    let paren_end = after
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
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

pub fn parse_timestamp_secs(s: &str) -> PgWireResult<u64> {
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    if let Some(dt) = nodedb_types::NdbDateTime::parse(s) {
        return Ok((dt.micros / 1_000_000) as u64);
    }
    Err(sqlstate_error(
        "22007",
        &format!("cannot parse '{s}' as timestamp"),
    ))
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

pub fn return_single_value(value: &str) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&value.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}
