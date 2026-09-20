// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for the atomic `Transfer` op (fungible balance
//! move), shared by the autocommit handler (`transfer.rs`) and the
//! in-transaction staging handler (`stage_kv_transfer.rs`) so a staged
//! value and its COMMIT-time durable replay are always computed by the
//! exact same code — mirrors the `engine_atomic_compute` / `stage_kv_atomic`
//! split for `Incr`/`Cas`/etc.

use std::collections::HashMap;

use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_to_row, row_to_kv_body};
use nodedb_types::Value;

/// Failure modes of [`compute_transfer`], translated to `ErrorCode` at each
/// call site (the live handler and the staging handler render slightly
/// different `ErrorCode` variants around the same detail message).
#[derive(Debug)]
pub(in crate::data::executor) enum TransferError {
    TypeMismatch(String),
    InsufficientBalance { have: f64, need: f64 },
}

/// The two updated document bodies and post-transfer balances for a
/// `Transfer` op, computed from BASE ∪ OVERLAY current values.
#[derive(Debug)]
pub(in crate::data::executor) struct TransferComputation {
    pub new_source: Vec<u8>,
    pub new_dest: Vec<u8>,
    pub source_balance_after: f64,
    pub dest_balance_after: f64,
}

/// Compute the read-validate-write outcome of an atomic fungible transfer.
///
/// `dest_bytes` is `None` when the destination key does not exist under
/// BASE ∪ OVERLAY -- a fresh document is created holding just `field`. A
/// destination that exists but lacks `field` starts from 0. Either side
/// holding a bare value (the single-`value` SQL form, RESP `SET`) or a
/// non-numeric `field` is a type mismatch, never silently treated as 0.
pub(in crate::data::executor) fn compute_transfer(
    source_bytes: &[u8],
    dest_bytes: Option<&[u8]>,
    field: &str,
    amount: f64,
) -> Result<TransferComputation, TransferError> {
    let mut source = map_row(source_bytes, "source")?;
    let source_balance = numeric_field(&source, field)?.ok_or_else(|| {
        TransferError::TypeMismatch(format!("field '{field}' is not numeric or missing"))
    })?;

    if source_balance < amount {
        return Err(TransferError::InsufficientBalance {
            have: source_balance,
            need: amount,
        });
    }

    let mut dest = match dest_bytes.filter(|b| !b.is_empty()) {
        None => HashMap::with_capacity(1),
        Some(bytes) => map_row(bytes, "destination")?,
    };
    let dest_balance = numeric_field(&dest, field)?.unwrap_or(0.0);

    let source_balance_after = source_balance - amount;
    let dest_balance_after = dest_balance + amount;
    source.insert(field.to_string(), numeric_value(source_balance_after));
    dest.insert(field.to_string(), numeric_value(dest_balance_after));

    Ok(TransferComputation {
        new_source: encode_map(source, "source")?,
        new_dest: encode_map(dest, "destination")?,
        source_balance_after,
        dest_balance_after,
    })
}

/// Decode a KV body as the typed-column map a transfer operates on.
fn map_row(bytes: &[u8], side: &str) -> Result<HashMap<String, Value>, TransferError> {
    let (row, shape) = kv_body_to_row(bytes)
        .map_err(|e| TransferError::TypeMismatch(format!("{side} body: {e}")))?;
    if shape == KvBodyShape::Raw {
        return Err(TransferError::TypeMismatch(format!(
            "{side} holds a bare value, not a hash"
        )));
    }
    match row {
        Value::Object(map) => Ok(map),
        other => Err(TransferError::TypeMismatch(format!(
            "{side} body is {}, not an object",
            other.type_name()
        ))),
    }
}

/// `field` as f64: `Ok(None)` when absent, a type mismatch when present but
/// not numeric.
fn numeric_field(map: &HashMap<String, Value>, field: &str) -> Result<Option<f64>, TransferError> {
    match map.get(field) {
        None => Ok(None),
        Some(Value::Float(f)) => Ok(Some(*f)),
        Some(Value::Integer(i)) => Ok(Some(*i as f64)),
        Some(other) => Err(TransferError::TypeMismatch(format!(
            "field '{field}' is {}, not numeric",
            other.type_name()
        ))),
    }
}

/// A whole-number balance stays an integer on disk; anything else is a float.
fn numeric_value(v: f64) -> Value {
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        Value::Integer(v as i64)
    } else {
        Value::Float(v)
    }
}

fn encode_map(map: HashMap<String, Value>, side: &str) -> Result<Vec<u8>, TransferError> {
    row_to_kv_body(&Value::Object(map), KvBodyShape::Map)
        .map_err(|e| TransferError::TypeMismatch(format!("serialize {side}: {e}")))
}

/// Extract a numeric field from a MessagePack-encoded KV value.
#[cfg(test)]
pub(in crate::data::executor) fn extract_numeric_field(value: &[u8], field: &str) -> Option<f64> {
    let (row, _) = kv_body_to_row(value).ok()?;
    match row.get(field)? {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(field: &str, value: f64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ field: value })).unwrap()
    }

    #[test]
    fn transfer_moves_balance_between_existing_docs() {
        let source = doc("balance", 100.0);
        let dest = doc("balance", 10.0);
        let result = compute_transfer(&source, Some(&dest), "balance", 30.0).unwrap();
        assert_eq!(result.source_balance_after, 70.0);
        assert_eq!(result.dest_balance_after, 40.0);
    }

    #[test]
    fn transfer_creates_dest_when_absent() {
        let source = doc("balance", 100.0);
        let result = compute_transfer(&source, None, "balance", 30.0).unwrap();
        assert_eq!(result.dest_balance_after, 30.0);
        assert_eq!(
            extract_numeric_field(&result.new_dest, "balance"),
            Some(30.0)
        );
    }

    #[test]
    fn transfer_rejects_insufficient_balance() {
        let source = doc("balance", 10.0);
        let err = compute_transfer(&source, None, "balance", 30.0);
        assert!(matches!(
            err,
            Err(TransferError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn transfer_rejects_a_bare_value_destination() {
        // A raw body (the single-`value` form, RESP `SET`) is not a hash:
        // never treated as a zero balance and re-encoded as a map.
        let source = doc("balance", 100.0);
        let err = compute_transfer(&source, Some(b"5"), "balance", 30.0);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_a_bare_value_source() {
        let err = compute_transfer(b"100", None, "balance", 30.0);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_non_numeric_destination_field() {
        let source = doc("balance", 100.0);
        let dest = nodedb_types::json_to_msgpack(&serde_json::json!({"balance": "abc"})).unwrap();
        let err = compute_transfer(&source, Some(&dest), "balance", 30.0);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_non_numeric_field() {
        let source = nodedb_types::json_to_msgpack(&serde_json::json!({"balance": "abc"})).unwrap();
        let err = compute_transfer(&source, None, "balance", 30.0);
        assert!(matches!(err, Err(TransferError::TypeMismatch(_))));
    }
}
