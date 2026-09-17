// SPDX-License-Identifier: BUSL-1.1

//! KV point-get / batch-get response shaping: inject the primary key(s)
//! into the stored value(s) before the protocol layer turns them into
//! SQL rows.

use std::collections::HashMap;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::decode_payload_value;
use nodedb_physical::physical_plan::KvOp;
use nodedb_query::msgpack_scan;
use nodedb_types::Value;

/// When `plan` is a KV point-get or batch-get, turn the engine's stored
/// bytes into row-shaped msgpack.
///
/// The single-key rule is `msgpack_scan::kv_row_msgpack` — the same function
/// the Data Plane's scan and `RETURNING` paths call, deliberately not a
/// second copy here. Two copies of it existed once and diverged, and every
/// scan path served `value: 118` for a stored `"v1"` until they were merged.
///
/// `KvOp::BatchGet` gets its own arm: `execute_kv_batch_get` (Data Plane)
/// emits a bare msgpack array of per-key results (base64 `value` string,
/// or `null` for a missing key) positionally parallel to the plan's
/// `keys` list. That array of scalars has no `key` attached, so the
/// generic row-flattener (`push_flat_rows`) would silently drop every
/// scalar element (its catch-all only forwards objects/arrays). Zip the
/// results with `keys` here and wrap each pair into the same `{key,
/// value}` row shape the single-key `Get` arm above produces, with a
/// missing key represented as `value: null` (matching how
/// `execute_kv_batch_get` already encodes a miss).
///
/// For every other plan, return the payload unchanged.
pub fn apply_kv_wrap(plan: &PhysicalPlan, payload: &[u8]) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    match plan {
        PhysicalPlan::Kv(KvOp::Get { key, .. }) => {
            msgpack_scan::kv_row_msgpack(&String::from_utf8_lossy(key), payload)
        }
        PhysicalPlan::Kv(KvOp::BatchGet { keys, .. }) => wrap_batch_get(keys, payload),
        _ => payload.to_vec(),
    }
}

/// Zip `KvOp::BatchGet`'s `keys` with the Data Plane's positional
/// `[value_or_null, ...]` array and wrap each pair into a `{key, value}`
/// row, msgpack-encoded so the rest of the shaping pipeline
/// (`decode_payload_value` -> `push_flat_rows`) treats it exactly like
/// any other row-array payload.
///
/// Falls back to the raw payload (rather than panicking) if the Data
/// Plane payload is not the expected JSON/msgpack array — a malformed
/// upstream payload degrades to an empty-looking shape instead of taking
/// down the connection.
fn wrap_batch_get(keys: &[Vec<u8>], payload: &[u8]) -> Vec<u8> {
    let Ok(Value::Array(values)) = decode_payload_value(payload) else {
        return payload.to_vec();
    };

    let rows: Vec<Value> = keys
        .iter()
        .zip(values)
        .map(|(key, value)| {
            let mut row = HashMap::with_capacity(2);
            row.insert(
                "key".to_string(),
                Value::String(String::from_utf8_lossy(key).into_owned()),
            );
            row.insert("value".to_string(), value);
            Value::Object(row)
        })
        .collect();

    nodedb_types::value_to_msgpack(&Value::Array(rows)).unwrap_or_else(|_| payload.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each positional result is paired with its key into a `{key, value}`
    /// row; a missing key stays `null` and the value keeps its type.
    #[test]
    fn wrap_batch_get_pairs_keys_with_positional_values() {
        let keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        let payload = nodedb_types::value_to_msgpack(&Value::Array(vec![
            Value::String("dmFs".into()),
            Value::Null,
        ]))
        .expect("encode");

        let wrapped = wrap_batch_get(&keys, &payload);
        let Value::Array(rows) = decode_payload_value(&wrapped).expect("decode") else {
            panic!("wrapped payload must be an array");
        };
        let Value::Object(first) = &rows[0] else {
            panic!("row must be an object");
        };
        assert_eq!(first["key"], Value::String("k1".into()));
        assert_eq!(first["value"], Value::String("dmFs".into()));
        let Value::Object(second) = &rows[1] else {
            panic!("row must be an object");
        };
        assert_eq!(second["key"], Value::String("k2".into()));
        assert_eq!(second["value"], Value::Null);
    }

    /// A payload that is not an array passes through unchanged.
    #[test]
    fn wrap_batch_get_passes_a_non_array_payload_through() {
        let payload = nodedb_types::value_to_msgpack(&Value::Integer(1)).expect("encode");
        assert_eq!(wrap_batch_get(&[b"k".to_vec()], &payload), payload);
    }
}
