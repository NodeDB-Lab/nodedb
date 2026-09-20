// SPDX-License-Identifier: BUSL-1.1

//! Conversion utilities for CRDT value types.

use crate::engine::document::crdt_store::loro_value_to_json;

/// The stored body of a CRDT row whose Loro map holds `row`: the row's JSON
/// encoded as MessagePack, before the put path canonicalizes it. The live
/// materialization (`encode_crdt_row`) and the transaction staging path
/// (`stage_crdt`) both build a row body here, so a staged row and its COMMIT
/// replay agree byte for byte. `None` when the row cannot encode.
pub(in crate::data::executor) fn crdt_row_body(row: &loro::LoroValue) -> Option<Vec<u8>> {
    let json = loro_value_to_json(row);
    nodedb_types::json_to_msgpack(&json).ok()
}

/// Convert a `serde_json::Value` to a `loro::LoroValue`.
pub(in crate::data::executor) fn json_to_loro_value(val: &serde_json::Value) -> loro::LoroValue {
    match val {
        serde_json::Value::Null => loro::LoroValue::Null,
        serde_json::Value::Bool(b) => loro::LoroValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                loro::LoroValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                loro::LoroValue::Double(f)
            } else {
                loro::LoroValue::Null
            }
        }
        serde_json::Value::String(s) => loro::LoroValue::String(s.clone().into()),
        serde_json::Value::Array(arr) => {
            let items: Vec<loro::LoroValue> = arr.iter().map(json_to_loro_value).collect();
            loro::LoroValue::List(items.into())
        }
        serde_json::Value::Object(map) => {
            let entries: std::collections::HashMap<String, loro::LoroValue> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_loro_value(v)))
                .collect();
            loro::LoroValue::Map(entries.into())
        }
    }
}
