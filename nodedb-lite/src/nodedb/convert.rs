//! Value conversion helpers between `nodedb_types` and `loro` value types.

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::value::Value;

/// Convert `nodedb_types::Value` to `LoroValue`.
pub(crate) fn value_to_loro(v: &Value) -> LoroValue {
    match v {
        Value::Null => LoroValue::Null,
        Value::Bool(b) => LoroValue::Bool(*b),
        Value::Integer(i) => LoroValue::I64(*i),
        Value::Float(f) => LoroValue::Double(*f),
        Value::String(s) => LoroValue::String(s.clone().into()),
        Value::Bytes(b) => LoroValue::Binary(b.clone().into()),
        Value::Array(_) | Value::Object(_) => {
            // Serialize complex values as JSON string.
            let json = serde_json::to_string(v).unwrap_or_default();
            LoroValue::String(json.into())
        }
    }
}

/// Convert a `LoroValue` (row) into a `Document`.
pub(crate) fn loro_value_to_document(id: &str, value: &LoroValue) -> Document {
    let mut doc = Document::new(id);
    if let LoroValue::Map(map) = value {
        for (k, v) in map.iter() {
            doc.set(k.to_string(), loro_value_to_value(v));
        }
    }
    doc
}

/// Convert `LoroValue` to `nodedb_types::Value`.
pub(crate) fn loro_value_to_value(v: &LoroValue) -> Value {
    match v {
        LoroValue::Null => Value::Null,
        LoroValue::Bool(b) => Value::Bool(*b),
        LoroValue::I64(i) => Value::Integer(*i),
        LoroValue::Double(f) => Value::Float(*f),
        LoroValue::String(s) => Value::String(s.to_string()),
        LoroValue::Binary(b) => Value::Bytes(b.to_vec()),
        LoroValue::List(arr) => Value::Array(arr.iter().map(loro_value_to_value).collect()),
        LoroValue::Map(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.to_string(), loro_value_to_value(v)))
                .collect(),
        ),
        _ => Value::Null,
    }
}
