//! Conversions between `Value` and `serde_json::Value`.

use super::core::Value;

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Integer(i) => serde_json::json!(i),
            Value::Float(f) => serde_json::json!(f),
            Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                serde_json::Value::String(s)
            }
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                serde_json::Value::String(hex)
            }
            Value::Array(arr) | Value::Set(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Duration(d) => serde_json::Value::String(d.to_string()),
            Value::Decimal(d) => serde_json::Value::String(d.to_string()),
            Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
            Value::Range { .. } | Value::Record { .. } => serde_json::Value::Null,
            Value::NdArrayCell(cell) => serde_json::json!({
                "coords": cell.coords.into_iter().map(serde_json::Value::from).collect::<Vec<_>>(),
                "attrs": cell.attrs.into_iter().map(serde_json::Value::from).collect::<Vec<_>>(),
            }),
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else if let Some(u) = n.as_u64() {
                    Value::Integer(u as i64)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(map) => {
                Value::Object(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}
