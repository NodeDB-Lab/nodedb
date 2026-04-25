//! Convert `Value` to SQL literal string for substitution into SQL text.

use super::core::Value;

impl Value {
    /// Convert to a SQL literal string for substitution into SQL text.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                format!("'\\x{hex}'")
            }
            Value::Array(arr) | Value::Set(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_sql_literal()).collect();
                format!("ARRAY[{}]", elements.join(", "))
            }
            Value::Object(map) => {
                let json_str = serde_json::to_string(&serde_json::Value::Object(
                    map.iter()
                        .map(|(k, v)| (k.clone(), value_to_json(v)))
                        .collect(),
                ))
                .unwrap_or_default();
                format!("'{}'", json_str.replace('\'', "''"))
            }
            Value::DateTime(dt) => format!("'{dt}'"),
            Value::Duration(d) => format!("'{d}'"),
            Value::Decimal(d) => d.to_string(),
            Value::Geometry(g) => format!("'{}'", serde_json::to_string(g).unwrap_or_default()),
            Value::Range { .. } | Value::Record { .. } => "NULL".into(),
            Value::NdArrayCell(cell) => {
                let coords: Vec<String> = cell.coords.iter().map(|v| v.to_sql_literal()).collect();
                let attrs: Vec<String> = cell.attrs.iter().map(|v| v.to_sql_literal()).collect();
                format!(
                    "ARRAY_CELL(coords=[{}], attrs=[{}])",
                    coords.join(", "),
                    attrs.join(", ")
                )
            }
        }
    }
}

/// Convert nodedb_types::Value back to serde_json::Value (for object serialization).
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Array(arr) | Value::Set(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        other => serde_json::Value::String(other.to_sql_literal()),
    }
}
