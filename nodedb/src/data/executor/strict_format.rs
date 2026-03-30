//! Strict document encoding/decoding: JSON ↔ Binary Tuple.
//!
//! Used by the point handlers when a collection's storage mode is `Strict`.
//! Converts between the JSON representation used by the SQL planner and the
//! compact Binary Tuple format stored in redb.

use nodedb_types::columnar::{ColumnType, StrictSchema};
use nodedb_types::value::Value;

/// Encode a JSON document as a Binary Tuple according to the given schema.
///
/// The JSON bytes are deserialized, each field is matched to a schema column
/// by name, and the values are converted to the column's declared type.
/// Missing nullable columns become NULL; missing non-nullable columns cause
/// an error.
pub(super) fn json_to_binary_tuple(
    json_bytes: &[u8],
    schema: &StrictSchema,
) -> Result<Vec<u8>, String> {
    let doc: serde_json::Value =
        serde_json::from_slice(json_bytes).map_err(|e| format!("invalid JSON: {e}"))?;

    let obj = doc
        .as_object()
        .ok_or_else(|| "strict INSERT value must be a JSON object".to_string())?;

    let encoder = nodedb_strict::TupleEncoder::new(schema);
    let mut values = Vec::with_capacity(schema.columns.len());

    for col in &schema.columns {
        let json_val = obj.get(&col.name);
        let value = match json_val {
            None | Some(serde_json::Value::Null) => {
                if !col.nullable {
                    return Err(format!(
                        "column '{}' is NOT NULL but no value provided",
                        col.name
                    ));
                }
                Value::Null
            }
            Some(v) => json_to_typed_value(v, &col.column_type, &col.name)?,
        };
        values.push(value);
    }

    encoder
        .encode(&values)
        .map_err(|e| format!("Binary Tuple encode: {e}"))
}

/// Decode a Binary Tuple to a JSON object using the schema.
pub(super) fn binary_tuple_to_json(
    tuple_bytes: &[u8],
    schema: &StrictSchema,
) -> Option<serde_json::Value> {
    let decoder = nodedb_strict::TupleDecoder::new(schema);
    let values = decoder.extract_all(tuple_bytes).ok()?;

    let mut obj = serde_json::Map::with_capacity(schema.columns.len());
    for (i, col) in schema.columns.iter().enumerate() {
        let json_val = typed_value_to_json(&values[i]);
        obj.insert(col.name.clone(), json_val);
    }
    Some(serde_json::Value::Object(obj))
}

/// Convert a JSON value to a typed `Value` according to the column type.
fn json_to_typed_value(
    val: &serde_json::Value,
    col_type: &ColumnType,
    col_name: &str,
) -> Result<Value, String> {
    match col_type {
        ColumnType::Bool => match val {
            serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
            serde_json::Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(format!("column '{col_name}': cannot parse '{s}' as BOOL")),
            },
            _ => Err(format!("column '{col_name}': expected BOOL, got {val}")),
        },
        ColumnType::Int64 => match val {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Integer(f as i64))
                } else {
                    Err(format!("column '{col_name}': number out of range"))
                }
            }
            serde_json::Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as INT")),
            _ => Err(format!("column '{col_name}': expected INT, got {val}")),
        },
        ColumnType::Float64 => match val {
            serde_json::Value::Number(n) => Ok(Value::Float(n.as_f64().unwrap_or(0.0))),
            serde_json::Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as FLOAT")),
            _ => Err(format!("column '{col_name}': expected FLOAT, got {val}")),
        },
        ColumnType::String | ColumnType::Uuid => match val {
            serde_json::Value::String(s) => Ok(Value::String(s.clone())),
            other => Ok(Value::String(other.to_string())),
        },
        ColumnType::Bytes => match val {
            serde_json::Value::String(s) => {
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec());
                Ok(Value::Bytes(bytes))
            }
            _ => Err(format!(
                "column '{col_name}': expected BYTES (base64 string), got {val}"
            )),
        },
        ColumnType::Timestamp => match val {
            serde_json::Value::Number(n) => {
                let ts = n.as_i64().unwrap_or(0);
                Ok(Value::DateTime(nodedb_types::NdbDateTime::from_millis(ts)))
            }
            serde_json::Value::String(s) => {
                if let Ok(ts) = s.parse::<i64>() {
                    Ok(Value::DateTime(nodedb_types::NdbDateTime::from_millis(ts)))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            _ => Err(format!(
                "column '{col_name}': expected TIMESTAMP, got {val}"
            )),
        },
        ColumnType::Decimal => match val {
            serde_json::Value::Number(n) => {
                let f = n.as_f64().ok_or_else(|| {
                    format!("column '{col_name}': number not representable as f64")
                })?;
                let d = rust_decimal::Decimal::try_from(f)
                    .map_err(|_| format!("column '{col_name}': cannot convert {f} to DECIMAL"))?;
                Ok(Value::Decimal(d))
            }
            serde_json::Value::String(s) => {
                let d: rust_decimal::Decimal = s
                    .parse()
                    .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as DECIMAL"))?;
                Ok(Value::Decimal(d))
            }
            _ => Err(format!("column '{col_name}': expected DECIMAL, got {val}")),
        },
        ColumnType::Vector(dim) => match val {
            serde_json::Value::Array(arr) => {
                let floats: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                if floats.len() != *dim as usize {
                    return Err(format!(
                        "column '{col_name}': expected VECTOR({dim}), got {} elements",
                        floats.len()
                    ));
                }
                let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
                Ok(Value::Bytes(bytes))
            }
            _ => Err(format!(
                "column '{col_name}': expected VECTOR array, got {val}"
            )),
        },
        ColumnType::Geometry => {
            // Store geometry as string (GeoJSON or WKT).
            Ok(Value::String(val.to_string()))
        }
    }
}

/// Convert a typed `Value` back to JSON.
fn typed_value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::Uuid(s) => serde_json::Value::String(s.clone()),
        Value::Ulid(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(dt) => serde_json::json!(dt.micros / 1000),
        Value::Duration(d) => serde_json::json!(d.as_millis()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(typed_value_to_json).collect())
        }
        Value::Object(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), typed_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. } => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::ColumnDef;

    fn test_schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
        }
    }

    #[test]
    fn roundtrip_strict_encoding() {
        let schema = test_schema();
        let json = serde_json::json!({"id": "u1", "name": "Alice", "age": 30});
        let json_bytes = serde_json::to_vec(&json).unwrap();

        let tuple_bytes = json_to_binary_tuple(&json_bytes, &schema).unwrap();

        let decoded = binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u1");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["age"], 30);
    }

    #[test]
    fn nullable_field_omitted() {
        let schema = test_schema();
        let json = serde_json::json!({"id": "u2", "name": "Bob"});
        let json_bytes = serde_json::to_vec(&json).unwrap();

        let tuple_bytes = json_to_binary_tuple(&json_bytes, &schema).unwrap();
        let decoded = binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u2");
        assert_eq!(decoded["name"], "Bob");
        assert!(decoded["age"].is_null());
    }

    #[test]
    fn non_nullable_missing_errors() {
        let schema = test_schema();
        let json = serde_json::json!({"id": "u3"});
        let json_bytes = serde_json::to_vec(&json).unwrap();

        let result = json_to_binary_tuple(&json_bytes, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("NOT NULL"));
    }
}
