//! Schema inference, field coercion, bitemporal column injection, and
//! schema-ordered row <-> object conversion.

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

/// Build a `nodedb_types::Value::Object` from a schema-ordered row. Used
/// by the ON CONFLICT DO UPDATE path to present `existing` and `EXCLUDED`
/// rows to `apply_on_conflict_updates` in the same shape the document
/// upsert path uses.
pub(super) fn row_values_to_object(schema: &ColumnarSchema, row: &[Value]) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col, val) in schema.columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), val.clone());
    }
    nodedb_types::Value::Object(map)
}

/// Coerce a `nodedb_types::Value` field to match the column type.
pub(super) fn ndb_field_to_value(val: Option<&Value>, col_type: &ColumnType) -> Value {
    let Some(val) = val else { return Value::Null };
    match (col_type, val) {
        (_, Value::Null) => Value::Null,
        (ColumnType::Int64, Value::Integer(_)) => val.clone(),
        (ColumnType::Int64, Value::Float(f)) => Value::Integer(*f as i64),
        (ColumnType::Int64, Value::String(s)) => {
            s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
        }
        (ColumnType::Float64, Value::Float(_)) => val.clone(),
        (ColumnType::Float64, Value::Integer(n)) => Value::Float(*n as f64),
        (ColumnType::Float64, Value::String(s)) => {
            s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        (ColumnType::Bool, Value::Bool(_)) => val.clone(),
        (ColumnType::String, Value::String(_)) => val.clone(),
        (ColumnType::Timestamp, Value::Integer(n)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*n))
        }
        (ColumnType::Timestamp, Value::Float(f)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*f as i64))
        }
        (ColumnType::Timestamp, Value::String(s)) => nodedb_types::datetime::NdbDateTime::parse(s)
            .map(Value::DateTime)
            .unwrap_or_else(|| Value::String(s.clone())),
        (ColumnType::Uuid, Value::String(_)) => val.clone(),
        // Fallback: integers as floats, strings as strings.
        (ColumnType::Float64, _) => Value::Null,
        (ColumnType::Int64, _) => Value::Null,
        _ => val.clone(),
    }
}

/// Infer a columnar schema from a `nodedb_types::Value::Object` (first row).
pub(super) fn infer_schema_from_value(row: &Value) -> ColumnarSchema {
    let obj = match row {
        Value::Object(m) => m,
        _ => {
            return ColumnarSchema::new(vec![ColumnDef::required("value", ColumnType::Float64)])
                .expect("single-column schema");
        }
    };

    let mut columns = Vec::new();
    for (key, val) in obj {
        let lower = key.to_lowercase();
        let col_type = if lower == "timestamp" || lower == "ts" || lower == "time" {
            ColumnType::Timestamp
        } else {
            match val {
                Value::Float(_) => ColumnType::Float64,
                Value::Integer(_) => ColumnType::Int64,
                Value::Bool(_) => ColumnType::Bool,
                _ => ColumnType::String,
            }
        };
        if lower == "id" {
            columns.push(ColumnDef::required(key.clone(), col_type).with_primary_key());
        } else {
            columns.push(ColumnDef::nullable(key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(ColumnDef::required("value", ColumnType::Float64));
    }

    ColumnarSchema::new(columns).expect("inferred schema must be valid")
}

/// Prepend the three reserved bitemporal columns (`_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until`) at positions 0/1/2 of a columnar
/// schema. All three are required Int64; `_ts_system` is engine-stamped
/// on every write, the valid-time pair is client-provided (or defaults
/// to the open interval).
pub(super) fn prepend_bitemporal_columns(base: ColumnarSchema) -> ColumnarSchema {
    let mut cols = Vec::with_capacity(3 + base.columns.len());
    cols.push(ColumnDef::required("_ts_system", ColumnType::Int64));
    cols.push(ColumnDef::required("_ts_valid_from", ColumnType::Int64));
    cols.push(ColumnDef::required("_ts_valid_until", ColumnType::Int64));
    cols.extend(base.columns);
    ColumnarSchema::new(cols).expect("bitemporal columnar schema must be valid")
}

/// Infer a columnar schema from a JSON object — used by the spatial insert path.
pub(super) fn infer_schema_from_json(row: &serde_json::Value) -> ColumnarSchema {
    let ndb: Value = row.clone().into();
    infer_schema_from_value(&ndb)
}
