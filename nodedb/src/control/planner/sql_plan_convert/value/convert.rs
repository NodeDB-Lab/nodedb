//! Conversions from `nodedb_sql::types::SqlValue` into runtime / wire forms.

use nodedb_sql::types::SqlValue;

use super::msgpack_write::write_msgpack_value;

pub(crate) fn sql_value_to_nodedb_value(v: &SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Int(i) => nodedb_types::Value::Integer(*i),
        SqlValue::Float(f) => nodedb_types::Value::Float(*f),
        SqlValue::String(s) => nodedb_types::Value::String(s.clone()),
        SqlValue::Bool(b) => nodedb_types::Value::Bool(*b),
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(sql_value_to_nodedb_value).collect())
        }
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b.clone()),
    }
}

pub(crate) fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

pub(crate) fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().as_bytes().to_vec(),
        _ => sql_value_to_string(v).into_bytes(),
    }
}

/// Encode a SQL value as standard msgpack for field-level updates.
pub(crate) fn sql_value_to_msgpack(v: &SqlValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    write_msgpack_value(&mut buf, v);
    buf
}
