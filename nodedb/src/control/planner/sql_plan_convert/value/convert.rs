// SPDX-License-Identifier: BUSL-1.1

//! Conversions from `nodedb_sql::types::SqlValue` into runtime / wire forms.

use nodedb_sql::types::SqlValue;

use super::msgpack_write::write_msgpack_value;

pub(crate) fn sql_value_to_nodedb_value(v: &SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Int(i) => nodedb_types::Value::Integer(*i),
        SqlValue::Float(f) => nodedb_types::Value::Float(*f),
        SqlValue::Decimal(d) => nodedb_types::Value::Decimal(*d),
        SqlValue::String(s) => nodedb_types::Value::String(s.clone()),
        SqlValue::Bool(b) => nodedb_types::Value::Bool(*b),
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(sql_value_to_nodedb_value).collect())
        }
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b.clone()),
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(*dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(*dt),
    }
}

pub(crate) fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Timestamp(dt) | SqlValue::Timestamptz(dt) => dt.to_iso8601(),
        SqlValue::Bytes(b) => format!("\\x{}", hex_encode(b)),
        SqlValue::Array(arr) => format_pg_array(arr),
        SqlValue::Null => String::new(),
    }
}

fn format_pg_array(values: &[SqlValue]) -> String {
    let elements = values
        .iter()
        .map(|value| match value {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::String(s) if pg_array_string_needs_quotes(s) => {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('\"', "\\\""))
            }
            SqlValue::String(s) => s.clone(),
            other => sql_value_to_string(other),
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", elements.join(","))
}

fn pg_array_string_needs_quotes(value: &str) -> bool {
    value.is_empty()
        || value.eq_ignore_ascii_case("null")
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, ',' | '{' | '}' | '"' | '\\'))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub(crate) fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().as_bytes().to_vec(),
        SqlValue::Decimal(d) => d.to_string().as_bytes().to_vec(),
        _ => sql_value_to_string(v).into_bytes(),
    }
}

/// Encode a SQL value as standard msgpack for field-level updates.
pub(crate) fn sql_value_to_msgpack(v: &SqlValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    write_msgpack_value(&mut buf, v);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::document::store::extract::json_scalar_to_string;

    /// The read-side stringifier (`sql_value_to_string`, which the index-range
    /// read-set capture uses) and the write-side index-key stringifier
    /// (`json_scalar_to_string`) MUST agree on the canonical string for every
    /// scalar type — otherwise a captured `IndexEq` value would never match the
    /// index key a write records. This parity is the load-bearing guarantee the
    /// per-value comparison (a later change) will rest on.
    #[test]
    fn arrays_use_postgresql_text_syntax() {
        assert_eq!(
            sql_value_to_string(&SqlValue::Array(vec![
                SqlValue::String("public".into()),
                SqlValue::String("two words".into()),
                SqlValue::String("NULL".into()),
                SqlValue::Null,
            ])),
            "{public,\"two words\",\"NULL\",NULL}"
        );
    }

    #[test]
    fn read_and_write_scalar_stringifiers_agree() {
        let cases = [
            (
                SqlValue::String("a@b.c".to_string()),
                serde_json::json!("a@b.c"),
            ),
            (SqlValue::Int(42), serde_json::json!(42)),
            (SqlValue::Float(1.5), serde_json::json!(1.5)),
            (SqlValue::Bool(true), serde_json::json!(true)),
            (SqlValue::Bool(false), serde_json::json!(false)),
        ];
        for (sql, json) in cases {
            let read_side = sql_value_to_string(&sql);
            let write_side =
                json_scalar_to_string(&json).expect("scalar json must stringify to Some");
            assert_eq!(
                read_side, write_side,
                "read/write stringifiers diverge for {sql:?}"
            );
        }
    }
}
