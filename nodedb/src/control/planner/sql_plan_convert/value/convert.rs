// SPDX-License-Identifier: BUSL-1.1

//! Conversions from `nodedb_sql::types::SqlValue` into runtime / wire forms.

use nodedb_sql::types::SqlValue;

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
        SqlValue::Uuid(u) => nodedb_types::Value::Uuid(u.clone()),
        SqlValue::Object(fields) => nodedb_types::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), sql_value_to_nodedb_value(value)))
                .collect(),
        ),
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(*dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(*dt),
        SqlValue::Geometry(g) => nodedb_types::Value::Geometry(g.clone()),
        SqlValue::Duration(d) => nodedb_types::Value::Duration(*d),
    }
}

pub(crate) fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) | SqlValue::Uuid(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Timestamp(dt) | SqlValue::Timestamptz(dt) => dt.to_iso8601(),
        SqlValue::Bytes(b) => format!("\\x{}", hex::encode(b)),
        SqlValue::Array(arr) => format_pg_array(arr),
        // The JSON text of the object, as a client reads it.
        SqlValue::Object(_) => {
            crate::util::wire_json::value_to_wire_json(&sql_value_to_nodedb_value(v)).to_string()
        }
        SqlValue::Null => String::new(),
        // GeoJSON text, `crs` member included for a non-WGS 84 SRID.
        SqlValue::Geometry(g) => nodedb_types::geometry::geojson_tree(g).to_string(),
        SqlValue::Duration(d) => d.to_string(),
    }
}

/// The text a document secondary index files `v` under, for an index
/// probe.
///
/// The index files a field by the JSON text of the stored value. Bytes are
/// the JSON text of the bytes value, its base64, so a probe on a bytea
/// literal finds the row that stores those bytes. Every other value is
/// [`sql_value_to_string`], which already matches the stored JSON text.
pub(crate) fn sql_value_to_index_text(v: &SqlValue) -> String {
    if let SqlValue::Bytes(bytes) = v
        && let Ok(encoded) =
            nodedb_types::value_to_msgpack(&nodedb_types::Value::Bytes(bytes.clone()))
        && let Ok(serde_json::Value::String(text)) = nodedb_types::json_from_msgpack(&encoded)
    {
        return text;
    }
    sql_value_to_string(v)
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

/// Raw bytes for a KV key or a single-`value` column body.
///
/// A scalar encodes through `nodedb_types::scalar_to_raw_bytes`, the same
/// rule a KV read-modify-write uses to re-encode a raw body. An array is
/// PostgreSQL array text.
pub(crate) fn sql_value_to_bytes(v: &SqlValue) -> crate::Result<Vec<u8>> {
    match v {
        SqlValue::Array(_) | SqlValue::Object(_) => Ok(sql_value_to_string(v).into_bytes()),
        SqlValue::Int(_)
        | SqlValue::Float(_)
        | SqlValue::Decimal(_)
        | SqlValue::String(_)
        | SqlValue::Bool(_)
        | SqlValue::Null
        | SqlValue::Bytes(_)
        | SqlValue::Uuid(_)
        | SqlValue::Timestamp(_)
        | SqlValue::Timestamptz(_)
        | SqlValue::Geometry(_)
        | SqlValue::Duration(_) => Ok(nodedb_types::scalar_to_raw_bytes(
            &sql_value_to_nodedb_value(v),
        )?),
    }
}

/// Encode a SQL value as plain msgpack for a field-level update literal.
///
/// Every typed scalar keeps its type: an instant is the instant ext, a UUID
/// the UUID ext, a decimal the decimal ext (a `u64` past `i64::MAX` stays a
/// `uint64`). The literal applies to the field as is, so text in their place
/// would store text in a schemaless field.
pub(crate) fn sql_value_to_msgpack(v: &SqlValue) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_typed_msgpack(&sql_value_to_nodedb_value(v)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("update literal: {e}"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::document::store::extract::json_scalar_to_string;

    /// The read-side stringifier (`sql_value_to_string`, which the index-range
    /// read-set capture uses) and the write-side index-key stringifier
    /// (`json_scalar_to_string`) MUST agree on the canonical string for every
    /// scalar type — otherwise a captured `IndexEq` value will never match the
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

    /// A bytes probe reads the index text the stored bytes are filed under.
    #[test]
    fn a_bytes_probe_matches_the_index_text_of_stored_bytes() {
        let stored = nodedb_types::json_from_msgpack(
            &nodedb_types::value_to_msgpack(&nodedb_types::Value::Bytes(vec![1, 2]))
                .expect("encode"),
        )
        .expect("json");
        assert_eq!(
            json_scalar_to_string(&stored),
            Some(sql_value_to_index_text(&SqlValue::Bytes(vec![1, 2])))
        );
        assert_eq!(sql_value_to_index_text(&SqlValue::Int(7)), "7");
    }

    #[test]
    fn raw_bytes_follow_the_shared_scalar_rule() {
        let cases = [
            (SqlValue::String("v1".into()), b"v1".to_vec()),
            (SqlValue::Int(7), b"7".to_vec()),
            (SqlValue::Bool(false), b"false".to_vec()),
            (SqlValue::Bytes(vec![0xff]), vec![0xff]),
            (SqlValue::Null, Vec::new()),
        ];
        for (sql, expected) in cases {
            assert_eq!(sql_value_to_bytes(&sql).unwrap(), expected, "{sql:?}");
        }
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
