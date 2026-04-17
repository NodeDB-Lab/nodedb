//! SQL literal canonicalization for `nodedb_types::Value`.

/// Convert a `nodedb_types::Value` to a SQL literal string.
///
/// Used by pre-processing and by Origin's pgwire handlers to build SQL
/// from parsed field maps. Handles all Value variants.
pub fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                // NaN / ±inf have no canonical SQL literal form. Emitting
                // `NaN` or `inf` would be parsed as an identifier
                // reference by the planner and either bind to an unrelated
                // column or fail with an opaque error. Canonicalize to
                // NULL instead; the column value becomes unknown, which
                // matches the semantic of a non-finite numeric.
                "NULL".to_string()
            }
        }
        nodedb_types::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        nodedb_types::Value::Null => "NULL".to_string(),
        nodedb_types::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(value_to_sql_literal).collect();
            format!("ARRAY[{}]", inner.join(", "))
        }
        nodedb_types::Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("'\\x{hex}'")
        }
        nodedb_types::Value::Object(_) => "NULL".to_string(),
        nodedb_types::Value::Uuid(u) => format!("'{u}'"),
        nodedb_types::Value::Ulid(u) => format!("'{u}'"),
        nodedb_types::Value::DateTime(dt) => format!("'{dt}'"),
        nodedb_types::Value::Duration(d) => format!("'{d}'"),
        nodedb_types::Value::Decimal(d) => d.to_string(),
        other => format!("'{}'", format!("{other:?}").replace('\'', "''")),
    }
}
