// SPDX-License-Identifier: BUSL-1.1

/// Quote a user-supplied map key for safe inclusion as a SQL column
/// identifier. Object-literal keys come from untrusted client payloads;
/// concatenating them raw into generated SQL allows a crafted key to
/// close the column list and inject arbitrary statements. Double-quoted
/// identifiers are the SQL standard form; embedded double quotes are
/// escaped by doubling.
fn quote_column_identifier(key: &str) -> String {
    format!("\"{}\"", key.replace('"', "\"\""))
}

/// Build a SQL INSERT statement from field map.
///
/// Produces `INSERT INTO coll ("col1", "col2") VALUES ('val1', 'val2')`.
/// Column identifiers are double-quoted so that map keys containing
/// punctuation, whitespace, or SQL syntax are treated as a single
/// identifier by the downstream parser instead of fragmenting the
/// statement.
pub(in crate::control::server::shared::ddl::neutral::collection) fn fields_to_insert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    fields_to_write_sql("INSERT INTO", collection, fields)
}

/// Build a SQL UPSERT statement from field map. See
/// `fields_to_insert_sql` for the identifier quoting rationale.
pub(in crate::control::server::shared::ddl::neutral::collection) fn fields_to_upsert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    fields_to_write_sql("UPSERT INTO", collection, fields)
}

fn fields_to_write_sql(
    keyword: &str,
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());
    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(key, _)| key.as_str());

    for (key, value) in entries {
        cols.push(quote_column_identifier(key));
        vals.push(value_to_sql_literal(value));
    }

    format!(
        "{keyword} {} ({}) VALUES ({})",
        collection,
        cols.join(", "),
        vals.join(", ")
    )
}

/// Delegate to the shared implementation in nodedb-sql.
fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    nodedb_sql::parser::preprocess::value_to_sql_literal(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn one_field(key: &str, value: nodedb_types::Value) -> HashMap<String, nodedb_types::Value> {
        let mut fields = HashMap::new();
        fields.insert("id".into(), nodedb_types::Value::String("r1".into()));
        fields.insert(key.into(), value);
        fields
    }

    #[test]
    fn upsert_sql_quotes_injection_key_as_single_identifier() {
        let fields = one_field(
            "a); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(1),
        );
        let sql = fields_to_upsert_sql("t", &fields);
        let quoted = "\"a); DROP COLLECTION other; --\"";
        assert!(sql.contains(quoted));
        for part in sql.split(quoted) {
            assert!(!part.contains("DROP"));
        }
    }

    #[test]
    fn insert_sql_quotes_injection_key_as_single_identifier() {
        let fields = one_field(
            "b); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(2),
        );
        let sql = fields_to_insert_sql("t", &fields);
        let quoted = "\"b); DROP COLLECTION other; --\"";
        assert!(sql.contains(quoted));
        for part in sql.split(quoted) {
            assert!(!part.contains("DROP"));
        }
    }

    #[test]
    fn upsert_sql_escapes_embedded_double_quote_in_key() {
        let fields = one_field(
            "a\"); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(1),
        );
        let sql = fields_to_upsert_sql("t", &fields);
        assert!(sql.contains("\"a\"\"); DROP COLLECTION other; --\""));
    }

    #[test]
    fn upsert_sql_canonicalizes_nan_float() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::NAN));
        let sql = fields_to_upsert_sql("t", &fields);
        assert!(!sql.contains("NaN"));
    }

    #[test]
    fn upsert_sql_canonicalizes_infinity_float() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::INFINITY));
        let sql = fields_to_upsert_sql("t", &fields);
        assert!(!sql.contains("inf"));
    }
}
