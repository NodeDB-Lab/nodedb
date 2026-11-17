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
    fields_to_write_sql("INSERT INTO", collection, std::slice::from_ref(fields))
}

/// Build one SQL INSERT statement from the field maps of its rows, in row
/// order. See `fields_to_insert_sql` for the identifier quoting rationale.
pub(in crate::control::server::shared::ddl::neutral::collection) fn rows_to_insert_sql(
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    fields_to_write_sql("INSERT INTO", collection, rows)
}

/// Build one SQL UPSERT statement from the field maps of its rows, in row
/// order. See `fields_to_insert_sql` for the identifier quoting rationale.
pub(in crate::control::server::shared::ddl::neutral::collection) fn fields_to_upsert_sql(
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    fields_to_write_sql("UPSERT INTO", collection, rows)
}

/// The column list is the sorted union of every row's keys. A row missing
/// a column writes `NULL` there.
fn fields_to_write_sql(
    keyword: &str,
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    let columns: std::collections::BTreeSet<&str> = rows
        .iter()
        .flat_map(|row| row.keys().map(String::as_str))
        .collect();
    let cols: Vec<String> = columns
        .iter()
        .map(|key| quote_column_identifier(key))
        .collect();
    let tuples: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = columns
                .iter()
                .map(|key| match row.get(*key) {
                    Some(value) => value_to_sql_literal(value),
                    None => "NULL".to_owned(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    format!(
        "{keyword} {} ({}) VALUES {}",
        collection,
        cols.join(", "),
        tuples.join(", ")
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
        let sql = fields_to_upsert_sql("t", std::slice::from_ref(&fields));
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
        let sql = fields_to_upsert_sql("t", std::slice::from_ref(&fields));
        assert!(sql.contains("\"a\"\"); DROP COLLECTION other; --\""));
    }

    /// A non-finite float is the `FLOAT8` cast of its text, never a bare
    /// `NaN` or `inf` the planner would read as a column name.
    #[test]
    fn upsert_sql_writes_a_nan_float_as_a_float8_cast() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::NAN));
        let sql = fields_to_upsert_sql("t", std::slice::from_ref(&fields));
        assert!(sql.ends_with(", 'NaN'::FLOAT8)"), "{sql}");
    }

    #[test]
    fn upsert_sql_writes_an_infinite_float_as_a_float8_cast() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::INFINITY));
        let sql = fields_to_upsert_sql("t", std::slice::from_ref(&fields));
        assert!(sql.ends_with(", 'Infinity'::FLOAT8)"), "{sql}");
    }

    #[test]
    fn upsert_sql_writes_one_tuple_per_row_over_the_column_union() {
        let mut first = HashMap::new();
        first.insert("id".into(), nodedb_types::Value::String("a".into()));
        first.insert("n".into(), nodedb_types::Value::Integer(1));
        let mut second = HashMap::new();
        second.insert("id".into(), nodedb_types::Value::String("b".into()));
        let sql = fields_to_upsert_sql("t", &[first, second]);
        assert!(
            sql.starts_with("UPSERT INTO t (\"id\", \"n\") VALUES ("),
            "{sql}"
        );
        assert_eq!(sql.matches("), (").count(), 1, "{sql}");
        assert!(sql.ends_with(", NULL)"), "{sql}");
    }
}
