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

/// Build the SQL INSERT statements for the field maps of `rows`, in row order.
/// See `fields_to_insert_sql` for the identifier quoting rationale and
/// `write_statements` for the grouping rule.
pub(in crate::control::server::shared::ddl::neutral::collection) fn rows_to_insert_sqls(
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> Vec<String> {
    write_statements("INSERT INTO", collection, rows)
}

/// Build the SQL UPSERT statements for the field maps of `rows`, in row order.
/// See `fields_to_insert_sql` for the identifier quoting rationale and
/// `write_statements` for the grouping rule.
pub(in crate::control::server::shared::ddl::neutral::collection) fn fields_to_upsert_sqls(
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> Vec<String> {
    write_statements("UPSERT INTO", collection, rows)
}

/// One statement per run of consecutive rows that name the same keys. A key a
/// row does not name stays out of its statement, so a declared DEFAULT fills it
/// on insert and an upsert leaves the stored field alone. A statement never
/// writes `NULL` for a key its row did not supply.
fn write_statements(
    keyword: &str,
    collection: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> Vec<String> {
    let mut statements = Vec::new();
    let mut start = 0;
    for end in 1..=rows.len() {
        let run_ends = end == rows.len()
            || rows[end].len() != rows[start].len()
            || rows[end].keys().any(|k| !rows[start].contains_key(k));
        if run_ends {
            statements.push(fields_to_write_sql(keyword, collection, &rows[start..end]));
            start = end;
        }
    }
    statements
}

/// The column list is the sorted union of every row's keys. The caller passes
/// rows that name the same keys, so no cell is filled.
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
        let sql = fields_to_upsert_sqls("t", std::slice::from_ref(&fields)).remove(0);
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
        let sql = fields_to_upsert_sqls("t", std::slice::from_ref(&fields)).remove(0);
        assert!(sql.contains("\"a\"\"); DROP COLLECTION other; --\""));
    }

    /// A non-finite float is the `FLOAT8` cast of its text, never a bare
    /// `NaN` or `inf` the planner would read as a column name.
    #[test]
    fn upsert_sql_writes_a_nan_float_as_a_float8_cast() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::NAN));
        let sql = fields_to_upsert_sqls("t", std::slice::from_ref(&fields)).remove(0);
        assert!(sql.ends_with(", 'NaN'::FLOAT8)"), "{sql}");
    }

    #[test]
    fn upsert_sql_writes_an_infinite_float_as_a_float8_cast() {
        let fields = one_field("score", nodedb_types::Value::Float(f64::INFINITY));
        let sql = fields_to_upsert_sqls("t", std::slice::from_ref(&fields)).remove(0);
        assert!(sql.ends_with(", 'Infinity'::FLOAT8)"), "{sql}");
    }

    #[test]
    fn upsert_sql_writes_one_tuple_per_row_of_the_same_keys() {
        let mut first = HashMap::new();
        first.insert("id".into(), nodedb_types::Value::String("a".into()));
        first.insert("n".into(), nodedb_types::Value::Integer(1));
        let mut second = first.clone();
        second.insert("n".into(), nodedb_types::Value::Integer(2));
        let sqls = fields_to_upsert_sqls("t", &[first, second]);
        assert_eq!(sqls.len(), 1);
        assert!(
            sqls[0].starts_with("UPSERT INTO t (\"id\", \"n\") VALUES ("),
            "{}",
            sqls[0]
        );
        assert_eq!(sqls[0].matches("), (").count(), 1, "{}", sqls[0]);
    }

    #[test]
    fn a_row_missing_a_key_never_gets_a_null_for_it() {
        let mut first = HashMap::new();
        first.insert("id".into(), nodedb_types::Value::String("a".into()));
        first.insert("n".into(), nodedb_types::Value::Integer(1));
        let mut second = HashMap::new();
        second.insert("id".into(), nodedb_types::Value::String("b".into()));
        for sqls in [
            rows_to_insert_sqls("t", &[first.clone(), second.clone()]),
            fields_to_upsert_sqls("t", &[first, second]),
        ] {
            assert_eq!(sqls.len(), 2, "{sqls:?}");
            assert!(!sqls[1].contains("NULL"), "{}", sqls[1]);
            assert!(!sqls[1].contains("\"n\""), "{}", sqls[1]);
        }
    }
}
