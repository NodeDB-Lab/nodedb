//! SQL pre-processing: rewrite NodeDB-specific syntax into standard SQL
//! before handing to sqlparser-rs.
//!
//! Handles:
//! - `UPSERT INTO coll (cols) VALUES (vals)` → `INSERT INTO coll (cols) VALUES (vals)` + upsert flag
//! - `INSERT INTO coll { key: 'val', ... }` → `INSERT INTO coll (key) VALUES ('val')` + object literal flag
//! - `UPSERT INTO coll { key: 'val', ... }` → both rewrites combined

use super::object_literal::{parse_object_literal, parse_object_literal_array};

/// Result of pre-processing a SQL string.
pub struct PreprocessedSql {
    /// The rewritten SQL (standard SQL that sqlparser can handle).
    pub sql: String,
    /// Whether the original statement was UPSERT (not INSERT).
    pub is_upsert: bool,
}

/// Pre-process a SQL string, rewriting NodeDB-specific syntax.
///
/// Returns `None` if no rewriting was needed (pass through to sqlparser as-is).
pub fn preprocess(sql: &str) -> Option<PreprocessedSql> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Check for UPSERT INTO.
    let is_upsert = upper.starts_with("UPSERT INTO ");

    if is_upsert {
        // Rewrite UPSERT INTO → INSERT INTO, then check for { } literal.
        let rewritten = format!("INSERT INTO {}", &trimmed["UPSERT INTO ".len()..]);
        if let Some(result) = try_rewrite_object_literal(&rewritten) {
            return Some(PreprocessedSql {
                sql: result,
                is_upsert: true,
            });
        }
        return Some(PreprocessedSql {
            sql: rewritten,
            is_upsert: true,
        });
    }

    // Check for INSERT INTO coll { ... } object literal syntax.
    if upper.starts_with("INSERT INTO ")
        && let Some(result) = try_rewrite_object_literal(trimmed)
    {
        return Some(PreprocessedSql {
            sql: result,
            is_upsert: false,
        });
    }

    None
}

/// Try to rewrite `INSERT INTO coll { ... }` or `INSERT INTO coll [{ ... }, { ... }]`
/// into standard `INSERT INTO coll (cols) VALUES (row1), (row2)`.
///
/// Returns `None` if the statement doesn't use object literal syntax.
fn try_rewrite_object_literal(sql: &str) -> Option<String> {
    // Find collection name after INSERT INTO.
    let after_into = sql["INSERT INTO ".len()..].trim_start();
    let coll_end = after_into.find(|c: char| c.is_whitespace())?;
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();

    if obj_str.starts_with('[') {
        // Array form: INSERT INTO coll [{ ... }, { ... }]
        return rewrite_array_form(coll_name, obj_str);
    }

    if !obj_str.starts_with('{') {
        return None;
    }

    // Single object form: INSERT INTO coll { ... }
    let fields = parse_object_literal(obj_str)?.ok()?;
    if fields.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &[fields]))
}

/// Rewrite `[{ ... }, { ... }]` → multi-row VALUES.
fn rewrite_array_form(coll_name: &str, obj_str: &str) -> Option<String> {
    let objects = parse_object_literal_array(obj_str)?.ok()?;
    if objects.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &objects))
}

/// Build `INSERT INTO coll (col_union) VALUES (row1), (row2), ...`
///
/// Collects the union of all keys across all rows. Missing keys get NULL.
fn fields_to_values_sql(
    coll_name: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    // Collect union of all keys, sorted for deterministic output.
    let mut all_keys: Vec<String> = rows
        .iter()
        .flat_map(|r| r.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    all_keys.sort();

    let col_list = all_keys.join(", ");

    let row_strs: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = all_keys
                .iter()
                .map(|k| match row.get(k) {
                    Some(v) => value_to_sql_literal(v),
                    None => "NULL".to_string(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        coll_name,
        col_list,
        row_strs.join(", ")
    )
}

/// Convert a `nodedb_types::Value` to a SQL literal string.
///
/// Used by pre-processing and by Origin's pgwire handlers to build SQL
/// from parsed field maps. Handles all Value variants.
pub fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => format!("{f}"),
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
        // Exotic types: format as string literal for SQL passthrough.
        other => format!("'{}'", format!("{other:?}").replace('\'', "''")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_standard_sql() {
        assert!(preprocess("SELECT * FROM users").is_none());
        assert!(preprocess("INSERT INTO users (name) VALUES ('alice')").is_none());
        assert!(preprocess("DELETE FROM users WHERE id = 1").is_none());
    }

    #[test]
    fn upsert_rewrite() {
        let result = preprocess("UPSERT INTO users (name) VALUES ('alice')").unwrap();
        assert!(result.is_upsert);
        assert_eq!(result.sql, "INSERT INTO users (name) VALUES ('alice')");
    }

    #[test]
    fn object_literal_insert() {
        let result = preprocess("INSERT INTO users { name: 'alice', age: 30 }").unwrap();
        assert!(!result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("30"));
    }

    #[test]
    fn object_literal_upsert() {
        let result = preprocess("UPSERT INTO users { name: 'bob' }").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'bob'"));
    }

    #[test]
    fn batch_array_insert() {
        let result =
            preprocess("INSERT INTO users [{ name: 'alice', age: 30 }, { name: 'bob', age: 25 }]")
                .unwrap();
        assert!(!result.is_upsert);
        // Should produce multi-row VALUES: ... VALUES (...), (...)
        assert!(result.sql.contains("VALUES"));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("'bob'"));
        assert!(result.sql.contains("30"));
        assert!(result.sql.contains("25"));
        // Two row groups separated by comma
        let values_part = result.sql.split("VALUES").nth(1).unwrap();
        let row_count = values_part.matches('(').count();
        assert_eq!(row_count, 2, "should have 2 row groups: {}", result.sql);
    }

    #[test]
    fn batch_array_heterogeneous_keys() {
        let result =
            preprocess("INSERT INTO docs [{ id: 'a', name: 'Alice' }, { id: 'b', role: 'admin' }]")
                .unwrap();
        // Union of keys: id, name, role — missing keys get NULL.
        assert!(result.sql.contains("NULL"));
        assert!(result.sql.contains("'Alice'"));
        assert!(result.sql.contains("'admin'"));
    }

    #[test]
    fn batch_array_upsert() {
        let result =
            preprocess("UPSERT INTO users [{ id: 'u1', name: 'a' }, { id: 'u2', name: 'b' }]")
                .unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.contains("VALUES"));
    }
}
