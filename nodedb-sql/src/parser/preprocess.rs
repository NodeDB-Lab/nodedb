//! SQL pre-processing: rewrite NodeDB-specific syntax into standard SQL
//! before handing to sqlparser-rs.
//!
//! Handles:
//! - `UPSERT INTO coll (cols) VALUES (vals)` → `INSERT INTO coll (cols) VALUES (vals)` + upsert flag
//! - `INSERT INTO coll { key: 'val', ... }` → `INSERT INTO coll (key) VALUES ('val')` + object literal flag
//! - `UPSERT INTO coll { key: 'val', ... }` → both rewrites combined

use super::object_literal::parse_object_literal;

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
    if upper.starts_with("INSERT INTO ") {
        if let Some(result) = try_rewrite_object_literal(trimmed) {
            return Some(PreprocessedSql {
                sql: result,
                is_upsert: false,
            });
        }
    }

    None
}

/// Try to rewrite `INSERT INTO coll { key: val, ... }` → `INSERT INTO coll (key) VALUES (val)`.
///
/// Returns `None` if the statement doesn't use object literal syntax.
fn try_rewrite_object_literal(sql: &str) -> Option<String> {
    // Find collection name after INSERT INTO.
    let after_into = sql["INSERT INTO ".len()..].trim_start();
    let coll_end = after_into.find(|c: char| c.is_whitespace())?;
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    if !rest.starts_with('{') {
        return None;
    }

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();
    let fields = parse_object_literal(obj_str)?.ok()?;

    if fields.is_empty() {
        return None;
    }

    // Build standard INSERT INTO coll (cols) VALUES (vals).
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());

    // Sort for deterministic output.
    let mut entries: Vec<_> = fields.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (key, value) in entries {
        cols.push(key);
        vals.push(value_to_sql_literal(&value));
    }

    Some(format!(
        "INSERT INTO {} ({}) VALUES ({})",
        coll_name,
        cols.join(", "),
        vals.join(", ")
    ))
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
}
