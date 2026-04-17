//! SQL pre-processing orchestrator: rewrite NodeDB-specific syntax into
//! standard SQL before handing to sqlparser-rs.
//!
//! Handles:
//! - `UPSERT INTO coll (cols) VALUES (vals)` → `INSERT INTO ...` + upsert flag
//! - `INSERT INTO coll { key: 'val', ... }` → `INSERT INTO coll (key) VALUES ('val')`
//! - `UPSERT INTO coll { ... }` → both rewrites combined
//! - `expr <-> expr` → `vector_distance(expr, expr)`
//! - `{ key: val }` in function args → JSON string literal

use super::function_args::rewrite_object_literal_args;
use super::object_literal_stmt::try_rewrite_object_literal;
use super::vector_ops::rewrite_arrow_distance;

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

    let is_upsert = upper.starts_with("UPSERT INTO ");

    if is_upsert {
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

    if upper.starts_with("INSERT INTO ")
        && let Some(result) = try_rewrite_object_literal(trimmed)
    {
        return Some(PreprocessedSql {
            sql: result,
            is_upsert: false,
        });
    }

    let mut sql_buf = trimmed.to_string();
    let mut any_rewrite = false;

    if sql_buf.contains("<->")
        && let Some(rewritten) = rewrite_arrow_distance(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if (sql_buf.contains("{ ") || sql_buf.contains("{f") || sql_buf.contains("{d"))
        && let Some(rewritten) = rewrite_object_literal_args(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if any_rewrite {
        return Some(PreprocessedSql {
            sql: sql_buf,
            is_upsert: false,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::function_args::rewrite_object_literal_args;
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
        assert!(result.sql.contains("VALUES"));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("'bob'"));
        assert!(result.sql.contains("30"));
        assert!(result.sql.contains("25"));
        let values_part = result.sql.split("VALUES").nth(1).unwrap();
        let row_count = values_part.matches('(').count();
        assert_eq!(row_count, 2, "should have 2 row groups: {}", result.sql);
    }

    #[test]
    fn batch_array_heterogeneous_keys() {
        let result =
            preprocess("INSERT INTO docs [{ id: 'a', name: 'Alice' }, { id: 'b', role: 'admin' }]")
                .unwrap();
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

    #[test]
    fn arrow_distance_operator_select() {
        let result = preprocess(
            "SELECT title FROM articles ORDER BY embedding <-> ARRAY[0.1, 0.2, 0.3] LIMIT 5",
        )
        .unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {}",
            result.sql
        );
        assert!(!result.sql.contains("<->"));
    }

    #[test]
    fn arrow_distance_operator_where() {
        let result =
            preprocess("SELECT * FROM docs WHERE embedding <-> ARRAY[1.0, 2.0] < 0.5").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[1.0, 2.0])"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn arrow_distance_no_match() {
        assert!(preprocess("SELECT * FROM users WHERE age > 30").is_none());
    }

    #[test]
    fn arrow_distance_with_alias() {
        let result =
            preprocess("SELECT embedding <-> ARRAY[0.1, 0.2] AS dist FROM articles").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2]) AS dist"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_in_function() {
        let direct = rewrite_object_literal_args(
            "SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })",
        );
        assert!(direct.is_some(), "rewrite_object_literal_args should match");
        let rewritten = direct.unwrap();
        assert!(
            rewritten.contains("\"fuzzy\""),
            "direct rewrite should contain JSON, got: {}",
            rewritten
        );

        let result =
            preprocess("SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })")
                .unwrap();
        assert!(
            !result.sql.contains("{ fuzzy"),
            "should not contain object literal, got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_with_distance() {
        let result = preprocess(
            "SELECT * FROM articles WHERE text_match(title, 'test', { fuzzy: true, distance: 2 })",
        )
        .unwrap();
        assert!(result.sql.contains("\"fuzzy\""), "got: {}", result.sql);
        assert!(result.sql.contains("\"distance\""), "got: {}", result.sql);
    }

    #[test]
    fn object_literal_not_rewritten_outside_function() {
        let result = preprocess("INSERT INTO docs { name: 'Alice' }").unwrap();
        assert!(result.sql.contains("VALUES"), "got: {}", result.sql);
    }
}
