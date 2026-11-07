// SPDX-License-Identifier: Apache-2.0

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
use super::lex::{first_sql_word, has_brace_outside_literals, has_operator_outside_literals};
use super::object_literal_stmt::{text_after_into, try_rewrite_object_literal};
use super::search_vector::{
    try_rewrite_nested_search_using_vector, try_rewrite_search_using_vector,
};
use super::temporal::extract as extract_temporal;
use super::vector_ops::{
    rewrite_arrow_distance, rewrite_cosine_distance, rewrite_neg_inner_product,
};
use crate::error::SqlError;
use crate::temporal::TemporalScope;

/// Result of pre-processing a SQL string.
#[derive(Debug)]
pub struct PreprocessedSql {
    /// The rewritten SQL (standard SQL that sqlparser can handle).
    pub sql: String,
    /// Whether the original statement was UPSERT (not INSERT).
    pub is_upsert: bool,
    /// Bitemporal qualifier extracted from `FOR SYSTEM_TIME` /
    /// `FOR VALID_TIME` / `__system_as_of__(...)`. Default when none
    /// was present.
    pub temporal: TemporalScope,
}

/// Pre-process a SQL string, rewriting NodeDB-specific syntax.
///
/// Returns `Ok(None)` if no rewriting was needed. Temporal clause parse
/// errors bubble up as `SqlError::Parse` so they surface to the caller
/// identically to sqlparser errors.
pub fn preprocess(sql: &str) -> Result<Option<PreprocessedSql>, SqlError> {
    let trimmed = sql.trim();

    // Extract temporal clauses first — they can appear in both SELECT and
    // INSERT...SELECT, and stripping them before the UPSERT/object-literal
    // rewrites keeps those rewriters pattern-free of NodeDB extensions.
    let (temporal_sql, temporal) =
        match extract_temporal(trimmed).map_err(|e| SqlError::Parse { detail: e.0 })? {
            Some(ex) => (ex.sql, ex.temporal),
            None => (trimmed.to_string(), TemporalScope::default()),
        };
    let any_temporal = temporal != TemporalScope::default();

    // Rewrite `SEARCH <coll> USING VECTOR(...)` to canonical
    // `SELECT * FROM <coll> ORDER BY vector_distance(...) LIMIT k` before any
    // first-word dispatch — the rewritten form re-enters the rest of the
    // pipeline as a plain SELECT.
    //
    // The same clause in subquery position — `FROM (SEARCH ...) s` — is
    // rewritten in place so a k-NN result composes with joins and filters.
    let (temporal_sql, search_vector_rewritten) =
        match try_rewrite_search_using_vector(&temporal_sql) {
            Some(rewritten) => (rewritten, true),
            None => match try_rewrite_nested_search_using_vector(&temporal_sql) {
                Some(rewritten) => (rewritten, true),
                None => (temporal_sql, false),
            },
        };

    let first_word = first_sql_word(&temporal_sql)
        .map(|w| w.to_uppercase())
        .unwrap_or_default();
    let is_upsert = first_word == "UPSERT";

    if is_upsert {
        let Some(after_into) = text_after_into(&temporal_sql) else {
            return Err(SqlError::Parse {
                detail: "UPSERT needs the form UPSERT INTO <collection> ...".to_string(),
            });
        };
        let rewritten = format!("INSERT INTO {after_into}");
        // `?`: a malformed object literal — or a clause the literal form cannot
        // carry — must fail the statement here rather than fall through to be
        // reparsed as something else, which would let the error disappear.
        if let Some(result) = try_rewrite_object_literal(&rewritten)? {
            return Ok(Some(PreprocessedSql {
                sql: result,
                is_upsert: true,
                temporal,
            }));
        }
        return Ok(Some(PreprocessedSql {
            sql: rewritten,
            is_upsert: true,
            temporal,
        }));
    }

    if first_word == "INSERT"
        && let Some(result) = try_rewrite_object_literal(&temporal_sql)?
    {
        return Ok(Some(PreprocessedSql {
            sql: result,
            is_upsert: false,
            temporal,
        }));
    }

    let mut sql_buf = temporal_sql;
    let mut any_rewrite = any_temporal || search_vector_rewritten;

    if has_operator_outside_literals(&sql_buf, "<->")
        && let Some(rewritten) = rewrite_arrow_distance(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if has_operator_outside_literals(&sql_buf, "<=>")
        && let Some(rewritten) = rewrite_cosine_distance(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if has_operator_outside_literals(&sql_buf, "<#>")
        && let Some(rewritten) = rewrite_neg_inner_product(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if has_brace_outside_literals(&sql_buf)
        && let Some(rewritten) = rewrite_object_literal_args(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if any_rewrite {
        return Ok(Some(PreprocessedSql {
            sql: sql_buf,
            is_upsert: false,
            temporal,
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::super::function_args::rewrite_object_literal_args;
    use super::*;

    fn pp(sql: &str) -> Option<PreprocessedSql> {
        super::preprocess(sql).unwrap()
    }

    #[test]
    fn passthrough_standard_sql() {
        assert!(pp("SELECT * FROM users").is_none());
        assert!(pp("INSERT INTO users (name) VALUES ('alice')").is_none());
        assert!(pp("DELETE FROM users WHERE id = 1").is_none());
    }

    #[test]
    fn upsert_rewrite() {
        let result = pp("UPSERT INTO users (name) VALUES ('alice')").unwrap();
        assert!(result.is_upsert);
        assert_eq!(result.sql, "INSERT INTO users (name) VALUES ('alice')");
    }

    #[test]
    fn object_literal_insert() {
        let result = pp("INSERT INTO users { name: 'alice', age: 30 }").unwrap();
        assert!(!result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("30"));
    }

    #[test]
    fn object_literal_upsert() {
        let result = pp("UPSERT INTO users { name: 'bob' }").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'bob'"));
    }

    /// The brace form reconstructs the statement from its fields, so a clause
    /// written after the literal cannot be carried. It is refused, by name —
    /// the failure this guards is the earlier behavior, where the clause was
    /// dropped and the statement succeeded as though it had never been written.
    #[test]
    fn a_clause_after_the_object_literal_is_refused_not_dropped() {
        for sql in [
            "INSERT INTO users { name: 'alice' } RETURNING *",
            "UPSERT INTO users { name: 'alice' } RETURNING *",
            "INSERT INTO users { name: 'alice' } ON CONFLICT (id) DO NOTHING",
            "INSERT INTO users [{ name: 'alice' }] RETURNING *",
        ] {
            let error = super::preprocess(sql)
                .expect_err("a clause the brace form cannot carry must fail the statement");
            let detail = error.to_string();
            assert!(
                detail.contains("RETURNING") || detail.contains("ON CONFLICT"),
                "the refusal must name the clause it could not carry; sql = {sql}, got {detail}"
            );
        }
    }

    /// …while the clean forms, and a statement terminator, still rewrite.
    #[test]
    fn a_trailing_semicolon_is_not_a_clause() {
        assert!(pp("INSERT INTO users { name: 'alice' };").is_some());
        assert!(pp("INSERT INTO users [{ name: 'alice' }];").is_some());
    }

    #[test]
    fn batch_array_insert() {
        let result =
            pp("INSERT INTO users [{ name: 'alice', age: 30 }, { name: 'bob', age: 25 }]").unwrap();
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
            pp("INSERT INTO docs [{ id: 'a', name: 'Alice' }, { id: 'b', role: 'admin' }]")
                .unwrap();
        assert!(result.sql.contains("NULL"));
        assert!(result.sql.contains("'Alice'"));
        assert!(result.sql.contains("'admin'"));
    }

    #[test]
    fn batch_array_upsert() {
        let result =
            pp("UPSERT INTO users [{ id: 'u1', name: 'a' }, { id: 'u2', name: 'b' }]").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.contains("VALUES"));
    }

    #[test]
    fn arrow_distance_operator_select() {
        let result =
            pp("SELECT title FROM articles ORDER BY embedding <-> ARRAY[0.1, 0.2, 0.3] LIMIT 5")
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
        let result = pp("SELECT * FROM docs WHERE embedding <-> ARRAY[1.0, 2.0] < 0.5").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[1.0, 2.0])"),
            "got: {}",
            result.sql
        );
        // Guard against computing the left-operand start with
        // `before.len() - left.len()`, which is off-by-one when
        // there is whitespace between the column and `<->` — that leaves the
        // column's leading character in the prefix, producing
        // `WHERE evector_distance(embedding, ...)`. Lock the corrected slice.
        assert!(
            !result.sql.contains("evector_distance"),
            "rewriter must not leave the column's leading char in the prefix; got: {}",
            result.sql
        );
        assert!(
            result.sql.contains("WHERE vector_distance("),
            "rewritten WHERE clause must start `WHERE vector_distance(`; got: {}",
            result.sql
        );
    }

    #[test]
    fn arrow_distance_operator_where_no_whitespace_before_op() {
        // No whitespace between column and `<->` — the spacing-sensitive
        // off-by-one cannot fire here; the rewriter must still produce a
        // clean `vector_distance(embedding, ARRAY[...])` form.
        let result = pp("SELECT id FROM docs WHERE embedding<->ARRAY[1.0, 2.0]").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[1.0, 2.0])"),
            "got: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("evector_distance"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn arrow_distance_no_match() {
        assert!(pp("SELECT * FROM users WHERE age > 30").is_none());
    }

    #[test]
    fn arrow_distance_with_alias() {
        let result = pp("SELECT embedding <-> ARRAY[0.1, 0.2] AS dist FROM articles").unwrap();
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
            pp("SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })").unwrap();
        assert!(
            !result.sql.contains("{ fuzzy"),
            "should not contain object literal, got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_with_distance() {
        let result = pp(
            "SELECT * FROM articles WHERE text_match(title, 'test', { fuzzy: true, distance: 2 })",
        )
        .unwrap();
        assert!(result.sql.contains("\"fuzzy\""), "got: {}", result.sql);
        assert!(result.sql.contains("\"distance\""), "got: {}", result.sql);
    }

    #[test]
    fn object_literal_not_rewritten_outside_function() {
        let result = pp("INSERT INTO docs { name: 'Alice' }").unwrap();
        assert!(result.sql.contains("VALUES"), "got: {}", result.sql);
    }

    #[test]
    fn comment_prefix_insert_routes_correctly() {
        // A block comment before INSERT must still trigger the INSERT path.
        let result = pp("/* hint */ INSERT INTO t VALUES ({})");
        // Either rewrites (object literal) or passes through as INSERT — must
        // not trigger the UPSERT path.
        if let Some(r) = result {
            assert!(!r.is_upsert);
        }
    }

    #[test]
    fn comment_prefix_upsert_routes_correctly() {
        // A block comment before UPSERT must still trigger the UPSERT path.
        let result = pp("/* hint */ UPSERT INTO t (name) VALUES ('a')").unwrap();
        assert!(result.is_upsert);
        assert_eq!(result.sql, "INSERT INTO t (name) VALUES ('a')");
    }

    #[test]
    fn line_comment_before_insert_does_not_trigger_insert() {
        // `-- INSERT INTO t` is a comment; the real statement is SELECT 1.
        // It must pass through without upsert rewriting.
        let result = pp("-- INSERT INTO t\nSELECT 1");
        assert!(
            result.is_none(),
            "line-commented INSERT must pass through, got: {result:?}"
        );
    }

    #[test]
    fn string_literal_brace_not_rewritten_as_object_literal() {
        // `{ foo }` is inside a string literal — must not be touched.
        let result = pp("SELECT func('{ foo }')");
        assert!(
            result.is_none() || !result.unwrap().sql.contains("\"foo\""),
            "string literal brace must not be rewritten"
        );
    }

    /// A statement can end at any byte, so a verb alone is never a panic.
    #[test]
    fn a_verb_with_no_into_does_not_panic() {
        for sql in [
            "INSERT",
            "INSERT ",
            "INSERT INTO",
            "INSERT é",
            "INSERT INTOx",
        ] {
            assert!(preprocess(sql).is_ok(), "{sql}");
        }
        for sql in ["UPSERT", "UPSERT ", "UPSERT INTO", "UPSERT é"] {
            let error = preprocess(sql).expect_err(sql);
            assert!(matches!(error, SqlError::Parse { .. }), "{sql}: {error}");
        }
    }

    /// Any whitespace can separate the keywords, and the collection name stays whole.
    #[test]
    fn whitespace_between_the_keywords_keeps_the_collection_name() {
        let rewritten = pp("INSERT   INTO\n docs { id: 'a' }").expect("object literal rewrites");
        assert!(
            rewritten.sql.starts_with("INSERT INTO docs "),
            "{}",
            rewritten.sql
        );
        let rewritten = pp("UPSERT   INTO docs { id: 'a' }").expect("object literal rewrites");
        assert!(rewritten.is_upsert);
        assert!(
            rewritten.sql.starts_with("INSERT INTO docs "),
            "{}",
            rewritten.sql
        );
    }
}
