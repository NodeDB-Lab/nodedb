// SPDX-License-Identifier: BUSL-1.1

//! Stripping a `RETURNING` clause from raw DML SQL text before planning.
//!
//! The planner does not parse RETURNING on DML, so the clause is removed from
//! the raw SQL before planning; the item text is resolved later, once the
//! target collection is known.

use crate::Error;
use nodedb_sql::parser::preprocess::lex::keyword_position_outside_literals;
use nodedb_types::starts_with_ascii_case_insensitive;

const RETURNING_KEYWORD: &str = "RETURNING";

/// Check if a DML statement contains a RETURNING clause and strip it.
///
/// Returns `(cleaned_sql, returning_items)`. The cleaned SQL has the
/// `RETURNING ...` suffix removed so the planner can parse it, and
/// `returning_items` is the raw item text after the keyword, resolved later by
/// [`super::resolve_returning_clause`] once the target collection is known.
///
/// RETURNING is honored on INSERT, UPSERT, UPDATE, DELETE and MERGE. Whether
/// the resulting plan has a slot to carry the clause is not decidable from the
/// statement text — it depends on the shape the planner produces — so that
/// judgement is made once the plan exists, by
/// [`super::refuse_unprojectable_insert_returning`].
pub fn strip_returning(sql: &str) -> Result<(String, Option<String>), Error> {
    let trimmed = sql.trim_start();

    // Gated on the DML verbs rather than on "everything that is not a SELECT",
    // so an unrelated statement whose text merely contains the word is never
    // truncated at it.
    if !starts_with_ascii_case_insensitive(trimmed, "INSERT")
        && !starts_with_ascii_case_insensitive(trimmed, "UPSERT")
        && !starts_with_ascii_case_insensitive(trimmed, "UPDATE")
        && !starts_with_ascii_case_insensitive(trimmed, "DELETE")
        && !starts_with_ascii_case_insensitive(trimmed, "MERGE")
    {
        return Ok((sql.to_string(), None));
    }

    if let Some(pos) = keyword_position_outside_literals(sql, RETURNING_KEYWORD) {
        let cleaned = sql[..pos].trim_end().to_string();
        let items = sql[pos + RETURNING_KEYWORD.len()..].trim();
        if items.is_empty() {
            return Err(Error::BadRequest {
                detail: "empty RETURNING column list".into(),
            });
        }
        Ok((cleaned, Some(items.to_string())))
    } else {
        Ok((sql.to_string(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The document engines carry the clause, so it is stripped like any
    /// other verb's — the statement text alone cannot decide the engine, so
    /// nothing is refused here.
    #[test]
    fn insert_returning_is_stripped() {
        let (sql, items) =
            strip_returning("INSERT INTO items (id, name) VALUES ('a', 'alpha') RETURNING *")
                .expect("INSERT RETURNING must plan");
        assert_eq!(sql, "INSERT INTO items (id, name) VALUES ('a', 'alpha')");
        assert_eq!(items.as_deref(), Some("*"));

        let (sql, items) = strip_returning("insert into items (id) values ('a') returning id AS k")
            .expect("INSERT RETURNING must plan");
        assert_eq!(sql, "insert into items (id) values ('a')");
        assert_eq!(items.as_deref(), Some("id AS k"));
    }

    /// An INSERT with no such clause is untouched — planning must not turn
    /// ordinary inserts into errors.
    #[test]
    fn a_plain_insert_is_untouched() {
        let sql = "INSERT INTO items (id, name) VALUES ('a', 'alpha')";
        let (out, items) = strip_returning(sql).expect("a plain insert must plan");
        assert_eq!(out, sql);
        assert!(items.is_none());
    }

    /// The word inside a string literal is data, not a clause.
    #[test]
    fn returning_inside_a_string_literal_is_not_a_clause() {
        let sql = "INSERT INTO items (id, note) VALUES ('a', 'RETURNING soon')";
        let (out, items) = strip_returning(sql).expect("a quoted keyword is not a clause");
        assert_eq!(out, sql);
        assert!(items.is_none());
    }

    /// Only DML verbs are scanned for the clause: a SELECT whose column name
    /// merely embeds the word is left alone.
    #[test]
    fn a_non_dml_statement_is_not_scanned_for_the_clause() {
        let sql = "SELECT returning_count FROM items";
        let (out, items) = strip_returning(sql).expect("a select must pass through");
        assert_eq!(out, sql);
        assert!(items.is_none());
    }

    #[test]
    fn strips_star_returning_from_update() {
        let (sql, items) =
            strip_returning("UPDATE products SET stock = 1 WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "UPDATE products SET stock = 1 WHERE id = 'p1'");
        assert_eq!(items.as_deref(), Some("*"));
    }

    #[test]
    fn strips_named_columns_returning_from_update() {
        let (sql, items) = strip_returning(
            "UPDATE products SET stock = stock - 1 WHERE id = 'p1' RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(sql, "UPDATE products SET stock = stock - 1 WHERE id = 'p1'");
        assert_eq!(items.as_deref(), Some("id, stock"));
    }

    #[test]
    fn strips_returning_from_delete() {
        let (sql, items) =
            strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING id").unwrap();
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
        assert_eq!(items.as_deref(), Some("id"));
    }

    #[test]
    fn strips_returning_from_merge() {
        let (sql, items) = strip_returning(
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
        );
        assert_eq!(items.as_deref(), Some("id, stock"));
    }

    #[test]
    fn merge_without_returning_is_unchanged() {
        let original = "MERGE INTO products t USING staging s ON t.id = s.id \
                        WHEN MATCHED THEN DELETE";
        let (sql, items) = strip_returning(original).unwrap();
        assert!(items.is_none());
        assert_eq!(sql, original);
    }

    #[test]
    fn returning_inside_identifier_not_treated_as_keyword() {
        // A collection whose name embeds "returning" (with `_` as an
        // identifier boundary) must NOT match the RETURNING keyword inside the
        // name — the real keyword is the trailing one after WHERE.
        let (sql, items) =
            strip_returning("DELETE FROM orders_returning WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
        assert_eq!(items.as_deref(), Some("*"));

        let (sql, items) = strip_returning("DELETE FROM orders_returning WHERE id = 'p1'").unwrap();
        assert!(items.is_none());
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
    }

    #[test]
    fn case_insensitive() {
        let (sql, items) =
            strip_returning("update products set stock = 0 where id = 'p1' returning id").unwrap();
        assert_eq!(sql, "update products set stock = 0 where id = 'p1'");
        assert_eq!(items.as_deref(), Some("id"));
    }

    #[test]
    fn unicode_identifier_before_returning_preserves_original_offsets() {
        let (sql, items) = strip_returning("DELETE FROM tﬀﬀ RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM tﬀﬀ");
        assert_eq!(items.as_deref(), Some("*"));
    }

    /// The raw item text is kept verbatim: an expression is resolved later
    /// against the target, never rejected at the strip.
    #[test]
    fn an_expression_survives_the_strip() {
        let (sql, items) = strip_returning("UPDATE t SET x=1 RETURNING x*2 AS d").unwrap();
        assert_eq!(sql, "UPDATE t SET x=1");
        assert_eq!(items.as_deref(), Some("x*2 AS d"));
    }

    #[test]
    fn an_empty_item_list_is_refused() {
        assert!(strip_returning("UPDATE t SET x=1 RETURNING ").is_err());
    }
}
