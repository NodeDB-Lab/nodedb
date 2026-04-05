//! RETURNING clause pre-processing for DML statements.
//!
//! DataFusion does not support RETURNING on DML (INSERT/UPDATE/DELETE).
//! This module detects and strips the RETURNING clause from raw SQL before
//! DataFusion planning, storing a flag so the response handler can format
//! the Data Plane's returned documents as a pgwire QueryResponse.

/// Check if a DML statement contains a RETURNING clause and strip it.
///
/// Returns `(cleaned_sql, has_returning)`. The cleaned SQL has the
/// `RETURNING ...` suffix removed so DataFusion can parse it.
///
/// Only strips RETURNING from UPDATE and DELETE statements (INSERT
/// RETURNING is handled separately in `collection_insert.rs`).
pub(super) fn strip_returning(sql: &str) -> (String, bool) {
    let upper = sql.to_uppercase();

    // Only process UPDATE and DELETE statements.
    let trimmed = upper.trim_start();
    if !trimmed.starts_with("UPDATE") && !trimmed.starts_with("DELETE") {
        return (sql.to_string(), false);
    }

    // Find the last occurrence of RETURNING (case-insensitive).
    // RETURNING must appear as a standalone keyword, not inside a string literal.
    if let Some(pos) = find_returning_keyword(&upper) {
        let cleaned = sql[..pos].trim_end().to_string();
        (cleaned, true)
    } else {
        (sql.to_string(), false)
    }
}

/// Find the byte offset of the RETURNING keyword in uppercased SQL.
///
/// Skips occurrences inside string literals (single-quoted).
fn find_returning_keyword(upper: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let keyword = b"RETURNING";
    let kw_len = keyword.len();

    if bytes.len() < kw_len {
        return None;
    }

    let mut in_string = false;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_string = !in_string;
            i += 1;
            continue;
        }

        if in_string {
            i += 1;
            continue;
        }

        // Check for RETURNING keyword with word boundaries.
        if i + kw_len <= bytes.len()
            && &bytes[i..i + kw_len] == keyword
            && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
            && (i + kw_len >= bytes.len() || !bytes[i + kw_len].is_ascii_alphanumeric())
        {
            return Some(i);
        }

        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_returning_from_update() {
        let (sql, has) = strip_returning(
            "UPDATE products SET stock = stock - 1 WHERE id = 'p1' RETURNING id, stock",
        );
        assert!(has);
        assert_eq!(sql, "UPDATE products SET stock = stock - 1 WHERE id = 'p1'");
    }

    #[test]
    fn strips_returning_from_delete() {
        let (sql, has) = strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING *");
        assert!(has);
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
    }

    #[test]
    fn no_returning() {
        let (sql, has) = strip_returning("UPDATE products SET stock = 0 WHERE id = 'p1'");
        assert!(!has);
        assert_eq!(sql, "UPDATE products SET stock = 0 WHERE id = 'p1'");
    }

    #[test]
    fn returning_in_string_literal_ignored() {
        let (sql, has) =
            strip_returning("UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'");
        assert!(!has);
        assert_eq!(
            sql,
            "UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'"
        );
    }

    #[test]
    fn select_not_affected() {
        let (sql, has) = strip_returning("SELECT * FROM products");
        assert!(!has);
        assert_eq!(sql, "SELECT * FROM products");
    }

    #[test]
    fn case_insensitive() {
        let (sql, has) =
            strip_returning("update products set stock = 0 where id = 'p1' returning id");
        assert!(has);
        assert_eq!(sql, "update products set stock = 0 where id = 'p1'");
    }
}
