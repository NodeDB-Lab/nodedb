// SPDX-License-Identifier: Apache-2.0

//! Rewrite pgvector's `<->` distance operator into a `vector_distance()`
//! function call that standard sqlparser can parse.

use nodedb_types::starts_with_ascii_case_insensitive;

use super::lex::{SqlSegment, find_operator_positions, has_operator_outside_literals, segments};

/// Rewrite all occurrences of `expr <-> expr` to `vector_distance(expr, expr)`.
///
/// Handles: `column_name <-> ARRAY[...]`, `column <-> $param`, etc.
/// Returns `None` if no valid `<->` patterns are found outside string
/// literals, quoted identifiers, or comments.
pub(super) fn rewrite_arrow_distance(sql: &str) -> Option<String> {
    rewrite_binary_op(sql, "<->", "vector_distance")
}

/// Extract the left operand before `<->`: a column name or dotted path.
fn extract_left_operand(before: &str) -> Option<String> {
    // The operand ends `before`, so the caller can find it by length. A literal,
    // quoted identifier, or comment in that place is no operand this rewrite takes.
    let segs = segments(before);
    let Some(SqlSegment::Text(text_before)) = segs.last() else {
        return None;
    };
    let trimmed = text_before.trim_end();
    // The character before the identifier can be multi-byte, so this trims and never adds 1.
    let start = trimmed
        .trim_end_matches(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        .len();
    let ident = &trimmed[start..];
    if ident.is_empty() {
        return None;
    }
    // Reconstruct including the prefix text up to the ident within `before`.
    // We need the ident string only (the caller uses its length for offset math
    // and only the text content for the rewrite).
    Some(ident.to_string())
}

/// Extract the right operand after `<->`: ARRAY[...], $param, or identifier.
/// Returns (operand_text, consumed_length).
fn extract_right_operand(after: &str) -> Option<(String, usize)> {
    let trimmed = after.trim_start();
    if starts_with_ascii_case_insensitive(trimmed, "ARRAY[") {
        let mut depth = 0;
        for (i, c) in trimmed.char_indices() {
            match c {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some((trimmed[..=i].to_string(), i + 1));
                    }
                }
                _ => {}
            }
        }
        None
    } else if trimmed.starts_with('$') {
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '$')
            .unwrap_or(trimmed.len());
        Some((trimmed[..end].to_string(), end))
    } else {
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
            .unwrap_or(trimmed.len());
        if end == 0 {
            return None;
        }
        Some((trimmed[..end].to_string(), end))
    }
}

/// Rewrite all occurrences of `expr <=> expr` to `vector_cosine_distance(expr, expr)`.
///
/// Handles: `column_name <=> ARRAY[...]`, `column <=> $param`, etc.
/// Returns `None` if no valid `<=>` patterns are found outside string
/// literals, quoted identifiers, or comments.
pub(super) fn rewrite_cosine_distance(sql: &str) -> Option<String> {
    rewrite_binary_op(sql, "<=>", "vector_cosine_distance")
}

/// Rewrite all occurrences of `expr <#> expr` to `vector_neg_inner_product(expr, expr)`.
///
/// Handles: `column_name <#> ARRAY[...]`, `column <#> $param`, etc.
/// Returns `None` if no valid `<#>` patterns are found outside string
/// literals, quoted identifiers, or comments.
pub(super) fn rewrite_neg_inner_product(sql: &str) -> Option<String> {
    rewrite_binary_op(sql, "<#>", "vector_neg_inner_product")
}

/// Generic binary-operator rewriter: replaces all occurrences of `op`
/// (outside literals/comments/quoted idents) with `func_name(left, right)`.
fn rewrite_binary_op(sql: &str, op: &str, func_name: &str) -> Option<String> {
    if !has_operator_outside_literals(sql, op) {
        return None;
    }

    let positions = find_operator_positions(sql, op);
    if positions.is_empty() {
        return None;
    }

    let mut result = String::with_capacity(sql.len());
    let mut consumed = 0usize;
    let mut found = false;

    for op_pos in positions {
        if op_pos < consumed {
            continue;
        }

        let before = &sql[consumed..op_pos];
        let left = extract_left_operand(before)?;
        // The left operand is the trailing identifier of `before`. Subtract
        // its length from the *trimmed* end of `before` — using `before.len()`
        // directly is off-by-one whenever there is whitespace between the
        // column and the operator (e.g. `embedding <-> ARRAY[...]`), which
        // would leave the column's first character behind in the result and
        // corrupt the rewritten function name (`evector_distance(...)`).
        let trimmed_end_len = before.trim_end().len();
        let left_start = consumed + (trimmed_end_len - left.len());

        let after_op = &sql[op_pos + op.len()..];
        let (right, right_len) = extract_right_operand(after_op.trim_start())?;
        let ws_skip = after_op.len() - after_op.trim_start().len();

        result.push_str(&sql[consumed..left_start]);
        result.push_str(&format!("{func_name}({left}, {right})"));
        consumed = op_pos + op.len() + ws_skip + right_len;
        found = true;
    }

    if !found {
        return None;
    }

    result.push_str(&sql[consumed..]);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_match_returns_none() {
        assert!(rewrite_arrow_distance("SELECT * FROM t").is_none());
    }

    #[test]
    fn arrow_in_string_literal_ignored() {
        // The `<->` is inside a string literal — no rewrite should happen.
        assert!(rewrite_arrow_distance("SELECT '<->'").is_none());
    }

    #[test]
    fn arrow_in_line_comment_ignored() {
        assert!(rewrite_arrow_distance("SELECT col -- has <-> in comment\nFROM t").is_none());
    }

    #[test]
    fn arrow_in_block_comment_ignored() {
        assert!(rewrite_arrow_distance("SELECT /* <-> */ x").is_none());
    }

    #[test]
    fn arrow_in_quoted_ident_ignored() {
        assert!(rewrite_arrow_distance(r#"SELECT "col_<->""#).is_none());
    }

    #[test]
    fn nested_block_comment_arrow_ignored() {
        assert!(rewrite_arrow_distance("SELECT /* /* nested */ <-> */ x").is_none());
    }

    #[test]
    fn basic_array_rewrite() {
        let rewritten = rewrite_arrow_distance(
            "SELECT title FROM articles ORDER BY embedding <-> ARRAY[0.1, 0.2, 0.3] LIMIT 5",
        )
        .unwrap();
        assert!(
            rewritten.contains("vector_distance(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {rewritten}"
        );
        assert!(!rewritten.contains("<->"));
    }

    #[test]
    fn array_operand_with_unicode_case_expansions_preserves_original_text() {
        let rewritten = rewrite_arrow_distance(
            "SELECT * FROM docs ORDER BY embedding <-> ARRAY['ﬀ', 'İ', 'ß']",
        )
        .expect("array operand should rewrite");
        assert!(rewritten.contains("ARRAY['ﬀ', 'İ', 'ß']"));
    }

    #[test]
    fn rewrite_with_param() {
        let rewritten =
            rewrite_arrow_distance("SELECT * FROM docs WHERE embedding <-> $1 < 0.5").unwrap();
        assert!(
            rewritten.contains("vector_distance(embedding, $1)"),
            "got: {rewritten}"
        );
    }

    // ── <=> (cosine) ──────────────────────────────────────────────────────

    #[test]
    fn cosine_basic_array_rewrite() {
        let rewritten = rewrite_cosine_distance(
            "SELECT title FROM articles ORDER BY embedding <=> ARRAY[0.1, 0.2, 0.3] LIMIT 5",
        )
        .unwrap();
        assert!(
            rewritten.contains("vector_cosine_distance(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {rewritten}"
        );
        assert!(!rewritten.contains("<=>"));
    }

    #[test]
    fn cosine_in_string_literal_ignored() {
        assert!(rewrite_cosine_distance("SELECT '<=>'").is_none());
    }

    #[test]
    fn cosine_in_line_comment_ignored() {
        assert!(rewrite_cosine_distance("SELECT col -- has <=> in comment\nFROM t").is_none());
    }

    #[test]
    fn cosine_in_quoted_ident_ignored() {
        assert!(rewrite_cosine_distance(r#"SELECT "col_<=>""#).is_none());
    }

    #[test]
    fn cosine_no_match_returns_none() {
        assert!(rewrite_cosine_distance("SELECT * FROM t").is_none());
    }

    // ── <#> (neg-inner-product) ───────────────────────────────────────────

    #[test]
    fn nip_basic_array_rewrite() {
        let rewritten = rewrite_neg_inner_product(
            "SELECT title FROM articles ORDER BY embedding <#> ARRAY[0.1, 0.2, 0.3] LIMIT 5",
        )
        .unwrap();
        assert!(
            rewritten.contains("vector_neg_inner_product(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {rewritten}"
        );
        assert!(!rewritten.contains("<#>"));
    }

    #[test]
    fn nip_in_string_literal_ignored() {
        assert!(rewrite_neg_inner_product("SELECT '<#>'").is_none());
    }

    #[test]
    fn nip_in_line_comment_ignored() {
        assert!(rewrite_neg_inner_product("SELECT col -- has <#> in comment\nFROM t").is_none());
    }

    #[test]
    fn nip_in_quoted_ident_ignored() {
        assert!(rewrite_neg_inner_product(r#"SELECT "col_<#>""#).is_none());
    }

    #[test]
    fn nip_no_match_returns_none() {
        assert!(rewrite_neg_inner_product("SELECT * FROM t").is_none());
    }

    // ── Multi-byte text before an operand ─────────────────────────────────

    /// A multi-byte character directly before the left operand is not an
    /// identifier character, and the slice stays on a character boundary.
    #[test]
    fn a_multi_byte_character_before_the_left_operand_does_not_panic() {
        for (sql, expected) in [
            ("ةSu<#>s", "ةvector_neg_inner_product(Su, s)"),
            ("é col <#> s", "é vector_neg_inner_product(col, s)"),
        ] {
            assert_eq!(rewrite_neg_inner_product(sql).as_deref(), Some(expected));
        }
        assert_eq!(
            rewrite_arrow_distance("日本語a <-> b").as_deref(),
            Some("日本語vector_distance(a, b)")
        );
        assert_eq!(
            rewrite_cosine_distance("ßa <=> $1").as_deref(),
            Some("ßvector_cosine_distance(a, $1)")
        );
    }

    /// A quoted identifier, literal, or comment directly before the operator is
    /// not an operand. The rewrite leaves the text alone, and never takes a word
    /// from before it.
    #[test]
    fn a_non_text_segment_before_the_operator_is_no_operand() {
        for sql in [
            "SELECT \"a é\" <-> b",
            "SELECT 'x' <-> b",
            "SELECT col /* c */ <-> b",
            "SELECT col -- c\n <-> b",
        ] {
            assert_eq!(rewrite_arrow_distance(sql), None, "{sql}");
        }
    }

    /// The exact input the `sql_preprocess` fuzz target found.
    #[test]
    fn the_fuzz_input_runs_through_the_pipeline() {
        let sql = std::str::from_utf8(b"\xd3\xa9Su<#>s\"L  ").expect("utf-8 input");
        let _ = crate::parser::preprocess::pipeline::preprocess(sql);
    }
}
