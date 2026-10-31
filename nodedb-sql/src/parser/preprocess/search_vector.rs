// SPDX-License-Identifier: Apache-2.0

//! Rewrite `SEARCH <coll> USING VECTOR(<field>, ARRAY[...], <k>)` to the
//! canonical `SELECT * FROM <coll> ORDER BY vector_distance(<field>, ARRAY[...]) LIMIT <k>`.
//!
//! `<field>` may be omitted (and the third arg becomes the limit). When the
//! collection has a single declared vector column the planner resolves the
//! field; otherwise `vector_distance` rejects the call with a typed error.
//!
//! The same clause is also accepted in subquery position — `FROM (SEARCH ...)
//! s`, `IN (SELECT id FROM (SEARCH ...) s)` — so a k-NN result composes with
//! joins and relational filters inside one statement.

use super::lex::{find_ascii_case_insensitive, find_operator_positions};

const SEARCH_KEYWORD: &str = "SEARCH";
const USING_KEYWORD: &str = "USING";
const VECTOR_KEYWORD: &str = "VECTOR";

pub fn try_rewrite_search_using_vector(sql: &str) -> Option<String> {
    let trimmed = sql.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let leading = trimmed.len() - trimmed.trim_start().len();
    let (select, consumed) = parse_search_clause(&trimmed[leading..], "*")?;
    // A top-level `SEARCH` clause owns the whole statement; anything past the
    // closing paren is not part of the DSL form and must not be rewritten.
    if !trimmed[leading + consumed..].trim().is_empty() {
        return None;
    }

    let trailing = &sql[trimmed.len()..];
    Some(format!("{select}{trailing}"))
}

/// Rewrite every `(SEARCH <coll> USING VECTOR(...))` occurrence that sits in
/// subquery position into `(SELECT ...)`, leaving the rest of `sql` untouched.
///
/// Returns `None` when no occurrence was rewritten.
pub fn try_rewrite_nested_search_using_vector(sql: &str) -> Option<String> {
    find_ascii_case_insensitive(sql, SEARCH_KEYWORD)?;

    let mut out = String::new();
    let mut copied = 0usize;
    for open in find_operator_positions(sql, "(") {
        if open < copied {
            continue;
        }
        let after_open = open + 1;
        let inner = &sql[after_open..];
        let leading = inner.len() - inner.trim_start().len();
        let projection = in_operand(&sql[..open]).unwrap_or("*");
        let Some((select, consumed)) = parse_search_clause(&inner[leading..], projection) else {
            continue;
        };
        out.push_str(&sql[copied..after_open]);
        out.push_str(&select);
        copied = after_open + leading + consumed;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// The column on the left of an `IN` (or `NOT IN`) that ends `before`, or
/// `None` when the parenthesis is not an `IN` operand's list. A bare
/// `col IN (SEARCH ...)` selects that column from the matches, so the list
/// holds the values the operand compares against.
fn in_operand(before: &str) -> Option<&str> {
    let head = trailing_word(before, "IN")?;
    let head = trailing_word(head, "NOT").unwrap_or(head);
    let head = head.trim_end();
    let start = head
        .char_indices()
        .rev()
        .take_while(|&(_, c)| is_ident_char(c))
        .last()
        .map(|(i, _)| i)?;
    Some(&head[start..]).filter(|c| !c.starts_with(|d: char| d.is_ascii_digit()))
}

/// `text` without its last word when that word is `word` (case-insensitive,
/// whole word); `None` otherwise.
fn trailing_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let text = text.trim_end();
    let at = text.len().checked_sub(word.len())?;
    if !text.is_char_boundary(at) || !text[at..].eq_ignore_ascii_case(word) {
        return None;
    }
    let head = &text[..at];
    (!head.ends_with(is_ident_char)).then_some(head)
}

/// Parse a `SEARCH <coll> USING VECTOR(...)` clause at the start of `input`.
///
/// Returns the canonical `SELECT` and the number of bytes consumed (through
/// the clause's closing paren).
fn parse_search_clause(input: &str, projection: &str) -> Option<(String, usize)> {
    let after_search = keyword_rest(input, SEARCH_KEYWORD)?;
    let (collection, rest) = take_identifier(after_search.trim_start())?;
    let after_using = keyword_rest(rest, USING_KEYWORD)?;
    let after_vector = keyword_rest(after_using, VECTOR_KEYWORD)?;
    let (body, consumed_args) = take_parenthesized(after_vector)?;
    let (field, vector_expr, limit) = split_vector_args(body)?;

    let order_by = match field {
        Some(name) => format!("vector_distance({name}, {vector_expr})"),
        None => format!("vector_distance({vector_expr})"),
    };
    let select = format!("SELECT {projection} FROM {collection} ORDER BY {order_by} LIMIT {limit}");
    let consumed = input.len() - after_vector.len() + consumed_args;
    Some((select, consumed))
}

/// Match `keyword` at the start of `input` (case-insensitive, whole word) and
/// return the remainder with leading whitespace stripped.
fn keyword_rest<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    // Compare bytes: slicing `trimmed` at `keyword.len()` would panic when the
    // input starts with a multi-byte character. An ASCII match guarantees the
    // offset is a char boundary, so the slices below are safe.
    let bytes = trimmed.as_bytes();
    if bytes.len() < keyword.len()
        || !bytes[..keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
    {
        return None;
    }
    let rest = &trimmed[keyword.len()..];
    if rest.chars().next().is_some_and(is_ident_char) {
        return None;
    }
    Some(rest.trim_start())
}

/// Take a collection name: a bare identifier, or a double-quoted one. The
/// quoted form is returned with its quotes intact so the rewritten `SELECT`
/// keeps the exact spelling the user wrote — a name that needs quoting still
/// needs it after the rewrite.
fn take_identifier(input: &str) -> Option<(&str, &str)> {
    if input.starts_with('"') {
        return take_quoted_identifier(input);
    }
    let end = input
        .char_indices()
        .find(|(_, c)| !is_ident_char(*c))
        .map(|(i, _)| i)
        .unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    Some((&input[..end], &input[end..]))
}

/// Take a `"quoted name"`, honouring the doubled-quote escape (`""`). Returns
/// the quoted span verbatim and the remainder.
fn take_quoted_identifier(input: &str) -> Option<(&str, &str)> {
    let mut chars = input.char_indices().skip(1);
    while let Some((offset, c)) = chars.next() {
        if c != '"' {
            continue;
        }
        // A doubled quote is an escaped quote, not the end of the identifier.
        if input[offset + 1..].starts_with('"') {
            chars.next();
            continue;
        }
        let end = offset + 1;
        // An empty name ("") is not a usable collection reference.
        if end == 2 {
            return None;
        }
        return Some((&input[..end], &input[end..]));
    }
    None
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Take a parenthesized group at the start of `input`, returning its body and
/// the number of bytes consumed (through the matching close paren). Parens
/// inside string literals do not affect nesting depth.
fn take_parenthesized(input: &str) -> Option<(&str, usize)> {
    if !input.starts_with('(') {
        return None;
    }
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    for (offset, c) in input.char_indices() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some((input[1..offset].trim(), offset + 1));
                }
            }
            _ => {}
        }
    }
    None
}

fn split_vector_args(body: &str) -> Option<(Option<String>, String, String)> {
    let parts = split_top_level_commas(body);
    match parts.as_slice() {
        [field, vec, k] => {
            let field = field.trim();
            let trimmed = if field.is_empty() {
                None
            } else {
                Some(field.to_string())
            };
            Some((trimmed, vec.trim().to_string(), k.trim().to_string()))
        }
        [vec, k] => Some((None, vec.trim().to_string(), k.trim().to_string())),
        _ => None,
    }
}

fn split_top_level_commas(body: &str) -> Vec<String> {
    let mut depth_paren = 0i32;
    let mut depth_bracket = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut current = String::new();
    let mut out = Vec::new();
    for c in body.chars() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth_paren += 1,
            ')' if !in_single && !in_double => depth_paren -= 1,
            '[' if !in_single && !in_double => depth_bracket += 1,
            ']' if !in_single && !in_double => depth_bracket -= 1,
            ',' if !in_single && !in_double && depth_paren == 0 && depth_bracket == 0 => {
                out.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_three_arg_form() {
        let out = try_rewrite_search_using_vector(
            "SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2], 10)",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM articles ORDER BY vector_distance(embedding, ARRAY[0.1, 0.3, -0.2]) LIMIT 10"
        );
    }

    #[test]
    fn unicode_case_expansions_in_vector_expression_preserve_original_text() {
        let out = try_rewrite_search_using_vector(
            "SEARCH docs USING VECTOR(embedding, ARRAY['ß', 'ﬀ', 'İ'], 5)",
        )
        .expect("search vector form should rewrite");
        assert!(out.contains("ARRAY['ß', 'ﬀ', 'İ']"));
    }

    #[test]
    fn rewrites_two_arg_form_when_field_omitted() {
        let out =
            try_rewrite_search_using_vector("SEARCH articles USING VECTOR(ARRAY[0.1, 0.3], 5)")
                .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM articles ORDER BY vector_distance(ARRAY[0.1, 0.3]) LIMIT 5"
        );
    }

    #[test]
    fn returns_none_when_not_search() {
        assert!(try_rewrite_search_using_vector("SELECT * FROM t").is_none());
    }

    #[test]
    fn returns_none_when_using_fusion() {
        assert!(try_rewrite_search_using_vector("SEARCH c USING FUSION(ARRAY[0.5])").is_none());
    }

    #[test]
    fn rewrites_search_in_derived_table_position() {
        let out = try_rewrite_nested_search_using_vector(
            "SELECT * FROM (SEARCH docs USING VECTOR(emb, ARRAY[0.1], 2)) s WHERE s.id = 'a1'",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM (SELECT * FROM docs ORDER BY vector_distance(emb, ARRAY[0.1]) LIMIT 2) s WHERE s.id = 'a1'"
        );
    }

    #[test]
    fn a_bare_search_after_in_selects_the_compared_column() {
        let out = try_rewrite_nested_search_using_vector(
            "SELECT fact FROM facts WHERE agent_id = 'a1' AND id IN (SEARCH facts USING VECTOR(embedding, ARRAY[0.1], 3))",
        )
        .unwrap();
        assert!(
            out.ends_with("id IN (SELECT id FROM facts ORDER BY vector_distance(embedding, ARRAY[0.1]) LIMIT 3)"),
            "got: {out}"
        );
        let out = try_rewrite_nested_search_using_vector(
            "SELECT 1 FROM t WHERE t.k NOT IN (SEARCH f USING VECTOR(e, ARRAY[0.1], 1))",
        )
        .unwrap();
        assert!(out.contains("t.k NOT IN (SELECT k FROM f"), "got: {out}");
    }

    #[test]
    fn a_join_keeps_the_star_projection() {
        let out = try_rewrite_nested_search_using_vector(
            "SELECT 1 FROM a JOIN (SEARCH f USING VECTOR(e, ARRAY[0.1], 1)) b ON a.id = b.id",
        )
        .unwrap();
        assert!(out.contains("JOIN (SELECT * FROM f"), "got: {out}");
    }

    #[test]
    fn rewrites_every_nested_occurrence() {
        let out = try_rewrite_nested_search_using_vector(
            "SELECT a.id FROM (SEARCH docs USING VECTOR(emb, ARRAY[0.1], 2)) a \
             JOIN (SEARCH docs USING VECTOR(emb, ARRAY[0.9], 3)) b ON a.id = b.id",
        )
        .unwrap();
        assert_eq!(out.matches("SELECT * FROM docs ORDER BY").count(), 2);
        assert!(!out.to_uppercase().contains("SEARCH"));
    }

    #[test]
    fn nested_rewrite_ignores_search_inside_string_literal() {
        assert!(
            try_rewrite_nested_search_using_vector(
                "SELECT * FROM docs WHERE title = '(SEARCH docs USING VECTOR(emb, ARRAY[0.1], 2))'",
            )
            .is_none()
        );
    }

    #[test]
    fn rewrites_a_quoted_collection_name() {
        let out = try_rewrite_search_using_vector(
            "SEARCH \"MixedCase\" USING VECTOR(embedding, ARRAY[0.1, 0.3], 2)",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM \"MixedCase\" ORDER BY vector_distance(embedding, ARRAY[0.1, 0.3]) LIMIT 2",
            "the quotes must survive the rewrite, or a name that needs them stops resolving"
        );
    }

    #[test]
    fn rewrites_a_quoted_collection_name_in_subquery_position() {
        let out = try_rewrite_nested_search_using_vector(
            "SELECT id FROM (SEARCH \"MixedCase\" USING VECTOR(embedding, ARRAY[0.1], 2)) s",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT id FROM (SELECT * FROM \"MixedCase\" ORDER BY vector_distance(embedding, ARRAY[0.1]) LIMIT 2) s"
        );
    }

    #[test]
    fn quoted_collection_name_keeps_its_escaped_quotes() {
        let out = try_rewrite_search_using_vector(
            "SEARCH \"od\"\"d\" USING VECTOR(embedding, ARRAY[0.1], 1)",
        )
        .unwrap();
        assert!(
            out.starts_with("SELECT * FROM \"od\"\"d\" ORDER BY"),
            "got: {out}"
        );
    }

    #[test]
    fn rejects_an_unterminated_or_empty_quoted_collection_name() {
        assert!(
            try_rewrite_search_using_vector("SEARCH \"unterminated USING VECTOR(e, ARRAY[0.1], 1)")
                .is_none()
        );
        assert!(
            try_rewrite_search_using_vector("SEARCH \"\" USING VECTOR(e, ARRAY[0.1], 1)").is_none()
        );
    }

    #[test]
    fn nested_rewrite_survives_multibyte_text_after_an_open_paren() {
        // The clause scan runs at every `(`. Matching the keyword on bytes
        // keeps a multi-byte literal from slicing mid-character.
        assert!(
            try_rewrite_nested_search_using_vector(
                "INSERT INTO t (name) VALUES ('日本語') /* SEARCH */",
            )
            .is_none()
        );
        assert!(
            try_rewrite_nested_search_using_vector(
                "SELECT * FROM t WHERE tag IN ('🙂😀', 'SEARCH')"
            )
            .is_none()
        );
    }

    #[test]
    fn nested_rewrite_returns_none_for_plain_subquery() {
        assert!(
            try_rewrite_nested_search_using_vector("SELECT * FROM (SELECT * FROM docs) s")
                .is_none()
        );
    }

    #[test]
    fn nested_rewrite_returns_none_for_fusion_subquery() {
        assert!(
            try_rewrite_nested_search_using_vector(
                "SELECT * FROM (SEARCH docs USING FUSION(ARRAY[0.5])) s"
            )
            .is_none()
        );
    }

    #[test]
    fn top_level_rewrite_rejects_trailing_clause() {
        // Only the DSL form is a statement; trailing SQL means the caller wrote
        // something else and must get a parse error, not a silent rewrite.
        assert!(
            try_rewrite_search_using_vector(
                "SEARCH t USING VECTOR(emb, ARRAY[1.0], 3) WHERE x = 1"
            )
            .is_none()
        );
    }

    #[test]
    fn handles_trailing_semicolon() {
        let out =
            try_rewrite_search_using_vector("SEARCH t USING VECTOR(emb, ARRAY[1.0], 3);").unwrap();
        assert!(
            out.starts_with("SELECT * FROM t ORDER BY vector_distance(emb, ARRAY[1.0]) LIMIT 3")
        );
    }
}
