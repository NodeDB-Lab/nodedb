// SPDX-License-Identifier: BUSL-1.1

//! Parsing helpers for TYPEGUARD DDL field definitions.
//!
//! Ported verbatim from the pgwire `ddl::typeguard::parse` helpers; only the
//! error construction changed from pgwire `PgWireError` to the protocol-neutral
//! [`DdlError`].

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};
use nodedb_sql::parser::type_expr::parse_type_expr;
use nodedb_types::TypeGuardFieldDef;

use super::super::super::result::DdlError;
use super::injected_expr::validate_injected_expr;

/// Extract collection name from `... TYPEGUARD [IF EXISTS] ON <collection> ...`.
pub(super) fn extract_collection_name(sql: &str) -> Result<String, DdlError> {
    let on_pos = find_ascii_case_insensitive(sql, " ON ")
        .ok_or_else(|| err("42601", "TYPEGUARD requires ON <collection>"))?;
    let after_on = sql[on_pos + 4..].trim_start();
    // Collection name ends at whitespace or '('
    let end = after_on
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(after_on.len());
    let name = after_on[..end].trim().to_lowercase();
    if name.is_empty() {
        return Err(err("42601", "missing collection name after ON"));
    }
    Ok(name)
}

/// Extract the content between the outermost `(` and matching `)` in `sql`.
pub(super) fn extract_outer_parens(sql: &str) -> Result<String, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| err("42601", "TYPEGUARD requires ( ... ) field list"))?;
    let body = &sql[start + 1..];
    let mut depth = 1usize;
    let mut end = 0;
    for (i, ch) in body.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(err(
            "42601",
            "unmatched parentheses in TYPEGUARD field list",
        ));
    }
    Ok(body[..end].trim().to_string())
}

/// Parse a comma-separated list of field definitions.
///
/// Each definition: `field_name type_expr [REQUIRED] [CHECK (expr)]`
pub(super) fn parse_field_list(list: &str) -> Result<Vec<TypeGuardFieldDef>, DdlError> {
    let mut guards = Vec::new();
    // Split on commas that are not inside parentheses.
    let mut depth = 0i32;
    let mut start = 0;
    let mut segments: Vec<&str> = Vec::new();
    for (i, ch) in list.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                segments.push(&list[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    segments.push(&list[start..]);

    for seg in segments {
        let seg = seg.trim();
        if seg.is_empty() {
            continue;
        }
        guards.push(parse_single_field(seg)?);
    }
    Ok(guards)
}

/// Parse one field definition: `field_name type_expr [REQUIRED] [CHECK (expr)]`.
pub(super) fn parse_single_field(s: &str) -> Result<TypeGuardFieldDef, DdlError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(err("42601", "empty field definition"));
    }

    // Field name: first token (may contain dots for nested paths).
    let mut tokens = s.splitn(2, |c: char| c.is_whitespace());
    let field = tokens
        .next()
        .ok_or_else(|| err("42601", "missing field name"))?
        .to_lowercase();
    let rest = tokens.next().unwrap_or("").trim();

    if rest.is_empty() {
        return Err(err(
            "42601",
            &format!("field '{field}': missing type expression"),
        ));
    }

    // Detect REQUIRED keyword (must be standalone token, not part of type expr).
    let required = rest
        .split_whitespace()
        .any(|token| token.eq_ignore_ascii_case("REQUIRED"));

    // Extract CHECK expression if present.
    // Find CHECK in the original-case `rest` to preserve case in the expression,
    // using case-insensitive search to avoid byte-offset mismatch with `upper_rest`.
    let check_expr = if let Some(check_pos) = find_word_boundary(rest, "CHECK") {
        let after_check = &rest[check_pos + 5..];
        let paren_start = after_check
            .find('(')
            .ok_or_else(|| err("42601", &format!("field '{field}': CHECK requires (expr)")))?;
        let body = &after_check[paren_start + 1..];
        let mut depth = 1usize;
        let mut end = 0;
        for (i, ch) in body.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return Err(err(
                "42601",
                &format!("field '{field}': unmatched parentheses in CHECK"),
            ));
        }
        Some(body[..end].trim().to_string())
    } else {
        None
    };

    // The type expression is everything before REQUIRED, CHECK, DEFAULT, VALUE.
    let type_end = {
        let mut end = rest.len();
        for kw in &["REQUIRED", "CHECK", "DEFAULT", "VALUE"] {
            if let Some(pos) = find_word_boundary(rest, kw) {
                end = end.min(pos);
            }
        }
        end
    };

    let type_expr = rest[..type_end].trim().to_uppercase();

    if type_expr.is_empty() {
        return Err(err(
            "42601",
            &format!("field '{field}': missing type expression"),
        ));
    }

    // The declaration must name a type the engine resolves. `parse_type_expr`
    // is the parser that enforces the guard on every write, so a spelling it
    // rejects fails here instead of failing each later write.
    parse_type_expr(&type_expr).map_err(|e| err("42601", &format!("field '{field}': {e}")))?;

    // Extract DEFAULT expression if present.
    let default_expr = if let Some(def_pos) = find_word_boundary(rest, "DEFAULT") {
        let after_default = rest[def_pos + 7..].trim_start();
        // DEFAULT value extends until the next keyword (REQUIRED, CHECK, VALUE) or end.
        let default_end = find_next_keyword(after_default);
        Some(after_default[..default_end].trim().to_string())
    } else {
        None
    };

    // Extract VALUE expression if present.
    let value_expr = if let Some(val_pos) = find_word_boundary(rest, "VALUE") {
        let after_value = rest[val_pos + 5..].trim_start();
        let value_end = find_next_keyword(after_value);
        Some(after_value[..value_end].trim().to_string())
    } else {
        None
    };

    // DEFAULT and VALUE are mutually exclusive.
    if default_expr.is_some() && value_expr.is_some() {
        return Err(err(
            "42601",
            &format!("field '{field}': DEFAULT and VALUE are mutually exclusive"),
        ));
    }

    // Both clauses produce a field value on every write, so a declaration the
    // write path cannot evaluate is refused here.
    if let Some(expr) = default_expr.as_deref() {
        validate_injected_expr(&field, "DEFAULT", expr)?;
    }
    if let Some(expr) = value_expr.as_deref() {
        validate_injected_expr(&field, "VALUE", expr)?;
    }

    Ok(TypeGuardFieldDef {
        field,
        type_expr,
        required,
        check_expr,
        default_expr,
        value_expr,
    })
}

/// Find the byte position of `word` as a standalone token (preceded by whitespace or start).
fn find_word_boundary(haystack: &str, word: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(abs_pos) = find_ascii_case_insensitive_from(haystack, word, start) {
        let before_ok = abs_pos == 0
            || haystack
                .as_bytes()
                .get(abs_pos - 1)
                .is_some_and(|&b| b == b' ' || b == b'\t');
        let after_ok = abs_pos + word.len() >= haystack.len()
            || haystack
                .as_bytes()
                .get(abs_pos + word.len())
                .is_some_and(|&b| b == b' ' || b == b'\t' || b == b'(');
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        start = abs_pos + word.len();
    }
    None
}

/// Find the end of a DEFAULT/VALUE expression — stops at the next keyword
/// (REQUIRED, CHECK, DEFAULT, VALUE) or end of string.
fn find_next_keyword(s: &str) -> usize {
    let mut end = s.len();
    for kw in &["REQUIRED", "CHECK", "DEFAULT", "VALUE"] {
        if let Some(pos) = find_word_boundary(s, kw) {
            end = end.min(pos);
        }
    }
    end
}

/// Build a protocol-neutral [`DdlError`] with the given SQLSTATE + message.
pub(super) fn err(code: &str, msg: &str) -> DdlError {
    DdlError::new(code, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_after_unicode_text_preserves_original_offsets() {
        let name = extract_collection_name("CREATE TYPEGUARD rpﬀﬀ ON metrics (v INT)")
            .expect("collection name should parse");
        assert_eq!(name, "metrics");
    }

    /// A declared type the engine cannot resolve stops the declaration.
    ///
    /// The guard would otherwise be stored and fail every later write.
    /// `TIMESTAMP GARBAGE` covers the trailing-word case, which resolving the
    /// leading token alone would read as `TIMESTAMP`.
    #[test]
    fn unresolvable_field_type_is_refused_at_declaration() {
        for declaration in ["gadget WIDGET", "at TIMESTAMP GARBAGE"] {
            let error = parse_single_field(declaration)
                .expect_err("an unresolvable declared type must be refused");
            assert_eq!(error.sqlstate, "42601", "on {declaration}");
        }
    }

    /// Every spelling the engine resolves stays accepted at declaration.
    #[test]
    fn every_resolvable_field_type_is_accepted_at_declaration() {
        for declaration in [
            "a INT2",
            "b INT4",
            "c INT8",
            "d SMALLINT",
            "e FLOAT4",
            "f FLOAT8",
            "g FLOAT32",
            "h DOUBLE PRECISION",
            "i JSONB",
            "j SYSTEM_TIMESTAMP",
            "k VARCHAR(255)",
            "l DECIMAL(10, 2)",
            "m TIMESTAMP WITH TIME ZONE",
            "n TIMESTAMP WITHOUT TIME ZONE",
            "o OBJECT",
            "p ARRAY<STRING>",
            "q STRING|NULL",
            "r VECTOR(384)",
            "s STRING REQUIRED CHECK (s <> '')",
        ] {
            parse_single_field(declaration)
                .unwrap_or_else(|e| panic!("{declaration} must parse: {}", e.message));
        }
    }

    #[test]
    fn field_keywords_after_unicode_default_preserve_original_offsets() {
        let field = parse_single_field("label STRING DEFAULT 'ﬀﬀ' CHECK (label <> '') REQUIRED")
            .expect("typeguard field should parse");
        assert_eq!(field.type_expr, "STRING");
        assert_eq!(field.default_expr.as_deref(), Some("'ﬀﬀ'"));
        assert_eq!(field.check_expr.as_deref(), Some("label <> ''"));
        assert!(field.required);
    }
}
