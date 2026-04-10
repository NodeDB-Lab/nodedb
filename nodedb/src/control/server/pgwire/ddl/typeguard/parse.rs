//! Parsing helpers for TYPEGUARD DDL field definitions.

use nodedb_types::TypeGuardFieldDef;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Extract collection name from `... TYPEGUARD [IF EXISTS] ON <collection> ...`.
pub(super) fn extract_collection_name(sql: &str) -> PgWireResult<String> {
    let upper = sql.to_uppercase();
    let on_pos = upper
        .find(" ON ")
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
pub(super) fn extract_outer_parens(sql: &str) -> PgWireResult<String> {
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
pub(super) fn parse_field_list(list: &str) -> PgWireResult<Vec<TypeGuardFieldDef>> {
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
pub(super) fn parse_single_field(s: &str) -> PgWireResult<TypeGuardFieldDef> {
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

    let upper_rest = rest.to_uppercase();

    // Detect REQUIRED keyword (must be standalone token, not part of type expr).
    let required = upper_rest.split_whitespace().any(|t| t == "REQUIRED");

    // Extract CHECK expression if present.
    // Find CHECK in the original-case `rest` to preserve case in the expression,
    // using case-insensitive search to avoid byte-offset mismatch with `upper_rest`.
    let check_expr = if let Some(check_pos) = find_word_boundary(&upper_rest, "CHECK") {
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
        let mut end = upper_rest.len();
        for kw in &["REQUIRED", "CHECK", "DEFAULT", "VALUE"] {
            if let Some(pos) = find_word_boundary(&upper_rest, kw) {
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

    // Extract DEFAULT expression if present.
    let default_expr = if let Some(def_pos) = find_word_boundary(&upper_rest, "DEFAULT") {
        let after_default = rest[def_pos + 7..].trim_start();
        // DEFAULT value extends until the next keyword (REQUIRED, CHECK, VALUE) or end.
        let default_end = find_next_keyword(after_default);
        Some(after_default[..default_end].trim().to_string())
    } else {
        None
    };

    // Extract VALUE expression if present.
    let value_expr = if let Some(val_pos) = find_word_boundary(&upper_rest, "VALUE") {
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
    while let Some(pos) = haystack[start..].find(word) {
        let abs_pos = start + pos;
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
    let upper = s.to_uppercase();
    let mut end = s.len();
    for kw in &["REQUIRED", "CHECK", "DEFAULT", "VALUE"] {
        if let Some(pos) = find_word_boundary(&upper, kw) {
            end = end.min(pos);
        }
    }
    end
}

pub(super) fn err(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}
