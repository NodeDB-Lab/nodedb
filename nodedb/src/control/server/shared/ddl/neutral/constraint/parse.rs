// SPDX-License-Identifier: BUSL-1.1

//! Parsing helpers for constraint DDL: transition rules, predicates, expressions.
//!
//! Ported verbatim from the pgwire `ddl::constraint::parse`; only the error type
//! changed from pgwire `PgWireError` to the protocol-neutral [`DdlError`].

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};

use crate::control::security::catalog::types::TransitionRule;
use crate::control::server::shared::ddl::result::DdlError;

use super::support::err;

/// Parse transition rules from `TRANSITIONS ('a' -> 'b', 'b' -> 'c' BY ROLE 'role', ...)`.
pub(super) fn parse_transitions(upper: &str) -> Result<Vec<TransitionRule>, DdlError> {
    let start = upper
        .find("TRANSITIONS")
        .ok_or_else(|| err("42601", "missing TRANSITIONS keyword"))?;
    let after = &upper[start + "TRANSITIONS".len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| err("42601", "TRANSITIONS requires (...)"))?;
    let paren_end = after
        .rfind(')')
        .ok_or_else(|| err("42601", "missing closing ')' in TRANSITIONS"))?;
    let inner = &after[paren_start + 1..paren_end];

    let mut rules = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let rule = parse_single_transition(part)?;
        rules.push(rule);
    }

    if rules.is_empty() {
        return Err(err("42601", "TRANSITIONS must define at least one rule"));
    }

    Ok(rules)
}

/// Parse `'from' -> 'to'` or `'from' -> 'to' BY ROLE 'role'`.
fn parse_single_transition(s: &str) -> Result<TransitionRule, DdlError> {
    let (from_part, rest) = if let Some(pos) = s.find("->") {
        (&s[..pos], &s[pos + 2..])
    } else if let Some(pos) = s.find("→") {
        (&s[..pos], &s[pos + "→".len()..])
    } else {
        return Err(err(
            "42601",
            &format!("transition rule must contain '->' or '→': '{s}'"),
        ));
    };

    let from = from_part
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    let (to, required_role) = if let Some(by_pos) = rest.find("BY ROLE") {
        let to = rest[..by_pos]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        let role = rest[by_pos + "BY ROLE".len()..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        (to, Some(role))
    } else {
        let to = rest.trim().trim_matches('\'').trim_matches('"').to_string();
        (to, None)
    };

    if from.is_empty() || to.is_empty() {
        return Err(err("42601", "transition rule: from and to values required"));
    }

    Ok(TransitionRule {
        from,
        to,
        required_role,
    })
}

/// Extract the parenthesized predicate/expression after the CHECK keyword.
///
/// Used by both TRANSITION CHECK and general CHECK constraints.
/// Finds `CHECK`, skips to `(`, extracts balanced content.
pub(super) fn extract_parenthesized_predicate(sql: &str) -> Result<String, DdlError> {
    extract_check_body(sql, "CHECK")
}

/// Extract balanced parenthesized content after a keyword.
///
/// Finds `keyword` in `sql`, then extracts the content between the next `(` and
/// its matching `)`, respecting nesting.
pub(super) fn extract_check_body(sql: &str, keyword: &str) -> Result<String, DdlError> {
    let kw_pos = find_ascii_case_insensitive(sql, keyword)
        .ok_or_else(|| err("42601", &format!("missing {keyword} keyword")))?;
    let after = &sql[kw_pos + keyword.len()..];

    let paren_start = after
        .find('(')
        .ok_or_else(|| err("42601", &format!("{keyword} requires (expression)")))?;

    let body = &after[paren_start + 1..];
    let mut depth = 1i32;
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
            &format!("unmatched parentheses in {keyword} expression"),
        ));
    }

    let expr = body[..end].trim().to_string();
    if expr.is_empty() {
        return Err(err(
            "42601",
            &format!("{keyword} expression cannot be empty"),
        ));
    }

    Ok(expr)
}

/// Parse a transition check predicate string into a SqlExpr.
pub(super) fn parse_transition_predicate(
    s: &str,
) -> Result<crate::bridge::expr_eval::SqlExpr, DdlError> {
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    let s = s.trim();
    if s.is_empty() {
        return Err(err("42601", "empty predicate"));
    }

    if let Some((left, right)) = split_top_level(s, " OR ") {
        let l = parse_transition_predicate(left)?;
        let r = parse_transition_predicate(right)?;
        return Ok(SqlExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::Or,
            right: Box::new(r),
        });
    }

    if let Some((left, right)) = split_top_level(s, " AND ") {
        let l = parse_transition_predicate(left)?;
        let r = parse_transition_predicate(right)?;
        return Ok(SqlExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::And,
            right: Box::new(r),
        });
    }

    if s.starts_with('(') && s.ends_with(')') {
        let inner = &s[1..s.len() - 1];
        let mut depth = 0i32;
        let mut all_inner = true;
        for (_, ch) in inner.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        all_inner = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if all_inner && depth == 0 {
            return parse_transition_predicate(inner);
        }
    }

    parse_simple_comparison(s)
}

/// Parse a simple comparison like `OLD.sealed = FALSE` or `NEW.amount >= OLD.amount`.
fn parse_simple_comparison(s: &str) -> Result<crate::bridge::expr_eval::SqlExpr, DdlError> {
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    for (op_str, op) in &[
        ("!=", BinaryOp::NotEq),
        ("<>", BinaryOp::NotEq),
        (">=", BinaryOp::GtEq),
        ("<=", BinaryOp::LtEq),
        ("=", BinaryOp::Eq),
        (">", BinaryOp::Gt),
        ("<", BinaryOp::Lt),
    ] {
        if let Some((left, right)) = split_on_operator(s, op_str) {
            let l = parse_value_ref(left.trim())?;
            let r = parse_value_ref(right.trim())?;
            return Ok(SqlExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            });
        }
    }

    Err(err(
        "42601",
        &format!("cannot parse transition predicate term: '{s}'"),
    ))
}

/// Parse a value reference: `OLD.column`, `NEW.column`, `column`, literal.
fn parse_value_ref(s: &str) -> Result<crate::bridge::expr_eval::SqlExpr, DdlError> {
    use crate::bridge::expr_eval::SqlExpr;

    let upper = s.to_uppercase();
    if let Some(col) = upper.strip_prefix("OLD.") {
        return Ok(SqlExpr::OldColumn(col.to_lowercase()));
    }
    if let Some(col) = upper.strip_prefix("NEW.") {
        return Ok(SqlExpr::Column(col.to_lowercase()));
    }

    if upper == "TRUE" {
        return Ok(SqlExpr::Literal(nodedb_types::Value::Bool(true)));
    }
    if upper == "FALSE" {
        return Ok(SqlExpr::Literal(nodedb_types::Value::Bool(false)));
    }
    if upper == "NULL" {
        return Ok(SqlExpr::Literal(nodedb_types::Value::Null));
    }
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        let inner = &s[1..s.len() - 1];
        return Ok(SqlExpr::Literal(nodedb_types::Value::String(
            inner.to_string(),
        )));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(SqlExpr::Literal(nodedb_types::Value::Integer(i)));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(SqlExpr::Literal(nodedb_types::Value::Float(f)));
    }

    Ok(SqlExpr::Column(s.to_lowercase()))
}

/// Split a string at the top-level occurrence of `sep` (respecting parentheses).
fn split_top_level<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0i32;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && find_ascii_case_insensitive_from(s, sep, i) == Some(i) {
            return Some((&s[..i], &s[i + sep.len()..]));
        }
    }
    None
}

/// Split on comparison operator, avoiding >= <= != <>.
fn split_on_operator<'a>(s: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut start = 0;
    while let Some(pos) = s[start..].find(op) {
        let abs_pos = start + pos;
        if op == "=" && abs_pos > 0 {
            let prev = s.as_bytes()[abs_pos - 1];
            if prev == b'>' || prev == b'<' || prev == b'!' {
                start = abs_pos + op.len();
                continue;
            }
        }
        if op == ">" && abs_pos + 1 < s.len() && s.as_bytes()[abs_pos + 1] == b'=' {
            start = abs_pos + 2;
            continue;
        }
        if op == "<" && abs_pos + 1 < s.len() {
            let next = s.as_bytes()[abs_pos + 1];
            if next == b'=' || next == b'>' {
                start = abs_pos + 2;
                continue;
            }
        }
        return Some((&s[..abs_pos], &s[abs_pos + op.len()..]));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    #[test]
    fn logical_separator_after_expanding_unicode_preserves_original_offsets() {
        let expr = parse_transition_predicate("OLD.ﬀﬀ = TRUE AND OLD.x = TRUE")
            .expect("transition predicate should parse");
        assert!(matches!(
            expr,
            SqlExpr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn non_ascii_predicate_does_not_index_inside_utf8() {
        parse_transition_predicate("OLD.é = TRUE")
            .expect("non-ASCII transition predicate should parse");
    }
}
