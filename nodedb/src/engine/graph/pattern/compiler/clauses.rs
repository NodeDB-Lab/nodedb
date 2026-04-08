//! WHERE, RETURN, and ORDER BY clause parsers.

use super::super::ast::*;
use super::helpers::{split_by_and, split_top_level_commas};
use super::parse_match_clauses;

/// Parse WHERE predicates.
pub(super) fn parse_where(text: &str) -> Result<Vec<WherePredicate>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut predicates = Vec::new();
    let parts = split_by_and(trimmed);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let upper_part = part.to_uppercase();

        // NOT EXISTS { MATCH ... }
        if upper_part.starts_with("NOT EXISTS") {
            let brace_start = part.find('{').ok_or("NOT EXISTS requires { MATCH ... }")?;
            let brace_end = part.rfind('}').ok_or("unclosed '{' in NOT EXISTS")?;
            let inner = part[brace_start + 1..brace_end].trim();
            let inner_clauses = parse_match_clauses(inner)?;
            if let Some(clause) = inner_clauses.into_iter().next() {
                predicates.push(WherePredicate::NotExists {
                    sub_pattern: clause,
                });
            }
            continue;
        }

        // Simple comparison: `a.name = 'Alice'` or `a.age > 25`.
        if let Some(pred) = parse_comparison_predicate(part) {
            predicates.push(pred);
        }
    }

    Ok(predicates)
}

/// Parse a comparison predicate: `a.name = 'Alice'`, `a.age > 25`.
fn parse_comparison_predicate(text: &str) -> Option<WherePredicate> {
    for (op_str, op) in &[
        ("<=", ComparisonOp::Lte),
        (">=", ComparisonOp::Gte),
        ("<>", ComparisonOp::Neq),
        ("!=", ComparisonOp::Neq),
        ("<", ComparisonOp::Lt),
        (">", ComparisonOp::Gt),
        ("=", ComparisonOp::Eq),
    ] {
        if let Some(pos) = text.find(op_str) {
            let lhs = text[..pos].trim();
            let rhs = text[pos + op_str.len()..].trim();

            let (binding, field) = if let Some(dot) = lhs.find('.') {
                (
                    lhs[..dot].trim().to_string(),
                    lhs[dot + 1..].trim().to_string(),
                )
            } else {
                (lhs.to_string(), String::new())
            };

            let value = rhs.trim_matches('\'').trim_matches('"').to_string();

            if *op == ComparisonOp::Eq {
                return Some(WherePredicate::Equals {
                    binding,
                    field,
                    value,
                });
            } else {
                return Some(WherePredicate::Comparison {
                    binding,
                    field,
                    op: *op,
                    value,
                });
            }
        }
    }
    None
}

/// Parse RETURN columns.
pub(super) fn parse_return(text: &str) -> (Vec<ReturnColumn>, bool) {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return (Vec::new(), false);
    }

    // Strip DISTINCT keyword.
    let (effective, distinct) = {
        let upper = trimmed.to_uppercase();
        if upper.starts_with("DISTINCT ") {
            (&trimmed[9..], true)
        } else {
            (trimmed, false)
        }
    };

    let columns = split_top_level_commas(effective)
        .into_iter()
        .map(|col| {
            let col = col.trim();
            let upper = col.to_uppercase();
            if let Some(as_pos) = upper.rfind(" AS ") {
                let expr = col[..as_pos].trim().to_string();
                let alias = col[as_pos + 4..].trim().to_string();
                ReturnColumn {
                    expr,
                    alias: Some(alias),
                }
            } else {
                ReturnColumn {
                    expr: col.to_string(),
                    alias: None,
                }
            }
        })
        .collect();
    (columns, distinct)
}

/// Parse ORDER BY columns.
pub(super) fn parse_order_by(text: &str) -> Vec<OrderByColumn> {
    split_top_level_commas(text)
        .into_iter()
        .map(|col| {
            let col = col.trim();
            let upper = col.to_uppercase();
            let ascending = !upper.ends_with(" DESC");
            let expr = col
                .trim_end_matches(|c: char| c.is_ascii_whitespace())
                .strip_suffix("DESC")
                .or_else(|| col.strip_suffix("ASC"))
                .unwrap_or(col)
                .trim()
                .to_string();
            OrderByColumn { expr, ascending }
        })
        .collect()
}
