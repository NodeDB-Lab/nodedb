//! WHEN clause condition parser for trigger binary fast-reject.
//!
//! Parses simple WHEN predicates of the form `NEW.field OP value` or
//! `OLD.field OP value` into a `(WhenTarget, ScanFilter)` pair.
//!
//! The caller can then call `filter.matches_binary(raw_bytes)` on the raw
//! MessagePack row bytes to reject non-matching rows before full HashMap decode.
//!
//! Supported patterns:
//! - `NEW.field = 'value'`  / `NEW.field = 42`
//! - `NEW.field != 'value'` / `NEW.field <> 'value'`
//! - `NEW.field > 42`       / `NEW.field >= 42`
//! - `NEW.field < 42`       / `NEW.field <= 42`
//! - `NEW.field IS NULL`    / `NEW.field IS NOT NULL`
//! - Same with `OLD.` prefix

use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_types::Value;

/// Which row side the WHEN condition targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenTarget {
    New,
    Old,
}

/// Parse a WHEN condition that may contain `AND`-joined simple predicates.
///
/// Returns `None` if any part is too complex. All parts must target the same
/// side (NEW or OLD). For single predicates, returns a one-element Vec.
pub fn try_parse_when_to_filters(condition: &str) -> Option<(WhenTarget, Vec<ScanFilter>)> {
    // Split on " AND " (case-insensitive).
    let parts: Vec<&str> = split_and(condition);
    if parts.is_empty() {
        return None;
    }

    let mut target: Option<WhenTarget> = None;
    let mut filters = Vec::with_capacity(parts.len());

    for part in &parts {
        let (t, f) = try_parse_when_to_filter(part)?;
        if let Some(prev) = target
            && prev != t
        {
            return None; // Mixed NEW/OLD targets — not supported.
        }
        target = Some(t);
        filters.push(f);
    }

    Some((target?, filters))
}

/// Split a string on case-insensitive ` AND ` boundaries.
fn split_and(s: &str) -> Vec<&str> {
    let upper = s.to_uppercase();
    let mut parts = Vec::new();
    let mut start = 0;
    while let Some(pos) = upper[start..].find(" AND ") {
        parts.push(s[start..start + pos].trim());
        start += pos + 5; // " AND " is 5 chars
    }
    parts.push(s[start..].trim());
    parts.retain(|p| !p.is_empty());
    parts
}

/// Parse a simple WHEN condition string into a `(WhenTarget, ScanFilter)`.
///
/// Returns `None` if the condition is too complex to represent as a single
/// `ScanFilter` (compound expressions, subqueries, etc.) — the caller must
/// fall back to the full decode + substitute path.
pub fn try_parse_when_to_filter(condition: &str) -> Option<(WhenTarget, ScanFilter)> {
    let s = condition.trim();

    // Determine prefix: NEW. or OLD.
    let (target, rest) = if let Some(r) = strip_prefix_ci(s, "NEW.") {
        (WhenTarget::New, r)
    } else if let Some(r) = strip_prefix_ci(s, "OLD.") {
        (WhenTarget::Old, r)
    } else {
        return None;
    };

    // IS NULL / IS NOT NULL (checked before general operator split)
    let upper = rest.to_uppercase();
    if let Some(field) = upper.strip_suffix(" IS NOT NULL") {
        let field = field.trim();
        if is_simple_identifier(field) {
            return Some((
                target,
                ScanFilter {
                    field: rest[..field.len()].to_string(),
                    op: FilterOp::IsNotNull,
                    value: Value::Null,
                    clauses: vec![],
                    expr: None,
                },
            ));
        }
        return None;
    }
    if let Some(field) = upper.strip_suffix(" IS NULL") {
        let field = field.trim();
        if is_simple_identifier(field) {
            return Some((
                target,
                ScanFilter {
                    field: rest[..field.len()].to_string(),
                    op: FilterOp::IsNull,
                    value: Value::Null,
                    clauses: vec![],
                    expr: None,
                },
            ));
        }
        return None;
    }

    // General: FIELD OP VALUE
    // Split on first occurrence of a multi-char operator, then single-char.
    let ops: &[(&str, FilterOp)] = &[
        ("!=", FilterOp::Ne),
        ("<>", FilterOp::Ne),
        (">=", FilterOp::Gte),
        ("<=", FilterOp::Lte),
        (">", FilterOp::Gt),
        ("<", FilterOp::Lt),
        ("=", FilterOp::Eq),
    ];

    for (op_str, op) in ops {
        // Find op_str in rest (case-sensitive; SQL operators are ASCII symbols)
        if let Some(pos) = rest.find(op_str) {
            let field = rest[..pos].trim();
            let value_str = rest[pos + op_str.len()..].trim();
            if !is_simple_identifier(field) {
                return None;
            }
            let value = parse_value(value_str)?;
            return Some((
                target,
                ScanFilter {
                    field: field.to_string(),
                    op: *op,
                    value,
                    clauses: vec![],
                    expr: None,
                },
            ));
        }
    }

    None
}

/// Strip a case-insensitive prefix and return the remainder.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() < prefix.len() {
        return None;
    }
    if s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Return true if `s` is a simple SQL identifier (letters, digits, underscores).
fn is_simple_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Parse a SQL literal value into a `nodedb_types::Value`.
///
/// Handles:
/// - Single-quoted strings: `'hello'`  → `Value::String`
/// - Integer literals: `42`, `-7`      → `Value::Integer`
/// - Float literals: `3.14`, `-0.5`    → `Value::Float`
/// - NULL (case-insensitive)           → `Value::Null`
/// - Boolean: TRUE / FALSE             → `Value::Bool`
fn parse_value(s: &str) -> Option<Value> {
    let upper = s.to_uppercase();

    // NULL
    if upper == "NULL" {
        return Some(Value::Null);
    }

    // Boolean
    if upper == "TRUE" {
        return Some(Value::Bool(true));
    }
    if upper == "FALSE" {
        return Some(Value::Bool(false));
    }

    // Quoted string: 'text' or "text"
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        let inner = &s[1..s.len() - 1];
        return Some(Value::String(inner.to_string()));
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Some(Value::Integer(n));
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Some(Value::Float(f));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Option<(WhenTarget, ScanFilter)> {
        try_parse_when_to_filter(s)
    }

    // ── NEW.field = 'value' ─────────────────────────────────────────────

    #[test]
    fn eq_string_single_quote() {
        let (target, f) = parse("NEW.status = 'active'").unwrap();
        assert_eq!(target, WhenTarget::New);
        assert_eq!(f.field, "status");
        assert_eq!(f.op, FilterOp::Eq);
        assert_eq!(f.value, Value::String("active".into()));
    }

    #[test]
    fn eq_integer() {
        let (target, f) = parse("NEW.count = 42").unwrap();
        assert_eq!(target, WhenTarget::New);
        assert_eq!(f.field, "count");
        assert_eq!(f.op, FilterOp::Eq);
        assert_eq!(f.value, Value::Integer(42));
    }

    #[test]
    fn eq_float() {
        let (_, f) = parse("NEW.score = 9.81").unwrap();
        assert_eq!(f.op, FilterOp::Eq);
        assert_eq!(f.value, Value::Float(9.81));
    }

    #[test]
    fn ne_double_excl() {
        let (_, f) = parse("NEW.status != 'deleted'").unwrap();
        assert_eq!(f.op, FilterOp::Ne);
        assert_eq!(f.value, Value::String("deleted".into()));
    }

    #[test]
    fn ne_angle() {
        let (_, f) = parse("NEW.status <> 'draft'").unwrap();
        assert_eq!(f.op, FilterOp::Ne);
    }

    #[test]
    fn gt() {
        let (_, f) = parse("NEW.age > 18").unwrap();
        assert_eq!(f.op, FilterOp::Gt);
        assert_eq!(f.value, Value::Integer(18));
    }

    #[test]
    fn gte() {
        let (_, f) = parse("NEW.age >= 18").unwrap();
        assert_eq!(f.op, FilterOp::Gte);
    }

    #[test]
    fn lt() {
        let (_, f) = parse("NEW.price < 100").unwrap();
        assert_eq!(f.op, FilterOp::Lt);
    }

    #[test]
    fn lte() {
        let (_, f) = parse("NEW.price <= 99").unwrap();
        assert_eq!(f.op, FilterOp::Lte);
    }

    #[test]
    fn is_null() {
        let (target, f) = parse("NEW.deleted_at IS NULL").unwrap();
        assert_eq!(target, WhenTarget::New);
        assert_eq!(f.field, "deleted_at");
        assert_eq!(f.op, FilterOp::IsNull);
    }

    #[test]
    fn is_not_null() {
        let (_, f) = parse("NEW.email IS NOT NULL").unwrap();
        assert_eq!(f.op, FilterOp::IsNotNull);
        assert_eq!(f.field, "email");
    }

    // ── OLD.field ──────────────────────────────────────────────────────

    #[test]
    fn old_target() {
        let (target, f) = parse("OLD.status = 'pending'").unwrap();
        assert_eq!(target, WhenTarget::Old);
        assert_eq!(f.field, "status");
    }

    // ── Case-insensitive prefix ─────────────────────────────────────────

    #[test]
    fn prefix_case_insensitive() {
        let (target, _) = parse("new.status = 'x'").unwrap();
        assert_eq!(target, WhenTarget::New);
        let (target2, _) = parse("OLD.status = 'x'").unwrap();
        assert_eq!(target2, WhenTarget::Old);
    }

    // ── IS NULL/NOT NULL case-insensitive ───────────────────────────────

    #[test]
    fn is_null_lowercase() {
        let (_, f) = parse("NEW.x is null").unwrap();
        assert_eq!(f.op, FilterOp::IsNull);
    }

    #[test]
    fn is_not_null_lowercase() {
        let (_, f) = parse("NEW.x is not null").unwrap();
        assert_eq!(f.op, FilterOp::IsNotNull);
    }

    // ── Boolean literals ────────────────────────────────────────────────

    #[test]
    fn bool_true() {
        let (_, f) = parse("NEW.active = TRUE").unwrap();
        assert_eq!(f.value, Value::Bool(true));
    }

    #[test]
    fn bool_false() {
        let (_, f) = parse("NEW.active = FALSE").unwrap();
        assert_eq!(f.value, Value::Bool(false));
    }

    // ── Null literal ────────────────────────────────────────────────────

    #[test]
    fn null_literal_eq() {
        let (_, f) = parse("NEW.x = NULL").unwrap();
        assert_eq!(f.value, Value::Null);
        assert_eq!(f.op, FilterOp::Eq);
    }

    // ── Whitespace tolerance ────────────────────────────────────────────

    #[test]
    fn whitespace_trimmed() {
        let (_, f) = parse("  NEW.status  =  'active'  ").unwrap();
        assert_eq!(f.field, "status");
        assert_eq!(f.value, Value::String("active".into()));
    }

    // ── Negative numbers ───────────────────────────────────────────────

    #[test]
    fn negative_integer() {
        let (_, f) = parse("NEW.delta = -5").unwrap();
        assert_eq!(f.value, Value::Integer(-5));
    }

    // ── Unsupported / complex conditions return None ────────────────────

    #[test]
    fn complex_condition_returns_none() {
        assert!(parse("NEW.a = 1 AND NEW.b = 2").is_none());
        assert!(parse("1 = 1").is_none());
        assert!(parse("NEW.func(x) = 1").is_none());
    }

    #[test]
    fn bare_true_false_returns_none() {
        // These are handled by try_eval_simple_condition, not this parser.
        assert!(parse("TRUE").is_none());
        assert!(parse("FALSE").is_none());
    }

    #[test]
    fn missing_value_returns_none() {
        // No RHS at all
        assert!(parse("NEW.x = ").is_none());
    }

    // ── AND-joined conditions ─────────────────────────────────────────

    fn parse_multi(s: &str) -> Option<(WhenTarget, Vec<ScanFilter>)> {
        try_parse_when_to_filters(s)
    }

    #[test]
    fn and_two_conditions() {
        let (target, filters) =
            parse_multi("NEW.status = 'active' AND NEW.type = 'order'").unwrap();
        assert_eq!(target, WhenTarget::New);
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].field, "status");
        assert_eq!(filters[0].value, Value::String("active".into()));
        assert_eq!(filters[1].field, "type");
        assert_eq!(filters[1].value, Value::String("order".into()));
    }

    #[test]
    fn and_three_conditions() {
        let (_, filters) = parse_multi("NEW.a > 1 AND NEW.b < 10 AND NEW.c = 'x'").unwrap();
        assert_eq!(filters.len(), 3);
        assert_eq!(filters[0].op, FilterOp::Gt);
        assert_eq!(filters[1].op, FilterOp::Lt);
        assert_eq!(filters[2].op, FilterOp::Eq);
    }

    #[test]
    fn and_mixed_targets_returns_none() {
        assert!(parse_multi("NEW.a = 1 AND OLD.b = 2").is_none());
    }

    #[test]
    fn and_with_complex_part_returns_none() {
        assert!(parse_multi("NEW.a = 1 AND 1 = 1").is_none());
    }

    #[test]
    fn single_condition_through_multi_parser() {
        let (target, filters) = parse_multi("NEW.x = 42").unwrap();
        assert_eq!(target, WhenTarget::New);
        assert_eq!(filters.len(), 1);
    }
}
