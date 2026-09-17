// SPDX-License-Identifier: Apache-2.0

use nodedb_types::find_ascii_case_insensitive;

use super::{FilterOp, ScanFilter};

/// Parse simple SQL predicates into `ScanFilter` values.
///
/// Handles basic `field op value` predicates joined by AND.
/// Supports: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `LIKE`, `ILIKE`.
/// Values: single-quoted strings, numbers, `TRUE`/`FALSE`, `NULL`.
///
/// For complex predicates (OR, subqueries, functions), returns empty vec
/// (match all — facet counts will be unfiltered).
pub fn parse_simple_predicates(text: &str) -> Vec<ScanFilter> {
    let mut filters = Vec::new();
    for clause in text.split(" AND ").flat_map(|s| s.split(" and ")) {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        if let Some(f) = parse_single_predicate(clause) {
            filters.push(f);
        }
    }
    filters
}

fn parse_single_predicate(clause: &str) -> Option<ScanFilter> {
    let ops = &[">=", "<=", "!=", "<>", "=", ">", "<"];
    for op_str in ops {
        if let Some(pos) = clause.find(op_str) {
            let field = clause[..pos].trim().to_string();
            let raw_value = clause[pos + op_str.len()..].trim();
            let op = match *op_str {
                "=" => FilterOp::Eq,
                "!=" | "<>" => FilterOp::Ne,
                ">" => FilterOp::Gt,
                ">=" => FilterOp::Gte,
                "<" => FilterOp::Lt,
                "<=" => FilterOp::Lte,
                _ => return None,
            };
            return Some(ScanFilter {
                field,
                op,
                value: nodedb_types::Value::from(parse_predicate_value(raw_value)),
                clauses: Vec::new(),
                expr: None,
            });
        }
    }

    if let Some(pos) = find_ascii_case_insensitive(clause, " LIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 6..].trim();
        return Some(ScanFilter {
            field,
            op: FilterOp::Like,
            value: nodedb_types::Value::from(parse_predicate_value(raw_value)),
            clauses: Vec::new(),
            expr: None,
        });
    }
    if let Some(pos) = find_ascii_case_insensitive(clause, " ILIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 7..].trim();
        return Some(ScanFilter {
            field,
            op: FilterOp::Ilike,
            value: nodedb_types::Value::from(parse_predicate_value(raw_value)),
            clauses: Vec::new(),
            expr: None,
        });
    }

    None
}

fn parse_predicate_value(raw: &str) -> serde_json::Value {
    let raw = raw.trim();
    if let Some(inner) = raw
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
        return serde_json::Value::String(inner.replace("''", "'"));
    }
    if raw.eq_ignore_ascii_case("TRUE") {
        return serde_json::Value::Bool(true);
    }
    if raw.eq_ignore_ascii_case("FALSE") {
        return serde_json::Value::Bool(false);
    }
    if raw.eq_ignore_ascii_case("NULL") {
        return serde_json::Value::Null;
    }
    if let Ok(i) = raw.parse::<i64>() {
        return serde_json::json!(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return serde_json::json!(f);
    }
    serde_json::Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_before_like_preserves_original_byte_offsets() {
        let filters = parse_simple_predicates("Straße LIKE 'ß%'");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "Straße");
        assert_eq!(filters[0].value, nodedb_types::Value::String("ß%".into()));

        let filters = parse_simple_predicates("İstanbul ILIKE 'İ%'");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "İstanbul");
        assert_eq!(filters[0].value, nodedb_types::Value::String("İ%".into()));
    }
}
