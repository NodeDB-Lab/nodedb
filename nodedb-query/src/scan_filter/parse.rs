use super::ScanFilter;

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
                "=" => "eq",
                "!=" | "<>" => "ne",
                ">" => "gt",
                ">=" => "gte",
                "<" => "lt",
                "<=" => "lte",
                _ => return None,
            };
            return Some(ScanFilter {
                field,
                op: op.to_string(),
                value: parse_predicate_value(raw_value),
                clauses: Vec::new(),
            });
        }
    }

    let upper = clause.to_uppercase();
    if let Some(pos) = upper.find(" LIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 6..].trim();
        return Some(ScanFilter {
            field,
            op: "like".to_string(),
            value: parse_predicate_value(raw_value),
            clauses: Vec::new(),
        });
    }
    if let Some(pos) = upper.find(" ILIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 7..].trim();
        return Some(ScanFilter {
            field,
            op: "ilike".to_string(),
            value: parse_predicate_value(raw_value),
            clauses: Vec::new(),
        });
    }

    None
}

fn parse_predicate_value(raw: &str) -> serde_json::Value {
    let raw = raw.trim();
    if raw.starts_with('\'') && raw.ends_with('\'') && raw.len() >= 2 {
        let inner = &raw[1..raw.len() - 1];
        return serde_json::Value::String(inner.replace("''", "'"));
    }
    let upper = raw.to_uppercase();
    if upper == "TRUE" {
        return serde_json::Value::Bool(true);
    }
    if upper == "FALSE" {
        return serde_json::Value::Bool(false);
    }
    if upper == "NULL" {
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
