// SPDX-License-Identifier: BUSL-1.1

//! Predicate class hashing for OLLP circuit-breaker keying.
//!
//! The `predicate_class` function maps a (collection, filter-text) pair to a
//! stable u64 hash used by the `OllpOrchestrator` to track per-predicate
//! retry and circuit-breaker state. Two queries with the same predicate shape
//! but different bound values produce the same class hash.

use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_types::Value;

use crate::util::fnv1a_hash;

/// Compute a stable hash for a predicate class.
///
/// **Degraded path note**: `Filter` is not zerompk-encodable. This function
/// accepts the canonical SQL text representation of the filter and normalizes
/// numeric and string literals to their type tags before hashing. Two queries
/// with the same predicate shape but different bound values will produce the
/// same `predicate_class`. Example: `WHERE balance > 1000` and
/// `WHERE balance > 9999` both normalize to `WHERE balance > i64`.
///
/// The collection name is mixed in so predicates on different collections
/// don't collide.
pub fn predicate_class(canonical_filter_sql: &str, collection: &str) -> u64 {
    let normalized = normalize_predicate_text(canonical_filter_sql);
    let mut buf = Vec::with_capacity(collection.len() + normalized.len() + 1);
    buf.extend_from_slice(collection.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(normalized.as_bytes());
    fnv1a_hash(&buf)
}

/// Normalize a SQL text predicate by replacing literal values with type tags.
///
/// - Integer/float literals → `i64` or `f64`
/// - Quoted string literals → `str`
/// - Preserves operators, field names, and keywords
fn normalize_predicate_text(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Quoted string literal
        if c == '\'' {
            out.push_str("str");
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    i += 1;
                    // Handle escaped quote ''
                    if i < chars.len() && chars[i] == '\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Numeric literal (integer or float)
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let mut is_float = false;
            i += 1; // skip leading digit or minus
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                if chars[i] == '.' {
                    is_float = true;
                }
                i += 1;
            }
            if is_float {
                out.push_str("f64");
            } else {
                out.push_str("i64");
            }
            continue;
        }

        out.push(c);
        i += 1;
    }

    out
}

/// Map a `Value` variant to a stable, short type tag.
///
/// Only the type (not the literal) is emitted so that two filters with the
/// same predicate shape but different bound values hash to the same class.
fn value_type_tag(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Integer(_) => "i64",
        Value::Float(_) => "f64",
        Value::String(_) => "str",
        Value::Bytes(_) => "bytes",
        Value::Array(_) => "arr",
        Value::Set(_) => "arr",
        Value::Object(_) => "obj",
        Value::Uuid(_) => "uuid",
        Value::Ulid(_) => "ulid",
        Value::DateTime(_) => "datetime",
        Value::NaiveDateTime(_) => "naive_datetime",
        Value::Duration(_) => "duration",
        Value::Decimal(_) => "decimal",
        Value::Geometry(_) => "geometry",
        Value::Regex(_) => "regex",
        Value::Range { .. } => "range",
        Value::Record { .. } => "record",
        Value::ArrayCell(_) => "array_cell",
        Value::Vector(_) => "vec",
        // `Value` is `#[non_exhaustive]`; a wildcard is required. Any future
        // variant falls back to a stable generic tag until given its own.
        _ => "val",
    }
}

/// Render a single `ScanFilter` to a stable canonical string clause.
fn render_filter(f: &ScanFilter) -> String {
    match f.op {
        FilterOp::Or => {
            let groups: Vec<String> = f
                .clauses
                .iter()
                .map(|group| {
                    let parts: Vec<String> = group.iter().map(render_filter).collect();
                    format!("({})", parts.join(" AND "))
                })
                .collect();
            format!("({})", groups.join(" OR "))
        }
        FilterOp::Expr => {
            // NOTE: expr predicates are keyed per-exact (not shape-normalized) —
            // acceptable and bounded; shape-normalizing arbitrary SqlExpr is a
            // deferred refinement.
            let bytes = match &f.expr {
                Some(expr) => zerompk::to_msgpack_vec(expr).unwrap_or_default(),
                None => Vec::new(),
            };
            format!("expr:{:016x}", fnv1a_hash(&bytes))
        }
        // Value-less ops: no value type tag needed.
        FilterOp::IsNull
        | FilterOp::IsNotNull
        | FilterOp::MatchAll
        | FilterOp::Exists
        | FilterOp::NotExists => {
            format!("{} {}", f.field, f.op.as_str())
        }
        // Column-comparison ops: the value holds the RHS COLUMN NAME, which is
        // part of the predicate shape (an identifier, not a bound literal), so
        // emit it verbatim rather than collapsing it to a type tag.
        FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => {
            let rhs_col = f.value.as_str().unwrap_or("");
            format!("{} {} col:{}", f.field, f.op.as_str(), rhs_col)
        }
        // All other ops carry a bound value literal; emit field + op + type tag.
        _ => {
            format!("{} {} {}", f.field, f.op.as_str(), value_type_tag(&f.value))
        }
    }
}

/// Derive a stable canonical text from a slice of `ScanFilter`s.
///
/// Filters are rendered in slice order (the order the planner produces them)
/// and joined with `" AND "`. The resulting text is suitable for passing to
/// `predicate_class` for shape-grouped circuit-breaker keying.
fn canonical_filter_text(filters: &[ScanFilter]) -> String {
    filters
        .iter()
        .map(render_filter)
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Compute a stable circuit-breaker class hash from serialised `ScanFilter`
/// bytes and the collection name.
///
/// # Contract
///
/// This function is **intentionally infallible**. It is used only as a
/// circuit-breaker hash key, not for correctness. On decode failure (our own
/// bytes; not expected in practice) it falls back to a degraded-but-safe
/// `predicate_class("", collection)` and logs a debug message so the circuit
/// breaker remains functional — it just loses predicate-shape granularity for
/// that one call.
///
/// `filter_bytes` must be a `zerompk`-encoded `Vec<ScanFilter>` as produced
/// by `serialize_filters` / `extract_bulk_predicate_info`.
pub fn predicate_class_for_filters(filter_bytes: &[u8], collection: &str) -> u64 {
    match zerompk::from_msgpack::<Vec<ScanFilter>>(filter_bytes) {
        Ok(filters) => predicate_class(&canonical_filter_text(&filters), collection),
        Err(e) => {
            tracing::debug!(
                collection,
                error = %e,
                "predicate_class_for_filters: failed to decode filter bytes; \
                 falling back to collection-only class (degraded granularity)"
            );
            predicate_class("", collection)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_shape_different_literal_same_hash() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 9999", "accounts");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_field_different_hash() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE age > 1000", "accounts");
        assert_ne!(h1, h2);
    }

    #[test]
    fn different_collection_different_hash() {
        let h1 = predicate_class("WHERE x > 1", "col_a");
        let h2 = predicate_class("WHERE x > 1", "col_b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn string_literals_normalized() {
        let h1 = predicate_class("WHERE name = 'alice'", "users");
        let h2 = predicate_class("WHERE name = 'bob'", "users");
        assert_eq!(h1, h2);
    }

    #[test]
    fn float_literals_normalized() {
        let h1 = predicate_class("WHERE score > 1.5", "items");
        let h2 = predicate_class("WHERE score > 9.9", "items");
        assert_eq!(h1, h2);
    }

    // ── predicate_class_for_filters tests ────────────────────────────────────

    fn encode_filters(filters: &[ScanFilter]) -> Vec<u8> {
        zerompk::to_msgpack_vec(&filters.to_vec()).expect("encode filters")
    }

    #[test]
    fn for_filters_same_shape_different_integer_literals_same_class() {
        let f1 = vec![ScanFilter {
            field: "balance".into(),
            op: FilterOp::Gt,
            value: Value::Integer(1000),
            clauses: Vec::new(),
            expr: None,
        }];
        let f2 = vec![ScanFilter {
            field: "balance".into(),
            op: FilterOp::Gt,
            value: Value::Integer(9999),
            clauses: Vec::new(),
            expr: None,
        }];
        let h1 = predicate_class_for_filters(&encode_filters(&f1), "accounts");
        let h2 = predicate_class_for_filters(&encode_filters(&f2), "accounts");
        assert_eq!(
            h1, h2,
            "same shape, different integer literals must be same class"
        );
    }

    #[test]
    fn for_filters_same_shape_different_string_literals_same_class() {
        let f1 = vec![ScanFilter {
            field: "name".into(),
            op: FilterOp::Eq,
            value: Value::String("alice".into()),
            clauses: Vec::new(),
            expr: None,
        }];
        let f2 = vec![ScanFilter {
            field: "name".into(),
            op: FilterOp::Eq,
            value: Value::String("bob".into()),
            clauses: Vec::new(),
            expr: None,
        }];
        let h1 = predicate_class_for_filters(&encode_filters(&f1), "users");
        let h2 = predicate_class_for_filters(&encode_filters(&f2), "users");
        assert_eq!(
            h1, h2,
            "same shape, different string literals must be same class"
        );
    }

    #[test]
    fn for_filters_different_field_different_class() {
        let f1 = vec![ScanFilter {
            field: "balance".into(),
            op: FilterOp::Gt,
            value: Value::Integer(100),
            clauses: Vec::new(),
            expr: None,
        }];
        let f2 = vec![ScanFilter {
            field: "age".into(),
            op: FilterOp::Gt,
            value: Value::Integer(100),
            clauses: Vec::new(),
            expr: None,
        }];
        let h1 = predicate_class_for_filters(&encode_filters(&f1), "accounts");
        let h2 = predicate_class_for_filters(&encode_filters(&f2), "accounts");
        assert_ne!(h1, h2, "different field must produce different class");
    }

    #[test]
    fn for_filters_different_op_different_class() {
        let f1 = vec![ScanFilter {
            field: "score".into(),
            op: FilterOp::Gt,
            value: Value::Integer(5),
            clauses: Vec::new(),
            expr: None,
        }];
        let f2 = vec![ScanFilter {
            field: "score".into(),
            op: FilterOp::Lt,
            value: Value::Integer(5),
            clauses: Vec::new(),
            expr: None,
        }];
        let h1 = predicate_class_for_filters(&encode_filters(&f1), "items");
        let h2 = predicate_class_for_filters(&encode_filters(&f2), "items");
        assert_ne!(h1, h2, "different op must produce different class");
    }

    #[test]
    fn for_filters_different_collection_different_class() {
        let filters = vec![ScanFilter {
            field: "x".into(),
            op: FilterOp::Eq,
            value: Value::Integer(1),
            clauses: Vec::new(),
            expr: None,
        }];
        let bytes = encode_filters(&filters);
        let h1 = predicate_class_for_filters(&bytes, "col_a");
        let h2 = predicate_class_for_filters(&bytes, "col_b");
        assert_ne!(h1, h2, "different collection must produce different class");
    }

    #[test]
    fn for_filters_or_filter_stable_hash() {
        let or_filter = vec![ScanFilter {
            field: String::new(),
            op: FilterOp::Or,
            value: Value::Null,
            clauses: vec![
                vec![ScanFilter {
                    field: "status".into(),
                    op: FilterOp::Eq,
                    value: Value::String("active".into()),
                    clauses: Vec::new(),
                    expr: None,
                }],
                vec![ScanFilter {
                    field: "status".into(),
                    op: FilterOp::Eq,
                    value: Value::String("pending".into()),
                    clauses: Vec::new(),
                    expr: None,
                }],
            ],
            expr: None,
        }];
        let bytes = encode_filters(&or_filter);
        let h1 = predicate_class_for_filters(&bytes, "orders");
        let h2 = predicate_class_for_filters(&bytes, "orders");
        assert_eq!(
            h1, h2,
            "OR filter must produce stable hash across two calls"
        );
    }

    #[test]
    fn for_filters_empty_bytes_degraded_fallback_stable() {
        // Garbage bytes → decode error → degraded fallback; must not panic and
        // must produce the same result on repeated calls.
        let h1 = predicate_class_for_filters(b"not valid msgpack", "col");
        let h2 = predicate_class_for_filters(b"not valid msgpack", "col");
        assert_eq!(h1, h2, "degraded fallback must be deterministic");
    }
}
