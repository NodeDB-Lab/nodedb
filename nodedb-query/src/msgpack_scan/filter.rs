//! Binary filter evaluation on raw MessagePack documents.
//!
//! `ScanFilter::matches_binary(doc: &[u8])` evaluates a filter predicate
//! directly on msgpack bytes without decoding to `serde_json::Value`.
//! Uses `Value::eq_coerced`/`cmp_coerced` for type coercion — single
//! source of truth shared with the JSON filter path.

use std::cmp::Ordering;

use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::index::FieldIndex;
use crate::msgpack_scan::reader::{
    array_header, map_header, read_null, read_str, read_value, skip_value,
};
use crate::scan_filter::like::sql_like_match;
use crate::scan_filter::{FilterOp, ScanFilter};

impl ScanFilter {
    /// Evaluate this filter against a raw MessagePack document.
    ///
    /// Zero deserialization — extracts only the needed field bytes.
    pub fn matches_binary(&self, doc: &[u8]) -> bool {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return true,
            FilterOp::Or => {
                return self
                    .clauses
                    .iter()
                    .any(|clause| clause.iter().all(|f| f.matches_binary(doc)));
            }
            _ => {}
        }

        let (start, end) = match extract_field(doc, 0, &self.field) {
            Some(r) => r,
            None => {
                // Qualified-name fallback: "amount" might be stored as "orders.amount".
                let suffix = format!(".{}", self.field);
                match find_field_by_suffix(doc, &suffix) {
                    Some(r) => r,
                    None => return self.op == FilterOp::IsNull,
                }
            }
        };

        eval_op(self, doc, start, end)
    }

    /// Evaluate using a pre-built `FieldIndex` for O(1) field lookup.
    ///
    /// Use when evaluating multiple predicates on the same document.
    pub fn matches_binary_indexed(&self, doc: &[u8], idx: &FieldIndex) -> bool {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return true,
            FilterOp::Or => {
                return self
                    .clauses
                    .iter()
                    .any(|clause| clause.iter().all(|f| f.matches_binary_indexed(doc, idx)));
            }
            _ => {}
        }

        let (start, end) = match idx.get(&self.field) {
            Some(r) => r,
            None => return self.op == FilterOp::IsNull,
        };

        eval_op(self, doc, start, end)
    }
}

/// Shared filter op evaluation — used by both `matches_binary` and `matches_binary_indexed`.
fn eval_op(filter: &ScanFilter, doc: &[u8], start: usize, _end: usize) -> bool {
    match filter.op {
        FilterOp::IsNull => read_null(doc, start),
        FilterOp::IsNotNull => !read_null(doc, start),
        FilterOp::Eq => eq_value(doc, start, &filter.value),
        FilterOp::Ne => !eq_value(doc, start, &filter.value),
        FilterOp::Gt => cmp_value(doc, start, &filter.value) == Ordering::Greater,
        FilterOp::Gte => {
            let c = cmp_value(doc, start, &filter.value);
            c == Ordering::Greater || c == Ordering::Equal
        }
        FilterOp::Lt => cmp_value(doc, start, &filter.value) == Ordering::Less,
        FilterOp::Lte => {
            let c = cmp_value(doc, start, &filter.value);
            c == Ordering::Less || c == Ordering::Equal
        }
        FilterOp::Contains => {
            if let (Some(s), Some(pattern)) = (read_str(doc, start), filter.value.as_str()) {
                s.contains(pattern)
            } else {
                false
            }
        }
        FilterOp::Like => str_match(doc, start, &filter.value, false, false),
        FilterOp::NotLike => str_match(doc, start, &filter.value, false, true),
        FilterOp::Ilike => str_match(doc, start, &filter.value, true, false),
        FilterOp::NotIlike => str_match(doc, start, &filter.value, true, true),
        FilterOp::In => {
            if let Some(mut iter) = filter.value.as_array_iter() {
                iter.any(|v| eq_value(doc, start, v))
            } else {
                false
            }
        }
        FilterOp::NotIn => {
            if let Some(mut iter) = filter.value.as_array_iter() {
                !iter.any(|v| eq_value(doc, start, v))
            } else {
                true
            }
        }
        FilterOp::ArrayContains => array_any(doc, start, |elem_start| {
            eq_value(doc, elem_start, &filter.value)
        }),
        FilterOp::ArrayContainsAll => {
            if let Some(mut needles) = filter.value.as_array_iter() {
                needles.all(|needle| {
                    array_any(doc, start, |elem_start| eq_value(doc, elem_start, needle))
                })
            } else {
                false
            }
        }
        FilterOp::ArrayOverlap => {
            if let Some(mut needles) = filter.value.as_array_iter() {
                needles.any(|needle| {
                    array_any(doc, start, |elem_start| eq_value(doc, elem_start, needle))
                })
            } else {
                false
            }
        }
        // Column-vs-column: extract right-side field from the same doc.
        FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => {
            let other_col = match &filter.value {
                nodedb_types::Value::String(s) => s.as_str(),
                _ => return false,
            };
            // Try exact field name, then qualified suffix match.
            let other_range = extract_field(doc, 0, other_col).or_else(|| {
                let suffix = format!(".{other_col}");
                find_field_by_suffix(doc, &suffix)
            });
            let Some((other_start, _)) = other_range else {
                return false;
            };
            // Read both sides as Value and compare.
            let left = read_value(doc, start).unwrap_or(nodedb_types::Value::Null);
            let right = read_value(doc, other_start).unwrap_or(nodedb_types::Value::Null);
            match filter.op {
                FilterOp::GtColumn => left.cmp_coerced(&right) == Ordering::Greater,
                FilterOp::GteColumn => left.cmp_coerced(&right) != Ordering::Less,
                FilterOp::LtColumn => left.cmp_coerced(&right) == Ordering::Less,
                FilterOp::LteColumn => left.cmp_coerced(&right) != Ordering::Greater,
                FilterOp::EqColumn => left.eq_coerced(&right),
                FilterOp::NeColumn => !left.eq_coerced(&right),
                _ => false,
            }
        }
        _ => false,
    }
}

/// Find a field in a msgpack map by suffix match (e.g., ".amount" matches "orders.amount").
fn find_field_by_suffix(doc: &[u8], suffix: &str) -> Option<(usize, usize)> {
    let (count, mut pos) = map_header(doc, 0)?;
    for _ in 0..count {
        let key = read_str(doc, pos);
        let key_end = skip_value(doc, pos)?;
        let val_start = key_end;
        let val_end = skip_value(doc, val_start)?;
        if let Some(k) = key
            && k.ends_with(suffix)
        {
            return Some((val_start, val_end));
        }
        pos = val_end;
    }
    None
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Coerced equality: read msgpack value at offset → compare with `Value`.
/// Uses `Value::eq_coerced` — single source of truth for type coercion.
#[inline]
fn eq_value(buf: &[u8], offset: usize, filter_val: &nodedb_types::Value) -> bool {
    if read_null(buf, offset) {
        return filter_val.is_null();
    }
    match read_value(buf, offset) {
        Some(field_val) => filter_val.eq_coerced(&field_val),
        None => false,
    }
}

/// Coerced ordering: read msgpack value at offset → compare with `Value`.
/// Uses `Value::cmp_coerced` — single source of truth for ordering.
///
/// Returns ordering of field_val relative to filter_val (field <=> filter).
#[inline]
fn cmp_value(buf: &[u8], offset: usize, filter_val: &nodedb_types::Value) -> Ordering {
    match read_value(buf, offset) {
        Some(field_val) => field_val.cmp_coerced(filter_val),
        None => Ordering::Equal,
    }
}

/// LIKE/ILIKE/NOT LIKE/NOT ILIKE helper.
#[inline]
fn str_match(
    buf: &[u8],
    offset: usize,
    pattern_val: &nodedb_types::Value,
    icase: bool,
    negate: bool,
) -> bool {
    let result = if let (Some(s), Some(pattern)) = (read_str(buf, offset), pattern_val.as_str()) {
        sql_like_match(s, pattern, icase)
    } else {
        false
    };
    if negate { !result } else { result }
}

/// Iterate msgpack array elements, return true if any satisfies predicate.
fn array_any(buf: &[u8], start: usize, mut pred: impl FnMut(usize) -> bool) -> bool {
    let Some((count, mut pos)) = array_header(buf, start) else {
        return false;
    };
    for _ in 0..count {
        if pred(pos) {
            return true;
        }
        let Some(next) = skip_value(buf, pos) else {
            return false;
        };
        pos = next;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn filter(field: &str, op: &str, value: nodedb_types::Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op: op.into(),
            value,
            clauses: vec![],
        }
    }

    #[test]
    fn eq_integer() {
        let doc = encode(&json!({"age": 25}));
        assert!(filter("age", "eq", nodedb_types::Value::Integer(25)).matches_binary(&doc));
        assert!(!filter("age", "eq", nodedb_types::Value::Integer(30)).matches_binary(&doc));
    }

    #[test]
    fn eq_string() {
        let doc = encode(&json!({"name": "alice"}));
        assert!(
            filter("name", "eq", nodedb_types::Value::String("alice".into())).matches_binary(&doc)
        );
    }

    #[test]
    fn eq_coercion_int_vs_string() {
        let doc = encode(&json!({"age": 25}));
        assert!(filter("age", "eq", nodedb_types::Value::String("25".into())).matches_binary(&doc));
    }

    #[test]
    fn eq_coercion_string_vs_int() {
        let doc = encode(&json!({"score": "90"}));
        assert!(filter("score", "eq", nodedb_types::Value::Integer(90)).matches_binary(&doc));
    }

    #[test]
    fn ne() {
        let doc = encode(&json!({"x": 1}));
        assert!(filter("x", "ne", nodedb_types::Value::Integer(2)).matches_binary(&doc));
        assert!(!filter("x", "ne", nodedb_types::Value::Integer(1)).matches_binary(&doc));
    }

    #[test]
    fn gt_lt() {
        let doc = encode(&json!({"v": 10}));
        assert!(filter("v", "gt", nodedb_types::Value::Integer(5)).matches_binary(&doc));
        assert!(!filter("v", "gt", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(filter("v", "lt", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(!filter("v", "lt", nodedb_types::Value::Integer(5)).matches_binary(&doc));
    }

    #[test]
    fn gte_lte() {
        let doc = encode(&json!({"v": 10}));
        assert!(filter("v", "gte", nodedb_types::Value::Integer(10)).matches_binary(&doc));
        assert!(filter("v", "gte", nodedb_types::Value::Integer(5)).matches_binary(&doc));
        assert!(!filter("v", "gte", nodedb_types::Value::Integer(15)).matches_binary(&doc));
        assert!(filter("v", "lte", nodedb_types::Value::Integer(10)).matches_binary(&doc));
    }

    #[test]
    fn is_null_not_null() {
        let doc = encode(&json!({"a": null, "b": 1}));
        assert!(filter("a", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(!filter("b", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
        assert!(filter("b", "is_not_null", nodedb_types::Value::Null).matches_binary(&doc));
    }

    #[test]
    fn missing_field_is_null() {
        let doc = encode(&json!({"x": 1}));
        assert!(filter("missing", "is_null", nodedb_types::Value::Null).matches_binary(&doc));
    }

    #[test]
    fn contains_str() {
        let doc = encode(&json!({"msg": "hello world"}));
        assert!(
            filter(
                "msg",
                "contains",
                nodedb_types::Value::String("world".into())
            )
            .matches_binary(&doc)
        );
    }

    #[test]
    fn like_ilike() {
        let doc = encode(&json!({"name": "Alice"}));
        assert!(
            filter("name", "like", nodedb_types::Value::String("Ali%".into())).matches_binary(&doc)
        );
        assert!(
            !filter("name", "like", nodedb_types::Value::String("ali%".into()))
                .matches_binary(&doc)
        );
        assert!(
            filter("name", "ilike", nodedb_types::Value::String("ali%".into()))
                .matches_binary(&doc)
        );
        assert!(
            filter(
                "name",
                "not_like",
                nodedb_types::Value::String("Bob%".into())
            )
            .matches_binary(&doc)
        );
    }

    #[test]
    fn in_not_in() {
        let doc = encode(&json!({"status": "active"}));
        let vals = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("active".into()),
            nodedb_types::Value::String("pending".into()),
        ]);
        assert!(
            ScanFilter {
                field: "status".into(),
                op: "in".into(),
                value: vals.clone(),
                clauses: vec![]
            }
            .matches_binary(&doc)
        );

        let doc2 = encode(&json!({"status": "deleted"}));
        assert!(
            ScanFilter {
                field: "status".into(),
                op: "not_in".into(),
                value: vals,
                clauses: vec![]
            }
            .matches_binary(&doc2)
        );
    }

    #[test]
    fn array_contains() {
        let doc = encode(&json!({"tags": ["rust", "db", "fast"]}));
        assert!(
            filter(
                "tags",
                "array_contains",
                nodedb_types::Value::String("rust".into())
            )
            .matches_binary(&doc)
        );
        assert!(
            !filter(
                "tags",
                "array_contains",
                nodedb_types::Value::String("slow".into())
            )
            .matches_binary(&doc)
        );
    }

    #[test]
    fn array_contains_all() {
        let doc = encode(&json!({"tags": ["a", "b", "c"]}));
        let needles = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("a".into()),
            nodedb_types::Value::String("c".into()),
        ]);
        assert!(
            ScanFilter {
                field: "tags".into(),
                op: "array_contains_all".into(),
                value: needles,
                clauses: vec![]
            }
            .matches_binary(&doc)
        );
    }

    #[test]
    fn array_overlap() {
        let doc = encode(&json!({"tags": ["x", "y"]}));
        let needles = nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("y".into()),
            nodedb_types::Value::String("z".into()),
        ]);
        assert!(
            ScanFilter {
                field: "tags".into(),
                op: "array_overlap".into(),
                value: needles,
                clauses: vec![]
            }
            .matches_binary(&doc)
        );
    }

    #[test]
    fn or_clauses() {
        let doc = encode(&json!({"x": 5}));
        let f = ScanFilter {
            field: String::new(),
            op: "or".into(),
            value: nodedb_types::Value::Null,
            clauses: vec![
                vec![filter("x", "eq", nodedb_types::Value::Integer(10))],
                vec![filter("x", "eq", nodedb_types::Value::Integer(5))],
            ],
        };
        assert!(f.matches_binary(&doc));
    }

    #[test]
    fn match_all() {
        let doc = encode(&json!({"any": "thing"}));
        assert!(filter("", "match_all", nodedb_types::Value::Null).matches_binary(&doc));
    }

    #[test]
    fn float_comparison() {
        let doc = encode(&json!({"temp": 36.6}));
        assert!(filter("temp", "gt", nodedb_types::Value::Float(30.0)).matches_binary(&doc));
        assert!(filter("temp", "lt", nodedb_types::Value::Float(40.0)).matches_binary(&doc));
    }

    #[test]
    fn bool_eq() {
        let doc = encode(&json!({"active": true}));
        assert!(filter("active", "eq", nodedb_types::Value::Bool(true)).matches_binary(&doc));
        assert!(!filter("active", "eq", nodedb_types::Value::Bool(false)).matches_binary(&doc));
    }

    #[test]
    fn gt_coercion_string_field() {
        let doc = encode(&json!({"score": "90"}));
        assert!(filter("score", "gt", nodedb_types::Value::Integer(80)).matches_binary(&doc));
    }

    // ── Indexed variant tests ──────────────────────────────────────────

    #[test]
    fn indexed_matches_same_as_sequential() {
        let doc = encode(&json!({"a": 1, "b": "hello", "c": true, "d": null}));
        let idx = FieldIndex::build(&doc, 0).unwrap();

        let filters = vec![
            filter("a", "eq", nodedb_types::Value::Integer(1)),
            filter("b", "contains", nodedb_types::Value::String("ell".into())),
            filter("c", "eq", nodedb_types::Value::Bool(true)),
            filter("d", "is_null", nodedb_types::Value::Null),
            filter("missing", "is_null", nodedb_types::Value::Null),
        ];

        for f in &filters {
            assert_eq!(
                f.matches_binary(&doc),
                f.matches_binary_indexed(&doc, &idx),
                "mismatch for field={} op={:?}",
                f.field,
                f.op
            );
        }
    }
}
