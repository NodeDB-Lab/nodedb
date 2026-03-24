//! Post-scan filter evaluation for DocumentScan.
//!
//! `ScanFilter` represents a single filter predicate deserialized from the
//! `filters` bytes in a `PhysicalPlan::DocumentScan`. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.
//!
//! This module lives in the bridge layer so both the Control Plane (which
//! constructs `ScanFilter` values during query planning) and the Data Plane
//! (which evaluates them during physical execution) can share the type without
//! violating plane separation.

/// A single filter predicate for DocumentScan post-scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null) and disjunctive groups via the `"or"` operator.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanFilter {
    #[serde(default)]
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: serde_json::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        // match_all: always true (unsupported expression fallback).
        if self.op == "match_all" {
            return true;
        }

        // OR predicate: document matches if ANY clause group fully matches.
        if self.op == "or" {
            return self
                .clauses
                .iter()
                .any(|clause| clause.iter().all(|f| f.matches(doc)));
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == "is_null",
        };

        match self.op.as_str() {
            "eq" => coerced_eq(field_val, &self.value),
            "ne" | "neq" => !coerced_eq(field_val, &self.value),
            "gt" => {
                compare_json_values(Some(field_val), Some(&self.value))
                    == std::cmp::Ordering::Greater
            }
            "gte" | "ge" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            "lt" => {
                compare_json_values(Some(field_val), Some(&self.value)) == std::cmp::Ordering::Less
            }
            "lte" | "le" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            "contains" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            "like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "not_like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "not_ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "in" => {
                if let Some(arr) = self.value.as_array() {
                    arr.iter().any(|v| field_val == v)
                } else {
                    false
                }
            }
            "not_in" => {
                if let Some(arr) = self.value.as_array() {
                    !arr.iter().any(|v| field_val == v)
                } else {
                    true
                }
            }
            "is_null" => field_val.is_null(),
            "is_not_null" => !field_val.is_null(),
            _ => false,
        }
    }
}

/// SQL LIKE pattern matching.
///
/// Supports the standard SQL wildcards:
/// - `%` matches zero or more characters
/// - `_` matches exactly one character
///
/// No escape character support yet (future: `LIKE 'a\%b' ESCAPE '\'`).
///
/// When `case_insensitive` is true, both the input and pattern are lowercased
/// before matching (ILIKE behavior).
fn sql_like_match(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (input, pattern) = if case_insensitive {
        (input.to_lowercase(), pattern.to_lowercase())
    } else {
        (input.to_string(), pattern.to_string())
    };

    let input = input.as_bytes();
    let pattern = pattern.as_bytes();

    // DP-free two-pointer matching (same algorithm as `fnmatch` but for SQL LIKE).
    // Tracks the last `%` position for backtracking.
    let (mut i, mut j) = (0usize, 0usize);
    let (mut star_j, mut star_i) = (usize::MAX, 0usize);

    while i < input.len() {
        if j < pattern.len() && (pattern[j] == b'_' || pattern[j] == input[i]) {
            // Exact match or single-char wildcard.
            i += 1;
            j += 1;
        } else if j < pattern.len() && pattern[j] == b'%' {
            // Multi-char wildcard: remember position for backtracking.
            star_j = j;
            star_i = i;
            j += 1;
        } else if star_j != usize::MAX {
            // Backtrack: advance the input position matched by the last `%`.
            star_i += 1;
            i = star_i;
            j = star_j + 1;
        } else {
            return false;
        }
    }

    // Consume trailing `%` wildcards in the pattern.
    while j < pattern.len() && pattern[j] == b'%' {
        j += 1;
    }

    j == pattern.len()
}

// Re-export shared JSON operations from the centralized module.
pub use super::json_ops::{coerced_eq, compare_json_optional as compare_json_values};

/// Compute an aggregate function over a group of JSON documents.
///
/// Supported operations: count, sum, avg, min, max.
pub fn compute_aggregate(op: &str, field: &str, docs: &[serde_json::Value]) -> serde_json::Value {
    match op {
        "count" => serde_json::json!(docs.len()),

        "sum" => {
            let total: f64 = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .sum();
            serde_json::json!(total)
        }

        "avg" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                serde_json::json!(avg)
            }
        }

        "min" => {
            let min = docs
                .iter()
                .filter_map(|d| d.get(field))
                .min_by(|a, b| compare_json_values(Some(a), Some(b)));
            match min {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "max" => {
            let max = docs
                .iter()
                .filter_map(|d| d.get(field))
                .max_by(|a, b| compare_json_values(Some(a), Some(b)));
            match max {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "count_distinct" => {
            let mut seen = std::collections::HashSet::new();
            for d in docs {
                if let Some(v) = d.get(field) {
                    seen.insert(v.to_string());
                }
            }
            serde_json::json!(seen.len())
        }

        "stddev" | "stddev_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance.sqrt())
        }

        "stddev_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance.sqrt())
        }

        "variance" | "var_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance)
        }

        "var_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance)
        }

        "array_agg" => {
            let values: Vec<serde_json::Value> =
                docs.iter().filter_map(|d| d.get(field).cloned()).collect();
            serde_json::Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<String> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_str()).map(String::from))
                .collect();
            serde_json::Value::String(values.join(","))
        }

        "percentile_cont" => {
            // Expects field format "percentile:field_name" (e.g., "0.5:salary").
            // Falls back to median (p=0.5) if no percentile specified.
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                let p: f64 = field[..idx].parse().unwrap_or(0.5);
                (p, &field[idx + 1..])
            } else {
                (0.5, field)
            };
            let mut values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(actual_field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                return serde_json::Value::Null;
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            serde_json::json!(result)
        }

        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cmp::Ordering;

    // ── Type coercion tests ─────────────────────────────────────────

    #[test]
    fn coerce_number_number() {
        assert_eq!(
            compare_json_values(Some(&json!(5)), Some(&json!(4))),
            Ordering::Greater
        );
        assert_eq!(
            compare_json_values(Some(&json!(3.0)), Some(&json!(3.0))),
            Ordering::Equal
        );
    }

    #[test]
    fn coerce_string_number() {
        // "5" > 4 should work via coercion.
        assert_eq!(
            compare_json_values(Some(&json!("5")), Some(&json!(4))),
            Ordering::Greater
        );
        // 4 < "5" should also work.
        assert_eq!(
            compare_json_values(Some(&json!(4)), Some(&json!("5"))),
            Ordering::Less
        );
    }

    #[test]
    fn coerce_string_string_numeric() {
        // Both are numeric strings.
        assert_eq!(
            compare_json_values(Some(&json!("10")), Some(&json!("9"))),
            Ordering::Greater
        );
    }

    #[test]
    fn coerce_string_string_non_numeric() {
        // Non-numeric strings: lexicographic.
        assert_eq!(
            compare_json_values(Some(&json!("apple")), Some(&json!("banana"))),
            Ordering::Less
        );
    }

    #[test]
    fn coerced_eq_mixed_types() {
        assert!(coerced_eq(&json!(5), &json!("5")));
        assert!(coerced_eq(&json!("5"), &json!(5)));
        assert!(coerced_eq(&json!(2.125), &json!("2.125")));
        assert!(!coerced_eq(&json!(5), &json!("6")));
        assert!(!coerced_eq(&json!("hello"), &json!(5)));
    }

    #[test]
    fn coerced_eq_same_types() {
        assert!(coerced_eq(&json!(5), &json!(5)));
        assert!(coerced_eq(&json!("hello"), &json!("hello")));
        assert!(!coerced_eq(&json!(5), &json!(6)));
    }

    // ── ScanFilter with coercion ────────────────────────────────────

    #[test]
    fn filter_eq_coercion() {
        let doc = json!({"age": 25});
        let filter = ScanFilter {
            field: "age".into(),
            op: "eq".into(),
            value: json!("25"),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn filter_gt_coercion() {
        let doc = json!({"score": "90"});
        let filter = ScanFilter {
            field: "score".into(),
            op: "gt".into(),
            value: json!(80),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn filter_lt_coercion() {
        let doc = json!({"price": 10});
        let filter = ScanFilter {
            field: "price".into(),
            op: "lt".into(),
            value: json!("20"),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn filter_ne_coercion() {
        let doc = json!({"status": 1});
        let filter = ScanFilter {
            field: "status".into(),
            op: "ne".into(),
            value: json!("1"),
            clauses: vec![],
        };
        // 1 == "1" after coercion, so ne should be false.
        assert!(!filter.matches(&doc));
    }

    // ── SQL LIKE tests ──────────────────────────────────────────────

    #[test]
    fn like_basic() {
        assert!(sql_like_match("hello world", "%world", false));
        assert!(sql_like_match("hello world", "hello%", false));
        assert!(sql_like_match("hello world", "%lo wo%", false));
        assert!(!sql_like_match("hello world", "xyz%", false));
    }

    #[test]
    fn like_single_char() {
        assert!(sql_like_match("cat", "c_t", false));
        assert!(!sql_like_match("cat", "c__t", false));
    }

    #[test]
    fn ilike_case_insensitive() {
        assert!(sql_like_match("Hello", "hello", true));
        assert!(sql_like_match("WORLD", "%world%", true));
    }

    // ── Aggregate tests ─────────────────────────────────────────────

    #[test]
    fn aggregate_count() {
        let docs = vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})];
        assert_eq!(compute_aggregate("count", "x", &docs), json!(3));
    }

    #[test]
    fn aggregate_sum() {
        let docs = vec![json!({"v": 10}), json!({"v": 20}), json!({"v": 30})];
        assert_eq!(compute_aggregate("sum", "v", &docs), json!(60.0));
    }

    #[test]
    fn aggregate_min_max() {
        let docs = vec![json!({"v": 5}), json!({"v": 1}), json!({"v": 9})];
        assert_eq!(compute_aggregate("min", "v", &docs), json!(1));
        assert_eq!(compute_aggregate("max", "v", &docs), json!(9));
    }
}
